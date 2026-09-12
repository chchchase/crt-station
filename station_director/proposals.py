import copy
import hashlib
import json
import os
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import jsonschema

from station_director.inventory import load_snapshots
from station_director.policy import ROOT


PROPOSAL_ROOT = ROOT / "runtime" / "director" / "proposals"
STAGING_ROOT = ROOT / "runtime" / "director" / "staging"
SCHEMA_PATHS = {
    1: Path(__file__).parent / "schemas" / "proposal.v1.schema.json",
    2: Path(__file__).parent / "schemas" / "proposal.v2.schema.json",
}
STATION_TIMEZONE = ZoneInfo("America/Los_Angeles")


class ProposalError(RuntimeError):
    pass


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def latest_inventory(root=ROOT):
    snapshots = load_snapshots(Path(root) / "runtime/director/inventory")
    if not snapshots:
        raise ProposalError("An inventory snapshot is required")
    return snapshots[-1]


def source_hashes(root=ROOT, policy_path=None):
    root = Path(root)
    policy_path = Path(policy_path or root / "director_conf/channel_identities.v2.json")
    inventory_path, _ = latest_inventory(root)
    configs = {
        path.name: sha256(path)
        for path in sorted((root / "confs").glob("*.json"))
    }
    return {
        "configs": configs,
        "database": sha256(root / "runtime/fs42_fluid.db"),
        "policy": sha256(policy_path),
        "inventory": sha256(inventory_path),
        "wio_config": sha256(root / "confs/watch_in_order.json"),
        "wio_state": sha256(root / "runtime/watch_in_order_state.json"),
    }


def week_bounds(value):
    requested = date.fromisoformat(value)
    monday = requested - timedelta(days=requested.weekday())
    start = datetime.combine(monday, time(hour=6), tzinfo=STATION_TIMEZONE)
    return start, start + timedelta(days=7)


def normalize_legacy_boundaries(proposal):
    """Attach station time to proposals written before boundaries had offsets."""
    normalized = copy.deepcopy(proposal)
    for key in ("week_start", "week_end"):
        value = normalized.get(key)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                normalized[key] = parsed.replace(tzinfo=STATION_TIMEZONE).isoformat()
    return normalized


def _schema_errors(proposal, version):
    try:
        schema_path = SCHEMA_PATHS[version]
    except KeyError as exc:
        raise ProposalError(f"Unsupported proposal schema version: {version}") from exc
    schema = json.loads(schema_path.read_text())
    validator = jsonschema.Draft7Validator(schema, format_checker=jsonschema.FormatChecker())
    return sorted(validator.iter_errors(proposal), key=lambda item: list(item.path))


def _validate_week_scope(proposal):
    expected_start, expected_end = week_bounds(proposal["target_week"])
    try:
        actual_start = datetime.fromisoformat(proposal["week_start"])
        actual_end = datetime.fromisoformat(proposal["week_end"])
    except ValueError as exc:
        raise ProposalError(f"Proposal boundary is not a valid RFC 3339 timestamp: {exc}") from exc
    if actual_start.tzinfo is None or actual_start.utcoffset() is None:
        raise ProposalError("Proposal week_start must include a UTC offset")
    if actual_end.tzinfo is None or actual_end.utcoffset() is None:
        raise ProposalError("Proposal week_end must include a UTC offset")
    if (
        proposal["week_start"] != expected_start.isoformat()
        or proposal["week_end"] != expected_end.isoformat()
    ):
        raise ProposalError(
            "Proposal boundaries do not match the canonical America/Los_Angeles target week"
        )

    for index, change in enumerate(proposal["assignment_changes"]):
        if change["action"] == "move" and change["from_channel"] == change["to_channel"]:
            raise ProposalError(f"Assignment change {index} cannot move a series to its current channel")

    first_date = expected_start.date()
    last_date = first_date + timedelta(days=6)
    for index, directive in enumerate(proposal["directives"]):
        dtype = directive["type"]
        if dtype in ("date_slot", "marathon"):
            directive_date = date.fromisoformat(directive["date"])
            if not first_date <= directive_date <= last_date:
                raise ProposalError(
                    f"Directive {index} date {directive_date.isoformat()} is outside "
                    f"proposal week {first_date.isoformat()} through {last_date.isoformat()}"
                )
        elif dtype in ("seasonal", "theme"):
            start_date = date.fromisoformat(directive["start_date"])
            end_date = date.fromisoformat(directive["end_date"])
            if start_date > end_date:
                raise ProposalError(f"Directive {index} start_date is after end_date")
            if start_date < first_date or end_date > last_date:
                raise ProposalError(
                    f"Directive {index} range {start_date.isoformat()} through "
                    f"{end_date.isoformat()} is outside proposal week "
                    f"{first_date.isoformat()} through {last_date.isoformat()}"
                )


def validate_schema(proposal):
    version = proposal.get("schema_version") if isinstance(proposal, dict) else None
    errors = _schema_errors(proposal, version)
    if errors:
        raise ProposalError("Proposal schema error: " + "; ".join(error.message for error in errors))
    if version == 2:
        _validate_week_scope(proposal)


def migrate_v1_to_v2(proposal):
    """Safely migrate a v1 proposal in memory without changing its saved JSON."""
    normalized = normalize_legacy_boundaries(proposal)
    errors = _schema_errors(normalized, 1)
    if errors:
        raise ProposalError(
            "Legacy proposal schema error: " + "; ".join(error.message for error in errors)
        )

    for directive in normalized["directives"]:
        if directive["type"] == "marathon":
            raise ProposalError(
                "Legacy marathon directives use ambiguous 'hours'; recreate the proposal "
                "with an episode 'count'"
            )
        if directive["type"] in ("seasonal", "theme"):
            raise ProposalError(
                f"Legacy {directive['type']} directives have implicit full-day scope; "
                "recreate the proposal with explicit hours or all_day: true"
            )

    migrated = copy.deepcopy(normalized)
    migrated["schema_version"] = 2
    try:
        validate_schema(migrated)
    except ProposalError as exc:
        raise ProposalError(f"Legacy proposal cannot be safely migrated to v2: {exc}") from exc
    return migrated


def inventory_identifier_errors(proposal, inventory):
    """Require proposal series names to match one unambiguous inventory key exactly."""
    index = {}
    for name in inventory.get("shows", {}):
        index.setdefault(name.casefold(), []).append(name)

    requested = []
    requested.extend(
        (f"assignment_changes[{index}]", item["series"])
        for index, item in enumerate(proposal["assignment_changes"])
    )
    requested.extend(
        (f"directives[{index}]", item["series"])
        for index, item in enumerate(proposal["directives"])
    )
    requested.extend(
        (f"exclusions[{index}]", name)
        for index, name in enumerate(proposal["exclusions"])
    )

    failures = []
    for location, requested_name in requested:
        matches = sorted(index.get(requested_name.casefold(), []))
        if not matches:
            failures.append(f"{location} references unknown inventory identifier: {requested_name}")
        elif len(matches) > 1:
            failures.append(
                f"{location} inventory identifier is case-ambiguous: {requested_name} "
                f"matches {', '.join(matches)}"
            )
        elif requested_name != matches[0]:
            failures.append(
                f"{location} must use exact canonical inventory identifier "
                f"'{matches[0]}', not '{requested_name}'"
            )
    return failures


def create_proposal(target_week, source, seed, assignments, directives, exclusions, root=ROOT, policy_path=None):
    start, end = week_bounds(target_week)
    now = datetime.now(timezone.utc)
    proposal_id = f"p-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    proposal = {
        "schema_version": 2,
        "proposal_id": proposal_id,
        "created_at": now.isoformat(),
        "target_week": target_week,
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "source": source,
        "source_hashes": source_hashes(root, policy_path),
        "seed": seed,
        "assignment_changes": assignments,
        "directives": directives,
        "exclusions": exclusions,
    }
    validate_schema(proposal)
    unused_inventory_path, inventory = latest_inventory(root)
    identifier_failures = inventory_identifier_errors(proposal, inventory)
    if identifier_failures:
        raise ProposalError("; ".join(identifier_failures))
    directory = Path(root) / "runtime/director/proposals" / proposal_id
    directory.mkdir(parents=True, exist_ok=False)
    target = directory / "proposal.json"
    target.write_text(json.dumps(proposal, indent=2, sort_keys=True) + "\n")
    return proposal, target


def proposal_dirs(root=ROOT):
    base = Path(root) / "runtime/director/proposals"
    return sorted(path for path in base.glob("p-*") if path.is_dir()) if base.exists() else []


def load_proposal(proposal_id, root=ROOT):
    if not proposal_id.startswith("p-") or "/" in proposal_id or ".." in proposal_id:
        raise ProposalError("Invalid proposal ID")
    path = Path(root) / "runtime/director/proposals" / proposal_id / "proposal.json"
    if not path.is_file():
        raise ProposalError(f"Proposal not found: {proposal_id}")
    proposal = json.loads(path.read_text())
    version = proposal.get("schema_version") if isinstance(proposal, dict) else None
    if version == 1:
        proposal = migrate_v1_to_v2(proposal)
    elif version == 2:
        validate_schema(proposal)
    else:
        raise ProposalError(f"Unsupported proposal schema version: {version}")
    return proposal, path


def archive_proposal(proposal_id, confirmed, root=ROOT):
    if confirmed != proposal_id:
        raise ProposalError("Archiving requires --confirm with the exact proposal ID")
    proposal, path = load_proposal(proposal_id, root)
    source = path.parent
    archive = Path(root) / "runtime/director/proposals/archive"
    archive.mkdir(parents=True, exist_ok=True)
    target = archive / proposal_id
    if target.exists():
        raise ProposalError("Archived proposal already exists")
    os.replace(source, target)
    return target


def stale_sources(proposal, root=ROOT, policy_path=None):
    current = source_hashes(root, policy_path)
    return sorted(key for key in current if current[key] != proposal["source_hashes"][key])


def cleanup_staging(path):
    path = Path(path).resolve()
    expected = (ROOT / "runtime/director/staging").resolve()
    if expected not in path.parents:
        raise ProposalError("Refusing to clean a path outside Director staging")
    shutil.rmtree(path, ignore_errors=True)
