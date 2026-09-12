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
SCHEMA_PATH = Path(__file__).parent / "schemas" / "proposal.v1.schema.json"
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
    normalized = dict(proposal)
    for key in ("week_start", "week_end"):
        value = normalized.get(key)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                normalized[key] = parsed.replace(tzinfo=STATION_TIMEZONE).isoformat()
    return normalized


def validate_schema(proposal):
    schema = json.loads(SCHEMA_PATH.read_text())
    validator = jsonschema.Draft7Validator(schema, format_checker=jsonschema.FormatChecker())
    errors = sorted(validator.iter_errors(proposal), key=lambda item: list(item.path))
    if errors:
        raise ProposalError("Proposal schema error: " + "; ".join(error.message for error in errors))


def create_proposal(target_week, source, seed, assignments, directives, exclusions, root=ROOT, policy_path=None):
    start, end = week_bounds(target_week)
    now = datetime.now(timezone.utc)
    proposal_id = f"p-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    proposal = {
        "schema_version": 1,
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
    proposal = normalize_legacy_boundaries(json.loads(path.read_text()))
    validate_schema(proposal)
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
