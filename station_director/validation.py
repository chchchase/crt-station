import copy
import hashlib
import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from station_director.inventory import load_snapshots, validate_media_mount
from station_director.isolation import (
    IsolationLauncher,
    LaunchResult,
    check_invocation_context,
    cleanup_staging_directory,
    cleanup_stale_directories,
    cleanup_unit,
    create_staging_directory,
    new_run_id,
    sandbox_python,
    validate_probe_payload,
)
from station_director.preservation import (
    PreservationError,
    capture_media_manifest,
    compare_database_fingerprints,
    compare_json_fingerprints,
    compare_media_manifests,
    fingerprint_and_clone_database,
    fingerprint_database,
    fingerprint_json_files,
    protected_json_paths,
)
from station_director.proposals import (
    ProposalError,
    inventory_identifier_errors,
    stale_sources,
)
from station_director.recommend import configured_tags
from station_director.validation_context import (
    canonical_seed_inputs,
    derive_validation_context,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
    proposal_boundary_to_db,
)


DAYPARTS = {"morning": range(6, 10), "daytime": range(10, 18), "prime": range(18, 23), "late": [23, 0, 1, 2], "overnight": range(2, 6)}
PHASE_3_DISABLED = "Phase 3 validation is not yet enabled"
VALIDATION_REQUEST = "validation-request.json"
VALIDATION_RESULT = "validation-result.json"
VALIDATION_TIMEOUT_SECONDS = 120
MAX_WORKER_RESULT_BYTES = 2 * 1024 * 1024
LIVE_DATABASE = "runtime/fs42_fluid.db"
LIVE_MEDIA_ROOT = "/mnt/t7/CRT-Media"


def _channel_maps(policy):
    return ({c["number"]: c["name"] for c in policy["channels"]}, {c["name"]: c["number"] for c in policy["channels"]})


def _date_key(value):
    parsed = date.fromisoformat(value)
    return parsed.strftime("%B %d").replace(" 0", " ")


def _db_time(value):
    """Convert an RFC 3339 proposal boundary to FieldStation's naive local form."""
    return proposal_boundary_to_db(value)


def _remove_series(value, series):
    """Remove a series from all tag declarations, leaving slots to be smoothed."""
    if isinstance(value, dict):
        for key in list(value):
            child = value[key]
            if key == "tags":
                if isinstance(child, str) and child.casefold() == series.casefold():
                    del value[key]
                elif isinstance(child, list):
                    remaining = [tag for tag in child if not (isinstance(tag, str) and tag.casefold() == series.casefold())]
                    if remaining:
                        value[key] = remaining
                    else:
                        del value[key]
            else:
                _remove_series(child, series)
    elif isinstance(value, list):
        for child in value:
            _remove_series(child, series)


def apply_assignment_changes(configs, proposal, policy):
    """Remove transferred/removed series from their declared losing channel."""
    by_number, unused = _channel_maps(policy)
    affected = set()
    for change in proposal["assignment_changes"]:
        source_number = change.get("from_channel")
        if source_number in (None, 1, 8):
            continue
        source_name = by_number[source_number]
        _remove_series(configs[source_name]["station_conf"], change["series"])
        affected.add(source_name)
    return affected


def apply_exclusions(configs, proposal, policy):
    """Remove excluded inventory series from ordinary channels in the projection."""
    by_number, unused = _channel_maps(policy)
    affected = set()
    for series in proposal["exclusions"]:
        for number in range(2, 8):
            name = by_number[number]
            if name not in configs:
                continue
            before = json.dumps(configs[name]["station_conf"], sort_keys=True)
            _remove_series(configs[name]["station_conf"], series)
            after = json.dumps(configs[name]["station_conf"], sort_keys=True)
            if before != after:
                affected.add(name)
    return affected


def _put_slot(container, hour, slot, description):
    key = str(hour)
    existing = container.get(key)
    if existing is not None and existing != slot:
        raise ProposalError(f"Directive conflicts with existing programming at {description}")
    container[key] = slot


def apply_directives(configs, proposal, policy):
    by_number, unused = _channel_maps(policy)
    affected = set()
    for directive in proposal["directives"]:
        number = directive["channel"]
        if number in (1, 8):
            continue
        name = by_number[number]
        conf = configs[name]["station_conf"]
        affected.add(name)
        dtype = directive["type"]
        series = directive["series"]
        if dtype in ("date_slot", "marathon"):
            key = _date_key(directive["date"])
            slot = {"tags": series}
            if dtype == "marathon":
                slot["marathon"] = {"count": directive["count"], "chance": 1.0}
            slots = conf.setdefault("date_overrides", {}).setdefault(key, {})
            if not isinstance(slots, dict):
                raise ProposalError(f"Cannot merge directive into template override {key} on {name}")
            _put_slot(slots, directive["hour"], slot, f"{name} {directive['date']} hour {directive['hour']}")
        elif dtype == "daypart":
            start = datetime.fromisoformat(proposal["week_start"])
            for offset in range(7):
                value = (start + timedelta(days=offset)).date().isoformat()
                key = _date_key(value)
                slots = conf.setdefault("date_overrides", {}).setdefault(key, {})
                if not isinstance(slots, dict):
                    raise ProposalError(f"Cannot merge directive into template override {key} on {name}")
                for hour in DAYPARTS[directive["daypart"]]:
                    _put_slot(slots, hour, {"tags": series}, f"{name} {value} hour {hour}")
        else:
            start = date.fromisoformat(directive["start_date"])
            end = date.fromisoformat(directive["end_date"])
            hours = range(24) if directive.get("all_day") is True else directive["hours"]
            current = start
            while current <= end:
                value = current.isoformat()
                key = _date_key(value)
                slots = conf.setdefault("date_overrides", {}).setdefault(key, {})
                if not isinstance(slots, dict):
                    raise ProposalError(f"Cannot merge directive into template override {key} on {name}")
                for hour in hours:
                    _put_slot(slots, hour, {"tags": series}, f"{name} {value} hour {hour}")
                current += timedelta(days=1)
    return affected


def project_configuration(configs, proposal, policy):
    """Apply all declarative proposal effects to an isolated in-memory model."""
    projected = copy.deepcopy(configs)
    source_channels = apply_assignment_changes(projected, proposal, policy)
    source_channels.update(apply_exclusions(projected, proposal, policy))
    affected = set(source_channels)
    affected.update(apply_directives(projected, proposal, policy))
    return projected, affected, source_channels


def semantic_checks(proposal, policy, inventory, configs):
    failures, warnings = [], []
    by_number, by_name = _channel_maps(policy)
    failures.extend(inventory_identifier_errors(proposal, inventory))
    wio = {series.casefold() for series in policy.get("watch_in_order_series", [])}
    assignments, config_errors = configured_tags_from_data(configs)
    failures.extend(config_errors)
    for channel in policy["channels"]:
        data = configs.get(channel["name"])
        if not data or data.get("station_conf", {}).get("channel_number") != channel["number"]:
            failures.append(f"Canonical channel identity mismatch: {channel['number']} {channel['name']}")
    active_names = set(by_name)
    for series, owners in assignments.items():
        active = sorted(owner for owner in owners if owner in active_names)
        if len(active) > 1:
            failures.append(f"Exclusive ownership violation for {series}: {', '.join(active)}")
    intended = {}
    directives_by_series = {}
    for directive in proposal["directives"]:
        directives_by_series.setdefault(directive["series"].casefold(), set()).add(directive["channel"])
    seen_changes = set()
    for change in proposal["assignment_changes"]:
        series = change["series"]
        series_key = series.casefold()
        if series_key in seen_changes:
            failures.append(f"Multiple assignment changes target the same series: {series}")
        seen_changes.add(series_key)
        if series_key in wio or change.get("from_channel") == 8 or change.get("to_channel") == 8:
            failures.append(f"Watch In Order conflict for {series}; use a future dedicated WIO operation")
        current = sorted(by_name.get(name) for name in assignments.get(series_key, set()) if name in by_name)
        expected_from = change.get("from_channel")
        if expected_from and expected_from not in current:
            failures.append(f"{series} is not currently assigned to Channel {expected_from}")
        if change.get("to_channel") in (1, 8):
            failures.append(f"Normal proposals cannot assign series to Channel {change['to_channel']}")
        if change["action"] == "assign" and current:
            failures.append(f"{series} already belongs to Channel(s) {current}; use a move proposal")
        destination = change.get("to_channel")
        if destination is not None and destination not in directives_by_series.get(series_key, set()):
            failures.append(f"{series} assignment to Channel {destination} requires a scheduling directive on that channel")
        if change["action"] == "remove" and series_key in directives_by_series:
            failures.append(f"Removed series is also scheduled by a directive: {series}")
        intended[series_key] = destination
        warnings.append(f"Transfer report: {series}: lose {expected_from or 'none'}, gain {change.get('to_channel') or 'none'}")
    for directive in proposal["directives"]:
        series = directive["series"]
        series_key = series.casefold()
        if directive["channel"] in (1, 8) or series_key in wio:
            failures.append(f"Protected Watch In Order/guide programming conflict: {directive['series']}")
        current = sorted(by_name.get(name) for name in assignments.get(series_key, set()) if name in by_name)
        permitted = intended.get(series_key, current[0] if len(current) == 1 else None)
        if series_key not in intended and not current:
            failures.append(f"Unassigned series requires an assign action before scheduling: {series}")
        if permitted is not None and directive["channel"] != permitted:
            failures.append(f"Exclusive ownership conflict: {directive['series']} belongs to Channel {permitted}, not Channel {directive['channel']}")
    excluded = set()
    for series in proposal["exclusions"]:
        series_key = series.casefold()
        if series_key in excluded:
            failures.append(f"Duplicate exclusion differs only by case: {series}")
        if series_key in wio:
            failures.append(f"Watch In Order series cannot be excluded: {series}")
        excluded.add(series_key)
    used = {change["series"].casefold() for change in proposal["assignment_changes"]}
    used.update(directive["series"].casefold() for directive in proposal["directives"])
    for conflict in sorted(excluded & used):
        failures.append(f"Excluded series is also scheduled or assigned: {conflict}")

    try:
        projected, unused_affected, source_channels = project_configuration(
            configs, proposal, policy
        )
        projected_assignments, projected_errors = configured_tags_from_data(projected)
        failures.extend(projected_errors)
        for series, owners in projected_assignments.items():
            active = sorted(owner for owner in owners if owner in active_names)
            if len(active) > 1:
                failures.append(
                    f"Projected exclusive ownership violation for {series}: {', '.join(active)}"
                )
        for series in excluded:
            ordinary_owners = sorted(
                owner
                for owner in projected_assignments.get(series, set())
                if by_name.get(owner) in range(2, 8)
            )
            if ordinary_owners:
                failures.append(
                    f"Excluded series remains in projected configuration: {series} "
                    f"on {', '.join(ordinary_owners)}"
                )
    except (KeyError, TypeError, ValueError, ProposalError) as exc:
        failures.append(f"Could not build projected configuration: {exc}")
    return sorted(set(failures)), warnings


def configured_tags_from_data(configs):
    assignments, errors = {}, []
    for name, data in configs.items():
        def visit(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key == "tags":
                        for tag in child if isinstance(child, list) else [child]:
                            if isinstance(tag, str): assignments.setdefault(tag.casefold(), set()).add(name)
                    else: visit(child)
            elif isinstance(value, list):
                for child in value: visit(child)
        visit(data.get("station_conf", {}))
    return assignments, errors


def analyze_database(db_path, proposal, policy):
    start = _db_time(proposal["week_start"])
    end = _db_time(proposal["week_end"])
    result = {}
    digest_rows = []
    uri = f"file:{Path(db_path).resolve()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as con:
        con.execute("PRAGMA query_only=ON")
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        for channel in policy["channels"]:
            if not channel.get("has_schedule"): continue
            rows = con.execute(
                "SELECT b.start_time,b.end_time,b.title,c.tag FROM liquid_blocks b "
                "LEFT JOIN catalog_entries c ON c.id=CAST(b.content_json AS INTEGER) "
                "WHERE b.station=? AND b.start_time<? AND b.end_time>? ORDER BY b.start_time",
                (channel["name"], end, start),
            ).fetchall()
            digest_rows.extend((channel["name"], *row) for row in rows)
            gaps, overlaps = [], []
            cursor = start
            for block_start, block_end, title, tag in rows:
                effective_start = max(block_start, start)
                effective_end = min(block_end, end)
                if effective_start > cursor: gaps.append([cursor, effective_start])
                if effective_start < cursor: overlaps.append([effective_start, cursor])
                if effective_end > cursor: cursor = effective_end
            boundary_end = end
            if cursor < boundary_end: gaps.append([cursor, boundary_end])
            series_blocks = {}
            for unused_start, unused_end, title, tag in rows:
                series = tag or title.split(" - ", 1)[0]
                series_blocks[series] = series_blocks.get(series, 0) + 1
            result[str(channel["number"])] = {"name": channel["name"], "block_count": len(rows), "first": rows[0][0] if rows else None, "last": rows[-1][1] if rows else None, "gaps": gaps, "overlaps": overlaps, "series_blocks": dict(sorted(series_blocks.items()))}
        invalid_durations = con.execute("SELECT COUNT(*) FROM catalog_entries WHERE duration<=0").fetchone()[0]
        active_names = [channel["name"] for channel in policy["channels"]]
        placeholders = ",".join("?" for unused in active_names)
        paths = con.execute(f"SELECT DISTINCT COALESCE(realpath,path) FROM catalog_entries WHERE station IN ({placeholders})", active_names).fetchall()
        media_errors = []
        for (raw_path,) in paths:
            path = Path(raw_path)
            if not path.is_absolute(): path = Path.cwd() / path
            if not path.is_file() or not os.access(path, os.R_OK): media_errors.append(str(raw_path))
    digest = hashlib.sha256(json.dumps(digest_rows, separators=(",", ":")).encode()).hexdigest()
    return {"integrity": integrity, "invalid_catalog_durations": invalid_durations, "media_errors": media_errors, "schedule_digest": digest, "channels": result}


def clone_database(source, target):
    return fingerprint_and_clone_database(source, target)


def _static_validation_checks(proposal, root, policy):
    failures, warnings = [], []
    stale = stale_sources(proposal, root)
    if stale:
        failures.append(f"Stale source hashes: {', '.join(stale)}")
    validate_media_mount("/mnt/t7/CRT-Media")
    snapshots = load_snapshots(root / "runtime/director/inventory")
    inventory = snapshots[-1][1]
    configs = {}
    for path in sorted((root / "confs").glob("*.json")):
        if path.name == "main_config.json":
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        configs[data["station_conf"]["network_name"]] = data
    static_failures, static_warnings = semantic_checks(
        proposal, policy, inventory, configs
    )
    failures.extend(static_failures)
    warnings.extend(static_warnings)
    return sorted(set(failures)), warnings


def _disabled_report(
    proposal,
    failures=None,
    warnings=None,
    isolation=None,
    path_validation=None,
    preservation=None,
    schedule_preparation=None,
    validation_context=None,
):
    return {
        "proposal_id": proposal.get("proposal_id"),
        "valid": False,
        "scheduler_invoked": False,
        "failures": [PHASE_3_DISABLED, *(failures or [])],
        "warnings": warnings or [],
        "comparison": None,
        "isolation": isolation,
        "path_validation": path_validation,
        "preservation": preservation,
        "schedule_preparation": schedule_preparation,
        "validation_context": validation_context,
    }


def _write_request(path, run_id, proposal, policy, seed_inputs, validation_context):
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "proposal": proposal,
        "policy": policy,
        "seed_inputs": seed_inputs,
        "validation_context": validation_context,
    }
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _load_worker_result(path, run_id, proposal_id, validation_context):
    path = Path(path)
    if path.stat().st_size > MAX_WORKER_RESULT_BYTES:
        raise ValueError("validation worker result exceeds the size limit")
    raw = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version",
        "run_id",
        "proposal_id",
        "status",
        "failure",
        "scheduler_invoked",
        "probe_attestation",
        "path_validation",
        "b2_preparation",
        "validation_context",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("validation worker result fields are incomplete or unexpected")
    if raw["schema_version"] != 1 or raw["run_id"] != run_id or raw["proposal_id"] != proposal_id:
        raise ValueError("validation worker result identity or schema mismatch")
    if raw["scheduler_invoked"] is not False:
        raise ValueError("B1 worker reported scheduling behavior")
    if raw["validation_context"] != validation_context:
        raise ValueError("validation worker context does not match its request")
    probe_results, probe_error = validate_probe_payload(raw["probe_attestation"], run_id)
    if probe_error or not all(item["passed"] for item in probe_results.values()):
        raise ValueError(f"validation worker probe attestation failed: {probe_error or 'probe failure'}")
    if raw["status"] != "disabled" or raw["failure"] != PHASE_3_DISABLED:
        raise ValueError("validation worker did not return the required disabled status")
    path_validation = raw["path_validation"]
    if not isinstance(path_validation, dict) or path_validation.get("passed") is not True:
        raise ValueError("validation worker path confinement failed")
    preparation = raw["b2_preparation"]
    expected_preparation = {"schema_tables", "channels", "scheduler_gate"}
    if not isinstance(preparation, dict) or set(preparation) != expected_preparation:
        raise ValueError("validation worker B2 preparation is malformed")
    if preparation["scheduler_gate"] != "disabled":
        raise ValueError("validation worker scheduler gate is not disabled")
    if not isinstance(preparation["schema_tables"], list) or not isinstance(
        preparation["channels"], list
    ):
        raise ValueError("validation worker B2 preparation lists are malformed")
    expected_channel = {
        "channel",
        "original_horizon",
        "proposal_boundary",
        "proposal_end",
        "effective_horizon",
        "regeneration_start",
        "retained_row_count",
        "protected_catalog_count",
        "boundary_crossing_ids",
    }
    if any(
        not isinstance(item, dict) or set(item) != expected_channel
        for item in preparation["channels"]
    ):
        raise ValueError("validation worker channel history summary is malformed")
    return raw, probe_results


def validate_proposal(proposal, root, policy):
    """Permanently disabled compatibility entry point.

    Public validation is owned exclusively by validation_coordinator.  This
    legacy object-based API intentionally performs no validation, file access,
    staging, or worker launch, regardless of the master public gate.
    """
    return _disabled_report(proposal)
