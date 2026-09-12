import copy
import hashlib
import json
import os
import random
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

from station_director.inventory import load_snapshots, validate_media_mount
from station_director.proposals import (
    ProposalError,
    cleanup_staging,
    inventory_identifier_errors,
    stale_sources,
)
from station_director.recommend import configured_tags


DAYPARTS = {"morning": range(6, 10), "daytime": range(10, 18), "prime": range(18, 23), "late": [23, 0, 1, 2], "overnight": range(2, 6)}


def _channel_maps(policy):
    return ({c["number"]: c["name"] for c in policy["channels"]}, {c["name"]: c["number"] for c in policy["channels"]})


def _date_key(value):
    parsed = date.fromisoformat(value)
    return parsed.strftime("%B %d").replace(" 0", " ")


def _db_time(value):
    """Convert an RFC 3339 proposal boundary to FieldStation's naive local form."""
    return datetime.fromisoformat(value).replace(tzinfo=None).isoformat(sep=" ")


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


def _slot_has_tags(slot):
    if not isinstance(slot, dict):
        return False
    tags = slot.get("tags")
    if isinstance(tags, str):
        return bool(tags)
    if isinstance(tags, list):
        return bool(tags) and all(isinstance(tag, str) and tag for tag in tags)
    return False


def _resolved_week_slots(configs, channel_names, proposal):
    from fs42.config_processor import ConfigProcessor
    from fs42.slot_reader import SlotReader

    start = datetime.fromisoformat(proposal["week_start"])
    resolved = {}
    errors = []
    for name in sorted(channel_names):
        data = configs.get(name)
        if not data:
            errors.append(f"Cannot resolve source slots for missing channel configuration: {name}")
            continue
        try:
            conf = ConfigProcessor.preprocess(copy.deepcopy(data["station_conf"]))
            conf = SlotReader.smooth_tags(conf)
            states = {}
            for offset in range(7):
                current_date = (start + timedelta(days=offset)).date()
                for hour in range(24):
                    when = datetime.combine(current_date, datetime.min.time()).replace(hour=hour)
                    slot, unused_slot_number = SlotReader.get_slot(conf, when)
                    states[f"{current_date.isoformat()}T{hour:02d}:00"] = _slot_has_tags(slot)
            resolved[name] = states
        except Exception as exc:
            errors.append(f"Could not resolve projected slots for {name}: {exc}")
    return resolved, errors


def _newly_unresolved_source_slots(original, projected, source_channels, proposal):
    if not source_channels:
        return [], []
    before, before_errors = _resolved_week_slots(original, source_channels, proposal)
    after, after_errors = _resolved_week_slots(projected, source_channels, proposal)
    failures = []
    for name in sorted(source_channels):
        lost = [
            timestamp
            for timestamp, was_tagged in before.get(name, {}).items()
            if was_tagged and not after.get(name, {}).get(timestamp, False)
        ]
        if lost:
            sample = ", ".join(lost[:8])
            remainder = f" and {len(lost) - 8} more" if len(lost) > 8 else ""
            failures.append(
                f"{name} has {len(lost)} newly unresolved or tagless source slot(s): "
                f"{sample}{remainder}"
            )
    return failures, before_errors + after_errors


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
        unresolved, resolution_errors = _newly_unresolved_source_slots(
            configs, projected, source_channels, proposal
        )
        failures.extend(unresolved)
        failures.extend(resolution_errors)
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
    with sqlite3.connect(f"file:{Path(source).resolve()}?mode=ro", uri=True) as src, sqlite3.connect(target) as dst:
        src.backup(dst)


def validate_proposal(proposal, root, policy):
    root = Path(root)
    failures, warnings = [], []
    stale = stale_sources(proposal, root)
    if stale:
        return {"proposal_id": proposal["proposal_id"], "valid": False, "failures": [f"Stale source hashes: {', '.join(stale)}"], "warnings": [], "comparison": None}
    validate_media_mount("/mnt/t7/CRT-Media")
    snapshots = load_snapshots(root / "runtime/director/inventory")
    inventory = snapshots[-1][1]
    configs = {}
    for path in sorted((root / "confs").glob("*.json")):
        if path.name == "main_config.json": continue
        data = json.loads(path.read_text()); configs[data["station_conf"]["network_name"]] = data
    static_failures, static_warnings = semantic_checks(proposal, policy, inventory, configs)
    failures.extend(static_failures); warnings.extend(static_warnings)
    try:
        import jsonschema
        from fs42.config_processor import ConfigProcessor
        from fs42.slot_reader import SlotReader
        station_schema = json.loads((root / "fs42/station_config_schema.json").read_text())
        for name, data in configs.items():
            jsonschema.validate(data, station_schema)
            processed = ConfigProcessor.preprocess(copy.deepcopy(data["station_conf"]))
            if processed.get("network_type", "standard") == "standard": SlotReader.smooth_tags(processed)
    except Exception as exc:
        failures.append(f"FieldStation42 configuration processing failed: {exc}")
    if failures:
        return {"proposal_id": proposal["proposal_id"], "valid": False, "failures": failures, "warnings": warnings, "comparison": None}
    stage = root / "runtime/director/staging" / proposal["proposal_id"]
    try:
        (stage / "confs").mkdir(parents=True, exist_ok=False); (stage / "runtime").mkdir(); (stage / "catalog").mkdir(); (stage / "fs42").mkdir()
        os.symlink("/mnt/t7/CRT-Media", stage / "catalog/crt_media", target_is_directory=True)
        shutil.copy2(root / "fs42/station_config_schema.json", stage / "fs42/station_config_schema.json")
        staged_configs = copy.deepcopy(configs)
        affected = apply_assignment_changes(staged_configs, proposal, policy)
        affected.update(apply_directives(staged_configs, proposal, policy))
        staged_assignments, assignment_errors = configured_tags_from_data(staged_configs)
        failures.extend(assignment_errors)
        active_names = {channel["name"] for channel in policy["channels"]}
        for series, owners in staged_assignments.items():
            active = sorted(name for name in owners if name in active_names)
            if len(active) > 1:
                failures.append(f"Exclusive ownership violation for {series}: {', '.join(active)}")
        if failures:
            return {"proposal_id": proposal["proposal_id"], "valid": False, "failures": sorted(set(failures)), "warnings": warnings, "comparison": None}
        for name, data in staged_configs.items():
            jsonschema.validate(data, station_schema)
            processed = ConfigProcessor.preprocess(copy.deepcopy(data["station_conf"]))
            if processed.get("network_type", "standard") == "standard":
                SlotReader.smooth_tags(processed)
            filename = next(p.name for p in (root / "confs").glob("*.json") if json.loads(p.read_text()).get("station_conf",{}).get("network_name") == name)
            (stage / "confs" / filename).write_text(json.dumps(data, indent=2) + "\n")
        main = json.loads((root / "confs/main_config.json").read_text()) if (root / "confs/main_config.json").exists() else {}
        main["db_path"] = "runtime/fs42_fluid.db"; (stage / "confs/main_config.json").write_text(json.dumps(main, indent=2)+"\n")
        clone_database(root / "runtime/fs42_fluid.db", stage / "runtime/fs42_fluid.db")
        before = analyze_database(root / "runtime/fs42_fluid.db", proposal, policy)
        if affected:
            instructions = {"seed": proposal["seed"], "affected": sorted(affected), "week_end": _db_time(proposal["week_end"]).replace(" ", "T")}
            (stage / "instructions.json").write_text(json.dumps(instructions))
            run = subprocess.run([str(root / "env/bin/python3"), str(root / "station_director/stage_runner.py"), str(stage / "instructions.json")], cwd=stage, capture_output=True, text=True, timeout=1800)
            if run.returncode: failures.append("Staged scheduler failed: " + (run.stderr.strip()[-2000:] or run.stdout.strip()[-2000:]))
        after = analyze_database(stage / "runtime/fs42_fluid.db", proposal, policy)
        for number, channel in after["channels"].items():
            if channel["gaps"]: failures.append(f"Channel {number} has {len(channel['gaps'])} coverage gap(s)")
            if channel["overlaps"]: failures.append(f"Channel {number} has {len(channel['overlaps'])} overlap(s)")
        if after["integrity"] != "ok": failures.append("Staged SQLite integrity check failed")
        if after["invalid_catalog_durations"]: failures.append("Catalog contains non-positive durations")
        if after["media_errors"]: failures.append(f"Staged catalog has {len(after['media_errors'])} missing or unreadable media path(s)")
        comparison = {}
        for number, data in after["channels"].items():
            current = before["channels"].get(number, {})
            current_series = current.get("series_blocks", {}); proposed_series = data["series_blocks"]
            series_delta = {series: proposed_series.get(series, 0)-current_series.get(series, 0) for series in sorted(set(current_series)|set(proposed_series)) if proposed_series.get(series, 0) != current_series.get(series, 0)}
            comparison[number] = {"name": data["name"], "current_blocks": current.get("block_count", 0), "proposed_blocks": data["block_count"], "block_delta": data["block_count"] - current.get("block_count", 0), "added_series": sorted(set(proposed_series)-set(current_series)), "removed_series": sorted(set(current_series)-set(proposed_series)), "series_block_delta": series_delta}
        staged_summary = copy.deepcopy(after)
        return {"proposal_id": proposal["proposal_id"], "valid": not failures, "failures": failures, "warnings": warnings, "week_start": proposal["week_start"], "week_end": proposal["week_end"], "seed": proposal["seed"], "schedule_digest": after["schedule_digest"], "staged": staged_summary, "comparison": comparison}
    finally:
        if stage.exists(): cleanup_staging(stage)
