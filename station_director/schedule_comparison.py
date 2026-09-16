"""Bounded baseline-versus-proposed schedule comparison for C3a2.

This module is private Director infrastructure.  It reads only disposable C2
databases and writes only private comparison spools beneath the owning stage.
"""

import hashlib
import json
import os
import sqlite3
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path

from station_director.preservation import readonly_database
from station_director.schedule_normalization import (
    MAX_RECORD_BYTES,
    NormalizationError,
    canonical_catalog_semantics,
    canonical_typed_value_bytes,
    _block_playback,
    _catalog_descriptor,
    _check_hidden_references,
    _columns,
    _parse_json,
    _quote,
)

_canonical = canonical_typed_value_bytes
_catalog_semantics = canonical_catalog_semantics


MAX_COMPARISON_BLOCKS = 1_000_000
MAX_COMPONENT_BLOCKS = 100_000
MAX_COMPARISON_SPOOL_BYTES = 512 * 1024 * 1024
MAX_COMPARISON_SAMPLES = 50
MAX_SAMPLE_VALUE_BYTES = 512
MAX_SUMMARY_BYTES = 256 * 1024
MAX_REQUESTED_EFFECT_BYTES = 128 * 1024
COMPARISON_SPOOL_RESERVE_BYTES = MAX_COMPARISON_SPOOL_BYTES


class ScheduleComparisonError(RuntimeError):
    pass


def _time_us(value, label):
    if not isinstance(value, str):
        raise ScheduleComparisonError(f"{label} must be SQLite text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ScheduleComparisonError(f"invalid {label}") from exc
    if parsed.tzinfo is not None:
        raise ScheduleComparisonError(f"{label} must be naive station-local time")
    # Integer arithmetic preserves microseconds exactly and is independent of
    # the host epoch, locale, and floating-point formatting.
    return ((parsed.toordinal() * 86400 + parsed.hour * 3600
             + parsed.minute * 60 + parsed.second) * 1_000_000
            + parsed.microsecond)


def _bounded_preview(raw):
    if len(raw) <= MAX_SAMPLE_VALUE_BYTES:
        return raw.decode("utf-8"), False
    return raw[:MAX_SAMPLE_VALUE_BYTES].decode("utf-8", "replace"), True


def _spool_size(connection, path, logical_bytes):
    connection.commit()
    physical = path.stat().st_size
    if logical_bytes > MAX_COMPARISON_SPOOL_BYTES or physical > MAX_COMPARISON_SPOOL_BYTES:
        raise ScheduleComparisonError("comparison spool limit exceeded")


def _physical_spool_check(connection):
    page_count = connection.execute("PRAGMA page_count").fetchone()[0]
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    if page_count * page_size > MAX_COMPARISON_SPOOL_BYTES:
        raise ScheduleComparisonError("comparison spool limit exceeded")
    path = connection.execute("PRAGMA database_list").fetchone()[2]
    if os.stat(path).st_size > MAX_COMPARISON_SPOOL_BYTES:
        raise ScheduleComparisonError("comparison spool limit exceeded")


def _new_spool(stage):
    directory = Path(stage) / "schedule-comparison"
    directory.mkdir(mode=0o700, exist_ok=False)
    path = directory / "comparison.sqlite"
    connection = None
    try:
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(descriptor)
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA cache_size=-8192")
        connection.executescript(
            "CREATE TABLE blocks(side INT NOT NULL,start_us INT NOT NULL,end_us INT NOT NULL,"
            "interval_key BLOB NOT NULL,semantic BLOB NOT NULL);"
            "CREATE INDEX block_order ON blocks(side,start_us,end_us,semantic);"
            "CREATE INDEX component_sweep ON blocks(start_us,end_us,side,semantic);"
            "CREATE TABLE catalog(side INT NOT NULL,id INTEGER NOT NULL,token TEXT NOT NULL,"
            "semantic BLOB NOT NULL,PRIMARY KEY(side,id),UNIQUE(side,token));"
            "CREATE TABLE component(side INT NOT NULL,interval_key BLOB NOT NULL,semantic BLOB NOT NULL);"
            "CREATE INDEX component_match ON component(interval_key,semantic,side);"
            "CREATE TABLE coverage_spans(side INT NOT NULL,kind TEXT NOT NULL,start_us INT NOT NULL,end_us INT NOT NULL);"
            "CREATE INDEX coverage_match ON coverage_spans(side,kind,start_us,end_us);"
        )
        return directory, path, connection
    except Exception:
        if connection is not None:
            connection.close()
        try: path.unlink()
        except FileNotFoundError: pass
        try: directory.rmdir()
        except FileNotFoundError: pass
        raise


def comparison_query_plans(connection):
    """Return exact plans whose ordering/grouping must remain index-backed."""
    statements = (
        ("block_sweep", "SELECT side,start_us,end_us,interval_key,semantic FROM blocks "
         "ORDER BY start_us,end_us,side,semantic", ()),
        ("coverage", "SELECT start_us,end_us FROM blocks WHERE side=? "
         "ORDER BY start_us,end_us", (0,)),
        ("coverage_spans", "SELECT kind,start_us,end_us FROM coverage_spans WHERE side=? "
         "ORDER BY kind,start_us,end_us", (0,)),
        ("interval_difference", "SELECT 1 FROM (SELECT interval_key,"
         "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END) b,"
         "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) p FROM component "
         "GROUP BY interval_key HAVING b!=p) LIMIT 1", ()),
        ("interval_groups", "SELECT interval_key,"
         "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END),"
         "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) FROM component "
         "GROUP BY interval_key ORDER BY interval_key", ()),
        ("semantic_equal", "SELECT COALESCE(SUM(CASE WHEN b<p THEN b ELSE p END),0) "
         "FROM (SELECT semantic,SUM(CASE WHEN side=0 THEN 1 ELSE 0 END) b,"
         "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) p FROM component "
         "WHERE interval_key=? GROUP BY semantic)", (b"",)),
        ("semantic_remaining", "SELECT semantic,"
         "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END)-"
         "MIN(SUM(CASE WHEN side=0 THEN 1 ELSE 0 END),"
         "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END)) FROM component "
         "WHERE interval_key=? GROUP BY semantic HAVING "
         "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END)>"
         "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) ORDER BY semantic", (b"",)),
    )
    return {
        name: [str(row[3]) for row in connection.execute(
            "EXPLAIN QUERY PLAN " + sql, parameters)]
        for name, sql, parameters in statements
    }


def source_query_plans(connection, station, proposal_boundary):
    """Return plans for the exact source scans used during comparison."""
    columns = _columns(connection, "liquid_blocks")
    if "id" not in columns:
        raise ScheduleComparisonError("liquid_blocks.id is absent")
    selected = ",".join(_quote(item) for item in columns)
    statements = (
        ("future_rows", f"SELECT {selected} FROM liquid_blocks WHERE station=?",
         (station,)),
        ("retained_rows", f"SELECT {selected} FROM liquid_blocks "
         "WHERE station=? AND start_time<? ORDER BY id", (station, proposal_boundary)),
    )
    return {
        name: [str(row[3]) for row in connection.execute(
            "EXPLAIN QUERY PLAN " + sql, parameters)]
        for name, sql, parameters in statements
    }


def _catalog_tokens(spool, connection, side):
    columns = _columns(connection, "catalog_entries")
    if "id" not in columns:
        raise ScheduleComparisonError("catalog_entries.id is absent")
    selected = ",".join(_quote(item) for item in columns)
    count = logical = 0
    for row in connection.execute(f"SELECT {selected} FROM catalog_entries"):
        count += 1
        if count > MAX_COMPARISON_BLOCKS:
            raise ScheduleComparisonError("catalog row limit exceeded")
        catalog_id = row[columns.index("id")]
        semantic = _canonical(_catalog_semantics(connection, columns, row))
        if len(semantic) > MAX_RECORD_BYTES:
            raise ScheduleComparisonError("catalog semantics exceed record limit")
        if side == 0:
            token = f"historical:{catalog_id}"
        else:
            historical = spool.execute(
                "SELECT semantic FROM catalog WHERE side=0 AND id=?", (catalog_id,)
            ).fetchone()
            if historical is not None:
                if bytes(historical[0]) != semantic:
                    raise ScheduleComparisonError("historical catalog semantics changed")
                token = f"historical:{catalog_id}"
            else:
                if _catalog_descriptor(dict(zip(columns, row))):
                    raise ScheduleComparisonError("new AutoBump catalog descriptor is unsupported")
                token = "provisional:" + hashlib.sha256(
                    b"FS42-C3A2-CATALOG\0" + semantic
                ).hexdigest()
        if isinstance(catalog_id, bool) or not isinstance(catalog_id, int):
            raise ScheduleComparisonError("ambiguous catalog identity")
        record_size = len(semantic) + len(token) + 32
        if logical + record_size > MAX_COMPARISON_SPOOL_BYTES:
            raise ScheduleComparisonError("comparison spool limit exceeded")
        try:
            spool.execute("INSERT INTO catalog VALUES(?,?,?,?)",
                          (side, catalog_id, token, semantic))
        except sqlite3.IntegrityError as exc:
            raise ScheduleComparisonError("ambiguous catalog identity") from exc
        logical += record_size
        _physical_spool_check(spool)
    return logical


def _block_semantics(connection, columns, row, spool, side, *, allow_descriptors=False):
    values = {}
    references, plan, unused_descriptor = _block_playback(
        dict(zip(columns, row)), allow_descriptors=allow_descriptors)
    for name, value in zip(columns, row):
        if isinstance(value, str) and len(value.encode("utf-8")) > MAX_RECORD_BYTES:
            raise ScheduleComparisonError(f"oversized liquid_blocks.{name}")
        if isinstance(value, (bytes, bytearray, memoryview)) and len(value) > MAX_RECORD_BYTES:
            raise ScheduleComparisonError(f"oversized liquid_blocks.{name}")
        if name == "id":
            # A generated block ID is excluded only here: equivalence is proved
            # by the complete remaining typed block and catalog semantics.
            continue
        if name == "content_json":
            resolved = []
            for catalog_id in references:
                found = spool.execute(
                    "SELECT token,semantic FROM catalog WHERE side=? AND id=?",
                    (side, catalog_id),
                ).fetchone()
                if found is None:
                    raise ScheduleComparisonError("unresolved catalog reference")
                if not allow_descriptors:
                    catalog_columns = ("path", "realpath", "tag", "duration", "content_type", "media_type")
                    entry = connection.execute(
                        "SELECT path,realpath,tag,duration,content_type,media_type FROM catalog_entries WHERE id=?",
                        (catalog_id,),
                    ).fetchone()
                    if entry is None or _catalog_descriptor(dict(zip(catalog_columns, entry))):
                        raise ScheduleComparisonError("generated AutoBump is unsupported")
                token, semantic = found[0], bytes(found[1])
                resolved.append({"token": token, "semantics": json.loads(semantic)})
            value = (None if not references else
                     resolved if value.lstrip().startswith("[") else resolved[0])
        elif name == "plan_json":
            value = plan
        elif name in ("break_info", "sequence_key") and value not in (None, ""):
            value = _parse_json(value, f"liquid_blocks.{name}", allow_empty=True)
            _check_hidden_references(value, f"liquid_blocks.{name}")
        values[name] = value
    encoded = _canonical(values)
    if len(encoded) > MAX_RECORD_BYTES:
        raise ScheduleComparisonError("block semantics exceed record limit")
    return encoded


def _load_side(spool, source, side, channel_name, seam_us, horizon_us):
    logical = _catalog_tokens(spool, source, side)
    columns = _columns(source, "liquid_blocks")
    for required in ("station", "start_time", "end_time"):
        if required not in columns:
            raise ScheduleComparisonError(f"liquid_blocks.{required} is absent")
    selected = ",".join(_quote(item) for item in columns)
    total = crossing_start = crossing_end = 0
    cursor = source.execute(
        f"SELECT {selected} FROM liquid_blocks WHERE station=?",
        (channel_name,),
    )
    for row in cursor:
        start = _time_us(row[columns.index("start_time")], "block start")
        end = _time_us(row[columns.index("end_time")], "block end")
        if end <= start:
            raise ScheduleComparisonError("schedule block has non-positive duration")
        if start >= horizon_us or end <= seam_us:
            continue
        semantic = _block_semantics(source, columns, row, spool, side,
                                    allow_descriptors=side == 0 or start < seam_us)
        interval = _canonical([start, end])
        record_size = len(semantic) + len(interval) + 32
        if logical + record_size > MAX_COMPARISON_SPOOL_BYTES:
            raise ScheduleComparisonError("comparison spool limit exceeded")
        spool.execute("INSERT INTO blocks VALUES(?,?,?,?,?)", (side, start, end, interval, semantic))
        logical += record_size
        total += 1
        crossing_start += start < seam_us < end
        crossing_end += start < horizon_us < end
        if total > MAX_COMPARISON_BLOCKS:
            raise ScheduleComparisonError("comparison block limit exceeded")
        _physical_spool_check(spool)
    return total, crossing_start, crossing_end, logical


def _coverage(spool, side, seam, horizon):
    covered = gaps = overlaps = gap_us = overlap_us = 0
    cursor_at = seam
    previous_end = seam
    first = True
    for start, end in spool.execute(
        "SELECT start_us,end_us FROM blocks WHERE side=? ORDER BY start_us,end_us", (side,)
    ):
        start = max(start, seam)
        end = min(end, horizon)
        if end <= start:
            continue
        if first and start > seam:
            gaps += 1; gap_us += start - seam
            spool.execute("INSERT INTO coverage_spans VALUES(?,?,?,?)", (side, "gap", seam, start))
            _physical_spool_check(spool)
        elif not first and start > previous_end:
            gaps += 1; gap_us += start - previous_end
            spool.execute("INSERT INTO coverage_spans VALUES(?,?,?,?)", (side, "gap", previous_end, start))
            _physical_spool_check(spool)
        if not first and start < previous_end:
            overlaps += 1; overlap_us += min(previous_end, end) - start
            spool.execute("INSERT INTO coverage_spans VALUES(?,?,?,?)",
                          (side, "overlap", start, min(previous_end, end)))
            _physical_spool_check(spool)
        newly = max(0, end - max(start, cursor_at))
        covered += newly
        cursor_at = max(cursor_at, end)
        previous_end = max(previous_end, end)
        first = False
    if cursor_at < horizon:
        gaps += 1; gap_us += horizon - cursor_at
        spool.execute("INSERT INTO coverage_spans VALUES(?,?,?,?)", (side, "gap", cursor_at, horizon))
        _physical_spool_check(spool)
    digest = hashlib.sha256()
    for kind, start, end in spool.execute(
        "SELECT kind,start_us,end_us FROM coverage_spans WHERE side=? ORDER BY kind,start_us,end_us",
        (side,),
    ):
        digest.update(_canonical([kind, start, end]))
    return {
        "covered_us": covered, "gap_count": gaps, "gap_us": gap_us,
        "overlap_count": overlaps, "overlap_us": overlap_us,
        "interval_us": horizon - seam, "finding_digest": digest.hexdigest(),
    }


def _append_sample(summary, category, value):
    samples = summary["samples"][category]
    if len(samples) >= MAX_COMPARISON_SAMPLES:
        summary["samples_truncated"] = True
        return
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    preview, truncated = _bounded_preview(raw)
    samples.append({"value": preview, "value_truncated": truncated})


def _classify_components(spool, summary):
    left = iter(spool.execute(
        "SELECT side,start_us,end_us,interval_key,semantic FROM blocks "
        "ORDER BY start_us,end_us,side,semantic"
    ))
    pending = next(left, None)
    while pending is not None:
        component_start = pending[1]
        component_end = pending[2]
        component_rows = 0
        spool.execute("DELETE FROM component")
        while pending is not None and pending[1] < component_end:
            side, start, end, interval, semantic = pending
            spool.execute("INSERT INTO component VALUES(?,?,?)", (side, interval, semantic))
            _physical_spool_check(spool)
            component_rows += 1
            if component_rows > MAX_COMPONENT_BLOCKS:
                raise ScheduleComparisonError("comparison component limit exceeded")
            component_end = max(component_end, end)
            pending = next(left, None)
        summary["counts"]["components"] += 1
        baseline_count = spool.execute("SELECT COUNT(*) FROM component WHERE side=0").fetchone()[0]
        proposed_count = spool.execute("SELECT COUNT(*) FROM component WHERE side=1").fetchone()[0]
        if not proposed_count:
            summary["counts"]["removed_blocks"] += baseline_count
            continue
        if not baseline_count:
            summary["counts"]["generated_blocks"] += proposed_count
            continue
        interval_difference = spool.execute(
            "SELECT 1 FROM (SELECT interval_key,"
            "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END) b,"
            "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) p "
            "FROM component GROUP BY interval_key HAVING b!=p) LIMIT 1"
        ).fetchone()
        if interval_difference:
            summary["counts"]["reshaped_components"] += 1
            summary["counts"]["reshaped_baseline_blocks"] += baseline_count
            summary["counts"]["reshaped_proposed_blocks"] += proposed_count
            _append_sample(summary, "reshaped", {
                "component_start_us": component_start, "component_end_us": component_end,
                "baseline_blocks": baseline_count, "proposed_blocks": proposed_count
            })
            continue
        unchanged = replaced = 0
        for interval, b_count, p_count in spool.execute(
            "SELECT interval_key,"
            "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) "
            "FROM component GROUP BY interval_key ORDER BY interval_key"
        ):
            if b_count != p_count:
                raise ScheduleComparisonError("interval multiset accounting failed")
            equal = spool.execute(
                "SELECT COALESCE(SUM(CASE WHEN b<p THEN b ELSE p END),0) FROM ("
                "SELECT semantic,"
                "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END) b,"
                "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) p "
                "FROM component WHERE interval_key=? GROUP BY semantic)", (interval,)
            ).fetchone()[0]
            unchanged += equal
            replaced += b_count - equal
            baseline_remaining = spool.execute(
                "SELECT semantic,"
                "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END)-"
                "MIN(SUM(CASE WHEN side=0 THEN 1 ELSE 0 END),SUM(CASE WHEN side=1 THEN 1 ELSE 0 END)) "
                "FROM component WHERE interval_key=? GROUP BY semantic HAVING "
                "SUM(CASE WHEN side=0 THEN 1 ELSE 0 END)>SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) "
                "ORDER BY semantic", (interval,),
            )
            proposed_remaining = spool.execute(
                "SELECT semantic,"
                "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END)-"
                "MIN(SUM(CASE WHEN side=0 THEN 1 ELSE 0 END),SUM(CASE WHEN side=1 THEN 1 ELSE 0 END)) "
                "FROM component WHERE interval_key=? GROUP BY semantic HAVING "
                "SUM(CASE WHEN side=1 THEN 1 ELSE 0 END)>SUM(CASE WHEN side=0 THEN 1 ELSE 0 END) "
                "ORDER BY semantic", (interval,),
            )
            def expanded(cursor):
                for raw, count in cursor:
                    for unused in range(count):
                        yield bytes(raw)
            for baseline_semantic, proposed_semantic in zip(
                expanded(baseline_remaining), expanded(proposed_remaining)
            ):
                _record_semantic_change(
                    summary, interval, baseline_semantic, proposed_semantic
                )
        summary["counts"]["unchanged_blocks"] += unchanged
        summary["counts"]["replaced_pairs"] += replaced
        if unchanged + replaced != baseline_count or unchanged + replaced != proposed_count:
            raise ScheduleComparisonError("exact-interval block accounting failed")
        if replaced:
            _append_sample(summary, "replaced", {"pairs": replaced})


def _typed_object(raw):
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, list) or len(value) != 2 or value[0] != "object":
        raise ScheduleComparisonError("unsupported canonical block semantics")
    return dict(value[1])


def _record_semantic_change(summary, interval_raw, baseline_raw, proposed_raw):
    baseline = _typed_object(baseline_raw)
    proposed = _typed_object(proposed_raw)
    mapping = {
        "title": "title_changes", "content_json": "selected_media_changes",
        "plan_json": "playback_plan_changes", "liquid_type": "block_type_changes",
        "sequence_key": "sequence_changes", "break_info": "break_changes",
    }
    changed_fields = []
    for field, category in mapping.items():
        if baseline.get(field) != proposed.get(field):
            summary["counts"][category] += 1
            changed_fields.append(field)
    left, left_truncated = _bounded_preview(baseline_raw)
    right, right_truncated = _bounded_preview(proposed_raw)
    _append_sample(summary, "replaced", {
        "interval": json.loads(bytes(interval_raw).decode("utf-8")),
        "changed_fields": changed_fields,
        "baseline": left, "proposed": right,
        "semantic_truncated": left_truncated or right_truncated,
    })


def _retained_count(baseline, proposed, station, proposal_boundary, seam):
    columns = _columns(baseline, "liquid_blocks")
    other = _columns(proposed, "liquid_blocks")
    if columns != other:
        raise ScheduleComparisonError("liquid_blocks schema differs")
    if "id" not in columns:
        raise ScheduleComparisonError("liquid_blocks.id is absent")
    selected = ",".join(_quote(item) for item in columns)
    baseline_rows = baseline.execute(
        f"SELECT {selected} FROM liquid_blocks WHERE station=? AND start_time<? ORDER BY id",
        (station, proposal_boundary),
    )
    proposed_rows = proposed.execute(
        f"SELECT {selected} FROM liquid_blocks WHERE station=? AND start_time<? ORDER BY id",
        (station, proposal_boundary),
    )
    end_index = columns.index("end_time")
    seam_us = _time_us(seam, "regeneration_start")
    count = 0
    while True:
        row = baseline_rows.fetchone()
        other_row = proposed_rows.fetchone()
        if row is None or other_row is None:
            if row != other_row:
                raise ScheduleComparisonError("retained pre-seam history changed")
            break
        count += 1
        if count > MAX_COMPARISON_BLOCKS:
            raise ScheduleComparisonError("retained history count limit exceeded")
        if row != other_row:
            raise ScheduleComparisonError("retained pre-seam history changed")
        if _time_us(row[end_index], "retained block end") > seam_us:
            raise ScheduleComparisonError("retained block anomalously overlaps regeneration seam")
    return count


def canonical_requested_configuration_effects(proposal):
    """Return only direct, declared proposal operations in canonical order."""
    effects = []
    aggregate = 0
    for kind, key in (("assignment", "assignment_changes"),
                      ("directive", "directives"), ("exclusion", "exclusions")):
        for value in proposal.get(key, []):
            operation = _canonical(value).decode("utf-8")
            aggregate += len(operation.encode("utf-8"))
            if aggregate > MAX_REQUESTED_EFFECT_BYTES:
                raise ScheduleComparisonError("requested-effect byte limit exceeded")
            effects.append({
                "kind": kind,
                "canonical_operation": operation,
            })
    effects.sort(key=_canonical)
    if len(effects) > 10_000:
        raise ScheduleComparisonError("requested-effect limit exceeded")
    return effects


def compare_baseline_to_proposed(stage, baseline_database, proposed_database,
                                 channels, proposal_boundary, proposal):
    """Compare one proposed run to its captured baseline using a disk spool."""
    directory, spool_path, spool = _new_spool(stage)
    try:
        summaries = []
        with ExitStack() as stack:
            baseline = stack.enter_context(readonly_database(baseline_database))
            proposed = stack.enter_context(readonly_database(proposed_database))
            for connection in (baseline, proposed):
                connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_RECORD_BYTES)
            for channel in sorted(channels, key=lambda item: item["number"]):
                seam = _time_us(channel["regeneration_start"], "regeneration_start")
                horizon = _time_us(channel["effective_horizon"], "effective_horizon")
                if seam >= horizon:
                    raise ScheduleComparisonError("invalid comparison interval")
                spool.execute("DELETE FROM blocks")
                spool.execute("DELETE FROM coverage_spans")
                spool.execute("DELETE FROM catalog")
                b_count, b_cross_start, b_cross_end, b_bytes = _load_side(
                    spool, baseline, 0, channel["name"], seam, horizon
                )
                p_count, p_cross_start, p_cross_end, p_bytes = _load_side(
                    spool, proposed, 1, channel["name"], seam, horizon
                )
                _spool_size(spool, spool_path, b_bytes + p_bytes)
                summary = {
                    "number": channel["number"], "name": channel["name"],
                    "channel_seed": channel["channel_seed"],
                    "regeneration_start": channel["regeneration_start"],
                    "effective_horizon": channel["effective_horizon"],
                    "retained_pre_seam_blocks": _retained_count(
                        baseline, proposed, channel["name"], proposal_boundary,
                        channel["regeneration_start"],
                    ),
                    "boundary_crossing": {
                        "baseline_start": b_cross_start, "proposed_start": p_cross_start,
                        "baseline_horizon": b_cross_end, "proposed_horizon": p_cross_end,
                    },
                    "counts": {
                        "baseline_blocks": b_count, "proposed_blocks": p_count,
                        "components": 0, "unchanged_blocks": 0, "replaced_pairs": 0,
                        "removed_blocks": 0, "generated_blocks": 0,
                        "reshaped_components": 0, "reshaped_baseline_blocks": 0,
                        "reshaped_proposed_blocks": 0,
                        "title_changes": 0, "selected_media_changes": 0,
                        "playback_plan_changes": 0, "block_type_changes": 0,
                        "sequence_changes": 0, "break_changes": 0,
                    },
                    "coverage": {
                        "baseline": _coverage(spool, 0, seam, horizon),
                        "proposed": _coverage(spool, 1, seam, horizon),
                    },
                    "samples": {"replaced": [], "reshaped": []},
                    "samples_truncated": False,
                }
                _classify_components(spool, summary)
                counts = summary["counts"]
                if (counts["unchanged_blocks"] + counts["replaced_pairs"]
                        + counts["removed_blocks"] + counts["reshaped_baseline_blocks"]
                        != counts["baseline_blocks"]):
                    raise ScheduleComparisonError("baseline block accounting failed")
                if (counts["unchanged_blocks"] + counts["replaced_pairs"]
                        + counts["generated_blocks"] + counts["reshaped_proposed_blocks"]
                        != counts["proposed_blocks"]):
                    raise ScheduleComparisonError("proposed block accounting failed")
                baseline_coverage = summary["coverage"]["baseline"]
                proposed_coverage = summary["coverage"]["proposed"]
                summary["baseline_findings"] = []
                summary["errors"] = []
                for finding in ("gap", "overlap"):
                    count_key = finding + "_count"
                    if baseline_coverage[count_key]:
                        summary["baseline_findings"].append({
                            "code": "baseline_" + finding,
                            "count": baseline_coverage[count_key],
                        })
                    created = spool.execute(
                        "SELECT COUNT(*) FROM coverage_spans AS p WHERE p.side=1 AND p.kind=? "
                        "AND NOT EXISTS (SELECT 1 FROM coverage_spans AS b WHERE b.side=0 "
                        "AND b.kind=p.kind AND b.start_us=p.start_us AND b.end_us=p.end_us)",
                        (finding,),
                    ).fetchone()[0]
                    if created:
                        summary["errors"].append({
                            "code": "proposal_created_" + finding,
                            "count": created,
                        })
                digest_value = dict(summary)
                summary["digest"] = hashlib.sha256(_canonical(digest_value)).hexdigest()
                if len(_canonical(summary)) > MAX_SUMMARY_BYTES:
                    raise ScheduleComparisonError("channel comparison summary exceeds limit")
                summaries.append(summary)
        result = {
            "status": "pass",
            "channels": summaries,
            "requested_configuration_effects": canonical_requested_configuration_effects(proposal),
            "resulting_schedule_changes": {
                "changed_channels": sum(
                    bool(item["counts"]["replaced_pairs"]
                         or item["counts"]["removed_blocks"]
                         or item["counts"]["generated_blocks"]
                         or item["counts"]["reshaped_components"])
                    for item in summaries
                ),
                **{
                    key: sum(item["counts"][key] for item in summaries)
                    for key in (
                        "unchanged_blocks", "replaced_pairs", "removed_blocks",
                        "generated_blocks", "reshaped_components", "title_changes",
                        "selected_media_changes", "playback_plan_changes",
                        "block_type_changes", "sequence_changes", "break_changes",
                    )
                },
            },
            "unexpected_differences": [
                {"channel": item["number"], **error}
                for item in summaries for error in item["errors"]
            ],
        }
        result["digest"] = hashlib.sha256(_canonical(result)).hexdigest()
        if len(_canonical(result)) > MAX_SUMMARY_BYTES:
            raise ScheduleComparisonError("comparison summary exceeds limit")
        return result
    except (NormalizationError, sqlite3.Error) as exc:
        raise ScheduleComparisonError(str(exc)) from exc
    finally:
        spool.close()
        try:
            spool_path.unlink()
            directory.rmdir()
        except FileNotFoundError:
            pass


def compare_baseline_summaries(first, second):
    left = _canonical(first)
    right = _canonical(second)
    return {
        "passed": left == right,
        "run_1_digest": hashlib.sha256(left).hexdigest(),
        "run_2_digest": hashlib.sha256(right).hexdigest(),
    }
