import json
from dataclasses import dataclass
from datetime import datetime

from station_director.path_safety import canonical_media_mapping, validate_scheduled_media
from station_director.preservation import canonical_sqlite_value, inspect_database_schema


REQUIRED_COLUMNS = {
    "liquid_blocks": (
        "id", "station", "liquid_type", "start_time", "end_time",
        "break_strategy", "title", "sequence_key", "break_info",
        "content_json", "plan_json",
    ),
    "catalog_entries": (
        "id", "station", "path", "title", "duration", "tag", "count",
        "hints", "created_at", "updated_at", "realpath", "content_type",
        "media_type",
    ),
    "named_sequence": (
        "id", "station", "sequence_name", "tag_path", "start_perc",
        "end_perc", "current_index", "initialized", "parent_tag",
    ),
    "sequence_entries": ("id", "fpath", "sequence_index", "named_sequence_id"),
    "sequence_group_state": (
        "station", "sequence_name", "parent_tag", "active_tag_path",
    ),
    "file_meta": (
        "path", "duration", "size", "first_added", "last_mod",
        "last_checked", "last_updated", "meta", "media_type",
    ),
    "break_points": ("path", "points", "last_updated"),
    "chapter_points": ("path", "points", "last_updated"),
}


class StagedScheduleError(RuntimeError):
    pass


def _quote(value):
    return '"' + str(value).replace('"', '""') + '"'


def _columns(connection, table):
    return [row[1] for row in connection.execute(f"PRAGMA table_xinfo({_quote(table)})")]


def inspect_required_schema(connection):
    schema = inspect_database_schema(connection)
    failures = []
    for table, required in REQUIRED_COLUMNS.items():
        actual = [column["name"] for column in schema["tables"].get(table, [])]
        if not actual:
            failures.append(f"missing table: {table}")
            continue
        missing = [name for name in required if name not in actual]
        if missing:
            failures.append(f"{table} missing columns: {', '.join(missing)}")
    if failures:
        raise StagedScheduleError("; ".join(failures))
    return schema


def _parse_time(value, description):
    if not isinstance(value, str):
        raise StagedScheduleError(f"{description} is not text")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise StagedScheduleError(f"{description} is not an ISO timestamp: {value}") from exc


def parse_catalog_references(raw):
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise StagedScheduleError(f"unsupported content_json: {exc}") from exc
    values = value if isinstance(value, list) else [value]
    if not values:
        raise StagedScheduleError("unsupported empty content_json list")
    if isinstance(value, bool) or not isinstance(value, (int, list)):
        raise StagedScheduleError(
            f"unsupported content_json shape: {type(value).__name__}"
        )
    if any(isinstance(item, bool) or not isinstance(item, int) or item <= 0 for item in values):
        raise StagedScheduleError("content_json references must be positive JSON integers")
    return tuple(values)


def validate_plan_json(raw):
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise StagedScheduleError(f"unsupported plan_json: {exc}") from exc
    if not isinstance(value, list):
        raise StagedScheduleError("plan_json must be an array")
    required = {"path", "skip", "duration", "is_stream", "content_type", "media_type"}
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != required:
            raise StagedScheduleError(f"unsupported plan_json entry at index {index}")
        if not isinstance(item["is_stream"], bool) or not isinstance(item["path"], str):
            raise StagedScheduleError(f"invalid plan_json path/stream fields at index {index}")
    return value


def _rows(connection, table, where="", parameters=()):
    columns = _columns(connection, table)
    selected = ",".join(_quote(column) for column in columns)
    sql = f"SELECT {selected} FROM {_quote(table)}"
    if where:
        sql += " WHERE " + where
    primary = [row[1] for row in connection.execute(f"PRAGMA table_info({_quote(table)})") if row[5]]
    ordering = primary or columns
    sql += " ORDER BY " + ",".join(_quote(column) for column in ordering)
    return columns, [tuple(row) for row in connection.execute(sql, parameters)]


def _row_dict(columns, row):
    return dict(zip(columns, row))


@dataclass
class ChannelHistory:
    channel: str
    original_horizon: str | None
    proposal_boundary: str
    proposal_end: str
    effective_horizon: str
    regeneration_start: str
    retained_rows: list
    protected_catalog_ids: set
    retained_catalog_rows: dict
    boundary_crossing_ids: list

    def summary(self):
        return {
            "channel": self.channel,
            "original_horizon": self.original_horizon,
            "proposal_boundary": self.proposal_boundary,
            "proposal_end": self.proposal_end,
            "effective_horizon": self.effective_horizon,
            "regeneration_start": self.regeneration_start,
            "retained_row_count": len(self.retained_rows),
            "protected_catalog_count": len(self.protected_catalog_ids),
            "boundary_crossing_ids": self.boundary_crossing_ids,
        }


def capture_channel_history(connection, channel, proposal_boundary, proposal_end):
    columns, retained = _rows(
        connection,
        "liquid_blocks",
        "station=? AND start_time<?",
        (channel, proposal_boundary),
    )
    by_name = [_row_dict(columns, row) for row in retained]
    horizon_values = [
        row[0]
        for row in connection.execute(
            "SELECT end_time FROM liquid_blocks WHERE station=?", (channel,)
        )
    ]
    original_horizon = (
        max(
            horizon_values,
            key=lambda value: _parse_time(value, "original schedule end"),
        )
        if horizon_values
        else None
    )
    effective = proposal_end
    if original_horizon and _parse_time(original_horizon, "original horizon") > _parse_time(proposal_end, "proposal end"):
        effective = original_horizon
    crossing = [
        row for row in by_name
        if _parse_time(row["end_time"], "block end") > _parse_time(proposal_boundary, "proposal boundary")
    ]
    regeneration_start = proposal_boundary
    if crossing:
        regeneration_start = max(crossing, key=lambda row: _parse_time(row["end_time"], "block end"))["end_time"]
    references = set()
    for row in by_name:
        references.update(parse_catalog_references(row["content_json"]))
        validate_plan_json(row["plan_json"])
    retained_catalog_rows = {}
    if references:
        placeholders = ",".join("?" for unused in references)
        catalog_columns, catalog_rows = _rows(
            connection,
            "catalog_entries",
            f"id IN ({placeholders})",
            tuple(sorted(references)),
        )
        retained_catalog_rows = {
            row[catalog_columns.index("id")]: row for row in catalog_rows
        }
        missing = sorted(references - set(retained_catalog_rows))
        if missing:
            raise StagedScheduleError(
                "retained schedule references missing catalog IDs: "
                + ", ".join(str(item) for item in missing)
            )
        station_index = catalog_columns.index("station")
        mismatched = sorted(
            catalog_id
            for catalog_id, row in retained_catalog_rows.items()
            if row[station_index] != channel
        )
        if mismatched:
            raise StagedScheduleError(
                f"retained schedule for {channel} references another channel's catalog IDs: "
                + ", ".join(str(item) for item in mismatched)
            )
    return ChannelHistory(
        channel,
        original_horizon,
        proposal_boundary,
        proposal_end,
        effective,
        regeneration_start,
        retained,
        references,
        retained_catalog_rows,
        [row["id"] for row in crossing],
    )


def capture_protected_state(connection, affected_channels, protected_channels=()):
    affected_channels = tuple(sorted(affected_channels))
    protected_channels = tuple(sorted(protected_channels))
    if affected_channels:
        unaffected_clause = "station NOT IN ({})".format(
            ",".join("?" for unused in affected_channels)
        )
        parameters = affected_channels
        if protected_channels:
            unaffected_clause = "(" + unaffected_clause + ") OR station IN ({})".format(
                ",".join("?" for unused in protected_channels)
            )
            parameters += protected_channels
    else:
        unaffected_clause = "1=1"
        parameters = ()
    state = {}
    for table in ("liquid_blocks", "catalog_entries"):
        columns, rows = _rows(connection, table, unaffected_clause, parameters)
        state[table] = {
            "columns": columns,
            "rows": rows,
            "where": unaffected_clause,
            "parameters": parameters,
        }
    for table in ("named_sequence", "sequence_entries", "sequence_group_state"):
        columns, rows = _rows(connection, table)
        state[table] = {
            "columns": columns,
            "rows": rows,
            "where": "",
            "parameters": (),
        }
    return state


def assert_protected_state(connection, state):
    def typed_rows(rows):
        return tuple(
            tuple(canonical_sqlite_value(value) for value in row) for row in rows
        )

    failures = []
    for table, snapshot in state.items():
        columns = snapshot["columns"]
        expected_rows = snapshot["rows"]
        actual_columns, actual_rows = _rows(
            connection,
            table,
            snapshot["where"],
            snapshot["parameters"],
        )
        if actual_columns != columns or typed_rows(actual_rows) != typed_rows(expected_rows):
            failures.append(f"protected {table} rows changed")
    if failures:
        raise StagedScheduleError("; ".join(failures))


def assert_retained_history(connection, history):
    columns, rows = _rows(
        connection,
        "liquid_blocks",
        "station=? AND start_time<?",
        (history.channel, history.proposal_boundary),
    )
    if columns != _columns(connection, "liquid_blocks") or rows != history.retained_rows:
        raise StagedScheduleError(f"pre-boundary history changed for {history.channel}")
    if history.protected_catalog_ids:
        placeholders = ",".join("?" for unused in history.protected_catalog_ids)
        unused_columns, rows = _rows(
            connection,
            "catalog_entries",
            f"id IN ({placeholders})",
            tuple(sorted(history.protected_catalog_ids)),
        )
        present = {
            row[0]: row for row in rows
        }
        missing = sorted(history.protected_catalog_ids - set(present))
        if missing:
            raise StagedScheduleError(
                f"protected catalog IDs disappeared for {history.channel}: {missing}"
            )
        changed = sorted(
            catalog_id
            for catalog_id, expected in history.retained_catalog_rows.items()
            if present[catalog_id] != expected
        )
        if changed:
            raise StagedScheduleError(
                f"protected catalog rows changed for {history.channel}: {changed}"
            )


def _catalog_identity(row):
    path = row.get("realpath") or row.get("path")
    mapping = canonical_media_mapping(path, "catalog path", allow_sandbox=True)
    return row["station"], row["tag"], mapping.logical_identity


def reconcile_catalog(connection, channel, generated_rows, protected_ids):
    columns, original_rows = _rows(connection, "catalog_entries", "station=?", (channel,))
    originals = [_row_dict(columns, row) for row in original_rows]
    by_identity = {}
    for row in originals:
        identity = _catalog_identity(row)
        if identity in by_identity:
            raise StagedScheduleError(f"ambiguous catalog identity for {identity}")
        by_identity[identity] = row
    protected = {row["id"]: row for row in originals if row["id"] in protected_ids}
    generated_by_identity = {}
    for supplied in generated_rows:
        row = dict(supplied)
        missing = [name for name in columns if name != "id" and name not in row]
        if missing:
            raise StagedScheduleError(f"generated catalog row missing columns: {missing}")
        row["station"] = channel
        mapping = canonical_media_mapping(
            row.get("realpath") or row["path"], "generated catalog path", allow_sandbox=True
        )
        row["path"] = mapping.sandbox_path
        row["realpath"] = mapping.sandbox_path
        identity = _catalog_identity(row)
        if identity in generated_by_identity:
            raise StagedScheduleError(f"duplicate generated catalog identity for {identity}")
        generated_by_identity[identity] = row

    connection.execute("DELETE FROM catalog_entries WHERE station=?", (channel,))
    active_ids = set()
    insert_columns = columns
    placeholders = ",".join("?" for unused in insert_columns)
    sql = (
        f"INSERT INTO catalog_entries ({','.join(_quote(name) for name in insert_columns)}) "
        f"VALUES ({placeholders})"
    )
    for catalog_id in sorted(protected):
        row = protected[catalog_id]
        connection.execute(sql, [row.get(name) for name in insert_columns])

    for identity in sorted(generated_by_identity):
        row = generated_by_identity[identity]
        original = by_identity.get(identity)
        original_is_protected = original and original["id"] in protected
        original_is_sandbox = original and str(
            original.get("realpath") or original["path"]
        ).startswith("/media/")
        if original_is_protected and original_is_sandbox:
            active_ids.add(original["id"])
            continue
        row["id"] = original["id"] if original and not original_is_protected else None
        if original:
            row["count"] = original["count"]
            row["created_at"] = original["created_at"]
        values = [row.get(name) for name in insert_columns]
        if row["id"] is None:
            auto_columns = [name for name in insert_columns if name != "id"]
            cursor = connection.execute(
                f"INSERT INTO catalog_entries ({','.join(_quote(name) for name in auto_columns)}) "
                f"VALUES ({','.join('?' for unused in auto_columns)})",
                [row.get(name) for name in auto_columns],
            )
            row["id"] = cursor.lastrowid
        else:
            connection.execute(sql, values)
        active_ids.add(row["id"])

    return active_ids


def coverage_report(connection, history):
    columns, rows = _rows(
        connection,
        "liquid_blocks",
        "station=? AND start_time<? AND end_time>?",
        (history.channel, history.effective_horizon, history.proposal_boundary),
    )
    records = [_row_dict(columns, row) for row in rows]
    records.sort(
        key=lambda row: (
            _parse_time(row["start_time"], "block start"),
            _parse_time(row["end_time"], "block end"),
            row["id"],
        )
    )
    boundary = _parse_time(history.proposal_boundary, "proposal boundary")
    proposal_end = _parse_time(history.proposal_end, "proposal end")
    horizon = _parse_time(history.effective_horizon, "effective horizon")
    cursor = boundary
    gaps, overlaps = [], []
    for row in records:
        start = max(_parse_time(row["start_time"], "block start"), boundary)
        end = min(_parse_time(row["end_time"], "block end"), horizon)
        if start > cursor:
            gaps.append((cursor.isoformat(sep=" "), start.isoformat(sep=" ")))
        elif start < cursor:
            overlaps.append((start.isoformat(sep=" "), cursor.isoformat(sep=" ")))
        if end > cursor:
            cursor = end
    if cursor < horizon:
        gaps.append((cursor.isoformat(sep=" "), horizon.isoformat(sep=" ")))

    def crossing(point):
        return [
            row["id"] for row in records
            if _parse_time(row["start_time"], "block start") < point
            < _parse_time(row["end_time"], "block end")
        ]

    report = {
        "channel": history.channel,
        "proposal_boundary_crossing_ids": crossing(boundary),
        "proposal_end_crossing_ids": crossing(proposal_end),
        "effective_horizon_crossing_ids": crossing(horizon),
        "gaps": gaps,
        "overlaps": overlaps,
        "final_end": max((row["end_time"] for row in records), default=None),
    }
    if gaps or overlaps or cursor < horizon:
        raise StagedScheduleError(
            f"coverage failure for {history.channel}: {len(gaps)} gap(s), {len(overlaps)} overlap(s)"
        )
    return report


def validate_scheduled_paths(connection, histories, sandbox_media_root):
    checked = 0
    for history in histories:
        rows = connection.execute(
            "SELECT id,content_json,plan_json FROM liquid_blocks "
            "WHERE station=? AND start_time<? ORDER BY start_time,id",
            (history.channel, history.effective_horizon),
        )
        for block_id, content_json, plan_json in rows:
            references = parse_catalog_references(content_json)
            placeholders = ",".join("?" for unused in references)
            catalog_rows = connection.execute(
                f"SELECT id,COALESCE(realpath,path) FROM catalog_entries "
                f"WHERE id IN ({placeholders})",
                references,
            ).fetchall()
            if len({row[0] for row in catalog_rows}) != len(set(references)):
                raise StagedScheduleError(
                    f"block {block_id} has unresolved catalog references"
                )
            for unused_catalog_id, path in catalog_rows:
                mapping = canonical_media_mapping(
                    path, "scheduled catalog path", allow_sandbox=True
                )
                validate_scheduled_media(
                    mapping.sandbox_path, sandbox_media_root=sandbox_media_root
                )
                checked += 1
            for item in validate_plan_json(plan_json):
                if item["is_stream"]:
                    raise StagedScheduleError(
                        f"block {block_id} contains stream content instead of confined media"
                    )
                mapping = canonical_media_mapping(
                    item["path"], "scheduled plan path", allow_sandbox=True
                )
                validate_scheduled_media(
                    mapping.sandbox_path, sandbox_media_root=sandbox_media_root
                )
                checked += 1
    return checked


def regenerate_with_callback(
    connection,
    affected_channels,
    proposal_boundary,
    proposal_end,
    generated_catalogs,
    scheduler_callback,
    *,
    protected_channels=("CRT Station Guide", "Watch In Order"),
    sandbox_media_root="/media",
):
    """Exercise B2 against a staged DB. Production keeps this behind a disabled gate."""
    affected = tuple(sorted(set(affected_channels)))
    forbidden = sorted(set(affected) & set(protected_channels))
    if forbidden:
        raise StagedScheduleError(
            "protected channels cannot be regenerated: " + ", ".join(forbidden)
        )
    if scheduler_callback is None:
        raise StagedScheduleError("Phase 3 validation is not yet enabled")
    inspect_required_schema(connection)
    histories = [
        capture_channel_history(connection, channel, proposal_boundary, proposal_end)
        for channel in affected
    ]
    protected_state = capture_protected_state(
        connection, affected, protected_channels=protected_channels
    )
    try:
        active_by_channel = {}
        for history in histories:
            connection.execute(
                "DELETE FROM liquid_blocks WHERE station=? AND start_time>=?",
                (history.channel, history.proposal_boundary),
            )
            active_by_channel[history.channel] = reconcile_catalog(
                connection,
                history.channel,
                generated_catalogs.get(history.channel, []),
                history.protected_catalog_ids,
            )
            scheduler_callback(
                connection,
                history.channel,
                history.regeneration_start,
                history.effective_horizon,
                active_by_channel[history.channel],
            )
        reports = []
        for history in histories:
            assert_retained_history(connection, history)
            report = history.summary()
            report["coverage"] = coverage_report(connection, history)
            reports.append(report)
        assert_protected_state(connection, protected_state)
        checked_paths = validate_scheduled_paths(
            connection, histories, sandbox_media_root
        )
        return {
            "channels": reports,
            "scheduled_path_checks": checked_paths,
        }
    except Exception:
        connection.rollback()
        raise
