import json
import hashlib
from dataclasses import dataclass
from datetime import datetime

from station_director.path_safety import canonical_media_mapping, validate_scheduled_media
from station_director.preservation import canonical_sqlite_value, inspect_database_schema
from station_director.c1_diagnostics import (
    attach_preservation_detail, preservation_check, preservation_step,
)


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


def _playback_catalog_references(raw, liquid_type):
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise StagedScheduleError(f"unsupported content_json: {exc}") from exc
    if value is None:
        if liquid_type == "LiquidWebBlock":
            return (), True
        raise StagedScheduleError("unsupported content_json shape: NoneType")
    return parse_catalog_references(raw), False


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


def validate_all_catalog_reference_shapes(connection):
    checked = 0
    for block_id, liquid_type, content_json in connection.execute(
        "SELECT id,liquid_type,content_json FROM liquid_blocks ORDER BY id"
    ):
        try:
            _playback_catalog_references(content_json, liquid_type)
        except StagedScheduleError as exc:
            raise StagedScheduleError(
                f"block {block_id} has an unsupported catalog reference: {exc}"
            ) from exc
        checked += 1
    return checked


def _playback_representations(connection, rows):
    for block_id, liquid_type, content_json, plan_json in rows:
        with preservation_step("_playback_representations", "plan_invalid"):
            plan = validate_plan_json(plan_json)
        with preservation_step("_playback_representations", "reference_failure"):
            references, content_missing = _playback_catalog_references(
                content_json, liquid_type
            )
        catalog = []
        if references:
            placeholders = ",".join("?" for unused in references)
            with preservation_step("_playback_representations", "reference_failure"):
                catalog = [
                    {
                        "id": row[0], "path": row[1], "realpath": row[2],
                        "tag": row[3], "duration": row[4],
                        "content_type": row[5], "media_type": row[6],
                    }
                    for row in connection.execute(
                        "SELECT id,path,realpath,tag,duration,content_type,media_type "
                        f"FROM catalog_entries WHERE id IN ({placeholders}) ORDER BY id",
                        references,
                    )
                ]
                if len({entry["id"] for entry in catalog}) != len(set(references)):
                    raise attach_preservation_detail(StagedScheduleError(
                        f"block {block_id} has unresolved catalog references"
                    ), "_playback_representations", "reference_failure")
        yield {
            "block_id": block_id,
            "liquid_type": liquid_type,
            "content_missing": content_missing,
            "plan": plan,
            "catalog": catalog,
        }


def generated_playback_representations(connection, history):
    """Yield exact post-seam playback data for the native worker."""
    rows = connection.execute(
        "SELECT id,liquid_type,content_json,plan_json FROM liquid_blocks "
        "WHERE station=? AND start_time>=? AND start_time<? ORDER BY start_time,id",
        (history.channel, history.regeneration_start, history.effective_horizon),
    )
    yield from _playback_representations(connection, rows)


def retained_playback_representations(connection, history):
    """Yield exact pre-seam playback data for worker-side classification."""
    rows = connection.execute(
        "SELECT id,liquid_type,content_json,plan_json FROM liquid_blocks "
        "WHERE station=? AND start_time<? ORDER BY start_time,id",
        (history.channel, history.regeneration_start),
    )
    yield from _playback_representations(connection, rows)


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
        block_references, unused_missing = _playback_catalog_references(
            row["content_json"], row["liquid_type"]
        )
        references.update(block_references)
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


def assert_protected_state(connection, state, *, sequence_verification=False):
    helper = "_restore_sequences" if sequence_verification else "assert_protected_state"
    category = "verification_failed" if sequence_verification else "check_failed"
    with preservation_step(helper, category):
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
            raise attach_preservation_detail(StagedScheduleError("; ".join(failures)),
                                             helper, category if sequence_verification else "mismatch")


def restore_sequence_state(connection, state):
    """Restore all sequence tables globally from a schema-checked snapshot."""
    tables = ("sequence_entries", "sequence_group_state", "named_sequence")
    expected = {"named_sequence", "sequence_entries", "sequence_group_state"}
    if not expected.issubset(state):
        raise attach_preservation_detail(StagedScheduleError("sequence snapshot is incomplete"),
                                         "restore_sequence_state", "schema_mismatch")
    for table in tables:
        with preservation_step("restore_sequence_state", "schema_check_failed"):
            if _columns(connection, table) != state[table]["columns"]:
                raise attach_preservation_detail(StagedScheduleError(f"sequence schema changed for {table}"),
                                                 "restore_sequence_state", "schema_mismatch")
        with preservation_step("restore_sequence_state", "delete_failed"):
            connection.execute(f"DELETE FROM {_quote(table)}")
    for table in ("named_sequence", "sequence_entries", "sequence_group_state"):
        columns = state[table]["columns"]
        sql = (
            f"INSERT INTO {_quote(table)} ({','.join(_quote(item) for item in columns)}) "
            f"VALUES ({','.join('?' for unused in columns)})"
        )
        for row in state[table]["rows"]:
            with preservation_step("restore_sequence_state", "insert_failed"):
                connection.execute(sql, row)


@preservation_check("assert_retained_history", "schedule_check_failed")
def assert_retained_history(connection, history):
    columns, rows = _rows(
        connection,
        "liquid_blocks",
        "station=? AND start_time<?",
        (history.channel, history.proposal_boundary),
    )
    if columns != _columns(connection, "liquid_blocks") or rows != history.retained_rows:
        raise attach_preservation_detail(StagedScheduleError(f"pre-boundary history changed for {history.channel}"),
                                         "assert_retained_history", "schedule_mismatch")
    if history.protected_catalog_ids:
        placeholders = ",".join("?" for unused in history.protected_catalog_ids)
        with preservation_step("assert_retained_history", "catalog_check_failed"):
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
                raise attach_preservation_detail(StagedScheduleError(
                    f"protected catalog IDs disappeared for {history.channel}: {missing}"
                ), "assert_retained_history", "catalog_mismatch")
            changed = sorted(
                catalog_id
                for catalog_id, expected in history.retained_catalog_rows.items()
                if present[catalog_id] != expected
            )
            if changed:
                raise attach_preservation_detail(StagedScheduleError(
                    f"protected catalog rows changed for {history.channel}: {changed}"
                ), "assert_retained_history", "catalog_mismatch")


def _catalog_identity(row):
    path = row.get("realpath") or row.get("path")
    mapping = canonical_media_mapping(path, "catalog path", allow_sandbox=True)
    return row["station"], row["tag"], mapping.logical_identity


def capture_catalog_rows(connection, channel):
    columns, rows = _rows(connection, "catalog_entries", "station=?", (channel,))
    return columns, rows


def capture_catalog_media_metadata(connection, rows, columns):
    result = {}
    for values in rows:
        row = _row_dict(columns, values)
        identity = _catalog_identity(row)
        path = row.get("realpath") or row.get("path")
        metadata = {}
        for table in ("file_meta", "break_points", "chapter_points"):
            table_columns, table_rows = _rows(connection, table, "path=?", (path,))
            if table_rows:
                normalized = list(table_rows[0])
                if "path" in table_columns:
                    normalized[table_columns.index("path")] = identity[2]
                metadata[table] = tuple(normalized)
            else:
                metadata[table] = None
            metadata[f"{table}_columns"] = tuple(table_columns)
        result[identity] = metadata
    return result


def _typed_catalog_semantics(row, media_metadata=None):
    values = []
    for name in sorted(name for name in row if name != "id"):
        value = row[name]
        if name in ("path", "realpath") and value is not None:
            value = canonical_media_mapping(
                value, f"catalog {name}", allow_sandbox=True
            ).logical_identity
        values.append((name, canonical_sqlite_value(value)))
    metadata = media_metadata or {}
    for name in ("file_meta", "break_points", "chapter_points"):
        row = metadata.get(name)
        columns = metadata.get(f"{name}_columns", ())
        if row is None:
            encoded = canonical_sqlite_value(None)
        else:
            encoded = b"".join(
                column.encode("utf-8") + b"\0" + canonical_sqlite_value(value)
                for column, value in zip(columns, row)
            )
        values.append((f"media:{name}", canonical_sqlite_value(encoded)))
    digest = hashlib.sha256()
    for name, value in values:
        digest.update(name.encode("utf-8") + b"\0" + value)
    return digest.digest()


def _next_catalog_id(connection):
    maximum = connection.execute(
        "SELECT COALESCE(MAX(id),0) FROM catalog_entries"
    ).fetchone()[0]
    sequence = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
    ).fetchone()
    sequence_value = sequence[0] if sequence else 0
    return max(maximum, sequence_value) + 1


def catalog_allocation_floor(connection):
    maximum = connection.execute(
        "SELECT COALESCE(MAX(id),0) FROM catalog_entries"
    ).fetchone()[0]
    sequence = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
    ).fetchone()
    return max(maximum, sequence[0] if sequence else 0)


def reconcile_catalog(
    connection,
    channel,
    generated_rows,
    protected_ids,
    *,
    original_rows=None,
    original_media_metadata=None,
    generated_media_metadata=None,
    statistics=None,
    allocation_floor=None,
):
    columns, current_rows = _rows(connection, "catalog_entries", "station=?", (channel,))
    if original_rows is None:
        original_rows = current_rows
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
    if allocation_floor is not None:
        remaining_max = connection.execute(
            "SELECT COALESCE(MAX(id),0) FROM catalog_entries"
        ).fetchone()[0]
        restored = max(allocation_floor, remaining_max)
        updated = connection.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name='catalog_entries'",
            (restored,),
        )
        if updated.rowcount == 0:
            connection.execute(
                "INSERT INTO sqlite_sequence(name,seq) VALUES('catalog_entries',?)",
                (restored,),
            )
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

    next_id = _next_catalog_id(connection)
    reused = new = 0
    for identity in sorted(generated_by_identity):
        row = generated_by_identity[identity]
        original = by_identity.get(identity)
        same_semantics = bool(original) and _typed_catalog_semantics(
            original, (original_media_metadata or {}).get(identity)
        ) == _typed_catalog_semantics(
            row, (generated_media_metadata or {}).get(identity)
        )
        original_is_protected = bool(original) and original["id"] in protected
        if original_is_protected:
            original_is_sandbox = str(
                original.get("realpath") or original["path"]
            ).startswith("/media/")
            if original_is_sandbox and not same_semantics:
                raise StagedScheduleError(
                    f"generated catalog conflicts with retained historical row {original['id']}"
                )
            if original_is_sandbox:
                active_ids.add(original["id"])
                reused += 1
                continue
            # The historical host-path row stays exact. A deterministic stage
            # alias gets a provisional ID so native code can open /media.
            row["id"] = next_id
            next_id += 1
            new += 1
        else:
            row["id"] = original["id"] if same_semantics else next_id
            if not same_semantics:
                next_id += 1
                new += 1
            else:
                reused += 1
        values = [row.get(name) for name in insert_columns]
        connection.execute(sql, values)
        active_ids.add(row["id"])

    if new:
        final_sequence = next_id - 1
        updated = connection.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name='catalog_entries'",
            (final_sequence,),
        )
        if updated.rowcount == 0:
            connection.execute(
                "INSERT INTO sqlite_sequence(name,seq) VALUES('catalog_entries',?)",
                (final_sequence,),
            )

    if statistics is not None:
        statistics.update(
            {"reused": reused, "new": new, "protected": len(protected)}
        )
    return active_ids


@preservation_check("coverage_report", "check_failed")
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
        category = "gap_and_overlap" if gaps and overlaps else "overlap" if overlaps else "gap"
        raise attach_preservation_detail(StagedScheduleError(
            f"coverage failure for {history.channel}: {len(gaps)} gap(s), {len(overlaps)} overlap(s)"
        ), "coverage_report", category)
    return report


def validate_scheduled_paths(connection, histories, sandbox_media_root):
    checked = 0
    for history in histories:
        rows = connection.execute(
            "SELECT id,liquid_type,start_time,content_json,plan_json FROM liquid_blocks "
            "WHERE station=? AND start_time<? ORDER BY start_time,id",
            (history.channel, history.effective_horizon),
        )
        for block_id, liquid_type, unused_start_time, content_json, plan_json in rows:
            references, unused_missing = _playback_catalog_references(
                content_json, liquid_type
            )
            catalog_rows = []
            if references:
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
