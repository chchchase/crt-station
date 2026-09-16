"""Bounded, typed normalization of a completed C1 staged database."""

import base64
import hashlib
import json
import math
import os
import sqlite3
import stat
import struct
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from station_director.path_safety import canonical_media_mapping
from station_director.preservation import readonly_database
from station_director.staged_schedule import parse_catalog_references, validate_plan_json


MAX_RECORD_BYTES = 2 * 1024 * 1024
MAX_DIFFERENCES = 50
MAX_PREVIEW_BYTES = 512
MAX_DIAGNOSTIC_BYTES = 64 * 1024
MAX_TABLES = 512
MAX_SCHEMA_OBJECTS = 4096
MAX_COLUMNS_PER_TABLE = 512
MAX_REFERENCE_LEDGER_ROWS = 1000000
MAX_CANONICAL_RECORDS = 10000000
MAX_ROWS_PER_TABLE = 100000000
GUIDE_MAGIC = b"FS42-GUIDE\x00\x01"
GUIDE_ARTIFACT_IDENTITY = "guide/guide-v1.records"
MAX_GUIDE_RECORD_BYTES = 512 * 1024
MAX_GUIDE_STREAM_BYTES = 64 * 1024 * 1024
MAX_GUIDE_RECORDS = 100000
JSON_COLUMNS = {
    ("liquid_blocks", "sequence_key"),
    ("liquid_blocks", "break_info"),
    ("liquid_blocks", "content_json"),
    ("liquid_blocks", "plan_json"),
    ("catalog_entries", "hints"),
    ("file_meta", "meta"),
    ("break_points", "points"),
    ("chapter_points", "points"),
}
KNOWN_ID_COLUMNS = {
    ("catalog_entries", "id"),
    ("liquid_blocks", "id"),
    ("named_sequence", "id"),
    ("sequence_entries", "id"),
    ("sequence_entries", "named_sequence_id"),
}
KNOWN_FOREIGN_KEYS = {
    ("sequence_entries", "named_sequence_id", "named_sequence"),
    ("break_points", "path", "file_meta"),
    ("chapter_points", "path", "file_meta"),
}
# Inspected native reference graph:
# - liquid_blocks.content_json -> catalog_entries.id (scalar or integer array)
# - sequence_entries.named_sequence_id -> named_sequence.id
# - file_meta/break_points/chapter_points.path -> the catalog media identity
# Native liquid_blocks.id and sequence_entries.id have no reference sites.  The
# three sequence tables are globally restored by C1 and therefore stay exact;
# only newly allocated catalog IDs are eligible for semantic tokenization.
# Any additional *_id column, foreign key, or nested reference-like key is
# rejected below until its semantics are explicitly classified.
POSSIBLE_REFERENCE_KEYS = {
    "catalog_id", "catalog_ids", "liquid_block_id", "liquid_block_ids",
    "named_sequence_id", "sequence_entry_id",
}
OPERATIONAL_RESPONSE_PATHS = {
    "/run_id",
    "/timings_ms",
    "/verification/fingerprints/staged_source_physical_configuration_fingerprint",
}
EXPECTED_RESPONSE_FIELDS = {
    "schema_version", "operation", "run_id", "proposal_id", "status",
    "phase_reached", "scheduler_invoked", "validation_context",
    "affected_channels", "channels", "verification", "preservation",
    "path_validation", "warnings", "failure", "timings_ms", "diagnostics",
    "guide_validation",
}


class NormalizationError(RuntimeError):
    pass


def _reject_constant(value):
    raise NormalizationError(f"non-finite JSON number is unsupported: {value}")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise NormalizationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_json(raw, label, *, allow_empty=False):
    if raw is None and allow_empty:
        return None
    if not isinstance(raw, str):
        raise NormalizationError(f"{label} must be SQLite text")
    if len(raw.encode("utf-8")) > MAX_RECORD_BYTES:
        raise NormalizationError(f"{label} exceeds the normalization limit")
    if raw == "" and allow_empty:
        return ""
    try:
        return json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_pairs)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise NormalizationError(f"malformed {label}: {exc}") from exc


def _typed(value):
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NormalizationError("non-finite SQLite/JSON real is unsupported")
        return ["real-ieee754", base64.b64encode(struct.pack(">d", value)).decode("ascii")]
    if isinstance(value, str):
        value.encode("utf-8")
        return ["text", value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ["blob", base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, list):
        return ["array", [_typed(item) for item in value]]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise NormalizationError("JSON object keys must be text")
        return ["object", [[key, _typed(value[key])] for key in sorted(value)]]
    raise NormalizationError(f"unsupported value type: {type(value).__name__}")


def _canonical(value):
    return json.dumps(_typed(value), ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def canonical_typed_value_bytes(value):
    """Public C2/C3 canonical scalar/container encoding contract."""
    return _canonical(value)


def canonical_catalog_semantics(connection, columns, row):
    """Return the complete normalized catalog/media semantics for one row."""
    return _catalog_semantics(connection, columns, row)


def _quote(value):
    return '"' + str(value).replace('"', '""') + '"'


def _columns(connection, table):
    values = []
    for row in connection.execute(f"PRAGMA table_xinfo({_quote(table)})"):
        if len(values) >= MAX_COLUMNS_PER_TABLE:
            raise NormalizationError(f"column limit exceeded for {table}")
        values.append(row[1])
    return values


def _primary_columns(connection, table):
    rows = _bounded_rows(
        connection.execute(f"PRAGMA table_info({_quote(table)})"),
        MAX_COLUMNS_PER_TABLE, f"primary-key metadata for {table}",
    )
    return [
        row[1] for row in sorted(
            rows,
            key=lambda row: row[5] or 10**9,
        ) if row[5]
    ]


def _check_reference_columns(connection, tables):
    for table in tables:
        columns = _columns(connection, table)
        primary = set(_primary_columns(connection, table))
        for column in columns:
            lowered = column.casefold()
            if (
                (lowered == "id" or lowered.endswith("_id"))
                and column not in primary
                and (table, column) not in KNOWN_ID_COLUMNS
            ):
                raise NormalizationError(
                    f"unclassified possible ID reference column: {table}.{column}"
                )
        foreign_key_count = 0
        for row in connection.execute(f"PRAGMA foreign_key_list({_quote(table)})"):
            foreign_key_count += 1
            if foreign_key_count > MAX_COLUMNS_PER_TABLE:
                raise NormalizationError(f"foreign-key limit exceeded for {table}")
            target_table = row[2]
            source_column = row[3]
            if (table, source_column, target_table) not in KNOWN_FOREIGN_KEYS:
                raise NormalizationError(
                    f"unclassified foreign-key reference: {table}.{source_column}->{target_table}"
                )


def _check_hidden_references(value, label):
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = key.casefold().replace("-", "_")
            suspicious = (
                lowered in POSSIBLE_REFERENCE_KEYS
                or any(word in lowered for word in ("catalog", "liquid_block", "sequence_entry"))
                and any(word in lowered for word in ("id", "ref"))
            )
            if suspicious:
                raise NormalizationError(f"unsupported nested reference at {label}.{key}")
            _check_hidden_references(child, f"{label}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _check_hidden_references(child, f"{label}[{index}]")


def _logical_path(value, label):
    if value is None:
        return None
    if not isinstance(value, str):
        raise NormalizationError(f"{label} must be text")
    try:
        return canonical_media_mapping(value, label, allow_sandbox=True).logical_identity
    except Exception as exc:
        raise NormalizationError(str(exc)) from exc


def _descriptor_digest(value):
    # Preserve the exact opaque bytes without publishing descriptor bodies in
    # comparison records/previews. This namespace cannot be a media identity.
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_RECORD_BYTES:
        raise NormalizationError("descriptor exceeds normalization limit")
    return "autobump-sha256:" + hashlib.sha256(
        b"FS42-RETAINED-AUTOBUMP\0" + _canonical(value)).hexdigest()


def _catalog_descriptor(values):
    # Pure classification only, loaded on use: no native runtime/player imports.
    from fs42 import autobump_descriptor as descriptor
    classification = descriptor.classify_catalog_entry(values)
    if classification == descriptor.DESCRIPTOR_INVALID:
        raise NormalizationError("invalid catalog descriptor")
    return classification == descriptor.DESCRIPTOR_SELECTED


def _catalog_path(values, name):
    if _catalog_descriptor(values):
        return _descriptor_digest(values[name]) if values[name] is not None else None
    return _logical_path(values[name], f"catalog_entries.{name}")


def _block_playback(values, *, allow_descriptors):
    """Share exact reference/plan interpretation with C3 comparison."""
    from fs42 import autobump_descriptor as descriptor
    content = _parse_json(values["content_json"], "liquid_blocks.content_json")
    _parse_json(values["plan_json"], "liquid_blocks.plan_json")
    plan = validate_plan_json(values["plan_json"])
    has_descriptor = False
    for entry in plan:
        classification = descriptor.classify_plan_entry(
            entry, liquid_type=values["liquid_type"], plan_size=len(plan),
            content_missing=content is None,
        )
        if classification == descriptor.DESCRIPTOR_INVALID:
            raise NormalizationError("invalid playback descriptor")
        if classification == descriptor.DESCRIPTOR_SELECTED:
            if not allow_descriptors:
                raise NormalizationError("generated AutoBump is unsupported")
            has_descriptor = True
            entry["path"] = _descriptor_digest(entry["path"])
        else:
            entry["path"] = _logical_path(entry["path"], "playback plan path")
        _check_hidden_references(entry, "liquid_blocks.plan_json")
    if content is None:
        if (not allow_descriptors or values["liquid_type"] != "LiquidWebBlock"
                or len(plan) != 1 or not has_descriptor
                or plan[0]["content_type"] != "feature"):
            raise NormalizationError("unsupported null catalog reference")
        references = ()
    else:
        references = parse_catalog_references(values["content_json"])
    return references, plan, has_descriptor


def _generated_block(values, seams):
    return values["station"] in seams and values["start_time"] >= seams[values["station"]]


def _require_retained_descriptor_block(baseline, columns, row, seams):
    values = dict(zip(columns, row))
    if _generated_block(values, seams):
        raise NormalizationError("generated AutoBump is unsupported")
    selected = ",".join(_quote(name) for name in columns)
    old = baseline.execute(f"SELECT {selected} FROM liquid_blocks WHERE id=?", (values["id"],)).fetchone()
    if old is None or _canonical(list(old)) != _canonical(list(row)):
        raise NormalizationError("descriptor block is not exact retained history")


def _metadata_for_path(connection, path):
    result = {}
    for table in ("file_meta", "break_points", "chapter_points"):
        columns = _columns(connection, table)
        cursor = connection.execute(
            f"SELECT {','.join(_quote(item) for item in columns)} FROM {_quote(table)} WHERE path=?",
            (path,),
        )
        row = cursor.fetchone()
        if cursor.fetchone() is not None:
            raise NormalizationError(f"ambiguous {table} rows for catalog path")
        if row is None:
            result[table] = None
            continue
        values = {}
        for name, value in zip(columns, row):
            if name == "path":
                value = _logical_path(value, f"{table}.path")
            elif (table, name) in JSON_COLUMNS and value not in (None, ""):
                value = _parse_json(value, f"{table}.{name}", allow_empty=True)
                _check_hidden_references(value, f"{table}.{name}")
            values[name] = value
        result[table] = values
    return result


def _catalog_semantics(connection, columns, row):
    values = dict(zip(columns, row))
    path = values.get("realpath") or values.get("path")
    for name in ("path", "realpath"):
        if values.get(name) is not None:
            values[name] = _catalog_path(dict(zip(columns, row)), name)
    hints = values.get("hints")
    if hints not in (None, ""):
        parsed = _parse_json(hints, "catalog_entries.hints")
        if not isinstance(parsed, list):
            raise NormalizationError("catalog_entries.hints must decode to an array")
        decoded = []
        for index, item in enumerate(parsed):
            if not isinstance(item, str):
                raise NormalizationError("catalog hint entries must be encoded JSON text")
            hint = _parse_json(item, f"catalog_entries.hints[{index}]")
            _check_hidden_references(hint, f"catalog_entries.hints[{index}]")
            decoded.append(hint)
        values["hints"] = decoded
    values.pop("id", None)
    return {"row": values, "media_records": _metadata_for_path(connection, path)}


def _catalog_core(values):
    return (
        values.get("station"), values.get("tag"),
        _catalog_path(values, "realpath" if values.get("realpath") else "path"),
    )


def _create_artifacts(stage):
    directory = Path(stage) / "normalization"
    stream_path = directory / "canonical.records"
    index_path = directory / "lookup.sqlite"
    stream_fd = index_fd = None
    index = None
    created_directory = False
    try:
        directory.mkdir(mode=0o700, exist_ok=False)
        created_directory = True
        stream_fd = os.open(
            stream_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        index_fd = os.open(
            index_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(index_fd)
        index_fd = None
        index = sqlite3.connect(index_path)
        index.execute("PRAGMA journal_mode=OFF")
        index.execute("PRAGMA synchronous=OFF")
        index.executescript(
            "CREATE TABLE catalog_map(id INTEGER PRIMARY KEY, token TEXT NOT NULL, semantics BLOB NOT NULL, logical_path TEXT);"
            "CREATE UNIQUE INDEX catalog_token ON catalog_map(token);"
            "CREATE TABLE baseline_catalog(id INTEGER PRIMARY KEY, row_value BLOB NOT NULL, semantics BLOB NOT NULL, protected INTEGER NOT NULL DEFAULT 0);"
            "CREATE TABLE reference_ledger(location TEXT NOT NULL, ordinal INTEGER NOT NULL, original_id INTEGER NOT NULL, expected_token TEXT NOT NULL, visited INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(location,ordinal));"
            "CREATE TABLE sort_rows(sort_key BLOB NOT NULL, path TEXT NOT NULL, value BLOB NOT NULL);"
            "CREATE INDEX sort_rows_order ON sort_rows(path COLLATE BINARY,sort_key,value);"
            "CREATE TABLE records(path TEXT PRIMARY KEY, value BLOB NOT NULL);"
            "CREATE TABLE normalization_counters(name TEXT PRIMARY KEY,value INTEGER NOT NULL);"
            "INSERT INTO normalization_counters VALUES('records',0);"
            f"CREATE TRIGGER record_limit BEFORE INSERT ON records WHEN "
            f"(SELECT value FROM normalization_counters WHERE name='records')>={MAX_CANONICAL_RECORDS} "
            "BEGIN SELECT RAISE(ABORT,'canonical record-count limit exceeded'); END;"
            "CREATE TRIGGER record_count AFTER INSERT ON records BEGIN "
            "UPDATE normalization_counters SET value=value+1 WHERE name='records'; END;"
        )
        return directory, stream_path, stream_fd, index
    except Exception:
        if index is not None:
            index.close()
        if index_fd is not None:
            os.close(index_fd)
        if stream_fd is not None:
            os.close(stream_fd)
        if created_directory:
            _remove_artifacts(directory)
        raise


def _remove_artifacts(directory):
    directory = Path(directory)
    for name in ("lookup.sqlite-journal", "lookup.sqlite-wal", "lookup.sqlite-shm",
                 "lookup.sqlite", "canonical.records"):
        try:
            (directory / name).unlink()
        except FileNotFoundError:
            pass
    try:
        directory.rmdir()
    except FileNotFoundError:
        pass


def _bounded_rows(cursor, limit, label):
    rows = []
    for row in cursor:
        if len(rows) >= limit:
            raise NormalizationError(f"{label} limit exceeded")
        rows.append(row)
    return rows


def _reference_location(columns, row, primary):
    values = dict(zip(columns, row))
    identity = {name: values[name] for name in primary} if primary else values
    return hashlib.sha256(_canonical(identity)).hexdigest()


def _queue_record(index, path, value):
    encoded = json.dumps(
        _typed(value), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    if len(encoded) + len(path.encode("utf-8")) > MAX_RECORD_BYTES:
        raise NormalizationError(f"normalized record exceeds limit: {path}")
    try:
        index.execute("INSERT INTO records(path,value) VALUES(?,?)", (path, encoded))
    except sqlite3.IntegrityError as exc:
        if "canonical record-count limit exceeded" in str(exc):
            raise NormalizationError("canonical record-count limit exceeded") from exc
        raise NormalizationError(f"duplicate normalized record path: {path}") from exc


def _flush_records(index, descriptor):
    digest = hashlib.sha256()
    count = 0
    for path, typed in index.execute(
        "SELECT path,value FROM records ORDER BY path COLLATE BINARY"
    ):
        if count >= MAX_CANONICAL_RECORDS:
            raise NormalizationError("canonical record-count limit exceeded")
        encoded = (
            b'["record",'
            + json.dumps(path, ensure_ascii=False).encode("utf-8")
            + b"," + bytes(typed) + b"]"
        )
        frame = len(encoded).to_bytes(8, "big") + encoded
        view = memoryview(frame)
        while view:
            view = view[os.write(descriptor, view):]
        digest.update(frame)
        count += 1
    os.fsync(descriptor)
    return digest.hexdigest(), count


def _normalized_response(response):
    if set(response) != EXPECTED_RESPONSE_FIELDS:
        raise NormalizationError("C1 response fields changed without equality classification")
    if response.get("status") != "success":
        raise NormalizationError("failed C1 response cannot be normalized")
    result = {}
    for key in sorted(response):
        path = f"/{key}"
        if path in OPERATIONAL_RESPONSE_PATHS:
            continue
        if key == "verification":
            verification = dict(response[key])
            fingerprints = dict(verification.get("fingerprints", {}))
            fingerprints.pop("staged_source_physical_configuration_fingerprint", None)
            verification["fingerprints"] = fingerprints
            result[key] = verification
        else:
            result[key] = response[key]
    return result


def _queue_guide_records(index, stage, response):
    guide = response["guide_validation"]
    if guide.get("artifact_identity") != GUIDE_ARTIFACT_IDENTITY:
        raise NormalizationError("guide artifact identity is not the fixed C3a1 identity")
    path = Path(stage) / GUIDE_ARTIFACT_IDENTITY
    directory = path.parent
    directory_descriptor = os.open(
        directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except Exception:
        os.close(directory_descriptor)
        raise
    digest = hashlib.sha256()
    try:
        directory_before = os.fstat(directory_descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_size > MAX_GUIDE_STREAM_BYTES
            or before.st_size != guide.get("byte_count")
            or not stat.S_ISDIR(directory_before.st_mode)
            or stat.S_IMODE(directory_before.st_mode) != 0o700
            or directory_before.st_uid != os.geteuid()
            or os.listdir(directory_descriptor) != [path.name]
        ):
            raise NormalizationError("guide artifact is unsafe or oversized")

        def read_exact(size):
            chunks = []
            remaining = size
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    raise NormalizationError("guide artifact is truncated")
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            digest.update(raw)
            return raw

        if read_exact(len(GUIDE_MAGIC)) != GUIDE_MAGIC:
            raise NormalizationError("guide artifact format/version mismatch")
        count = 0
        while True:
            length = os.read(descriptor, 8)
            if not length:
                break
            digest.update(length)
            if len(length) != 8:
                length += read_exact(8 - len(length))
            size = int.from_bytes(length, "big")
            if size > MAX_GUIDE_RECORD_BYTES:
                raise NormalizationError("guide artifact record exceeds size limit")
            raw = read_exact(size)
            count += 1
            if count > MAX_GUIDE_RECORDS:
                raise NormalizationError("guide artifact record-count limit exceeded")
            try:
                record = json.loads(
                    raw.decode("utf-8"), object_pairs_hook=_pairs,
                    parse_constant=_reject_constant,
                )
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise NormalizationError(f"malformed guide artifact record: {exc}") from exc
            if not isinstance(record, dict) or set(record) != {"path", "value"}:
                raise NormalizationError("guide artifact record shape is unsupported")
            record_path = record["path"]
            if not isinstance(record_path, str) or not record_path.startswith("/guide/"):
                raise NormalizationError("guide artifact record path is invalid")
            _queue_record(index, record_path, record["value"])
        after = os.fstat(descriptor)
        path_info = path.lstat()
        directory_after = os.fstat(directory_descriptor)
        identity = lambda item: (
            item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns,
            item.st_ctime_ns, item.st_nlink,
        )
        if identity(before) != identity(after) or (
            path_info.st_dev, path_info.st_ino
        ) != (after.st_dev, after.st_ino):
            raise NormalizationError("guide artifact changed or was replaced")
        if (
            identity(directory_before) != identity(directory_after)
            or os.listdir(directory_descriptor) != [path.name]
        ):
            raise NormalizationError("guide artifact directory changed")
        if count != guide.get("record_count"):
            raise NormalizationError("guide artifact record count mismatch")
        if digest.hexdigest() != guide.get("digest"):
            raise NormalizationError("guide artifact digest mismatch")
    finally:
        os.close(descriptor)
        os.close(directory_descriptor)


@dataclass
class NormalizedRun:
    stream_path: Path
    digest: str
    record_count: int
    provisional_count: int
    artifact_directory: Path
    stream_identity: tuple


def normalize_completed_run(stage, baseline_database, response, proposal_boundary):
    """Normalize without writing to either application database."""
    stage = Path(stage)
    seams = {channel["name"]: channel["regeneration_start"] for channel in response["channels"]}
    database = stage / "work/runtime/fs42_fluid.db"
    directory, stream_path, descriptor, index = _create_artifacts(stage)
    try:
        with readonly_database(baseline_database) as baseline, readonly_database(database) as current:
            tables = [row[0] for row in _bounded_rows(current.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ), MAX_TABLES, "table")]
            baseline_tables = [row[0] for row in _bounded_rows(baseline.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ), MAX_TABLES, "baseline table")]
            _check_reference_columns(current, tables)
            _check_reference_columns(baseline, baseline_tables)

            schema_count = 0
            for row in current.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master"
            ):
                if schema_count >= MAX_SCHEMA_OBJECTS:
                    raise NormalizationError("schema object limit exceeded")
                schema_count += 1
                item = {"type": row[0], "name": row[1], "table": row[2], "sql": row[3]}
                _queue_record(index, f"/schema/{item['type']}/{item['name']}", item)
            for table in tables:
                columns = _columns(current, table)
                column_info = [list(row) for row in _bounded_rows(
                    current.execute(f"PRAGMA table_xinfo({_quote(table)})"),
                    MAX_COLUMNS_PER_TABLE, f"column metadata for {table}",
                )]
                foreign_keys = [list(row) for row in _bounded_rows(
                    current.execute(f"PRAGMA foreign_key_list({_quote(table)})"),
                    MAX_COLUMNS_PER_TABLE, f"foreign keys for {table}",
                )]
                _queue_record(index, f"/columns/{table}", {
                    "order": columns, "table_xinfo": column_info, "foreign_keys": foreign_keys,
                })

            if "catalog_entries" not in tables or "liquid_blocks" not in tables:
                raise NormalizationError("required scheduling tables are absent")
            catalog_columns = _columns(current, "catalog_entries")
            baseline_catalog_columns = _columns(baseline, "catalog_entries")
            if catalog_columns != baseline_catalog_columns or "id" not in catalog_columns:
                raise NormalizationError("catalog schema differs from baseline")
            id_index = catalog_columns.index("id")
            baseline_maximum = 0
            baseline_catalog_count = 0
            baseline_selected = ",".join(_quote(item) for item in catalog_columns)
            for baseline_row in baseline.execute(
                f"SELECT {baseline_selected} FROM catalog_entries"
            ):
                baseline_catalog_count += 1
                if baseline_catalog_count > MAX_ROWS_PER_TABLE:
                    raise NormalizationError("baseline catalog row-count limit exceeded")
                baseline_id = baseline_row[id_index]
                if isinstance(baseline_id, bool) or not isinstance(baseline_id, int):
                    raise NormalizationError("baseline catalog ID is not an integer")
                baseline_maximum = max(baseline_maximum, baseline_id)
                index.execute(
                    "INSERT INTO baseline_catalog(id,row_value,semantics) VALUES(?,?,?)",
                    (
                        baseline_id,
                        _canonical(list(baseline_row)),
                        _canonical(_catalog_semantics(
                            baseline, catalog_columns, baseline_row
                        )),
                    ),
                )
            allocation_values = [0, baseline_maximum]
            allocation_values.extend(row[0] for row in baseline.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
            ))
            allocation_floor = max(allocation_values)
            for liquid_type, raw, plan in baseline.execute(
                "SELECT liquid_type,content_json,plan_json FROM liquid_blocks WHERE start_time<?", (proposal_boundary,)
            ):
                references, unused_plan, unused_descriptor = _block_playback(
                    {"liquid_type": liquid_type, "content_json": raw, "plan_json": plan}, allow_descriptors=True)
                for catalog_id in references:
                    if index.execute(
                        "UPDATE baseline_catalog SET protected=1 WHERE id=?", (catalog_id,)
                    ).rowcount != 1:
                        raise NormalizationError(
                            f"baseline retained block references missing catalog ID {catalog_id}"
                        )

            selected = ",".join(_quote(item) for item in catalog_columns)
            provisional = 0
            current_catalog_count = 0
            for row in current.execute(f"SELECT {selected} FROM catalog_entries"):
                current_catalog_count += 1
                if current_catalog_count > MAX_ROWS_PER_TABLE:
                    raise NormalizationError("catalog row-count limit exceeded")
                values = dict(zip(catalog_columns, row))
                catalog_id = values["id"]
                semantics = _catalog_semantics(current, catalog_columns, row)
                semantic_bytes = _canonical(semantics)
                baseline_record = index.execute(
                    "SELECT row_value,semantics,protected FROM baseline_catalog WHERE id=?",
                    (catalog_id,),
                ).fetchone()
                if baseline_record is not None:
                    current_semantics = _canonical(semantics)
                    if baseline_record[2] and _canonical(list(row)) != bytes(baseline_record[0]):
                        raise NormalizationError(f"protected catalog ID {catalog_id} changed")
                    if current_semantics != bytes(baseline_record[1]):
                        raise NormalizationError(
                            f"baseline catalog ID {catalog_id} changed semantic content"
                        )
                    token = f"historical:{catalog_id}"
                else:
                    if _catalog_descriptor(values):
                        raise NormalizationError("new AutoBump catalog descriptor is unsupported")
                    if isinstance(catalog_id, bool) or not isinstance(catalog_id, int) or catalog_id <= allocation_floor:
                        raise NormalizationError(f"invalid provisional catalog ID: {catalog_id!r}")
                    token = "provisional:" + hashlib.sha256(semantic_bytes).hexdigest()
                    collision = index.execute(
                        "SELECT semantics FROM catalog_map WHERE token=?", (token,)
                    ).fetchone()
                    if collision is not None:
                        if bytes(collision[0]) != semantic_bytes:
                            raise NormalizationError("provisional semantic digest collision")
                        raise NormalizationError("ambiguous duplicate provisional catalog semantics")
                    provisional += 1
                logical_path = _catalog_core(values)[2]
                index.execute(
                    "INSERT INTO catalog_map(id,token,semantics,logical_path) VALUES(?,?,?,?)",
                    (catalog_id, token, semantic_bytes, logical_path),
                )
            index.commit()
            sequence_row = current.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
            ).fetchone()
            maximum_current_id = current.execute(
                "SELECT COALESCE(MAX(id),0) FROM catalog_entries"
            ).fetchone()[0]
            if sequence_row is None or sequence_row[0] < maximum_current_id:
                raise NormalizationError("catalog sqlite_sequence is missing or regressed")
            for token, semantics in index.execute(
                "SELECT token,semantics FROM catalog_map ORDER BY token COLLATE BINARY"
            ):
                _queue_record(
                    index, f"/catalog/{token}",
                    json.loads(bytes(semantics).decode("utf-8")),
                )

            # First pass: inventory every proven catalog reference in a
            # disk-backed ledger. The second pass below must visit each exact
            # location once and resolve it to the same comparison token.
            liquid_columns = _columns(current, "liquid_blocks")
            liquid_primary = _primary_columns(current, "liquid_blocks")
            if "content_json" not in liquid_columns:
                raise NormalizationError("liquid_blocks.content_json is absent")
            liquid_selected = ",".join(_quote(item) for item in liquid_columns)
            ledger_count = 0
            for liquid_row in current.execute(
                f"SELECT {liquid_selected} FROM liquid_blocks"
            ):
                location = _reference_location(
                    liquid_columns, liquid_row, liquid_primary
                )
                block_values = dict(zip(liquid_columns, liquid_row))
                references, unused_plan, has_descriptor = _block_playback(
                    block_values, allow_descriptors=not _generated_block(block_values, seams))
                for ordinal, reference in enumerate(references):
                    ledger_count += 1
                    if ledger_count > MAX_REFERENCE_LEDGER_ROWS:
                        raise NormalizationError("catalog reference ledger limit exceeded")
                    found = index.execute(
                        "SELECT token,logical_path FROM catalog_map WHERE id=?", (reference,)
                    ).fetchone()
                    if found is None:
                        raise NormalizationError(
                            f"unresolved catalog reference {reference} in liquid_blocks"
                        )
                    has_descriptor |= found[1].startswith("autobump-sha256:")
                    index.execute(
                        "INSERT INTO reference_ledger(location,ordinal,original_id,expected_token) "
                        "VALUES(?,?,?,?)",
                        (location, ordinal, reference, found[0]),
                    )
                if has_descriptor:
                    _require_retained_descriptor_block(baseline, liquid_columns, liquid_row, seams)
            index.commit()

            for table in tables:
                columns = _columns(current, table)
                primary = _primary_columns(current, table)
                selected = ",".join(_quote(item) for item in columns)
                index.execute("DELETE FROM sort_rows")
                table_row_count = 0
                for row in current.execute(f"SELECT {selected} FROM {_quote(table)}"):
                    table_row_count += 1
                    if table_row_count > MAX_ROWS_PER_TABLE:
                        raise NormalizationError(f"row-count limit exceeded for {table}")
                    values = {}
                    row_dict = dict(zip(columns, row))
                    playback = (_block_playback(row_dict, allow_descriptors=not _generated_block(row_dict, seams))
                                if table == "liquid_blocks" else None)
                    reference_location = (
                        _reference_location(columns, row, primary)
                        if table == "liquid_blocks" else None
                    )
                    for name, value in row_dict.items():
                        if table == "catalog_entries" and name == "id":
                            value = index.execute(
                                "SELECT token FROM catalog_map WHERE id=?", (value,)
                            ).fetchone()[0]
                        elif table == "liquid_blocks" and name == "content_json":
                            references = playback[0]
                            if not references and _parse_json(value, "liquid_blocks.content_json") is None:
                                values[name] = None
                                continue
                            tokens = []
                            for ordinal, reference in enumerate(references):
                                found = index.execute(
                                    "SELECT expected_token,visited FROM reference_ledger "
                                    "WHERE location=? AND ordinal=? AND original_id=?",
                                    (reference_location, ordinal, reference),
                                ).fetchone()
                                if found is None:
                                    raise NormalizationError(
                                        "catalog reference was not inventoried exactly"
                                    )
                                if found[1] != 0:
                                    raise NormalizationError("catalog reference was visited more than once")
                                index.execute(
                                    "UPDATE reference_ledger SET visited=1 "
                                    "WHERE location=? AND ordinal=?",
                                    (reference_location, ordinal),
                                )
                                tokens.append(found[0])
                            if not tokens:
                                raise NormalizationError(
                                    "post-tokenization catalog reference integrity failed"
                                )
                            value = tokens if value.lstrip().startswith("[") else tokens[0]
                        elif table == "liquid_blocks" and name == "plan_json":
                            value = playback[1]
                        elif (table, name) in JSON_COLUMNS and value not in (None, ""):
                            value = _parse_json(value, f"{table}.{name}", allow_empty=True)
                            _check_hidden_references(value, f"{table}.{name}")
                        elif table in ("catalog_entries", "file_meta", "break_points", "chapter_points") and name in ("path", "realpath") and value is not None:
                            value = (_catalog_path(row_dict, name) if table == "catalog_entries"
                                     else _logical_path(value, f"{table}.{name}"))
                        values[name] = value
                    encoded = _canonical(values)
                    if primary:
                        key_value = {name: values[name] for name in primary}
                        stable_key = hashlib.sha256(_canonical(key_value)).hexdigest()
                    else:
                        stable_key = hashlib.sha256(encoded).hexdigest()
                    index.execute(
                        "INSERT INTO sort_rows(sort_key,path,value) VALUES(?,?,?)",
                        (encoded, stable_key, encoded),
                    )
                duplicate_path = None
                ordinal = 0
                for unused_sort_key, stable_key, encoded in index.execute(
                    "SELECT sort_key,path,value FROM sort_rows "
                    "ORDER BY path COLLATE BINARY,sort_key,value"
                ):
                    raw = bytes(encoded)
                    if stable_key == duplicate_path:
                        ordinal += 1
                    else:
                        duplicate_path = stable_key
                        ordinal = 0
                    path = f"/database/{table}/{stable_key}/{ordinal}"
                    value = json.loads(raw.decode("utf-8"))
                    _queue_record(index, path, value)

            _queue_guide_records(index, stage, response)
            response_value = _normalized_response(response)
            _queue_record(index, "/c1-response", response_value)
            missing = index.execute(
                "SELECT COUNT(*) FROM reference_ledger WHERE visited!=1"
            ).fetchone()[0]
            dangling = index.execute(
                "SELECT COUNT(*) FROM reference_ledger AS r LEFT JOIN catalog_map AS c "
                "ON c.id=r.original_id AND c.token=r.expected_token WHERE c.id IS NULL"
            ).fetchone()[0]
            ambiguous = index.execute(
                "SELECT COUNT(*) FROM (SELECT expected_token FROM reference_ledger "
                "GROUP BY expected_token HAVING COUNT(DISTINCT original_id)!=1)"
            ).fetchone()[0]
            if missing or dangling or ambiguous:
                raise NormalizationError(
                    "post-tokenization catalog reference integrity failed"
                )
        normalized_digest, count = _flush_records(index, descriptor)
        for artifact in (stream_path, directory / "lookup.sqlite"):
            info = artifact.lstat()
            if (
                not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise NormalizationError(f"normalization artifact is unsafe: {artifact.name}")
        stream_info = stream_path.lstat()
        stream_identity = (
            stream_info.st_dev, stream_info.st_ino, stream_info.st_size,
            stream_info.st_mtime_ns, stream_info.st_ctime_ns, stream_info.st_nlink,
        )
        return NormalizedRun(
            stream_path, normalized_digest, count, provisional, directory,
            stream_identity,
        )
    except Exception:
        if index is not None:
            try:
                index.close()
            except Exception:
                pass
        try:
            os.close(descriptor)
        except OSError:
            pass
        _remove_artifacts(directory)
        raise
    finally:
        if index:
            try:
                index.close()
            except Exception:
                pass
        try:
            os.close(descriptor)
        except OSError:
            pass


def _read_frame(handle):
    length = handle.read(8)
    if not length:
        return None
    if len(length) != 8:
        raise NormalizationError("truncated canonical record length")
    size = int.from_bytes(length, "big")
    if size > MAX_RECORD_BYTES:
        raise NormalizationError("canonical record exceeds limit")
    raw = handle.read(size)
    if len(raw) != size:
        raise NormalizationError("truncated canonical record")
    return raw


@contextmanager
def _held_stream(normalized):
    descriptor = os.open(
        normalized.stream_path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    handle = None
    try:
        info = os.fstat(descriptor)
        identity = (
            info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_nlink,
        )
        if (
            not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or identity != normalized.stream_identity
        ):
            raise NormalizationError("canonical stream identity changed")
        handle = os.fdopen(descriptor, "rb", closefd=False)
        yield handle
        after = os.fstat(descriptor)
        after_identity = (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
            after.st_ctime_ns, after.st_nlink,
        )
        path_info = normalized.stream_path.lstat()
        if (
            after_identity != identity
            or path_info.st_dev != after.st_dev or path_info.st_ino != after.st_ino
        ):
            raise NormalizationError("canonical stream changed during comparison")
    finally:
        if handle is not None:
            handle.close()
        os.close(descriptor)


def _preview(value):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) <= MAX_PREVIEW_BYTES:
        return raw.decode("utf-8"), False
    return raw[:MAX_PREVIEW_BYTES].decode("utf-8", errors="replace"), True


def _first_difference_path(left, right, prefix):
    """Return a stable JSON-pointer-like path for the first structural change."""
    if type(left) is not type(right):
        return prefix
    if isinstance(left, list):
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            if left_item != right_item:
                return _first_difference_path(
                    left_item, right_item, f"{prefix}/{index}"
                )
        return f"{prefix}/{min(len(left), len(right))}"
    if isinstance(left, dict):
        keys = sorted(set(left) | set(right))
        for key in keys:
            escaped = str(key).replace("~", "~0").replace("/", "~1")
            if key not in left or key not in right:
                return f"{prefix}/{escaped}"
            if left[key] != right[key]:
                return _first_difference_path(
                    left[key], right[key], f"{prefix}/{escaped}"
                )
        return prefix
    return prefix


def compare_normalized_runs(left, right):
    differences = []
    changed = added = removed = 0
    diagnostic_bytes = 0
    left_count = right_count = 0
    left_digest = hashlib.sha256()
    right_digest = hashlib.sha256()

    def read_and_hash(handle, digest):
        raw = _read_frame(handle)
        if raw is not None:
            digest.update(len(raw).to_bytes(8, "big") + raw)
        return raw

    with _held_stream(left) as left_handle, _held_stream(right) as right_handle:
        left_raw = read_and_hash(left_handle, left_digest)
        right_raw = read_and_hash(right_handle, right_digest)
        while left_raw is not None or right_raw is not None:
            if left_count > MAX_CANONICAL_RECORDS or right_count > MAX_CANONICAL_RECORDS:
                raise NormalizationError("comparison record-count limit exceeded")
            left_value = json.loads(left_raw) if left_raw is not None else None
            right_value = json.loads(right_raw) if right_raw is not None else None
            left_path = left_value[1] if left_value is not None else None
            right_path = right_value[1] if right_value is not None else None
            if left_path is not None and right_path is not None and left_path == right_path:
                left_count += 1
                right_count += 1
                if left_raw == right_raw:
                    left_raw = read_and_hash(left_handle, left_digest)
                    right_raw = read_and_hash(right_handle, right_digest)
                    continue
                changed += 1
                kind = "changed"
                field_path = _first_difference_path(
                    left_value[2], right_value[2], left_path
                )
                consume_left = consume_right = True
            elif right_path is None or (left_path is not None and left_path < right_path):
                left_count += 1
                removed += 1
                kind = "removed"
                field_path = left_path
                consume_left, consume_right = True, False
            else:
                right_count += 1
                added += 1
                kind = "added"
                field_path = right_path
                consume_left, consume_right = False, True
            if left_raw is None and right_raw is None:
                break
            if len(differences) < MAX_DIFFERENCES and diagnostic_bytes < MAX_DIAGNOSTIC_BYTES:
                left_preview, left_truncated = _preview(left_value)
                right_preview, right_truncated = _preview(right_value)
                item = {
                    "kind": kind, "field_path": field_path,
                    "run_1": left_preview, "run_2": right_preview,
                    "value_truncated": left_truncated or right_truncated,
                }
                size = len(json.dumps(item).encode("utf-8"))
                if diagnostic_bytes + size <= MAX_DIAGNOSTIC_BYTES:
                    differences.append(item)
                    diagnostic_bytes += size
            if consume_left:
                left_raw = read_and_hash(left_handle, left_digest)
            if consume_right:
                right_raw = read_and_hash(right_handle, right_digest)
    passed = (
        changed == 0 and added == 0 and removed == 0
        and left_count == right_count == left.record_count == right.record_count
        and left.digest == right.digest
        and left_digest.hexdigest() == left.digest
        and right_digest.hexdigest() == right.digest
    )
    return {
        "passed": passed,
        "run_1_digest": left.digest,
        "run_2_digest": right.digest,
        "run_1_record_count": left_count,
        "run_2_record_count": right_count,
        "changed_records": changed,
        "added_records": added,
        "removed_records": removed,
        "differences": differences,
        "differences_truncated": (changed + added + removed) > len(differences),
    }
