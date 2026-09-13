import base64
import errno
import hashlib
import json
import math
import os
import sqlite3
import stat
import struct
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class PreservationError(RuntimeError):
    pass


LOGICAL_PRAGMAS = (
    "application_id",
    "user_version",
    "schema_version",
    "encoding",
    "page_size",
    "auto_vacuum",
    "foreign_keys",
)
DIGEST_PRAGMAS = (
    "application_id",
    "user_version",
    "encoding",
    "page_size",
    "auto_vacuum",
)
RAW_SUFFIXES = ("", "-wal", "-shm", "-journal")
MAX_SCHEMA_OBJECTS = 4096
MAX_DATABASE_TABLES = 512
MAX_COLUMNS_PER_TABLE = 512
MAX_FOREIGN_KEY_FINDINGS = 100000
MAX_INTEGRITY_ROWS = 1000
MAX_PROTECTED_JSON_FILES = 256
MAX_PROTECTED_JSON_FILE_BYTES = 2 * 1024 * 1024
MAX_PROTECTED_JSON_TOTAL_BYTES = 16 * 1024 * 1024
MAX_LOGICAL_ROW_BYTES = 2 * 1024 * 1024
MAX_ROWS_PER_TABLE = 100000000
MAX_MEDIA_MANIFEST_ENTRIES = 10000000
MAX_DIRECTORY_ENTRIES = 100000
UNSUPPORTED_XATTR_ERRNOS = {
    value for value in (getattr(errno, "ENOTSUP", None), getattr(errno, "EOPNOTSUPP", None))
    if value is not None
}


def _quote_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def _frame(marker, payload=b""):
    return marker + len(payload).to_bytes(8, "big") + payload


def canonical_sqlite_value(value):
    if value is None:
        return _frame(b"N")
    if isinstance(value, bool):
        raise PreservationError("SQLite returned an unexpected boolean value")
    if isinstance(value, int):
        return _frame(b"I", str(value).encode("ascii"))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PreservationError("non-finite SQLite real is unsupported")
        return _frame(b"R", struct.pack(">d", value))
    if isinstance(value, str):
        return _frame(b"T", value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _frame(b"B", bytes(value))
    raise PreservationError(f"unsupported SQLite value type: {type(value).__name__}")


def _canonical_json_digest(value):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stat_fields(info):
    return {
        "type": stat.S_IFMT(info.st_mode),
        "size": info.st_size,
        "mode": stat.S_IMODE(info.st_mode),
        "uid": info.st_uid,
        "gid": info.st_gid,
        "device": info.st_dev,
        "inode": info.st_ino,
        "mtime_ns": info.st_mtime_ns,
        "ctime_ns": info.st_ctime_ns,
    }


def raw_database_metadata(path):
    path = Path(path)
    result = {}
    for suffix in RAW_SUFFIXES:
        candidate = Path(str(path) + suffix)
        label = "database" if not suffix else suffix[1:]
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            result[label] = {"present": False}
        except OSError as exc:
            raise PreservationError(f"cannot inspect {candidate}: {exc}") from exc
        else:
            result[label] = {"present": True, **_stat_fields(info)}
    return result


@contextmanager
def readonly_database(path):
    path = Path(path).resolve(strict=True)
    uri = path.as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
            raise PreservationError("SQLite query_only could not be established")
        connection.execute("BEGIN")
        connection.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
        yield connection
        connection.rollback()
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def inspect_database_schema(connection):
    objects = []
    for object_type, name, table_name, sql in connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
    ):
        if len(objects) >= MAX_SCHEMA_OBJECTS:
            raise PreservationError("database schema object limit exceeded")
        objects.append(
            {
                "type": object_type,
                "name": name,
                "table": table_name,
                "sql": sql,
            }
        )
    tables = {}
    for item in objects:
        if item["type"] != "table":
            continue
        if len(tables) >= MAX_DATABASE_TABLES:
            raise PreservationError("database table limit exceeded")
        name = item["name"]
        columns = []
        for cid, column, declared_type, not_null, default, primary_key, hidden in connection.execute(
            f"PRAGMA table_xinfo({_quote_identifier(name)})"
        ):
            if len(columns) >= MAX_COLUMNS_PER_TABLE:
                raise PreservationError(f"database column limit exceeded for {name}")
            columns.append(
                {
                    "cid": cid,
                    "name": column,
                    "declared_type": declared_type,
                    "not_null": bool(not_null),
                    "default": default,
                    "primary_key": primary_key,
                    "hidden": hidden,
                }
            )
        tables[name] = columns
    return {"objects": objects, "tables": tables}


@contextmanager
def _logical_sort_index():
    with tempfile.TemporaryDirectory(prefix="fs42-logical-") as directory:
        path = Path(directory) / "rows.sqlite"
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(descriptor)
        index = sqlite3.connect(path)
        try:
            index.execute("PRAGMA journal_mode=OFF")
            index.execute("PRAGMA synchronous=OFF")
            index.execute("CREATE TABLE rows(value BLOB NOT NULL)")
            index.execute("CREATE INDEX row_order ON rows(value)")
            yield index
        finally:
            index.close()


def _table_digest(connection, table, columns, sort_index):
    column_names = [column["name"] for column in columns]
    if not column_names:
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(table)}"
        ).fetchone()[0]
        return row_count, hashlib.sha256(b"").hexdigest()
    selected = ",".join(_quote_identifier(name) for name in column_names)
    cursor = connection.execute(f"SELECT {selected} FROM {_quote_identifier(table)}")
    sort_index.execute("DELETE FROM rows")
    count = 0
    for row in cursor:
        if count >= MAX_ROWS_PER_TABLE:
            raise PreservationError(f"row-count limit exceeded for {table}")
        encoded = _frame(
            b"r", b"".join(canonical_sqlite_value(value) for value in row)
        )
        if len(encoded) > MAX_LOGICAL_ROW_BYTES:
            raise PreservationError(f"logical row size limit exceeded for {table}")
        sort_index.execute("INSERT INTO rows(value) VALUES(?)", (encoded,))
        count += 1
    sort_index.commit()
    digest = hashlib.sha256()
    ordered_count = 0
    for encoded, in sort_index.execute("SELECT value FROM rows ORDER BY value"):
        digest.update(bytes(encoded))
        ordered_count += 1
    if ordered_count != count:
        raise PreservationError(f"logical row spool count mismatch for {table}")
    return count, digest.hexdigest()


def logical_database_fingerprint(connection):
    started = time.monotonic()
    schema = inspect_database_schema(connection)
    schema_digest = _canonical_json_digest(schema["objects"])
    pragmas = {}
    for name in LOGICAL_PRAGMAS:
        pragmas[name] = connection.execute(f"PRAGMA {name}").fetchone()[0]
    tables = {}
    aggregate = hashlib.sha256()
    with _logical_sort_index() as sort_index:
        for name in sorted(schema["tables"]):
            count, digest = _table_digest(
                connection, name, schema["tables"][name], sort_index
            )
            tables[name] = {"row_count": count, "digest": digest}
            aggregate.update(_frame(b"t", name.encode("utf-8")))
            aggregate.update(bytes.fromhex(digest))
            aggregate.update(canonical_sqlite_value(count))
    foreign_rows = []
    for row in connection.execute("PRAGMA foreign_key_check"):
        if len(foreign_rows) >= MAX_FOREIGN_KEY_FINDINGS:
            raise PreservationError("foreign-key finding limit exceeded")
        foreign_rows.append(row)
    foreign_digest = hashlib.sha256()
    for row in sorted(
        foreign_rows,
        key=lambda value: b"".join(canonical_sqlite_value(item) for item in value),
    ):
        foreign_digest.update(_frame(b"f", b"".join(canonical_sqlite_value(item) for item in row)))
    integrity_rows = []
    for row in connection.execute("PRAGMA integrity_check"):
        if len(integrity_rows) >= MAX_INTEGRITY_ROWS:
            raise PreservationError("integrity-check result limit exceeded")
        integrity_rows.append(row[0])
    overall = hashlib.sha256()
    overall.update(bytes.fromhex(schema_digest))
    overall.update(bytes.fromhex(aggregate.hexdigest()))
    persistent_pragmas = {name: pragmas[name] for name in DIGEST_PRAGMAS}
    overall.update(bytes.fromhex(_canonical_json_digest(persistent_pragmas)))
    overall.update(bytes.fromhex(foreign_digest.hexdigest()))
    overall.update(bytes.fromhex(_canonical_json_digest(integrity_rows)))
    sequence_tables = {
        name: tables[name]
        for name in ("named_sequence", "sequence_entries", "sequence_group_state")
        if name in tables
    }
    return {
        "digest": overall.hexdigest(),
        "schema_digest": schema_digest,
        "pragmas": pragmas,
        "tables": tables,
        "sequence_tables": sequence_tables,
        "foreign_key_check": {
            "count": len(foreign_rows),
            "digest": foreign_digest.hexdigest(),
        },
        "integrity": integrity_rows,
        "elapsed_seconds": round(time.monotonic() - started, 6),
    }


def canonical_foreign_key_findings(connection):
    """Return complete typed FK findings for exact baseline comparison."""
    rows = []
    for row in connection.execute("PRAGMA foreign_key_check"):
        if len(rows) >= MAX_FOREIGN_KEY_FINDINGS:
            raise PreservationError("foreign-key finding limit exceeded")
        rows.append(row)
    return tuple(
        sorted(
            (tuple(canonical_sqlite_value(value) for value in row) for row in rows),
            key=lambda row: b"".join(row),
        )
    )


def fingerprint_database(path):
    diagnostic = raw_database_metadata(path)
    with readonly_database(path) as connection:
        logical = logical_database_fingerprint(connection)
    return {"logical": logical, "raw_metadata": diagnostic}


def fingerprint_and_clone_database(source, target):
    source = Path(source)
    target = Path(target)
    if target.exists():
        raise PreservationError(f"refusing to overwrite staged database: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    diagnostic = raw_database_metadata(source)
    with readonly_database(source) as connection:
        logical = logical_database_fingerprint(connection)
        destination = sqlite3.connect(target)
        try:
            connection.backup(destination)
            destination.commit()
        finally:
            destination.close()
    return {"logical": logical, "raw_metadata": diagnostic}


def fingerprint_and_clone_database_targets(source, targets):
    """Clone one pinned read-only SQLite view into every distinct target.

    The source transaction is intentionally scoped only to fingerprinting,
    backup, and verification of the backups.  It is closed before this helper
    returns so a later native scheduling run cannot hold the live WAL open.
    """
    source = Path(source)
    targets = [Path(item) for item in targets]
    if not targets or len({str(item.resolve(strict=False)) for item in targets}) != len(targets):
        raise PreservationError("database backup targets must be non-empty and distinct")
    for target in targets:
        if target.exists() or os.path.lexists(target):
            raise PreservationError(f"refusing to overwrite staged database: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
    diagnostic = raw_database_metadata(source)
    results = []
    try:
        with readonly_database(source) as connection:
            logical = logical_database_fingerprint(connection)
            for target in targets:
                destination = sqlite3.connect(target)
                try:
                    connection.backup(destination)
                    destination.commit()
                finally:
                    destination.close()
                with readonly_database(target) as backup:
                    copied = logical_database_fingerprint(backup)
                if copied["digest"] != logical["digest"]:
                    raise PreservationError(
                        f"staged database backup differs from pinned source view: {target}"
                    )
                results.append({"target": target, "logical": copied})
    except Exception:
        for target in targets:
            try:
                target.unlink()
            except FileNotFoundError:
                pass
        raise
    return {
        "logical": logical,
        "raw_metadata": diagnostic,
        "backups": results,
    }


def _xattrs(path, device, capabilities):
    if capabilities.get(device) is False:
        return {"supported": False, "values": []}
    try:
        names = os.listxattr(path, follow_symlinks=False)
    except OSError as exc:
        if exc.errno in UNSUPPORTED_XATTR_ERRNOS:
            capabilities[device] = False
            return {"supported": False, "values": []}
        raise PreservationError(f"cannot list xattrs for {os.fsdecode(path)}: {exc}") from exc
    capabilities.setdefault(device, True)
    values = []
    for name in sorted(names, key=os.fsencode):
        try:
            value = os.getxattr(path, name, follow_symlinks=False)
        except OSError as exc:
            if exc.errno in UNSUPPORTED_XATTR_ERRNOS:
                capabilities[device] = False
                return {"supported": False, "values": []}
            raise PreservationError(
                f"cannot read xattr {name!r} for {os.fsdecode(path)}: {exc}"
            ) from exc
        encoded_name = base64.urlsafe_b64encode(os.fsencode(name)).decode("ascii")
        values.append({"name_b64": encoded_name, "digest": hashlib.sha256(value).hexdigest()})
    return {"supported": True, "values": values}


def _file_record(path, relative, capabilities):
    try:
        before = os.lstat(path)
        target = os.readlink(path) if stat.S_ISLNK(before.st_mode) else None
        xattrs = _xattrs(path, before.st_dev, capabilities)
        after = os.lstat(path)
    except OSError as exc:
        raise PreservationError(f"cannot inspect {os.fsdecode(path)}: {exc}") from exc
    if _stat_fields(before) != _stat_fields(after):
        raise PreservationError(f"metadata changed during capture: {os.fsdecode(path)}")
    return {
        "path_b64": base64.urlsafe_b64encode(relative).decode("ascii"),
        **_stat_fields(after),
        "symlink_target_b64": (
            base64.urlsafe_b64encode(target).decode("ascii") if target is not None else None
        ),
        "xattrs": xattrs,
    }


@dataclass
class MediaManifest:
    stream: object
    summary: dict

    def close(self):
        self.stream.close()


def capture_media_manifest(root, spool_directory=None):
    started = time.monotonic()
    root_bytes = os.fsencode(Path(root).resolve(strict=True))
    stream = tempfile.TemporaryFile(mode="w+b", dir=spool_directory)
    digest = hashlib.sha256()
    capabilities = {}
    count = 0

    def emit(record):
        nonlocal count
        if count >= MAX_MEDIA_MANIFEST_ENTRIES:
            raise PreservationError("media manifest entry-count limit exceeded")
        line = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        stream.write(line)
        digest.update(line)
        count += 1

    def walk(path, relative):
        before = os.lstat(path)
        emit(_file_record(path, relative, capabilities))
        if not stat.S_ISDIR(before.st_mode):
            return
        try:
            with os.scandir(path) as iterator:
                entries = []
                for entry in iterator:
                    if len(entries) >= MAX_DIRECTORY_ENTRIES:
                        raise PreservationError("media directory entry-count limit exceeded")
                    entries.append(entry)
                entries.sort(key=lambda entry: entry.name)
        except OSError as exc:
            raise PreservationError(f"cannot traverse {os.fsdecode(path)}: {exc}") from exc
        for entry in entries:
            name = entry.name
            child_relative = name if not relative else relative + b"/" + name
            walk(entry.path, child_relative)
        after = os.lstat(path)
        if _stat_fields(before) != _stat_fields(after):
            raise PreservationError(f"directory changed during capture: {os.fsdecode(path)}")

    try:
        walk(root_bytes, b"")
        stream.flush()
        stream.seek(0)
        summary = {
            "digest": digest.hexdigest(),
            "entry_count": count,
            "xattr_capabilities": {
                str(device): supported for device, supported in sorted(capabilities.items())
            },
            "elapsed_seconds": round(time.monotonic() - started, 6),
        }
        return MediaManifest(stream, summary)
    except Exception:
        stream.close()
        raise


def compare_media_manifests(before, after, difference_limit=20):
    before.stream.seek(0)
    after.stream.seek(0)
    differences = []
    changed = before.summary["digest"] != after.summary["digest"]
    if changed:
        left = before.stream.readline()
        right = after.stream.readline()
        while (left or right) and len(differences) < difference_limit:
            left_record = json.loads(left) if left else None
            right_record = json.loads(right) if right else None
            left_key = left_record["path_b64"] if left_record else None
            right_key = right_record["path_b64"] if right_record else None
            left_order = (
                tuple(base64.urlsafe_b64decode(left_key).split(b"/"))
                if left_key is not None and left_key
                else ()
            )
            right_order = (
                tuple(base64.urlsafe_b64decode(right_key).split(b"/"))
                if right_key is not None and right_key
                else ()
            )
            if right_record is None or (
                left_record is not None and left_order < right_order
            ):
                differences.append({"path_b64": left_key, "change": "removed"})
                left = before.stream.readline()
            elif left_record is None or right_order < left_order:
                differences.append({"path_b64": right_key, "change": "added"})
                right = after.stream.readline()
            else:
                if left_record != right_record:
                    differences.append({"path_b64": left_key, "change": "metadata"})
                left = before.stream.readline()
                right = after.stream.readline()
    return {
        "preserved": not changed,
        "before": before.summary,
        "after": after.summary,
        "differences": differences,
        "differences_truncated": changed and len(differences) >= difference_limit,
    }


def fingerprint_json_files(paths):
    started = time.monotonic()
    entries = {}
    if len(paths) > MAX_PROTECTED_JSON_FILES:
        raise PreservationError("protected JSON file-count limit exceeded")
    aggregate_bytes = 0
    for logical_name, path in sorted(paths.items()):
        path = Path(path)
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as exc:
            raise PreservationError(f"cannot open protected file {path}: {exc}") from exc
        try:
            before = os.fstat(descriptor)
            chunks = []
            file_bytes = 0
            while True:
                chunk = os.read(
                    descriptor,
                    min(65536, MAX_PROTECTED_JSON_FILE_BYTES + 1 - file_bytes),
                )
                if not chunk:
                    break
                chunks.append(chunk)
                file_bytes += len(chunk)
                if file_bytes > MAX_PROTECTED_JSON_FILE_BYTES:
                    raise PreservationError(
                        f"protected JSON file exceeds size limit: {logical_name}"
                    )
            aggregate_bytes += file_bytes
            if aggregate_bytes > MAX_PROTECTED_JSON_TOTAL_BYTES:
                raise PreservationError("protected JSON aggregate size limit exceeded")
            raw = b"".join(chunks)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if _stat_fields(before) != _stat_fields(after):
            raise PreservationError(f"protected file changed during capture: {path}")
        try:
            parsed = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise PreservationError(f"protected JSON is invalid: {path}: {exc}") from exc
        entries[logical_name] = {
            **_stat_fields(after),
            "byte_digest": hashlib.sha256(raw).hexdigest(),
            "json_digest": _canonical_json_digest(parsed),
        }
    aggregate = _canonical_json_digest(entries)
    return {
        "digest": aggregate,
        "file_count": len(entries),
        "files": entries,
        "elapsed_seconds": round(time.monotonic() - started, 6),
    }


def protected_json_paths(root):
    root = Path(root)
    paths = {}
    config_paths = []
    for path in (root / "confs").glob("*.json"):
        if len(config_paths) >= MAX_PROTECTED_JSON_FILES - 1:
            raise PreservationError("protected JSON file-count limit exceeded")
        config_paths.append(path)
    for path in sorted(config_paths):
        paths[f"confs/{path.name}"] = path
    state = root / "runtime/watch_in_order_state.json"
    paths["runtime/watch_in_order_state.json"] = state
    return paths


def compare_database_fingerprints(before, after):
    left = before["logical"]
    right = after["logical"]
    changed_tables = []
    for name in sorted(set(left["tables"]) | set(right["tables"])):
        if left["tables"].get(name) != right["tables"].get(name):
            changed_tables.append(
                {"table": name, "before": left["tables"].get(name), "after": right["tables"].get(name)}
            )
    def summary(fingerprint):
        logical = fingerprint["logical"]
        return {
            "digest": logical["digest"],
            "schema_digest": logical["schema_digest"],
            "table_count": len(logical["tables"]),
            "row_count": sum(
                table["row_count"] for table in logical["tables"].values()
            ),
            "foreign_key_issue_count": logical["foreign_key_check"]["count"],
            "elapsed_seconds": logical["elapsed_seconds"],
        }

    return {
        "preserved": left["digest"] == right["digest"],
        "before": summary(before),
        "after": summary(after),
        "changed_tables": changed_tables,
        "schema_changed": left["schema_digest"] != right["schema_digest"],
        "raw_metadata_before": before["raw_metadata"],
        "raw_metadata_after": after["raw_metadata"],
    }


def compare_json_fingerprints(before, after):
    differences = []
    for name in sorted(set(before["files"]) | set(after["files"])):
        if before["files"].get(name) != after["files"].get(name):
            differences.append(name)
    return {
        "preserved": before["digest"] == after["digest"],
        "before": {
            "digest": before["digest"],
            "file_count": before["file_count"],
            "elapsed_seconds": before["elapsed_seconds"],
        },
        "after": {
            "digest": after["digest"],
            "file_count": after["file_count"],
            "elapsed_seconds": after["elapsed_seconds"],
        },
        "differences": differences,
    }
