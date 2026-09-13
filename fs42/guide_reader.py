"""Bounded, read-only guide validation for a staged native schedule."""

import hashlib
import json
import os
import shutil
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import islice
from pathlib import Path
from urllib.parse import quote

from fs42.catalog_entry import CatalogEntry
from fs42.guide_payloads import build_channels_payload, iter_listing_projection
from fs42.liquid_io import LiquidIO
from fs42.metadata_io import MetadataIO
from fs42.title_parser import TitleParser
from station_director.preservation import logical_database_fingerprint


GUIDE_FORMAT_VERSION = 1
GUIDE_ARTIFACT_IDENTITY = "guide/guide-v1.records"
GUIDE_SNAPSHOT_IDENTITY = "guide-input/guide.db"
GUIDE_MAGIC = b"FS42-GUIDE\x00\x01"
MAX_GUIDE_RECORD_BYTES = 512 * 1024
MAX_GUIDE_STREAM_BYTES = 64 * 1024 * 1024
MAX_GUIDE_RECORDS = 100000
MAX_GUIDE_BOUNDARY_SUMMARIES = 16
MAX_GUIDE_CHANNELS = 64
MAX_GUIDE_BLOCKS = 100000
MAX_BLOCKS_PER_STATION = 25000
GUIDE_BATCH_BLOCKS = 32
MAX_CATALOG_REFERENCES_PER_BLOCK = 64
MAX_CATALOG_REFERENCES_PER_BATCH = 128
MAX_SQLITE_FIELD_BYTES = 256 * 1024
MAX_SQLITE_ROW_BYTES = 512 * 1024
MAX_JSON_BYTES = 192 * 1024
MAX_JSON_DEPTH = 32
MAX_JSON_MEMBERS = 10000
MAX_JSON_ARRAY_ITEMS = 512
MAX_JSON_STRING_BYTES = 64 * 1024
MAX_METADATA_BATCH_BYTES = 4 * 1024 * 1024
MAX_BOUNDARY_PROBES = 200000
SQLITE_CACHE_KIB = 8192


class GuideLoadingError(RuntimeError):
    pass


class GuideValidationError(RuntimeError):
    pass


class GuideArtifactError(RuntimeError):
    pass


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _bounded_text(value, label, *, maximum=MAX_SQLITE_FIELD_BYTES):
    if not isinstance(value, str):
        raise GuideLoadingError(f"{label} must be SQLite text")
    if len(value.encode("utf-8")) > maximum:
        raise GuideLoadingError(f"{label} exceeds the byte limit")
    return value


def _bounded_row(row, label):
    total = 0
    for value in row:
        if isinstance(value, str):
            size = len(value.encode("utf-8"))
        elif isinstance(value, bytes):
            size = len(value)
        else:
            size = 16
        if size > MAX_SQLITE_FIELD_BYTES:
            raise GuideLoadingError(f"{label} contains an oversized field")
        total += size
        if total > MAX_SQLITE_ROW_BYTES:
            raise GuideLoadingError(f"{label} exceeds the row byte limit")


def _validate_json_shape(value, label):
    stack = [(value, 1)]
    members = 0
    while stack:
        current, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            raise GuideLoadingError(f"{label} exceeds the JSON depth limit")
        if isinstance(current, dict):
            members += len(current)
            if members > MAX_JSON_MEMBERS:
                raise GuideLoadingError(f"{label} exceeds the JSON member limit")
            stack.extend((item, depth + 1) for item in current.values())
            for key in current:
                if not isinstance(key, str) or len(key.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                    raise GuideLoadingError(f"{label} contains an invalid JSON key")
        elif isinstance(current, list):
            if len(current) > MAX_JSON_ARRAY_ITEMS:
                raise GuideLoadingError(f"{label} exceeds the JSON array limit")
            members += len(current)
            if members > MAX_JSON_MEMBERS:
                raise GuideLoadingError(f"{label} exceeds the JSON member limit")
            stack.extend((item, depth + 1) for item in current)
        elif isinstance(current, str):
            if len(current.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                raise GuideLoadingError(f"{label} contains an oversized string")
        elif current is None or isinstance(current, (bool, int, float)):
            if isinstance(current, float) and not (-float("inf") < current < float("inf")):
                raise GuideLoadingError(f"{label} contains a non-finite number")
        else:
            raise GuideLoadingError(f"{label} contains an unsupported value")
    return value


def _bounded_json(raw, label, *, allow_none=True):
    if raw is None and allow_none:
        return None
    raw = _bounded_text(raw, label, maximum=MAX_JSON_BYTES)
    try:
        value = json.loads(raw, parse_constant=lambda item: (_ for _ in ()).throw(
            GuideLoadingError(f"{label} contains non-finite number {item}")
        ))
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise GuideLoadingError(f"malformed {label}") from exc
    return _validate_json_shape(value, label)


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_nlink, stat.S_IMODE(info.st_mode), info.st_uid)


def _file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sidecars(database):
    database = Path(database)
    result = {}
    for suffix in ("-wal", "-shm", "-journal"):
        path = database.parent / (database.name + suffix)
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise GuideLoadingError("working database has an unsafe sidecar")
            result[suffix] = {"identity": _identity(info), "digest": _file_digest(path)}
    return result


def _readonly_connection(path, *, immutable=False):
    query = "?mode=ro" + ("&immutable=1" if immutable else "")
    uri = "file:" + quote(str(Path(path).absolute()), safe="/") + query
    connection = sqlite3.connect(uri, uri=True)
    try:
        if hasattr(connection, "setlimit") and hasattr(sqlite3, "SQLITE_LIMIT_LENGTH"):
            connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_SQLITE_FIELD_BYTES)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_KIB}")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise GuideLoadingError("guide SQLite connection is not query-only")
        if connection.execute("PRAGMA temp_store").fetchone() != (1,):
            raise GuideLoadingError("guide SQLite temporary storage is not file-backed")
        if connection.execute("PRAGMA cache_size").fetchone() != (-SQLITE_CACHE_KIB,):
            raise GuideLoadingError("guide SQLite cache limit was not established")
        return connection
    except Exception:
        connection.close()
        raise


def _logical_fingerprint(path, *, immutable=False):
    connection = _readonly_connection(path, immutable=immutable)
    try:
        connection.execute("BEGIN")
        return logical_database_fingerprint(connection)
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


@dataclass(frozen=True)
class GuideSnapshot:
    path: Path
    directory: Path
    file_identity: tuple
    directory_identity: tuple
    logical_digest: str
    working_logical_digest: str
    working_file_identity: tuple
    working_sidecars: dict


def prepare_guide_snapshot(working_database, stage):
    """Create one immutable SQLite backup without retaining a source transaction."""
    working_database = Path(working_database)
    directory = Path(stage) / "guide-input"
    capture_directory = Path(stage) / "guide-capture"
    captured_database = capture_directory / "working.db"
    target = Path(stage) / GUIDE_SNAPSHOT_IDENTITY
    directory.mkdir(mode=0o700, exist_ok=False)
    capture_directory.mkdir(mode=0o700, exist_ok=False)
    source_descriptor = target_descriptor = None
    source = destination = None
    try:
        source_descriptor = os.open(working_database, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        source_before = os.fstat(source_descriptor)
        if not stat.S_ISREG(source_before.st_mode) or source_before.st_nlink != 1:
            raise GuideLoadingError("working guide database is not a private regular file")
        sidecars_before = _sidecars(working_database)
        shutil.copyfile(working_database, captured_database, follow_symlinks=False)
        os.chmod(captured_database, 0o600)
        for suffix in sidecars_before:
            captured_sidecar = Path(str(captured_database) + suffix)
            shutil.copyfile(Path(str(working_database) + suffix), captured_sidecar,
                            follow_symlinks=False)
            os.chmod(captured_sidecar, 0o600)
        if (_identity(source_before) != _identity(os.fstat(source_descriptor))
                or _identity(source_before) != _identity(working_database.lstat())):
            raise GuideLoadingError("working database changed during guide capture")
        if sidecars_before != _sidecars(working_database):
            raise GuideLoadingError("working database sidecars changed during guide capture")
        source = _readonly_connection(captured_database)
        source.execute("BEGIN")
        source_logical = logical_database_fingerprint(source)
        if target.exists() or target.is_symlink():
            raise GuideLoadingError("guide snapshot target already exists")
        destination = sqlite3.connect(target)
        destination.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_KIB}")
        source.backup(destination)
        destination.commit()
        destination.close()
        destination = None
        source.rollback()
        source.close()
        source = None
        _remove_private_tree(capture_directory)
        target_descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        target_info = os.fstat(target_descriptor)
        if not stat.S_ISREG(target_info.st_mode) or target_info.st_nlink != 1:
            raise GuideLoadingError("guide snapshot is not a private regular file")
        snapshot_logical = _logical_fingerprint(target, immutable=True)
        if snapshot_logical["digest"] != source_logical["digest"]:
            raise GuideLoadingError("guide snapshot logical fingerprint mismatch")
        if set(item.name for item in directory.iterdir()) != {target.name}:
            raise GuideLoadingError("guide snapshot directory contains unexpected files")
        os.chmod(target, 0o400)
        os.chmod(directory, 0o500)
        target_info = target.lstat()
        directory_info = directory.lstat()
        if (stat.S_IMODE(target_info.st_mode) != 0o400
                or stat.S_IMODE(directory_info.st_mode) != 0o500
                or target_info.st_uid != os.geteuid()
                or directory_info.st_uid != os.geteuid()):
            raise GuideLoadingError("guide snapshot permissions are unsafe")
        return GuideSnapshot(target, directory, _identity(target_info),
                             _identity(directory_info), snapshot_logical["digest"],
                             source_logical["digest"], _identity(source_before),
                             sidecars_before)
    except Exception:
        try:
            os.chmod(directory, 0o700)
            if target.exists() and not target.is_symlink():
                os.chmod(target, 0o600)
                target.unlink()
            directory.rmdir()
        except OSError:
            pass
        _remove_private_tree(capture_directory)
        raise
    finally:
        if destination is not None:
            destination.close()
        if source is not None:
            if source.in_transaction:
                source.rollback()
            source.close()
        if target_descriptor is not None:
            os.close(target_descriptor)
        if source_descriptor is not None:
            os.close(source_descriptor)


def verify_guide_snapshot(snapshot, working_database):
    working_info = Path(working_database).lstat()
    if _identity(working_info) != snapshot.working_file_identity:
        raise GuideValidationError("working database identity changed during guide validation")
    if _sidecars(working_database) != snapshot.working_sidecars:
        raise GuideValidationError("working database sidecars changed during guide validation")
    file_info = snapshot.path.lstat()
    directory_info = snapshot.directory.lstat()
    if (_identity(file_info) != snapshot.file_identity
            or _identity(directory_info) != snapshot.directory_identity
            or stat.S_IMODE(file_info.st_mode) != 0o400
            or stat.S_IMODE(directory_info.st_mode) != 0o500
            or set(item.name for item in snapshot.directory.iterdir()) != {snapshot.path.name}):
        raise GuideValidationError("guide snapshot identity, permissions, or contents changed")
    for suffix in ("-wal", "-shm", "-journal"):
        if (snapshot.directory / (snapshot.path.name + suffix)).exists():
            raise GuideValidationError("guide snapshot acquired a SQLite sidecar")
    return snapshot.logical_digest


def fingerprint_guide_snapshot(snapshot):
    logical = _logical_fingerprint(snapshot.path, immutable=True)
    if logical["digest"] != snapshot.logical_digest:
        raise GuideValidationError("guide snapshot logical fingerprint changed")
    return logical["digest"]


class GuideStreamWriter:
    def __init__(self, stage):
        self.root = Path(stage)
        self.directory = self.root / "guide"
        self.path = self.root / GUIDE_ARTIFACT_IDENTITY
        self.directory_fd = self.descriptor = None
        self.digest = hashlib.sha256()
        self.count = self.byte_count = 0
        self.seen = set()

    def __enter__(self):
        self.directory.mkdir(mode=0o700, exist_ok=False)
        try:
            self.directory_fd = os.open(
                self.directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            info = os.fstat(self.directory_fd)
            if (not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700
                    or info.st_uid != os.geteuid() or os.listdir(self.directory_fd)):
                raise GuideArtifactError("guide artifact directory is unsafe")
            self.descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_EXCL
                                      | getattr(os, "O_NOFOLLOW", 0), 0o600)
            self.directory_identity = _identity(os.fstat(self.directory_fd))
            self._write(GUIDE_MAGIC)
            return self
        except Exception:
            if self.descriptor is not None:
                os.close(self.descriptor)
                self.descriptor = None
            if self.directory_fd is not None:
                os.close(self.directory_fd)
                self.directory_fd = None
            try:
                self.path.unlink()
                self.directory.rmdir()
            except OSError:
                pass
            raise

    def _write(self, raw):
        view = memoryview(raw)
        while view:
            written = os.write(self.descriptor, view)
            if written <= 0:
                raise GuideArtifactError("guide artifact write made no progress")
            view = view[written:]
        self.digest.update(raw)
        self.byte_count += len(raw)

    def record(self, record_path, value):
        if (not isinstance(record_path, str) or not record_path.startswith("/guide/")
                or len(record_path.encode("utf-8")) > 1024):
            raise GuideArtifactError("invalid guide record path")
        if record_path in self.seen:
            raise GuideArtifactError("duplicate guide record path")
        self.seen.add(record_path)
        if len(self.seen) > MAX_GUIDE_RECORDS:
            raise GuideArtifactError("guide record-count limit exceeded")
        raw = _canonical({"path": record_path, "value": value})
        if len(raw) > MAX_GUIDE_RECORD_BYTES:
            raise GuideArtifactError("guide record exceeds size limit")
        frame = len(raw).to_bytes(8, "big") + raw
        if self.byte_count + len(frame) > MAX_GUIDE_STREAM_BYTES:
            raise GuideArtifactError("guide stream exceeds aggregate limit")
        self._write(frame)
        self.count += 1

    def finish(self):
        os.fsync(self.descriptor)
        info = os.fstat(self.descriptor)
        current = self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.geteuid()
                or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
                or current.st_size != self.byte_count):
            raise GuideArtifactError("guide artifact identity or permissions changed")
        os.lseek(self.descriptor, 0, os.SEEK_SET)
        verified = hashlib.sha256()
        remaining = self.byte_count
        while remaining:
            chunk = os.read(self.descriptor, min(65536, remaining))
            if not chunk:
                raise GuideArtifactError("guide artifact is truncated during verification")
            verified.update(chunk)
            remaining -= len(chunk)
        if verified.hexdigest() != self.digest.hexdigest():
            raise GuideArtifactError("guide artifact digest revalidation failed")
        if (_identity(os.fstat(self.directory_fd)) != self.directory_identity
                or os.listdir(self.directory_fd) != [self.path.name]):
            raise GuideArtifactError("guide artifact directory changed")
        os.fsync(self.directory_fd)
        return {"format_version": GUIDE_FORMAT_VERSION,
                "artifact_identity": GUIDE_ARTIFACT_IDENTITY,
                "digest": self.digest.hexdigest(), "record_count": self.count,
                "byte_count": self.byte_count}

    def __exit__(self, exc_type, exc, traceback):
        if self.descriptor is not None:
            os.close(self.descriptor)
        if self.directory_fd is not None:
            os.close(self.directory_fd)
        if exc_type is not None:
            try:
                self.path.unlink()
                self.directory.rmdir()
            except OSError:
                pass


def _index_connection(stage):
    directory = Path(stage) / "guide-work"
    directory.mkdir(mode=0o700, exist_ok=False)
    path = directory / "intervals.sqlite"
    connection = None
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(descriptor)
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_KIB}")
        connection.executescript("""
            CREATE TABLE expected(station TEXT, block_id INTEGER, start TEXT, end TEXT,
                                  title TEXT, PRIMARY KEY(station,block_id));
            CREATE TABLE actual(station TEXT, block_id INTEGER, start TEXT, end TEXT,
                                title TEXT, PRIMARY KEY(station,block_id));
            CREATE TABLE probes(station TEXT, instant TEXT, PRIMARY KEY(station,instant));
            CREATE INDEX expected_lookup ON expected(station,start,end);
            CREATE INDEX actual_lookup ON actual(station,start,end);
        """)
        return directory, connection
    except Exception:
        if connection is not None:
            connection.close()
        _remove_private_tree(directory)
        raise


def _remove_private_tree(directory):
    if directory is not None:
        try:
            shutil.rmtree(directory)
        except FileNotFoundError:
            pass


def _row_json_fields(row):
    values = {}
    for index, label in ((7, "sequence_key"), (8, "break_info"),
                         (9, "content_json"), (10, "plan_json")):
        values[label] = _bounded_json(row[index], label, allow_none=True)
    content = values["content_json"]
    references = [] if content is None else content if isinstance(content, list) else [content]
    if len(references) > MAX_CATALOG_REFERENCES_PER_BLOCK:
        raise GuideLoadingError("guide block catalog-reference limit exceeded")
    for value in references:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise GuideLoadingError("guide catalog references must be positive integers")
    return references


def _catalog_cache(connection, identifiers):
    if len(identifiers) > MAX_CATALOG_REFERENCES_PER_BATCH:
        raise GuideLoadingError("guide catalog batch limit exceeded")
    identifiers = sorted(set(identifiers))
    cache = {}
    for offset in range(0, len(identifiers), 500):
        chunk = identifiers[offset:offset + 500]
        placeholders = ",".join("?" for unused in chunk)
        for row in connection.execute(
                f"SELECT * FROM catalog_entries WHERE id IN ({placeholders}) ORDER BY id", chunk):
            _bounded_row(row, "catalog row")
            _bounded_json(row[7], "catalog hints", allow_none=True)
            entry = CatalogEntry.from_db_row(row)
            if entry.dbid in cache:
                raise GuideLoadingError("duplicate guide catalog identifier")
            cache[entry.dbid] = entry
    if set(identifiers) != set(cache):
        raise GuideLoadingError("guide schedule contains an unresolved catalog reference")
    return cache


def _attach_metadata_batch(connection, blocks):
    paths = []
    for block in blocks:
        content = getattr(block, "content", None)
        if content is not None and not isinstance(content, list):
            path = getattr(content, "path", None)
            if path:
                paths.append(path)
    if not paths:
        return {}
    by_real = MetadataIO.paths_by_real(paths)
    rows = []
    total = 0
    real_paths = sorted(by_real)
    for offset in range(0, len(real_paths), 500):
        chunk = real_paths[offset:offset + 500]
        placeholders = ",".join("?" for unused in chunk)
        for row in connection.execute(
                f"SELECT path,meta FROM file_meta WHERE path IN ({placeholders}) ORDER BY path", chunk):
            _bounded_row(row, "metadata row")
            total += sum(len(item.encode("utf-8")) for item in row if isinstance(item, str))
            if total > MAX_METADATA_BATCH_BYTES:
                raise GuideLoadingError("guide metadata batch byte limit exceeded")
            _bounded_json(row[1], "guide metadata", allow_none=True)
            rows.append(row)
    metadata = MetadataIO.decode_rows_by_real(by_real, rows)
    for block in blocks:
        content = getattr(block, "content", None)
        path = None if content is None or isinstance(content, list) else getattr(content, "path", None)
        if path and path in metadata:
            block.meta = metadata[path]
    return metadata


def _add_probe(index, station, instant, counter):
    before = index.total_changes
    index.execute("INSERT OR IGNORE INTO probes(station,instant) VALUES(?,?)",
                  (station, instant.isoformat()))
    if index.total_changes != before:
        counter[0] += 1
        if counter[0] > MAX_BOUNDARY_PROBES:
            raise GuideValidationError("guide boundary-probe limit exceeded")


def _matches(index, table, station, instant):
    return list(index.execute(
        f"SELECT block_id,start,end,title FROM {table} WHERE station=? AND start<=? AND end>? "
        "ORDER BY block_id LIMIT 2", (station, instant.isoformat(), instant.isoformat())
    ))


def stream_validate_staged_guide(snapshot, stage, stations, affected_channels,
                                 proposal_start, proposal_end, *, normalize_titles=True,
                                 title_patterns=()):
    station_list = list(islice(iter(stations), MAX_GUIDE_CHANNELS + 1))
    if len(station_list) > MAX_GUIDE_CHANNELS:
        raise GuideLoadingError("guide station limit exceeded")
    names = [item.get("network_name") for item in station_list]
    if any(not isinstance(name, str) or not name or len(name.encode("utf-8")) > 256
           for name in names):
        raise GuideLoadingError("guide contains an invalid station identity")
    if len(names) != len(set(names)):
        raise GuideValidationError("guide contains duplicate network names")
    channels_payload = build_channels_payload(station_list)
    for item in channels_payload["channels"]:
        if (not isinstance(item["channel_number"], int)
                or isinstance(item["channel_number"], bool)
                or not isinstance(item["network_long_name"], str)
                or len(item["network_long_name"]) > 1000
                or not isinstance(item["hidden"], bool)
                or not isinstance(item["has_schedule"], bool)):
            raise GuideLoadingError("guide channel payload exceeds its type or size limits")
    channels_by_name = {item["network_name"]: item for item in channels_payload["channels"]}
    for channel in affected_channels:
        item = channels_by_name.get(channel["name"])
        if item is None or item["channel_number"] != channel["number"]:
            raise GuideValidationError("wrong or missing guide channel identity")
        if item["has_schedule"] is not True:
            raise GuideValidationError("affected guide channel is not scheduled")
    start = min([proposal_start, *(datetime.fromisoformat(item["regeneration_start"])
                                  for item in affected_channels)]) - timedelta(microseconds=1)
    end = max([proposal_end, *(datetime.fromisoformat(item["effective_horizon"])
                              for item in affected_channels)]) + timedelta(microseconds=1)
    connection = _readonly_connection(snapshot.path, immutable=True)
    work_directory = index = None
    try:
        work_directory, index = _index_connection(stage)
        station_rows = []
        for row in connection.execute(
                "SELECT DISTINCT station FROM liquid_blocks WHERE start_time < ? AND end_time > ? "
                "ORDER BY station", (end.isoformat(sep=" "), start.isoformat(sep=" "))):
            _bounded_row(row, "station identity")
            station_rows.append(row[0])
            if len(station_rows) > MAX_GUIDE_CHANNELS:
                raise GuideLoadingError("guide schedule station limit exceeded")
        if any(name not in channels_by_name for name in station_rows):
            raise GuideValidationError("guide schedule uses an unknown station")
        summaries = []
        total_blocks = 0
        probe_count = [0]
        with GuideStreamWriter(stage) as writer:
            writer.record("/guide/channels", channels_payload)
            writer.record("/guide/query", {"start": start.isoformat(), "end": end.isoformat()})
            writer.record("/guide/schedule-station-order", station_rows)
            for station in station_rows:
                cursor = connection.execute(
                    "SELECT * FROM liquid_blocks WHERE station=? AND start_time < ? AND end_time > ? "
                    "ORDER BY start_time,end_time,id",
                    (station, end.isoformat(sep=" "), start.isoformat(sep=" ")),
                )
                station_count = ordinal = 0
                while True:
                    rows = cursor.fetchmany(GUIDE_BATCH_BLOCKS)
                    if not rows:
                        break
                    references = []
                    for row in rows:
                        _bounded_row(row, "schedule row")
                        if row[1] != station or isinstance(row[0], bool) or not isinstance(row[0], int):
                            raise GuideLoadingError("guide schedule row identity is invalid")
                        raw_start = datetime.fromisoformat(_bounded_text(row[3], "schedule start"))
                        raw_end = datetime.fromisoformat(_bounded_text(row[4], "schedule end"))
                        if raw_end <= raw_start:
                            raise GuideValidationError("guide schedule contains an invalid interval")
                        references.extend(_row_json_fields(row))
                        expected_title = (
                            TitleParser.parse_title(row[6], title_patterns)
                            if normalize_titles else row[6]
                        )
                        index.execute(
                            "INSERT INTO expected(station,block_id,start,end,title) VALUES(?,?,?,?,?)",
                            (station, row[0], raw_start.isoformat(), raw_end.isoformat(),
                             expected_title),
                        )
                        for instant in (raw_start, raw_end):
                            for probe in (instant - timedelta(microseconds=1), instant,
                                          instant + timedelta(microseconds=1)):
                                _add_probe(index, station, probe, probe_count)
                    cache = _catalog_cache(connection, references)
                    blocks = LiquidIO.blocks_from_rows(rows, cache,
                                                       normalize_titles=normalize_titles,
                                                       title_patterns=title_patterns)
                    expected_metadata = _attach_metadata_batch(connection, blocks)
                    for row, block, listing in zip(
                            rows, blocks, iter_listing_projection(blocks, True)):
                        raw_start = datetime.fromisoformat(row[3])
                        raw_end = datetime.fromisoformat(row[4])
                        if (datetime.fromisoformat(listing["start_time"]) != raw_start
                                or datetime.fromisoformat(listing["end_time"]) != raw_end):
                            raise GuideValidationError("guide projection changed a schedule interval")
                        if (not isinstance(listing.get("title"), str)
                                or len(listing["title"]) > 1000):
                            raise GuideLoadingError("guide title exceeds its type or size limit")
                        content = getattr(block, "content", None)
                        content_path = (None if content is None or isinstance(content, list)
                                        else getattr(content, "path", None))
                        expected_meta = expected_metadata.get(content_path)
                        if (("meta" in listing) != bool(expected_meta)
                                or (expected_meta and listing["meta"] != expected_meta)):
                            raise GuideValidationError("guide metadata projection mismatch")
                        index.execute("INSERT INTO actual(station,block_id,start,end,title) VALUES(?,?,?,?,?)",
                                      (station, row[0], raw_start.isoformat(), raw_end.isoformat(),
                                       listing["title"]))
                        writer.record(
                            f"/guide/schedules/{quote(station, safe='')}/{ordinal:08d}", listing
                        )
                        ordinal += 1
                    station_count += len(rows)
                    total_blocks += len(rows)
                    if station_count > MAX_BLOCKS_PER_STATION or total_blocks > MAX_GUIDE_BLOCKS:
                        raise GuideLoadingError("guide block limit exceeded")
                    del blocks, cache, references, rows
            for channel in affected_channels:
                station = channel["name"]
                named_instants = {}
                for label, instant in (
                    ("proposal_start", proposal_start),
                    ("regeneration_seam", datetime.fromisoformat(channel["regeneration_start"])),
                    ("proposal_end", proposal_end),
                    ("effective_horizon", datetime.fromisoformat(channel["effective_horizon"])),
                ):
                    for suffix, probe in (("before", instant - timedelta(microseconds=1)),
                                          ("at", instant),
                                          ("after", instant + timedelta(microseconds=1))):
                        named_instants.setdefault(probe.isoformat(), []).append(
                            f"{label}.{suffix}"
                        )
                        _add_probe(index, station, probe, probe_count)
                zero = one = checked = 0
                named_results = []
                for (instant_raw,) in index.execute(
                        "SELECT instant FROM probes WHERE station=? ORDER BY instant", (station,)):
                    instant = datetime.fromisoformat(instant_raw)
                    expected = _matches(index, "expected", station, instant)
                    actual = _matches(index, "actual", station, instant)
                    if len(expected) > 1 or len(actual) > 1:
                        raise GuideValidationError("multiple guide entries resolve at a boundary")
                    if expected != actual:
                        raise GuideValidationError("guide boundary resolution differs from raw schedule")
                    checked += 1
                    zero += not actual
                    one += bool(actual)
                    if instant_raw in named_instants:
                        title = None if not actual else index.execute(
                            "SELECT title FROM actual WHERE station=? AND block_id=?",
                            (station, actual[0][0]),
                        ).fetchone()[0]
                        for boundary_name in named_instants[instant_raw]:
                            named_results.append({"name": boundary_name,
                                                  "instant": instant_raw,
                                                  "match_count": len(actual), "title": title})
                listing_count = index.execute(
                    "SELECT COUNT(*) FROM actual WHERE station=?", (station,)
                ).fetchone()[0]
                configured = channels_by_name[station]
                summary = {"number": channel["number"], "name": station,
                           "network_long_name": configured["network_long_name"],
                           "hidden": configured["hidden"],
                           "has_schedule": configured["has_schedule"],
                           "listing_count": listing_count, "transition_probe_count": checked,
                           "zero_match_count": zero, "one_match_count": one,
                           "named_boundaries": sorted(named_results,
                                                       key=lambda item: item["instant"])[
                                                           :MAX_GUIDE_BOUNDARY_SUMMARIES],
                           "named_boundaries_truncated": len(named_results)
                           > MAX_GUIDE_BOUNDARY_SUMMARIES}
                summaries.append(summary)
                writer.record(f"/guide/summary/{channel['number']:02d}", summary)
            artifact = writer.finish()
        return artifact, summaries
    finally:
        if index is not None:
            index.close()
        connection.close()
        _remove_private_tree(work_directory)


def write_guide_stream(stage, records):
    """Compatibility helper for bounded synthetic artifact tests."""
    with GuideStreamWriter(stage) as writer:
        for path, value in records:
            writer.record(path, value)
        return writer.finish()
