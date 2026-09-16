"""Externally gated, resumable chapter-cache maintenance.

The default command is a read-only plan.  The execution path is deliberately
separate from scheduling and only becomes reachable after explicit external
admission and exact count confirmation.
"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import datetime
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE = PROJECT_ROOT / "runtime/fs42_fluid.db"
MEDIA_ROOT = Path("/mnt/t7/CRT-Media")
BACKUP_ROOT = PROJECT_ROOT / "runtime/director/chapter-cache-backups"
MAINTENANCE_LOCK = ".chapter-cache-maintenance.lock"
BASELINE_PIN = "migration-baseline.v1.json"
PROGRESS_FINAL = "warmup-progress.v1.json"
PROGRESS_PENDING = ".warmup-progress.v1.pending"
BACKUP_RE = re.compile(r"chapter-cache-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}\.sqlite3")
MAX_UNPINNED_BACKUPS = 4
MAX_BACKUPS = 1 + MAX_UNPINNED_BACKUPS
MAX_TRANSITION_BACKUPS = MAX_BACKUPS + 1
MIN_FREE_BYTES = 100 * 1024 * 1024
MAX_PROBE_FAILURES = 16
MAX_PROGRESS_IDENTITIES = 10_000
MAX_PROGRESS_BYTES = 64 * 1024
PROBE_FAILURE_CATEGORIES = (
    "probe_launch_failed", "probe_timeout", "probe_nonzero", "probe_signaled",
    "probe_output_too_large", "probe_output_invalid", "chapter_data_invalid",
    "probe_cleanup_failed",
)
ANALYSIS_TIMEOUT_SECONDS = 30
QUESTIONABLE_ATTEMPT_SECONDS = ANALYSIS_TIMEOUT_SECONDS + 1
SERVICE_COMMAND_TIMEOUT_SECONDS = 30
IN_FLIGHT_OPERATION_ALLOWANCE_SECONDS = (
    ANALYSIS_TIMEOUT_SECONDS + SERVICE_COMMAND_TIMEOUT_SECONDS
)
POST_WRITE_VERIFICATION_ALLOWANCE_SECONDS = 120
SERVICE_FINALIZATION_ALLOWANCE_SECONDS = 2 * SERVICE_COMMAND_TIMEOUT_SECONDS
FINALIZATION_SECONDS = (
    IN_FLIGHT_OPERATION_ALLOWANCE_SECONDS
    + POST_WRITE_VERIFICATION_ALLOWANCE_SECONDS
    + SERVICE_FINALIZATION_ALLOWANCE_SECONDS
)
CONTROL_SECONDS = 7200
ADMISSION_SECONDS = CONTROL_SECONDS - FINALIZATION_SECONDS
EXPECTED_KEYS = (
    "eligible", "missing", "legacy_empty", "current_empty",
    "unavailable_empty", "legacy_nonempty", "trusted_legacy_nonempty",
    "re_attestation_required", "unavailable_nonempty",
    "orphan_retirements", "attestations", "probes", "short_media",
    "versioned",
)
VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mpg", ".mpeg", ".avi", ".mov", ".mkv", ".ts", ".m4v", ".webm", ".wmv"}
)
AUDIO_EXTENSIONS = frozenset(
    {".mp3", ".m4a", ".flac", ".wav", ".aac", ".ogg", ".opus", ".wma"}
)
DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


class MaintenanceError(RuntimeError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    mode: int
    uid: int
    links: int
    size: int
    mtime_ns: int
    ctime_ns: int
    digest: str


@dataclass(frozen=True)
class PrivateGeneration:
    directory: Path
    database: Path
    raw_database: Path
    live_identity: tuple


@dataclass(frozen=True)
class MediaItem:
    relative: tuple
    size: int
    mtime_ns: int
    mtime: float | None = None


def _fixed_failure(code):
    allowed = {
        "invocation_context_rejected", "invalid_proposal_id", "execution_confirmation_missing",
        "expected_counts_invalid", "expected_counts_mismatch", "planning_journal_present",
        "planning_sidecar_inconsistent", "planning_state_changed", "database_identity_unsafe",
        "hot_journal_present", "validation_lock_busy", "maintenance_lock_busy",
        "maintenance_lock_unsafe", "backup_capacity_exceeded", "backup_space_unavailable",
        "backup_failed", "backup_invalid", "baseline_missing", "baseline_invalid",
        "database_integrity_failed", "database_equivalence_failed", "service_stop_failed",
        "service_state_invalid", "service_restart_failed", "input_identity_changed",
        "media_identity_invalid", "chapter_cache_invalid", "database_write_failed",
        "authorized_change_failed", "probe_failures", "interrupted", "deadline_partial",
        "rollback_journal_present", "rollback_failed", "progress_state_invalid",
    }
    return code if code in allowed else "database_equivalence_failed"


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise MaintenanceError("deadline_partial")


def _identity_fd(descriptor):
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
        raise MaintenanceError("database_identity_unsafe")
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        block = os.read(descriptor, 1024 * 1024)
        if not block:
            break
        digest.update(block)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return FileIdentity(
        info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
        info.st_size, info.st_mtime_ns, info.st_ctime_ns, digest.hexdigest(),
    )


def _open_read_noatime(name, directory_fd):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    noatime = getattr(os, "O_NOATIME", 0)
    try:
        return os.open(name, flags | noatime, dir_fd=directory_fd)
    except OSError as exc:
        if noatime and exc.errno in {1, 22, 95}:
            raise MaintenanceError("database_identity_unsafe") from exc
        raise


def _generation_identity(database, *, journal_code="planning_journal_present"):
    parent_fd = os.open(
        database.parent,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    identities = []
    try:
        listed = os.listdir(parent_fd)
        if len(listed) > 10_000:
            raise MaintenanceError("database_identity_unsafe")
        names = set(listed)
        main = database.name
        journal = main + "-journal"
        wal = main + "-wal"
        shm = main + "-shm"
        if journal in names:
            raise MaintenanceError(journal_code)
        if shm in names and wal not in names:
            raise MaintenanceError("planning_sidecar_inconsistent")
        for suffix in ("", "-wal", "-shm"):
            name = main + suffix
            if name not in names:
                identities.append((suffix, None))
                continue
            descriptor = _open_read_noatime(name, parent_fd)
            try:
                identities.append((suffix, _identity_fd(descriptor)))
            finally:
                os.close(descriptor)
        return tuple(identities)
    finally:
        os.close(parent_fd)


def _copy_component(source_name, source_fd, destination, *, deadline=None):
    descriptor = os.open(
        destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0), 0o600,
    )
    try:
        os.lseek(source_fd, 0, os.SEEK_SET)
        while True:
            _check_deadline(deadline)
            block = os.read(source_fd, 1024 * 1024)
            if not block:
                break
            view = memoryview(block)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_raw_generation(generation):
    expected = dict(generation.live_identity)
    for suffix in ("", "-wal"):
        identity = expected.get(suffix)
        target = Path(str(generation.raw_database) + suffix)
        if identity is None:
            if target.exists():
                raise MaintenanceError("backup_invalid")
            continue
        descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            copied = _identity_fd(descriptor)
        finally:
            os.close(descriptor)
        if copied.size != identity.size or copied.digest != identity.digest:
            raise MaintenanceError("backup_invalid")


@contextlib.contextmanager
def stable_private_generation(database=None, *, temporary_parent=None,
                              journal_code="planning_journal_present",
                              verify_after_use=True, deadline=None):
    """Copy a stable live main+WAL generation without opening it in SQLite."""
    database = DATABASE if database is None else Path(database)
    before = _generation_identity(database, journal_code=journal_code)
    parent_fd = os.open(
        database.parent,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    temporary = Path(tempfile.mkdtemp(prefix="fs42-chapter-copy-", dir=temporary_parent))
    os.chmod(temporary, 0o700)
    raw_database = temporary / "raw.sqlite3"
    private_database = temporary / "snapshot.sqlite3"
    try:
        main_fd = _open_read_noatime(database.name, parent_fd)
        try:
            _copy_component(database.name, main_fd, raw_database, deadline=deadline)
        finally:
            os.close(main_fd)
        if dict(before).get("-wal") is not None:
            wal_fd = _open_read_noatime(database.name + "-wal", parent_fd)
            try:
                _copy_component(
                    database.name + "-wal", wal_fd,
                    Path(str(raw_database) + "-wal"), deadline=deadline)
            finally:
                os.close(wal_fd)
        private_main = os.open(raw_database, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            copied_main = _identity_fd(private_main)
        finally:
            os.close(private_main)
        if copied_main.size != dict(before)[""].size or copied_main.digest != dict(before)[""].digest:
            raise MaintenanceError("planning_state_changed")
        if dict(before).get("-wal") is not None:
            private_wal = os.open(
                str(raw_database) + "-wal", os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                copied_wal = _identity_fd(private_wal)
            finally:
                os.close(private_wal)
            if copied_wal.size != dict(before)["-wal"].size or copied_wal.digest != dict(before)["-wal"].digest:
                raise MaintenanceError("planning_state_changed")

        # SQLite is allowed to create or alter sidecars only beside this second
        # private copy.  The raw generation above remains unopened until the
        # verified logical backup and (when needed) baseline pin are durable.
        raw_fd = os.open(raw_database, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            _copy_component(
                raw_database.name, raw_fd, private_database, deadline=deadline)
        finally:
            os.close(raw_fd)
        if dict(before).get("-wal") is not None:
            raw_wal_fd = os.open(
                str(raw_database) + "-wal",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                _copy_component(
                    raw_database.name + "-wal", raw_wal_fd,
                    Path(str(private_database) + "-wal"), deadline=deadline,
                )
            finally:
                os.close(raw_wal_fd)
        directory_fd = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        after_copy = _generation_identity(database, journal_code=journal_code)
        if before != after_copy:
            raise MaintenanceError("planning_state_changed")
        try:
            yield PrivateGeneration(temporary, private_database, raw_database, before)
        finally:
            if verify_after_use:
                after_use = _generation_identity(database, journal_code=journal_code)
                if before != after_use:
                    raise MaintenanceError("planning_state_changed")
    finally:
        os.close(parent_fd)
        shutil.rmtree(temporary, ignore_errors=True)


@contextlib.contextmanager
def _bounded_sqlite(connection, deadline):
    if deadline is not None:
        connection.set_progress_handler(
            lambda: int(time.monotonic() >= deadline), 1_000)
    try:
        _check_deadline(deadline)
        yield
        _check_deadline(deadline)
    except sqlite3.OperationalError as exc:
        if deadline is not None and time.monotonic() >= deadline:
            raise MaintenanceError("deadline_partial") from exc
        raise
    finally:
        if deadline is not None:
            connection.set_progress_handler(None, 0)


def _database_checks(connection, *, deadline=None,
                     allowed_chapter_orphans=0):
    with _bounded_sqlite(connection, deadline):
        if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise MaintenanceError("database_integrity_failed")
        violations = connection.execute("PRAGMA foreign_key_check").fetchmany(100_001)
        if len(violations) > 100_000:
            raise MaintenanceError("database_integrity_failed")
        chapter_orphans = all(
            len(row) == 4 and row[0] == "chapter_points"
            and row[2] == "file_meta"
            for row in violations
        )
        if allowed_chapter_orphans is True:
            if violations and not chapter_orphans:
                raise MaintenanceError("database_integrity_failed")
        elif (not isinstance(allowed_chapter_orphans, int)
                or isinstance(allowed_chapter_orphans, bool)
                or allowed_chapter_orphans < 0
                or len(violations) != allowed_chapter_orphans
                or (violations and not chapter_orphans)):
            raise MaintenanceError("database_integrity_failed")
        required = {"file_meta", "chapter_points", "catalog_entries"}
        present = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if not required.issubset(present):
            raise MaintenanceError("database_integrity_failed")
        triggers = connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name='chapter_points'"
        ).fetchone()[0]
        if triggers:
            raise MaintenanceError("database_integrity_failed")


def _logical_digest(connection, *, exclude_tables=(), deadline=None):
    from station_director.preservation import canonical_sqlite_value

    digest = hashlib.sha256()
    excluded = frozenset(exclude_tables)
    for pragma in (
            "application_id", "auto_vacuum", "encoding", "page_size",
            "user_version"):
        value = connection.execute(f"PRAGMA {pragma}").fetchone()
        digest.update(pragma.encode() + b"\0")
        digest.update(json.dumps(value, separators=(",", ":")).encode() + b"\0")
    objects = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
    ).fetchall()
    if len(objects) > 4_096:
        raise MaintenanceError("database_integrity_failed")
    for item in objects:
        _check_deadline(deadline)
        if item[2] in excluded or item[1] in excluded:
            continue
        digest.update(json.dumps(item, separators=(",", ":"), default=str).encode())
    for table, in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        _check_deadline(deadline)
        if table in excluded:
            continue
        columns = [row[1] for row in connection.execute(f'PRAGMA table_xinfo("{table.replace(chr(34), chr(34)*2)}")')]
        quoted = '"' + table.replace('"', '""') + '"'
        selected = ",".join(
            chr(34) + column.replace(chr(34), chr(34) * 2) + chr(34)
            for column in columns
        )
        with tempfile.TemporaryDirectory(prefix="fs42-chapter-digest-") as directory:
            spool = sqlite3.connect(Path(directory) / "rows.sqlite3")
            try:
                spool.execute("PRAGMA journal_mode=OFF")
                spool.execute("PRAGMA synchronous=OFF")
                spool.execute("CREATE TABLE rows(value BLOB NOT NULL)")
                spool.execute("CREATE INDEX row_order ON rows(value)")
                count = 0
                for row in connection.execute(f"SELECT {selected} FROM {quoted}"):
                    _check_deadline(deadline)
                    count += 1
                    if count > 1_000_000:
                        raise MaintenanceError("database_integrity_failed")
                    encoded = b"".join(canonical_sqlite_value(value) for value in row)
                    if len(encoded) > 1024 * 1024:
                        raise MaintenanceError("database_integrity_failed")
                    spool.execute("INSERT INTO rows(value) VALUES(?)", (encoded,))
                spool.commit()
                digest.update(table.encode() + b"\0")
                digest.update(count.to_bytes(8, "big"))
                for row, in spool.execute("SELECT value FROM rows ORDER BY value"):
                    _check_deadline(deadline)
                    digest.update(bytes(row) + b"\0")
            finally:
                spool.close()
    return digest.hexdigest()


def _chapter_table_digest(connection, *, deadline=None):
    from station_director.preservation import canonical_sqlite_value

    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(
            "SELECT path,points,last_updated FROM chapter_points ORDER BY path"):
        _check_deadline(deadline)
        count += 1
        if count > 100_000:
            raise MaintenanceError("chapter_cache_invalid")
        encoded = b"".join(canonical_sqlite_value(value) for value in row)
        if len(encoded) > 1024 * 1024:
            raise MaintenanceError("chapter_cache_invalid")
        digest.update(len(encoded).to_bytes(8, "big") + encoded)
    digest.update(count.to_bytes(8, "big"))
    return digest.hexdigest()


def _validate_legacy_without_duration(value):
    from fs42.chapter_analysis import ChapterAnalysisError, validate_chapters

    if not isinstance(value, list) or not value:
        raise MaintenanceError("chapter_cache_invalid")
    try:
        ends = [item.get("chapter_end") for item in value if isinstance(item, dict)]
        if len(ends) != len(value):
            raise ChapterAnalysisError("chapter_data_invalid")
        duration = max(float(end) for end in ends)
        validate_chapters(value, duration)
    except (TypeError, ValueError, ChapterAnalysisError) as exc:
        raise MaintenanceError("chapter_cache_invalid") from exc


def _is_final_chapter_duration_overrun(value, duration):
    from fs42.chapter_analysis import ChapterAnalysisError, validate_chapters

    if not isinstance(value, list) or not value:
        return False
    try:
        final_end = value[-1].get("chapter_end")
        if (not isinstance(final_end, (int, float)) or isinstance(final_end, bool)
                or not math.isfinite(float(final_end))
                or float(final_end) <= float(duration)
                or any(float(item.get("chapter_end")) > float(duration)
                       for item in value[:-1])):
            return False
        validate_chapters(value, float(final_end))
        return True
    except (AttributeError, TypeError, ValueError, ChapterAnalysisError):
        return False


def _chapter_counts(connection, eligible, existing_paths, *, deadline=None):
    from fs42.chapter_analysis import (
        ChapterAnalysisError, classify_attestation, load_chapter_json, validate_chapters,
    )

    eligible_paths = set(eligible)
    counts = Counter()
    counts["eligible"] = len(eligible_paths)
    metadata = {}
    if eligible_paths:
        placeholders = ",".join("?" for unused in eligible_paths)
        for path, duration, size, last_mod in connection.execute(
                f"SELECT path,duration,size,last_mod FROM file_meta "
                f"WHERE path IN ({placeholders})", tuple(sorted(eligible_paths))):
            metadata[path] = (duration, size, last_mod)
    if set(metadata) != eligible_paths:
        raise MaintenanceError("chapter_cache_invalid")
    for path, item in eligible.items():
        _check_deadline(deadline)
        duration, size, last_mod = metadata[path]
        if (not isinstance(duration, (int, float)) or isinstance(duration, bool)
                or not isinstance(size, int) or isinstance(size, bool)
                or not isinstance(last_mod, (int, float)) or isinstance(last_mod, bool)
                or not all(math.isfinite(float(value)) for value in (duration, last_mod))
                or duration <= 0 or (size, float(last_mod)) != (
                    item.size, item.mtime if item.mtime is not None else item.mtime_ns / 1e9)):
            raise MaintenanceError("chapter_cache_invalid")
    stored = {}
    stale_negative = set()
    for path in eligible_paths:
        _check_deadline(deadline)
        row = connection.execute(
            "SELECT points FROM chapter_points WHERE path=?", (path,)
        ).fetchone()
        stored[path] = None if row is None else row[0]
        if row is None:
            counts["missing"] += 1
            continue
        try:
            value = load_chapter_json(row[0])
        except (TypeError, ValueError, RecursionError) as exc:
            raise MaintenanceError("chapter_cache_invalid") from exc
        if value == []:
            counts["current_empty"] += 1
        elif isinstance(value, list) and value:
            counts["legacy_nonempty"] += 1
            try:
                validate_chapters(value, metadata[path][0])
            except ChapterAnalysisError as exc:
                if not _is_final_chapter_duration_overrun(
                        value, metadata[path][0]):
                    raise MaintenanceError("chapter_cache_invalid") from exc
                counts["re_attestation_required"] += 1
            else:
                counts["trusted_legacy_nonempty"] += 1
        elif isinstance(value, dict):
            try:
                status, unused_chapters = classify_attestation(
                    value, metadata[path][0], eligible[path].size, eligible[path].mtime_ns)
            except (ValueError, TypeError, ChapterAnalysisError) as exc:
                raise MaintenanceError("chapter_cache_invalid") from exc
            if status == "re_attestation_required":
                counts["re_attestation_required"] += 1
                stale_negative.add(path)
            else:
                counts["versioned"] += 1
        else:
            raise MaintenanceError("chapter_cache_invalid")
    chapter_rows = connection.execute(
        "SELECT path,points FROM chapter_points ORDER BY path"
    ).fetchmany(100_001)
    if len(chapter_rows) > 100_000:
        raise MaintenanceError("chapter_cache_invalid")
    file_meta_paths = {
        path for path, in connection.execute("SELECT path FROM file_meta")
    }
    all_empty = set()
    for path, points in chapter_rows:
        _check_deadline(deadline)
        try:
            value = json.loads(points)
            if value == []:
                all_empty.add(path)
        except (TypeError, json.JSONDecodeError) as exc:
            raise MaintenanceError("chapter_cache_invalid") from exc
        if path not in file_meta_paths:
            if path in eligible_paths or path in existing_paths:
                raise MaintenanceError("chapter_cache_invalid")
            if value == []:
                counts["unavailable_empty"] += 1
            elif isinstance(value, list) and value:
                _validate_legacy_without_duration(value)
                counts["unavailable_nonempty"] += 1
            else:
                raise MaintenanceError("chapter_cache_invalid")
    counts["legacy_empty"] = len(all_empty)
    if counts["legacy_empty"] != counts["current_empty"] + counts["unavailable_empty"]:
        raise MaintenanceError("chapter_cache_invalid")
    counts["orphan_retirements"] = (
        counts["unavailable_empty"] + counts["unavailable_nonempty"])
    counts["attestations"] = (
        counts["missing"] + counts["current_empty"]
        + counts["re_attestation_required"])
    targets = set(stale_negative)
    for path, raw in stored.items():
        if raw is None:
            targets.add(path)
            continue
        try:
            value = json.loads(raw)
            if value == []:
                targets.add(path)
            elif isinstance(value, list) and value:
                try:
                    validate_chapters(value, metadata[path][0])
                except ChapterAnalysisError as exc:
                    if not _is_final_chapter_duration_overrun(
                            value, metadata[path][0]):
                        raise MaintenanceError("chapter_cache_invalid") from exc
                    targets.add(path)
        except (TypeError, json.JSONDecodeError) as exc:
            raise MaintenanceError("chapter_cache_invalid") from exc
    counts["short_media"] = sum(
        0 < metadata[path][0] < 300 for path in targets
    )
    counts["probes"] = counts["attestations"] - counts["short_media"]
    for key in EXPECTED_KEYS:
        counts.setdefault(key, 0)
    return dict(counts)


def _load_projected_configs(proposal_id):
    from station_director import secure_validation_inputs as secure
    from station_director.validation import project_configuration
    proposal = secure.load_canonical_proposal(proposal_id)
    policy = secure.load_canonical_policy()
    configs = {}
    directory = PROJECT_ROOT / "confs"
    directory_fd = os.open(
        directory, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        before = os.fstat(directory_fd)
        names = sorted(os.listdir(directory_fd))
        if len(names) > 1_000:
            raise MaintenanceError("input_identity_changed")
        for filename in names:
            if not filename.endswith(".json") or "/" in filename:
                continue
            descriptor = os.open(
                filename, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd,
            )
            try:
                first = os.fstat(descriptor)
                if (not stat.S_ISREG(first.st_mode) or first.st_nlink != 1
                        or first.st_uid != os.geteuid() or first.st_size > 2 * 1024 * 1024):
                    raise MaintenanceError("input_identity_changed")
                payload = bytearray()
                while len(payload) <= 2 * 1024 * 1024:
                    block = os.read(descriptor, 64 * 1024)
                    if not block:
                        break
                    payload.extend(block)
                second = os.fstat(descriptor)
                if (len(payload) > 2 * 1024 * 1024
                        or (first.st_dev, first.st_ino, first.st_size,
                            first.st_mtime_ns, first.st_ctime_ns) != (
                            second.st_dev, second.st_ino, second.st_size,
                            second.st_mtime_ns, second.st_ctime_ns)):
                    raise MaintenanceError("input_identity_changed")
                data = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MaintenanceError("input_identity_changed") from exc
            finally:
                os.close(descriptor)
            name = data.get("station_conf", {}).get("network_name")
            if name:
                configs[name] = data
        after = os.fstat(directory_fd)
        if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns):
            raise MaintenanceError("input_identity_changed")
    finally:
        os.close(directory_fd)
    projected, unused_affected, unused_source = project_configuration(configs, proposal, policy)
    return projected, policy


def _harvest_slot(slot, tags, bump_overrides, commercial_overrides):
    if not isinstance(slot, dict):
        raise MaintenanceError("input_identity_changed")
    value = slot.get("tags")
    if isinstance(value, list):
        for tag in value:
            tags.setdefault(tag, None)
    elif value is not None:
        tags.setdefault(value, None)
    if "bump_dir" in slot:
        bump_overrides.setdefault(slot["bump_dir"], None)
    if "commercial_dir" in slot:
        commercial_overrides.setdefault(slot["commercial_dir"], None)


def _configured_scan_tags(configuration):
    tags, bump_overrides, commercial_overrides = {}, {}, {}
    for day in DAYS:
        for slot in configuration[day].values():
            _harvest_slot(slot, tags, bump_overrides, commercial_overrides)
    for override in configuration.get("tag_overrides", {}).values():
        if "bump_dir" in override:
            bump_overrides.setdefault(override["bump_dir"], None)
        if "commercial_dir" in override:
            commercial_overrides.setdefault(override["commercial_dir"], None)
    for slots in configuration.get("date_overrides", {}).values():
        for slot in slots.values():
            _harvest_slot(slot, tags, bump_overrides, commercial_overrides)
    for week in configuration.get("week_overrides", {}).values():
        for day in DAYS:
            for slot in week.get(day, {}).values():
                _harvest_slot(slot, tags, bump_overrides, commercial_overrides)
    if "fallback_tag" in configuration:
        tags.setdefault(configuration["fallback_tag"], None)
    clip_shows = configuration.get("clip_shows", {})
    if clip_shows == []:
        clip_shows = {}
    if not isinstance(clip_shows, dict):
        raise MaintenanceError("input_identity_changed")
    for tag, clip in clip_shows.items():
        tags.setdefault(tag, None)
        if "start_clip" in clip:
            tags.setdefault(clip["start_clip"], None)
        if "end_clip" in clip:
            tags.setdefault(clip["end_clip"], None)
    for value in (
        configuration.get("commercial_dir"), configuration.get("bump_dir"),
        *bump_overrides, *commercial_overrides,
    ):
        if value:
            tags.setdefault(value, None)
    return tuple(tags)


def _relative_directory(configuration, tag):
    content = Path(configuration["content_dir"])
    candidate = Path(tag) if Path(tag).is_absolute() else content / tag
    resolved = candidate.resolve(strict=False)
    root = MEDIA_ROOT.resolve(strict=False)
    try:
        return resolved.relative_to(root).parts
    except ValueError as exc:
        raise MaintenanceError("media_identity_invalid") from exc


def _open_root():
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
    try:
        for component in MEDIA_ROOT.parts[1:]:
            following = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = following
        yield descriptor
    finally:
        os.close(descriptor)


_open_root = contextlib.contextmanager(_open_root)


def _relative_media_path(path):
    candidate = Path(path)
    if not candidate.is_absolute():
        raise MaintenanceError("media_identity_invalid")
    try:
        components = candidate.relative_to(MEDIA_ROOT).parts
    except ValueError as exc:
        raise MaintenanceError("media_identity_invalid") from exc
    if not components or any(
            component in {"", ".", ".."} or "/" in component
            for component in components):
        raise MaintenanceError("media_identity_invalid")
    return components


@contextlib.contextmanager
def _open_media(root_fd, components):
    """Open one regular media file without following any path-component link."""
    if not components:
        raise MaintenanceError("media_identity_invalid")
    directory = os.dup(root_fd)
    media = None
    try:
        for component in components[:-1]:
            if component in {"", ".", ".."} or "/" in component:
                raise MaintenanceError("media_identity_invalid")
            following = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory,
            )
            os.close(directory)
            directory = following
        final = components[-1]
        if final in {"", ".", ".."} or "/" in final:
            raise MaintenanceError("media_identity_invalid")
        media = os.open(
            final,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory,
        )
        info = os.fstat(media)
        if not stat.S_ISREG(info.st_mode):
            raise MaintenanceError("media_identity_invalid")
        yield media, info
    finally:
        if media is not None:
            os.close(media)
        os.close(directory)


def _existing_media_paths(paths):
    existing = set()
    with _open_root() as root_fd:
        for path in paths:
            try:
                components = _relative_media_path(path)
                with _open_media(root_fd, components):
                    existing.add(path)
            except FileNotFoundError:
                continue
    return existing


def _walk_media(root_fd, components, extensions):
    descriptors = [os.dup(root_fd)]
    try:
        for component in components:
            if component in {"", ".", ".."} or "/" in component:
                raise MaintenanceError("media_identity_invalid")
            descriptors.append(os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0), dir_fd=descriptors[-1],
            ))
        found = []

        def visit(directory_fd, prefix):
            for name in sorted(os.listdir(directory_fd)):
                if name.startswith("."):
                    continue
                info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise MaintenanceError("media_identity_invalid")
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(
                        name, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd,
                    )
                    try:
                        visit(child, prefix + (name,))
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode) and Path(name).suffix.lower() in extensions:
                    found.append(MediaItem(
                        prefix + (name,), info.st_size, info.st_mtime_ns, info.st_mtime))
            return found

        return visit(descriptors[-1], tuple(components))
    except FileNotFoundError:
        return []
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _eligible_inventory(proposal_id):
    from fs42.config_processor import ConfigProcessor
    projected, policy = _load_projected_configs(proposal_id)
    by_number = {item["number"]: item["name"] for item in policy["channels"]}
    media = {}
    with _open_root() as root_fd:
        for number in range(2, 8):
            configuration = ConfigProcessor.preprocess(
                json.loads(json.dumps(projected[by_number[number]]["station_conf"]))
            )
            if configuration.get("network_type") != "standard":
                continue
            media_filter = configuration.get("media_filter", "video")
            extensions = (
                AUDIO_EXTENSIONS if media_filter == "audio" else
                VIDEO_EXTENSIONS if media_filter == "video" else
                VIDEO_EXTENSIONS | AUDIO_EXTENSIONS
            )
            for tag in _configured_scan_tags(configuration):
                for item in _walk_media(
                        root_fd, _relative_directory(configuration, tag), extensions):
                    media[item.relative] = item
    paths = {
        os.fspath(MEDIA_ROOT.joinpath(*relative)): item
        for relative, item in media.items()
    }
    return paths


def _counts_from_private(private_database, eligible, *, deadline=None):
    connection = sqlite3.connect(f"file:{private_database}?mode=rw", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        cached_paths = {path for path, in connection.execute("SELECT path FROM file_meta")}
        chapter_paths = {
            path for path, in connection.execute("SELECT path FROM chapter_points")
        }
        existing = _existing_media_paths(cached_paths | chapter_paths)
        counts = _chapter_counts(
            connection, eligible, existing, deadline=deadline)
        connection.rollback()
        return counts
    finally:
        connection.close()


def _plan_with_inventory(private_database, proposal_id):
    eligible = _eligible_inventory(proposal_id)
    return eligible, _counts_from_private(private_database, eligible)


def _plan_from_private(private_database, proposal_id):
    unused_inventory, counts = _plan_with_inventory(private_database, proposal_id)
    return counts


def plan(proposal_id, *, database=None, temporary_parent=None):
    database = DATABASE if database is None else Path(database)
    with stable_private_generation(database, temporary_parent=temporary_parent) as generation:
        return _plan_from_private(generation.database, proposal_id)


def _validate_expected(counts, expected):
    for key in EXPECTED_KEYS:
        if counts.get(key) != expected.get(key):
            raise MaintenanceError("expected_counts_mismatch")


def _fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _progress_identity(inventory):
    digest = hashlib.sha256()
    if len(inventory) > MAX_PROGRESS_IDENTITIES:
        raise MaintenanceError("progress_state_invalid")
    for path, item in sorted(inventory.items()):
        encoded = json.dumps(
            [path, list(item.relative), item.size, item.mtime_ns],
            separators=(",", ":"), ensure_ascii=False,
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big") + encoded)
    return digest.hexdigest()


def _private_file_digest(path, *, maximum=MAX_PROGRESS_BYTES):
    try:
        info = os.stat(path, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > maximum):
            raise MaintenanceError("progress_state_invalid")
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid,
                    opened.st_nlink, opened.st_size) != (
                    info.st_dev, info.st_ino, info.st_mode, info.st_uid,
                    info.st_nlink, info.st_size):
                raise MaintenanceError("progress_state_invalid")
            payload = bytearray()
            while len(payload) <= maximum:
                block = os.read(descriptor, min(64 * 1024, maximum + 1 - len(payload)))
                if not block:
                    break
                payload.extend(block)
            if len(payload) != info.st_size:
                raise MaintenanceError("progress_state_invalid")
        finally:
            os.close(descriptor)
        return hashlib.sha256(bytes(payload)).hexdigest()
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _empty_bitmap(count):
    return "00" * ((count + 7) // 8)


def _bitmap_has(encoded, index):
    data = bytes.fromhex(encoded)
    return bool(data[index // 8] & (1 << (index % 8)))


def _bitmap_add(encoded, index):
    data = bytearray.fromhex(encoded)
    data[index // 8] |= 1 << (index % 8)
    return data.hex()


def _bitmap_clear(encoded, index):
    data = bytearray.fromhex(encoded)
    data[index // 8] &= ~(1 << (index % 8))
    return data.hex()


def _validate_bitmap(encoded, count):
    if (not isinstance(encoded, str)
            or len(encoded) != 2 * ((count + 7) // 8)
            or not re.fullmatch(r"[0-9a-f]*", encoded)):
        raise MaintenanceError("progress_state_invalid")
    if count % 8 and encoded:
        used = count % 8
        if bytes.fromhex(encoded)[-1] & ~((1 << used) - 1):
            raise MaintenanceError("progress_state_invalid")


def _validate_progress(document, *, proposal_id, baseline_identity,
                       inventory_identity, inventory_count,
                       primary_count=None, questionable_count=None):
    expected = {
        "version", "sequence", "proposal_id", "baseline_identity",
        "inventory_identity", "chapter_identity", "phase", "primary_count",
        "questionable_count", "primary_indices", "questionable_indices",
        "primary_attempted", "questionable_attempted",
        "primary_unresolved", "questionable_unresolved", "inflight",
        "operational_streak", "failures", "dataset_rejections", "unresolved",
    }
    if not isinstance(document, dict) or set(document) != expected:
        raise MaintenanceError("progress_state_invalid")
    integers = (
        "sequence", "primary_count", "questionable_count",
        "operational_streak", "dataset_rejections", "unresolved",
    )
    if any(not isinstance(document[key], int) or isinstance(document[key], bool)
           or document[key] < 0 for key in integers):
        raise MaintenanceError("progress_state_invalid")
    if (not isinstance(document["version"], int)
            or isinstance(document["version"], bool)
            or document["version"] != 1 or document["sequence"] > 100_000
            or document["proposal_id"] != proposal_id
            or document["baseline_identity"] != baseline_identity
            or document["inventory_identity"] != inventory_identity
            or (primary_count is not None
                and document["primary_count"] != primary_count)
            or (questionable_count is not None
                and document["questionable_count"] != questionable_count)
            or inventory_count > MAX_PROGRESS_IDENTITIES
            or document["operational_streak"] > MAX_PROBE_FAILURES
            or document["dataset_rejections"] > 100_000
            or document["phase"] not in {
                "primary", "questionable", "orphans", "complete", "blocked"
            }
            or not isinstance(document["chapter_identity"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", document["chapter_identity"])
            or document["unresolved"] > (
                document["primary_count"] + document["questionable_count"])):
        raise MaintenanceError("progress_state_invalid")
    primary_count = document["primary_count"]
    questionable_count = document["questionable_count"]
    for key, count in (
            ("primary_indices", primary_count),
            ("questionable_indices", questionable_count)):
        indices = document[key]
        if (not isinstance(indices, list) or len(indices) != count
                or indices != sorted(set(indices))
                or any(not isinstance(index, int) or isinstance(index, bool)
                       or index < 0 or index >= inventory_count for index in indices)):
            raise MaintenanceError("progress_state_invalid")
    if set(document["primary_indices"]) & set(document["questionable_indices"]):
        raise MaintenanceError("progress_state_invalid")
    _validate_bitmap(document["primary_attempted"], primary_count)
    _validate_bitmap(document["questionable_attempted"], questionable_count)
    _validate_bitmap(document["primary_unresolved"], primary_count)
    _validate_bitmap(document["questionable_unresolved"], questionable_count)
    primary_attempted = int.from_bytes(
        bytes.fromhex(document["primary_attempted"]), "little")
    questionable_attempted = int.from_bytes(
        bytes.fromhex(document["questionable_attempted"]), "little")
    primary_unresolved = int.from_bytes(
        bytes.fromhex(document["primary_unresolved"]), "little")
    questionable_unresolved = int.from_bytes(
        bytes.fromhex(document["questionable_unresolved"]), "little")
    if ((primary_unresolved & ~primary_attempted)
            or (questionable_unresolved & ~questionable_attempted)
            or document["unresolved"] != (
                primary_unresolved.bit_count()
                + questionable_unresolved.bit_count())):
        raise MaintenanceError("progress_state_invalid")
    failures = document["failures"]
    if (not isinstance(failures, dict) or set(failures) != set(PROBE_FAILURE_CATEGORIES)
            or any(not isinstance(value, int) or isinstance(value, bool) or value < 0
                   or value > 100_000 for value in failures.values())):
        raise MaintenanceError("progress_state_invalid")
    inflight = document["inflight"]
    if inflight is not None:
        if (not isinstance(inflight, dict)
                or set(inflight) != {"phase", "index", "stable_chapter_identity"}
                or inflight["phase"] not in {"primary", "questionable", "orphans"}
                or not isinstance(inflight["index"], int)
                or isinstance(inflight["index"], bool)
                or not isinstance(inflight["stable_chapter_identity"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", inflight["stable_chapter_identity"])):
            raise MaintenanceError("progress_state_invalid")
        if inflight["phase"] == "orphans":
            if inflight["index"] != 0:
                raise MaintenanceError("progress_state_invalid")
        else:
            bound = (primary_count if inflight["phase"] == "primary"
                     else questionable_count)
            bitmap = (document["primary_attempted"]
                      if inflight["phase"] == "primary"
                      else document["questionable_attempted"])
            if (inflight["index"] < 0 or inflight["index"] >= bound
                    or _bitmap_has(bitmap, inflight["index"])):
                raise MaintenanceError("progress_state_invalid")
        if document["phase"] != inflight["phase"]:
            raise MaintenanceError("progress_state_invalid")
    elif document["phase"] == "orphans":
        raise MaintenanceError("progress_state_invalid")
    return document


def _read_progress_payload(path):
    try:
        info = os.stat(path, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > MAX_PROGRESS_BYTES):
            raise MaintenanceError("progress_state_invalid")
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid,
                    opened.st_nlink, opened.st_size) != (
                    info.st_dev, info.st_ino, info.st_mode, info.st_uid,
                    info.st_nlink, info.st_size):
                raise MaintenanceError("progress_state_invalid")
            payload = bytearray()
            while len(payload) <= MAX_PROGRESS_BYTES:
                block = os.read(
                    descriptor,
                    min(64 * 1024, MAX_PROGRESS_BYTES + 1 - len(payload)))
                if not block:
                    break
                payload.extend(block)
        finally:
            os.close(descriptor)
        if len(payload) != info.st_size:
            raise MaintenanceError("progress_state_invalid")
        return bytes(payload)
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _read_progress_file(path):
    try:
        return json.loads(_read_progress_payload(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _recover_progress_tail(*, final_document=None, validation_arguments,
                           backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    pending = backup_root / PROGRESS_PENDING
    try:
        info = os.stat(pending, follow_symlinks=False)
    except FileNotFoundError:
        return
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size > MAX_PROGRESS_BYTES):
        raise MaintenanceError("progress_state_invalid")
    payload = _read_progress_payload(pending)
    try:
        pending_document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        pending_document = None
    if pending_document is not None:
        pending_document = _validate_progress(
            pending_document, **validation_arguments)
        expected_sequence = (
            0 if final_document is None else final_document["sequence"] + 1)
        if pending_document["sequence"] != expected_sequence:
            raise MaintenanceError("progress_state_invalid")
    try:
        os.unlink(pending)
        _fsync_directory(backup_root)
    except OSError as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _load_progress(*, proposal_id, baseline_identity, inventory_identity,
                   inventory_count, primary_count=None,
                   questionable_count=None, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    _ensure_private_directory(backup_root)
    entries = list(backup_root.iterdir())
    if len(entries) > 64:
        raise MaintenanceError("progress_state_invalid")
    allowed = {PROGRESS_FINAL, PROGRESS_PENDING, BASELINE_PIN}
    names = set()
    for path in entries:
        if path.name in names:
            raise MaintenanceError("progress_state_invalid")
        names.add(path.name)
        if path.name not in allowed and not BACKUP_RE.fullmatch(path.name):
            raise MaintenanceError("progress_state_invalid")
    final = backup_root / PROGRESS_FINAL
    document = None
    if PROGRESS_FINAL in names:
        document = _validate_progress(
            _read_progress_file(final), proposal_id=proposal_id,
            baseline_identity=baseline_identity,
            inventory_identity=inventory_identity, inventory_count=inventory_count,
            primary_count=primary_count, questionable_count=questionable_count)
    _recover_progress_tail(
        final_document=document,
        validation_arguments={
            "proposal_id": proposal_id,
            "baseline_identity": baseline_identity,
            "inventory_identity": inventory_identity,
            "inventory_count": inventory_count,
            "primary_count": primary_count,
            "questionable_count": questionable_count,
        },
        backup_root=backup_root)
    return document


def _publish_progress(document, previous=None, *, backup_root=None,
                      proposal_id, baseline_identity, inventory_identity,
                      inventory_count, primary_count=None,
                      questionable_count=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    validated = _validate_progress(
        document, proposal_id=proposal_id, baseline_identity=baseline_identity,
        inventory_identity=inventory_identity, inventory_count=inventory_count,
        primary_count=primary_count,
        questionable_count=questionable_count)
    if previous is not None and validated["sequence"] != previous["sequence"] + 1:
        raise MaintenanceError("progress_state_invalid")
    payload = json.dumps(
        validated, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_PROGRESS_BYTES:
        raise MaintenanceError("progress_state_invalid")
    pending = backup_root / PROGRESS_PENDING
    final = backup_root / PROGRESS_FINAL
    try:
        descriptor = os.open(
            pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            _write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if _read_progress_file(pending) != validated:
            raise MaintenanceError("progress_state_invalid")
        if previous is None:
            _rename_noreplace(pending, final)
        else:
            current = _read_progress_file(final)
            if current != previous:
                raise MaintenanceError("progress_state_invalid")
            os.replace(pending, final)
        _fsync_directory(backup_root)
        return validated
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _remove_progress(document, *, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    final = backup_root / PROGRESS_FINAL
    if _read_progress_file(final) != document:
        raise MaintenanceError("progress_state_invalid")
    try:
        os.unlink(final)
        _fsync_directory(backup_root)
    except OSError as exc:
        raise MaintenanceError("progress_state_invalid") from exc


def _remove_progress_after_rollback(*, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    _verify_progress_file_identities(backup_root=backup_root)
    paths = []
    for name in (PROGRESS_PENDING, PROGRESS_FINAL):
        path = backup_root / name
        try:
            info = os.stat(path, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > MAX_PROGRESS_BYTES):
            raise MaintenanceError("progress_state_invalid")
        paths.append(path)
    for path in paths:
        try:
            os.unlink(path)
        except OSError as exc:
            raise MaintenanceError("progress_state_invalid") from exc
    if paths:
        _fsync_directory(backup_root)


def _verify_progress_file_identities(*, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    for name in (PROGRESS_PENDING, PROGRESS_FINAL):
        path = backup_root / name
        try:
            info = os.stat(path, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > MAX_PROGRESS_BYTES):
            raise MaintenanceError("progress_state_invalid")


def _ensure_private_directory(path):
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    info = os.stat(path, follow_symlinks=False)
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise MaintenanceError("database_identity_unsafe")


def _write_all(descriptor, payload):
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise MaintenanceError("backup_failed")
        view = view[written:]


def _rename_noreplace(source, destination):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "renameat2", None)
    if function is None:
        raise MaintenanceError("backup_failed")
    result = function(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    if result != 0:
        raise OSError(ctypes.get_errno(), "no-replace rename failed")


def _logical_backup(private_database, pending, *, deadline=None):
    source = sqlite3.connect(private_database)
    destination = sqlite3.connect(pending)
    try:
        source.backup(
            destination,
            progress=lambda unused_status, unused_remaining, unused_total:
            _check_deadline(deadline),
        )
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.commit()
    finally:
        destination.close()
        source.close()
    descriptor = os.open(pending, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _require_no_sqlite_sidecars(pending)


def _require_no_sqlite_sidecars(database):
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(Path(str(database) + suffix)):
            raise MaintenanceError("backup_invalid")


def _verified_equivalent(left_path, right_path, *, deadline=None):
    left = sqlite3.connect(f"file:{left_path}?mode=ro", uri=True)
    right = sqlite3.connect(f"file:{right_path}?mode=ro", uri=True)
    try:
        left.execute("PRAGMA query_only=ON")
        right.execute("PRAGMA query_only=ON")
        _database_checks(left, deadline=deadline, allowed_chapter_orphans=True)
        _database_checks(right, deadline=deadline, allowed_chapter_orphans=True)
        if (_logical_digest(left, deadline=deadline)
                != _logical_digest(right, deadline=deadline)):
            raise MaintenanceError("database_equivalence_failed")
    finally:
        right.close()
        left.close()


def _backup_name(now=None, entropy=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    entropy = entropy or os.urandom(6).hex()
    return f"chapter-cache-{now.strftime('%Y%m%dT%H%M%SZ')}-{entropy}.sqlite3"


def _safe_backup_info(path):
    try:
        info = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise MaintenanceError("backup_invalid") from exc
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600):
        raise MaintenanceError("backup_invalid")
    _require_no_sqlite_sidecars(path)
    return info


def _retention_inventory(backup_root=None):
    """Return the complete recognized retention set or fail closed."""
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    if not backup_root.exists():
        return ()
    _ensure_private_directory(backup_root)
    entries = list(backup_root.iterdir())
    if len(entries) > 64:
        raise MaintenanceError("backup_capacity_exceeded")
    completed = []
    pin = None
    for path in entries:
        if BACKUP_RE.fullmatch(path.name):
            _safe_backup_info(path)
            completed.append(path)
        elif path.name == BASELINE_PIN:
            if pin is not None:
                raise MaintenanceError("backup_invalid")
            info = os.stat(path, follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600):
                raise MaintenanceError("backup_invalid")
            pin = path
        elif path.name in {PROGRESS_FINAL, PROGRESS_PENDING}:
            info = os.stat(path, follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_size > MAX_PROGRESS_BYTES):
                raise MaintenanceError("progress_state_invalid")
        else:
            # Pending files, sidecars, and every unknown entry require an
            # explicit operator decision.  They are never silently consumed.
            raise MaintenanceError("backup_capacity_exceeded")
    if len(completed) > MAX_TRANSITION_BACKUPS:
        raise MaintenanceError("backup_capacity_exceeded")
    if len(completed) == MAX_TRANSITION_BACKUPS and pin is None:
        raise MaintenanceError("backup_capacity_exceeded")
    return tuple(sorted(completed, key=lambda path: path.name))


def _verify_rollback_point(path, *, deadline=None):
    """Verify one published rollback point without permitting sidecar recovery."""
    before = _safe_backup_info(path)
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            _database_checks(
                connection, deadline=deadline, allowed_chapter_orphans=True)
        finally:
            connection.close()
    except MaintenanceError:
        raise
    except sqlite3.Error as exc:
        raise MaintenanceError("backup_invalid") from exc
    after = _safe_backup_info(path)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
            before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
            after.st_ctime_ns):
        raise MaintenanceError("backup_invalid")
    return after


def _retention_order(path):
    info = _safe_backup_info(path)
    return info.st_mtime_ns, path.name


def _unambiguous_retention_extreme(paths, *, newest):
    ranked = [(_retention_order(path), path) for path in paths]
    if not ranked:
        raise MaintenanceError("backup_capacity_exceeded")
    extreme_time = (max if newest else min)(item[0][0] for item in ranked)
    matches = [path for order, path in ranked if order[0] == extreme_time]
    if len(matches) != 1:
        raise MaintenanceError("backup_invalid")
    return matches[0]


def _complete_retention_transition(newest, baseline, *, backup_root=None,
                                   deadline=None):
    """Verify newest, then reduce the sole legal one-over-cap transition."""
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    completed = _retention_inventory(backup_root)
    if newest not in completed or baseline not in completed:
        raise MaintenanceError("backup_invalid")
    _verify_rollback_point(newest, deadline=deadline)
    if len(completed) <= MAX_BACKUPS:
        return
    if len(completed) != MAX_TRANSITION_BACKUPS:
        raise MaintenanceError("backup_capacity_exceeded")
    candidates = [path for path in completed if path not in {baseline, newest}]
    if not candidates:
        raise MaintenanceError("backup_capacity_exceeded")
    oldest = _unambiguous_retention_extreme(candidates, newest=False)
    verified = _verify_rollback_point(oldest, deadline=deadline)
    try:
        current = os.stat(oldest, follow_symlinks=False)
        if (current.st_dev, current.st_ino, current.st_size,
                current.st_mtime_ns, current.st_ctime_ns) != (
                verified.st_dev, verified.st_ino, verified.st_size,
                verified.st_mtime_ns, verified.st_ctime_ns):
            raise MaintenanceError("backup_invalid")
        os.unlink(oldest)
        _fsync_directory(backup_root)
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("backup_failed") from exc
    remaining = _retention_inventory(backup_root)
    if (len(remaining) != MAX_BACKUPS or baseline not in remaining
            or newest not in remaining):
        raise MaintenanceError("backup_invalid")


def _recover_retention_transition(baseline, *, backup_root=None, deadline=None):
    """Finish exactly one interrupted post-publication pruning transition."""
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    completed = _retention_inventory(backup_root)
    if len(completed) <= MAX_BACKUPS:
        return
    newest = _unambiguous_retention_extreme(completed, newest=True)
    _complete_retention_transition(
        newest, baseline, backup_root=backup_root, deadline=deadline)


def _check_backup_admission(database=None, backup_root=None):
    database = DATABASE if database is None else Path(database)
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    _retention_inventory(backup_root)
    database_info = os.stat(database, follow_symlinks=False)
    if (not stat.S_ISREG(database_info.st_mode)
            or database_info.st_uid != os.geteuid() or database_info.st_nlink != 1):
        raise MaintenanceError("database_identity_unsafe")
    generation_bytes = database_info.st_size
    try:
        wal_info = os.stat(Path(str(database) + "-wal"), follow_symlinks=False)
        if (not stat.S_ISREG(wal_info.st_mode) or wal_info.st_uid != os.geteuid()
                or wal_info.st_nlink != 1):
            raise MaintenanceError("database_identity_unsafe")
        generation_bytes += wal_info.st_size
    except FileNotFoundError:
        pass
    required = 4 * generation_bytes + MIN_FREE_BYTES + 8 * 1024 * 1024
    if shutil.disk_usage(database.parent).free < required:
        raise MaintenanceError("backup_space_unavailable")


def _prepare_pending_backup(private_database, *, backup_root=None,
                            deadline=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    try:
        backup_root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _ensure_private_directory(backup_root)
        name = _backup_name()
        pending = backup_root / (".pending-" + name)
        descriptor = os.open(
            pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(descriptor)
        _logical_backup(private_database, pending, deadline=deadline)
        return pending, backup_root / name
    except MaintenanceError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise MaintenanceError("backup_failed") from exc


def _publish_prepared_backup(pending, final, *, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    try:
        _require_no_sqlite_sidecars(pending)
        _rename_noreplace(pending, final)
        _fsync_directory(backup_root)
        return final
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("backup_failed") from exc


def _publish_baseline_pin(backup, logical_digest, chapter_digest, *, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    target = backup_root / BASELINE_PIN
    pending = backup_root / (".pending-baseline-" + os.urandom(6).hex() + ".json")
    payload = json.dumps({
        "version": 1,
        "state": "pinned",
        "backup_id": backup.name,
        "logical_identity": logical_digest,
        "chapter_identity": chapter_digest,
    }, sort_keys=True, separators=(",", ":")).encode()
    try:
        descriptor = os.open(
            pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        try:
            _write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _rename_noreplace(pending, target)
        _fsync_directory(backup_root)
    except MaintenanceError:
        raise
    except OSError as exc:
        raise MaintenanceError("backup_failed") from exc


def _verify_baseline_pin(*, backup_root=None):
    backup_root = BACKUP_ROOT if backup_root is None else Path(backup_root)
    if not backup_root.exists():
        raise MaintenanceError("baseline_missing")
    try:
        _ensure_private_directory(backup_root)
    except (OSError, MaintenanceError) as exc:
        raise MaintenanceError("baseline_invalid") from exc
    target = backup_root / BASELINE_PIN
    try:
        descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError as exc:
        raise MaintenanceError("baseline_missing") from exc
    except OSError as exc:
        raise MaintenanceError("baseline_invalid") from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > 4096):
            raise MaintenanceError("baseline_invalid")
        document = json.loads(os.read(descriptor, 4097))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaintenanceError("baseline_invalid") from exc
    finally:
        os.close(descriptor)
    if (not isinstance(document, dict) or set(document) != {
            "version", "state", "backup_id", "logical_identity", "chapter_identity"}
            or document["version"] != 1 or document["state"] != "pinned"
            or not isinstance(document["backup_id"], str)
            or not BACKUP_RE.fullmatch(document["backup_id"])
            or not isinstance(document["logical_identity"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", document["logical_identity"])
            or not isinstance(document["chapter_identity"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", document["chapter_identity"])):
        raise MaintenanceError("baseline_invalid")
    backup = backup_root / document["backup_id"]
    _require_no_sqlite_sidecars(backup)
    backup_info = os.stat(backup, follow_symlinks=False)
    if (not stat.S_ISREG(backup_info.st_mode) or backup_info.st_uid != os.geteuid()
            or backup_info.st_nlink != 1 or stat.S_IMODE(backup_info.st_mode) != 0o600):
        raise MaintenanceError("baseline_invalid")
    connection = sqlite3.connect(f"file:{backup}?mode=ro&immutable=1", uri=True)
    try:
        _database_checks(connection, allowed_chapter_orphans=True)
        if _logical_digest(connection) != document["logical_identity"]:
            raise MaintenanceError("baseline_invalid")
        if _chapter_table_digest(connection) != document["chapter_identity"]:
            raise MaintenanceError("baseline_invalid")
    finally:
        connection.close()
    after = os.stat(backup, follow_symlinks=False)
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
            backup_info.st_dev, backup_info.st_ino, backup_info.st_size,
            backup_info.st_mtime_ns):
        raise MaintenanceError("baseline_invalid")
    return backup


@contextlib.contextmanager
def _maintenance_lock():
    BACKUP_ROOT.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = BACKUP_ROOT.parent / MAINTENANCE_LOCK
    descriptor = os.open(
        target, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise MaintenanceError("maintenance_lock_unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise MaintenanceError("maintenance_lock_busy") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(descriptor)


@contextlib.contextmanager
def _director_validation_lock(secure):
    from station_director.validation_coordinator import (
        CoordinatorError, _validation_lock,
    )
    try:
        with _validation_lock(secure):
            yield
    except CoordinatorError as exc:
        code = exc.code if exc.code in {
            "validation_lock_busy", "validation_lock_unsafe"
        } else "validation_lock_unsafe"
        raise MaintenanceError(code) from exc


def _systemctl(arguments, *, timeout=SERVICE_COMMAND_TIMEOUT_SECONDS):
    try:
        result = subprocess.run(
            ["systemctl", "--user", *arguments], stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=timeout, check=False,
        )
        if (len(result.stdout.encode("utf-8", "replace")) > 16 * 1024
                or len(result.stderr.encode("utf-8", "replace")) > 16 * 1024):
            raise MaintenanceError("service_state_invalid")
        return result
    except (OSError, subprocess.SubprocessError) as exc:
        raise MaintenanceError("service_state_invalid") from exc


def _service_state():
    result = _systemctl([
        "show", "fs42.service",
        "--property=LoadState,ActiveState,SubState,MainPID",
    ])
    values = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise MaintenanceError("service_state_invalid")
        values[key] = value
    keys = ("LoadState", "ActiveState", "SubState", "MainPID")
    if result.returncode != 0 or set(values) != set(keys):
        raise MaintenanceError("service_state_invalid")
    return tuple(values[key] for key in keys)


def _stop_service():
    initial = _service_state()
    was_running = initial[1] == "active" and initial[3].isdigit() and int(initial[3]) > 0
    if was_running and _systemctl(["stop", "fs42.service"]).returncode != 0:
        raise MaintenanceError("service_stop_failed")
    state = _service_state()
    if state[0] != "loaded" or state[1:3] != ("inactive", "dead") or state[3] != "0":
        raise MaintenanceError("service_state_invalid")
    return was_running


def _restart_service():
    if _systemctl(["start", "fs42.service"]).returncode != 0:
        raise MaintenanceError("service_restart_failed")
    state = _service_state()
    if state[1] != "active" or not state[3].isdigit() or int(state[3]) <= 0:
        raise MaintenanceError("service_restart_failed")


def _admit(args, *, environ=None, ancestry=None):
    from station_director.isolation import check_invocation_context
    from station_director import secure_validation_inputs as secure
    allowed, unused_detail = check_invocation_context(environ=environ, ancestry=ancestry)
    if not allowed:
        raise MaintenanceError("invocation_context_rejected")
    try:
        secure.validate_proposal_id(args.proposal_id)
    except secure.SecureInputError as exc:
        raise MaintenanceError("invalid_proposal_id") from exc
    expected = {key: getattr(args, "expect_" + key) for key in EXPECTED_KEYS}
    executing = bool(getattr(args, "execute", False) or getattr(
        args, "rollback_baseline", False))
    if executing and any(value is None for value in expected.values()):
        raise MaintenanceError("execution_confirmation_missing")
    if any(value is not None and (isinstance(value, bool) or value < 0) for value in expected.values()):
        raise MaintenanceError("expected_counts_invalid")
    return expected


def execute(args, expected):
    """Perform the approved operation.  Tests inject all external boundaries."""
    from station_director import secure_validation_inputs as secure

    started = time.monotonic()
    admission_deadline = started + ADMISSION_SECONDS
    verification_deadline = (
        started + CONTROL_SECONDS - SERVICE_FINALIZATION_ALLOWANCE_SECONDS)
    was_running = False
    safe_to_restart = False
    operation_complete = False
    operation_status = "partial"
    unresolved = 0
    partial_reason = "deadline_partial"
    failure_counts = {category: 0 for category in PROBE_FAILURE_CATEGORIES}
    with _director_validation_lock(secure):
        with _maintenance_lock():
            was_running = _stop_service()
            # A verified stop makes failures before the live writer safe to
            # restart.  Writer entry revokes that decision until all authorized
            # change and integrity checks have passed.
            safe_to_restart = True
            try:
                _check_backup_admission()
                _check_deadline(admission_deadline)
                with stable_private_generation(
                        DATABASE, journal_code="hot_journal_present",
                        verify_after_use=False,
                        deadline=admission_deadline) as generation:
                    inventory, counts = _plan_with_inventory(
                        generation.database, args.proposal_id)
                    _validate_expected(counts, expected)

                    pin = BACKUP_ROOT / BASELINE_PIN
                    baseline = None
                    if pin.exists():
                        baseline = _verify_baseline_pin()
                        _recover_retention_transition(
                            baseline, deadline=admission_deadline)
                        _load_progress(
                            proposal_id=args.proposal_id,
                            baseline_identity=_private_file_digest(pin),
                            inventory_identity=_progress_identity(inventory),
                            inventory_count=len(inventory))
                    elif counts.get("versioned", 0):
                        raise MaintenanceError("baseline_missing")
                    elif ((BACKUP_ROOT / PROGRESS_FINAL).exists()
                          or (BACKUP_ROOT / PROGRESS_PENDING).exists()):
                        raise MaintenanceError("baseline_missing")

                    # The backup remains unpublished while all integrity,
                    # schema, count, and logical-equivalence checks run.
                    _check_deadline(admission_deadline)
                    pending, final = _prepare_pending_backup(
                        generation.database, deadline=admission_deadline)
                    source = sqlite3.connect(
                        f"file:{generation.database}?mode=ro", uri=True)
                    candidate = sqlite3.connect(f"file:{pending}?mode=ro", uri=True)
                    try:
                        source.execute("PRAGMA query_only=ON")
                        candidate.execute("PRAGMA query_only=ON")
                        _database_checks(
                            source, deadline=admission_deadline,
                            allowed_chapter_orphans=counts["orphan_retirements"])
                        _database_checks(
                            candidate, deadline=admission_deadline,
                            allowed_chapter_orphans=counts["orphan_retirements"])
                        source_counts = _counts_from_private(
                            generation.database, inventory,
                            deadline=admission_deadline)
                        candidate_counts = _counts_from_private(
                            pending, inventory, deadline=admission_deadline)
                        if source_counts != counts or candidate_counts != counts:
                            raise MaintenanceError("database_equivalence_failed")
                        source_logical = _logical_digest(
                            source, deadline=admission_deadline)
                        source_chapters = _chapter_table_digest(
                            source, deadline=admission_deadline)
                        if baseline is not None and not counts.get("versioned", 0):
                            baseline_connection = sqlite3.connect(
                                f"file:{baseline}?mode=ro&immutable=1", uri=True)
                            try:
                                if _chapter_table_digest(
                                        baseline_connection,
                                        deadline=admission_deadline) != source_chapters:
                                    raise MaintenanceError("baseline_invalid")
                            finally:
                                baseline_connection.close()
                        if source_logical != _logical_digest(
                                candidate, deadline=admission_deadline):
                            raise MaintenanceError("database_equivalence_failed")
                        protected_logical = _logical_digest(
                            candidate, exclude_tables={"chapter_points"},
                            deadline=admission_deadline)
                    finally:
                        candidate.close()
                        source.close()

                    # Identity D precedes publication.  No pin can exist for an
                    # unverified pending backup.
                    _verify_raw_generation(generation)
                    identity_d = _generation_identity(
                        DATABASE, journal_code="hot_journal_present")
                    if identity_d != generation.live_identity:
                        raise MaintenanceError("input_identity_changed")
                    backup = _publish_prepared_backup(pending, final)
                    if not pin.exists():
                        _publish_baseline_pin(
                            backup, source_logical, source_chapters)
                        baseline = backup
                    else:
                        baseline = _verify_baseline_pin()
                    _complete_retention_transition(
                        backup, baseline, deadline=admission_deadline)
                    _verify_raw_generation(generation)

                    # Identity E is captured only after backup and pin directory
                    # entries are durable.  This is the final check before the
                    # first SQLite open of the live database.
                    identity_e = _generation_identity(
                        DATABASE, journal_code="hot_journal_present")
                    if identity_e != generation.live_identity:
                        raise MaintenanceError("input_identity_changed")
                    if time.monotonic() < admission_deadline:
                        safe_to_restart = False
                        (operation_status, safe_to_restart, failure_counts,
                         unresolved, partial_reason) = _run_writer(
                            args.proposal_id, counts, started,
                            expected_inventory=inventory,
                            expected_full_digest=source_logical,
                            expected_protected_digest=protected_logical,
                            verification_deadline=verification_deadline,
                            baseline=baseline,
                        )
                        operation_complete = operation_status == "complete"
            except MaintenanceError as exc:
                if exc.code in {
                    "database_integrity_failed", "database_equivalence_failed",
                    "database_write_failed", "authorized_change_failed",
                    "service_state_invalid", "input_identity_changed",
                }:
                    safe_to_restart = False
                raise
            finally:
                if was_running and safe_to_restart:
                    _restart_service()
    return {
        "status": operation_status,
        **counts, "failures": failure_counts,
        "unresolved": unresolved,
        "partial_reason": None if operation_complete else partial_reason,
    }


def rollback(args, expected):
    """Restore only chapter metadata from the pinned pre-envelope baseline."""
    from station_director import secure_validation_inputs as secure

    was_running = False
    safe_to_restart = False
    started = time.monotonic()
    admission_deadline = started + ADMISSION_SECONDS
    verification_deadline = (
        started + CONTROL_SECONDS - SERVICE_FINALIZATION_ALLOWANCE_SECONDS)
    with _director_validation_lock(secure):
        with _maintenance_lock():
            was_running = _stop_service()
            safe_to_restart = True
            try:
                _check_backup_admission()
                _check_deadline(admission_deadline)
                baseline = _verify_baseline_pin()
                _recover_retention_transition(
                    baseline, deadline=admission_deadline)
                with stable_private_generation(
                        DATABASE, journal_code="rollback_journal_present",
                        verify_after_use=False,
                        deadline=admission_deadline) as generation:
                    unused_inventory, counts = _plan_with_inventory(
                        generation.database, args.proposal_id)
                    _validate_expected(counts, expected)
                    pending, final = _prepare_pending_backup(
                        generation.database, deadline=admission_deadline)
                    _verified_equivalent(
                        generation.database, pending, deadline=admission_deadline)
                    if _counts_from_private(
                            pending, unused_inventory,
                            deadline=admission_deadline) != counts:
                        raise MaintenanceError("database_equivalence_failed")
                    current_backup = _publish_prepared_backup(pending, final)
                    _complete_retention_transition(
                        current_backup, baseline, deadline=admission_deadline)
                    _verify_raw_generation(generation)
                    if _generation_identity(
                            DATABASE, journal_code="rollback_journal_present"
                    ) != generation.live_identity:
                        raise MaintenanceError("input_identity_changed")
                    backup_connection = sqlite3.connect(
                        f"file:{current_backup}?mode=ro&immutable=1", uri=True)
                    try:
                        expected_full = _logical_digest(backup_connection)
                        expected_protected = _logical_digest(
                            backup_connection, exclude_tables={"chapter_points"})
                    finally:
                        backup_connection.close()
                    _verify_baseline_pin()
                    if _generation_identity(
                            DATABASE, journal_code="rollback_journal_present"
                    ) != generation.live_identity:
                        raise MaintenanceError("input_identity_changed")
                    _check_deadline(admission_deadline)
                    _verify_progress_file_identities()
                    safe_to_restart = False
                    _run_rollback(
                        baseline, expected_full_digest=expected_full,
                        expected_protected_digest=expected_protected,
                        verification_deadline=verification_deadline,
                    )
                    _remove_progress_after_rollback()
                    safe_to_restart = True
            except MaintenanceError as exc:
                if exc.code in {
                    "database_integrity_failed", "database_equivalence_failed",
                    "database_write_failed", "authorized_change_failed",
                    "service_state_invalid", "input_identity_changed",
                    "rollback_failed",
                }:
                    safe_to_restart = False
                raise
            finally:
                if was_running and safe_to_restart:
                    _restart_service()
    return {"status": "rolled_back", **counts}


def _writer_authorizer(action, arg1, arg2, unused_database, unused_trigger):
    allowed = {
        sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
        sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT,
    }
    if action in allowed:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_PRAGMA:
        if arg1 == "table_xinfo" or (
                arg1 in {"application_id", "auto_vacuum", "encoding",
                         "foreign_key_check", "integrity_check", "page_size",
                         "user_version"}
                and arg2 in {None, ""}):
            return sqlite3.SQLITE_OK
    if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE} and arg1 == "chapter_points":
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def _run_rollback(baseline, *, expected_full_digest, expected_protected_digest,
                  verification_deadline=None):
    live = sqlite3.connect(DATABASE)
    source = sqlite3.connect(f"file:{baseline}?mode=ro&immutable=1", uri=True)
    try:
        source.execute("PRAGMA query_only=ON")
        _database_checks(
            source, deadline=verification_deadline,
            allowed_chapter_orphans=True)
        _database_checks(
            live, deadline=verification_deadline,
            allowed_chapter_orphans=True)
        if _logical_digest(
                live, deadline=verification_deadline) != expected_full_digest:
            raise MaintenanceError("database_equivalence_failed")
        rows = []
        for row in source.execute(
                "SELECT path,points,last_updated FROM chapter_points ORDER BY path"):
            if len(rows) >= 100_000:
                raise MaintenanceError("baseline_invalid")
            rows.append(row)
        live.set_authorizer(_writer_authorizer)
        try:
            live.execute("BEGIN IMMEDIATE")
            live.execute("DELETE FROM chapter_points")
            live.executemany(
                "INSERT INTO chapter_points(path,points,last_updated) VALUES(?,?,?)", rows)
            live.commit()
        except Exception:
            live.rollback()
            raise
        _database_checks(
            live, deadline=verification_deadline,
            allowed_chapter_orphans=True)
        if _logical_digest(
                live, exclude_tables={"chapter_points"},
                deadline=verification_deadline) != expected_protected_digest:
            raise MaintenanceError("authorized_change_failed")
        if live.execute(
                "SELECT path,points,last_updated FROM chapter_points ORDER BY path"
        ).fetchall() != rows:
            raise MaintenanceError("rollback_failed")
    except MaintenanceError:
        raise
    except sqlite3.Error as exc:
        raise MaintenanceError("rollback_failed") from exc
    finally:
        source.close()
        live.close()


def _chapter_digest_excluding(connection, excluded=()):
    from station_director.preservation import canonical_sqlite_value

    excluded = frozenset(excluded)
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(
            "SELECT path,points,last_updated FROM chapter_points ORDER BY path"):
        if row[0] in excluded:
            continue
        count += 1
        if count > 100_000:
            raise MaintenanceError("chapter_cache_invalid")
        encoded = b"".join(canonical_sqlite_value(value) for value in row)
        if len(encoded) > 1024 * 1024:
            raise MaintenanceError("chapter_cache_invalid")
        digest.update(len(encoded).to_bytes(8, "big") + encoded)
    digest.update(count.to_bytes(8, "big"))
    return digest.hexdigest()


def _writer_row_state(connection, path, item):
    from fs42.chapter_analysis import (
        ChapterAnalysisError, classify_attestation, load_chapter_json, validate_chapters,
    )

    row = connection.execute(
        "SELECT points FROM chapter_points WHERE path=?", (path,)).fetchone()
    if row is None:
        return {"status": "missing", "raw": None}
    raw = row[0]
    duration_row = connection.execute(
        "SELECT duration FROM file_meta WHERE path=?", (path,)).fetchone()
    if duration_row is None:
        raise MaintenanceError("chapter_cache_invalid")
    try:
        value = load_chapter_json(raw)
        if isinstance(value, list):
            if not value:
                return {"status": "legacy_empty", "raw": raw}
            try:
                validate_chapters(value, duration_row[0])
            except ChapterAnalysisError as exc:
                if not _is_final_chapter_duration_overrun(
                        value, duration_row[0]):
                    raise MaintenanceError("chapter_cache_invalid") from exc
                return {"status": "re_attestation_required", "raw": raw}
            return {"status": "legacy_nonempty", "raw": raw}
        status, unused_chapters = classify_attestation(
            value, duration_row[0], item.size, item.mtime_ns)
        return {"status": status, "raw": raw}
    except MaintenanceError:
        raise
    except (TypeError, KeyError, ValueError, RecursionError, ChapterAnalysisError) as exc:
        raise MaintenanceError("chapter_cache_invalid") from exc


def _writer_targets(connection, inventory):
    primary = []
    questionable = []
    for path, item in sorted(inventory.items()):
        previous = _writer_row_state(connection, path, item)
        if previous["status"] in {"missing", "legacy_empty"}:
            primary.append(path)
        elif previous["status"] == "re_attestation_required":
            questionable.append(path)
    return tuple(primary), tuple(questionable)


def _verified_orphan_rows(connection, inventory, root_fd):
    rows = connection.execute(
        "SELECT c.path,c.points,c.last_updated FROM chapter_points AS c "
        "LEFT JOIN file_meta AS f ON f.path=c.path WHERE f.path IS NULL "
        "ORDER BY c.path"
    ).fetchmany(100_001)
    if len(rows) > 100_000:
        raise MaintenanceError("chapter_cache_invalid")
    verified = []
    for path, raw, last_updated in rows:
        if path in inventory:
            raise MaintenanceError("chapter_cache_invalid")
        try:
            value = json.loads(raw)
            if value != []:
                _validate_legacy_without_duration(value)
            components = _relative_media_path(path)
            with _open_media(root_fd, components):
                raise MaintenanceError("chapter_cache_invalid")
        except FileNotFoundError:
            verified.append((path, raw, last_updated))
        except (TypeError, json.JSONDecodeError) as exc:
            raise MaintenanceError("chapter_cache_invalid") from exc
        except OSError as exc:
            raise MaintenanceError("media_identity_invalid") from exc
    return tuple(verified)


def _new_progress(proposal_id, baseline_identity, inventory_identity,
                  chapter_identity, primary_indices, questionable_indices):
    primary_count = len(primary_indices)
    questionable_count = len(questionable_indices)
    return {
        "version": 1,
        "sequence": 0,
        "proposal_id": proposal_id,
        "baseline_identity": baseline_identity,
        "inventory_identity": inventory_identity,
        "chapter_identity": chapter_identity,
        "phase": "primary",
        "primary_count": primary_count,
        "questionable_count": questionable_count,
        "primary_indices": list(primary_indices),
        "questionable_indices": list(questionable_indices),
        "primary_attempted": _empty_bitmap(primary_count),
        "questionable_attempted": _empty_bitmap(questionable_count),
        "primary_unresolved": _empty_bitmap(primary_count),
        "questionable_unresolved": _empty_bitmap(questionable_count),
        "inflight": None,
        "operational_streak": 0,
        "failures": {category: 0 for category in PROBE_FAILURE_CATEGORIES},
        "dataset_rejections": 0,
        "unresolved": 0,
    }


def _next_progress(document, **updates):
    following = json.loads(json.dumps(document))
    following.update(updates)
    following["sequence"] = document["sequence"] + 1
    return following


def _progress_arguments(proposal_id, baseline_identity, inventory_identity,
                        inventory_count, primary_count=None,
                        questionable_count=None):
    return {
        "proposal_id": proposal_id,
        "baseline_identity": baseline_identity,
        "inventory_identity": inventory_identity,
        "inventory_count": inventory_count,
        "primary_count": primary_count,
        "questionable_count": questionable_count,
    }


def _publish_next_progress(document, *, arguments, **updates):
    following = _next_progress(document, **updates)
    return _publish_progress(following, document, **arguments)


def _reconcile_inflight(connection, document, primary, questionable, inventory,
                       orphans, *, arguments, root_fd):
    from fs42.chapter_analysis import TRUSTED_CHAPTER_STATES
    inflight = document["inflight"]
    if inflight is None:
        if _chapter_table_digest(connection) != document["chapter_identity"]:
            raise MaintenanceError("progress_state_invalid")
        return document
    if inflight["phase"] == "orphans":
        current_identity = _chapter_table_digest(connection)
        if current_identity == document["chapter_identity"]:
            return document
        if current_identity != inflight["stable_chapter_identity"]:
            raise MaintenanceError("progress_state_invalid")
        _database_checks(connection)
        phase = "blocked" if document["unresolved"] else "complete"
        return _publish_next_progress(
            document, arguments=arguments, phase=phase, inflight=None,
            chapter_identity=_chapter_table_digest(connection),
            operational_streak=0)
    targets = primary if inflight["phase"] == "primary" else questionable
    path = targets[inflight["index"]]
    if (_chapter_digest_excluding(connection, {path})
            != inflight["stable_chapter_identity"]):
        raise MaintenanceError("progress_state_invalid")
    current = _writer_row_state(connection, path, inventory[path])
    original_statuses = (
        {"missing", "legacy_empty"} if inflight["phase"] == "primary"
        else {"re_attestation_required"}
    )
    if current["status"] in original_statuses:
        if _chapter_table_digest(connection) != document["chapter_identity"]:
            raise MaintenanceError("progress_state_invalid")
        return document
    if current["status"] not in TRUSTED_CHAPTER_STATES:
        raise MaintenanceError("progress_state_invalid")
    bitmap_key = inflight["phase"] + "_attempted"
    resolved_key = inflight["phase"] + "_unresolved"
    index = inflight["index"]
    return _publish_next_progress(
        document, arguments=arguments,
        **{
            bitmap_key: _bitmap_add(document[bitmap_key], index),
            resolved_key: _bitmap_clear(document[resolved_key], index),
            "chapter_identity": _chapter_table_digest(connection),
            "inflight": None,
            "operational_streak": 0,
        })


def _verify_progress_target_states(connection, document, primary, questionable,
                                   inventory):
    from fs42.chapter_analysis import TRUSTED_CHAPTER_STATES
    for phase, targets, original in (
            ("primary", primary, {"missing", "legacy_empty"}),
            ("questionable", questionable, {"re_attestation_required"})):
        attempted = document[phase + "_attempted"]
        unresolved = document[phase + "_unresolved"]
        for index, path in enumerate(targets):
            state = _writer_row_state(connection, path, inventory[path])["status"]
            if _bitmap_has(attempted, index) and not _bitmap_has(unresolved, index):
                if state not in TRUSTED_CHAPTER_STATES:
                    raise MaintenanceError("progress_state_invalid")
            elif state not in original:
                raise MaintenanceError("progress_state_invalid")


def _run_writer(proposal_id, initial_counts, started, *, expected_inventory=None,
                expected_full_digest, expected_protected_digest,
                verification_deadline=None, baseline=None):
    from fs42.chapter_analysis import ChapterAnalysisError, analyze_chapters
    from fs42.fluid_statements import FluidStatements

    inventory = _eligible_inventory(proposal_id)
    if expected_inventory is not None and inventory != expected_inventory:
        raise MaintenanceError("input_identity_changed")
    if baseline is None:
        raise MaintenanceError("baseline_invalid")
    baseline_identity = _private_file_digest(BACKUP_ROOT / BASELINE_PIN)
    inventory_identity = _progress_identity(inventory)
    connection = sqlite3.connect(DATABASE)
    try:
        _database_checks(
            connection, deadline=verification_deadline,
            allowed_chapter_orphans=initial_counts["orphan_retirements"])
        if _logical_digest(
                connection, deadline=verification_deadline) != expected_full_digest:
            raise MaintenanceError("database_equivalence_failed")
        connection.set_authorizer(_writer_authorizer)
        with _open_root() as root_fd:
            current_primary, current_questionable = _writer_targets(
                connection, inventory)
            if (len(current_primary)
                    != initial_counts["missing"] + initial_counts["current_empty"]
                    or len(current_questionable)
                    != initial_counts["re_attestation_required"]):
                raise MaintenanceError("chapter_cache_invalid")
            orphans = _verified_orphan_rows(connection, inventory, root_fd)
            if len(orphans) != initial_counts["orphan_retirements"]:
                raise MaintenanceError("chapter_cache_invalid")
            ordered_inventory = tuple(sorted(inventory))
            inventory_indexes = {
                path: index for index, path in enumerate(ordered_inventory)
            }
            arguments = _progress_arguments(
                proposal_id, baseline_identity, inventory_identity,
                len(ordered_inventory))
            progress = _load_progress(**arguments)
            if progress is None:
                primary_indices = tuple(
                    inventory_indexes[path] for path in current_primary)
                questionable_indices = tuple(
                    inventory_indexes[path] for path in current_questionable)
                progress = _new_progress(
                    proposal_id, baseline_identity, inventory_identity,
                    _chapter_table_digest(connection), primary_indices,
                    questionable_indices)
                publication_arguments = dict(
                    arguments, primary_count=len(primary_indices),
                    questionable_count=len(questionable_indices))
                progress = _publish_progress(
                    progress, None, **publication_arguments)
            else:
                publication_arguments = dict(
                    arguments, primary_count=progress["primary_count"],
                    questionable_count=progress["questionable_count"])
            primary = tuple(
                ordered_inventory[index] for index in progress["primary_indices"])
            questionable = tuple(
                ordered_inventory[index]
                for index in progress["questionable_indices"])
            if (not set(current_primary).issubset(primary)
                    or not set(current_questionable).issubset(questionable)):
                raise MaintenanceError("progress_state_invalid")
            progress = _reconcile_inflight(
                connection, progress, primary, questionable, inventory,
                orphans, arguments=publication_arguments, root_fd=root_fd)
            arguments = publication_arguments
            _verify_progress_target_states(
                connection, progress, primary, questionable, inventory)

            # A prior complete sweep is explicitly retriable, but only its
            # unresolved identities are reopened. Resolved work remains fixed.
            if progress["phase"] == "blocked":
                updates = {
                    "phase": "primary", "operational_streak": 0,
                    "unresolved": 0, "dataset_rejections": 0,
                    "primary_unresolved": _empty_bitmap(len(primary)),
                    "questionable_unresolved": _empty_bitmap(len(questionable)),
                }
                for phase in ("primary", "questionable"):
                    attempted = bytearray.fromhex(progress[phase + "_attempted"])
                    unresolved = bytes.fromhex(progress[phase + "_unresolved"])
                    for index, value in enumerate(unresolved):
                        attempted[index] &= ~value
                    updates[phase + "_attempted"] = attempted.hex()
                progress = _publish_next_progress(
                    progress, arguments=arguments, **updates)

            partial_reason = None
            skip_sweeps = progress["phase"] in {"orphans", "complete"}
            phases = (("primary", primary), ("questionable", questionable))
            for phase, targets in phases:
                if skip_sweeps:
                    break
                attempted_key = phase + "_attempted"
                unresolved_key = phase + "_unresolved"
                if phase == "questionable":
                    remaining = sum(
                        not _bitmap_has(progress[attempted_key], index)
                        for index in range(len(targets)))
                    if (remaining and time.monotonic()
                            + remaining * QUESTIONABLE_ATTEMPT_SECONDS
                            >= started + ADMISSION_SECONDS):
                        partial_reason = "deadline_partial"
                        break
                if progress["phase"] != phase:
                    progress = _publish_next_progress(
                        progress, arguments=arguments, phase=phase)
                if progress["operational_streak"] >= MAX_PROBE_FAILURES:
                    progress = _publish_next_progress(
                        progress, arguments=arguments, operational_streak=0)
                for index, path in enumerate(targets):
                    if _bitmap_has(progress[attempted_key], index):
                        continue
                    if time.monotonic() >= started + ADMISSION_SECONDS:
                        partial_reason = "deadline_partial"
                        break
                    item = inventory[path]
                    stable = _chapter_digest_excluding(connection, {path})
                    progress = _publish_next_progress(
                        progress, arguments=arguments,
                        inflight={
                            "phase": phase, "index": index,
                            "stable_chapter_identity": stable,
                        })
                    state = _service_state()
                    if state[1:3] != ("inactive", "dead") or state[3] != "0":
                        raise MaintenanceError("service_state_invalid")
                    category = None
                    completed = False
                    try:
                        previous = _writer_row_state(connection, path, item)
                        with _open_media(root_fd, item.relative) as (media_fd, info):
                            if (info.st_size, info.st_mtime_ns) != (
                                    item.size, item.mtime_ns):
                                raise MaintenanceError("input_identity_changed")
                            row = connection.execute(
                                "SELECT duration FROM file_meta WHERE path=?", (path,)
                            ).fetchone()
                            if row is None:
                                raise MaintenanceError("chapter_cache_invalid")
                            analysis = analyze_chapters(
                                f"/proc/self/fd/{media_fd}", row[0],
                                timeout=ANALYSIS_TIMEOUT_SECONDS,
                                pass_fds=(media_fd,))
                            completed = True
                            final_info = os.fstat(media_fd)
                            if (final_info.st_size, final_info.st_mtime_ns) != (
                                    info.st_size, info.st_mtime_ns):
                                raise MaintenanceError("input_identity_changed")
                            try:
                                with connection:
                                    FluidStatements.add_chapter_points(
                                        connection, path, analysis, final_info,
                                        previous, baseline_verified=True,
                                        replace_questionable=(phase == "questionable"))
                            except (sqlite3.Error, RuntimeError, ValueError) as exc:
                                raise MaintenanceError("database_write_failed") from exc
                    except ChapterAnalysisError as exc:
                        category = exc.category
                    except OSError as exc:
                        raise MaintenanceError("media_identity_invalid") from exc

                    failures = dict(progress["failures"])
                    unresolved_bitmap = progress[unresolved_key]
                    unresolved = progress["unresolved"]
                    dataset_rejections = progress["dataset_rejections"]
                    streak = progress["operational_streak"]
                    if category is None and completed:
                        streak = 0
                    elif category == "chapter_data_invalid":
                        failures[category] += 1
                        dataset_rejections += 1
                        unresolved_bitmap = _bitmap_add(unresolved_bitmap, index)
                        unresolved += 1
                        streak = 0
                    elif category is not None:
                        failures[category] += 1
                        unresolved_bitmap = _bitmap_add(unresolved_bitmap, index)
                        unresolved += 1
                        streak += 1
                    progress = _publish_next_progress(
                        progress, arguments=arguments,
                        **{
                            attempted_key: _bitmap_add(
                                progress[attempted_key], index),
                            unresolved_key: unresolved_bitmap,
                            "chapter_identity": _chapter_table_digest(connection),
                            "inflight": None, "operational_streak": streak,
                            "failures": failures,
                            "dataset_rejections": dataset_rejections,
                            "unresolved": unresolved,
                        })
                    if streak >= MAX_PROBE_FAILURES:
                        partial_reason = "systemic_analysis_partial"
                        break
                if partial_reason is not None:
                    break

            all_attempted = all(
                _bitmap_has(progress[phase + "_attempted"], index)
                for phase, targets in phases for index in range(len(targets)))
            if partial_reason is None and all_attempted and progress["phase"] != "complete":
                if progress["phase"] != "orphans":
                    progress = _publish_next_progress(
                        progress, arguments=arguments, phase="orphans",
                        inflight={
                            "phase": "orphans", "index": 0,
                            "stable_chapter_identity": _chapter_digest_excluding(
                                connection, {row[0] for row in orphans}),
                        })
                current_orphans = _verified_orphan_rows(
                    connection, inventory, root_fd)
                if current_orphans:
                    if current_orphans != orphans:
                        raise MaintenanceError("progress_state_invalid")
                    try:
                        with connection:
                            for path, raw, last_updated in orphans:
                                changed = connection.execute(
                                    "DELETE FROM chapter_points WHERE path=? "
                                    "AND points=? AND last_updated=?",
                                    (path, raw, last_updated))
                                if changed.rowcount != 1:
                                    raise MaintenanceError("database_write_failed")
                    except sqlite3.Error as exc:
                        raise MaintenanceError("database_write_failed") from exc
                if _verified_orphan_rows(connection, inventory, root_fd):
                    raise MaintenanceError("database_write_failed")
                _database_checks(connection, deadline=verification_deadline)
                phase = "blocked" if progress["unresolved"] else "complete"
                progress = _publish_next_progress(
                    progress, arguments=arguments, phase=phase, inflight=None,
                    chapter_identity=_chapter_table_digest(connection),
                    operational_streak=0)
            elif partial_reason is None and not skip_sweeps:
                partial_reason = "deadline_partial"

        _database_checks(
            connection, deadline=verification_deadline,
            allowed_chapter_orphans=(
                0 if progress["phase"] in {"complete", "blocked"}
                else initial_counts["orphan_retirements"]))
        if _logical_digest(
                connection, exclude_tables={"chapter_points"},
                deadline=verification_deadline) != expected_protected_digest:
            raise MaintenanceError("authorized_change_failed")
        final_counts = _chapter_counts(
            connection, inventory, _existing_media_paths(
                {path for path, in connection.execute("SELECT path FROM file_meta")}
                | {path for path, in connection.execute(
                    "SELECT path FROM chapter_points")}),
            deadline=verification_deadline,
        )
        if progress["phase"] == "complete" and (
                final_counts.get("missing", 0) != 0
                or final_counts.get("current_empty", 0) != 0
                or final_counts.get("unavailable_empty", 0) != 0
                or final_counts.get("unavailable_nonempty", 0) != 0
                or final_counts.get("re_attestation_required", 0) != 0
                or final_counts.get("versioned", 0)
                < initial_counts.get("versioned", 0) + initial_counts["attestations"]):
            raise MaintenanceError("authorized_change_failed")
        if _eligible_inventory(proposal_id) != inventory:
            raise MaintenanceError("input_identity_changed")
        successful = progress["phase"] == "complete"
        blocked = progress["phase"] == "blocked"
        if successful:
            _remove_progress(progress)
        return (
            "complete" if successful else "blocked" if blocked else "partial",
            True,
            dict(progress["failures"]),
            progress["unresolved"],
            None if successful else (
                "re_attestation_unresolved" if blocked else partial_reason),
        )
    except sqlite3.DatabaseError as exc:
        raise MaintenanceError("database_write_failed") from exc
    finally:
        connection.close()


def _parser():
    parser = argparse.ArgumentParser(prog="chapter-cache-warmup")
    parser.add_argument("proposal_id")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--rollback-baseline", action="store_true")
    for key in EXPECTED_KEYS:
        parser.add_argument("--expect-" + key.replace("_", "-"), dest="expect_" + key, type=int)
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        expected = _admit(args)
        if args.execute:
            result = execute(args, expected)
        elif args.rollback_baseline:
            result = rollback(args, expected)
        else:
            counts = plan(args.proposal_id)
            result = {"status": "provisional", **counts}
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except MaintenanceError as exc:
        print(json.dumps({"status": "rejected", "code": _fixed_failure(exc.code)}, separators=(",", ":")))
        return 1
    except Exception:
        print(json.dumps({
            "status": "rejected", "code": "database_equivalence_failed"
        }, separators=(",", ":")))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
