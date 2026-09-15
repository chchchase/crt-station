import logging
import sqlite3
import datetime
import os
import json
import math
import re
import stat
from pathlib import Path
from fs42.chapter_analysis import (
    COMPLETED_METHODS,
    METHOD_SHORT,
    CompletedChapterAnalysis,
    ChapterAnalysisError,
    validate_chapters,
)
from fs42.fluid_objects import FileRepoEntry
from fs42.scheduling_context import (
    ValidationCatalogMetadataUnavailable,
    current_validation_context,
    in_validation_mode,
    scheduling_now,
)


class FluidStatements:
    """Basic static SQL functions for interacting with the Fluid catalog DB"""

    @staticmethod
    def check_file_cache(connection: sqlite3.Connection, full_path) -> FileRepoEntry:
        """Find full_path and return fullpath if its in the file cache"""

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM file_meta WHERE path = ?;", (full_path,))
        row = cursor.fetchone()
        if row is None and in_validation_mode():
            cache_path = FluidStatements.validation_cache_path(full_path)
            cursor.execute("SELECT * FROM file_meta WHERE path = ?;", (cache_path,))
            row = cursor.fetchone()
        result = None
        if row:
            repo_entry = FileRepoEntry(row)
            if in_validation_mode():
                FluidStatements.validate_cached_entry(repo_entry)
                try:
                    info = os.stat(full_path)
                except OSError as exc:
                    raise ValidationCatalogMetadataUnavailable(
                        "cached media identity is unavailable") from exc
                if (info.st_size, info.st_mtime) != (
                        repo_entry.size, repo_entry.last_mod):
                    raise ValidationCatalogMetadataUnavailable(
                        "cached media metadata is stale")
            result = repo_entry
        cursor.close()
        return result

    @staticmethod
    def validation_cache_path(path):
        normalized = os.path.normpath(os.fspath(path))
        media_root = os.path.normpath(current_validation_context().media_root)
        for root in dict.fromkeys((media_root, "/media")):
            if normalized == root:
                return "/mnt/t7/CRT-Media"
            prefix = root + "/"
            if normalized.startswith(prefix):
                return "/mnt/t7/CRT-Media/" + normalized[len(prefix):]
        raise ValidationCatalogMetadataUnavailable(
            "validation media cache identity is invalid")

    @staticmethod
    def iterate_file_entries(connection: sqlite3.Connection, entries: list[FileRepoEntry]) -> None:
        """Takes a list of file entries, determines if they are cached and adds them if not."""

        cursor = connection.cursor()
        for entry in entries:
            # see if there is an entry already
            lookup_path = (FluidStatements.validation_cache_path(entry.path)
                           if in_validation_mode() else entry.path)
            cursor.execute("SELECT * FROM file_meta WHERE path = ?;", (lookup_path,))
            row = cursor.fetchone()
            if row:
                repo_entry = FileRepoEntry()
                repo_entry.from_db_row(row)

                # Check if we need to update this entry
                needs_update = False

                # Preserve the native comparison outside validation; validation
                # deliberately ignores the staged-path identity and compares the
                # immutable cache fields that establish freshness.
                if in_validation_mode():
                    if (entry.size, entry.last_mod) != (repo_entry.size, repo_entry.last_mod):
                        needs_update = True
                elif entry != repo_entry:
                    needs_update = True

                # Also update if this is an audio file with empty metadata
                # (happens when catalog was built before mutagen was installed)
                if repo_entry.media_type == 'audio' and not repo_entry.meta:
                    logging.getLogger("FLUID").info(f"Audio file missing metadata, will refresh: {entry.path}")
                    needs_update = True

                if needs_update:
                    if in_validation_mode():
                        raise ValidationCatalogMetadataUnavailable(
                            "cached media metadata is stale")
                    FluidStatements.update_file_entry(connection, entry)
                elif in_validation_mode():
                    FluidStatements.validate_cached_entry(repo_entry)
                elif repo_entry.media_type != 'audio':
                    FluidStatements.refresh_video_meta(connection, repo_entry)

            else:
                if in_validation_mode():
                    raise ValidationCatalogMetadataUnavailable(
                        "cached media metadata is missing")
                FluidStatements.add_file_entry(connection, entry)
        cursor.close()

    @staticmethod
    def validate_cached_entry(entry):
        if (
            not isinstance(entry.duration, (int, float)) or isinstance(entry.duration, bool)
            or entry.duration <= 0 or entry.media_type not in {"video", "audio"}
        ):
            raise ValidationCatalogMetadataUnavailable(
                "cached media metadata is invalid")
        if entry.meta:
            try:
                metadata = json.loads(entry.meta)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValidationCatalogMetadataUnavailable(
                    "cached media metadata is malformed") from exc
            if (not isinstance(metadata, dict)
                    or not isinstance(metadata.get("type"), str)):
                raise ValidationCatalogMetadataUnavailable(
                    "cached media metadata is untyped")

    @staticmethod
    def refresh_video_meta(connection: sqlite3.Connection, repo_entry: FileRepoEntry):
        from fs42.media_processor import MediaProcessor

        metadata = MediaProcessor.extract_metadata(repo_entry.path, 'video')
        new_meta = json.dumps(metadata) if metadata else ""

        if new_meta != (repo_entry.meta or ""):
            logging.getLogger("FLUID").info(f"NFO metadata changed, refreshing: {repo_entry.path}")
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE file_meta SET meta=?, last_checked=? WHERE path=?",
                (new_meta, scheduling_now(), repo_entry.path),
            )
            cursor.close()
            connection.commit()

    @staticmethod
    def trim_file_entries(connection: sqlite3.Connection, older_than: datetime):
        """Checks all files in the cache to ensure still on disk and removes them if not."""

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM file_meta WHERE last_updated < ?;", (older_than,))
        to_remove = []
        rows = cursor.fetchall()
        logging.getLogger("FLUID").info(f"Checking {len(rows)} files on the filesystem")
        for row in rows:
            repo_entry = FileRepoEntry()
            repo_entry.from_db_row(row)
            if not os.path.exists(repo_entry.path):
                logging.getLogger("FLUID").info(f"File not found on filesystem - will remove: {repo_entry}")
                to_remove.append(repo_entry.path)

        connection.execute("BEGIN TRANSACTION;")

        for p in to_remove:
            cursor.execute("DELETE from file_meta WHERE path=?", (p,))

        cursor.close()
        connection.commit()

    @staticmethod
    def update_file_entry(connection: sqlite3.Connection, entry: FileRepoEntry):
        """An old entry has changed, get the new stats and update it."""
        if in_validation_mode():
            raise ValidationCatalogMetadataUnavailable(
                "validation cannot refresh media metadata")
        from fs42.media_processor import MediaProcessor
        cursor = connection.cursor()
        now = scheduling_now()

        processed = MediaProcessor.process_one(entry.path, "processing", [])
        if not processed:
            return False
        entry.duration = processed.duration

        # Extract metadata: ID3 tags for audio, NFO sidecar for video
        media_type = MediaProcessor.get_media_type(entry.path)
        metadata = MediaProcessor.extract_metadata(entry.path, media_type)
        entry.meta = json.dumps(metadata) if metadata else ""

        logging.getLogger("FLUID").info(f"Updating existing file entry: {entry.path}")

        update = """UPDATE file_meta SET duration=?, size=?, last_mod=?, last_updated=?, last_checked=?, meta=?, media_type=?
        WHERE path=?;
        """
        values = (entry.duration, entry.size, entry.last_mod, now, now, entry.meta, media_type, entry.path)
        cursor.execute(update, values)
        cursor.close()
        connection.commit()

    @staticmethod
    def add_file_entry(connection: sqlite3.Connection, entry: FileRepoEntry):
        """This file isn't in the cache - add it."""
        if in_validation_mode():
            raise ValidationCatalogMetadataUnavailable(
                "validation cannot generate media metadata")
        from fs42.media_processor import MediaProcessor
        cursor = connection.cursor()
        now = scheduling_now()

        entry.first_added = now
        entry.last_checked = now
        entry.last_updates = now

        processed = MediaProcessor.process_one(entry.path, "processing", [])
        if not processed:
            return False

        entry.duration = processed.duration

        # Extract metadata: ID3 tags for audio, NFO sidecar for video
        media_type = MediaProcessor.get_media_type(entry.path)
        metadata = MediaProcessor.extract_metadata(entry.path, media_type)
        entry.meta = json.dumps(metadata) if metadata else ""

        logging.getLogger("FLUID").info(f"Caching new file entry: {entry}")

        # Note: to_db_row() should now include media_type column
        cursor.execute("INSERT INTO file_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", entry.to_db_row() + (media_type,))
        cursor.close()
        connection.commit()

    @staticmethod
    def add_break_points(connection: sqlite3.Connection, path: str, points: dict):
        """Add or update the break points for this file"""
        cursor = connection.cursor()
        now = scheduling_now()
        json_points = json.dumps(points)
        cursor.execute("REPLACE INTO break_points VALUES(?, ?, ?)", (path, json_points, now))
        cursor.close()
        connection.commit()

    @staticmethod
    def get_break_points(connection: sqlite3.Connection, path: str) -> dict:
        """Get the break points for this file"""
        cursor = connection.cursor()
        lookup_path = (FluidStatements.validation_cache_path(path)
                       if in_validation_mode() else path)
        cursor.execute("SELECT points FROM break_points WHERE path=?", (lookup_path,))
        row = cursor.fetchone()
        result = {}
        if row:
            result = json.loads(row[0])
        cursor.close()
        return result
    
    def delete_break_points(connection: sqlite3.Connection, path: str):
        """Delete any break points for this file"""
        cursor = connection.cursor()
        cursor.execute("DELETE FROM break_points WHERE path=?", (path,))
        cursor.close()
        connection.commit()

    @staticmethod
    def _chapter_lookup_path(path):
        return (FluidStatements.validation_cache_path(path)
                if in_validation_mode() else path)

    @staticmethod
    def _chapter_identity_path(path):
        if not in_validation_mode():
            return path
        normalized = os.path.normpath(os.fspath(path))
        context_root = os.path.normpath(current_validation_context().media_root)
        if normalized == "/media":
            return context_root
        if normalized.startswith("/media/"):
            return context_root + normalized[len("/media"):]
        return path

    @staticmethod
    def _chapter_duration(connection, lookup_path):
        row = connection.execute(
            "SELECT duration FROM file_meta WHERE path=?", (lookup_path,)
        ).fetchone()
        if (row is None or not isinstance(row[0], (int, float))
                or isinstance(row[0], bool) or not math.isfinite(row[0])
                or row[0] <= 0):
            raise ValueError("chapter metadata has no valid cached duration")
        return float(row[0])

    @staticmethod
    def _chapter_baseline_is_durable():
        root = Path(__file__).resolve().parents[1]
        backup_root = root / "runtime/director/chapter-cache-backups"
        target = backup_root / "migration-baseline.v1.json"
        descriptor = None
        try:
            root_info = os.stat(backup_root, follow_symlinks=False)
            if (not stat.S_ISDIR(root_info.st_mode)
                    or root_info.st_uid != os.geteuid()
                    or stat.S_IMODE(root_info.st_mode) != 0o700):
                return False
            descriptor = os.open(
                target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
            )
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_size > 4096):
                return False
            document = json.loads(os.read(descriptor, 4097))
            if (not isinstance(document, dict) or set(document) != {
                    "version", "state", "backup_id", "logical_identity",
                    "chapter_identity"}
                    or document["version"] != 1 or document["state"] != "pinned"
                    or not isinstance(document["backup_id"], str)
                    or not re.fullmatch(
                        r"chapter-cache-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}\.sqlite3",
                        document["backup_id"])
                    or not isinstance(document["logical_identity"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", document["logical_identity"])
                    or not isinstance(document["chapter_identity"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", document["chapter_identity"])):
                return False
            backup = os.stat(
                backup_root / document["backup_id"], follow_symlinks=False)
            return (
                stat.S_ISREG(backup.st_mode) and backup.st_uid == os.geteuid()
                and backup.st_nlink == 1 and stat.S_IMODE(backup.st_mode) == 0o600
            )
        except (OSError, TypeError, json.JSONDecodeError):
            return False
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @staticmethod
    def classify_chapter_points(connection: sqlite3.Connection, path: str,
                                *, lookup_path=None):
        """Classify stored chapter data without exposing an envelope to callers."""
        lookup_path = (FluidStatements._chapter_lookup_path(path)
                       if lookup_path is None else lookup_path)
        row = connection.execute(
            "SELECT points FROM chapter_points WHERE path=?", (lookup_path,)
        ).fetchone()
        if row is None:
            return {"status": "missing", "chapters": [], "raw": None}
        raw = row[0]
        try:
            loaded = json.loads(raw)
            duration = FluidStatements._chapter_duration(connection, lookup_path)
            if isinstance(loaded, list):
                if not loaded:
                    return {"status": "legacy_empty", "chapters": [], "raw": raw}
                chapters = validate_chapters(loaded, duration)
                return {
                    "status": "legacy_nonempty",
                    "chapters": [dict(item) for item in chapters],
                    "raw": raw,
                }
            if not isinstance(loaded, dict) or set(loaded) != {
                "attestation_version", "method", "media_identity", "chapters"
            }:
                raise ValueError("invalid chapter attestation")
            if loaded["attestation_version"] != 1 or loaded["method"] not in COMPLETED_METHODS:
                raise ValueError("unknown chapter attestation")
            identity = loaded["media_identity"]
            if (
                not isinstance(identity, dict) or set(identity) != {"size", "mtime_ns"}
                or not isinstance(identity["size"], int) or isinstance(identity["size"], bool)
                or identity["size"] < 0
                or not isinstance(identity["mtime_ns"], int)
                or isinstance(identity["mtime_ns"], bool)
            ):
                raise ValueError("invalid chapter attestation identity")
            info = os.stat(FluidStatements._chapter_identity_path(path))
            if (info.st_size, info.st_mtime_ns) != (
                    identity["size"], identity["mtime_ns"]):
                raise ValueError("stale chapter attestation identity")
            chapters = validate_chapters(loaded["chapters"], duration)
            if loaded["method"] == METHOD_SHORT and (chapters or duration >= 5 * 60):
                raise ValueError("invalid short-media chapter attestation")
            return {
                "status": "trusted_v1",
                "chapters": [dict(item) for item in chapters],
                "raw": raw,
            }
        except (OSError, TypeError, json.JSONDecodeError, ChapterAnalysisError) as exc:
            raise ValueError("invalid chapter attestation") from exc

    @staticmethod
    def add_chapter_points(connection: sqlite3.Connection, path: str,
                           analysis: CompletedChapterAnalysis, info,
                           previous=None, *, baseline_verified=False,
                           replace_questionable=False):
        """Publish one completed analysis; the caller owns the transaction."""
        if not isinstance(analysis, CompletedChapterAnalysis):
            raise TypeError("a completed chapter analysis is required")
        if not baseline_verified and not FluidStatements._chapter_baseline_is_durable():
            raise RuntimeError("chapter migration baseline is unavailable")
        previous = previous or FluidStatements.classify_chapter_points(connection, path)
        replaceable = {"missing", "legacy_empty"}
        if replace_questionable:
            replaceable.add("re_attestation_required")
        if previous["status"] not in replaceable:
            raise ValueError("chapter attestation is not replaceable")
        envelope = {
            "attestation_version": 1,
            "method": analysis.method,
            "media_identity": {
                "size": info.st_size,
                "mtime_ns": info.st_mtime_ns,
            },
            "chapters": analysis.as_list(),
        }
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        now = scheduling_now()
        if previous["status"] == "missing":
            connection.execute(
                "INSERT INTO chapter_points(path,points,last_updated) VALUES(?,?,?)",
                (path, encoded, now),
            )
        else:
            changed = connection.execute(
                "UPDATE chapter_points SET points=?,last_updated=? "
                "WHERE path=? AND points=?",
                (encoded, now, path, previous["raw"]),
            )
            if changed.rowcount != 1:
                raise RuntimeError("chapter attestation changed concurrently")

    @staticmethod
    def get_chapter_points(connection: sqlite3.Connection, path: str) -> dict:
        """Get the chapter points for this file. Returns {} if no chapters or never scanned."""
        classified = FluidStatements.classify_chapter_points(connection, path)
        return classified["chapters"] or {}

    @staticmethod
    def delete_chapter_points(connection: sqlite3.Connection, path: str):
        """Delete any chapter points for this file"""
        cursor = connection.cursor()
        cursor.execute("DELETE FROM chapter_points WHERE path=?", (path,))
        cursor.close()
        connection.commit()

    @staticmethod
    def init_db(connection: sqlite3.Connection):
        cursor = connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS file_meta (
                            path TEXT PRIMARY KEY,
                            duration REAL,
                            size INTEGER,
                            first_added TIMESTAMP,
                            last_mod TIMESTAMP,
                            last_checked TIMESTAMP,
                            last_updated TIMESTAMP,
                            meta TEXT
                            )
                            """)

        # Check if media_type column exists, add it if it doesn't
        cursor.execute("PRAGMA table_info(file_meta)")
        columns = [column[1] for column in cursor.fetchall()]

        if "media_type" not in columns:
            logging.getLogger("FLUID").info("Adding media_type column to file_meta table")
            cursor.execute("ALTER TABLE file_meta ADD COLUMN media_type TEXT DEFAULT 'video'")
            connection.commit()
            logging.getLogger("FLUID").info("Added media_type column to file_meta table")

        cursor.execute("""CREATE TABLE IF NOT EXISTS break_points (
                            path TEXT REFERENCES file_meta(path) PRIMARY KEY,
                            points TEXT,
                            last_updated TIMESTAMP
                            )
                       """)

        cursor.execute("""CREATE TABLE IF NOT EXISTS chapter_points (
                            path TEXT REFERENCES file_meta(path) PRIMARY KEY,
                            points TEXT,
                            last_updated TIMESTAMP
                            )
                       """)

        cursor.close()
