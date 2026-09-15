import logging
import sqlite3
import datetime
import os
import json
from fs42.media_processor import MediaProcessor
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
    def add_chapter_points(connection: sqlite3.Connection, path: str, points: dict):
        """Add or update the chapter points for this file"""
        cursor = connection.cursor()
        now = scheduling_now()
        json_points = json.dumps(points)
        cursor.execute("REPLACE INTO chapter_points VALUES(?, ?, ?)", (path, json_points, now))
        cursor.close()
        connection.commit()

    @staticmethod
    def get_chapter_points(connection: sqlite3.Connection, path: str) -> dict:
        """Get the chapter points for this file. Returns {} if no chapters or never scanned."""
        cursor = connection.cursor()
        lookup_path = (FluidStatements.validation_cache_path(path)
                       if in_validation_mode() else path)
        cursor.execute("SELECT points FROM chapter_points WHERE path=?", (lookup_path,))
        row = cursor.fetchone()
        result = {}
        if row:
            loaded = json.loads(row[0])
            # Return {} if empty list (file was scanned but has no chapters)
            # This way schedule build sees it as "no usable chapters"
            result = loaded if loaded else {}
        cursor.close()
        return result

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
