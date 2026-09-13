import os
import json
import sqlite3
import logging

_logger = logging.getLogger("MetadataIO")


class MetadataIO:

    @staticmethod
    def _default_db_path():
        from fs42.station_manager import StationManager
        return StationManager().server_conf["db_path"]

    @staticmethod
    def normalize(meta):
        if not isinstance(meta, dict):
            return meta

        # Legacy audio rows stored the year under `date` and carried no type
        # discriminator. Bring them up to the normalized shape on read.
        needs_year = "date" in meta and "year" not in meta
        needs_type = "type" not in meta
        if needs_year or needs_type:
            meta = dict(meta)
            if needs_year:
                meta["year"] = meta.pop("date")
            if needs_type:
                meta["type"] = "music"
        return meta

    @staticmethod
    def decode_rows(file_paths, rows):
        """Apply the route's path matching and metadata normalization to loaded rows."""
        return MetadataIO.decode_rows_by_real(
            MetadataIO.paths_by_real(file_paths), rows
        )

    @staticmethod
    def paths_by_real(file_paths):
        by_real = {}
        for path in file_paths:
            try:
                real_path = os.path.realpath(os.path.abspath(path))
            except Exception:
                continue
            by_real.setdefault(real_path, []).append(path)
        return by_real

    @staticmethod
    def decode_rows_by_real(by_real, rows):
        results = {}
        for real_path, meta_json in rows:
            if not meta_json:
                continue
            meta = MetadataIO.normalize(json.loads(meta_json))
            for original in by_real.get(real_path, []):
                results[original] = meta
        return results

    @staticmethod
    def read_many(file_paths, db_path=None):
        """Read metadata for many paths on one connection.

        Returns {original_path: meta} for paths that have metadata. Callers
        fetching more than one path should prefer this over read(), which
        opens a connection per call.
        """
        if not file_paths:
            return {}

        if db_path is None:
            try:
                db_path = MetadataIO._default_db_path()
            except Exception as e:
                _logger.warning(f"Could not resolve db_path for metadata read: {e}")
                return {}

        # Rows are keyed by realpath, so map back to what the caller asked for.
        # Several inputs can resolve to the same real file.
        by_real = MetadataIO.paths_by_real(file_paths)

        results = {}
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            real_paths = list(by_real.keys())
            # Chunked to stay under SQLite's bound-variable limit
            chunk_size = 500
            for i in range(0, len(real_paths), chunk_size):
                chunk = real_paths[i:i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                cursor.execute(f"SELECT path, meta FROM file_meta WHERE path IN ({placeholders})", chunk)
                results.update(MetadataIO.decode_rows_by_real(by_real, cursor.fetchall()))
            cursor.close()
        except Exception as e:
            _logger.warning(f"Could not batch read metadata: {e}")
        finally:
            if conn is not None:
                conn.close()

        return results

    @staticmethod
    def read(file_path, db_path=None):
        if db_path is None:
            try:
                db_path = MetadataIO._default_db_path()
            except Exception as e:
                _logger.warning(f"Could not resolve db_path for metadata read: {e}")
                return None

        conn = None
        try:
            real_path = os.path.realpath(os.path.abspath(file_path))
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT meta FROM file_meta WHERE path = ?", (real_path,))
            row = cursor.fetchone()
            cursor.close()

            if row and row[0]:
                return MetadataIO.normalize(json.loads(row[0]))
        except Exception as e:
            _logger.warning(f"Could not read metadata for {file_path}: {e}")
        finally:
            if conn is not None:
                conn.close()

        return None
