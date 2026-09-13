import errno
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from station_director.preservation import (
    canonical_sqlite_value,
    capture_media_manifest,
    compare_database_fingerprints,
    compare_media_manifests,
    fingerprint_and_clone_database,
    fingerprint_database,
    fingerprint_json_files,
)
from station_director.staged_schedule import (
    StagedScheduleError,
    capture_channel_history,
    inspect_required_schema,
    parse_catalog_references,
    reconcile_catalog,
    validate_all_catalog_reference_shapes,
    regenerate_with_callback,
)


def create_database(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE liquid_blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL,
            liquid_type TEXT NOT NULL,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            break_strategy TEXT NOT NULL,
            title TEXT NOT NULL,
            sequence_key TEXT,
            break_info TEXT,
            content_json TEXT NOT NULL,
            plan_json TEXT NOT NULL
        );
        CREATE TABLE catalog_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL,
            path TEXT NOT NULL,
            title TEXT NOT NULL,
            duration REAL NOT NULL,
            tag TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            hints TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            realpath TEXT,
            content_type TEXT DEFAULT 'feature',
            media_type TEXT DEFAULT 'video',
            UNIQUE(station, tag, path)
        );
        CREATE TABLE named_sequence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL,
            sequence_name TEXT NOT NULL,
            tag_path TEXT NOT NULL,
            start_perc REAL NOT NULL,
            end_perc REAL NOT NULL,
            current_index INTEGER NOT NULL,
            initialized INTEGER NOT NULL DEFAULT 1,
            parent_tag TEXT,
            UNIQUE(station, sequence_name, tag_path)
        );
        CREATE TABLE sequence_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fpath TEXT NOT NULL,
            sequence_index INTEGER NOT NULL,
            named_sequence_id INTEGER NOT NULL,
            FOREIGN KEY(named_sequence_id) REFERENCES named_sequence(id)
        );
        CREATE TABLE sequence_group_state (
            station TEXT NOT NULL,
            sequence_name TEXT NOT NULL,
            parent_tag TEXT NOT NULL,
            active_tag_path TEXT NOT NULL,
            PRIMARY KEY(station, sequence_name, parent_tag)
        );
        CREATE TABLE file_meta (
            path TEXT PRIMARY KEY, duration REAL, size INTEGER,
            first_added TIMESTAMP, last_mod TIMESTAMP, last_checked TIMESTAMP,
            last_updated TIMESTAMP, meta TEXT, media_type TEXT DEFAULT 'video'
        );
        CREATE TABLE break_points (
            path TEXT REFERENCES file_meta(path) PRIMARY KEY,
            points TEXT, last_updated TIMESTAMP
        );
        CREATE TABLE chapter_points (
            path TEXT REFERENCES file_meta(path) PRIMARY KEY,
            points TEXT, last_updated TIMESTAMP
        );
        """
    )
    connection.commit()
    return connection


def plan(path):
    return json.dumps(
        [
            {
                "path": path,
                "skip": 0,
                "duration": 3600,
                "is_stream": False,
                "content_type": "feature",
                "media_type": "video",
            }
        ]
    )


def catalog_row(station, path, title, tag, count=0):
    return {
        "station": station,
        "path": path,
        "title": title,
        "duration": 3600.0,
        "tag": tag,
        "count": count,
        "hints": None,
        "created_at": "2026-09-01 00:00:00",
        "updated_at": "2026-09-01 00:00:00",
        "realpath": path,
        "content_type": "feature",
        "media_type": "video",
    }


def insert_block(connection, station, start, end, catalog_id, media_path, title="Show"):
    connection.execute(
        "INSERT INTO liquid_blocks "
        "(station,liquid_type,start_time,end_time,break_strategy,title,sequence_key,break_info,content_json,plan_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            station,
            "LiquidBlock",
            start,
            end,
            "end",
            title,
            None,
            "{}",
            json.dumps(catalog_id),
            plan(media_path),
        ),
    )


class DatabaseFingerprintTests(unittest.TestCase):
    def test_transactional_readonly_clone_and_logical_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.db"
            clone = Path(directory) / "clone.db"
            connection = create_database(source)
            connection.execute(
                "INSERT INTO file_meta(path,duration,size,meta,media_type) VALUES(?,?,?,?,?)",
                ("a", 1.25, 7, sqlite3.Binary(b"blob"), "video"),
            )
            connection.commit()
            connection.close()
            before = source.read_bytes()
            source_fingerprint = fingerprint_and_clone_database(source, clone)
            clone_fingerprint = fingerprint_database(clone)
            self.assertEqual(
                source_fingerprint["logical"]["digest"],
                clone_fingerprint["logical"]["digest"],
            )
            self.assertEqual(source.read_bytes(), before)
            self.assertTrue(compare_database_fingerprints(source_fingerprint, clone_fingerprint)["preserved"])

    def test_typed_encoding_distinguishes_sqlite_storage_classes(self):
        values = [None, 1, 1.0, "1", b"1", -0.0, 0.0]
        encoded = [canonical_sqlite_value(value) for value in values]
        self.assertEqual(len(encoded), len(set(encoded)))

    def test_schema_inspection_uses_singular_real_table_names(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "db.sqlite"
            connection = create_database(database)
            schema = inspect_required_schema(connection)
            self.assertIn("named_sequence", schema["tables"])
            self.assertNotIn("named_sequences", schema["tables"])
            connection.execute("DROP TABLE sequence_group_state")
            with self.assertRaisesRegex(StagedScheduleError, "sequence_group_state"):
                inspect_required_schema(connection)
            connection.close()


class MediaFingerprintTests(unittest.TestCase):
    def test_byte_safe_paths_symlink_nontraversal_and_bounded_difference(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = os.fsencode(directory)
            descriptor = os.open(root + b"/bad-\xff.mp4", os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(descriptor)
            Path(directory, "external").symlink_to(outside, target_is_directory=True)
            Path(outside, "not-walked.mp4").write_bytes(b"x")
            first = capture_media_manifest(directory)
            self.assertEqual(first.summary["entry_count"], 3)
            Path(directory, "new.mp4").write_bytes(b"payload is not hashed")
            second = capture_media_manifest(directory)
            comparison = compare_media_manifests(first, second, difference_limit=1)
            self.assertFalse(comparison["preserved"])
            self.assertEqual(len(comparison["differences"]), 1)
            self.assertNotIn("manifest", comparison)
            first.close()
            second.close()

    def test_xattr_unsupported_is_recorded_as_capability(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "station_director.preservation.os.listxattr",
            side_effect=OSError(errno.ENOTSUP, "unsupported"),
        ):
            manifest = capture_media_manifest(directory)
            try:
                self.assertIn(False, manifest.summary["xattr_capabilities"].values())
            finally:
                manifest.close()

    def test_unexpected_xattr_error_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "station_director.preservation.os.listxattr",
            side_effect=OSError(errno.EIO, "failure"),
        ):
            with self.assertRaisesRegex(Exception, "xattrs"):
                capture_media_manifest(directory)

    def test_json_fingerprint_detects_name_content_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{"a":1}\n')
            first = fingerprint_json_files({"confs/config.json": path})
            path.write_text('{"a":2}\n')
            second = fingerprint_json_files({"confs/config.json": path})
            self.assertNotEqual(first["digest"], second["digest"])


class HistoryTests(unittest.TestCase):
    def test_catalog_reuse_requires_complete_typed_semantics(self):
        mutations = {
            "duration": lambda row: row.update(duration=3599.0),
            "title": lambda row: row.update(title="Changed"),
            "tag": lambda row: row.update(tag="Changed tag"),
            "storage_class": lambda row: row.update(duration=3600),
            "count": lambda row: row.update(count=9),
            "created_at": lambda row: row.update(created_at="changed-created"),
            "updated_at": lambda row: row.update(updated_at="changed-updated"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                connection = create_database(Path(directory) / "db.sqlite")
                original = catalog_row("Action", "/mnt/t7/CRT-Media/a.mp4", "Show A", "Show A")
                connection.execute(
                    "INSERT INTO catalog_entries "
                    "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                    "VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [original[key] for key in original],
                )
                generated = catalog_row("Action", "/media/a.mp4", "Show A", "Show A")
                mutate(generated)
                active = reconcile_catalog(connection, "Action", [generated], set())
                self.assertEqual(active, {2})
                self.assertIsNone(connection.execute(
                    "SELECT id FROM catalog_entries WHERE id=1"
                ).fetchone())
                connection.close()

    def test_catalog_reuse_compares_break_and_chapter_metadata(self):
        for changed_table in ("break_points", "chapter_points"):
          with self.subTest(changed_table=changed_table), tempfile.TemporaryDirectory() as directory:
            connection = create_database(Path(directory) / "db.sqlite")
            original = catalog_row("Action", "/mnt/t7/CRT-Media/a.mp4", "Show A", "Show A")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?)",
                [original[key] for key in original],
            )
            identity = ("Action", "Show A", "crt-media:/a.mp4")
            columns = ("path", "points", "last_updated")
            original_meta = {identity: {
                "file_meta": None, "file_meta_columns": (),
                "break_points": ("crt-media:/a.mp4", "[1]", "t"),
                "break_points_columns": columns,
                "chapter_points": ("crt-media:/a.mp4", "[2]", "t"),
                "chapter_points_columns": columns,
            }}
            changed = dict(original_meta[identity])
            changed[changed_table] = ("crt-media:/a.mp4", "[9]", "t")
            generated_meta = {identity: changed}
            active = reconcile_catalog(
                connection, "Action",
                [catalog_row("Action", "/media/a.mp4", "Show A", "Show A")],
                set(), original_media_metadata=original_meta,
                generated_media_metadata=generated_meta,
            )
            self.assertEqual(active, {2})
            connection.close()

    def test_new_catalog_id_uses_maximum_and_sqlite_sequence_explicitly(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = create_database(Path(directory) / "db.sqlite")
            row = catalog_row("Old", "/mnt/t7/CRT-Media/old.mp4", "Old", "Old")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (20,?,?,?,?,?,?,?,?,?,?,?,?)",
                [row[key] for key in row],
            )
            connection.execute("DELETE FROM catalog_entries WHERE id=20")
            active = reconcile_catalog(
                connection, "Action",
                [catalog_row("Action", "/media/a.mp4", "A", "A")], set(),
            )
            self.assertEqual(active, {21})
            self.assertEqual(connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
            ).fetchone()[0], 21)
            connection.close()

    def test_transient_native_ids_do_not_advance_provisional_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = create_database(Path(directory) / "db.sqlite")
            transient = catalog_row("Action", "/media/transient.mp4", "Transient", "Transient")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (99,?,?,?,?,?,?,?,?,?,?,?,?)",
                [transient[key] for key in transient],
            )
            active = reconcile_catalog(
                connection, "Action",
                [catalog_row("Action", "/media/a.mp4", "A", "A")], set(),
                original_rows=[], allocation_floor=20,
            )
            self.assertEqual(active, {21})
            self.assertEqual(connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='catalog_entries'"
            ).fetchone()[0], 21)
            connection.close()

    def test_supported_and_unsupported_catalog_reference_shapes(self):
        self.assertEqual(parse_catalog_references("7"), (7,))
        self.assertEqual(parse_catalog_references("[7,8]"), (7, 8))
        for value in ("null", "true", "1.5", '"7"', "{}", "[]", "[7,[8]]", "0"):
            with self.subTest(value=value), self.assertRaises(StagedScheduleError):
                parse_catalog_references(value)

    def test_all_reference_shapes_are_checked_before_future_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = create_database(Path(directory) / "db.sqlite")
            connection.execute(
                "INSERT INTO liquid_blocks "
                "(station,liquid_type,start_time,end_time,break_strategy,title,content_json,plan_json) "
                "VALUES ('Action','LiquidBlock','2026-09-20','2026-09-21','end','bad','{}','[]')"
            )
            with self.assertRaisesRegex(StagedScheduleError, "unsupported catalog reference"):
                validate_all_catalog_reference_shapes(connection)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM liquid_blocks").fetchone()[0], 1)
            connection.close()

    def _populated_database(self, database, media):
        connection = create_database(database)
        files = {}
        for name in ("a.mp4", "b.mp4", "other.mp4", "wio.mp4"):
            target = media / name
            target.write_bytes(b"x")
            files[name] = target
        entries = (
            (1, "Action", "a.mp4", "Show A", "Show A"),
            (2, "Action", "b.mp4", "Show B", "Show B"),
            (3, "Other", "other.mp4", "Other", "Other"),
            (4, "Watch In Order", "wio.mp4", "WIO", "WIO"),
        )
        for catalog_id, station, name, title, tag in entries:
            host = f"/mnt/t7/CRT-Media/{name}"
            row = catalog_row(station, host, title, tag)
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (catalog_id, *[row[key] for key in row]),
            )
        insert_block(
            connection,
            "Action",
            "2026-09-13 23:00:00",
            "2026-09-14 01:00:00",
            1,
            "/mnt/t7/CRT-Media/a.mp4",
        )
        insert_block(connection, "Action", "2026-09-14 01:00:00", "2026-09-14 02:00:00", 2, "/media/b.mp4")
        insert_block(connection, "Action", "2026-09-14 04:00:00", "2026-09-14 05:00:00", 2, "/media/b.mp4")
        insert_block(connection, "Other", "2026-09-14 00:00:00", "2026-09-14 06:00:00", 3, "/media/other.mp4")
        insert_block(connection, "Watch In Order", "2026-09-14 00:00:00", "2026-09-14 06:00:00", 4, "/media/wio.mp4")
        connection.execute(
            "INSERT INTO named_sequence(id,station,sequence_name,tag_path,start_perc,end_perc,current_index,initialized,parent_tag) "
            "VALUES(1,'Watch In Order','wio','WIO',0,1,2,1,NULL)"
        )
        connection.execute(
            "INSERT INTO sequence_entries(id,fpath,sequence_index,named_sequence_id) VALUES(1,'/media/wio.mp4',0,1)"
        )
        connection.execute(
            "INSERT INTO named_sequence(id,station,sequence_name,tag_path,start_perc,end_perc,current_index,initialized,parent_tag) "
            "VALUES(2,'Action','action-order','Show A',0,1,0,1,NULL)"
        )
        connection.execute(
            "INSERT INTO sequence_entries(id,fpath,sequence_index,named_sequence_id) VALUES(2,'/media/a.mp4',0,2)"
        )
        connection.execute(
            "INSERT INTO sequence_group_state(station,sequence_name,parent_tag,active_tag_path) "
            "VALUES('Action','action-order','Action','Show A')"
        )
        connection.commit()
        return connection, files

    def test_preserves_crossing_history_catalog_ids_and_original_horizon(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            media = base / "media"
            media.mkdir()
            connection, unused_files = self._populated_database(base / "db.sqlite", media)
            original_other = connection.execute(
                "SELECT * FROM liquid_blocks WHERE station='Other'"
            ).fetchall()
            original_wio = connection.execute(
                "SELECT * FROM liquid_blocks WHERE station='Watch In Order'"
            ).fetchall()
            generated = {
                "Action": [
                    catalog_row("Action", "/media/a.mp4", "Show A", "Show A"),
                    catalog_row("Action", "/media/b.mp4", "Show B", "Show B"),
                ]
            }

            def scheduler(con, channel, start, horizon, active_ids):
                self.assertEqual(channel, "Action")
                self.assertEqual(start, "2026-09-14 01:00:00")
                self.assertEqual(horizon, "2026-09-14 05:00:00")
                self.assertEqual(active_ids, {2, 5})
                insert_block(con, channel, start, "2026-09-14 03:00:00", 2, "/media/b.mp4")
                insert_block(con, channel, "2026-09-14 03:00:00", "2026-09-14 05:30:00", 1, "/media/a.mp4")

            report = regenerate_with_callback(
                connection,
                ["Action"],
                "2026-09-14 00:00:00",
                "2026-09-14 03:00:00",
                generated,
                scheduler,
                sandbox_media_root=media,
            )
            channel = report["channels"][0]
            self.assertEqual(channel["effective_horizon"], "2026-09-14 05:00:00")
            self.assertEqual(channel["regeneration_start"], "2026-09-14 01:00:00")
            self.assertTrue(channel["coverage"]["effective_horizon_crossing_ids"])
            retained = connection.execute(
                "SELECT id,start_time,end_time,content_json FROM liquid_blocks "
                "WHERE station='Action' AND start_time<'2026-09-14 00:00:00'"
            ).fetchone()
            self.assertEqual(retained[1:3], ("2026-09-13 23:00:00", "2026-09-14 01:00:00"))
            self.assertEqual(json.loads(retained[3]), 1)
            retained_catalog = connection.execute(
                "SELECT id,path,realpath FROM catalog_entries WHERE id=1"
            ).fetchone()
            self.assertEqual(
                retained_catalog,
                (1, "/mnt/t7/CRT-Media/a.mp4", "/mnt/t7/CRT-Media/a.mp4"),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT id FROM catalog_entries WHERE station='Action' AND path='/media/a.mp4'"
                ).fetchone(),
                (5,),
            )
            self.assertEqual(connection.execute("SELECT * FROM liquid_blocks WHERE station='Other'").fetchall(), original_other)
            self.assertEqual(connection.execute("SELECT * FROM liquid_blocks WHERE station='Watch In Order'").fetchall(), original_wio)
            connection.close()

    def test_gap_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            media = base / "media"
            media.mkdir()
            connection, unused = self._populated_database(base / "db.sqlite", media)
            generated = {"Action": [catalog_row("Action", "/media/a.mp4", "Show A", "Show A")]}

            def scheduler(con, unused_channel, unused_start, unused_horizon, unused_ids):
                insert_block(con, "Action", "2026-09-14 02:00:00", "2026-09-14 05:00:00", 1, "/media/a.mp4")

            with self.assertRaisesRegex(StagedScheduleError, "coverage failure"):
                regenerate_with_callback(
                    connection, ["Action"], "2026-09-14 00:00:00",
                    "2026-09-14 03:00:00", generated, scheduler,
                    sandbox_media_root=media,
                )
            connection.close()

    def test_unaffected_and_wio_mutation_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            media = base / "media"
            media.mkdir()
            connection, unused = self._populated_database(base / "db.sqlite", media)
            generated = {
                "Action": [catalog_row("Action", "/media/a.mp4", "Show A", "Show A")]
            }

            def scheduler(con, unused_channel, start, unused_horizon, unused_ids):
                insert_block(
                    con,
                    "Action",
                    start,
                    "2026-09-14 05:00:00",
                    1,
                    "/media/a.mp4",
                )
                con.execute(
                    "UPDATE named_sequence SET current_index=99 WHERE station='Watch In Order'"
                )

            with self.assertRaisesRegex(StagedScheduleError, "protected named_sequence"):
                regenerate_with_callback(
                    connection,
                    ["Action"],
                    "2026-09-14 00:00:00",
                    "2026-09-14 03:00:00",
                    generated,
                    scheduler,
                    sandbox_media_root=media,
                )
            connection.close()

    def _assert_sequence_mutation_fails(self, mutation, expected_table):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            media = base / "media"
            media.mkdir()
            connection, unused = self._populated_database(base / "db.sqlite", media)
            generated = {
                "Action": [catalog_row("Action", "/media/a.mp4", "Show A", "Show A")]
            }

            def scheduler(con, unused_channel, start, unused_horizon, unused_ids):
                insert_block(
                    con,
                    "Action",
                    start,
                    "2026-09-14 05:00:00",
                    1,
                    "/media/a.mp4",
                )
                mutation(con)

            with self.assertRaisesRegex(
                StagedScheduleError, f"protected {expected_table} rows changed"
            ):
                regenerate_with_callback(
                    connection,
                    ["Action"],
                    "2026-09-14 00:00:00",
                    "2026-09-14 03:00:00",
                    generated,
                    scheduler,
                    sandbox_media_root=media,
                )
            connection.close()

    def test_affected_named_sequence_update_fails_closed(self):
        self._assert_sequence_mutation_fails(
            lambda connection: connection.execute(
                "UPDATE named_sequence SET current_index=7 WHERE station='Action'"
            ),
            "named_sequence",
        )

    def test_affected_sequence_entry_insertion_fails_closed(self):
        self._assert_sequence_mutation_fails(
            lambda connection: connection.execute(
                "INSERT INTO sequence_entries(fpath,sequence_index,named_sequence_id) "
                "VALUES('/media/b.mp4',1,2)"
            ),
            "sequence_entries",
        )

    def test_affected_sequence_group_deletion_fails_closed(self):
        self._assert_sequence_mutation_fails(
            lambda connection: connection.execute(
                "DELETE FROM sequence_group_state WHERE station='Action'"
            ),
            "sequence_group_state",
        )

    def test_missing_reference_and_disabled_scheduler_fail_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "db.sqlite"
            connection = create_database(database)
            insert_block(connection, "Action", "2026-09-13 23:00:00", "2026-09-14 01:00:00", 999, "/media/missing.mp4")
            connection.commit()
            with self.assertRaisesRegex(StagedScheduleError, "missing catalog IDs"):
                capture_channel_history(
                    connection, "Action", "2026-09-14 00:00:00", "2026-09-21 00:00:00"
                )
            before = connection.total_changes
            with self.assertRaisesRegex(StagedScheduleError, "not yet enabled"):
                regenerate_with_callback(
                    connection, [], "2026-09-14 00:00:00", "2026-09-21 00:00:00", {}, None
                )
            self.assertEqual(connection.total_changes, before)
            connection.close()


if __name__ == "__main__":
    unittest.main()
