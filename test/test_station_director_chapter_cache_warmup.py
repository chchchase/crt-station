import argparse
import ast
import contextlib
import datetime
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from station_director import chapter_cache_warmup as warmup


def create_database(path):
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE file_meta (
            path TEXT PRIMARY KEY,
            duration REAL NOT NULL,
            size INTEGER,
            last_mod REAL,
            last_validate TEXT,
            last_update TEXT,
            json TEXT,
            media_type TEXT
        );
        CREATE TABLE chapter_points (
            path TEXT PRIMARY KEY,
            points TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );
        CREATE TABLE catalog_entries (
            id INTEGER PRIMARY KEY,
            filepath TEXT
        );
    """)
    connection.commit()
    return connection


def database_digests(path):
    connection = sqlite3.connect(path)
    try:
        return (
            warmup._logical_digest(connection),
            warmup._logical_digest(connection, exclude_tables={"chapter_points"}),
        )
    finally:
        connection.close()


class ChapterAnalysisTests(unittest.TestCase):
    def test_short_media_is_a_completed_empty_attestation(self):
        from fs42.chapter_analysis import METHOD_SHORT, analyze_chapters

        result = analyze_chapters("unused", 299, popen=mock.Mock())
        self.assertEqual(result.method, METHOD_SHORT)
        self.assertEqual(result.chapters, ())

    def test_validation_guard_precedes_path_and_process_work(self):
        from fs42.chapter_analysis import analyze_chapters
        from fs42.scheduling_context import (
            ValidationCatalogMetadataUnavailable,
            ValidationSchedulingContext,
            activate_validation_context,
        )
        import datetime

        start = datetime.datetime(2026, 9, 14, 0, 0)
        context = ValidationSchedulingContext(
            reference_clock=start, start_time=start,
            end_time=start + datetime.timedelta(hours=1), seed=1,
        )
        launcher = mock.Mock()
        with activate_validation_context(context):
            with self.assertRaises(ValidationCatalogMetadataUnavailable):
                analyze_chapters("missing", 600, popen=launcher)
        launcher.assert_not_called()

    def test_successful_empty_probe_is_distinct_from_failure(self):
        from fs42.chapter_analysis import (
            ChapterAnalysisError, METHOD_FFPROBE, _parse_probe_output,
        )

        result = _parse_probe_output(b'{"chapters":[]}', 600)
        self.assertEqual(result, ())
        with self.assertRaises(ChapterAnalysisError):
            _parse_probe_output(b'{', 600)
        self.assertEqual(METHOD_FFPROBE, "ffprobe_show_chapters_v1")

    def test_native_chapter_detect_never_turns_failure_into_empty(self):
        from fs42.chapter_analysis import ChapterAnalysisError
        from fs42.media_processor import MediaProcessor

        with mock.patch(
                "fs42.chapter_analysis.analyze_chapters",
                side_effect=ChapterAnalysisError("probe_nonzero")), mock.patch(
                    "fs42.media_processor.logging.getLogger", return_value=mock.Mock()):
            self.assertIsNone(MediaProcessor.chapter_detect("opaque", 600))

    def test_probe_contract_is_one_local_bounded_invocation(self):
        from fs42.chapter_analysis import analyze_chapters

        read_fd, write_fd = os.pipe()
        os.write(write_fd, b'{"chapters":[]}')
        os.close(write_fd)

        class Output:
            def fileno(self):
                return read_fd

            def close(self):
                os.close(read_fd)

        class Process:
            pid = 12345
            stdout = Output()

            @staticmethod
            def poll():
                return 0

            @staticmethod
            def wait(timeout=None):
                return 0

        launcher = mock.Mock(return_value=Process())
        result = analyze_chapters("/proc/self/fd/9", 600, pass_fds=(9,), popen=launcher)
        self.assertEqual(result.chapters, ())
        launcher.assert_called_once()
        arguments = launcher.call_args.args[0]
        self.assertEqual(arguments[0], "/usr/bin/ffprobe")
        self.assertEqual(arguments[arguments.index("-protocol_whitelist") + 1], "file,pipe")
        self.assertEqual(launcher.call_args.kwargs["pass_fds"], (9,))


class ChapterEnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.media = self.root / "clip.mp4"
        self.media.write_bytes(b"synthetic")
        self.database = self.root / "db.sqlite3"
        self.connection = create_database(self.database)
        info = self.media.stat()
        self.connection.execute(
            "INSERT INTO file_meta(path,duration,size,last_mod,last_validate,last_update,json,media_type) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (os.fspath(self.media), 600.0, info.st_size, info.st_mtime,
             "now", "now", "{}", "video"),
        )
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def test_legacy_nonempty_reader_remains_raw_list_compatible(self):
        from fs42.fluid_statements import FluidStatements

        chapters = [{"chapter_start": 0, "chapter_end": 10, "segment_duration": 10}]
        self.connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)",
            (os.fspath(self.media), json.dumps(chapters), "now"),
        )
        self.connection.commit()
        self.assertEqual(FluidStatements.get_chapter_points(
            self.connection, os.fspath(self.media)), chapters)

    def test_completed_empty_envelope_never_reaches_list_consumer(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE
        from fs42.fluid_statements import FluidStatements

        info = self.media.stat()
        with self.connection:
            FluidStatements.add_chapter_points(
                self.connection, os.fspath(self.media),
                CompletedChapterAnalysis(METHOD_FFPROBE, ()), info,
                baseline_verified=True,
            )
        raw = json.loads(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone()[0])
        self.assertEqual(raw["attestation_version"], 1)
        self.assertEqual(FluidStatements.get_chapter_points(
            self.connection, os.fspath(self.media)), {})

    def test_legacy_empty_is_replaceable_but_failed_result_is_not_writable(self):
        from fs42.fluid_statements import FluidStatements

        self.connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)",
            (os.fspath(self.media), "[]", "old"),
        )
        self.connection.commit()
        with self.assertRaises(TypeError):
            with self.connection:
                FluidStatements.add_chapter_points(
                    self.connection, os.fspath(self.media), [], self.media.stat())
        self.assertEqual(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone(), ("[]",))

    def test_no_versioned_write_precedes_durable_migration_baseline(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE
        from fs42.fluid_statements import FluidStatements

        with mock.patch.object(
                FluidStatements, "_chapter_baseline_is_durable", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "baseline"):
                with self.connection:
                    FluidStatements.add_chapter_points(
                        self.connection, os.fspath(self.media),
                        CompletedChapterAnalysis(METHOD_FFPROBE, ()),
                        self.media.stat(),
                    )
        self.assertIsNone(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone())

    def test_ordinary_completed_write_is_enabled_after_durable_baseline(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE
        from fs42.fluid_statements import FluidStatements

        with mock.patch.object(
                FluidStatements, "_chapter_baseline_is_durable", return_value=True):
            with self.connection:
                FluidStatements.add_chapter_points(
                    self.connection, os.fspath(self.media),
                    CompletedChapterAnalysis(METHOD_FFPROBE, ()),
                    self.media.stat(),
                )
        self.assertEqual(json.loads(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone()[0])["chapters"], [])

    def test_stale_attestation_is_rejected(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE
        from fs42.fluid_statements import FluidStatements

        with self.connection:
            FluidStatements.add_chapter_points(
                self.connection, os.fspath(self.media),
                CompletedChapterAnalysis(METHOD_FFPROBE, ()), self.media.stat(),
                baseline_verified=True)
        self.media.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            FluidStatements.get_chapter_points(self.connection, os.fspath(self.media))

    def test_future_native_failure_does_not_create_legacy_empty_row(self):
        from fs42.chapter_analysis import ChapterAnalysisError
        from fs42.fluid_builder import FluidBuilder
        from fs42.fluid_statements import FluidStatements

        entry = type("Entry", (), {
            "realpath": os.fspath(self.media), "duration": 600.0,
        })()
        builder = FluidBuilder.__new__(FluidBuilder)
        builder.db_path = self.database
        builder._l = mock.Mock()
        with mock.patch(
                "fs42.fluid_builder.analyze_chapters",
                side_effect=ChapterAnalysisError("probe_nonzero")), mock.patch.object(
                    FluidStatements,
                    "_chapter_baseline_is_durable", return_value=True):
            builder.scan_chapters_for_entries([entry])
        self.assertIsNone(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone())

    def test_ordinary_scan_defers_without_baseline_before_analyzer(self):
        from fs42.fluid_builder import FluidBuilder
        from fs42.fluid_statements import FluidStatements

        entry = type("Entry", (), {
            "realpath": os.fspath(self.media), "duration": 600.0,
        })()
        builder = FluidBuilder.__new__(FluidBuilder)
        builder.db_path = self.database
        builder._l = mock.Mock()
        with mock.patch.object(
                FluidStatements, "_chapter_baseline_is_durable",
                return_value=False), mock.patch(
                    "fs42.fluid_builder.analyze_chapters") as analyze:
            builder.scan_chapters_for_entries([entry])
        analyze.assert_not_called()
        self.assertIsNone(self.connection.execute(
            "SELECT points FROM chapter_points").fetchone())


class PrivateGenerationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "live.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def test_wal_only_committed_data_is_read_from_private_copy_without_live_sqlite_open(self):
        writer = sqlite3.connect(self.database)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE sample(value INTEGER)")
        writer.execute("INSERT INTO sample VALUES(42)")
        writer.commit()
        shm = Path(str(self.database) + "-shm")
        before = shm.read_bytes()
        before_info = shm.stat()
        with warmup.stable_private_generation(
                self.database, temporary_parent=self.root) as generation:
            raw_main = generation.raw_database.read_bytes()
            raw_wal = Path(str(generation.raw_database) + "-wal").read_bytes()
            copy = sqlite3.connect(generation.database)
            try:
                self.assertEqual(copy.execute("SELECT value FROM sample").fetchone(), (42,))
            finally:
                copy.close()
            self.assertEqual(generation.raw_database.read_bytes(), raw_main)
            self.assertEqual(Path(str(generation.raw_database) + "-wal").read_bytes(), raw_wal)
        after_info = shm.stat()
        self.assertEqual(shm.read_bytes(), before)
        self.assertEqual((after_info.st_size, after_info.st_mtime_ns),
                         (before_info.st_size, before_info.st_mtime_ns))
        writer.close()

    def test_hot_journal_is_rejected_before_any_sqlite_open(self):
        self.database.write_bytes(b"not opened")
        Path(str(self.database) + "-journal").write_bytes(b"pending")
        with mock.patch.object(warmup.sqlite3, "connect") as connect:
            with self.assertRaisesRegex(warmup.MaintenanceError, "planning_journal_present"):
                warmup.plan("p-20260912T064839Z-04191281", database=self.database,
                            temporary_parent=self.root)
        connect.assert_not_called()

    def test_generation_change_discards_provisional_copy(self):
        self.database.write_bytes(b"initial")
        original = warmup._generation_identity
        calls = 0

        def identity(path, **kwargs):
            nonlocal calls
            calls += 1
            value = original(path, **kwargs)
            if calls == 2:
                self.database.write_bytes(b"changed")
                return original(path, **kwargs)
            return value

        with mock.patch.object(warmup, "_generation_identity", side_effect=identity):
            with self.assertRaisesRegex(warmup.MaintenanceError, "planning_state_changed"):
                with warmup.stable_private_generation(
                        self.database, temporary_parent=self.root):
                    pass


class InventorySafetyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "safe").mkdir()
        (self.root / "safe/clip.mp4").write_bytes(b"media")

    def tearDown(self):
        self.temporary.cleanup()

    def test_component_symlink_is_never_followed(self):
        (self.root / "linked").symlink_to(self.root / "safe", target_is_directory=True)
        with mock.patch.object(warmup, "MEDIA_ROOT", self.root):
            with warmup._open_root() as root_fd:
                with self.assertRaises(OSError):
                    with warmup._open_media(root_fd, ("linked", "clip.mp4")):
                        pass

    def test_duplicate_channel_candidates_share_one_identity(self):
        item = warmup.MediaItem(("safe", "clip.mp4"), 5, 1)
        inventory = {}
        inventory[item.relative] = item
        inventory[item.relative] = item
        self.assertEqual(len(inventory), 1)


class BackupProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.backups = self.root / "backups"
        self.backups.mkdir(mode=0o700)
        self.database = self.root / "source.sqlite3"
        connection = create_database(self.database)
        connection.close()

    def tearDown(self):
        self.temporary.cleanup()

    def make_backup(self, index):
        name = (
            f"chapter-cache-20260914T0000{index:02d}Z-"
            f"{index:012x}.sqlite3"
        )
        target = self.backups / name
        source = sqlite3.connect(self.database)
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
            source.close()
        os.chmod(target, 0o600)
        stamp = 1_700_000_000_000_000_000 + index
        os.utime(target, ns=(stamp, stamp))
        return target

    def pin(self, backup):
        connection = sqlite3.connect(backup)
        try:
            logical = warmup._logical_digest(connection)
            chapters = warmup._chapter_table_digest(connection)
        finally:
            connection.close()
        warmup._publish_baseline_pin(
            backup, logical, chapters, backup_root=self.backups)
        return backup

    def test_pending_backup_is_verified_before_publication_and_pin(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        warmup._verified_equivalent(self.database, pending)
        self.assertTrue(pending.exists())
        self.assertFalse(final.exists())
        self.assertFalse((self.backups / warmup.BASELINE_PIN).exists())
        backup = warmup._publish_prepared_backup(
            pending, final, backup_root=self.backups)
        connection = sqlite3.connect(backup)
        try:
            logical = warmup._logical_digest(connection)
            chapters = warmup._chapter_table_digest(connection)
        finally:
            connection.close()
        warmup._publish_baseline_pin(
            backup, logical, chapters, backup_root=self.backups)
        self.assertEqual(
            warmup._verify_baseline_pin(backup_root=self.backups), backup)

    def test_verification_failure_never_publishes_pin(self):
        with mock.patch.object(
                warmup, "_logical_backup",
                side_effect=warmup.MaintenanceError("backup_invalid")):
            with self.assertRaises(warmup.MaintenanceError):
                warmup._prepare_pending_backup(
                    self.database, backup_root=self.backups)
        self.assertFalse((self.backups / warmup.BASELINE_PIN).exists())
        self.assertFalse(any(warmup.BACKUP_RE.fullmatch(p.name) for p in self.backups.iterdir()))

    def test_no_replace_and_directory_fsync_failures_do_not_create_pin(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        with mock.patch.object(
                warmup, "_rename_noreplace",
                side_effect=warmup.MaintenanceError("backup_failed")):
            with self.assertRaises(warmup.MaintenanceError):
                warmup._publish_prepared_backup(
                    pending, final, backup_root=self.backups)
        self.assertFalse((self.backups / warmup.BASELINE_PIN).exists())

    def test_backup_directory_fsync_failure_leaves_only_unreferenced_backup(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        with mock.patch.object(
                warmup, "_fsync_directory", side_effect=OSError("synthetic")):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup._publish_prepared_backup(
                    pending, final, backup_root=self.backups)
        self.assertTrue(final.exists())
        self.assertFalse((self.backups / warmup.BASELINE_PIN).exists())

    def test_pending_backup_file_fsync_failure_never_publishes(self):
        with mock.patch.object(warmup.os, "fsync", side_effect=OSError("synthetic")):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup._prepare_pending_backup(
                    self.database, backup_root=self.backups)
        self.assertFalse(any(
            warmup.BACKUP_RE.fullmatch(path.name) for path in self.backups.iterdir()
        ))
        self.assertFalse((self.backups / warmup.BASELINE_PIN).exists())

    def test_baseline_pin_is_written_and_fsynced_before_atomic_publication(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        backup = warmup._publish_prepared_backup(
            pending, final, backup_root=self.backups)
        connection = sqlite3.connect(backup)
        try:
            logical = warmup._logical_digest(connection)
            chapters = warmup._chapter_table_digest(connection)
        finally:
            connection.close()
        events = []
        real_fsync = warmup.os.fsync
        real_rename = warmup._rename_noreplace

        def fsync(descriptor):
            events.append("fsync")
            return real_fsync(descriptor)

        def rename(source, destination):
            events.append("rename")
            return real_rename(source, destination)

        with mock.patch.object(warmup.os, "fsync", side_effect=fsync), mock.patch.object(
                warmup, "_rename_noreplace", side_effect=rename):
            warmup._publish_baseline_pin(
                backup, logical, chapters, backup_root=self.backups)
        self.assertLess(events.index("fsync"), events.index("rename"))
        self.assertEqual(events[-1], "fsync")

    def test_each_pin_publication_failure_prevents_writer_adoption(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        backup = warmup._publish_prepared_backup(
            pending, final, backup_root=self.backups)
        connection = sqlite3.connect(backup)
        try:
            logical = warmup._logical_digest(connection)
            chapters = warmup._chapter_table_digest(connection)
        finally:
            connection.close()
        for boundary in ("write", "file_fsync", "rename"):
            case = self.backups / boundary
            case.mkdir(mode=0o700)
            case_backup = case / backup.name
            case_backup.write_bytes(backup.read_bytes())
            os.chmod(case_backup, 0o600)
            patches = []
            if boundary == "write":
                patches.append(mock.patch.object(
                    warmup, "_write_all", side_effect=warmup.MaintenanceError("backup_failed")))
            elif boundary == "file_fsync":
                patches.append(mock.patch.object(
                    warmup.os, "fsync", side_effect=OSError("synthetic")))
            else:
                patches.append(mock.patch.object(
                    warmup, "_rename_noreplace", side_effect=OSError("synthetic")))
            with contextlib.ExitStack() as stack:
                for patcher in patches:
                    stack.enter_context(patcher)
                with self.assertRaises((OSError, warmup.MaintenanceError)):
                    warmup._publish_baseline_pin(
                        case_backup, logical, chapters, backup_root=case)
            self.assertFalse((case / warmup.BASELINE_PIN).exists())

    def test_more_than_five_resumptions_keep_baseline_and_four_newest_points(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, 9):
            newest = self.make_backup(index)
            warmup._complete_retention_transition(
                newest, baseline, backup_root=self.backups)
            completed = warmup._retention_inventory(self.backups)
            self.assertLessEqual(len(completed), warmup.MAX_BACKUPS)
            self.assertIn(baseline, completed)
            self.assertIn(newest, completed)
        names = {path.name for path in warmup._retention_inventory(self.backups)}
        expected = {baseline.name}
        expected.update(
            (self.backups / warmup._backup_name(
                now=datetime.datetime(
                    2026, 9, 14, 0, 0, index,
                    tzinfo=datetime.timezone.utc),
                entropy=f"{index:012x}",
            )).name
            for index in range(5, 9)
        )
        self.assertEqual(names, expected)

    def test_rollback_point_can_be_published_at_normal_capacity(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_BACKUPS):
            self.make_backup(index)
        usage = type("Usage", (), {"free": 1 << 50})()
        with mock.patch.object(warmup.shutil, "disk_usage", return_value=usage):
            warmup._check_backup_admission(
                database=self.database, backup_root=self.backups)
        newest = self.make_backup(warmup.MAX_BACKUPS)
        warmup._complete_retention_transition(
            newest, baseline, backup_root=self.backups)
        remaining = warmup._retention_inventory(self.backups)
        self.assertEqual(len(remaining), warmup.MAX_BACKUPS)
        self.assertIn(baseline, remaining)
        self.assertIn(newest, remaining)

        connection = sqlite3.connect(self.database)
        connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)", ("new", "[]", "new"))
        connection.commit()
        connection.close()
        current = self.make_backup(warmup.MAX_BACKUPS + 1)
        warmup._complete_retention_transition(
            current, baseline, backup_root=self.backups)
        full, protected = database_digests(current)
        with mock.patch.object(warmup, "DATABASE", self.database):
            warmup._run_rollback(
                baseline, expected_full_digest=full,
                expected_protected_digest=protected)
        connection = sqlite3.connect(self.database)
        try:
            self.assertEqual(
                connection.execute("SELECT * FROM chapter_points").fetchall(), [])
        finally:
            connection.close()

    def test_interrupted_over_cap_transition_is_completed_next_time(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_TRANSITION_BACKUPS):
            newest = self.make_backup(index)
        self.assertEqual(
            len(warmup._retention_inventory(self.backups)),
            warmup.MAX_TRANSITION_BACKUPS,
        )
        warmup._recover_retention_transition(
            baseline, backup_root=self.backups)
        remaining = warmup._retention_inventory(self.backups)
        self.assertEqual(len(remaining), warmup.MAX_BACKUPS)
        self.assertIn(baseline, remaining)
        self.assertIn(newest, remaining)

    def test_deletion_failure_preserves_baseline_newest_and_over_cap_state(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_TRANSITION_BACKUPS):
            newest = self.make_backup(index)
        with mock.patch.object(warmup.os, "unlink", side_effect=OSError("synthetic")):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup._complete_retention_transition(
                    newest, baseline, backup_root=self.backups)
        remaining = warmup._retention_inventory(self.backups)
        self.assertEqual(len(remaining), warmup.MAX_TRANSITION_BACKUPS)
        self.assertIn(baseline, remaining)
        self.assertIn(newest, remaining)

    def test_directory_fsync_failure_prevents_writer_but_is_recoverable(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_TRANSITION_BACKUPS):
            newest = self.make_backup(index)
        with mock.patch.object(
                warmup, "_fsync_directory", side_effect=OSError("synthetic")):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup._complete_retention_transition(
                    newest, baseline, backup_root=self.backups)
        remaining = warmup._retention_inventory(self.backups)
        self.assertEqual(len(remaining), warmup.MAX_BACKUPS)
        self.assertIn(baseline, remaining)
        self.assertIn(newest, remaining)
        warmup._recover_retention_transition(
            baseline, backup_root=self.backups)

    def test_newest_is_integrity_checked_before_any_deletion(self):
        baseline = self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_TRANSITION_BACKUPS):
            newest = self.make_backup(index)
        newest.write_bytes(b"not sqlite")
        removed = mock.Mock()
        with mock.patch.object(warmup.os, "unlink", removed):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_invalid"):
                warmup._complete_retention_transition(
                    newest, baseline, backup_root=self.backups)
        removed.assert_not_called()
        self.assertTrue(newest.exists())
        self.assertTrue(baseline.exists())

    def test_newest_and_baseline_are_never_prune_candidates(self):
        baseline = self.pin(self.make_backup(0))
        points = [self.make_backup(index) for index in range(1, 6)]
        newest = points[-1]
        removed = []
        real_unlink = warmup.os.unlink

        def unlink(path):
            removed.append(Path(path))
            real_unlink(path)

        with mock.patch.object(warmup.os, "unlink", side_effect=unlink):
            warmup._complete_retention_transition(
                newest, baseline, backup_root=self.backups)
        self.assertNotIn(baseline, removed)
        self.assertNotIn(newest, removed)
        self.assertEqual(removed, [points[0]])

    def test_unknown_unsafe_and_more_than_one_over_cap_fail_closed(self):
        baseline = self.pin(self.make_backup(0))
        unknown = self.backups / "unknown"
        unknown.write_bytes(b"x")
        with self.assertRaisesRegex(warmup.MaintenanceError, "backup_capacity_exceeded"):
            warmup._retention_inventory(self.backups)
        unknown.unlink()
        unsafe = self.make_backup(1)
        os.chmod(unsafe, 0o644)
        with self.assertRaisesRegex(warmup.MaintenanceError, "backup_invalid"):
            warmup._retention_inventory(self.backups)
        os.chmod(unsafe, 0o600)
        for index in range(2, warmup.MAX_TRANSITION_BACKUPS + 1):
            self.make_backup(index)
        self.assertTrue((self.backups / warmup.BASELINE_PIN).exists())
        with self.assertRaisesRegex(warmup.MaintenanceError, "backup_capacity_exceeded"):
            warmup._retention_inventory(self.backups)

    def test_ambiguous_oldest_or_newest_timestamp_fails_closed(self):
        baseline = self.pin(self.make_backup(0))
        points = [self.make_backup(index) for index in range(1, 6)]
        same = 1_699_999_999_999_999_900
        os.utime(points[0], ns=(same, same))
        os.utime(points[1], ns=(same, same))
        with self.assertRaisesRegex(warmup.MaintenanceError, "backup_invalid"):
            warmup._complete_retention_transition(
                points[-1], baseline, backup_root=self.backups)
        later = 1_800_000_000_000_000_000
        os.utime(points[-1], ns=(later, later))
        os.utime(points[-2], ns=(later, later))
        with self.assertRaisesRegex(warmup.MaintenanceError, "backup_invalid"):
            warmup._recover_retention_transition(
                baseline, backup_root=self.backups)

    def test_pre_chapter_identity_pin_shape_is_never_adopted(self):
        backup = self.backups / warmup._backup_name(
            now=datetime.datetime(
                2026, 9, 14, tzinfo=datetime.timezone.utc),
            entropy="000000000000",
        )
        backup.write_bytes(self.database.read_bytes())
        os.chmod(backup, 0o600)
        pin = self.backups / warmup.BASELINE_PIN
        pin.write_text(json.dumps({
            "version": 1, "state": "pinned", "backup_id": backup.name,
            "logical_identity": "0" * 64,
        }), encoding="utf-8")
        os.chmod(pin, 0o600)
        with self.assertRaisesRegex(warmup.MaintenanceError, "baseline_invalid"):
            warmup._verify_baseline_pin(backup_root=self.backups)

    def test_free_space_bound_counts_four_generations_and_fixed_reserve(self):
        wal = Path(str(self.database) + "-wal")
        wal.write_bytes(b"wal-bytes")
        generation = self.database.stat().st_size + wal.stat().st_size
        required = (
            4 * generation + warmup.MIN_FREE_BYTES + 8 * 1024 * 1024)
        usage = type("Usage", (), {"free": required - 1})()
        with mock.patch.object(warmup.shutil, "disk_usage", return_value=usage):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_space_unavailable"):
                warmup._check_backup_admission(
                    database=self.database, backup_root=self.backups)
        usage.free = required
        with mock.patch.object(warmup.shutil, "disk_usage", return_value=usage):
            warmup._check_backup_admission(
                database=self.database, backup_root=self.backups)

    def test_capacity_requires_space_for_temporary_extra_backup(self):
        self.pin(self.make_backup(0))
        for index in range(1, warmup.MAX_BACKUPS):
            self.make_backup(index)
        generation = self.database.stat().st_size
        required = 4 * generation + warmup.MIN_FREE_BYTES + 8 * 1024 * 1024
        usage = type("Usage", (), {"free": required - 1})()
        with mock.patch.object(warmup.shutil, "disk_usage", return_value=usage):
            with self.assertRaisesRegex(
                    warmup.MaintenanceError, "backup_space_unavailable"):
                warmup._check_backup_admission(
                    database=self.database, backup_root=self.backups)


class AdmissionTests(unittest.TestCase):
    def args(self, execute=False, **values):
        namespace = argparse.Namespace(
            proposal_id="p-20260912T064839Z-04191281", execute=execute)
        for key in warmup.EXPECTED_KEYS:
            setattr(namespace, "expect_" + key, values.get(key))
        return namespace

    def test_execution_rejection_precedes_locks_and_database_work(self):
        args = self.args(execute=True)
        with mock.patch(
                "station_director.isolation.check_invocation_context",
                return_value=(False, "fixed")), mock.patch.object(
                    warmup, "plan") as plan, mock.patch.object(
                        warmup, "execute") as execute:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(warmup.main([
                    args.proposal_id, "--execute"
                ]), 1)
        plan.assert_not_called()
        execute.assert_not_called()

    def test_normal_ssh_and_exact_counts_are_required(self):
        args = self.args(execute=True, **{key: 0 for key in warmup.EXPECTED_KEYS})
        expected = warmup._admit(
            args, environ={}, ancestry=["python", "sshd", "systemd"])
        self.assertEqual(expected, {key: 0 for key in warmup.EXPECTED_KEYS})
        with self.assertRaisesRegex(warmup.MaintenanceError, "invocation_context_rejected"):
            warmup._admit(args, environ={"CODEX_TASK": "1"}, ancestry=["sshd"])

    def test_service_state_requires_all_exact_authoritative_properties(self):
        completed = type("Completed", (), {
            "returncode": 0,
            "stdout": "SubState=dead\nMainPID=0\nLoadState=loaded\nActiveState=inactive\n",
        })()
        with mock.patch.object(warmup, "_systemctl", return_value=completed):
            self.assertEqual(
                warmup._service_state(), ("loaded", "inactive", "dead", "0"))
        completed.stdout = "LoadState=loaded\nActiveState=inactive\n"
        with mock.patch.object(warmup, "_systemctl", return_value=completed):
            with self.assertRaisesRegex(warmup.MaintenanceError, "service_state_invalid"):
                warmup._service_state()


class ExecuteOrderingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "live.sqlite3"
        connection = create_database(self.database)
        connection.close()
        self.backups = self.root / "backups"
        self.counts = {
            "eligible": 0, "missing": 0, "legacy_empty": 0,
            "current_empty": 0, "unavailable_empty": 0,
            "attestations": 0, "probes": 0, "short_media": 0,
            "legacy_nonempty": 0, "versioned": 0,
        }
        self.args = argparse.Namespace(
            proposal_id="p-20260912T064839Z-04191281", execute=True)

    def tearDown(self):
        self.temporary.cleanup()

    def test_live_writer_follows_verified_backup_pin_and_identity_e(self):
        events = []
        identity = warmup._generation_identity(self.database)
        real_prepare = warmup._prepare_pending_backup
        real_publish = warmup._publish_prepared_backup
        real_pin = warmup._publish_baseline_pin

        def generation_identity(*args, **kwargs):
            events.append("identity")
            return identity

        def prepare(path, **unused):
            events.append("prepare")
            return real_prepare(path, backup_root=self.backups)

        def publish(pending, final):
            events.append("backup_publish")
            return real_publish(pending, final, backup_root=self.backups)

        def pin(backup, logical, chapters):
            events.append("pin_publish")
            return real_pin(
                backup, logical, chapters, backup_root=self.backups)

        def writer(*args, **kwargs):
            events.append("writer")
            self.assertTrue((self.backups / warmup.BASELINE_PIN).exists())
            return (True, True,
                    {category: 0 for category in warmup.PROBE_FAILURE_CATEGORIES}, None)

        @contextlib.contextmanager
        def no_lock(*args, **kwargs):
            yield

        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "BACKUP_ROOT", self.backups), mock.patch.object(
                    warmup, "_maintenance_lock", no_lock), mock.patch(
                        "station_director.validation_coordinator._validation_lock", no_lock), mock.patch.object(
                            warmup, "_check_backup_admission"), mock.patch.object(
                                warmup, "_stop_service", return_value=False), mock.patch.object(
                                warmup, "_plan_with_inventory", return_value=({}, self.counts)), mock.patch.object(
                                        warmup, "_generation_identity", side_effect=generation_identity), mock.patch.object(
                                            warmup, "_prepare_pending_backup", side_effect=prepare), mock.patch.object(
                                                warmup, "_publish_prepared_backup", side_effect=publish), mock.patch.object(
                                                    warmup, "_publish_baseline_pin", side_effect=pin), mock.patch.object(
                                                        warmup, "_run_writer", side_effect=writer):
            result = warmup.execute(self.args, self.counts)
        self.assertEqual(result["status"], "complete")
        self.assertLess(events.index("prepare"), events.index("backup_publish"))
        self.assertLess(events.index("backup_publish"), events.index("pin_publish"))
        self.assertLess(events.index("pin_publish"), events.index("writer"))
        self.assertGreaterEqual(events.count("identity"), 2)

    def test_authoritative_recount_mismatch_precedes_backup_and_writer(self):
        wrong = dict(self.counts, missing=1)

        @contextlib.contextmanager
        def no_lock(*args, **kwargs):
            yield

        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "BACKUP_ROOT", self.backups), mock.patch.object(
                    warmup, "_maintenance_lock", no_lock), mock.patch(
                        "station_director.validation_coordinator._validation_lock", no_lock), mock.patch.object(
                            warmup, "_check_backup_admission"), mock.patch.object(
                                warmup, "_stop_service", return_value=False), mock.patch.object(
                                    warmup, "_plan_with_inventory", return_value=({}, wrong)), mock.patch.object(
                                        warmup, "_prepare_pending_backup") as backup, mock.patch.object(
                                            warmup, "_run_writer") as writer:
            with self.assertRaisesRegex(warmup.MaintenanceError, "expected_counts_mismatch"):
                warmup.execute(self.args, self.counts)
        backup.assert_not_called()
        writer.assert_not_called()

    def test_retention_failure_precedes_live_writer(self):
        events = []
        identity = warmup._generation_identity(self.database)
        real_prepare = warmup._prepare_pending_backup
        real_publish = warmup._publish_prepared_backup
        real_pin = warmup._publish_baseline_pin

        @contextlib.contextmanager
        def no_lock(*args, **kwargs):
            yield

        def prepare(path, **unused):
            return real_prepare(path, backup_root=self.backups)

        def publish(pending, final):
            return real_publish(pending, final, backup_root=self.backups)

        def pin(backup, logical, chapters):
            return real_pin(backup, logical, chapters, backup_root=self.backups)

        writer = mock.Mock()
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "BACKUP_ROOT", self.backups), mock.patch.object(
                    warmup, "_maintenance_lock", no_lock), mock.patch.object(
                        warmup, "_director_validation_lock", no_lock), mock.patch.object(
                            warmup, "_check_backup_admission"), mock.patch.object(
                                warmup, "_stop_service", return_value=False), mock.patch.object(
                                    warmup, "_plan_with_inventory", return_value=({}, self.counts)), mock.patch.object(
                                        warmup, "_generation_identity", return_value=identity), mock.patch.object(
                                            warmup, "_prepare_pending_backup", side_effect=prepare), mock.patch.object(
                                                warmup, "_publish_prepared_backup", side_effect=publish), mock.patch.object(
                                                    warmup, "_publish_baseline_pin", side_effect=pin), mock.patch.object(
                                                        warmup, "_complete_retention_transition",
                                                        side_effect=warmup.MaintenanceError("backup_failed")), mock.patch.object(
                                                            warmup, "_run_writer", writer):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup.execute(self.args, self.counts)
        writer.assert_not_called()

    def test_retention_failure_precedes_destructive_rollback(self):
        pending, final = warmup._prepare_pending_backup(
            self.database, backup_root=self.backups)
        warmup._verified_equivalent(self.database, pending)
        baseline = warmup._publish_prepared_backup(
            pending, final, backup_root=self.backups)
        logical, unused_protected = database_digests(baseline)
        connection = sqlite3.connect(baseline)
        try:
            chapters = warmup._chapter_table_digest(connection)
        finally:
            connection.close()
        warmup._publish_baseline_pin(
            baseline, logical, chapters, backup_root=self.backups)

        @contextlib.contextmanager
        def no_lock(*args, **kwargs):
            yield

        real_prepare = warmup._prepare_pending_backup
        real_publish = warmup._publish_prepared_backup

        def prepare(path, **unused):
            return real_prepare(path, backup_root=self.backups)

        def publish(source, destination):
            return real_publish(source, destination, backup_root=self.backups)

        destructive = mock.Mock()
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "BACKUP_ROOT", self.backups), mock.patch.object(
                    warmup, "_maintenance_lock", no_lock), mock.patch.object(
                        warmup, "_director_validation_lock", no_lock), mock.patch.object(
                            warmup, "_stop_service", return_value=False), mock.patch.object(
                                warmup, "_plan_with_inventory", return_value=({}, self.counts)), mock.patch.object(
                                    warmup, "_prepare_pending_backup", side_effect=prepare), mock.patch.object(
                                        warmup, "_publish_prepared_backup", side_effect=publish), mock.patch.object(
                                            warmup, "_complete_retention_transition",
                                            side_effect=warmup.MaintenanceError("backup_failed")), mock.patch.object(
                                                warmup, "_run_rollback", destructive):
            with self.assertRaisesRegex(warmup.MaintenanceError, "backup_failed"):
                warmup.rollback(self.args, self.counts)
        destructive.assert_not_called()

    def test_clean_full_lifecycle_writes_attestation_and_restarts_service(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE

        media_root = self.root / "media"
        media_root.mkdir()
        media = media_root / "clip.mp4"
        media.write_bytes(b"synthetic")
        info = media.stat()
        connection = sqlite3.connect(self.database)
        connection.execute(
            "INSERT INTO file_meta(path,duration,size,last_mod,last_validate,last_update,json,media_type) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (os.fspath(media), 600.0, info.st_size, info.st_mtime,
             "now", "now", "{}", "video"),
        )
        connection.commit()
        connection.close()
        item = warmup.MediaItem(
            ("clip.mp4",), info.st_size, info.st_mtime_ns, info.st_mtime)
        inventory = {os.fspath(media): item}
        with mock.patch.object(warmup, "MEDIA_ROOT", media_root):
            counts = warmup._counts_from_private(self.database, inventory)

        @contextlib.contextmanager
        def no_lock(*args, **kwargs):
            yield

        restart = mock.Mock()
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "MEDIA_ROOT", media_root), mock.patch.object(
                    warmup, "BACKUP_ROOT", self.backups), mock.patch.object(
                        warmup, "_maintenance_lock", no_lock), mock.patch.object(
                            warmup, "_director_validation_lock", no_lock), mock.patch.object(
                                warmup, "_stop_service", return_value=True), mock.patch.object(
                                    warmup, "_restart_service", restart), mock.patch.object(
                                        warmup, "_plan_with_inventory", return_value=(inventory, counts)), mock.patch.object(
                                            warmup, "_eligible_inventory", return_value=inventory), mock.patch.object(
                                                warmup, "_service_state", return_value=("loaded", "inactive", "dead", "0")), mock.patch(
                                                    "fs42.chapter_analysis.analyze_chapters",
                                                    return_value=CompletedChapterAnalysis(METHOD_FFPROBE, ())):
            result = warmup.execute(self.args, counts)
        self.assertEqual(result["status"], "complete")
        self.assertIsNone(result["partial_reason"])
        restart.assert_called_once_with()
        connection = sqlite3.connect(self.database)
        try:
            value = json.loads(connection.execute(
                "SELECT points FROM chapter_points").fetchone()[0])
        finally:
            connection.close()
        self.assertEqual(value["attestation_version"], 1)


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.media_root = self.root / "media"
        self.media_root.mkdir()
        self.media = self.media_root / "clip.mp4"
        self.media.write_bytes(b"synthetic media")
        self.database = self.root / "live.sqlite3"
        connection = create_database(self.database)
        info = self.media.stat()
        connection.execute(
            "INSERT INTO file_meta(path,duration,size,last_mod,last_validate,last_update,json,media_type) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (os.fspath(self.media), 600.0, info.st_size, info.st_mtime,
             "now", "now", "{}", "video"),
        )
        connection.commit()
        connection.close()

    def tearDown(self):
        self.temporary.cleanup()

    def run_writer(self, analysis):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE

        full, protected = database_digests(self.database)
        info = self.media.stat()
        item = warmup.MediaItem(
            ("clip.mp4",), info.st_size, info.st_mtime_ns, info.st_mtime)
        initial = {
            "eligible": 1, "missing": 1, "legacy_empty": 0,
            "current_empty": 0, "unavailable_empty": 0,
            "attestations": 1, "probes": 1, "short_media": 0,
            "versioned": 0,
        }
        completed = analysis or CompletedChapterAnalysis(METHOD_FFPROBE, ())
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "MEDIA_ROOT", self.media_root), mock.patch.object(
                    warmup, "_eligible_inventory", return_value={os.fspath(self.media): item}), mock.patch.object(
                        warmup, "_service_state", return_value=("loaded", "inactive", "dead", "0")), mock.patch(
                            "fs42.chapter_analysis.analyze_chapters", return_value=completed):
            return warmup._run_writer(
                "p-20260912T064839Z-04191281", initial, warmup.time.monotonic(),
                expected_full_digest=full, expected_protected_digest=protected,
            )

    def test_writer_publishes_successful_empty_attestation_via_held_descriptor(self):
        complete, safe, failures, reason = self.run_writer(None)
        self.assertTrue(complete)
        self.assertTrue(safe)
        self.assertFalse(any(failures.values()))
        self.assertIsNone(reason)
        connection = sqlite3.connect(self.database)
        try:
            value = json.loads(connection.execute(
                "SELECT points FROM chapter_points").fetchone()[0])
        finally:
            connection.close()
        self.assertEqual(value["attestation_version"], 1)
        self.assertEqual(value["chapters"], [])

    def test_stale_file_meta_rejects_before_analysis(self):
        info = self.media.stat()
        item = warmup.MediaItem(
            ("clip.mp4",), info.st_size, info.st_mtime_ns, info.st_mtime)
        connection = sqlite3.connect(self.database)
        connection.execute(
            "UPDATE file_meta SET last_mod=last_mod-1 WHERE path=?",
            (os.fspath(self.media),),
        )
        connection.commit()
        with self.assertRaisesRegex(warmup.MaintenanceError, "chapter_cache_invalid"):
            warmup._chapter_counts(
                connection, {os.fspath(self.media): item}, {os.fspath(self.media)})
        connection.close()

    def test_downgrade_rollback_restores_only_pinned_chapter_rows(self):
        baseline = self.root / "baseline.sqlite3"
        shutil_connection = create_database(baseline)
        shutil_connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)", ("legacy", "[]", "old"))
        shutil_connection.execute(
            "INSERT INTO catalog_entries VALUES(?,?)", (1, "baseline"))
        shutil_connection.commit()
        shutil_connection.close()

        live = sqlite3.connect(self.database)
        live.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)", ("new", '{"attestation_version":1}', "new"))
        live.execute("INSERT INTO catalog_entries VALUES(?,?)", (2, "current"))
        live.commit()
        live.close()
        full, protected = database_digests(self.database)
        with mock.patch.object(warmup, "DATABASE", self.database):
            warmup._run_rollback(
                baseline, expected_full_digest=full,
                expected_protected_digest=protected,
            )
        live = sqlite3.connect(self.database)
        try:
            self.assertEqual(live.execute(
                "SELECT path,points,last_updated FROM chapter_points").fetchall(),
                [("legacy", "[]", "old")],
            )
            self.assertEqual(live.execute(
                "SELECT id,filepath FROM catalog_entries").fetchall(),
                [(2, "current")],
            )
        finally:
            live.close()

    def test_failed_analysis_leaves_no_false_empty_attestation(self):
        from fs42.chapter_analysis import ChapterAnalysisError

        full, protected = database_digests(self.database)
        info = self.media.stat()
        item = warmup.MediaItem(
            ("clip.mp4",), info.st_size, info.st_mtime_ns, info.st_mtime)
        initial = {
            "eligible": 1, "missing": 1, "legacy_empty": 0,
            "current_empty": 0, "unavailable_empty": 0,
            "attestations": 1, "probes": 1, "short_media": 0, "versioned": 0,
        }
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "MEDIA_ROOT", self.media_root), mock.patch.object(
                    warmup, "_eligible_inventory", return_value={os.fspath(self.media): item}), mock.patch.object(
                        warmup, "_service_state", return_value=("loaded", "inactive", "dead", "0")), mock.patch(
                            "fs42.chapter_analysis.analyze_chapters",
                            side_effect=ChapterAnalysisError("probe_nonzero")):
            complete, safe, failures, reason = warmup._run_writer(
                "p-20260912T064839Z-04191281", initial, warmup.time.monotonic(),
                expected_full_digest=full, expected_protected_digest=protected,
            )
        self.assertFalse(complete)
        self.assertTrue(safe)
        self.assertEqual(failures["probe_nonzero"], 1)
        self.assertEqual(reason, "probe_failures")
        connection = sqlite3.connect(self.database)
        try:
            self.assertIsNone(connection.execute(
                "SELECT points FROM chapter_points").fetchone())
        finally:
            connection.close()

    def test_writer_replaces_eligible_empty_and_retires_confirmed_unavailable_empty(self):
        from fs42.chapter_analysis import CompletedChapterAnalysis, METHOD_FFPROBE

        unavailable = self.media_root / "gone.mp4"
        connection = sqlite3.connect(self.database)
        connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)",
            (os.fspath(self.media), "[]", "old"),
        )
        connection.execute(
            "INSERT INTO chapter_points VALUES(?,?,?)",
            (os.fspath(unavailable), "[]", "old"),
        )
        connection.commit()
        connection.close()
        full, protected = database_digests(self.database)
        info = self.media.stat()
        item = warmup.MediaItem(
            ("clip.mp4",), info.st_size, info.st_mtime_ns, info.st_mtime)
        initial = {
            "eligible": 1, "missing": 0, "legacy_empty": 2,
            "current_empty": 1, "unavailable_empty": 1,
            "attestations": 1, "probes": 1, "short_media": 0,
            "legacy_nonempty": 0, "versioned": 0,
        }
        with mock.patch.object(warmup, "DATABASE", self.database), mock.patch.object(
                warmup, "MEDIA_ROOT", self.media_root), mock.patch.object(
                    warmup, "_eligible_inventory", return_value={os.fspath(self.media): item}), mock.patch.object(
                        warmup, "_service_state", return_value=("loaded", "inactive", "dead", "0")), mock.patch(
                            "fs42.chapter_analysis.analyze_chapters",
                            return_value=CompletedChapterAnalysis(METHOD_FFPROBE, ())):
            complete, safe, failures, reason = warmup._run_writer(
                "p-20260912T064839Z-04191281", initial, warmup.time.monotonic(),
                expected_inventory={os.fspath(self.media): item},
                expected_full_digest=full, expected_protected_digest=protected,
            )
        self.assertTrue(complete)
        self.assertTrue(safe)
        self.assertFalse(any(failures.values()))
        self.assertIsNone(reason)
        connection = sqlite3.connect(self.database)
        try:
            rows = connection.execute(
                "SELECT path,points FROM chapter_points ORDER BY path").fetchall()
        finally:
            connection.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][1])["attestation_version"], 1)


class StaticBoundaryTests(unittest.TestCase):
    def test_maintenance_module_has_no_top_level_fs42_import(self):
        source = Path(warmup.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = []
        for node in tree.body:
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        self.assertFalse(any(
            name == "fs42" or name.startswith("fs42.") for name in imported
        ), imported)

    def test_constants_keep_execution_and_finalization_bounded(self):
        self.assertEqual(warmup.MAX_UNPINNED_BACKUPS, 4)
        self.assertEqual(warmup.MAX_BACKUPS, 1 + warmup.MAX_UNPINNED_BACKUPS)
        self.assertEqual(warmup.MAX_TRANSITION_BACKUPS, warmup.MAX_BACKUPS + 1)
        self.assertEqual(warmup.CONTROL_SECONDS, 7200)
        self.assertEqual(warmup.IN_FLIGHT_OPERATION_ALLOWANCE_SECONDS, 60)
        self.assertEqual(warmup.POST_WRITE_VERIFICATION_ALLOWANCE_SECONDS, 120)
        self.assertEqual(warmup.SERVICE_FINALIZATION_ALLOWANCE_SECONDS, 60)
        self.assertEqual(warmup.FINALIZATION_SECONDS, 60 + 120 + 60)
        self.assertEqual(
            warmup.ADMISSION_SECONDS + warmup.FINALIZATION_SECONDS,
            warmup.CONTROL_SECONDS,
        )
        self.assertEqual(warmup.ANALYSIS_TIMEOUT_SECONDS, 30)
        self.assertEqual(warmup.SERVICE_COMMAND_TIMEOUT_SECONDS, 30)

    def test_synthetic_path_defaults_are_confined_to_injected_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "runtime/live.sqlite3"
            database.parent.mkdir()
            connection = create_database(database)
            connection.close()
            backups = root / "runtime/backups"
            media = root / "media"
            media.mkdir()
            with mock.patch.object(warmup, "PROJECT_ROOT", root), mock.patch.object(
                    warmup, "DATABASE", database), mock.patch.object(
                        warmup, "BACKUP_ROOT", backups), mock.patch.object(
                            warmup, "MEDIA_ROOT", media):
                resolved = (
                    warmup.DATABASE, warmup.BACKUP_ROOT,
                    warmup.BACKUP_ROOT / warmup.BASELINE_PIN,
                    warmup.BACKUP_ROOT.parent / warmup.MAINTENANCE_LOCK,
                    warmup.MEDIA_ROOT,
                )
                self.assertTrue(all(
                    path.resolve(strict=False).is_relative_to(root)
                    for path in resolved
                ))
                with warmup._maintenance_lock():
                    self.assertTrue(
                        (backups.parent / warmup.MAINTENANCE_LOCK).exists())
                with warmup.stable_private_generation(
                        temporary_parent=root) as generation:
                    self.assertTrue(generation.directory.is_relative_to(root))
                    pending, final = warmup._prepare_pending_backup(
                        generation.database)
                    published = warmup._publish_prepared_backup(pending, final)
                    self.assertTrue(published.is_relative_to(root))
                with warmup._open_root() as descriptor:
                    self.assertTrue(os.fstat(descriptor).st_ino)


if __name__ == "__main__":
    unittest.main()
