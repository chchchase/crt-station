import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import shutil
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fs42.guide_payloads import (
    _listing_projection,
    build_all_schedules_payload,
    build_channels_payload,
)
from fs42.guide_reader import (
    GUIDE_ARTIFACT_IDENTITY,
    GUIDE_MAGIC,
    MAX_GUIDE_STREAM_BYTES,
    GuideArtifactError,
    GuideLoadingError,
    GuideValidationError,
    _bounded_json,
    prepare_guide_snapshot,
    stream_validate_staged_guide,
    verify_guide_snapshot,
    write_guide_stream,
)
from station_director.schedule_normalization import (
    MAX_DIFFERENCES,
    NormalizationError,
    compare_normalized_runs,
    normalize_completed_run,
)
from station_director.single_run_protocol import (
    RESPONSE_SCHEMA,
    validate_document,
    validate_response_semantics,
)
from test.test_station_director_milestone_b2 import catalog_row, create_database, insert_block
from test.test_station_director_milestone_c2 import make_run, response


ROOT = Path(__file__).parents[1]
PROPOSAL_START = datetime(2026, 11, 1, 0, 30, 0, 1)
PROPOSAL_END = datetime(2026, 11, 1, 2, 30, 0, 1)


def add_catalog(connection, catalog_id, station, path, title="my_show_s01e01"):
    row = catalog_row(station, path, title, "Synthetic")
    connection.execute(
        "INSERT INTO catalog_entries "
        "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (catalog_id, *row.values()),
    )


def guide_fixture(root, *, overlap=False, gap=False, metadata=True):
    database = root / "fs42.db"
    connection = create_database(database)
    add_catalog(connection, 1, "Action", "/media/a.mp4")
    add_catalog(connection, 2, "Action", "/media/b.mp4", "second_show_s01e02")
    first_end = "2026-11-01 01:30:00.000001"
    second_start = ("2026-11-01 01:29:59.999999" if overlap else
                    "2026-11-01 01:31:00.000001" if gap else first_end)
    insert_block(connection, "Action", "2026-11-01 00:30:00.000001",
                 first_end, 1, "/media/a.mp4", title="my_show_s01e01")
    insert_block(connection, "Action", second_start, "2026-11-01 02:30:00.000001",
                 2, "/media/b.mp4", title="second_show_s01e02")
    if metadata:
        connection.execute(
            "INSERT INTO file_meta(path,duration,size,meta,media_type) VALUES(?,?,?,?,?)",
            ("/media/a.mp4", 3600.0, 1,
             json.dumps({"type": "episode", "title": "Pilot", "plot": "Plot"}), "video"),
        )
    connection.commit()
    connection.close()
    os.chmod(database, 0o600)
    stations = [{"network_name": "Action", "network_long_name": "Action Channel",
                 "channel_number": 2, "hidden": False, "_has_schedule": True}]
    channel = {"number": 2, "name": "Action",
               "regeneration_start": "2026-11-01 01:30:00.000001",
               "effective_horizon": "2026-11-01 02:30:00.000001"}
    return database, stations, channel


def decode_guide(path):
    raw = path.read_bytes()
    assert raw.startswith(GUIDE_MAGIC)
    position = len(GUIDE_MAGIC)
    records = []
    while position < len(raw):
        size = int.from_bytes(raw[position:position + 8], "big")
        position += 8
        records.append(json.loads(raw[position:position + size]))
        position += size
    return records


def unlock_snapshot(snapshot):
    os.chmod(snapshot.path, 0o600)
    os.chmod(snapshot.directory, 0o700)


def run_stream(root, database, stations, channel):
    snapshot = prepare_guide_snapshot(database, root)
    try:
        artifact, summaries = stream_validate_staged_guide(
            snapshot, root, stations, [channel], PROPOSAL_START, PROPOSAL_END,
            normalize_titles=True,
        )
        verify_guide_snapshot(snapshot, database)
        return artifact, summaries, decode_guide(root / GUIDE_ARTIFACT_IDENTITY), snapshot
    except Exception:
        unlock_snapshot(snapshot)
        raise


class PayloadCompatibilityTests(unittest.TestCase):
    def test_fixed_payloads_signatures_ordering_metadata_and_case_distinct_names(self):
        block = SimpleNamespace(title="Program", start_time=datetime(2026, 9, 14, 6),
                                end_time=datetime(2026, 9, 14, 7),
                                meta={"type": "episode"})
        stations = [
            {"network_name": "Action", "network_long_name": "Upper", "channel_number": 2,
             "hidden": True, "_has_schedule": True},
            {"network_name": "action", "channel_number": 3},
        ]
        self.assertEqual(build_channels_payload(stations), {"channels": [
            {"network_name": "Action", "network_long_name": "Upper", "channel_number": 2,
             "hidden": True, "has_schedule": True},
            {"network_name": "action", "network_long_name": "", "channel_number": 3,
             "hidden": False, "has_schedule": False},
        ]})
        with_meta = [{"title": "Program", "start_time": "2026-09-14T06:00:00",
                      "end_time": "2026-09-14T07:00:00", "meta": {"type": "episode"}}]
        without_meta = [{"title": "Program", "start_time": "2026-09-14T06:00:00",
                         "end_time": "2026-09-14T07:00:00"}]
        self.assertEqual(_listing_projection([block], True), with_meta)
        self.assertEqual(_listing_projection([block], False), without_meta)
        self.assertEqual(build_all_schedules_payload("s", "e", {"Action": [block],
                                                                  "action": []}, False),
                         {"start": "s", "end": "e",
                          "schedules": {"Action": without_meta, "action": []}})

    def test_fixed_route_success_empty_dates_metadata_and_malformed_metadata(self):
        from fs42.fs42_server.api import schedules

        block = SimpleNamespace(title="Program", start_time=datetime(2026, 9, 14, 6),
                                end_time=datetime(2026, 9, 14, 7), content=None,
                                meta={"type": "episode"})
        expected_without = {"start": "2026-09-14T06:00:00",
                            "end": "2026-09-14T07:00:00",
                            "schedules": {"Action": [{"title": "Program",
                                "start_time": "2026-09-14T06:00:00",
                                "end_time": "2026-09-14T07:00:00"}]}}
        with patch.object(schedules.LiquidAPI, "get_all_blocks",
                          return_value={"Action": [block]}):
            self.assertEqual(schedules.get_all_schedules(
                "2026-09-14T06:00:00", "2026-09-14T07:00:00", False), expected_without)
        with patch.object(schedules.LiquidAPI, "get_all_blocks", return_value={}):
            self.assertEqual(schedules.get_all_schedules(
                "2026-09-14T06:00:00", "2026-09-14T07:00:00", True),
                {"start": "2026-09-14T06:00:00", "end": "2026-09-14T07:00:00",
                 "schedules": {}})
        self.assertEqual(schedules.get_all_schedules(),
                         {"error": "start and end are both required."})
        self.assertEqual(schedules.get_all_schedules("bad", "2026-09-14T07:00:00"),
                         {"error": "Invalid date format. Use ISO format "
                                   "(YYYY-MM-DDTHH:MM:SS) for start and end."})
        block.content = SimpleNamespace(path="/media/a.mp4")
        block.meta = None
        with tempfile.TemporaryDirectory() as directory:
            metadata_db = Path(directory) / "metadata.db"
            connection = sqlite3.connect(metadata_db)
            connection.execute("CREATE TABLE file_meta(path TEXT,meta TEXT)")
            connection.execute("INSERT INTO file_meta VALUES(?,?)",
                               (os.path.realpath("/media/a.mp4"), "{"))
            connection.commit(); connection.close()
            with patch.object(schedules.LiquidAPI, "get_all_blocks",
                              return_value={"Action": [block]}), patch.object(
                                  schedules.MetadataIO, "_default_db_path",
                                  return_value=str(metadata_db)):
                self.assertEqual(schedules.get_all_schedules(
                    "2026-09-14T06:00:00", "2026-09-14T07:00:00", True),
                    expected_without)


class StreamingGuideTests(unittest.TestCase):
    def test_snapshot_stream_is_read_only_bounded_and_transforms_metadata_titles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            stage = root / "stage"
            stage.mkdir(mode=0o700)
            before = sorted(item.name for item in root.iterdir())
            artifact, summaries, records, snapshot = run_stream(
                stage, database, stations, channel
            )
            try:
                self.assertEqual(
                    artifact["byte_count"], (stage / GUIDE_ARTIFACT_IDENTITY).stat().st_size
                )
                listings = [item["value"] for item in records
                            if item["path"].startswith("/guide/schedules/")]
                self.assertEqual([item["title"] for item in listings], ["My Show", "Second Show"])
                self.assertEqual(listings[0]["meta"]["title"], "Pilot")
                self.assertNotIn("meta", listings[1])
                self.assertEqual(summaries[0]["listing_count"], 2)
                self.assertEqual(stat_mode(snapshot.path), 0o400)
                self.assertEqual(stat_mode(snapshot.directory), 0o500)
                self.assertEqual(set(item.name for item in snapshot.directory.iterdir()), {"guide.db"})
                self.assertFalse(any((snapshot.directory / ("guide.db" + suffix)).exists()
                                     for suffix in ("-wal", "-shm", "-journal")))
                self.assertEqual(before, ["fs42.db", "stage"])
                self.assertEqual(sorted(item.name for item in root.iterdir()), before)
            finally:
                unlock_snapshot(snapshot)

    def test_half_open_touching_gap_crossing_overlap_and_dst_boundaries(self):
        for kind in ("touching", "gap", "overlap"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database, stations, channel = guide_fixture(
                    root, overlap=kind == "overlap", gap=kind == "gap")
                if kind == "overlap":
                    snapshot = prepare_guide_snapshot(database, root)
                    try:
                        with self.assertRaisesRegex(GuideValidationError, "multiple"):
                            stream_validate_staged_guide(snapshot, root, stations, [channel],
                                                         PROPOSAL_START, PROPOSAL_END)
                    finally:
                        unlock_snapshot(snapshot)
                else:
                    unused, summaries, unused_records, snapshot = run_stream(
                        root, database, stations, channel)
                    try:
                        named = {item["name"]: item for item in summaries[0]["named_boundaries"]}
                        self.assertEqual(named["regeneration_seam.before"]["match_count"], 1)
                        self.assertEqual(named["regeneration_seam.at"]["match_count"],
                                         0 if kind == "gap" else 1)
                        self.assertEqual(named["effective_horizon.at"]["match_count"], 0)
                        self.assertEqual(named["effective_horizon.after"]["match_count"], 0)
                    finally:
                        unlock_snapshot(snapshot)

    def test_coincident_proposal_and_seam_boundaries_keep_both_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            channel["regeneration_start"] = PROPOSAL_START.isoformat(sep=" ")
            unused, summaries, unused_records, snapshot = run_stream(
                root, database, stations, channel
            )
            try:
                names = {item["name"] for item in summaries[0]["named_boundaries"]}
                for suffix in ("before", "at", "after"):
                    self.assertIn(f"proposal_start.{suffix}", names)
                    self.assertIn(f"regeneration_seam.{suffix}", names)
            finally:
                unlock_snapshot(snapshot)

    def test_wrong_projected_title_is_detected_against_raw_schedule(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            snapshot = prepare_guide_snapshot(database, root)
            try:
                def wrong_title(blocks, include_meta):
                    for block in blocks:
                        listing = {"title": "Wrong",
                                   "start_time": block.start_time.isoformat(),
                                   "end_time": block.end_time.isoformat()}
                        if include_meta and getattr(block, "meta", None):
                            listing["meta"] = block.meta
                        yield listing

                with patch("fs42.guide_reader.iter_listing_projection", wrong_title):
                    with self.assertRaisesRegex(GuideValidationError, "raw schedule"):
                        stream_validate_staged_guide(snapshot, root, stations, [channel],
                                                     PROPOSAL_START, PROPOSAL_END)
            finally:
                unlock_snapshot(snapshot)

    def test_independent_raw_index_catches_missing_projection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            snapshot = prepare_guide_snapshot(database, root)
            try:
                with patch("fs42.guide_reader.iter_listing_projection", return_value=iter(())):
                    with self.assertRaisesRegex(GuideValidationError, "raw schedule"):
                        stream_validate_staged_guide(snapshot, root, stations, [channel],
                                                     PROPOSAL_START, PROPOSAL_END)
            finally:
                unlock_snapshot(snapshot)

    def test_wal_working_sidecars_and_immutable_snapshot_remain_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            connection = sqlite3.connect(database)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("UPDATE catalog_entries SET count=count+1 WHERE id=1")
            connection.commit()
            sidecars = {suffix: hashlib.sha256((Path(str(database) + suffix)).read_bytes()).hexdigest()
                        for suffix in ("-wal", "-shm")}
            stage = root / "stage"
            stage.mkdir(mode=0o700)
            snapshot = prepare_guide_snapshot(database, stage)
            try:
                stream_validate_staged_guide(snapshot, stage, stations, [channel],
                                             PROPOSAL_START, PROPOSAL_END)
                verify_guide_snapshot(snapshot, database)
                self.assertEqual(sidecars, {suffix: hashlib.sha256(
                    Path(str(database) + suffix).read_bytes()).hexdigest()
                    for suffix in ("-wal", "-shm")})
            finally:
                unlock_snapshot(snapshot)
                connection.close()

    def test_working_database_replacement_during_snapshot_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, unused_stations, unused_channel = guide_fixture(root)
            stage = root / "stage"
            stage.mkdir(mode=0o700)
            replacement = root / "replacement.db"
            shutil.copyfile(database, replacement)
            real_copyfile = shutil.copyfile

            def replacing_copy(source, target, **kwargs):
                result = real_copyfile(source, target, **kwargs)
                if Path(source) == database and Path(target).name == "working.db":
                    os.replace(replacement, database)
                return result

            with patch("fs42.guide_reader.shutil.copyfile", side_effect=replacing_copy):
                with self.assertRaisesRegex(GuideLoadingError, "changed"):
                    prepare_guide_snapshot(database, stage)

    def test_limits_reject_oversized_json_before_decode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, stations, channel = guide_fixture(root)
            connection = sqlite3.connect(database)
            connection.execute("UPDATE liquid_blocks SET plan_json=? WHERE id=1",
                               (json.dumps(["x" * (300 * 1024)]),))
            connection.commit(); connection.close()
            snapshot = None
            try:
                with self.assertRaises((GuideLoadingError, sqlite3.DataError)):
                    snapshot = prepare_guide_snapshot(database, root)
                    stream_validate_staged_guide(snapshot, root, stations, [channel],
                                                 PROPOSAL_START, PROPOSAL_END)
            finally:
                if snapshot is not None:
                    unlock_snapshot(snapshot)

    def test_bounded_limits_fail_before_unbounded_accumulation(self):
        with patch("fs42.guide_reader.MAX_GUIDE_CHANNELS", 1):
            with self.assertRaisesRegex(GuideLoadingError, "station limit"):
                stream_validate_staged_guide(
                    SimpleNamespace(path=Path("unused")), Path("unused"),
                    iter([{"network_name": "A"}, {"network_name": "B"}]), [],
                    PROPOSAL_START, PROPOSAL_END,
                )

        for limit_name, expected in (
            ("MAX_JSON_DEPTH", "depth"),
            ("MAX_JSON_MEMBERS", "member"),
            ("MAX_JSON_ARRAY_ITEMS", "array"),
            ("MAX_JSON_STRING_BYTES", "string"),
        ):
            with self.subTest(limit=limit_name), patch(
                    f"fs42.guide_reader.{limit_name}", 1):
                value = {"a": {"b": 1}} if limit_name == "MAX_JSON_DEPTH" else (
                    {"a": 1, "b": 2} if limit_name == "MAX_JSON_MEMBERS" else
                    [1, 2] if limit_name == "MAX_JSON_ARRAY_ITEMS" else "ab"
                )
                with self.assertRaisesRegex(GuideLoadingError, expected):
                    _bounded_json(json.dumps(value), "synthetic")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("fs42.guide_reader.MAX_GUIDE_RECORD_BYTES", 8):
                with self.assertRaisesRegex(GuideArtifactError, "record"):
                    write_guide_stream(root, [("/guide/item", {"value": "too large"})])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("fs42.guide_reader.MAX_GUIDE_STREAM_BYTES", len(GUIDE_MAGIC) + 8):
                with self.assertRaisesRegex(GuideArtifactError, "aggregate"):
                    write_guide_stream(root, [("/guide/item", None)])

        for limit_name, expected in (
            ("MAX_GUIDE_BLOCKS", "block limit"),
            ("MAX_METADATA_BATCH_BYTES", "metadata batch"),
            ("MAX_BOUNDARY_PROBES", "boundary-probe"),
        ):
            with self.subTest(limit=limit_name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database, stations, channel = guide_fixture(root)
                snapshot = prepare_guide_snapshot(database, root)
                try:
                    with patch(f"fs42.guide_reader.{limit_name}", 1):
                        with self.assertRaisesRegex((GuideLoadingError, GuideValidationError),
                                                    expected):
                            stream_validate_staged_guide(
                                snapshot, root, stations, [channel],
                                PROPOSAL_START, PROPOSAL_END,
                            )
                finally:
                    unlock_snapshot(snapshot)


def stat_mode(path):
    return os.stat(path).st_mode & 0o777


class GuideArtifactTests(unittest.TestCase):
    def test_missing_replaced_corrupt_oversized_duplicate_and_extra_fail(self):
        for mutation in ("missing", "symlink", "corrupt", "oversized", "extra", "extra_file",
                         "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); baseline = root / "baseline.db"; create_database(baseline).close()
                stage, unused = make_run(root, "stage", 10)
                path = stage / GUIDE_ARTIFACT_IDENTITY; payload = response("run")
                if mutation == "missing": path.unlink()
                elif mutation == "symlink": path.unlink(); path.symlink_to(baseline)
                elif mutation == "corrupt": path.write_bytes(b"bad")
                elif mutation == "oversized":
                    with path.open("r+b") as handle: handle.truncate(MAX_GUIDE_STREAM_BYTES + 1)
                    payload["guide_validation"]["byte_count"] = MAX_GUIDE_STREAM_BYTES + 1
                elif mutation == "extra":
                    with path.open("ab") as handle: handle.write((0).to_bytes(8, "big"))
                    payload["guide_validation"]["byte_count"] = path.stat().st_size
                elif mutation == "extra_file": (path.parent / "unexpected").write_bytes(b"x")
                else:
                    raw = path.read_bytes(); duplicated = raw + raw[len(GUIDE_MAGIC):]
                    path.write_bytes(duplicated)
                    payload["guide_validation"].update(
                        digest=hashlib.sha256(duplicated).hexdigest(),
                        byte_count=len(duplicated), record_count=2)
                with self.assertRaises((OSError, NormalizationError)):
                    normalize_completed_run(stage, baseline, payload, "2026-09-14 00:00:00")

    def test_stream_comparison_catches_guide_only_changes_beyond_diagnostic_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); baseline = root / "baseline.db"; create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, unused = make_run(root, "right", 20)
            for stage in (left_stage, right_stage):
                (stage / GUIDE_ARTIFACT_IDENTITY).unlink(); (stage / "guide").rmdir()
            total = MAX_DIFFERENCES + 25
            left_artifact = write_guide_stream(left_stage, ((f"/guide/item/{i:04d}", {"v": "l"})
                                                            for i in range(total)))
            right_artifact = write_guide_stream(right_stage, ((f"/guide/item/{i:04d}", {"v": "r"})
                                                              for i in range(total)))
            left_response = response("left"); left_response["guide_validation"].update(left_artifact)
            right_response = response("right"); right_response["guide_validation"].update(right_artifact)
            left = normalize_completed_run(left_stage, baseline, left_response, "2026-09-14 00:00:00")
            right = normalize_completed_run(right_stage, baseline, right_response, "2026-09-14 00:00:00")
            compared = compare_normalized_runs(left, right)
            self.assertFalse(compared["passed"])
            self.assertGreater(compared["changed_records"], MAX_DIFFERENCES)
            self.assertEqual(len(compared["differences"]), MAX_DIFFERENCES)


class FailureAndBoundaryTests(unittest.TestCase):
    def test_structured_primary_verification_and_combined_failures(self):
        from station_director import native_single_run as native

        snapshot = SimpleNamespace(working_logical_digest="same")
        manager = SimpleNamespace(stations=[], server_conf={})
        cases = ((GuideLoadingError("primary"), None),
                 (None, GuideValidationError("verify")),
                 (GuideValidationError("primary"), GuideValidationError("verify")))
        for primary, verification in cases:
            stream = patch.object(native, "stream_validate_staged_guide")
            with self.subTest(primary=primary, verification=verification), patch.object(
                    native, "prepare_guide_snapshot", return_value=snapshot), patch.object(
                    native, "StationManager", return_value=manager), stream as stream_mock, patch.object(
                    native, "fingerprint_guide_snapshot", return_value="same"), patch.object(
                    native, "verify_guide_snapshot", side_effect=verification):
                if primary is not None:
                    stream_mock.side_effect = primary
                else:
                    stream_mock.return_value = ({"format_version": 1}, [])
                with self.assertRaises(native.NativeRunError) as raised:
                    native._run_guide_validation(Path("synthetic.db"), [], PROPOSAL_START,
                                                 PROPOSAL_END)
            state = raised.exception.guide_validation
            self.assertEqual(state["primary_failure"] is not None, primary is not None)
            self.assertEqual(state["snapshot_verification"]["status"],
                             "failed" if verification else "pass")
            self.assertEqual(state["post_read_verification"]["status"], "pass")

        with patch.object(native, "prepare_guide_snapshot", return_value=snapshot), patch.object(
                native, "StationManager", return_value=manager), patch.object(
                native, "stream_validate_staged_guide", return_value=(
                    {"format_version": 1}, [])), patch.object(
                native, "fingerprint_guide_snapshot", side_effect=GuideValidationError("post")), patch.object(
                native, "verify_guide_snapshot"):
            with self.assertRaises(native.NativeRunError) as raised:
                native._run_guide_validation(Path("synthetic.db"), [], PROPOSAL_START,
                                             PROPOSAL_END)
        state = raised.exception.guide_validation
        self.assertIsNone(state["primary_failure"])
        self.assertEqual(state["post_read_verification"]["status"], "failed")
        self.assertEqual(state["snapshot_verification"]["status"], "pass")

    def test_protocol_accepts_structured_guide_failure_combinations(self):
        diagnostic = {"phase": "guide", "channel": None,
                      "code": "guide_validation_failed", "type": "GuideValidationError",
                      "message": "bounded"}
        for primary, snapshot_status, post_status in (
            ({"phase": "guide_read", "code": "guide_loading_failed",
              "message": "primary"}, "pass", "pass"),
            (None, "failed", "pass"),
            ({"phase": "guide_read", "code": "guide_validation_failed",
              "message": "primary"}, "failed", "failed"),
        ):
            guide = {"status": "failed", "primary_failure": primary,
                     "snapshot_preparation": {"status": "pass", "message": None},
                     "snapshot_verification": {
                         "status": snapshot_status,
                         "message": "snapshot" if snapshot_status == "failed" else None,
                     },
                     "post_read_verification": {
                         "status": post_status,
                         "message": "post" if post_status == "failed" else None,
                     }, "errors": [], "errors_truncated": False}
            payload = {"schema_version": 1, "operation": "native_single_run",
                       "run_id": "run", "proposal_id": "proposal", "status": "failed",
                       "phase_reached": "guide", "scheduler_invoked": True,
                       "validation_context": {"input_fingerprint": "0" * 64,
                                              "requested_seed": 1, "effective_seed": 2},
                       "affected_channels": [{"number": 2, "name": "Action"}],
                       "channels": [], "verification": {}, "preservation": {},
                       "path_validation": {}, "guide_validation": guide,
                       "warnings": [], "failure": diagnostic, "timings_ms": {},
                       "diagnostics": {"messages": [], "truncated": False}}
            validate_document(payload, RESPONSE_SCHEMA)
            validate_response_semantics(payload)

    def test_public_gate_and_host_imports_remain_closed(self):
        source = (ROOT / "station_director/validation.py").read_text(encoding="utf-8")
        self.assertIn('PHASE_3_DISABLED = "Phase 3 validation is not yet enabled"', source)
        self.assertIn('"scheduler_invoked": False', source)
        completed = subprocess.run([sys.executable, "-c", (
            "import sys; import station_director.dual_run; "
            "assert not any(n == 'fs42' or n.startswith('fs42.') for n in sys.modules)")],
            cwd=ROOT, check=False)
        self.assertEqual(completed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
