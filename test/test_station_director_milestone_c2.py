import json
import hashlib
import io
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock, patch

from station_director.dual_run import (
    CAPTURE_FAILURE_KINDS,
    DualRunError,
    _assert_inputs_stable,
    _capture_step,
    _capture_shared_inputs,
    _prepare_scope,
    _space_requirement,
    _validate_result_semantics,
    run_dual_comparison,
)
from station_director.isolation import LaunchResult
from station_director.isolation_probe import PROBE_RESULTS
from station_director.isolation_probe import PROBE_RESULTS
from station_director.policy import load_policy
from station_director.single_run import SingleRunError
from station_director.preservation import (
    MAX_FOREIGN_KEY_FINDINGS,
    MAX_PROTECTED_JSON_FILE_BYTES,
    fingerprint_and_clone_database_targets,
    fingerprint_database,
    fingerprint_json_files,
    logical_database_fingerprint,
)
from station_director.schedule_normalization import (
    MAX_DIFFERENCES,
    NormalizationError,
    _create_artifacts,
    _canonical,
    compare_normalized_runs,
    normalize_completed_run,
)
from test.test_station_director_schedule import base_proposal
from test.test_station_director_milestone_b2 import (
    catalog_row,
    create_database,
    insert_block,
)


def insert_catalog(connection, catalog_id, path, **changes):
    row = catalog_row("Action", path, "Show", "show")
    row.update(changes)
    columns = ["id", *row]
    connection.execute(
        f"INSERT INTO catalog_entries ({','.join(columns)}) VALUES ({','.join('?' for unused in columns)})",
        [catalog_id, *row.values()],
    )


ROOT = Path(__file__).parents[1]

GUIDE_MAGIC = b"FS42-GUIDE\x00\x01"
GUIDE_VALUE = {"path": "/guide/test", "value": {"title": "Synthetic"}}


def guide_bytes(value=GUIDE_VALUE):
    raw = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return GUIDE_MAGIC + len(raw).to_bytes(8, "big") + raw


def write_test_guide(stage, value=GUIDE_VALUE):
    directory = Path(stage) / "guide"
    directory.mkdir(mode=0o700, exist_ok=True)
    raw = guide_bytes(value)
    path = directory / "guide-v1.records"
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def guide_result(value=GUIDE_VALUE):
    raw = guide_bytes(value)
    return {
        "status": "pass", "format_version": 1,
        "primary_failure": None,
        "snapshot_preparation": {"status": "pass", "message": None},
        "snapshot_verification": {"status": "pass", "message": None},
        "post_read_verification": {"status": "pass", "message": None},
        "artifact_identity": "guide/guide-v1.records",
        "digest": hashlib.sha256(raw).hexdigest(), "record_count": 1,
        "byte_count": len(raw),
        "channels": [{
            "number": 2, "name": "Action", "network_long_name": "",
            "hidden": False, "has_schedule": True, "listing_count": 1,
            "transition_probe_count": 1, "zero_match_count": 0,
            "one_match_count": 1, "named_boundaries": [],
            "named_boundaries_truncated": False,
        }],
        "errors": [], "errors_truncated": False,
    }


def response(run_id, *, warning=None, timing=1, name="Action"):
    warnings = [] if warning is None else [{
        "phase": "native", "channel": None, "code": "native_warning",
        "type": "WARNING", "message": warning,
    }]
    return {
        "schema_version": 1,
        "operation": "native_single_run",
        "run_id": run_id,
        "proposal_id": "proposal",
        "status": "success",
        "phase_reached": "complete",
        "scheduler_invoked": True,
        "validation_context": {
            "input_fingerprint": "1" * 64,
            "requested_seed": 42,
            "effective_seed": 7,
        },
        "affected_channels": [{"number": 2, "name": name}],
        "channels": [{
            "name": name, "number": 2, "channel_seed": 9,
            "regeneration_start": "2026-09-14 00:00:00",
            "effective_horizon": "2026-09-21 00:00:00",
            "retained_blocks": 0, "generated_blocks": 1, "final_blocks": 1,
            "catalog_reused": 0, "catalog_new": 1, "catalog_protected": 0,
            "new_catalog_ids": "provisional",
            "coverage": {
                "channel": name, "proposal_boundary_crossing_ids": [],
                "proposal_end_crossing_ids": [], "effective_horizon_crossing_ids": [1],
                "gaps": [], "overlaps": [], "final_end": "2026-09-21 00:00:00",
            },
        }],
        "verification": {
            "original_snapshot": "pass", "deterministic_projection": "pass",
            "projected_configuration": "pass", "working_database": "pass",
            "logical_media": "pass", "physical_transition": "pass",
            "fingerprints": {
                "original_logical_configuration_fingerprint": "2" * 64,
                "original_logical_database_fingerprint": "3" * 64,
                "logical_media_manifest_fingerprint": "4" * 64,
                "live_physical_configuration_fingerprint": "5" * 64,
                "staged_source_physical_configuration_fingerprint": run_id[0] * 64,
                "projected_configuration_fingerprint": "6" * 64,
                "working_database_fingerprint": "7" * 64,
            },
        },
        "preservation": {
            "retained_history": "pass", "protected_channels": "pass",
            "sequence_tables_restored": "pass", "foreign_key_baseline": "pass",
        },
        "path_validation": {
            "passed": True, "mapping_count": 1, "scheduled_path_checks": 1,
        },
        "guide_validation": guide_result(),
        "warnings": warnings,
        "failure": None,
        "timings_ms": {
            "prepare": timing, "catalog": timing, "scheduler": timing,
            "preservation": timing, "guide": timing, "total": timing,
        },
        "diagnostics": {"messages": [], "truncated": False},
    }


def complete_worker_child_main(stage_text, media_text, project_text):
    """Test-only transport endpoint around the genuine C1 worker lifecycle."""
    import station_director.path_safety as path_safety
    import station_director.single_run_worker as worker
    import station_director.worker_bootstrap as bootstrap

    stage = Path(stage_text)
    media = Path(media_text)
    project = Path(project_text)
    request_path = stage / "native-single-run.request.json"
    response_path = stage / "native-single-run.response.json"
    request_value = json.loads(request_path.read_text(encoding="utf-8"))
    normalized_probes = {
        name: {"passed": True, "detail": "synthetic isolated transport"}
        for name in PROBE_RESULTS
    }
    # Only the unavailable external isolation transport/probe result is
    # replaced. Bootstrap, native imports, scheduler/catalog, and writes remain real.
    worker.STAGE_ROOT = stage
    worker.MEDIA_ROOT = media
    worker.PROJECT_ROOT = project
    canonical_media_mapping = path_safety.canonical_media_mapping

    def mounted_media_mapping(value, *args, **kwargs):
        text = str(value)
        try:
            relative = Path(text).relative_to(media)
        except ValueError:
            pass
        else:
            value = str(Path("/media") / relative)
        return canonical_media_mapping(value, *args, **kwargs)

    path_safety.canonical_media_mapping = mounted_media_mapping
    import station_director.staged_schedule as staged_schedule
    staged_schedule.canonical_media_mapping = mounted_media_mapping
    native_import = worker.importlib.import_module

    def mapped_import(name):
        module = native_import(name)
        if name == "station_director.native_single_run":
            module.STAGE_ROOT = stage
            module.MEDIA_ROOT = media
            module.PROJECT_ROOT = project
        return module

    with patch.object(bootstrap, "build_probe_payload", return_value={}), patch.object(
        bootstrap, "validate_probe_payload", return_value=(normalized_probes, None)
    ), patch.object(
        worker.importlib, "import_module", side_effect=mapped_import
    ):
        result = worker.run_worker(request_path, response_path)
    from fs42.catalog import ShowCatalog
    database_path = stage / "work/runtime/fs42_fluid.db"
    catalog_paths = []
    if database_path.exists():
        connection = sqlite3.connect(database_path)
        try:
            catalog_paths = connection.execute(
                "SELECT path,realpath FROM catalog_entries LIMIT 4"
            ).fetchall()
        finally:
            connection.close()
    info = {
        "pid": os.getpid(), "stage": str(stage),
        "database": str(stage / "work/runtime/fs42_fluid.db"),
        "request": str(request_path), "response": str(response_path),
        "fs42_loaded": any(name == "fs42" or name.startswith("fs42.") for name in sys.modules),
        "cache_size": len(ShowCatalog._fluid_cache_scanned),
        "status": result["status"], "failure": result["failure"],
        "guide_validation": result.get("guide_validation"),
        "catalog_paths": catalog_paths,
    }
    info_path = stage / "native-info.json"
    info_path.write_text(json.dumps(info), encoding="utf-8")
    os.chmod(info_path, 0o600)


def synthetic_project(root, media):
    project = root / "project"
    (project / "confs").mkdir(parents=True)
    (project / "runtime").mkdir()
    (project / "confs/main_config.json").write_text("{}\n", encoding="utf-8")
    station_conf = {
        "network_name": "Action", "channel_number": 2,
        "network_type": "standard", "schedule_increment": 60,
        "break_strategy": "end", "clip_shows": [],
        "content_dir": "/mnt/t7/CRT-Media/synthetic",
        "commercial_free": True, "shuffle_loop": False,
    }
    slots = {str(hour): {"tags": "Synthetic"} for hour in range(24)}
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
        station_conf[day] = slots
    (project / "confs/action.json").write_text(
        json.dumps({"station_conf": station_conf}) + "\n", encoding="utf-8"
    )
    (project / "runtime/watch_in_order_state.json").write_text("{}\n", encoding="utf-8")
    create_database(project / "runtime/fs42_fluid.db").close()
    (project / "fs42").mkdir()
    shutil.copy(
        ROOT / "fs42/station_config_schema.json",
        project / "fs42/station_config_schema.json",
    )
    return project


def make_run(root, name, catalog_id, media="a.mp4", **catalog_changes):
    stage = root / name
    (stage / "work/runtime").mkdir(parents=True)
    database = stage / "work/runtime/fs42_fluid.db"
    connection = create_database(database)
    insert_catalog(connection, catalog_id, f"/media/{media}", **catalog_changes)
    insert_block(
        connection, "Action", "2026-09-14 00:00:00",
        "2026-09-14 01:00:00", catalog_id, f"/media/{media}",
    )
    # Tests that isolate provisional-ID normalization hold allocation history
    # constant. Dedicated sqlite_sequence tests below vary it explicitly.
    connection.execute(
        "UPDATE sqlite_sequence SET seq=100 WHERE name IN ('catalog_entries','liquid_blocks')"
    )
    connection.commit()
    connection.close()
    write_test_guide(stage)
    return stage, database


def make_capture_fixture(root):
    source = root / "source"
    (source / "confs").mkdir(parents=True)
    (source / "runtime").mkdir()
    (source / "confs/main_config.json").write_text("{}\n", encoding="utf-8")
    (source / "confs/action.json").write_text(
        json.dumps({"station_conf": {"network_name": "Action"}}) + "\n",
        encoding="utf-8",
    )
    (source / "runtime/watch_in_order_state.json").write_text("{}\n", encoding="utf-8")
    create_database(source / "runtime/fs42_fluid.db").close()
    media = root / "media"
    media.mkdir()
    (media / "clip.mp4").write_bytes(b"synthetic")
    stages = [root / "stage-one", root / "stage-two"]
    for stage in stages:
        stage.mkdir(mode=0o700)
    with patch("station_director.dual_run._require_space"):
        capture = _capture_shared_inputs(source, media, stages)
    return source, media, capture


class CanonicalEncodingTests(unittest.TestCase):
    def test_real_encoding_is_ieee_and_preserves_negative_zero(self):
        self.assertNotEqual(_canonical(0.0), _canonical(-0.0))
        self.assertEqual(_canonical(1.5), _canonical(1.5))

    def test_nonfinite_values_are_rejected(self):
        for value in (math.inf, -math.inf, math.nan):
            with self.subTest(value=value), self.assertRaises(NormalizationError):
                _canonical(value)

    def test_text_and_blob_are_distinct_and_text_is_not_normalized(self):
        self.assertNotEqual(_canonical("é"), _canonical("e\u0301"))
        self.assertNotEqual(_canonical("abc"), _canonical(b"abc"))


class SharedSnapshotTests(unittest.TestCase):
    def _preparation_fixture(self, root):
        source = root / "project"
        (source / "confs").mkdir(parents=True)
        (source / "runtime").mkdir()
        (source / "confs/main_config.json").write_text("{}\n", encoding="utf-8")
        (source / "confs/action.json").write_text(
            json.dumps({"station_conf": {"network_name": "Action"}}) + "\n",
            encoding="utf-8",
        )
        (source / "runtime/watch_in_order_state.json").write_text(
            "{}\n", encoding="utf-8")
        create_database(source / "runtime/fs42_fluid.db").close()
        media = root / "media"
        media.mkdir()
        (media / "clip.mp4").write_bytes(b"synthetic")
        stages = [root / "fs42-i-000000000001", root / "fs42-i-000000000002"]
        for stage in stages:
            stage.mkdir(mode=0o700)
        locks = [Mock(name="lock-one"), Mock(name="lock-two")]
        allocations = [
            ("000000000001", stages[0], locks[0]),
            ("000000000002", stages[1], locks[1]),
        ]
        return source, media, stages, locks, allocations

    @staticmethod
    def _remove_test_stage(stage):
        shutil.rmtree(stage)
        return True, "removed"

    def test_every_capture_subphase_has_a_precise_safe_classification(self):
        hostile = RuntimeError(
            "password=hunter2 /etc/shadow /home/chaseanderegg secret-token")
        for kind in sorted(CAPTURE_FAILURE_KINDS):
            with self.subTest(kind=kind), self.assertRaises(DualRunError) as raised:
                _capture_step(kind, Mock(side_effect=hostile))
            self.assertEqual(raised.exception.code, "source_capture_failed")
            self.assertEqual(raised.exception.phase, "capture")
            self.assertEqual(raised.exception.capture_failure_kind, kind)
            self.assertEqual(str(raised.exception), "Source capture failed.")
            self.assertNotIn("hunter2", str(raised.exception))

    def test_capture_classification_preserves_explicit_codes_and_interrupts(self):
        explicit = DualRunError("capture", "backup_mismatch", "fixed")
        with self.assertRaises(DualRunError) as raised:
            _capture_step("database_backup_verification", Mock(side_effect=explicit))
        self.assertIs(raised.exception, explicit)
        with self.assertRaises(KeyboardInterrupt):
            _capture_step("database_snapshot", Mock(side_effect=KeyboardInterrupt()))

        class Cancellation(RuntimeError):
            is_validation_cancellation = True

        cancellation = Cancellation("stop")
        with self.assertRaises(Cancellation) as raised:
            _capture_step("media_manifest_capture", Mock(side_effect=cancellation))
        self.assertIs(raised.exception, cancellation)

        explicit_c1 = SingleRunError("prepare", "duplicate_stage_file", "fixed")
        with self.assertRaises(SingleRunError) as raised:
            _capture_step("single_run_finalization", Mock(side_effect=explicit_c1))
        self.assertIs(raised.exception, explicit_c1)

        class Impostor(RuntimeError):
            code = "backup_mismatch"
            phase = "complete"

        with self.assertRaises(DualRunError) as raised:
            _capture_step("database_snapshot", Mock(side_effect=Impostor("hostile")))
        self.assertEqual(raised.exception.code, "source_capture_failed")
        self.assertEqual(
            raised.exception.capture_failure_kind, "database_snapshot")

    def test_actual_capture_call_sites_classify_and_clean_every_created_stage(self):
        cases = (
            ("configuration_inventory", "protected_json_paths"),
            ("configuration_physical_fingerprint", "fingerprint_json_files"),
            ("configuration_logical_fingerprint",
             "logical_protected_configuration_fingerprint"),
            ("configuration_snapshot", "_captured_configuration_bytes"),
            ("free_space_check", "_require_space"),
            ("stage_source_publication", "_publish_sources"),
            ("database_snapshot", "fingerprint_and_clone_database_targets"),
            ("database_backup_verification", "os.chmod"),
            ("media_manifest_capture", "capture_media_manifest"),
            ("media_logical_fingerprint", "logical_media_manifest_fingerprint"),
            ("capture_artifact_initialization", "SharedCapture"),
            ("post_capture_stability", "_assert_inputs_stable"),
        )
        module = "station_director.dual_run."
        for kind, target in cases:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                source, media, stages, locks, allocations = self._preparation_fixture(
                    Path(directory))
                patches = [
                    patch(module + "check_invocation_context", return_value=(True, "ok")),
                    patch(module + "create_staging_directory", side_effect=allocations),
                    patch(module + "cleanup_staging_directory",
                          side_effect=self._remove_test_stage),
                ]
                if kind != "free_space_check":
                    patches.append(patch(module + "_require_space"))
                patches.append(patch(
                    module + target,
                    side_effect=RuntimeError("password=hunter2 /etc/shadow"),
                ))
                with ExitStack() as stack:
                    for active in patches:
                        stack.enter_context(active)
                    with self.assertRaises(DualRunError) as raised:
                        _prepare_scope(source, source, media, {}, {}, "comparison")
                self.assertEqual(raised.exception.code, "source_capture_failed")
                self.assertEqual(raised.exception.capture_failure_kind, kind)
                self.assertEqual(str(raised.exception), "Source capture failed.")
                self.assertEqual(
                    getattr(raised.exception, "preparation_cleanup"),
                    [{"run": 2, "passed": True, "quarantined": False,
                      "detail": "cleaned"},
                     {"run": 1, "passed": True, "quarantined": False,
                      "detail": "cleaned"}],
                )
                for lock in locks:
                    lock.close.assert_called_once_with()
                self.assertTrue(all(not stage.exists() for stage in stages))

    def test_repeated_capture_kind_call_sites_are_independently_classified(self):
        module = "station_director.dual_run."

        class BadLength:
            def __len__(self):
                raise RuntimeError("configuration size failed /etc/private")

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            with patch(module + "check_invocation_context", return_value=(True, "ok")), \
                    patch(module + "create_staging_directory", side_effect=allocations), \
                    patch(module + "_captured_configuration_bytes",
                          return_value={"confs/main_config.json": BadLength()}), \
                    patch(module + "cleanup_staging_directory",
                          side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(
                result["failure"]["capture_failure_kind"], "configuration_snapshot")
            self.assertTrue(all(item["passed"] for item in result["cleanup"]))
            self.assertTrue(all(not stage.exists() for stage in stages))

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            with patch(module + "check_invocation_context", return_value=(True, "ok")), \
                    patch(module + "create_staging_directory", side_effect=allocations), \
                    patch(module + "_require_space",
                          side_effect=[None, RuntimeError("second space check failed")]), \
                    patch(module + "cleanup_staging_directory",
                          side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(
                result["failure"]["capture_failure_kind"], "free_space_check")
            self.assertTrue(all(item["passed"] for item in result["cleanup"]))
            self.assertTrue(all(not stage.exists() for stage in stages))

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))

            class ReplacedPhysical(dict):
                def __getitem__(self, unused_key):
                    raise RuntimeError("physical capture replaced /etc/private")

            capture = SimpleNamespace(
                configuration_physical=ReplacedPhysical(),
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            with patch(module + "check_invocation_context", return_value=(True, "ok")), \
                    patch(module + "create_staging_directory", side_effect=allocations), \
                    patch(module + "_capture_shared_inputs", return_value=capture), \
                    patch(module + "cleanup_staging_directory",
                          side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(
                result["failure"]["capture_failure_kind"],
                "capture_artifact_initialization")
            capture.close.assert_called_once_with()
            self.assertTrue(all(not stage.exists() for stage in stages))

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            capture = SimpleNamespace(
                configuration_physical={"digest": "a" * 64},
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            first = SimpleNamespace(request={"validation_context": {}})
            with patch(module + "check_invocation_context", return_value=(True, "ok")), \
                    patch(module + "create_staging_directory", side_effect=allocations), \
                    patch(module + "_capture_shared_inputs", return_value=capture), \
                    patch(module + "_finalize_prepared_single_run",
                          side_effect=[first, RuntimeError("run two finalize failed")]), \
                    patch(module + "cleanup_staging_directory",
                          side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(
                result["failure"]["capture_failure_kind"],
                "single_run_finalization")
            capture.close.assert_called_once_with()
            for lock in locks:
                lock.close.assert_called_once_with()
            self.assertTrue(all(not stage.exists() for stage in stages))

    def test_path_invocation_allocation_and_context_call_sites_are_classified(self):
        class BadPath:
            def __fspath__(self):
                raise RuntimeError("path failed")

        with self.assertRaises(DualRunError) as raised:
            _prepare_scope(BadPath(), BadPath(), BadPath(), {}, {}, "comparison")
        self.assertEqual(raised.exception.capture_failure_kind, "source_path_resolution")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("station_director.dual_run.check_invocation_context",
                       side_effect=RuntimeError("ancestry failed")):
                with self.assertRaises(DualRunError) as raised:
                    _prepare_scope(root, root, root, {}, {}, "comparison")
            self.assertEqual(
                raised.exception.capture_failure_kind, "invocation_verification")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=RuntimeError("allocation failed")):
                with self.assertRaises(DualRunError) as raised:
                    _prepare_scope(root, root, root, {}, {}, "comparison")
            self.assertEqual(raised.exception.capture_failure_kind, "stage_allocation")

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            capture = SimpleNamespace(
                configuration_physical={"digest": "a" * 64},
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            lifecycle = SimpleNamespace(request={})
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run._capture_shared_inputs",
                    return_value=capture), patch(
                    "station_director.dual_run._finalize_prepared_single_run",
                    return_value=lifecycle), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=self._remove_test_stage):
                with self.assertRaises(DualRunError) as raised:
                    _prepare_scope(source, source, media, {}, {}, "comparison")
            self.assertEqual(raised.exception.capture_failure_kind, "context_consistency")
            capture.close.assert_called_once_with()
            self.assertTrue(all(not stage.exists() for stage in stages))

    def test_single_run_error_from_real_finalization_boundary_keeps_code(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            capture = SimpleNamespace(
                configuration_physical={"digest": "a" * 64},
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run._capture_shared_inputs",
                    return_value=capture), patch(
                    "station_director.dual_run._finalize_prepared_single_run",
                    side_effect=SingleRunError(
                        "prepare", "duplicate_stage_file", "/tmp/private")), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "duplicate_stage_file")
            self.assertEqual(result["failure"]["phase"], "capture")
            self.assertEqual(
                result["failure"]["message"],
                "A staged validation file already exists.")
            self.assertNotIn("capture_failure_kind", result["failure"])
            self.assertEqual([item["passed"] for item in result["cleanup"]],
                             [True, True])
            self.assertTrue(all(not stage.exists() for stage in stages))

    def test_uncoded_real_finalization_call_site_is_classified_and_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            capture = SimpleNamespace(
                configuration_physical={"digest": "a" * 64},
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run._capture_shared_inputs",
                    return_value=capture), patch(
                    "station_director.dual_run._finalize_prepared_single_run",
                    side_effect=RuntimeError("password=hunter2 /etc/shadow")), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "source_capture_failed")
            self.assertEqual(
                result["failure"]["capture_failure_kind"],
                "single_run_finalization")
            capture.close.assert_called_once_with()
            for lock in locks:
                lock.close.assert_called_once_with()
            self.assertTrue(all(not stage.exists() for stage in stages))
            self.assertNotIn("hunter2", json.dumps(result))
            self.assertNotIn("/etc", json.dumps(result))

    def test_post_preparation_context_failure_keeps_cleanup_owned(self):
        class OneReadContext(dict):
            reads = 0

            def __getitem__(self, key):
                if key == "validation_context":
                    self.reads += 1
                    if self.reads > 1:
                        raise RuntimeError("context replaced /etc/private")
                return super().__getitem__(key)

        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            context = {
                "input_fingerprint": "a" * 64, "requested_seed": None,
                "effective_seed": 1, "reference_clock": "2026-01-01T00:00:00",
                "start_time": "2026-01-01T00:00:00",
                "end_time": "2026-01-02T00:00:00",
                "timezone": "America/Los_Angeles",
            }
            capture = SimpleNamespace(
                configuration_physical={"digest": "a" * 64},
                configuration_digest="b" * 64, database_digest="c" * 64,
                media_logical_digest="d" * 64, close=Mock(),
            )
            lifecycles = []

            def finalize(unused_root, stage, lock, token, run_id,
                         unused_proposal, unused_policy, **unused_kwargs):
                request = OneReadContext(
                    validation_context=context,
                    affected_channels=[{"number": 2, "name": "Action"}],
                ) if not lifecycles else {
                    "validation_context": context,
                    "affected_channels": [{"number": 2, "name": "Action"}],
                }

                def cleanup(stage=stage, lock=lock):
                    lock.close()
                    shutil.rmtree(stage)
                    return {"passed": True, "detail": "cleaned"}

                lifecycle = SimpleNamespace(
                    request=request, stage=stage, run_id=run_id,
                    cleanup=Mock(side_effect=cleanup),
                )
                lifecycles.append(lifecycle)
                return lifecycle

            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run._capture_shared_inputs",
                    return_value=capture), patch(
                    "station_director.dual_run._finalize_prepared_single_run",
                    side_effect=finalize), patch(
                    "station_director.dual_run._assert_inputs_stable",
                    return_value={"checkpoint": "before_success", "passed": True,
                                  "changed_categories": []}):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "source_capture_failed")
            self.assertEqual(
                result["failure"]["capture_failure_kind"], "context_consistency")
            self.assertEqual([item["passed"] for item in result["cleanup"]],
                             [True, True])
            for lifecycle in lifecycles:
                lifecycle.cleanup.assert_called_once_with()
            capture.close.assert_called_once_with()
            self.assertTrue(all(not stage.exists() for stage in stages))

    def test_manifest_close_failure_is_separate_from_classified_primary(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            manifest = SimpleNamespace(
                stream=io.BytesIO(b"manifest"),
                close=Mock(side_effect=OSError("secret close failure /etc/shadow")),
            )
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run._require_space"), patch(
                    "station_director.dual_run.capture_media_manifest",
                    return_value=manifest), patch(
                    "station_director.dual_run.logical_media_manifest_fingerprint",
                    side_effect=RuntimeError("password=hunter2")), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "source_capture_failed")
            self.assertEqual(
                result["failure"]["capture_failure_kind"],
                "media_logical_fingerprint")
            manifest.close.assert_called_once_with()
            self.assertFalse(result["cleanup"][1]["passed"])
            self.assertIn("capture_close_failed", result["cleanup"][1]["detail"])
            self.assertNotIn("hunter2", json.dumps(result))
            self.assertNotIn("/etc", json.dumps(result))
            self.assertTrue(all(not stage.exists() for stage in stages))

    def test_each_preparation_cleanup_failure_preserves_primary_and_attempts_all(self):
        for failure_mode in (
                "lock_close", "stage_inspection", "stage_removal", "quarantine"):
            with self.subTest(failure_mode=failure_mode), \
                    tempfile.TemporaryDirectory() as directory:
                source, media, stages, locks, allocations = self._preparation_fixture(
                    Path(directory))
                if failure_mode == "lock_close":
                    locks[1].close.side_effect = OSError("private lock path")
                removal_calls = []
                quarantine_calls = []

                def remove(stage):
                    removal_calls.append(stage)
                    if failure_mode in {"stage_removal", "quarantine"} and stage == stages[1]:
                        return False, "private removal failure"
                    return self._remove_test_stage(stage)

                from station_director import dual_run as dual_module
                real_inspect = dual_module._preparation_stage_exists
                real_quarantine = dual_module._quarantine_preparation_stage

                def inspect(stage):
                    if failure_mode == "stage_inspection" and stage == stages[1]:
                        raise OSError("private inspection failure")
                    return real_inspect(stage)

                def quarantine(stage):
                    quarantine_calls.append(stage)
                    if failure_mode == "quarantine" and stage == stages[1]:
                        return False
                    return real_quarantine(stage)

                with patch("station_director.dual_run.check_invocation_context",
                           return_value=(True, "ok")), patch(
                        "station_director.dual_run.create_staging_directory",
                        side_effect=allocations), patch(
                        "station_director.dual_run.fingerprint_json_files",
                        side_effect=RuntimeError("password=hunter2 /var/private")), patch(
                        "station_director.dual_run.cleanup_staging_directory",
                        side_effect=remove), patch(
                        "station_director.dual_run._preparation_stage_exists",
                        side_effect=inspect), patch(
                        "station_director.dual_run._quarantine_preparation_stage",
                        side_effect=quarantine):
                    result = run_dual_comparison(
                        source, source, media, {}, {}, "comparison")

                self.assertEqual(result["failure"]["code"], "source_capture_failed")
                self.assertEqual(
                    result["failure"]["capture_failure_kind"],
                    "configuration_physical_fingerprint")
                self.assertEqual([item["run"] for item in result["cleanup"]], [2, 1])
                self.assertTrue(any(not item["passed"] for item in result["cleanup"]))
                for lock in locks:
                    lock.close.assert_called_once_with()
                if failure_mode != "stage_inspection":
                    self.assertEqual(set(removal_calls), set(stages))
                else:
                    self.assertIn(stages[0], removal_calls)
                    self.assertIn(stages[1], quarantine_calls)
                if failure_mode in {"stage_inspection", "stage_removal"}:
                    self.assertTrue((stages[1] / ".quarantine").is_file())
                    self.assertTrue(result["cleanup"][0]["quarantined"])
                elif failure_mode == "quarantine":
                    self.assertTrue(stages[1].is_dir())
                    self.assertFalse(result["cleanup"][0]["quarantined"])
                    self.assertIn(
                        "stage_quarantine_failed", result["cleanup"][0]["detail"])
                else:
                    self.assertTrue(all(not stage.exists() for stage in stages))
                encoded = json.dumps(result, sort_keys=True)
                self.assertNotIn("hunter2", encoded)
                self.assertNotIn("/var", encoded)

    def test_multiple_cleanup_failures_do_not_short_circuit_or_erase_primary(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            for lock in locks:
                lock.close.side_effect = OSError("private lock failure")
            inspection_calls = []
            removal_calls = []
            quarantine_calls = []

            def inspect(stage):
                inspection_calls.append(stage)
                if stage == stages[1]:
                    raise OSError("private inspection failure")
                return True

            def remove(stage):
                removal_calls.append(stage)
                return False, "private removal failure"

            def quarantine(stage):
                quarantine_calls.append(stage)
                if stage == stages[1]:
                    return False
                return True

            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run.fingerprint_json_files",
                    side_effect=RuntimeError("password=hunter2 /var/private")), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=remove), patch(
                    "station_director.dual_run._preparation_stage_exists",
                    side_effect=inspect), patch(
                    "station_director.dual_run._quarantine_preparation_stage",
                    side_effect=quarantine):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")

            self.assertEqual(result["failure"]["code"], "source_capture_failed")
            self.assertEqual(
                result["failure"]["capture_failure_kind"],
                "configuration_physical_fingerprint")
            for lock in locks:
                lock.close.assert_called_once_with()
            self.assertEqual(set(inspection_calls), set(stages))
            self.assertEqual(removal_calls, [stages[0]])
            self.assertEqual(set(quarantine_calls), set(stages))
            self.assertEqual([item["passed"] for item in result["cleanup"]],
                             [False, False])
            self.assertIn("stage_quarantine_failed",
                          result["cleanup"][0]["detail"])
            self.assertTrue(result["cleanup"][1]["quarantined"])
            self.assertTrue(all(stage.exists() for stage in stages))
            encoded = json.dumps(result, sort_keys=True)
            self.assertNotIn("hunter2", encoded)
            self.assertNotIn("/var", encoded)

    def test_second_stage_allocation_failure_cleans_first_stage_and_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, stages, locks, allocations = self._preparation_fixture(
                Path(directory))
            shutil.rmtree(stages[1])
            allocations = [allocations[0], RuntimeError("second allocation failed")]
            with patch("station_director.dual_run.check_invocation_context",
                       return_value=(True, "ok")), patch(
                    "station_director.dual_run.create_staging_directory",
                    side_effect=allocations), patch(
                    "station_director.dual_run.cleanup_staging_directory",
                    side_effect=self._remove_test_stage):
                result = run_dual_comparison(
                    source, source, media, {}, {}, "comparison")
            self.assertEqual(
                result["failure"]["capture_failure_kind"], "stage_allocation")
            locks[0].close.assert_called_once_with()
            self.assertFalse(stages[0].exists())
            self.assertEqual(result["cleanup"], [
                {"run": 1, "passed": True, "quarantined": False,
                 "detail": "cleaned"},
            ])

    def test_c2_result_carries_kind_without_hostile_capture_detail(self):
        for kind in sorted(CAPTURE_FAILURE_KINDS):
            with self.subTest(kind=kind):
                with self.assertRaises(DualRunError) as raised:
                    _capture_step(kind, Mock(side_effect=RuntimeError(
                        "password=hunter2 /etc/shadow SECRET_TOKEN=value")))
                classified = raised.exception
                with patch("station_director.dual_run._prepare_scope",
                           side_effect=classified):
                    result = run_dual_comparison(
                        Path("synthetic"), Path("synthetic"), Path("synthetic"),
                        {}, {}, "comparison")
                self.assertEqual(result["failure"], {
                    "phase": "capture", "code": "source_capture_failed",
                    "category": None, "message": "Source capture failed.",
                    "capture_failure_kind": kind,
                })
                encoded = json.dumps(result, sort_keys=True)
                self.assertNotIn("hunter2", encoded)
                self.assertNotIn("/etc", encoded)
                self.assertNotIn("SECRET_TOKEN", encoded)

    def test_c2_preserves_an_existing_explicit_capture_error_code(self):
        explicit = DualRunError("capture", "backup_mismatch", "fixed")
        with patch("station_director.dual_run._prepare_scope", side_effect=explicit):
            result = run_dual_comparison(
                Path("synthetic"), Path("synthetic"), Path("synthetic"),
                {}, {}, "comparison")
        self.assertEqual(result["failure"]["code"], "backup_mismatch")
        self.assertNotIn("capture_failure_kind", result["failure"])

    def test_c2_schema_rejects_unclassified_or_misclassified_capture_failure(self):
        from station_director.dual_run import RESULT_SCHEMA
        from station_director.single_run_protocol import validate_document

        result = {
            "schema_version": 1, "operation": "native_dual_run_comparison",
            "comparison_id": "comparison", "status": "failed",
            "phase_reached": "capture",
            "scheduler_invoked": {"run_1": False, "run_2": False},
            "validation_context": {}, "affected_channels": [],
            "source_checks": [], "runs": [], "reproducibility": None,
            "baseline_comparison": None, "cleanup": [],
            "timings_ms": {"total": 0},
            "failure": {"phase": "capture", "code": "source_capture_failed",
                        "category": None, "message": "Source capture failed."},
        }
        with self.assertRaises(Exception):
            validate_document(result, RESULT_SCHEMA)
        result["failure"]["capture_failure_kind"] = "unknown"
        with self.assertRaises(Exception):
            validate_document(result, RESULT_SCHEMA)
        result["failure"] = {
            "phase": "capture", "code": "backup_mismatch", "category": None,
            "message": "fixed", "capture_failure_kind": "database_snapshot",
        }
        with self.assertRaises(Exception):
            validate_document(result, RESULT_SCHEMA)

    def test_two_backups_are_verified_from_one_pinned_view(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            connection = create_database(source)
            connection.execute(
                "INSERT INTO sequence_group_state VALUES ('Action','s','p','a')"
            )
            connection.commit()
            connection.close()
            result = fingerprint_and_clone_database_targets(
                source, [root / "one.db", root / "two.db"]
            )
            connection = sqlite3.connect(source)
            connection.execute(
                "UPDATE sequence_group_state SET active_tag_path='changed'"
            )
            connection.commit()
            connection.close()
            self.assertEqual(len(result["backups"]), 2)
            self.assertEqual(
                result["backups"][0]["logical"]["digest"],
                result["backups"][1]["logical"]["digest"],
            )
            self.assertNotEqual(
                (root / "one.db").stat().st_ino,
                (root / "two.db").stat().st_ino,
            )
            self.assertNotEqual(
                fingerprint_database(source)["logical"]["digest"],
                result["logical"]["digest"],
            )

    def test_space_requirement_has_quarantine_margin(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "db"
            database.write_bytes(b"x" * 4096)
            self.assertGreaterEqual(_space_requirement(database, 100, 100), 512 * 1024 * 1024)

    def test_stability_check_classifies_source_categories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            (source / "confs").mkdir(parents=True)
            (source / "runtime").mkdir()
            (source / "confs/main_config.json").write_text("{}\n")
            (source / "confs/action.json").write_text(
                json.dumps({"station_conf": {"network_name": "Action"}}) + "\n"
            )
            (source / "runtime/watch_in_order_state.json").write_text("{}\n")
            connection = create_database(source / "runtime/fs42_fluid.db")
            connection.commit()
            connection.close()
            media = root / "media"
            media.mkdir()
            (media / "clip.mp4").write_bytes(b"one")
            stages = [root / "stage-one", root / "stage-two"]
            for stage in stages:
                stage.mkdir(mode=0o700)
            with patch("station_director.dual_run._require_space"):
                capture = _capture_shared_inputs(source, media, stages)
            try:
                (source / "confs/action.json").write_text(
                    json.dumps({"station_conf": {"network_name": "Changed"}}) + "\n"
                )
                connection = sqlite3.connect(source / "runtime/fs42_fluid.db")
                connection.execute(
                    "INSERT INTO sequence_group_state VALUES ('Action','s','p','a')"
                )
                connection.commit()
                connection.close()
                (media / "clip.mp4").write_bytes(b"different-size")
                with self.assertRaises(DualRunError) as raised:
                    _assert_inputs_stable(source, media, capture, "between_runs")
                self.assertEqual(raised.exception.code, "input_changed")
                self.assertEqual(set(raised.exception.category), {
                    "physical_configuration", "logical_configuration",
                    "logical_database", "physical_media_metadata", "logical_media",
                })
            finally:
                capture.close()

    def test_each_recheck_fingerprint_exception_is_input_changed(self):
        capture = SimpleNamespace(
            configuration_physical={"digest": "p"}, configuration_digest="c",
            database_digest="d", media_manifest=object(), media_logical_digest="m",
            spool_directory=Path("/tmp"),
        )
        cases = (
            ("fingerprint_json_files", "physical_configuration"),
            ("logical_protected_configuration_fingerprint", "logical_configuration"),
            ("fingerprint_database", "logical_database"),
            ("capture_media_manifest", "physical_media_metadata"),
            ("logical_media_manifest_fingerprint", "logical_media"),
        )
        physical = {"digest": "p"}
        logical = {"digest": "c"}
        database = {"logical": {"digest": "d"}}
        manifest = SimpleNamespace(close=lambda: None)
        comparison = {"preserved": True}
        for target, category in cases:
            mocks = {
                "fingerprint_json_files": Mock(return_value=physical),
                "logical_protected_configuration_fingerprint": Mock(return_value=logical),
                "fingerprint_database": Mock(return_value=database),
                "capture_media_manifest": Mock(return_value=manifest),
                "compare_media_manifests": Mock(return_value=comparison),
                "logical_media_manifest_fingerprint": Mock(return_value={"digest": "m"}),
            }
            mocks[target].side_effect = OSError("synthetic fingerprint failure")
            with self.subTest(category=category), patch.multiple(
                "station_director.dual_run", **mocks
            ):
                with self.assertRaises(DualRunError) as raised:
                    _assert_inputs_stable(Path("/source"), Path("/media"), capture, "between_runs")
                self.assertEqual(raised.exception.code, "input_changed")
                self.assertEqual(raised.exception.category, [category])

    def test_deleted_unreadable_malformed_and_symlinked_inputs_are_input_changed(self):
        mutations = ("deleted", "unreadable", "malformed_json", "symlink", "malformed_database")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, media, capture = make_capture_fixture(root)
                target = source / "confs/action.json"
                try:
                    if mutation == "deleted":
                        target.unlink()
                    elif mutation == "unreadable":
                        target.chmod(0)
                    elif mutation == "malformed_json":
                        target.write_text("{", encoding="utf-8")
                    elif mutation == "symlink":
                        target.unlink()
                        target.symlink_to(source / "confs/main_config.json")
                    else:
                        (source / "runtime/fs42_fluid.db").write_bytes(b"not sqlite")
                    with self.assertRaises(DualRunError) as raised:
                        _assert_inputs_stable(source, media, capture, "between_runs")
                    self.assertEqual(raised.exception.code, "input_changed")
                    if mutation == "deleted":
                        self.assertEqual(
                            raised.exception.category,
                            ["physical_configuration", "logical_configuration"],
                        )
                    else:
                        expected = (
                            "logical_database" if mutation == "malformed_database"
                            else "logical_configuration" if mutation == "malformed_json"
                            else "physical_configuration"
                        )
                        self.assertEqual(raised.exception.category, [expected])
                finally:
                    capture.close()

    def test_protected_configuration_and_schema_limits_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            oversized = root / "oversized.json"
            oversized.write_bytes(b" " * (MAX_PROTECTED_JSON_FILE_BYTES + 1))
            with self.assertRaisesRegex(Exception, "size limit"):
                fingerprint_json_files({"confs/oversized.json": oversized})

            database = root / "bounded.db"
            connection = sqlite3.connect(database)
            connection.execute("CREATE TABLE one(value)")
            connection.execute("CREATE TABLE two(value)")
            connection.commit()
            try:
                with patch("station_director.preservation.MAX_DATABASE_TABLES", 1), \
                        self.assertRaisesRegex(Exception, "table limit"):
                    logical_database_fingerprint(connection)
            finally:
                connection.close()

    def test_foreign_key_findings_are_iterated_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = sqlite3.connect(Path(directory) / "foreign.db")
            connection.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            connection.execute("CREATE TABLE child(parent_id INTEGER REFERENCES parent(id))")
            connection.executemany("INSERT INTO child VALUES (?)", [(1,), (2,)])
            connection.commit()
            try:
                with patch("station_director.preservation.MAX_FOREIGN_KEY_FINDINGS", 1), \
                        self.assertRaisesRegex(Exception, "foreign-key finding limit"):
                    logical_database_fingerprint(connection)
            finally:
                connection.close()


class NormalizationTests(unittest.TestCase):
    def test_sqlite_sequence_is_exact_and_exposes_allocation_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, left_db = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)

            identical_left = normalize_completed_run(
                left_stage, baseline, response("left"), "2026-09-14 00:00:00"
            )
            identical_right = normalize_completed_run(
                right_stage, baseline, response("right"), "2026-09-14 00:00:00"
            )
            self.assertTrue(compare_normalized_runs(identical_left, identical_right)["passed"])

            advanced_stage, advanced_db = make_run(root, "right-advanced", 10)
            connection = sqlite3.connect(advanced_db)
            connection.execute(
                "UPDATE sqlite_sequence SET seq=101 WHERE name='catalog_entries'"
            )
            connection.commit()
            connection.close()
            advanced = normalize_completed_run(
                advanced_stage,
                baseline, response("right"), "2026-09-14 00:00:00"
            )
            self.assertFalse(compare_normalized_runs(identical_left, advanced)["passed"])

    def test_allocated_then_deleted_catalog_id_is_not_normalized_away(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)
            connection = sqlite3.connect(right_db)
            insert_catalog(connection, 101, "/media/deleted.mp4")
            connection.execute("DELETE FROM catalog_entries WHERE id=101")
            connection.commit()
            connection.close()
            left = normalize_completed_run(left_stage, baseline, response("left"), "2026-09-14 00:00:00")
            right = normalize_completed_run(right_stage, baseline, response("right"), "2026-09-14 00:00:00")
            self.assertFalse(compare_normalized_runs(left, right)["passed"])

    def test_sqlite_sequence_regression_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, database = make_run(root, "stage", 10)
            connection = sqlite3.connect(database)
            connection.execute(
                "UPDATE sqlite_sequence SET seq=9 WHERE name='catalog_entries'"
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "regressed"):
                normalize_completed_run(stage, baseline, response("run"), "2026-09-14 00:00:00")

    def test_reference_ledger_detects_a_second_pass_omission(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, unused = make_run(root, "stage", 10)
            calls = iter(((10,), ()))
            with patch(
                "station_director.schedule_normalization.parse_catalog_references",
                side_effect=lambda unused: next(calls),
            ), self.assertRaisesRegex(NormalizationError, "reference integrity"):
                normalize_completed_run(stage, baseline, response("run"), "2026-09-14 00:00:00")

    def test_partial_artifacts_are_removed_when_lookup_creation_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "stage"
            stage.mkdir()
            before_fds = len(tuple(Path("/proc/self/fd").iterdir()))
            with patch(
                "station_director.schedule_normalization.sqlite3.connect",
                side_effect=sqlite3.OperationalError("synthetic"),
            ), self.assertRaises(sqlite3.OperationalError):
                from station_director.schedule_normalization import _create_artifacts
                _create_artifacts(stage)
            self.assertFalse((stage / "normalization").exists())
            self.assertEqual(len(tuple(Path("/proc/self/fd").iterdir())), before_fds)

    def test_partial_artifacts_close_after_each_acquisition_failure(self):
        real_open = os.open
        for failed_open in (1, 2):
            with self.subTest(failed_open=failed_open), tempfile.TemporaryDirectory() as directory:
                stage = Path(directory) / "stage"
                stage.mkdir()
                calls = 0

                def controlled_open(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == failed_open:
                        raise OSError("synthetic open failure")
                    return real_open(*args, **kwargs)

                before = len(tuple(Path("/proc/self/fd").iterdir()))
                with patch("station_director.schedule_normalization.os.open", side_effect=controlled_open), \
                        self.assertRaises(OSError):
                    _create_artifacts(stage)
            self.assertFalse((stage / "normalization").exists())
            self.assertEqual(len(tuple(Path("/proc/self/fd").iterdir())), before)

        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "stage"
            existing = stage / "normalization"
            existing.mkdir(parents=True)
            sentinel = existing / "canonical.records"
            sentinel.write_text("preexisting", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                _create_artifacts(stage)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preexisting")

        class SchemaFailureProxy:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, *args, **kwargs):
                return self.connection.execute(*args, **kwargs)

            def executescript(self, unused):
                raise sqlite3.OperationalError("synthetic schema failure")

            def close(self):
                self.connection.close()

        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "stage"
            stage.mkdir()
            real_connect = sqlite3.connect
            before = len(tuple(Path("/proc/self/fd").iterdir()))
            with patch(
                "station_director.schedule_normalization.sqlite3.connect",
                side_effect=lambda path: SchemaFailureProxy(real_connect(path)),
            ), self.assertRaises(sqlite3.OperationalError):
                _create_artifacts(stage)
            self.assertFalse((stage / "normalization").exists())
            self.assertEqual(len(tuple(Path("/proc/self/fd").iterdir())), before)

    def test_preexisting_normalization_directory_is_not_modified(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "stage"
            existing = stage / "normalization"
            existing.mkdir(parents=True)
            marker = existing / "canonical.records"
            marker.write_text("belongs to another operation", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                _create_artifacts(stage)
            self.assertEqual(marker.read_text(encoding="utf-8"), "belongs to another operation")
    def test_equivalent_provisional_ids_compare_equal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, unused = make_run(root, "right", 20)
            left = normalize_completed_run(
                left_stage, baseline, response("a-run", timing=1), "2026-09-14 00:00:00"
            )
            right = normalize_completed_run(
                right_stage, baseline, response("b-run", timing=999), "2026-09-14 00:00:00"
            )
            comparison = compare_normalized_runs(left, right)
            self.assertTrue(comparison["passed"], comparison["differences"][:2])
            self.assertEqual(left.provisional_count, 1)

    def test_selected_media_and_playback_plan_difference_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10, "a.mp4")
            right_stage, unused = make_run(root, "right", 20, "b.mp4")
            left = normalize_completed_run(left_stage, baseline, response("a"), "2026-09-14 00:00:00")
            right = normalize_completed_run(right_stage, baseline, response("b"), "2026-09-14 00:00:00")
            comparison = compare_normalized_runs(left, right)
            self.assertFalse(comparison["passed"])
            self.assertGreater(
                comparison["changed_records"] + comparison["added_records"] + comparison["removed_records"], 0
            )

    def test_catalog_timestamp_count_and_storage_class_remain_equality_inputs(self):
        variants = [
            {"count": 2},
            {"updated_at": "2026-09-02 00:00:00"},
            {"duration": 3601.0},
            {"count": "not-an-integer"},
        ]
        for changes in variants:
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                baseline = root / "baseline.db"
                create_database(baseline).close()
                left_stage, unused = make_run(root, "left", 10)
                right_stage, unused = make_run(root, "right", 20, **changes)
                left = normalize_completed_run(left_stage, baseline, response("a"), "2026-09-14 00:00:00")
                right = normalize_completed_run(right_stage, baseline, response("b"), "2026-09-14 00:00:00")
                self.assertFalse(compare_normalized_runs(left, right)["passed"])

    def test_channel_seed_schedule_time_and_playback_fields_remain_equality_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)
            changed_response = response("right")
            changed_response["channels"][0]["channel_seed"] = 10
            left = normalize_completed_run(
                left_stage, baseline, response("left"), "2026-09-14 00:00:00"
            )
            right = normalize_completed_run(
                right_stage, baseline, changed_response, "2026-09-14 00:00:00"
            )
            self.assertFalse(compare_normalized_runs(left, right)["passed"])

            third_stage, third_db = make_run(root, "third", 30)
            connection = sqlite3.connect(third_db)
            plan = json.loads(connection.execute(
                "SELECT plan_json FROM liquid_blocks LIMIT 1"
            ).fetchone()[0])
            plan[0]["skip"] = 1
            connection.execute(
                "UPDATE liquid_blocks SET start_time=?, plan_json=?",
                ("2026-09-14 00:00:01", json.dumps(plan)),
            )
            connection.commit()
            connection.close()
            third = normalize_completed_run(
                third_stage, baseline, response("third"), "2026-09-14 00:00:00"
            )
            self.assertFalse(compare_normalized_runs(left, third)["passed"])

    def test_warnings_and_counters_compare_but_run_id_timing_and_stage_physical_do_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, unused = make_run(root, "right", 20)
            left = normalize_completed_run(left_stage, baseline, response("a", timing=1), "2026-09-14 00:00:00")
            right = normalize_completed_run(right_stage, baseline, response("b", timing=900), "2026-09-14 00:00:00")
            self.assertTrue(compare_normalized_runs(left, right)["passed"])
            changed_stage, unused = make_run(root, "changed", 30)
            changed = normalize_completed_run(
                changed_stage, baseline, response("c", warning="warning changed"),
                "2026-09-14 00:00:00",
            )
            self.assertFalse(compare_normalized_runs(left, changed)["passed"])

    def test_hidden_reference_fails_but_unrelated_integer_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            safe_stage, safe_db = make_run(root, "safe", 10)
            connection = sqlite3.connect(safe_db)
            connection.execute(
                "UPDATE liquid_blocks SET break_info=?", (json.dumps({"episode_number": 10}),)
            )
            connection.commit()
            connection.close()
            normalize_completed_run(safe_stage, baseline, response("safe"), "2026-09-14 00:00:00")
            bad_stage, bad_db = make_run(root, "bad", 20)
            connection = sqlite3.connect(bad_db)
            connection.execute(
                "UPDATE liquid_blocks SET break_info=?", (json.dumps({"nested": {"catalog_id": 20}}),)
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "nested reference"):
                normalize_completed_run(bad_stage, baseline, response("bad"), "2026-09-14 00:00:00")

            camel_stage, camel_db = make_run(root, "camel", 30)
            connection = sqlite3.connect(camel_db)
            connection.execute(
                "UPDATE liquid_blocks SET break_info=?",
                (json.dumps({"hidden": {"catalogReferenceId": 30}}),),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "nested reference"):
                normalize_completed_run(
                    camel_stage, baseline, response("camel"),
                    "2026-09-14 00:00:00",
                )

    def test_unknown_table_without_primary_key_has_typed_deterministic_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, left_db = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)
            for database, values in ((left_db, [(2, "b"), (1, "a")]), (right_db, [(1, "a"), (2, "b")])):
                connection = sqlite3.connect(database)
                connection.execute("CREATE TABLE extra(value, label TEXT)")
                connection.executemany("INSERT INTO extra VALUES (?,?)", values)
                connection.commit()
                connection.close()
            left = normalize_completed_run(left_stage, baseline, response("a"), "2026-09-14 00:00:00")
            right = normalize_completed_run(right_stage, baseline, response("b"), "2026-09-14 00:00:00")
            self.assertTrue(compare_normalized_runs(left, right)["passed"])

    def test_artifacts_are_private_and_working_database_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, database = make_run(root, "stage", 10)
            before = fingerprint_database(database)["logical"]["digest"]
            normalized = normalize_completed_run(stage, baseline, response("a"), "2026-09-14 00:00:00")
            after = fingerprint_database(database)["logical"]["digest"]
            self.assertEqual(before, after)
            for path in normalized.artifact_directory.iterdir():
                self.assertTrue(path.is_file())
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_canonical_stream_replacement_or_modification_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, unused = make_run(root, "left", 10)
            right_stage, unused = make_run(root, "right", 20)
            left = normalize_completed_run(
                left_stage, baseline, response("left"), "2026-09-14 00:00:00"
            )
            right = normalize_completed_run(
                right_stage, baseline, response("right"), "2026-09-14 00:00:00"
            )
            left.stream_path.write_bytes(left.stream_path.read_bytes() + b"x")
            with self.assertRaisesRegex(NormalizationError, "identity changed"):
                compare_normalized_runs(left, right)

    def test_protected_historical_catalog_change_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            connection = create_database(baseline)
            insert_catalog(connection, 1, "/media/a.mp4")
            insert_block(connection, "Action", "2026-09-13 00:00:00", "2026-09-13 01:00:00", 1, "/media/a.mp4")
            connection.commit()
            connection.close()
            stage = root / "stage"
            (stage / "work/runtime").mkdir(parents=True)
            fingerprint_and_clone_database_targets(baseline, [stage / "work/runtime/fs42_fluid.db"])
            connection = sqlite3.connect(stage / "work/runtime/fs42_fluid.db")
            connection.execute("UPDATE catalog_entries SET count=99 WHERE id=1")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "protected catalog"):
                normalize_completed_run(stage, baseline, response("a"), "2026-09-14 00:00:00")

    def test_ambiguous_provisional_semantics_and_unresolved_reference_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, database = make_run(root, "stage", 10)
            connection = sqlite3.connect(database)
            duplicate = catalog_row(
                "Action", "/mnt/t7/CRT-Media/a.mp4", "Show", "show"
            )
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (11, *[duplicate[key] for key in duplicate]),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "ambiguous duplicate"):
                normalize_completed_run(stage, baseline, response("a"), "2026-09-14 00:00:00")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, database = make_run(root, "stage", 10)
            connection = sqlite3.connect(database)
            connection.execute("UPDATE liquid_blocks SET content_json='999'")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "unresolved catalog reference"):
                normalize_completed_run(stage, baseline, response("a"), "2026-09-14 00:00:00")

    def test_unclassified_reference_column_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            stage, database = make_run(root, "stage", 10)
            connection = sqlite3.connect(database)
            connection.execute("CREATE TABLE extra(label TEXT, catalog_id INTEGER)")
            connection.execute("INSERT INTO extra VALUES ('not trusted',10)")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(NormalizationError, "unclassified possible ID"):
                normalize_completed_run(stage, baseline, response("a"), "2026-09-14 00:00:00")

    def test_schema_index_and_trigger_definitions_are_compared(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, left_db = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)
            left = sqlite3.connect(left_db)
            left.execute("CREATE INDEX extra_index ON liquid_blocks(title)")
            left.commit()
            left.close()
            right = sqlite3.connect(right_db)
            right.execute(
                "CREATE TRIGGER extra_trigger AFTER INSERT ON liquid_blocks BEGIN SELECT 1; END"
            )
            right.commit()
            right.close()
            one = normalize_completed_run(left_stage, baseline, response("a"), "2026-09-14 00:00:00")
            two = normalize_completed_run(right_stage, baseline, response("b"), "2026-09-14 00:00:00")
            self.assertFalse(compare_normalized_runs(one, two)["passed"])

    def test_comparison_diagnostics_are_bounded_but_streams_are_consumed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.db"
            create_database(baseline).close()
            left_stage, left_db = make_run(root, "left", 10)
            right_stage, right_db = make_run(root, "right", 20)
            for database, prefix in ((left_db, "left"), (right_db, "right")):
                connection = sqlite3.connect(database)
                connection.execute("CREATE TABLE extra(value TEXT)")
                connection.executemany(
                    "INSERT INTO extra VALUES (?)",
                    [(f"{prefix}-{index}-" + "x" * 1000,) for index in range(80)],
                )
                connection.commit()
                connection.close()
            one = normalize_completed_run(left_stage, baseline, response("a"), "2026-09-14 00:00:00")
            two = normalize_completed_run(right_stage, baseline, response("b"), "2026-09-14 00:00:00")
            comparison = compare_normalized_runs(one, two)
            self.assertFalse(comparison["passed"])
            self.assertLessEqual(len(comparison["differences"]), MAX_DIFFERENCES)
            self.assertTrue(comparison["differences_truncated"])
            self.assertEqual(comparison["run_1_record_count"], one.record_count)
            self.assertEqual(comparison["run_2_record_count"], two.record_count)


class DualRunLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.baseline_summary = {
            "status": "pass", "channels": [{
                "number": 2, "name": "Action",
                "channel_seed": 9,
                "regeneration_start": "2026-09-14 00:00:00",
                "effective_horizon": "2026-09-21 00:00:00",
                "retained_pre_seam_blocks": 0,
                "boundary_crossing": {"baseline_start": 0, "proposed_start": 0,
                                      "baseline_horizon": 0, "proposed_horizon": 0},
                "counts": {"baseline_blocks": 1, "proposed_blocks": 1,
                           "components": 1, "unchanged_blocks": 1,
                           "replaced_pairs": 0, "removed_blocks": 0,
                           "generated_blocks": 0, "reshaped_components": 0,
                           "reshaped_baseline_blocks": 0,
                           "reshaped_proposed_blocks": 0,
                           "title_changes": 0, "selected_media_changes": 0,
                           "playback_plan_changes": 0, "block_type_changes": 0,
                           "sequence_changes": 0, "break_changes": 0},
                "coverage": {side: {"covered_us": 604800000000,
                                    "gap_count": 0, "gap_us": 0,
                                    "overlap_count": 0, "overlap_us": 0,
                                    "interval_us": 604800000000,
                                    "finding_digest": "d" * 64}
                             for side in ("baseline", "proposed")},
                "samples": {"replaced": [], "reshaped": []},
                "samples_truncated": False, "baseline_findings": [], "errors": [],
                "digest": "b" * 64,
            }],
            "requested_configuration_effects": [],
            "resulting_schedule_changes": {
                "changed_channels": 0, "unchanged_blocks": 1,
                "replaced_pairs": 0, "removed_blocks": 0, "generated_blocks": 0,
                "reshaped_components": 0, "title_changes": 0,
                "selected_media_changes": 0, "playback_plan_changes": 0,
                "block_type_changes": 0, "sequence_changes": 0, "break_changes": 0,
            },
            "unexpected_differences": [], "digest": "c" * 64,
        }
        self.baseline_patch = patch(
            "station_director.dual_run.compare_baseline_to_proposed",
            return_value=self.baseline_summary,
        )
        self.baseline_patch.start()
        self.addCleanup(self.baseline_patch.stop)

    def _scope(self, root, events, *, cleanup_failure=False):
        context = {
            "input_fingerprint": "1" * 64,
            "requested_seed": 42,
            "effective_seed": 7,
            "reference_clock": "2026-09-14 00:00:00-07:00",
            "start_time": "2026-09-14 00:00:00",
            "end_time": "2026-09-21 00:00:00",
            "timezone": "America/Los_Angeles",
        }
        affected = [{"number": 2, "name": "Action"}]
        lifecycles = []
        for index in (1, 2):
            lifecycle = SimpleNamespace(
                stage=root / f"stage-{index}",
                run_id=f"comparison.run-{index}",
                request={"validation_context": context, "affected_channels": affected},
            )
            lifecycle.settle_unit = lambda index=index: events.append(f"settle-{index}")
            lifecycles.append(lifecycle)

        class Scope:
            capture = object()
            cleanup_results = []

            def cleanup_stages(self):
                self.cleanup_results = [
                    {"run": 2, "passed": not cleanup_failure,
                     "quarantined": cleanup_failure, "detail": "run 2"},
                    {"run": 1, "passed": True, "quarantined": False, "detail": "run 1"},
                ]
                events.append("cleanup")
                if cleanup_failure:
                    raise DualRunError("cleanup", "cleanup_failed", "cleanup failed")

            def close_capture(self):
                events.append("close-capture")

        scope = Scope()
        scope.lifecycles = lifecycles
        return scope

    def test_sequential_independent_success_and_reverse_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            scope = self._scope(root, events)
            normalized = [
                SimpleNamespace(digest="a" * 64, record_count=4, provisional_count=1),
                SimpleNamespace(digest="a" * 64, record_count=4, provisional_count=1),
            ]
            comparison = {
                "passed": True, "run_1_digest": "a" * 64,
                "run_2_digest": "a" * 64, "run_1_record_count": 4,
                "run_2_record_count": 4, "changed_records": 0,
                "added_records": 0, "removed_records": 0,
                "differences": [], "differences_truncated": False,
            }

            def launch(lifecycle, timeout):
                events.append(f"launch-{lifecycle.run_id[-1]}")

            def inspect(lifecycle):
                events.append(f"inspect-{lifecycle.run_id[-1]}")
                return response(lifecycle.run_id)

            def stable(unused_source, unused_media, unused_capture, checkpoint):
                events.append(checkpoint)
                return {"checkpoint": checkpoint, "passed": True, "changed_categories": []}

            with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                    patch("station_director.dual_run.launch_single_run", side_effect=launch), \
                    patch("station_director.dual_run.inspect_single_run", side_effect=inspect), \
                    patch("station_director.dual_run._assert_inputs_stable", side_effect=stable), \
                    patch("station_director.dual_run.normalize_completed_run", side_effect=normalized), \
                    patch("station_director.dual_run.compare_normalized_runs", return_value=comparison):
                result = run_dual_comparison(root, root, root, {
                    "week_start": "2026-09-14T00:00:00-07:00"
                }, {}, "comparison")
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["scheduler_invoked"], {"run_1": True, "run_2": True})
            self.assertLess(events.index("settle-1"), events.index("launch-2"))
            self.assertEqual(result["cleanup"], [
                {"run": 2, "passed": True, "quarantined": False, "detail": "run 2"},
                {"run": 1, "passed": True, "quarantined": False, "detail": "run 1"},
            ])

    def test_input_change_between_runs_is_not_nondeterminism(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            scope = self._scope(root, events)
            with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                    patch("station_director.dual_run.launch_single_run"), \
                    patch("station_director.dual_run.inspect_single_run", return_value=response("comparison.run-1")), \
                    patch("station_director.dual_run._assert_inputs_stable", side_effect=DualRunError(
                        "between_runs", "input_changed", "logical database changed",
                        category=["logical_database"],
                    )), \
                    patch("station_director.dual_run.normalize_completed_run"):
                result = run_dual_comparison(root, root, root, {
                    "week_start": "2026-09-14T00:00:00-07:00"
                }, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "input_changed")
            self.assertEqual(result["failure"]["category"], ["logical_database"])
            self.assertFalse(result["scheduler_invoked"]["run_2"])
            self.assertIsNone(result["reproducibility"])

    def test_first_and_second_run_failures_stop_comparison_and_cleanup(self):
        for failed_index in (1, 2):
            with self.subTest(failed_index=failed_index), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                scope = self._scope(root, events)
                outcomes = []
                if failed_index == 2:
                    outcomes.append(response("comparison.run-1"))
                outcomes.append(
                    DualRunError(f"run_{failed_index}", "worker_timeout", "timed out")
                )

                def inspect(unused_lifecycle):
                    outcome = outcomes.pop(0)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome

                with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                        patch("station_director.dual_run.launch_single_run"), \
                        patch("station_director.dual_run.inspect_single_run", side_effect=inspect), \
                        patch("station_director.dual_run._assert_inputs_stable", return_value={
                            "checkpoint": "between_runs", "passed": True,
                            "changed_categories": [],
                        }), patch("station_director.dual_run.normalize_completed_run", return_value=SimpleNamespace(
                            digest="a" * 64, record_count=1, provisional_count=0,
                        )), patch("station_director.dual_run.compare_normalized_runs") as comparison:
                    result = run_dual_comparison(root, root, root, {
                        "week_start": "2026-09-14T00:00:00-07:00"
                    }, {}, "comparison")
                self.assertEqual(result["failure"]["code"], "c1_run_failed")
                self.assertEqual(result["failure"]["category"], "worker_timeout")
                comparison.assert_not_called()
                self.assertIn("cleanup", events)

    def test_cleanup_failure_overrides_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scope = self._scope(root, [], cleanup_failure=True)
            normalized = [
                SimpleNamespace(digest="a" * 64, record_count=1, provisional_count=0),
                SimpleNamespace(digest="a" * 64, record_count=1, provisional_count=0),
            ]
            comparison = {
                "passed": True, "run_1_digest": "a" * 64,
                "run_2_digest": "a" * 64, "run_1_record_count": 1,
                "run_2_record_count": 1, "changed_records": 0,
                "added_records": 0, "removed_records": 0,
                "differences": [], "differences_truncated": False,
            }
            with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                    patch("station_director.dual_run.launch_single_run"), \
                    patch("station_director.dual_run.inspect_single_run", side_effect=[
                        response("comparison.run-1"), response("comparison.run-2")
                    ]), patch("station_director.dual_run._assert_inputs_stable", side_effect=lambda a, b, c, name: {
                        "checkpoint": name, "passed": True, "changed_categories": []
                    }), patch("station_director.dual_run.normalize_completed_run", side_effect=normalized), \
                    patch("station_director.dual_run.compare_normalized_runs", return_value=comparison):
                result = run_dual_comparison(root, root, root, {
                    "week_start": "2026-09-14T00:00:00-07:00"
                }, {}, "comparison")
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["failure"]["code"], "cleanup_failed")

    def test_guide_primary_and_cleanup_failures_remain_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scope = self._scope(root, [], cleanup_failure=True)
            with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                    patch("station_director.dual_run.launch_single_run"), \
                    patch("station_director.dual_run.inspect_single_run", side_effect=DualRunError(
                        "run_1", "c1_run_failed", "guide failed",
                        category="guide_validation_failed",
                    )), patch("station_director.dual_run._assert_inputs_stable", return_value={
                        "checkpoint": "before_success", "passed": True,
                        "changed_categories": [],
                    }):
                result = run_dual_comparison(root, root, root, {
                    "week_start": "2026-09-14T00:00:00-07:00"
                }, {}, "comparison")
            self.assertEqual(result["failure"]["code"], "c1_run_failed")
            self.assertEqual(result["failure"]["category"], "guide_validation_failed")
            self.assertTrue(any(not item["passed"] for item in result["cleanup"]))

    def test_input_mutation_during_cleanup_prevents_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            scope = self._scope(root, events)
            normalized = [
                SimpleNamespace(digest="a" * 64, record_count=1, provisional_count=0),
                SimpleNamespace(digest="a" * 64, record_count=1, provisional_count=0),
            ]
            comparison = {
                "passed": True, "run_1_digest": "a" * 64,
                "run_2_digest": "a" * 64, "run_1_record_count": 1,
                "run_2_record_count": 1, "changed_records": 0,
                "added_records": 0, "removed_records": 0,
                "differences": [], "differences_truncated": False,
            }

            def stable(unused_source, unused_media, unused_capture, checkpoint):
                if checkpoint == "before_success":
                    self.assertIn("cleanup", events)
                    raise DualRunError(
                        checkpoint, "input_changed", "media changed during cleanup",
                        category=["logical_media"],
                    )
                return {"checkpoint": checkpoint, "passed": True, "changed_categories": []}

            with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                    patch("station_director.dual_run.launch_single_run"), \
                    patch("station_director.dual_run.inspect_single_run", side_effect=[
                        response("comparison.run-1"), response("comparison.run-2")
                    ]), patch("station_director.dual_run._assert_inputs_stable", side_effect=stable), \
                    patch("station_director.dual_run.normalize_completed_run", side_effect=normalized), \
                    patch("station_director.dual_run.compare_normalized_runs", return_value=comparison):
                result = run_dual_comparison(
                    root, root, root,
                    {"week_start": "2026-09-14T00:00:00-07:00"}, {}, "comparison",
                )
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["failure"]["code"], "input_changed")
            self.assertEqual(result["failure"]["category"], ["logical_media"])
            self.assertFalse(result["source_checks"][-1]["passed"])

    def test_failure_codes_distinguish_normalization_comparison_and_mismatch(self):
        cases = (
            ("normalization", NormalizationError("bad"), None, "normalization_failed"),
            ("comparison", None, RuntimeError("bad"), "comparison_failed"),
            ("mismatch", None, None, "reproducibility_mismatch"),
        )
        for label, normalization_error, comparison_error, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                scope = self._scope(root, [])
                normalized = SimpleNamespace(
                    digest="a" * 64, record_count=1, provisional_count=0
                )
                comparison = {
                    "passed": label != "mismatch", "run_1_digest": "a" * 64,
                    "run_2_digest": "a" * 64, "run_1_record_count": 1,
                    "run_2_record_count": 1, "changed_records": int(label == "mismatch"),
                    "added_records": 0, "removed_records": 0,
                    "differences": [], "differences_truncated": False,
                }
                normalize_effect = normalization_error or [normalized, normalized]
                compare_effect = comparison_error or comparison
                with patch("station_director.dual_run._prepare_scope", return_value=scope), \
                        patch("station_director.dual_run.launch_single_run"), \
                        patch("station_director.dual_run.inspect_single_run", side_effect=[
                            response("comparison.run-1"), response("comparison.run-2")
                        ]), patch("station_director.dual_run._assert_inputs_stable", side_effect=lambda a, b, c, name: {
                            "checkpoint": name, "passed": True, "changed_categories": []
                        }), patch(
                            "station_director.dual_run.normalize_completed_run",
                            side_effect=normalize_effect,
                        ), patch(
                            "station_director.dual_run.compare_normalized_runs",
                            side_effect=compare_effect if isinstance(compare_effect, Exception) else None,
                            return_value=None if isinstance(compare_effect, Exception) else compare_effect,
                        ):
                    result = run_dual_comparison(
                        root, root, root,
                        {"week_start": "2026-09-14T00:00:00-07:00"}, {}, "comparison",
                    )
                self.assertEqual(result["failure"]["code"], expected)

    def test_cancellation_across_dual_run_phases_preserves_cleanup(self):
        class Cancelled(Exception):
            is_validation_cancellation = True

        cases = ("run_1", "between_runs", "run_2", "normalization",
                 "comparison", "baseline_comparison", "before_success")
        for target in cases:
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                scope = self._scope(root, events)
                launches = [None, None]
                if target == "run_1": launches[0] = Cancelled()
                if target == "run_2": launches[1] = Cancelled()

                def launch(unused_lifecycle, timeout):
                    outcome = launches.pop(0)
                    if outcome is not None:
                        raise outcome

                stability_calls = []
                def stable(unused_source, unused_media, unused_capture, checkpoint):
                    stability_calls.append(checkpoint)
                    if checkpoint == target:
                        raise Cancelled()
                    return {"checkpoint": checkpoint, "passed": True,
                            "changed_categories": []}

                normal = SimpleNamespace(digest="a" * 64, record_count=1,
                                         provisional_count=0)
                normalize_effect = Cancelled() if target == "normalization" else [normal, normal]
                comparison = {
                    "passed": True, "run_1_digest": "a" * 64,
                    "run_2_digest": "a" * 64, "run_1_record_count": 1,
                    "run_2_record_count": 1, "changed_records": 0,
                    "added_records": 0, "removed_records": 0,
                    "differences": [], "differences_truncated": False,
                }
                compare_effect = Cancelled() if target == "comparison" else None
                baseline_effect = Cancelled() if target == "baseline_comparison" else self.baseline_summary
                with patch("station_director.dual_run._prepare_scope", return_value=scope),                         patch("station_director.dual_run.launch_single_run", side_effect=launch),                         patch("station_director.dual_run.inspect_single_run", side_effect=[
                            response("comparison.run-1"), response("comparison.run-2")]),                         patch("station_director.dual_run._assert_inputs_stable", side_effect=stable),                         patch("station_director.dual_run.normalize_completed_run",
                              side_effect=normalize_effect),                         patch("station_director.dual_run.compare_normalized_runs",
                              side_effect=compare_effect, return_value=comparison),                         patch("station_director.dual_run.compare_baseline_to_proposed",
                              side_effect=baseline_effect if isinstance(baseline_effect, Exception) else None,
                              return_value=None if isinstance(baseline_effect, Exception) else baseline_effect):
                    result = run_dual_comparison(
                        root, root, root,
                        {"week_start": "2026-09-14T00:00:00-07:00"}, {}, "comparison",
                    )
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["failure"]["code"], "validation_interrupted")
                self.assertIn("cleanup", events)

    def test_cancellation_during_stage_cleanup_remains_interrupted(self):
        class Cancelled(Exception):
            is_validation_cancellation = True
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            scope = self._scope(root, events)
            def cleanup():
                scope.cleanup_results = [
                    {"run": 2, "passed": False, "quarantined": True, "detail": "interrupted"},
                    {"run": 1, "passed": True, "quarantined": False, "detail": "clean"},
                ]
                events.append("cleanup")
                raise Cancelled()
            scope.cleanup_stages = cleanup
            normal = SimpleNamespace(digest="a" * 64, record_count=1, provisional_count=0)
            comparison = {
                "passed": True, "run_1_digest": "a" * 64, "run_2_digest": "a" * 64,
                "run_1_record_count": 1, "run_2_record_count": 1,
                "changed_records": 0, "added_records": 0, "removed_records": 0,
                "differences": [], "differences_truncated": False,
            }
            with patch("station_director.dual_run._prepare_scope", return_value=scope),                     patch("station_director.dual_run.launch_single_run"),                     patch("station_director.dual_run.inspect_single_run", side_effect=[
                        response("comparison.run-1"), response("comparison.run-2")]),                     patch("station_director.dual_run._assert_inputs_stable", side_effect=lambda a,b,c,name: {
                        "checkpoint": name, "passed": True, "changed_categories": []}),                     patch("station_director.dual_run.normalize_completed_run", side_effect=[normal, normal]),                     patch("station_director.dual_run.compare_normalized_runs", return_value=comparison):
                result = run_dual_comparison(
                    root, root, root,
                    {"week_start": "2026-09-14T00:00:00-07:00"}, {}, "comparison",
                )
            self.assertEqual(result["failure"]["code"], "validation_interrupted")
            self.assertTrue(result["cleanup"][0]["quarantined"])

    def test_semantic_validator_rejects_contradictory_success(self):
        result = {
            "status": "success", "phase_reached": "complete", "failure": None,
            "scheduler_invoked": {"run_1": True, "run_2": False},
            "runs": [], "source_checks": [], "reproducibility": None,
            "cleanup": [],
        }
        with self.assertRaisesRegex(DualRunError, "both schedulers"):
            _validate_result_semantics(result)

    def test_public_modules_do_not_import_or_reference_c2(self):
        for relative in (
            "station_director/cli.py",
            "station_director/validation.py",
            "station_director/stage_runner.py",
            "station_director/__init__.py",
        ):
            path = ROOT / relative
            if not path.exists():
                continue
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("dual_run", source, relative)
            self.assertNotIn("schedule_normalization", source, relative)
        self.assertIn(
            'PHASE_3_DISABLED = "Phase 3 validation is not yet enabled"',
            (ROOT / "station_director/validation.py").read_text(encoding="utf-8"),
        )
        completed = subprocess.run(
            [sys.executable, "-c", (
                "import sys; import station_director.dual_run; "
                "assert not any(n == 'fs42' or n.startswith('fs42.') for n in sys.modules)"
            )],
            cwd=ROOT, check=False, capture_output=True, text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        from station_director.validation import _disabled_report

        public = _disabled_report({"proposal_id": "synthetic"})
        self.assertFalse(public["scheduler_invoked"])
        self.assertEqual(
            public["failures"][0], "Phase 3 validation is not yet enabled"
        )


class NativeTwoProcessIntegrationTests(unittest.TestCase):
    def _complete_dual_run(self, root, *, alter_second=False, alter_guide_second=False, execute=None):
        media = root / "media"
        content = media / "synthetic" / "Synthetic"
        content.mkdir(parents=True)
        for target, color in ((content / "clip.mp4", "black"), (content / "other.mp4", "white")):
            subprocess.run(
                [
                    "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
                    f"color=c={color}:s=16x16:r=1", "-t", "3600",
                    "-pix_fmt", "yuv420p", str(target),
                ], check=True, timeout=30,
            )
        project = synthetic_project(root, media)
        proposal = base_proposal()
        proposal["directives"] = [{
            "type": "theme", "name": "synthetic", "channel": 2,
            "start_date": "2026-09-14", "end_date": "2026-09-14",
            "hours": [0], "series": "Synthetic",
        }]
        policy = load_policy()
        staging = root / "staging"
        staging.mkdir()
        details = []

        from station_director import path_safety as parent_path_safety
        parent_canonical = parent_path_safety.canonical_media_mapping

        def normalize_media_mapping(value, *args, **kwargs):
            try:
                relative = Path(str(value)).relative_to(media)
            except ValueError:
                pass
            else:
                value = str(Path("/media") / relative)
            return parent_canonical(value, *args, **kwargs)

        def transport(lifecycle, timeout):
            environment = dict(os.environ)
            environment.update(TZ="America/Los_Angeles", PYTHONHASHSEED="0")
            completed = subprocess.run(
                [
                    str(ROOT / "env/bin/python"), "-c",
                    "import sys; from test.test_station_director_milestone_c2 import complete_worker_child_main; complete_worker_child_main(*sys.argv[1:])",
                    str(lifecycle.stage), str(media), str(project),
                ],
                cwd=ROOT, env=environment, timeout=timeout,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            info = json.loads((lifecycle.stage / "native-info.json").read_text(encoding="utf-8"))
            details.append(info)
            if alter_second and lifecycle.run_id.endswith("run-2") and info["status"] == "success":
                database = lifecycle.stage / "work/runtime/fs42_fluid.db"
                connection = sqlite3.connect(database)
                try:
                    row = connection.execute(
                        "SELECT id,content_json,plan_json FROM liquid_blocks ORDER BY start_time LIMIT 1"
                    ).fetchone()
                    catalog_id = int(json.loads(row[1]))
                    replacement = str(media / "synthetic/Synthetic/other.mp4")
                    connection.execute(
                        "UPDATE catalog_entries SET path=?,realpath=? WHERE id=?",
                        (replacement, replacement, catalog_id),
                    )
                    plan = json.loads(row[2])
                    plan[0]["path"] = replacement
                    connection.execute(
                        "UPDATE liquid_blocks SET plan_json=? WHERE id=?",
                        (json.dumps(plan), row[0]),
                    )
                    connection.commit()
                finally:
                    connection.close()
            if alter_guide_second and lifecycle.run_id.endswith("run-2") and info["status"] == "success":
                guide_path = lifecycle.stage / "guide/guide-v1.records"
                raw = guide_path.read_bytes()
                position = len(GUIDE_MAGIC)
                records = []
                changed = False
                while position < len(raw):
                    size = int.from_bytes(raw[position:position + 8], "big")
                    position += 8
                    item = json.loads(raw[position:position + size])
                    position += size
                    if not changed and item["path"].startswith("/guide/schedules/"):
                        item["value"]["title"] += " Different"
                        changed = True
                    records.append(item)
                self.assertTrue(changed)
                rebuilt = GUIDE_MAGIC
                for item in records:
                    encoded = json.dumps(
                        item, ensure_ascii=False, allow_nan=False, sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    rebuilt += len(encoded).to_bytes(8, "big") + encoded
                guide_path.write_bytes(rebuilt)
                guide_path.chmod(0o600)
                response_path = lifecycle.stage / "native-single-run.response.json"
                response_value = json.loads(response_path.read_text(encoding="utf-8"))
                response_value["guide_validation"]["digest"] = hashlib.sha256(rebuilt).hexdigest()
                response_value["guide_validation"]["byte_count"] = len(rebuilt)
                response_path.write_text(
                    json.dumps(response_value, sort_keys=True, separators=(",", ":")) + "\n",
                    encoding="utf-8",
                )
                response_path.chmod(0o600)
            lifecycle.launcher_result = LaunchResult(
                lifecycle.unit_name, 0 if info["status"] == "success" else 1,
                completed.stdout, completed.stderr,
            )
            return lifecycle.launcher_result

        # These patches model only the unavailable isolation transport: the
        # mount alias, probe attestation, transient-unit launch, and unit cleanup.
        with patch("station_director.dual_run.check_invocation_context", return_value=(True, "test SSH")), \
                patch("station_director.isolation.STAGING_PARENT", staging), \
                patch("station_director.dual_run.STAGING_PARENT", staging), \
                patch("station_director.dual_run.launch_single_run", side_effect=transport), \
                patch("station_director.schedule_normalization.canonical_media_mapping", side_effect=normalize_media_mapping), \
                patch("station_director.single_run.cleanup_unit", return_value=(True, "test unit absent")):
            result = (run_dual_comparison(
                project, project, media, proposal, policy, "native-integration"
            ) if execute is None else execute(project, media, proposal, policy))
        return result, details

    def test_complete_genuine_two_run_lifecycle_succeeds_identically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, details = self._complete_dual_run(root)
            self.assertEqual(result["status"], "success", (result.get("failure"), details))
            self.assertTrue(result["reproducibility"]["passed"])
            self.assertEqual(len(details), 2)
            self.assertNotEqual(details[0]["pid"], details[1]["pid"])
            for field in ("stage", "database", "request", "response"):
                self.assertNotEqual(details[0][field], details[1][field])
            self.assertTrue(all(item["fs42_loaded"] for item in details))
            self.assertEqual([item["cache_size"] for item in details], [1, 1])
            self.assertTrue(all(item["guide_validation"]["status"] == "pass" for item in details))

    def test_complete_genuine_lifecycle_detects_selected_media_difference(self):
        with tempfile.TemporaryDirectory() as directory:
            result, details = self._complete_dual_run(Path(directory), alter_second=True)
            self.assertEqual(len(details), 2)
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["failure"]["code"], "reproducibility_mismatch")
            self.assertFalse(result["reproducibility"]["passed"])

    def test_complete_genuine_lifecycle_detects_guide_only_difference(self):
        with tempfile.TemporaryDirectory() as directory:
            result, details = self._complete_dual_run(
                Path(directory), alter_guide_second=True
            )
            self.assertEqual(len(details), 2)
            self.assertEqual(result["status"], "failed")
            self.assertEqual(
                result["failure"]["code"], "guide_reproducibility_mismatch"
            )
            self.assertFalse(result["reproducibility"]["passed"])


if __name__ == "__main__":
    unittest.main()
