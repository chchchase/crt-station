import ast
import io
import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from station_director.isolation_probe import PROBE_RESULTS, PROBE_SCHEMA_VERSION
from station_director.policy import load_policy
from station_director.preservation import (
    capture_media_manifest,
    fingerprint_database,
    fingerprint_json_files,
    protected_json_paths,
)
from station_director.single_run_protocol import (
    HeldDocument,
    MAX_DOCUMENT_BYTES,
    ProtocolError,
    REQUEST_SCHEMA,
    RESPONSE_SCHEMA_V2 as RESPONSE_SCHEMA,
    RESPONSE_SCHEMA_V1,
    bind_request,
    strict_json_loads,
    write_private_json_exclusive,
)
from station_director.single_run_worker import run_worker, _base_response
from station_director.c1_diagnostics import (
    DIAGNOSTIC_RULES, FINGERPRINT_CATEGORIES, PROBE_IDENTIFIERS,
    make_diagnostic, validate_diagnostic,
)
from station_director.single_run import (
    FINALIZATION_SUBPHASES,
    SingleRunError,
    SingleRunFinalizationError,
    SingleRunLifecycle,
    _finalization_step,
    _finalize_prepared_single_run,
    inspect_single_run,
    launch_single_run,
    prepare_single_run,
)
from station_director.isolation import LaunchResult
from station_director.isolation import (
    build_bwrap_command, prepare_stage_temporary, IsolationError,
    IsolationLauncher, _run_bounded, MAX_CAPTURE_BYTES,
)
from station_director.isolation_probe import check_stage_backed_tmp
from station_director.cli import build_parser
from station_director.validation_context import (
    canonical_seed_inputs,
    derive_channel_seed,
    derive_validation_context,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
)
from station_director.worker_bootstrap import HeldSchedulingInputs, VerifiedWorkerAttestation, BootstrapError
from test.test_station_director_schedule import base_proposal
from test.test_station_director_milestone_b2 import (
    catalog_row,
    create_database,
    insert_block,
)


ROOT = Path(__file__).parents[1]


def test_attestation(request):
    attestation = object.__new__(VerifiedWorkerAttestation)
    attestation.run_id = request.get("run_id")
    attestation.request_digest = request.get("request_digest")
    attestation.snapshot = {}
    attestation.inputs = types.SimpleNamespace(assert_ready=lambda: None, close=lambda: None)
    attestation._active = True
    return attestation


def probes(run_id, passed=True):
    results = {
        name: {"passed": passed, "detail": name} for name in PROBE_RESULTS
    }
    return {
        "schema_version": PROBE_SCHEMA_VERSION,
        "run_id": run_id,
        "overall_pass": passed,
        "results": results,
    }


def passing_verification():
    return {
        "original_snapshot": "pass", "deterministic_projection": "pass",
        "projected_configuration": "pass", "working_database": "pass",
        "logical_media": "pass", "physical_transition": "pass",
        "fingerprints": {f"fingerprint_{index}": "0" * 64 for index in range(7)},
    }


def passing_channel():
    return {
        "name": "Action", "number": 2, "channel_seed": 1,
        "regeneration_start": "2026-09-14 06:00:00",
        "effective_horizon": "2026-09-21 06:00:00",
        "retained_blocks": 0, "generated_blocks": 1, "final_blocks": 1,
        "catalog_reused": 0, "catalog_new": 1, "catalog_protected": 0,
        "new_catalog_ids": "provisional",
        "coverage": {"channel": "Action", "proposal_boundary_crossing_ids": [],
                     "proposal_end_crossing_ids": [], "effective_horizon_crossing_ids": [1],
                     "gaps": [], "overlaps": [], "final_end": "2026-09-21 06:00:00"},
    }


def passing_guide():
    return {
        "status": "pass", "format_version": 1,
        "primary_failure": None,
        "snapshot_preparation": {"status": "pass", "message": None},
        "snapshot_verification": {"status": "pass", "message": None},
        "post_read_verification": {"status": "pass", "message": None},
        "artifact_identity": "guide/guide-v1.records", "digest": "8" * 64,
        "record_count": 1, "byte_count": 32,
        "channels": [{
            "number": 2, "name": "Action", "network_long_name": "",
            "hidden": False, "has_schedule": True, "listing_count": 1,
            "transition_probe_count": 1, "zero_match_count": 0,
            "one_match_count": 1, "named_boundaries": [],
            "named_boundaries_truncated": False,
        }],
        "errors": [], "errors_truncated": False,
    }
def make_snapshot(stage, media):
    source = stage / "source"
    (source / "confs").mkdir(parents=True)
    (source / "runtime").mkdir()
    (source / "confs/main_config.json").write_text("{}\n")
    (source / "confs/action.json").write_text(
        json.dumps({"station_conf": {"network_name": "Action"}}) + "\n"
    )
    (source / "runtime/watch_in_order_state.json").write_text("{}\n")
    sqlite3.connect(source / "runtime/fs42_fluid.db").close()
    for path in [*source.rglob("*.json"), source / "runtime/fs42_fluid.db"]:
        os.chmod(path, 0o600)
    (media / "show.mp4").write_bytes(b"synthetic")
    config = logical_protected_configuration_fingerprint(protected_json_paths(source))
    database = fingerprint_database(source / "runtime/fs42_fluid.db")["logical"]
    manifest = capture_media_manifest(media, spool_directory=stage)
    try:
        media_fingerprint = logical_media_manifest_fingerprint(manifest)
    finally:
        manifest.close()
    return canonical_seed_inputs(
        config["digest"], database["digest"], media_fingerprint["digest"]
    )


def request_for(stage, media):
    proposal = base_proposal()
    proposal["directives"] = [{
        "type": "theme", "name": "synthetic", "channel": 2,
        "start_date": "2026-09-14", "end_date": "2026-09-14",
        "series": "Synthetic", "hours": [6],
    }]
    policy = load_policy()
    seed_inputs = make_snapshot(stage, media)
    source = stage / "source"
    physical = fingerprint_json_files(protected_json_paths(source))["digest"]
    affected = [{"number": 2, "name": "Action"}]
    return bind_request(
        {
            "schema_version": 1,
            "operation": "native_single_run",
            "run_id": "c1-test-run",
            "proposal": proposal,
            "policy": policy,
            "seed_inputs": seed_inputs,
            "input_fingerprints": {
                "original_logical_configuration_fingerprint": seed_inputs["logical_protected_configuration_fingerprint"],
                "original_logical_database_fingerprint": seed_inputs["logical_database_fingerprint"],
                "logical_media_manifest_fingerprint": seed_inputs["logical_media_manifest_fingerprint"],
                "live_physical_configuration_fingerprint": physical,
                "staged_source_physical_configuration_fingerprint": physical,
            },
            "affected_channels": affected,
            "validation_context": derive_validation_context(
                proposal, policy, seed_inputs
            ),
        }
    )


class ChannelSeedTests(unittest.TestCase):
    def test_channel_seed_is_reproducible_domain_separated_and_sensitive(self):
        arguments = (123, 2, "Action", "2026-09-14 00:00:00", "2026-09-21 00:00:00")
        first = derive_channel_seed(*arguments)
        self.assertEqual(first, derive_channel_seed(*arguments))
        variants = [
            (124, *arguments[1:]),
            (123, 3, "After School", *arguments[3:]),
            (123, 2, "Action!", *arguments[3:]),
            (*arguments[:3], "2026-09-14 01:00:00", arguments[4]),
            (*arguments[:4], "2026-09-22 00:00:00"),
        ]
        self.assertTrue(all(derive_channel_seed(*item) != first for item in variants))
        second = derive_channel_seed(123, 3, "After School", arguments[3], arguments[4])
        self.assertNotEqual(
            [random.Random(first).randrange(10**9) for unused in range(3)],
            [random.Random(second).randrange(10**9) for unused in range(3)],
        )


class ProtocolTests(unittest.TestCase):
    def test_protocol_json_rejects_duplicate_keys_at_every_depth(self):
        samples = (
            '{"status":"success","status":"failed"}',
            '{"proposal":{"proposal_id":"one","proposal_id":"two"}}',
            '{"validation_context":{"effective_seed":1,"effective_seed":2}}',
            '{"channels":[{"number":2,"number":3}]}',
            '{"diagnostics":{"detail":{"code":"a","code":"b"}}}',
            '{"policy":{"nested":{"value":1,"value":2}}}',
        )
        for raw in samples:
            with self.subTest(raw=raw), self.assertRaisesRegex(
                ProtocolError, "duplicate key"
            ):
                strict_json_loads(raw)

    def test_held_request_and_response_reject_duplicate_keys_before_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, schema, raw in (
                ("request.json", REQUEST_SCHEMA,
                 '{"schema_version":1,"schema_version":1}'),
                ("response.json", RESPONSE_SCHEMA,
                 '{"status":"success","status":"failed"}'),
            ):
                path = root / name
                path.write_text(raw, encoding="utf-8")
                os.chmod(path, 0o600)
                with self.subTest(name=name), self.assertRaisesRegex(
                    ProtocolError, "duplicate key"
                ):
                    HeldDocument(path, schema)

    def test_nested_proposal_and_policy_are_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            media = stage / "media"
            media.mkdir()
            request = request_for(stage, media)
            changed = json.loads(json.dumps(request))
            changed["proposal"]["unexpected"] = True
            with self.assertRaises(ProtocolError):
                bind_request(changed)
            changed = json.loads(json.dumps(request))
            changed["policy"]["channels"][0]["unexpected"] = True
            with self.assertRaises(ProtocolError):
                bind_request(changed)

    def test_request_digest_binds_protocol_identity_and_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            original = request["request_digest"]
            for path, value in (
                (("run_id",), "other-run"),
                (("schema_version",), 2),
                (("proposal", "source"), "recommendation"),
                (("policy", "inventory_excluded_roots"), ["different"]),
                (("seed_inputs", "logical_media_manifest_fingerprint"), "f" * 64),
            ):
                changed = json.loads(json.dumps(request))
                target = changed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                from station_director.single_run_protocol import request_digest
                self.assertNotEqual(request_digest(changed), original)

    def test_private_file_rejects_permissions_symlinks_and_hardlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            path = root / "request.json"
            write_private_json_exclusive(path, request, REQUEST_SCHEMA)
            with HeldDocument(path, REQUEST_SCHEMA) as document:
                self.assertEqual(document.payload["run_id"], "c1-test-run")
            os.chmod(path, 0o644)
            with self.assertRaisesRegex(ProtocolError, "0600"):
                HeldDocument(path, REQUEST_SCHEMA)
            os.chmod(path, 0o600)
            link = root / "link.json"
            os.link(path, link)
            with self.assertRaisesRegex(ProtocolError, "hard-link"):
                HeldDocument(path, REQUEST_SCHEMA)
            link.unlink()
            symlink = root / "symlink.json"
            symlink.symlink_to(path)
            with self.assertRaises(ProtocolError):
                HeldDocument(symlink, REQUEST_SCHEMA)

    def test_duplicate_output_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            path = root / "output.json"
            path.write_text("occupied")
            with self.assertRaisesRegex(ProtocolError, "duplicate"):
                write_private_json_exclusive(path, request, REQUEST_SCHEMA)

    def test_bounded_read_rejects_oversized_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "huge.json"
            path.write_bytes(b"x" * (MAX_DOCUMENT_BYTES + 1))
            os.chmod(path, 0o600)
            with self.assertRaisesRegex(ProtocolError, "2 MiB"):
                HeldDocument(path, REQUEST_SCHEMA)


class WorkerBoundaryTests(unittest.TestCase):
    def _write_request(self, root, request):
        path = root / "request.json"
        write_private_json_exclusive(path, request, REQUEST_SCHEMA)
        return path

    def test_verified_native_input_replacement_between_checks_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            database = root / "db.sqlite"
            config.write_text("{}")
            database.write_bytes(b"db")
            held = HeldSchedulingInputs([config, database], database)
            try:
                held.assert_ready()
                replacement = root / "replacement.json"
                replacement.write_text("{}")
                os.replace(replacement, config)
                with self.assertRaisesRegex(BootstrapError, "replaced"):
                    held.assert_ready()
            finally:
                held.close()

    def test_snapshot_mismatch_prevents_first_native_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            request_path = self._write_request(root, request)
            (root / "source/confs/action.json").write_text('{"changed":true}\n')
            os.chmod(root / "source/confs/action.json", 0o600)
            output = root / "response.json"
            from station_director.worker_bootstrap import BootstrapError
            with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
                "station_director.single_run_worker.attest_before_native_import", side_effect=BootstrapError("snapshot changed")
            ), patch("station_director.single_run_worker.importlib.import_module") as loader:
                response = run_worker(request_path, output)
            self.assertNotIn(unittest.mock.call("station_director.native_single_run"), loader.mock_calls)
            self.assertEqual(response["failure"]["code"], "projected_configuration_verification_failed")
            self.assertEqual(response["phase_reached"], "snapshot")
            self.assertFalse(response["scheduler_invoked"])

    def test_database_and_media_mismatches_prevent_native_import(self):
        for kind in ("database", "media"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                media = root / "media"
                media.mkdir()
                request = request_for(root, media)
                if kind == "database":
                    connection = sqlite3.connect(root / "source/runtime/fs42_fluid.db")
                    connection.execute("CREATE TABLE changed(value TEXT)")
                    connection.commit()
                    connection.close()
                else:
                    (media / "added.mp4").write_bytes(b"new")
                request_path = self._write_request(root, request)
                from station_director.worker_bootstrap import BootstrapError
                with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
                    "station_director.single_run_worker.attest_before_native_import", side_effect=BootstrapError("snapshot changed")
                ), patch("station_director.single_run_worker.importlib.import_module") as loader:
                    response = run_worker(request_path, root / "response.json")
                self.assertNotIn(unittest.mock.call("station_director.native_single_run"), loader.mock_calls)
                self.assertEqual(response["phase_reached"], "snapshot")

    def test_seed_mismatch_prevents_first_native_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            request["validation_context"]["effective_seed"] += 1
            request = bind_request(request)
            request_path = self._write_request(root, request)
            from station_director.worker_bootstrap import BootstrapError
            with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
                "station_director.single_run_worker.attest_before_native_import", side_effect=BootstrapError("seed mismatch", phase="seed", code="effective_seed_verification_failed")
            ), patch("station_director.single_run_worker.importlib.import_module") as loader:
                response = run_worker(request_path, root / "response.json")
            self.assertNotIn(unittest.mock.call("station_director.native_single_run"), loader.mock_calls)
            self.assertEqual(response["phase_reached"], "seed")

    def test_success_imports_native_only_after_attestation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            request_path = self._write_request(root, request)
            output = root / "response.json"
            fake = types.SimpleNamespace(
                execute_native_single_run=lambda *unused: {
                    "scheduler_invoked": True,
                    "channels": [passing_channel()],
                    "verification": passing_verification(),
                    "preservation": {
                        "retained_history": "pass", "protected_channels": "pass",
                        "sequence_tables_restored": "pass", "foreign_key_baseline": "pass",
                    },
                    "path_validation": {
                        "passed": True, "mapping_count": 0, "scheduled_path_checks": 0,
                    },
                    "guide_validation": passing_guide(),
                    "timings_ms": {
                        "prepare": 0, "catalog": 0, "scheduler": 1,
                        "preservation": 0, "guide": 0, "total": 1,
                    },
                }
            )
            attestation = test_attestation(request)
            with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
                "station_director.single_run_worker.attest_before_native_import",
                return_value=(probes(request["run_id"]), attestation),
            ), patch("station_director.single_run_worker.importlib.import_module", return_value=fake) as loader:
                response = run_worker(request_path, output)
            loader.assert_called_once_with("station_director.native_single_run")
            self.assertEqual(response["status"], "success")
            self.assertTrue(response["scheduler_invoked"])

    def test_failed_probe_cannot_import_native(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            request_path = self._write_request(root, request)
            from station_director.worker_bootstrap import BootstrapError
            with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
            "station_director.single_run_worker.importlib.import_module"
            ) as loader, patch("station_director.single_run_worker.attest_before_native_import", side_effect=BootstrapError("probe failed", phase="probes", code="isolation_probe_failed", probe="environment_sanitized")):
                response = run_worker(request_path, root / "response.json")
            self.assertNotIn(unittest.mock.call("station_director.native_single_run"), loader.mock_calls)
            self.assertEqual(response["phase_reached"], "probes")

    def test_native_failure_reports_actual_scheduler_entry_entry(self):
        class Failure(RuntimeError):
            phase = "scheduler"
            channel = "Action"
            code = "scheduler_failure"
            scheduler_invoked = True

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            request_path = self._write_request(root, request)
            fake = types.SimpleNamespace(
                execute_native_single_run=lambda *unused: (_ for _ in ()).throw(Failure("boom"))
            )
            with patch.dict(os.environ, {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"}), patch(
                "station_director.single_run_worker.attest_before_native_import",
                return_value=(probes(request["run_id"]), test_attestation(request)),
            ), patch("station_director.single_run_worker.importlib.import_module", return_value=fake):
                response = run_worker(request_path, root / "response.json")
            self.assertEqual(response["failure"]["phase"], "scheduler")
            self.assertTrue(response["scheduler_invoked"])

    def test_public_modules_do_not_reference_internal_engine(self):
        for relative in (
            "station_director/cli.py",
            "station_director/stage_runner.py",
            "station_director/validation.py",
            "station_director/__main__.py",
        ):
            source = (ROOT / relative).read_text()
            self.assertNotIn("single_run_worker", source)
            self.assertNotIn("native_single_run", source)
        stage_source = (ROOT / "station_director/stage_runner.py").read_text()
        self.assertIn('DISABLED_MESSAGE = "Phase 3 validation is not yet enabled"', stage_source)

    def test_host_modules_have_no_native_imports(self):
        for relative in (
            "station_director/single_run.py",
            "station_director/single_run_protocol.py",
            "station_director/worker_bootstrap.py",
            "station_director/validation.py",
            "station_director/stage_runner.py",
        ):
            tree = ast.parse((ROOT / relative).read_text())
            imports = [
                node for node in ast.walk(tree)
                if isinstance(node, (ast.Import, ast.ImportFrom))
                and (
                    (isinstance(node, ast.ImportFrom) and (node.module or "").startswith("fs42"))
                    or (isinstance(node, ast.Import) and any(alias.name.startswith("fs42") for alias in node.names))
                )
            ]
            self.assertEqual(imports, [], relative)


class NativePolicyTests(unittest.TestCase):
    def test_autobump_is_detected_structurally_before_scheduling(self):
        from station_director.native_single_run import _autobump_fields

        self.assertEqual(
            _autobump_fields({"off_air_autobump": {"title": "x"}}),
            ["station_conf.off_air_autobump"],
        )
        self.assertEqual(_autobump_fields({"other": {"title": "x"}}), [])

    def test_main_config_allowlist_excludes_secrets_and_runtime_controls(self):
        from station_director.worker_bootstrap import (
            SCHEDULING_MAIN_CONFIG_KEYS, scheduling_main_config,
        )

        forbidden = {
            "tmdb_api_key", "parental_controls_pin", "server_host", "server_port",
            "channel_socket", "status_socket", "start_mpv", "schedule_agent",
        }
        self.assertFalse(SCHEDULING_MAIN_CONFIG_KEYS & forbidden)
        staged = scheduling_main_config({
            "tmdb_api_key": "secret", "server_host": "secret-host",
            "normalize_titles": False,
        })
        self.assertEqual(staged["normalize_titles"], False)
        self.assertEqual(set(staged), {"normalize_titles", "db_path"})
        self.assertNotIn("secret", json.dumps(staged))
        self.assertNotIn("day_parts", staged)

        native_shape = {
            "morning": {"start_hour": 6, "end_hour": 10},
        }
        staged = scheduling_main_config({"day_parts": native_shape})
        self.assertEqual(staged["day_parts"], native_shape)

    def test_catalog_invalid_entry_is_fatal_only_in_validation_mode(self):
        import datetime

        from fs42.catalog import ShowCatalog
        from fs42.scheduling_context import (
            ValidationSchedulingContext,
            activate_validation_context,
        )

        catalog = ShowCatalog.__new__(ShowCatalog)
        catalog.config = {"network_name": "Action"}
        catalog.clip_index = {"tag": [object()]}
        with patch("fs42.catalog.CatalogAPI.set_entries") as writer, redirect_stdout(io.StringIO()):
            catalog._write_catalog()
            writer.assert_called_once_with(catalog.config, [])
        context = ValidationSchedulingContext(
            datetime.datetime(2026, 9, 14),
            datetime.datetime(2026, 9, 14),
            datetime.datetime(2026, 9, 15),
            1,
        )
        with activate_validation_context(context), self.assertRaises(TypeError):
            catalog._write_catalog()


class SyntheticNativeEngineTests(unittest.TestCase):
    def test_genuine_native_loop_catalog_and_scheduler_on_temporary_fixture(self):
        import datetime
        from fs42.catalog import ShowCatalog
        from fs42.liquid_schedule import LiquidSchedule
        from fs42.scheduling_context import ValidationSchedulingContext, activate_validation_context
        from fs42.station_manager import StationManager

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "confs").mkdir()
            (root / "runtime").mkdir()
            media = root / "media"
            media.mkdir()
            shutil.copy(ROOT / "fs42/station_config_schema.json", root / "fs42-schema.json")
            (root / "fs42").mkdir()
            shutil.move(root / "fs42-schema.json", root / "fs42/station_config_schema.json")
            create_database(root / "runtime/fs42_fluid.db").close()
            (root / "confs/main_config.json").write_text(
                json.dumps({"db_path": "runtime/fs42_fluid.db"})
            )
            (root / "confs/synthetic.json").write_text(json.dumps({"station_conf": {
                "network_name": "Synthetic Loop", "channel_number": 2,
                "network_type": "loop", "content_dir": str(media),
                "commercial_free": True, "shuffle_loop": False,
            }}))
            subprocess.run(
                ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
                 "color=c=black:s=16x16:r=1", "-t", "1", "-pix_fmt", "yuv420p",
                 str(media / "clip.mp4")], check=True, timeout=20,
            )
            old_cwd = os.getcwd()
            try:
                os.chdir(root)
                StationManager._StationManager__we_are_all_one = {}
                StationManager._initialized = False
                StationManager.stations = []
                context = ValidationSchedulingContext(
                    datetime.datetime(2026, 9, 14), datetime.datetime(2026, 9, 14),
                    datetime.datetime(2026, 9, 15), 7,
                )
                with activate_validation_context(context):
                    config = StationManager().station_by_name("Synthetic Loop")
                    self.assertIsNotNone(config)
                    ShowCatalog(config, rebuild_catalog=True, load=False)
                    schedule = LiquidSchedule(config)
                schedule.generate_validation_range(context.start_time, context.end_time, context)
                connection = sqlite3.connect(root / "runtime/fs42_fluid.db")
                try:
                    self.assertGreater(connection.execute(
                        "SELECT COUNT(*) FROM catalog_entries WHERE station='Synthetic Loop'"
                    ).fetchone()[0], 0)
                    self.assertGreater(connection.execute(
                        "SELECT COUNT(*) FROM liquid_blocks WHERE station='Synthetic Loop'"
                    ).fetchone()[0], 0)
                finally:
                    connection.close()
            finally:
                os.chdir(old_cwd)
                StationManager._StationManager__we_are_all_one = {}
                StationManager._initialized = False
                StationManager.stations = []

    def test_native_engine_rejects_direct_unattested_execution(self):
        from station_director import native_single_run as native

        with self.assertRaisesRegex(native.NativeRunError, "attestation"):
            native.execute_native_single_run({})

    def _fixture(self, root):
        work = root / "work"
        (work / "runtime").mkdir(parents=True)
        media = root / "media"
        media.mkdir()
        for name in ("a.mp4", "other.mp4"):
            (media / name).write_bytes(b"synthetic")
        connection = create_database(work / "runtime/fs42_fluid.db")
        connection.execute(
            "INSERT INTO named_sequence VALUES (1,'Action','main','Show A',0.0,1.0,0,1,NULL)"
        )
        connection.execute(
            "INSERT INTO sequence_entries VALUES (1,'/media/a.mp4',0,1)"
        )
        connection.execute(
            "INSERT INTO sequence_group_state VALUES ('Action','main','Show A','Show A')"
        )
        action = catalog_row("Action", "/mnt/t7/CRT-Media/a.mp4", "Show A", "Show A")
        other = catalog_row("Other", "/media/other.mp4", "Other", "Other")
        for catalog_id, row in ((1, action), (2, other)):
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (catalog_id, *[row[key] for key in row]),
            )
        insert_block(
            connection, "Action", "2026-09-14 05:00:00", "2026-09-14 07:00:00",
            1, "/media/a.mp4",
        )
        insert_block(
            connection, "Action", "2026-09-14 07:00:00", "2026-09-21 06:00:00",
            1, "/media/a.mp4",
        )
        insert_block(
            connection, "Other", "2026-09-14 06:00:00", "2026-09-21 06:00:00",
            2, "/media/other.mp4",
        )
        connection.commit()
        connection.close()
        proposal = base_proposal()
        policy = load_policy()
        seed_inputs = canonical_seed_inputs("c", "d", "m")
        request = {
            "proposal": proposal,
            "policy": policy,
            "validation_context": derive_validation_context(proposal, policy, seed_inputs),
        }
        projected = {"Action": {"station_conf": {
            "network_name": "Action", "channel_number": 2,
            "network_type": "standard", "clip_shows": {},
        }}}
        return work, media, request, projected

    @staticmethod
    def _sequence_rows(database):
        connection = sqlite3.connect(database)
        try:
            return {
                table: connection.execute(f'SELECT * FROM "{table}" ORDER BY 1').fetchall()
                for table in ("named_sequence", "sequence_entries", "sequence_group_state")
            }
        finally:
            connection.close()

    def test_every_post_snapshot_failure_restores_all_sequence_tables(self):
        from station_director import native_single_run as native

        phases = (
            "context", "configuration", "allocation", "catalog", "reconciliation",
            "schedule_construction", "explicit_range", "channel_result",
            "preservation", "guide", "final_verification",
        )
        for phase in phases:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                work, media, request, projected = self._fixture(Path(directory))
                database = work / "runtime/fs42_fluid.db"
                before = self._sequence_rows(database)

                def mutate_and_fail(*unused, **unused_kwargs):
                    connection = sqlite3.connect(database)
                    try:
                        connection.execute("UPDATE named_sequence SET current_index=99")
                        connection.execute("DELETE FROM sequence_entries")
                        connection.execute("UPDATE sequence_group_state SET active_tag_path='changed'")
                        connection.commit()
                    finally:
                        connection.close()
                    raise RuntimeError(f"injected-{phase}")

                class Catalog:
                    def __init__(self, unused_config, rebuild_catalog=False, load=True):
                        if rebuild_catalog:
                            connection = sqlite3.connect(database)
                            try:
                                connection.execute("DELETE FROM catalog_entries WHERE station='Action'")
                                row = catalog_row("Action", "/media/a.mp4", "Show A", "Show A")
                                connection.execute(
                                    "INSERT INTO catalog_entries "
                                    "(station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [row[key] for key in row],
                                )
                                connection.commit()
                            finally:
                                connection.close()

                class Schedule:
                    def __init__(self, unused_config):
                        self.catalog = types.SimpleNamespace(clip_index={})

                    def generate_validation_range(self, start, end, unused_context):
                        connection = sqlite3.connect(database)
                        try:
                            catalog_id = connection.execute(
                                "SELECT id FROM catalog_entries WHERE station='Action' AND path='/media/a.mp4'"
                            ).fetchone()[0]
                            insert_block(connection, "Action", str(start), str(end), catalog_id, "/media/a.mp4")
                            connection.commit()
                        finally:
                            connection.close()

                attestation = test_attestation(request)
                with ExitStack() as stack:
                    stack.enter_context(patch.object(
                        native, "_verified_work_tree",
                        return_value=(work, projected, ["Action"], {"Action": 2}, 0),
                    ))
                    stack.enter_context(patch.object(native, "MEDIA_ROOT", media))
                    stack.enter_context(patch.object(
                        native, "_native_station_config",
                        side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
                    ))
                    stack.enter_context(patch.object(native, "ShowCatalog", Catalog))
                    stack.enter_context(patch.object(native, "LiquidSchedule", Schedule))
                    stack.enter_context(patch.object(native, "_validate_final_cross_channel_exclusions"))
                    if phase != "guide":
                        stack.enter_context(patch.object(
                            native, "_run_guide_validation", return_value=passing_guide()
                        ))
                    restore = stack.enter_context(patch.object(
                        native, "_restore_sequences", wraps=native._restore_sequences,
                    ))
                    target = {
                        "context": "ValidationSchedulingContext",
                        "configuration": "_native_station_config",
                        "allocation": "catalog_allocation_floor",
                        "catalog": "ShowCatalog",
                        "reconciliation": "reconcile_catalog",
                        "schedule_construction": "LiquidSchedule",
                        "channel_result": "_build_channel_result",
                        "preservation": "_verify_final_preservation",
                        "guide": "_run_guide_validation",
                        "final_verification": "_final_input_verification",
                    }.get(phase)
                    if target:
                        stack.enter_context(patch.object(native, target, side_effect=mutate_and_fail))
                    elif phase == "explicit_range":
                        stack.enter_context(patch.object(Schedule, "generate_validation_range", side_effect=mutate_and_fail))
                    with self.assertRaises(BaseException) as caught:
                        native.execute_native_single_run(request, attestation)
                self.assertIn(f"injected-{phase}", str(caught.exception))
                self.assertTrue(restore.called)
                self.assertEqual(self._sequence_rows(database), before)

    def test_restoration_failure_records_original_and_secondary_failure(self):
        from station_director import native_single_run as native

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "ValidationSchedulingContext", side_effect=RuntimeError("original-phase")
            ), patch.object(
                native, "_restore_sequences", side_effect=RuntimeError("restore-phase")
            ):
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "sequence_restore_failure")
            self.assertIn("original-phase", caught.exception.original_failure)
            self.assertIn("restore-phase", caught.exception.restoration_failure)

    def _cross_channel_fixture(self, root, *, collide=True, reversed_order=False,
                               seams=("2026-09-14 06:00:00", "2026-09-14 07:00:00"),
                               horizons=("2026-09-21 06:00:00", "2026-09-20 06:00:00")):
        from fs42.station_manager import StationManager

        (root / "confs").mkdir()
        (root / "runtime").mkdir()
        (root / "confs/main_config.json").write_text(
            json.dumps({"db_path": "runtime/fs42_fluid.db"})
        )
        connection = create_database(root / "runtime/fs42_fluid.db")
        names = ("Action", "Late Night")
        path = "/media/shared.mp4"
        for identifier, name in enumerate(names, 1):
            row = catalog_row(name, path, "Shared", "Shared")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (identifier, *[row[key] for key in row]),
            )
        insert_block(connection, names[0], "2026-09-14 08:00:00", "2026-09-14 10:00:00", 1, path)
        second_start = "2026-09-14 09:00:00" if collide else "2026-09-14 10:00:00"
        insert_block(connection, names[1], second_start, "2026-09-14 11:00:00", 2, path)
        connection.commit()
        connection.close()
        StationManager._StationManager__we_are_all_one = {}
        StationManager._initialized = False
        StationManager.stations = []
        projected = {
            name: {"station_conf": {
                "network_name": name, "network_type": "standard",
                "content_dir": "/media", "clip_shows": {"Shared": {}},
            }} for name in names
        }
        if reversed_order:
            projected = dict(reversed(tuple(projected.items())))
        histories = {
            name: types.SimpleNamespace(
                regeneration_start=seams[index], effective_horizon=horizons[index]
            ) for index, name in enumerate(names)
        }
        return projected, histories

    def test_final_exclusion_validator_catches_both_directions_and_different_ranges(self):
        from fs42.station_manager import StationManager
        from station_director import native_single_run as native

        for label, reversed_order, seams, horizons in (
            ("symmetric", False, ("2026-09-14 06:00:00", "2026-09-14 06:00:00"),
             ("2026-09-21 06:00:00", "2026-09-21 06:00:00")),
            ("first-to-last", False, ("2026-09-14 06:00:00", "2026-09-14 07:00:00"),
             ("2026-09-21 06:00:00", "2026-09-20 06:00:00")),
            ("last-to-first", True, ("2026-09-14 07:00:00", "2026-09-14 06:00:00"),
             ("2026-09-20 06:00:00", "2026-09-21 06:00:00")),
        ):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                projected, histories = self._cross_channel_fixture(
                    root, reversed_order=reversed_order, seams=seams, horizons=horizons
                )
                previous = os.getcwd()
                try:
                    os.chdir(root)
                    with self.assertRaisesRegex(Exception, "cross-channel exclusion collision"):
                        native._validate_final_cross_channel_exclusions(projected, histories)
                finally:
                    os.chdir(previous)
                    StationManager._StationManager__we_are_all_one = {}
                    StationManager._initialized = False
                    StationManager.stations = []

    def test_final_exclusion_validator_accepts_touching_nonoverlap(self):
        from fs42.station_manager import StationManager
        from station_director import native_single_run as native

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            projected, histories = self._cross_channel_fixture(root, collide=False)
            previous = os.getcwd()
            try:
                os.chdir(root)
                native._validate_final_cross_channel_exclusions(projected, histories)
            finally:
                os.chdir(previous)
                StationManager._StationManager__we_are_all_one = {}
                StationManager._initialized = False
                StationManager.stations = []

    def test_one_run_uses_native_catalog_and_explicit_scheduler_hooks_deterministically(self):
        from station_director import native_single_run as native

        results = []
        for unused in range(2):
            with tempfile.TemporaryDirectory() as directory:
                work, media, request, projected = self._fixture(Path(directory))

                class Catalog:
                    def __init__(self, config, rebuild_catalog=False, load=True):
                        if rebuild_catalog:
                            connection = sqlite3.connect("runtime/fs42_fluid.db")
                            connection.execute("DELETE FROM catalog_entries WHERE station='Action'")
                            row = catalog_row("Action", "/media/a.mp4", "Show A", "Show A")
                            connection.execute(
                                "INSERT INTO catalog_entries "
                                "(station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                [row[key] for key in row],
                            )
                            connection.commit()
                            connection.close()

                native_schedule_class = native.LiquidSchedule

                def schedule_factory(config):
                    schedule = native_schedule_class.__new__(native_schedule_class)
                    schedule.conf = config
                    schedule.catalog = types.SimpleNamespace(clip_index={})

                    def fluid(self, start, end):
                        connection = sqlite3.connect("runtime/fs42_fluid.db")
                        catalog_id = connection.execute(
                            "SELECT id FROM catalog_entries WHERE station='Action' AND path='/media/a.mp4'"
                        ).fetchone()[0]
                        insert_block(
                            connection, "Action", str(start), str(end), catalog_id,
                            "/media/a.mp4",
                        )
                        connection.commit()
                        connection.close()
                    schedule._fluid = types.MethodType(fluid, schedule)
                    return schedule

                with patch.object(
                    native, "_verified_work_tree",
                    return_value=(work, projected, ["Action"], {"Action": 2}, 1),
                ), patch.object(
                    native, "_native_station_config",
                    side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
                ), patch.object(native, "MEDIA_ROOT", media), patch.object(native, "ShowCatalog", Catalog), patch.object(
                    native, "LiquidSchedule", schedule_factory
                ), patch.object(native, "_validate_final_cross_channel_exclusions"), patch.object(
                    native, "_run_guide_validation", return_value=passing_guide()
                ):
                    result = native.execute_native_single_run(request, test_attestation(request))
                results.append(result["channels"])
                self.assertTrue(result["scheduler_invoked"])
                self.assertEqual(result["preservation"]["sequence_tables_restored"], "pass")
        self.assertEqual(results[0], results[1])

    def test_catalog_scheduler_and_preservation_failures_keep_phase_and_channel(self):
        from station_director import native_single_run as native

        class CatalogFailure:
            def __init__(self, *unused, **unused_kwargs):
                raise RuntimeError("catalog broke")

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(native, "ShowCatalog", CatalogFailure):
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual((caught.exception.phase, caught.exception.channel), ("catalog", "Action"))

    def test_all_affected_futures_are_deleted_before_canonical_first_channel(self):
        from station_director import native_single_run as native

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            (media / "b.mp4").write_bytes(b"synthetic")
            connection = sqlite3.connect(work / "runtime/fs42_fluid.db")
            row = catalog_row(
                "After School", "/mnt/t7/CRT-Media/b.mp4", "Show B", "Show B"
            )
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (3,?,?,?,?,?,?,?,?,?,?,?,?)",
                [row[key] for key in row],
            )
            insert_block(
                connection, "After School", "2026-09-14 04:00:00",
                "2026-09-14 08:00:00", 3, "/media/b.mp4",
            )
            insert_block(
                connection, "After School", "2026-09-14 08:00:00",
                "2026-09-22 06:00:00", 3, "/media/b.mp4",
            )
            connection.commit()
            connection.close()
            projected["After School"] = {"station_conf": {
                "network_name": "After School", "channel_number": 3,
                "network_type": "standard", "clip_shows": {},
            }}

            class StopAtFirstCatalog:
                def __init__(self, *unused, **unused_kwargs):
                    raise RuntimeError("stop")

            with patch.object(
                native, "_verified_work_tree",
                return_value=(
                    work, projected, ["Action", "After School"],
                    {"Action": 2, "After School": 3}, 0,
                ),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(native, "ShowCatalog", StopAtFirstCatalog):
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.channel, "Action")
            connection = sqlite3.connect(work / "runtime/fs42_fluid.db")
            try:
                for channel in ("Action", "After School"):
                    count = connection.execute(
                        "SELECT COUNT(*) FROM liquid_blocks WHERE station=? AND start_time>=?",
                        (channel, "2026-09-14 06:00:00"),
                    ).fetchone()[0]
                    self.assertEqual(count, 0, channel)
            finally:
                connection.close()

    def test_scheduler_system_exit_is_not_mislabeled_autobump(self):
        from station_director import native_single_run as native

        class Catalog:
            def __init__(self, *unused, **unused_kwargs):
                pass

        class Schedule:
            def __init__(self, unused):
                self.catalog = types.SimpleNamespace(clip_index={})

            def generate_validation_range(self, *unused):
                raise SystemExit(-1)

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(native, "ShowCatalog", Catalog), patch.object(
                native, "LiquidSchedule", Schedule
            ):
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "native_system_exit")
            self.assertTrue(caught.exception.scheduler_invoked)


class VersionedDiagnosticTests(unittest.TestCase):
    def _request_file(self, root):
        media = root / "media"
        media.mkdir()
        request = request_for(root, media)
        path = root / "request.json"
        write_private_json_exclusive(path, request, REQUEST_SCHEMA)
        return request, path

    def _success_response(self, request):
        return {
            "schema_version": 2, "operation": "native_single_run",
            "run_id": request["run_id"],
            "proposal_id": request["proposal"]["proposal_id"],
            "status": "success", "phase_reached": "complete",
            "scheduler_invoked": True,
            "validation_context": {key: request["validation_context"][key]
                                   for key in ("input_fingerprint", "requested_seed",
                                               "effective_seed")},
            "affected_channels": request["affected_channels"],
            "channels": [passing_channel()], "verification": passing_verification(),
            "preservation": {"retained_history": "pass", "protected_channels": "pass",
                             "sequence_tables_restored": "pass",
                             "foreign_key_baseline": "pass"},
            "path_validation": {"passed": True, "mapping_count": 0,
                                "scheduled_path_checks": 0},
            "guide_validation": passing_guide(), "warnings": [], "failure": None,
            "timings_ms": {"prepare": 0, "catalog": 0, "scheduler": 1,
                           "preservation": 0, "guide": 0, "total": 1},
            "diagnostics": {"messages": [], "truncated": False},
        }

    def test_dependency_free_rules_are_strict_and_value_free(self):
        source = (ROOT / "station_director/c1_diagnostics.py").read_text()
        tree = ast.parse(source)
        imports = {alias.name for node in ast.walk(tree)
                   if isinstance(node, ast.Import) for alias in node.names}
        imports.update(node.module for node in ast.walk(tree)
                       if isinstance(node, ast.ImportFrom))
        self.assertEqual(imports, set())
        for code, (domain, phases, template) in DIAGNOSTIC_RULES.items():
            item = make_diagnostic(code, phases[0])
            validate_diagnostic(item)
            self.assertEqual((item["domain"], item["template"]), (domain, template))
            self.assertNotIn("/", item["template"])
        with self.assertRaises(ValueError):
            make_diagnostic("isolation_probe_failed", "probes", probe="not-a-probe")

    def test_every_preimport_boundary_is_safe_and_prevents_import(self):
        cases = [
            ("isolation_probe_failed", "probes", "environment_sanitized", None),
            ("original_configuration_verification_failed", "snapshot", None, "original_logical_configuration"),
            ("original_database_verification_failed", "snapshot", None, "original_logical_database"),
            ("media_manifest_verification_failed", "snapshot", None, "logical_media_manifest"),
            ("physical_transition_verification_failed", "snapshot", None, "live_physical_configuration"),
            ("projected_configuration_verification_failed", "snapshot", None, "projected_logical_configuration"),
            ("working_database_verification_failed", "snapshot", None, "working_logical_database"),
            ("validation_context_failed", "seed", None, None),
            ("timezone_verification_failed", "seed", None, None),
            ("hash_seed_verification_failed", "seed", None, None),
            ("effective_seed_verification_failed", "seed", None, None),
        ]
        for code, phase, probe, category in cases:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                request, request_path = self._request_file(root)
                error = BootstrapError(
                    "password=hunter2 /etc/shadow", phase=phase, code=code,
                    probe=probe, fingerprint_category=category)
                with patch(
                    "station_director.single_run_worker.attest_before_native_import",
                    side_effect=error,
                ), patch(
                    "station_director.single_run_worker.importlib.import_module"
                ) as loader:
                    result = run_worker(request_path, root / "response.json")
                loader.assert_not_called()
                self.assertEqual(result["schema_version"], 2)
                self.assertEqual(result["failure"]["code"], code)
                self.assertEqual(result["failure"]["probe"], probe)
                self.assertEqual(result["failure"]["fingerprint_category"], category)
                self.assertNotIn("hunter2", json.dumps(result))
                self.assertNotIn("/etc", json.dumps(result))

    def test_native_import_and_execution_phases_are_classified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, request_path = self._request_file(root)
            with patch(
                "station_director.single_run_worker.attest_before_native_import",
                return_value=(probes(request["run_id"]), test_attestation(request)),
            ), patch(
                "station_director.single_run_worker.importlib.import_module",
                side_effect=RuntimeError("secret /var/private"),
            ):
                result = run_worker(request_path, root / "response.json")
            self.assertEqual(result["failure"]["code"], "native_import_failed")
            self.assertNotIn("private", json.dumps(result))

        native_cases = (
            ("invalid_configuration", "configuration", False),
            ("catalog_failure", "catalog", False),
            ("scheduler_failure", "scheduler", True),
            ("preservation_failure", "preservation", True),
            ("guide_loading_failed", "guide", True),
        )
        for code, phase, invoked in native_cases:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                request, request_path = self._request_file(root)
                failure = type("Failure", (RuntimeError,), {
                    "code": code, "phase": phase, "scheduler_invoked": invoked,
                    "channel": "Action", "guide_validation": None,
                })("token=secret /home/private")
                fake = types.SimpleNamespace(
                    execute_native_single_run=lambda *unused: (_ for _ in ()).throw(failure))
                with patch(
                    "station_director.single_run_worker.attest_before_native_import",
                    return_value=(probes(request["run_id"]), test_attestation(request)),
                ), patch(
                    "station_director.single_run_worker.importlib.import_module",
                    return_value=fake,
                ):
                    result = run_worker(request_path, root / "response.json")
                self.assertEqual(result["failure"]["code"], code)
                self.assertEqual(result["failure"]["scheduler_invoked"], invoked)
                self.assertEqual(result["scheduler_invoked"], invoked)
                self.assertNotIn("secret", json.dumps(result))

    def test_held_request_integrity_failure_prevents_native_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, request_path = self._request_file(root)
            with patch(
                "station_director.single_run_worker.attest_before_native_import",
                return_value=(probes(request["run_id"]), test_attestation(request)),
            ), patch.object(
                HeldDocument, "assert_unchanged",
                side_effect=ProtocolError("hostile /etc/passwd password=bad"),
            ), patch(
                "station_director.single_run_worker.importlib.import_module"
            ) as loader:
                result = run_worker(request_path, root / "response.json")
            loader.assert_not_called()
            self.assertEqual(result["failure"]["code"], "request_integrity_failed")
            self.assertNotIn("passwd", json.dumps(result))

    def test_v1_response_is_rejected_by_current_inspector(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, unused = self._request_file(root)
            lifecycle = SingleRunLifecycle(
                ROOT, root, types.SimpleNamespace(closed=False), "token",
                request["run_id"], "unit", request,
                launcher_result=LaunchResult("unit", 1, "", ""))
            response = _base_response(request)
            response["schema_version"] = 1
            response["failure"] = {
                "phase": "probes", "channel": None, "code": "attestation_failure",
                "type": "BootstrapError", "message": "old response"}
            write_private_json_exclusive(root / "native-single-run.response.json",
                                         response, RESPONSE_SCHEMA_V1)
            with self.assertRaisesRegex(SingleRunError, "response is invalid") as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"],
                             "worker_response_invalid")

    def test_worker_cannot_claim_a_host_only_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, unused = self._request_file(root)
            response = _base_response(request)
            response["failure"] = make_diagnostic("launcher_failed", "launch")
            with self.assertRaisesRegex(ProtocolError, "diagnostic is invalid"):
                write_private_json_exclusive(root / "response.json", response,
                                             RESPONSE_SCHEMA)

    def test_host_classifies_every_response_acquisition_boundary(self):
        variants = (
            ("missing", "worker_response_missing"),
            ("malformed", "worker_response_invalid"),
            ("run", "worker_response_identity_mismatch"),
            ("proposal", "worker_response_identity_mismatch"),
            ("context", "worker_context_mismatch"),
            ("channels", "worker_channels_mismatch"),
            ("exit", "worker_exit_status_mismatch"),
        )
        for variant, code in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                request, unused = self._request_file(root)
                lifecycle = SingleRunLifecycle(
                    ROOT, root, types.SimpleNamespace(closed=False), "token",
                    request["run_id"], "unit", request,
                    launcher_result=LaunchResult("unit", 0, "secret", "/etc/shadow",
                                                 stdout_bytes=6, stderr_bytes=11,
                                                 stdout_truncated=True,
                                                 stderr_truncated=True))
                response_path = root / "native-single-run.response.json"
                if variant == "malformed":
                    response_path.write_text("{broken", encoding="utf-8")
                    response_path.chmod(0o600)
                elif variant != "missing":
                    response = self._success_response(request)
                    if variant == "run": response["run_id"] = "different-run"
                    elif variant == "proposal": response["proposal_id"] = "p-20260913T120000Z-feedface"
                    elif variant == "context": response["validation_context"]["effective_seed"] += 1
                    elif variant == "channels":
                        response["affected_channels"] = [
                            {"number": 3, "name": "After School"}]
                        response["channels"][0]["number"] = 3
                        response["channels"][0]["name"] = "After School"
                    elif variant == "exit": lifecycle.launcher_result.returncode = 1
                    write_private_json_exclusive(response_path, response, RESPONSE_SCHEMA)
                with self.assertRaises(SingleRunError) as caught:
                    inspect_single_run(lifecycle)
                self.assertEqual(caught.exception.c1_diagnostic["code"], code)
                self.assertEqual(set(caught.exception.launcher_summary), {
                    "outcome", "stdout_bytes", "stderr_bytes", "stdout_truncated",
                    "stderr_truncated"})
                self.assertNotIn("secret", json.dumps(caught.exception.launcher_summary))
                self.assertNotIn("shadow", json.dumps(caught.exception.launcher_summary))


class LifecycleTests(unittest.TestCase):
    class Lock:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def _lifecycle(self, root, request):
        return SingleRunLifecycle(
            ROOT, root, self.Lock(), "abc123abc123", request["run_id"],
            "fs42-native-test.service", request,
        )

    def test_invocation_context_fails_before_staging(self):
        with patch("station_director.single_run.create_staging_directory") as create, patch(
            "station_director.single_run.check_invocation_context", return_value=(False, "Codex detected")
        ):
            with self.assertRaisesRegex(SingleRunError, "verified SSH"):
                prepare_single_run(
                    ROOT, ROOT, ROOT, base_proposal(), load_policy(), "run",
                )
        create.assert_not_called()

    def test_finalization_steps_are_safe_precise_and_preserve_explicit_codes(self):
        for subphase in sorted(FINALIZATION_SUBPHASES):
            with self.subTest(subphase=subphase), self.assertRaises(
                    SingleRunFinalizationError) as caught:
                _finalization_step(
                    subphase,
                    lambda: (_ for _ in ()).throw(
                        RuntimeError("password=hunter2 /etc/shadow")),
                )
            self.assertEqual(caught.exception.finalization_subphase, subphase)
            self.assertEqual(caught.exception.code, "single_run_finalization_failed")
            self.assertNotIn("hunter2", str(caught.exception))

        explicit = SingleRunError("prepare", "duplicate_stage_file", "fixed")
        with self.assertRaises(SingleRunError) as caught:
            _finalization_step("request_publication", lambda: (_ for _ in ()).throw(explicit))
        self.assertIs(caught.exception, explicit)
        self.assertEqual(caught.exception.finalization_subphase, "request_publication")

    def test_finalizer_rejects_empty_resolved_channels_before_request_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "fs42-i-123456abcdef"
            source = stage / "source/confs"
            source.mkdir(parents=True)
            config = source / "action.json"
            config.write_text(
                json.dumps({"station_conf": {"network_name": "Action"}}),
                encoding="utf-8",
            )
            proposal = base_proposal()
            policy = {"channels": [{"number": 2, "name": "Action"}]}
            with patch(
                    "station_director.single_run.protected_json_paths",
                    return_value={"confs/action.json": config}), patch(
                    "station_director.single_run.fingerprint_json_files",
                    return_value={"digest": "a" * 64}), patch(
                    "station_director.single_run.logical_protected_configuration_fingerprint",
                    return_value={"digest": "b" * 64}), patch(
                    "station_director.single_run.canonical_seed_inputs",
                    return_value={"configuration": "b" * 64}), patch(
                    "station_director.single_run.derive_validation_context",
                    return_value={"effective_seed": 1}), patch(
                    "station_director.single_run.project_configuration",
                    return_value=({}, set(), {})), patch(
                    "station_director.single_run.bind_request") as bind:
                with self.assertRaises(SingleRunError) as caught:
                    _finalize_prepared_single_run(
                        Path(directory), stage, object(), "123456abcdef", "run-1",
                        proposal, policy, configuration_digest="b" * 64,
                        database_digest="c" * 64, media_digest="d" * 64,
                        live_physical_digest="e" * 64,
                    )
            self.assertEqual(caught.exception.code, "proposal_has_no_effects")
            self.assertEqual(
                caught.exception.finalization_subphase,
                "affected_channel_resolution",
            )
            bind.assert_not_called()
            self.assertFalse((stage / "native-single-run.request.json").exists())

    def test_each_real_finalizer_operation_reports_its_exact_subphase(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "fs42-i-123456abcdef"
            config = stage / "source/confs/action.json"
            config.parent.mkdir(parents=True)
            config.write_text(
                json.dumps({"station_conf": {"network_name": "Action"}}),
                encoding="utf-8",
            )
            proposal = base_proposal()
            proposal["directives"] = [{
                "type": "date_slot", "channel": 2, "date": "2026-09-15",
                "hour": 20, "series": "Batman Beyond",
            }]
            policy = {"channels": [{"number": 2, "name": "Action"}]}
            for subphase in sorted(FINALIZATION_SUBPHASES):
                protected = Mock(
                    return_value={"confs/action.json": config})
                physical = Mock(return_value={"digest": "a" * 64})
                logical = Mock(return_value={"digest": "b" * 64})
                seeds = Mock(return_value={"configuration": "b" * 64})
                context = Mock(return_value={"effective_seed": 1})
                project = Mock(return_value=({}, {"Action"}, {}))
                bind = Mock(return_value={})
                publish = Mock(return_value=None)
                lifecycle = Mock(return_value=object())
                failing = RuntimeError("token=secret /etc/shadow")
                if subphase == "staged_physical_fingerprint":
                    physical.side_effect = failing
                elif subphase == "staged_logical_configuration_fingerprint":
                    logical.side_effect = failing
                elif subphase == "seed_input_construction":
                    seeds.side_effect = failing
                elif subphase == "validation_context_derivation":
                    context.side_effect = failing
                elif subphase == "staged_channel_configuration_loading":
                    protected.side_effect = [
                        {"confs/action.json": config},
                        {"confs/action.json": config},
                        failing,
                    ]
                elif subphase == "proposal_projection":
                    project.side_effect = failing
                elif subphase == "affected_channel_resolution":
                    project.return_value = ({}, {"Unknown"}, {})
                elif subphase == "request_binding":
                    bind.side_effect = failing
                elif subphase == "request_publication":
                    publish.side_effect = failing
                elif subphase == "lifecycle_construction":
                    lifecycle.side_effect = failing
                with self.subTest(subphase=subphase), patch(
                        "station_director.single_run.protected_json_paths", protected), patch(
                        "station_director.single_run.fingerprint_json_files", physical), patch(
                        "station_director.single_run.logical_protected_configuration_fingerprint",
                        logical), patch(
                        "station_director.single_run.canonical_seed_inputs", seeds), patch(
                        "station_director.single_run.derive_validation_context", context), patch(
                        "station_director.single_run.project_configuration", project), patch(
                        "station_director.single_run.bind_request", bind), patch(
                        "station_director.single_run.write_private_json_exclusive", publish), patch(
                        "station_director.single_run.SingleRunLifecycle", lifecycle):
                    with self.assertRaises(SingleRunFinalizationError) as caught:
                        _finalize_prepared_single_run(
                            Path(directory), stage, object(), "123456abcdef", "run-1",
                            proposal, policy, configuration_digest="b" * 64,
                            database_digest="c" * 64, media_digest="d" * 64,
                            live_physical_digest="e" * 64,
                        )
                self.assertEqual(caught.exception.finalization_subphase, subphase)
                self.assertNotIn("secret", str(caught.exception))

    def test_c1_tmp_is_stage_backed_without_changing_default_launcher(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)
            with patch("station_director.isolation.STAGING_PARENT", root):
                temporary = prepare_stage_temporary(stage)
                with patch("station_director.isolation._existing_runtime_mounts", return_value=[]):
                    default = build_bwrap_command(ROOT, stage, ["python"])
                    c1 = build_bwrap_command(
                        ROOT, stage, ["python"], stage_tmp=True,
                        verified_temporary=temporary,
                    )
            temporary.close()
            self.assertIn("--tmpfs", default)
            self.assertNotIn("--tmpfs", c1)
            index = c1.index(str(stage / "transient"))
            self.assertEqual(c1[index - 1:index + 2], ["--bind", str(stage / "transient"), "/tmp"])

    def test_launcher_creates_verified_stage_tmp_before_command_and_holds_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)
            observed = {}

            def launch(command, unit, timeout):
                transient = stage / "transient"
                observed["exists"] = transient.is_dir()
                observed["mode"] = transient.stat().st_mode & 0o777
                bind = command.index(str(transient))
                observed["mount"] = command[bind - 1:bind + 2]
                return LaunchResult(unit, 0, "", "")

            with patch("station_director.isolation.STAGING_PARENT", root), patch(
                "station_director.isolation._run_bounded", side_effect=launch
            ):
                result = IsolationLauncher(ROOT).run(
                    stage, ["python"], "test.service", stage_tmp=True
                )
            self.assertEqual(result.returncode, 0)
            self.assertTrue(observed["exists"])
            self.assertEqual(observed["mode"], 0o700)
            self.assertEqual(
                observed["mount"], ["--bind", str(stage / "transient"), "/tmp"]
            )

    def test_stage_tmp_refuses_preexisting_file_symlink_and_directory(self):
        for kind in ("file", "symlink", "directory"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                stage = root / "fs42-i-123456abcdef"
                stage.mkdir(mode=0o700)
                transient = stage / "transient"
                if kind == "file":
                    transient.write_text("unexpected")
                elif kind == "symlink":
                    transient.symlink_to(root)
                else:
                    transient.mkdir()
                with patch("station_director.isolation.STAGING_PARENT", root), patch(
                    "station_director.isolation._run_bounded"
                ) as runner:
                    with self.assertRaisesRegex(IsolationError, "pre-existing"):
                        IsolationLauncher(ROOT).run(
                            stage, ["python"], "test.service", stage_tmp=True
                        )
                runner.assert_not_called()

    def test_stage_tmp_requires_canonical_private_owned_stage(self):
        for defect in ("name", "parent", "mode", "owner"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                expected_parent = root / "expected"
                expected_parent.mkdir()
                parent = root if defect == "parent" else expected_parent
                name = "unexpected" if defect == "name" else "fs42-i-123456abcdef"
                stage = parent / name
                stage.mkdir(mode=0o700)
                if defect == "mode":
                    stage.chmod(0o750)
                uid = os.getuid() + 1 if defect == "owner" else os.getuid()
                with patch("station_director.isolation.STAGING_PARENT", expected_parent), patch(
                    "station_director.isolation.os.getuid", return_value=uid
                ), patch("station_director.isolation._run_bounded") as runner:
                    with self.assertRaisesRegex(IsolationError, "unsafe staging"):
                        IsolationLauncher(ROOT).run(
                            stage, ["python"], "test.service", stage_tmp=True
                        )
                runner.assert_not_called()
                self.assertFalse((stage / "transient").exists())

    def test_stage_tmp_command_requires_held_verified_source(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)
            with patch("station_director.isolation.STAGING_PARENT", Path(directory)):
                with self.assertRaisesRegex(IsolationError, "verified"):
                    build_bwrap_command(ROOT, stage, ["python"], stage_tmp=True)

    def test_stage_tmp_replacement_during_launch_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)

            def replace_during_launch(unused_command, unit, unused_timeout):
                transient = stage / "transient"
                transient.rename(stage / "replaced-transient")
                transient.mkdir(mode=0o700)
                return LaunchResult(unit, 0, "", "")

            with patch("station_director.isolation.STAGING_PARENT", root), patch(
                "station_director.isolation._run_bounded",
                side_effect=replace_during_launch,
            ):
                with self.assertRaisesRegex(IsolationError, "identity"):
                    IsolationLauncher(ROOT).run(
                        stage, ["python"], "test.service", stage_tmp=True
                    )

    def test_stage_tmp_launch_failure_leaves_source_for_outer_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)
            with patch("station_director.isolation.STAGING_PARENT", root), patch(
                "station_director.isolation._run_bounded",
                side_effect=RuntimeError("synthetic launch failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic launch failure"):
                    IsolationLauncher(ROOT).run(
                        stage, ["python"], "test.service", stage_tmp=True
                    )
            self.assertTrue((stage / "transient").is_dir())

    def test_ordinary_launcher_profile_does_not_create_transient(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "fs42-i-123456abcdef"
            stage.mkdir(mode=0o700)
            captured = {}

            def launch(command, unit, timeout):
                captured["command"] = command
                return LaunchResult(unit, 0, "", "")

            with patch("station_director.isolation.STAGING_PARENT", root), patch(
                "station_director.isolation._run_bounded", side_effect=launch
            ):
                result = IsolationLauncher(ROOT).run(stage, ["python"], "test.service")
            self.assertEqual(result.returncode, 0)
            self.assertFalse((stage / "transient").exists())
            self.assertIn("--tmpfs", captured["command"])

    def test_native_single_run_preflight_profile_is_explicit(self):
        args = build_parser().parse_args(
            ["isolation", "preflight", "--profile", "native-single-run"]
        )
        self.assertEqual(args.profile, "native-single-run")

    def test_launcher_drains_and_bounds_simultaneous_output(self):
        program = (
            "import os,threading; "
            "a=threading.Thread(target=lambda:os.write(1,b'x'*200000)); "
            "b=threading.Thread(target=lambda:os.write(2,b'y'*210000)); "
            "a.start();b.start();a.join();b.join()"
        )
        result = _run_bounded([sys.executable, "-c", program], "synthetic.service", 10)
        self.assertEqual(result.returncode, 0)
        self.assertEqual((result.stdout_bytes, result.stderr_bytes), (200000, 210000))
        self.assertTrue(result.stdout_truncated and result.stderr_truncated)
        self.assertLessEqual(len(result.stdout.encode()), MAX_CAPTURE_BYTES)
        self.assertLessEqual(len(result.stderr.encode()), MAX_CAPTURE_BYTES)

    def test_stage_backed_tmp_probe_compares_exact_mount_identity(self):
        same = types.SimpleNamespace(st_dev=7, st_ino=9)
        with patch(
            "station_director.isolation_probe.mount_info", return_value=({"rw"}, "ext4")
        ), patch("station_director.isolation_probe.Path.stat", return_value=same):
            self.assertTrue(check_stage_backed_tmp("/stage")["passed"])
        different = [same, types.SimpleNamespace(st_dev=7, st_ino=10)]
        with patch(
            "station_director.isolation_probe.mount_info", return_value=({"rw"}, "ext4")
        ), patch("station_director.isolation_probe.Path.stat", side_effect=different):
            self.assertFalse(check_stage_backed_tmp("/stage")["passed"])

    def test_launch_uses_only_isolation_launcher_and_inspection_is_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            lifecycle = self._lifecycle(root, request)

            class Launcher:
                def run(self, stage, argv, unit, timeout, *, stage_tmp=False):
                    self.args = (stage, argv, unit, timeout, stage_tmp)
                    return LaunchResult(unit, 0, "", "")

            launcher = Launcher()
            with patch("station_director.single_run.IsolationLauncher", return_value=launcher):
                launch_single_run(lifecycle, timeout=77)
            self.assertIn("/project/station_director/single_run_worker.py", launcher.args[1])
            self.assertTrue(launcher.args[4])
            response = {
                "schema_version": 2, "operation": "native_single_run",
                "run_id": request["run_id"], "proposal_id": request["proposal"]["proposal_id"],
                "status": "success", "phase_reached": "complete", "scheduler_invoked": True,
                "validation_context": {
                    key: request["validation_context"][key]
                    for key in ("input_fingerprint", "requested_seed", "effective_seed")
                },
                "affected_channels": request["affected_channels"],
                "channels": [passing_channel()],
                "verification": passing_verification(),
                "preservation": {
                    "retained_history": "pass", "protected_channels": "pass",
                    "sequence_tables_restored": "pass", "foreign_key_baseline": "pass",
                },
                "path_validation": {
                    "passed": True, "mapping_count": 0, "scheduled_path_checks": 0,
                },
                "guide_validation": passing_guide(),
                "warnings": [], "failure": None,
                "timings_ms": {
                    "prepare": 0, "catalog": 0, "scheduler": 1,
                    "preservation": 0, "guide": 0, "total": 1,
                },
                "diagnostics": {"messages": [], "truncated": False},
            }
            write_private_json_exclusive(root / "native-single-run.response.json", response, RESPONSE_SCHEMA)
            self.assertEqual(inspect_single_run(lifecycle)["status"], "success")
            self.assertIsNotNone(lifecycle.response)

    def test_timeout_is_structured_before_result_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            lifecycle = self._lifecycle(root, request)
            lifecycle.launcher_result = LaunchResult(
                lifecycle.unit_name, 124, "", "timeout", timed_out=True
            )
            with self.assertRaisesRegex(SingleRunError, "timed out") as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"], "launcher_timeout")
            self.assertEqual(caught.exception.launcher_summary["outcome"], "timed_out")

    def test_malformed_result_and_cleanup_failure_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            request = request_for(root, media)
            lifecycle = self._lifecycle(root, request)
            lifecycle.launcher_result = LaunchResult(lifecycle.unit_name, 0, "", "")
            result = root / "native-single-run.response.json"
            result.write_text("{partial")
            os.chmod(result, 0o600)
            with self.assertRaisesRegex(SingleRunError, "response is invalid"):
                inspect_single_run(lifecycle)
            with patch(
                "station_director.single_run.cleanup_unit", return_value=(False, "unit remained")
            ), patch(
                "station_director.single_run.cleanup_staging_directory",
                return_value=(True, "removed"),
            ):
                with self.assertRaisesRegex(SingleRunError, "unit remained"):
                    lifecycle.cleanup()
            self.assertFalse(lifecycle.lock.closed)


if __name__ == "__main__":
    unittest.main()
