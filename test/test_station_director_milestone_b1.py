import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from station_director import isolation, isolation_probe, validation
from station_director.isolation import LaunchResult
from station_director.path_safety import (
    PathSafetyError,
    map_media_path,
    map_station_config,
    validate_scheduled_media,
)
from station_director.stage_runner import DISABLED_MESSAGE, run_worker
from station_director.validation_context import derive_validation_context


def passing_probes(run_id, **unused):
    results = {
        name: {"passed": True, "detail": f"verified {name}"}
        for name in isolation_probe.PROBE_RESULTS
    }
    return {
        "schema_version": isolation_probe.PROBE_SCHEMA_VERSION,
        "run_id": run_id,
        "overall_pass": True,
        "results": results,
    }


def proposal():
    return {
        "schema_version": 2,
        "proposal_id": "proposal-b1",
        "week_start": "2026-09-14T00:00:00-07:00",
        "week_end": "2026-09-21T00:00:00-07:00",
        "seed": 42,
        "assignment_changes": [],
        "directives": [],
        "exclusions": [],
        "source_hashes": {},
    }


def seed_inputs():
    return {
        "logical_protected_configuration_fingerprint": "configs",
        "logical_database_fingerprint": "database",
        "logical_media_manifest_fingerprint": "media",
    }


def validation_context(proposal_value=None, policy=None):
    return derive_validation_context(
        proposal_value or proposal(), policy or {}, seed_inputs()
    )


class PathConfinementTests(unittest.TestCase):
    def test_live_paths_map_to_sandbox_with_canonical_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            media = Path(directory)
            target = media / "StationAssets/Action"
            target.mkdir(parents=True)
            mapping = map_media_path(
                "/mnt/t7/CRT-Media/StationAssets/Action",
                "commercial_dir",
                sandbox_media_root=media,
                expected="directory",
            )
            self.assertEqual(mapping.logical_identity, "crt-media:/StationAssets/Action")
            self.assertEqual(
                mapping.canonical_host_path,
                "/mnt/t7/CRT-Media/StationAssets/Action",
            )
            self.assertEqual(mapping.sandbox_path, str(target))
            self.assertTrue(mapping.sandbox_only)

            relative = map_media_path(
                "catalog/crt_media/StationAssets/Action",
                "commercial_dir",
                sandbox_media_root=media,
                expected="directory",
            )
            self.assertEqual(relative.logical_identity, mapping.logical_identity)

    def test_rejects_sandbox_path_as_live_identity_and_lexical_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            for value in (
                "/media/show.mp4",
                "catalog/crt_media/../secret",
                "/mnt/t7/CRT-Media/../secret",
                "/opt/video.mp4",
            ):
                with self.subTest(value=value), self.assertRaises(PathSafetyError):
                    map_media_path(value, "test", sandbox_media_root=directory)

    def test_resolved_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            media = Path(directory)
            (media / "escape").symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(PathSafetyError, "resolve safely"):
                map_media_path(
                    "catalog/crt_media/escape",
                    "content_dir",
                    sandbox_media_root=media,
                    expected="directory",
                )

    def test_scheduled_content_requires_readable_regular_file_beneath_media(self):
        with tempfile.TemporaryDirectory() as directory:
            media = Path(directory)
            item = media / "Show/episode.mp4"
            item.parent.mkdir()
            item.write_bytes(b"video")
            mapping = validate_scheduled_media(
                "/media/Show/episode.mp4", sandbox_media_root=media
            )
            self.assertEqual(mapping.logical_identity, "crt-media:/Show/episode.mp4")
            with self.assertRaisesRegex(PathSafetyError, "regular file"):
                validate_scheduled_media("/media/Show", sandbox_media_root=media)
            with self.assertRaisesRegex(PathSafetyError, "not rooted"):
                validate_scheduled_media(str(item), sandbox_media_root=media)
            fifo = media / "pipe"
            os.mkfifo(fifo)
            with self.assertRaisesRegex(PathSafetyError, "regular file"):
                validate_scheduled_media("/media/pipe", sandbox_media_root=media)

    def test_config_mapper_handles_inspected_active_path_fields(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as stage:
            media = Path(directory)
            (media / "StationAssets/commercial").mkdir(parents=True)
            (media / "StationAssets/bump").mkdir(parents=True)
            data = {
                "station_conf": {
                    "network_name": "Action",
                    "channel_number": 2,
                    "content_dir": "catalog/crt_media",
                    "commercial_dir": "/mnt/t7/CRT-Media/StationAssets/commercial",
                    "day_templates": {
                        "daily": {
                            "0": {
                                "tags": "Show",
                                "bump_dir": "/mnt/t7/CRT-Media/StationAssets/bump",
                            }
                        }
                    },
                }
            }
            mapped, mappings = map_station_config(
                data,
                "action.json",
                sandbox_media_root=media,
                stage_root=stage,
            )
            self.assertEqual(mapped["station_conf"]["content_dir"], str(media))
            self.assertTrue(all(item.sandbox_only for item in mappings))
            self.assertEqual(len(mappings), 3)


class WorkerTests(unittest.TestCase):
    def test_worker_attests_then_returns_disabled_without_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            project = base / "project"
            stage = base / "stage"
            media = base / "media"
            (project / "confs").mkdir(parents=True)
            stage.mkdir()
            media.mkdir()
            config = {
                "station_conf": {
                    "network_name": "Action",
                    "channel_number": 2,
                    "content_dir": "catalog/crt_media",
                }
            }
            (project / "confs/action.json").write_text(json.dumps(config))
            (project / "fs42").mkdir()
            (project / "fs42/station_config_schema.json").write_text("{}")
            policy = {"channels": [{"number": 2, "name": "Action"}]}
            request = {
                "schema_version": 1,
                "run_id": "run-b1",
                "proposal": proposal(),
                "policy": policy,
                "seed_inputs": seed_inputs(),
                "validation_context": validation_context(policy=policy),
            }
            request_path = stage / "request.json"
            result_path = stage / "result.json"
            request_path.write_text(json.dumps(request))
            (stage / "runtime").mkdir()
            sqlite3.connect(stage / "runtime/fs42_fluid.db").close()
            history = Mock()
            history.summary.return_value = {"channel": "Action"}
            with patch.dict(
                os.environ,
                {"TZ": "America/Los_Angeles", "PYTHONHASHSEED": "0"},
            ), patch(
                "station_director.stage_runner.inspect_required_schema",
                return_value={"tables": {"liquid_blocks": []}},
            ), patch(
                "station_director.stage_runner.capture_channel_history",
                return_value=history,
            ), patch(
                "station_director.native_config_checks.validate_processed_configurations",
                return_value=[],
            ), patch(
                "station_director.native_config_checks.newly_unresolved_source_slots",
                return_value=([], []),
            ), patch(
                "station_director.stage_runner.PROJECT_ROOT", project,
            ), patch(
                "station_director.stage_runner.STAGE_ROOT", stage,
            ), patch(
                "station_director.stage_runner.MEDIA_ROOT", media,
            ), patch(
                "station_director.stage_runner.build_probe_payload", side_effect=passing_probes,
            ):
                payload = run_worker(
                    request_path,
                    result_path,
                )
            self.assertEqual(payload["status"], "disabled")
            self.assertEqual(payload["failure"], DISABLED_MESSAGE)
            self.assertFalse(payload["scheduler_invoked"])
            self.assertTrue(payload["path_validation"]["passed"])
            self.assertEqual(payload["b2_preparation"]["scheduler_gate"], "disabled")
            self.assertEqual(json.loads(result_path.read_text()), payload)

    def test_failed_probe_stops_before_configuration_access(self):
        def failed(run_id, **unused):
            payload = passing_probes(run_id)
            payload["results"][isolation_probe.PROBE_RESULTS[0]]["passed"] = False
            payload["overall_pass"] = False
            return payload

        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            request_path = stage / "request.json"
            result_path = stage / "result.json"
            request_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": "run-b1",
                        "proposal": proposal(),
                        "policy": {},
                        "seed_inputs": seed_inputs(),
                        "validation_context": validation_context(),
                    }
                )
            )
            with patch("station_director.stage_runner.STAGE_ROOT", stage), patch(
                "station_director.stage_runner.build_probe_payload", side_effect=failed
            ):
                payload = run_worker(request_path, result_path)
            self.assertEqual(payload["status"], "failed")
            self.assertFalse(payload["path_validation"]["passed"])

    def test_worker_has_no_scheduler_or_subprocess_import(self):
        source = (Path(__file__).parents[1] / "station_director/stage_runner.py").read_text()
        self.assertNotIn("ShowCatalog", source)
        self.assertNotIn("LiquidSchedule", source)
        self.assertNotIn("subprocess", source)


class CoordinatorTests(unittest.TestCase):
    def test_compatibility_entry_point_is_disabled_before_any_side_effect(self):
        with patch.object(validation, "check_invocation_context") as invocation, patch.object(validation, "create_staging_directory") as create_stage, patch.object(validation, "_static_validation_checks") as checks:
            report = validation.validate_proposal(proposal(), "/unused", {})
        self.assertFalse(report["valid"])
        self.assertEqual(report["failures"], [validation.PHASE_3_DISABLED])
        self.assertFalse(report["scheduler_invoked"])
        invocation.assert_not_called()
        create_stage.assert_not_called()
        checks.assert_not_called()

    def test_compatibility_entry_point_never_launches_reads_or_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(validation, "check_invocation_context") as invocation, patch.object(validation, "create_staging_directory") as create_stage, patch.object(validation, "IsolationLauncher") as launcher, patch.object(validation, "load_proposal", create=True) as load_proposal, patch.object(validation, "load_policy", create=True) as load_policy:
                report = validation.validate_proposal(proposal(), root, {})
            self.assertEqual(report["failures"], [validation.PHASE_3_DISABLED])
            self.assertFalse(report["scheduler_invoked"])
            for mocked in (invocation, create_stage, launcher, load_proposal, load_policy):
                mocked.assert_not_called()
            self.assertEqual(list(root.iterdir()), [])

    def test_validation_module_has_no_direct_subprocess_runner(self):
        source = (Path(__file__).parents[1] / "station_director/validation.py").read_text()
        self.assertNotIn("import subprocess", source)
        self.assertNotIn("subprocess.run", source)


if __name__ == "__main__":
    unittest.main()
