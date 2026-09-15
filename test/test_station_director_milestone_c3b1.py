import contextlib
import copy
import fcntl
import io
import json
import os
import signal
import stat
import time
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from station_director import cli, validation, validation_control
from station_director.c1_diagnostics import DIAGNOSTIC_RULES
from station_director.validation_coordinator import (
    CoordinatorError,
    _recover_stale_stages,
    CoordinatorOutcome,
    _CancellationScope,
    _validation_lock,
    render_cli_outcome,
)
from station_director import secure_validation_inputs as secure_inputs
from test.test_station_director_milestone_c2 import NativeTwoProcessIntegrationTests
from test.test_station_director_milestone_c3a2 import (
    PROPOSAL,
    compare,
    fixture,
    success_c2,
)
from test.test_station_director_schedule import base_proposal

VALID_PROPOSAL = base_proposal()
VALID_PROPOSAL["directives"] = [{
    "type": "date_slot", "channel": 2, "date": "2026-09-15",
    "hour": 20, "series": "Batman Beyond",
}]
RUN_ID = "v-20260913T120000000001Z-" + "a" * 32


POLICY = {
    "schema_version": 2,
    "channels": [
        {"number": 1, "name": "CRT Station Guide"},
        {"number": 2, "name": "Action"},
        {"number": 3, "name": "After School"},
        {"number": 4, "name": "Anime"},
        {"number": 5, "name": "Cartoon Network"},
        {"number": 6, "name": "Disney"},
        {"number": 7, "name": "Late Night"},
        {"number": 8, "name": "Watch In Order"},
    ],
}


def private_directory(path):
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def private_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def prepare_secure_project(root, proposal=None, policy=POLICY):
    proposal = copy.deepcopy(VALID_PROPOSAL if proposal is None else proposal)
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o755)
    runtime = root / "runtime"
    runtime.mkdir(exist_ok=True)
    runtime.chmod(0o755)
    director = private_directory(runtime / "director")
    proposals = private_directory(director / "proposals")
    proposal_dir = private_directory(proposals / proposal["proposal_id"])
    private_json(proposal_dir / "proposal.json", proposal)
    policy_dir = private_directory(root / "director_conf")
    private_json(policy_dir / secure_inputs.CANONICAL_POLICY_NAME, policy)
    return proposal_dir


def valid_success_result(root):
    data = root / "comparison"
    data.mkdir()
    baseline, proposed, unused_stage = fixture(data, [
        ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001",
         "A", "/media/a.mp4"),
    ])
    return success_c2(compare(data, baseline, proposed))


class DisabledGateTests(unittest.TestCase):
    def test_disabled_cli_is_byte_exact_and_imports_no_coordinator(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(validation_control, "SCHEDULE_VALIDATION_ENABLED", False), \
                patch.object(cli, "load_policy") as load_policy, \
                patch.dict(sys.modules, {"station_director.validation_coordinator": None}), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(result, 1)
        self.assertEqual(stdout.getvalue(), "Phase 3 validation is not yet enabled\n")
        self.assertEqual(stderr.getvalue(), "")
        load_policy.assert_not_called()

    def test_disabled_gate_precedes_filesystem_and_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before = list(root.iterdir())
            with patch.object(validation_control, "SCHEDULE_VALIDATION_ENABLED", False), \
                    patch.object(cli, "load_policy") as policy:
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
            self.assertEqual(code, 1)
            self.assertEqual(list(root.iterdir()), before)
            policy.assert_not_called()

    def test_argparse_usage_error_may_precede_gate(self):
        with self.assertRaises(SystemExit) as raised, contextlib.redirect_stderr(io.StringIO()):
            cli.main(["schedule", "validate"])
        self.assertEqual(raised.exception.code, 2)

    def test_custom_policy_behavior_for_unrelated_command_is_unchanged(self):
        custom = Path("synthetic-policy.json")
        with patch.object(cli, "load_policy", return_value=POLICY) as load,                 patch.object(cli, "station_status", return_value={"errors": []}),                 contextlib.redirect_stdout(io.StringIO()):
            code = cli.main(["--policy", str(custom), "status"])
        self.assertEqual(code, 0)
        load.assert_called_once_with(custom)

    def test_legacy_entry_is_permanently_disabled_even_if_master_gate_true(self):
        with patch.object(validation_control, "SCHEDULE_VALIDATION_ENABLED", True), \
                patch.object(validation, "check_invocation_context") as invocation, \
                patch.object(validation, "create_staging_directory") as staging, \
                patch.object(validation, "IsolationLauncher") as launcher:
            result = validation.validate_proposal(copy.deepcopy(VALID_PROPOSAL), "/unused", POLICY)
        self.assertEqual(result["failures"], [validation_control.DISABLED_MESSAGE])
        self.assertFalse(result["scheduler_invoked"])
        invocation.assert_not_called()
        staging.assert_not_called()
        launcher.assert_not_called()


class SecureInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "project"
        self.proposal_dir = prepare_secure_project(self.root)
        self.root_patch = patch.object(secure_inputs, "TRUSTED_PROJECT_ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)

    def test_loads_and_migrates_without_rewriting_input(self):
        target = self.proposal_dir / "proposal.json"
        before = (target.read_bytes(), target.stat())
        loaded = secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        after = (target.read_bytes(), target.stat())
        self.assertEqual(loaded, VALID_PROPOSAL)
        self.assertEqual(before[0], after[0])
        self.assertEqual((before[1].st_ino, before[1].st_size, before[1].st_mtime_ns),
                         (after[1].st_ino, after[1].st_size, after[1].st_mtime_ns))
        self.assertEqual(secure_inputs.load_canonical_policy(), POLICY)

    def test_v1_migration_is_in_memory_only(self):
        legacy = base_proposal(version=1)
        target = self.proposal_dir / "proposal.json"
        private_json(target, legacy)
        before = target.read_bytes()
        migrated = secure_inputs.load_canonical_proposal(legacy["proposal_id"])
        self.assertEqual(migrated["schema_version"], 2)
        self.assertEqual(target.read_bytes(), before)

    def test_internal_identity_and_case_ambiguous_ancestor_are_rejected(self):
        target = self.proposal_dir / "proposal.json"
        wrong = copy.deepcopy(VALID_PROPOSAL)
        wrong["proposal_id"] = "p-20260912T000000Z-deadbeef"
        private_json(target, wrong)
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "proposal_identity_mismatch"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        private_json(target, VALID_PROPOSAL)
        duplicate = self.root / "Runtime"
        duplicate.mkdir(mode=0o700)
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "case_ambiguous_path"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])

    def test_rejects_traversal_case_ambiguity_and_unexpected_content(self):
        for identifier in ("../x", "/tmp/x", VALID_PROPOSAL["proposal_id"].upper()):
            with self.assertRaises(secure_inputs.SecureInputError):
                secure_inputs.load_canonical_proposal(identifier)
        (self.proposal_dir / "unexpected").write_text("x")
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "unexpected_proposal_contents"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])

    def test_rejects_symlink_hardlink_permissions_and_duplicate_json(self):
        target = self.proposal_dir / "proposal.json"
        backup = self.proposal_dir / "saved"
        target.rename(backup)
        target.symlink_to(backup.name)
        with self.assertRaises(secure_inputs.SecureInputError):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        target.unlink()
        os.link(backup, target)
        with self.assertRaises(secure_inputs.SecureInputError):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        target.unlink(); backup.rename(target); target.chmod(0o666)
        with self.assertRaises(secure_inputs.SecureInputError):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        target.chmod(0o600)
        target.write_text('{"schema_version":2,"schema_version":1}', encoding="utf-8")
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "invalid_proposal_json"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        policy_path = self.root / "director_conf" / secure_inputs.CANONICAL_POLICY_NAME
        policy_path.write_text('{"schema_version":2,"schema_version":1}', encoding="utf-8")
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "policy_validation_failed"):
            secure_inputs.load_canonical_policy()

    def test_concurrent_replacement_and_unsafe_intermediate_fail_closed(self):
        target = self.proposal_dir / "proposal.json"
        replacement = self.root / ".replacement"
        private_json(replacement, VALID_PROPOSAL)
        original_read = os.read
        changed = False
        def replace_after_open(descriptor, size):
            nonlocal changed
            data = original_read(descriptor, size)
            if not changed:
                changed = True
                os.replace(replacement, target)
            return data
        with patch.object(os, "read", side_effect=replace_after_open),                 self.assertRaisesRegex(secure_inputs.SecureInputError, "input_replaced"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        director = self.root / "runtime/director"
        director.chmod(0o777)
        with self.assertRaisesRegex(secure_inputs.SecureInputError, "unsafe_directory"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])

    def test_size_limits_fail_before_unbounded_read(self):
        with patch.object(secure_inputs, "MAX_PROPOSAL_BYTES", 8),                 self.assertRaisesRegex(secure_inputs.SecureInputError, "unsafe_input_file"):
            secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])

    def test_legacy_validation_is_allowed_but_never_read_or_modified(self):
        legacy = self.proposal_dir / "validation.json"
        legacy.write_bytes(b"legacy bytes")
        legacy.chmod(0o600)
        before = (legacy.read_bytes(), legacy.stat())
        secure_inputs.load_canonical_proposal(VALID_PROPOSAL["proposal_id"])
        after = (legacy.read_bytes(), legacy.stat())
        self.assertEqual(before[0], after[0])
        self.assertEqual((before[1].st_ino, before[1].st_size, before[1].st_mtime_ns),
                         (after[1].st_ino, after[1].st_size, after[1].st_mtime_ns))


class LockAndOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "project"
        prepare_secure_project(self.root)
        self.root_patch = patch.object(secure_inputs, "TRUSTED_PROJECT_ROOT", self.root)
        self.root_patch.start(); self.addCleanup(self.root_patch.stop)

    def test_lock_is_persistent_private_and_recoverable(self):
        from station_director import validation_coordinator as coordinator
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE((self.root / "runtime").stat().st_mode), 0o755)
        self.assertEqual(
            stat.S_IMODE((self.root / "runtime/director").stat().st_mode), 0o700
        )
        with _validation_lock(secure_inputs):
            lock = self.root / "runtime/director" / coordinator.LOCK_NAME
            self.assertTrue(lock.is_file())
            self.assertEqual(lock.stat().st_mode & 0o777, 0o600)
        self.assertTrue(lock.exists())
        with _validation_lock(secure_inputs):
            pass

    def test_mode_775_ancestors_fail_before_lock_creation(self):
        from station_director import validation_coordinator as coordinator

        runtime = self.root / "runtime"
        director = runtime / "director"
        lock = director / coordinator.LOCK_NAME
        self.assertFalse(lock.exists())
        for directory in (self.root, runtime, director):
            directory.chmod(0o775)

        with self.assertRaisesRegex(
            secure_inputs.SecureInputError, "unsafe_directory"
        ):
            with _validation_lock(secure_inputs):
                self.fail("unsafe lock unexpectedly acquired")
        self.assertFalse(lock.exists())

        with patch(
            "station_director.isolation.check_invocation_context",
            return_value=(True, "verified SSH"),
        ):
            outcome = coordinator.validate_saved_proposal(
                VALID_PROPOSAL["proposal_id"]
            )
        self.assertEqual(outcome.state, "rejected")
        self.assertEqual(outcome.failure_code, "validation_lock_unsafe")
        self.assertFalse(lock.exists())

    def test_busy_lock_and_unsafe_lock_are_distinct(self):
        from station_director import validation_coordinator as coordinator
        lock = self.root / "runtime/director" / coordinator.LOCK_NAME
        lock.write_bytes(b""); lock.chmod(0o600)
        descriptor = os.open(lock, os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            with patch.object(coordinator, "LOCK_WAIT_SECONDS", 0.0), self.assertRaisesRegex(Exception, "validation_lock_busy"):
                with _validation_lock(secure_inputs):
                    pass
        finally:
            os.close(descriptor)
        lock.chmod(0o644)
        with self.assertRaisesRegex(Exception, "validation_lock_unsafe"):
            with _validation_lock(secure_inputs):
                pass

    def test_outcome_rejects_contradictions_and_renderer_is_bounded(self):
        with self.assertRaises(ValueError):
            CoordinatorOutcome(state="passed", scheduler_invoked=(False, False))
        outcome = CoordinatorOutcome(state="rejected", phase="proposal",
                                     failure_code="invalid_proposal_id")
        self.assertEqual(render_cli_outcome(outcome),
                         "Validation rejected: invalid_proposal_id\n")

    def test_signal_scope_restores_handlers_and_second_signal_is_deferred(self):
        previous = {item: signal.getsignal(item) for item in
                    (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        scope = _CancellationScope()
        with self.assertRaises(Exception):
            with scope:
                scope._handle(signal.SIGTERM, None)
        self.assertEqual(previous, {item: signal.getsignal(item) for item in previous})
        scope.requested = True
        self.assertIsNone(scope._handle(signal.SIGTERM, None))


    def test_stale_recovery_uses_exact_unit_and_reviewed_cleanup(self):
        from station_director import isolation
        staging = Path(self.temp.name) / "staging"
        staging.mkdir(mode=0o700)
        stage = staging / "fs42-i-123456abcdef"
        stage.mkdir(mode=0o700)
        for name in (".active", "native-single-run.request.json"):
            target = stage / name
            target.write_bytes(b"{}")
            target.chmod(0o600)
        old = time.time() - isolation.STAGING_MAX_AGE_SECONDS - 10
        os.utime(stage, (old, old))
        def remove(path):
            self.assertEqual(path, stage)
            for child in path.iterdir():
                child.unlink()
            path.rmdir()
            return True, "removed"
        with patch.object(isolation, "STAGING_PARENT", staging),                 patch.object(isolation, "cleanup_unit", return_value=(True, "absent")) as unit,                 patch.object(isolation, "cleanup_staging_directory", side_effect=remove) as cleanup:
            _recover_stale_stages(isolation)
        unit.assert_called_once_with("fs42-native-123456abcdef.service")
        cleanup.assert_called_once()
        self.assertFalse(stage.exists())

    def test_stale_recovery_skips_locked_and_quarantines_live_unit(self):
        from station_director import isolation
        staging = Path(self.temp.name) / "staging-two"
        staging.mkdir(mode=0o700)
        stage = staging / "fs42-i-abcdef123456"
        stage.mkdir(mode=0o700)
        marker = stage / ".active"; marker.write_bytes(b""); marker.chmod(0o600)
        request = stage / "native-single-run.request.json"; request.write_bytes(b"{}"); request.chmod(0o600)
        old = time.time() - isolation.STAGING_MAX_AGE_SECONDS - 10
        os.utime(stage, (old, old))
        descriptor = os.open(marker, os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            with patch.object(isolation, "STAGING_PARENT", staging),                     patch.object(isolation, "cleanup_unit") as unit:
                _recover_stale_stages(isolation)
            unit.assert_not_called()
        finally:
            os.close(descriptor)
        with patch.object(isolation, "STAGING_PARENT", staging),                 patch.object(isolation, "cleanup_unit", return_value=(False, "still active")),                 self.assertRaisesRegex(CoordinatorError, "stale_unit_not_absent"):
            _recover_stale_stages(isolation)
        self.assertTrue((stage / ".quarantine").is_file())



class CoordinatorFlowTests(unittest.TestCase):
    def test_safe_c1_diagnostic_reaches_bounded_cli(self):
        outcome = CoordinatorOutcome(
            state="failed", proposal_id=VALID_PROPOSAL["proposal_id"],
            run_id=RUN_ID, validation_status="failed", phase="run_1",
            failure_code="c1_run_failed", scheduler_invoked=(False, False),
            c1_run=1, c1_domain="worker_verification", c1_phase="snapshot",
            c1_code="original_database_verification_failed",
            c1_fingerprint_category="original_logical_database",
            c1_launcher_outcome="nonzero_exit", c1_stdout_bytes=12,
            c1_stderr_bytes=34, c1_stdout_truncated=True,
            c1_stderr_truncated=False)
        rendered = render_cli_outcome(outcome)
        self.assertIn("C1 run: 1", rendered)
        self.assertIn("C1 code: original_database_verification_failed", rendered)
        self.assertIn("C1 fingerprint category: original_logical_database", rendered)
        self.assertIn("C1 output bytes: stdout=12 stderr=34", rendered)
        self.assertNotIn("/", rendered)

    def test_autobump_c1_codes_reach_cli_without_descriptor_data(self):
        for code, phase, invoked in (
            ("autobump_subprocess_required", "configuration", False),
            ("autobump_subprocess_blocked", "scheduler", True),
            ("autobump_selected", "scheduler", True),
            ("invalid_playback_descriptor", "scheduler", True),
            ("catalog_metadata_unavailable", "catalog", False),
        ):
            outcome = CoordinatorOutcome(
                state="failed", proposal_id=VALID_PROPOSAL["proposal_id"],
                run_id=RUN_ID, validation_status="failed", phase="run_1",
                failure_code="c1_run_failed",
                scheduler_invoked=(invoked, False),
                c1_run=1, c1_domain=DIAGNOSTIC_RULES[code][0],
                c1_phase=phase, c1_code=code, c1_channel_number=2,
                c1_launcher_outcome="nonzero_exit", c1_stdout_bytes=0,
                c1_stderr_bytes=0, c1_stdout_truncated=False,
                c1_stderr_truncated=False,
            )
            rendered = render_cli_outcome(outcome)
            self.assertIn(f"C1 code: {code}", rendered)
            self.assertIn("C1 channel: 2", rendered)
            self.assertNotIn("opaque-secret-suffix", rendered)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "project"
        prepare_secure_project(self.root)
        self.secure_patch = patch.object(secure_inputs, "TRUSTED_PROJECT_ROOT", self.root)
        self.secure_patch.start(); self.addCleanup(self.secure_patch.stop)
        from station_director import reporting, validation_coordinator as coordinator
        self.coordinator = coordinator
        self.reporting = reporting
        for patcher in (
            patch.object(coordinator, "TRUSTED_PROJECT_ROOT", self.root),
            patch.object(reporting, "PROJECT_ROOT", self.root),
            patch.object(reporting, "VALIDATIONS_ROOT", self.root / "runtime/director/validations"),
            patch.object(validation_control, "SCHEDULE_VALIDATION_ENABLED", True),
            patch("station_director.isolation.check_invocation_context", return_value=(True, "verified SSH")),
            patch.object(coordinator, "_recover_stale_stages"),
        ):
            patcher.start(); self.addCleanup(patcher.stop)

    def _run(self, result):
        result["comparison_id"] = RUN_ID
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("station_director.dual_run.run_dual_comparison", return_value=result), \
                patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        return code, stdout.getvalue(), stderr.getvalue()

    def test_saved_migrated_noop_is_rejected_before_policy_run_or_report(self):
        legacy = base_proposal(version=1)
        proposal_path = self.root / "runtime/director/proposals" / legacy["proposal_id"] / "proposal.json"
        private_json(proposal_path, legacy)
        legacy_report = proposal_path.with_name("validation.json")
        legacy_report.write_bytes(b'{"legacy":true}\n')
        legacy_report.chmod(0o600)
        before = {
            proposal_path: (proposal_path.read_bytes(), proposal_path.stat()),
            legacy_report: (legacy_report.read_bytes(), legacy_report.stat()),
        }
        validations = self.root / "runtime/director/validations"
        with patch.object(secure_inputs, "load_canonical_policy") as policy, \
                patch.object(self.reporting, "create_validation_run_id") as run_id, \
                patch.object(self.coordinator, "_recover_stale_stages") as stale, \
                patch("station_director.dual_run.run_dual_comparison") as dual, \
                patch.object(self.reporting, "publish_validation_report") as publish:
            outcome = self.coordinator.validate_saved_proposal(legacy["proposal_id"])
        self.assertEqual(outcome.state, "rejected")
        self.assertEqual(outcome.failure_code, "proposal_has_no_effects")
        self.assertEqual(outcome.phase, "proposal")
        self.assertEqual(outcome.scheduler_invoked, (False, False))
        policy.assert_not_called()
        run_id.assert_not_called()
        stale.assert_not_called()
        dual.assert_not_called()
        publish.assert_not_called()
        self.assertFalse(validations.exists())
        for path, (raw, metadata) in before.items():
            after = path.stat()
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(
                (after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode),
                (metadata.st_ino, metadata.st_size, metadata.st_mtime_ns,
                 metadata.st_mode),
            )

    def test_success_publishes_immutable_report_and_latest(self):
        result = valid_success_result(Path(self.temp.name))
        code, stdout, stderr = self._run(result)
        self.assertEqual(code, 0, (stdout, stderr))
        self.assertEqual(stderr, "")
        self.assertIn("Validation: PASS", stdout)
        reports = list((self.root / "runtime/director/validations" /
                        VALID_PROPOSAL["proposal_id"]).glob("v-*"))
        self.assertEqual(len(reports), 1)
        self.assertTrue((reports[0] / "validation.json").is_file())
        self.assertTrue((reports[0] / "validation.txt").is_file())
        self.assertTrue((reports[0].parent / "latest.json").is_file())

    def test_validation_failure_stays_failed_with_durable_report(self):
        result = self.coordinator._minimal_c2_failure(
            "placeholder", "source_capture_failed", "capture",
            capture_failure_kind="database_snapshot")
        code, stdout, stderr = self._run(result)
        self.assertEqual(code, 1)
        self.assertIn("Validation: FAIL", stdout)
        self.assertIn("Capture failure kind: database_snapshot", stdout)
        self.assertEqual(stderr, "")
        self.assertIn("Report: validations/", stdout)

    def test_source_capture_outcomes_require_an_allowlisted_kind(self):
        with self.assertRaises(ValueError):
            self.coordinator._minimal_c2_failure(
                RUN_ID, "source_capture_failed")
        with self.assertRaises(ValueError):
            CoordinatorOutcome(
                state="failed", proposal_id=VALID_PROPOSAL["proposal_id"],
                run_id=RUN_ID, validation_status="failed", phase="capture",
                failure_code="source_capture_failed")
        outcome = CoordinatorOutcome(
            state="failed", proposal_id=VALID_PROPOSAL["proposal_id"],
            run_id=RUN_ID, validation_status="failed", phase="capture",
            failure_code="source_capture_failed",
            capture_failure_kind="configuration_snapshot")
        rendered = render_cli_outcome(outcome)
        self.assertIn("Capture failure kind: configuration_snapshot", rendered)
        self.assertNotIn("/", rendered)
        self.assertNotIn("password", rendered.casefold())

        with self.assertRaises(ValueError):
            CoordinatorOutcome(
                state="failed", proposal_id=VALID_PROPOSAL["proposal_id"],
                run_id=RUN_ID, validation_status="failed", phase="capture",
                failure_code="source_capture_failed",
                capture_failure_kind="single_run_finalization")

    def test_finalization_run_and_subphase_reach_v4_report_text_and_cli(self):
        result = self.coordinator._minimal_c2_failure(
            RUN_ID, "source_capture_failed", "capture",
            capture_failure_kind="single_run_finalization",
            capture_run=2, finalization_subphase="proposal_projection")
        code, stdout, stderr = self._run(result)
        self.assertEqual(code, 1)
        self.assertEqual(stderr, "")
        self.assertIn("Capture failure kind: single_run_finalization", stdout)
        self.assertIn("Capture run: 2", stdout)
        self.assertIn("Finalization subphase: proposal_projection", stdout)
        report_directories = list((
            self.root / "runtime/director/validations" /
            VALID_PROPOSAL["proposal_id"]).glob("v-*"))
        self.assertEqual(len(report_directories), 1)
        document = json.loads(
            (report_directories[0] / "validation.json").read_text(encoding="utf-8"))
        self.assertEqual(document["schema_version"], 5)
        finding = document["findings"]["errors"][0]
        self.assertEqual(finding["capture_run"], 2)
        self.assertEqual(finding["finalization_subphase"], "proposal_projection")
        text = (report_directories[0] / "validation.txt").read_text(encoding="utf-8")
        self.assertIn("single_run_finalization", text)
        self.assertIn("proposal_projection", text)

    def test_explicit_single_run_preparation_code_reaches_bounded_cli(self):
        result = self.coordinator._minimal_c2_failure(
            RUN_ID, "duplicate_stage_file", "capture",
            capture_run=1, finalization_subphase="request_publication")
        code, stdout, stderr = self._run(result)
        self.assertEqual(code, 1)
        self.assertIn("Failure: duplicate_stage_file", stdout)
        self.assertIn("Capture run: 1", stdout)
        self.assertIn("Finalization subphase: request_publication", stdout)
        self.assertNotIn("/etc", stdout)
        self.assertNotIn("password", stdout.casefold())
        self.assertEqual(stderr, "")

    def test_unverified_invalid_and_lock_busy_create_no_report(self):
        validations = self.root / "runtime/director/validations"
        with patch("station_director.isolation.check_invocation_context", return_value=(False, "no")):
            code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(code, 1)
        self.assertFalse(validations.exists())
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = cli.main(["schedule", "validate", "../bad"])
        self.assertEqual(code, 1)
        self.assertFalse(validations.exists())

    def test_nondefault_policy_rejected_before_open(self):
        stderr = io.StringIO()
        with patch("pathlib.Path.open") as opened, contextlib.redirect_stderr(stderr):
            code = cli.main(["--policy", "/does/not/exist", "schedule", "validate",
                             VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(code, 2)
        opened.assert_not_called()
        self.assertIn("canonical policy", stderr.getvalue())

    def test_public_cli_output_never_exposes_absolute_paths_or_exception_text(self):
        result = self.coordinator._minimal_c2_failure("placeholder", "internal_error")
        code, stdout, unused = self._run(result)
        self.assertEqual(code, 1)
        self.assertNotIn(str(self.root), stdout)
        self.assertNotIn("Traceback", stdout)


    def test_stale_cleanup_failure_is_reported_without_entering_c2(self):
        stdout = io.StringIO()
        with patch.object(self.coordinator, "_recover_stale_stages",
                          side_effect=CoordinatorError("stale_unit_not_absent", "capture")),                 patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID),                 patch("station_director.dual_run.run_dual_comparison") as dual,                 contextlib.redirect_stdout(stdout):
            code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(code, 1)
        dual.assert_not_called()
        self.assertIn("Failure: stale_unit_not_absent", stdout.getvalue())
        self.assertIn("Report: validations/", stdout.getvalue())

    def test_enabled_call_order_is_fixed(self):
        events = []
        controls = {}
        result = self.coordinator._minimal_c2_failure(
            RUN_ID, "source_capture_failed",
            capture_failure_kind="database_snapshot")
        publication = {"publication_state": "published_durable",
                       "proposal_id": VALID_PROPOSAL["proposal_id"], "run_id": RUN_ID,
                       "validation_json_digest": "a" * 64,
                       "latest": {"status": "updated", "code": None,
                                  "publication_state": "latest_durable"}}
        @contextlib.contextmanager
        def locked(unused):
            events.append("lock")
            yield
            events.append("unlock")
        with patch("station_director.isolation.check_invocation_context",
                   side_effect=lambda: (events.append("invocation") or (True, "ok"))),                 patch.object(secure_inputs, "validate_proposal_id",
                             side_effect=lambda value: events.append("lexical")),                 patch.object(self.coordinator, "_validation_lock", side_effect=locked),                 patch.object(secure_inputs, "load_canonical_proposal",
                             side_effect=lambda value: (events.append("proposal") or copy.deepcopy(VALID_PROPOSAL))),                 patch.object(secure_inputs, "load_canonical_policy",
                             side_effect=lambda: (events.append("policy") or POLICY)),                 patch.object(self.reporting, "create_validation_run_id",
                             side_effect=lambda: (events.append("run_id") or RUN_ID)),                 patch.object(self.coordinator, "_recover_stale_stages",
                             side_effect=lambda value: events.append("stale")),                 patch("station_director.dual_run.run_dual_comparison",
                      side_effect=lambda *args, **kwargs: (events.append("c2") or controls.update(kwargs) or result)),                 patch.object(self.reporting, "build_validation_report",
                             side_effect=lambda *args: (events.append("projection") or Mock())),                 patch.object(self.reporting, "publish_validation_report",
                             side_effect=lambda value: (events.append("publication") or publication)):
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.state, "failed")
        self.assertEqual(events, ["invocation", "lexical", "lock", "proposal", "policy",
                                  "run_id", "stale", "c2", "projection", "publication", "unlock"])
        self.assertEqual(
            controls["admission_cutoff"] - controls["control_started"], 6900)
        self.assertEqual(
            controls["control_deadline"] - controls["control_started"], 7200)

    def test_lock_busy_and_policy_rejection_create_no_report(self):
        validations = self.root / "runtime/director/validations"
        with patch.object(self.coordinator, "_validation_lock",
                          side_effect=CoordinatorError("validation_lock_busy", "lock")),                 patch.object(self.reporting, "publish_validation_report") as publish:
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.failure_code, "validation_lock_busy")
        publish.assert_not_called(); self.assertFalse(validations.exists())
        with patch.object(secure_inputs, "load_canonical_policy",
                          side_effect=secure_inputs.SecureInputError("bad")),                 patch.object(self.reporting, "publish_validation_report") as publish:
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.failure_code, "policy_rejected")
        publish.assert_not_called(); self.assertFalse(validations.exists())

    def test_interruption_before_c2_result_publishes_bounded_failure_once(self):
        stdout = io.StringIO()
        with patch("station_director.dual_run.run_dual_comparison", side_effect=KeyboardInterrupt),                 patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID),                 contextlib.redirect_stdout(stdout):
            code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(code, 1)
        self.assertIn("Failure: validation_interrupted", stdout.getvalue())
        self.assertIn("Report: validations/", stdout.getvalue())

    def test_immutable_publication_failure_is_bounded_and_latest_untouched(self):
        result = valid_success_result(Path(self.temp.name)); result["comparison_id"] = RUN_ID
        error = self.reporting.ReportError("publication_failed", "raw /etc/passwd",
                                           publication_state="not_published")
        with patch("station_director.dual_run.run_dual_comparison", return_value=result),                 patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID),                 patch.object(self.reporting, "publish_validation_report", side_effect=error):
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.state, "failed")
        self.assertEqual(outcome.failure_code, "publication_failed")
        self.assertFalse(outcome.report_published)
        self.assertIsNone(outcome.latest_status)

    def test_projection_failure_uses_one_bounded_failure_report_and_preserves_truth(self):
        result = valid_success_result(Path(self.temp.name))
        result["comparison_id"] = RUN_ID
        original = self.reporting.build_validation_report
        calls = []
        def build(proposal, c2_result, run_id, completed_at):
            calls.append(c2_result)
            if len(calls) == 1:
                raise ValueError("unsafe raw failure")
            return original(proposal, c2_result, run_id, completed_at)
        stdout = io.StringIO()
        with patch("station_director.dual_run.run_dual_comparison", return_value=result), \
                patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID), \
                patch.object(self.reporting, "build_validation_report", side_effect=build), \
                contextlib.redirect_stdout(stdout):
            code = cli.main(["schedule", "validate", VALID_PROPOSAL["proposal_id"]])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("Validation: FAIL", stdout.getvalue())
        self.assertIn("Report: not published", stdout.getvalue())
        self.assertNotIn("unsafe raw failure", stdout.getvalue())

    def test_publication_interruption_preserves_publication_state(self):
        result = valid_success_result(Path(self.temp.name))
        result["comparison_id"] = RUN_ID
        error = self.reporting.ReportError(
            "validation_interrupted", "fixed",
            publication_state="published_not_durable",
        )
        with patch("station_director.dual_run.run_dual_comparison", return_value=result), \
                patch.object(self.reporting, "create_validation_run_id", return_value=RUN_ID), \
                patch.object(self.reporting, "publish_validation_report", side_effect=error):
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.state, "interrupted")
        self.assertTrue(outcome.report_published)
        self.assertFalse(outcome.report_durable)
        self.assertEqual(outcome.scheduler_invoked, (True, True))

    def test_latest_warning_does_not_change_success(self):
        result = valid_success_result(Path(self.temp.name))
        result["comparison_id"] = RUN_ID
        publication = {
            "publication_state": "published_durable",
            "proposal_id": VALID_PROPOSAL["proposal_id"],
            "run_id": RUN_ID,
            "validation_json_digest": "a" * 64,
            "latest": {"status": "warning", "code": "validation_interrupted",
                       "publication_state": "latest_not_replaced"},
        }
        with patch("station_director.dual_run.run_dual_comparison", return_value=result),                 patch.object(self.reporting, "publish_validation_report", return_value=publication),                 patch.object(self.reporting, "create_validation_run_id",
                             return_value=publication["run_id"]):
            outcome = self.coordinator.validate_saved_proposal(VALID_PROPOSAL["proposal_id"])
        self.assertEqual(outcome.state, "passed")
        self.assertEqual(outcome.latest_status, "warning")



class GenuineGateEnabledIntegrationTests(unittest.TestCase):
    def test_cli_traverses_genuine_two_worker_lifecycle_and_publication(self):
        harness = NativeTwoProcessIntegrationTests(methodName="runTest")
        with tempfile.TemporaryDirectory() as directory:
            outer = Path(directory)

            def execute(project, media, proposal, policy):
                prepare_secure_project(project, proposal, policy)
                from station_director import reporting, validation_coordinator as coordinator
                with patch.object(secure_inputs, "TRUSTED_PROJECT_ROOT", project), \
                        patch.object(coordinator, "TRUSTED_PROJECT_ROOT", project), \
                        patch.object(coordinator, "CANONICAL_MEDIA_ROOT", media), \
                        patch.object(reporting, "PROJECT_ROOT", project), \
                        patch.object(reporting, "VALIDATIONS_ROOT", project / "runtime/director/validations"), \
                        patch.object(validation_control, "SCHEDULE_VALIDATION_ENABLED", True), \
                        patch("station_director.isolation.check_invocation_context", return_value=(True, "test SSH")), \
                        patch.object(coordinator, "_recover_stale_stages"):
                    stdout = io.StringIO()
                    with contextlib.redirect_stdout(stdout):
                        exit_code = cli.main(["schedule", "validate", proposal["proposal_id"]])
                    report_root = project / "runtime/director/validations" / proposal["proposal_id"]
                    return {"exit_code": exit_code, "stdout": stdout.getvalue(),
                            "reports": [item.name for item in report_root.glob("v-*")]}

            observed, details = harness._complete_dual_run(outer, execute=execute)
        self.assertEqual(observed["exit_code"], 0, observed)
        self.assertIn("Validation: PASS", observed["stdout"])
        self.assertEqual(len(observed["reports"]), 1)
        self.assertEqual(len(details), 2)
        self.assertNotEqual(details[0]["pid"], details[1]["pid"])
        self.assertNotEqual(details[0]["stage"], details[1]["stage"])


class StaticBoundaryTests(unittest.TestCase):
    def test_master_control_is_dependency_free_and_enabled(self):
        source = Path("station_director/validation_control.py").read_text(encoding="utf-8")
        self.assertNotIn("import ", source)
        self.assertEqual(source.count("SCHEDULE_VALIDATION_ENABLED = "), 1)
        self.assertIn("SCHEDULE_VALIDATION_ENABLED = True", source)
        self.assertIs(validation_control.SCHEDULE_VALIDATION_ENABLED, True)
        self.assertEqual(validation_control.DISABLED_MESSAGE,
                         "Phase 3 validation is not yet enabled")

    def test_public_gate_precedes_dynamic_coordinator_import(self):
        source = Path("station_director/cli.py").read_text(encoding="utf-8")
        gate = source.index("if not validation_control.SCHEDULE_VALIDATION_ENABLED")
        coordinator = source.index("from station_director.validation_coordinator")
        self.assertLess(gate, coordinator)
        self.assertNotIn("dual_run", Path("station_director/validation.py").read_text())
        self.assertNotIn("reporting", Path("station_director/validation.py").read_text())

    def test_clean_host_imports_load_no_fs42(self):
        import subprocess
        completed = subprocess.run([
            sys.executable, "-c",
            "import sys; import station_director.cli, station_director.validation_coordinator, station_director.secure_validation_inputs; print([n for n in sys.modules if n == 'fs42' or n.startswith('fs42.')])",
        ], capture_output=True, text=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "[]")


if __name__ == "__main__":
    unittest.main()
