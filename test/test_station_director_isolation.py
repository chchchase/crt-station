import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from station_director import isolation, isolation_probe


def completed(argv, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


class FakeRunner:
    def __init__(self, mode="success"):
        self.mode = mode
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        if argv[0] == "systemd-run":
            if self.mode == "timeout":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            bind_index = argv.index("--bind")
            stage = Path(argv[bind_index + 1])
            probe_index = argv.index("/project/station_director/isolation_probe.py")
            run_id = argv[probe_index + 1]
            results = {
                name: {"passed": True, "detail": f"verified {name}"}
                for name in isolation.PROBE_RESULTS
            }
            payload = {
                "schema_version": 1,
                "run_id": run_id,
                "overall_pass": True,
                "results": results,
            }
            if self.mode == "malformed":
                (stage / isolation.PROBE_OUTPUT).write_text("{not-json")
                return completed(argv)
            if self.mode == "incomplete":
                payload["results"].pop(isolation.PROBE_RESULTS[-1])
            if self.mode == "contradictory":
                payload["overall_pass"] = False
            (stage / isolation.PROBE_OUTPUT).write_text(json.dumps(payload))
            return completed(argv, 1 if self.mode == "failure" else 0, stderr="bubblewrap failed" if self.mode == "failure" else "")
        if argv[:3] == ["systemctl", "--user", "stop"] and self.mode == "cleanup_failure":
            return completed(argv, 1, stderr="permission denied")
        if argv[:3] == ["systemctl", "--user", "show"]:
            if self.mode == "cleanup_failure":
                return completed(argv, 0, stdout="loaded\n")
            return completed(argv, 1, stderr="Unit could not be found")
        return completed(argv, 1, stderr="Unit not loaded")


class IsolationTests(unittest.TestCase):
    def test_authoritative_unit_termination_classification(self):
        def payload(result, code, status, started="123", active="failed", sub="failed"):
            return "\n".join((
                "LoadState=loaded", f"ActiveState={active}", f"SubState={sub}",
                f"Result={result}", f"ExecMainCode={code}",
                f"ExecMainStatus={status}",
                f"ExecMainStartTimestampMonotonic={started}",
            )) + "\n"
        cases = (
            (payload("success", 1, 0, active="inactive", sub="dead"), "completed", 0, None, True),
            (payload("success", 1, 0, active="active", sub="exited"), "completed", 0, None, True),
            (payload("exit-code", 1, 7), "nonzero_exit", 7, None, True),
            (payload("timeout", 2, 9), "runtime_timeout", None, 9, True),
            (payload("oom-kill", 2, 9), "oom_kill", None, 9, True),
            (payload("signal", 2, 15), "external_signal", None, 15, True),
            (payload("signal", 2, 9), "external_signal", None, 9, True),
            (payload("start-limit-hit", 0, 0, started="0"), "launcher_failure", None, None, False),
        )
        for raw, kind, exit_status, signal_number, started in cases:
            with self.subTest(kind=kind), patch.object(
                isolation.subprocess, "run", return_value=completed([], stdout=raw)
            ):
                evidence = isolation.inspect_unit_termination("fixed.service")
            self.assertTrue(evidence["valid"])
            self.assertEqual(evidence["termination_kind"], kind)
            self.assertEqual(evidence["exit_status"], exit_status)
            self.assertEqual(evidence["signal"], signal_number)
            self.assertIs(evidence["main_process_started"], started)

        running = payload("success", 0, 0, active="active", sub="running")
        with patch.object(
            isolation.subprocess, "run", return_value=completed([], stdout=running)
        ):
            ordinary = isolation.inspect_unit_termination("fixed.service")
            polling = isolation.inspect_unit_termination(
                "fixed.service", allow_running=True)
            timed_out = isolation.inspect_unit_termination(
                "fixed.service", launcher_timed_out=True)
        self.assertFalse(ordinary["valid"])
        self.assertEqual(ordinary["termination_kind"], "launcher_state_invalid")
        self.assertTrue(polling["valid"])
        self.assertEqual(polling["termination_kind"], "running")
        self.assertTrue(timed_out["valid"])
        self.assertEqual(timed_out["termination_kind"], "outer_watchdog")

    def test_retained_active_exited_process_returns_promptly_with_properties(self):
        running = {
            "valid": True,
            "termination_kind": "running",
            "exit_status": None,
            "signal": None,
            "main_process_started": True,
        }
        terminal = {
            "valid": True,
            "termination_kind": "completed",
            "exit_status": 0,
            "signal": None,
            "main_process_started": True,
        }
        started = time.monotonic()
        with patch.object(
            isolation, "inspect_unit_termination",
            side_effect=(running, terminal),
        ) as inspect, patch.object(isolation, "UNIT_POLL_SECONDS", 0):
            result = isolation._run_bounded(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                "fixed.service", 30, supervise_retained_unit=True,
            )
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result.returncode, 0)
        self.assertFalse(result.timed_out)
        self.assertTrue(result.unit_state_valid)
        self.assertEqual(result.termination_kind, "completed")
        self.assertEqual(result.exit_status, 0)
        self.assertEqual(inspect.call_count, 2)
        inspect.assert_called_with(
            "fixed.service", allow_running=True,
            inspection_timeout=isolation.UNIT_INSPECTION_TIMEOUT_SECONDS,
        )

    def test_retained_active_running_requires_affirmative_local_watchdog(self):
        running = {
            "valid": True,
            "termination_kind": "running",
            "exit_status": None,
            "signal": None,
            "main_process_started": True,
        }
        outer_timeout = {
            **running,
            "termination_kind": "outer_watchdog",
        }
        def inspect(unused_unit, **kwargs):
            return outer_timeout if kwargs.get("launcher_timed_out") else running
        with patch.object(
            isolation, "inspect_unit_termination", side_effect=inspect
        ):
            result = isolation._run_bounded(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                "fixed.service", 0.01, supervise_retained_unit=True,
            )
        self.assertTrue(result.timed_out)
        self.assertEqual(result.returncode, 124)
        self.assertEqual(result.termination_kind, "outer_watchdog")
        self.assertTrue(result.main_process_started)

    def test_missing_malformed_and_contradictory_unit_properties_fail_closed(self):
        values = (
            "LoadState=loaded\n",
            "LoadState=loaded\nLoadState=loaded\n",
            "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=success\n"
            "ExecMainCode=1\nExecMainStatus=9\nExecMainStartTimestampMonotonic=0\n",
            "LoadState=loaded\nActiveState=active\nSubState=running\nResult=success\n"
            "ExecMainCode=0\nExecMainStatus=0\nExecMainStartTimestampMonotonic=0\n",
            "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=timeout\n"
            "ExecMainCode=1\nExecMainStatus=9\nExecMainStartTimestampMonotonic=123\n",
            "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=signal\n"
            "ExecMainCode=2\nExecMainStatus=255\nExecMainStartTimestampMonotonic=123\n",
        )
        for raw in values:
            with self.subTest(raw=raw), patch.object(
                isolation.subprocess, "run", return_value=completed([], stdout=raw)
            ):
                evidence = isolation.inspect_unit_termination("fixed.service")
            self.assertFalse(evidence["valid"])
            self.assertEqual(evidence["termination_kind"], "launcher_state_invalid")

        with patch.object(
            isolation.subprocess, "run",
            return_value=completed([], returncode=1,
                                   stderr="Unit could not be found"),
        ):
            evidence = isolation.inspect_unit_termination("fixed.service")
        self.assertFalse(evidence["valid"])
        self.assertIs(evidence["main_process_started"], None)

    def test_systemd_run_exec_failure_is_the_only_launcher_local_never_started_proof(self):
        with patch.object(
            isolation.subprocess, "Popen", side_effect=FileNotFoundError("missing")
        ):
            result = isolation._run_bounded(
                ["systemd-run", "--user", "--", "fixed"], "fixed.service", 1)
        self.assertFalse(result.launcher_executed)
        self.assertTrue(result.unit_state_valid)
        self.assertIs(result.main_process_started, False)
        self.assertEqual(result.termination_kind, "launcher_failure")

    def test_cleanup_uses_one_deadline_and_attempts_the_production_maximum(self):
        calls = []
        def run(argv, **kwargs):
            calls.append((tuple(argv), kwargs["timeout"]))
            if argv[:3] == ["systemctl", "--user", "show"]:
                return completed(argv, 0, stdout="loaded\n")
            return completed(argv, 1, stderr="failed")
        with patch.object(isolation.subprocess, "run", side_effect=run), patch.object(
            isolation.time, "monotonic", side_effect=range(1, 20)
        ):
            ok, unused_detail = isolation.cleanup_unit("fixed.service", deadline=12)
        self.assertFalse(ok)
        self.assertEqual(len(calls), isolation.MAX_UNIT_CLEANUP_COMMANDS)
        self.assertEqual(len([argv for argv, unused in calls if "show" in argv]), 3)
        self.assertEqual(len([argv for argv, unused in calls if "kill" in argv]), 2)
        self.assertTrue(all(0 < timeout <= isolation.CLEANUP_TIMEOUT_SECONDS
                            for unused, timeout in calls))
        self.assertLess(calls[-1][1], calls[0][1])

    def test_native_timeout_hierarchy_has_no_timer_race(self):
        self.assertLess(isolation.NATIVE_UNIT_HARD_LIMIT_SECONDS,
                        isolation.NATIVE_OUTER_WATCHDOG_SECONDS)
        self.assertEqual(isolation.NATIVE_UNIT_HARD_LIMIT_SECONDS, 2700)
        self.assertEqual(isolation.NATIVE_OUTER_WATCHDOG_SECONDS, 2730)
        source = Path(isolation.__file__).read_text(encoding="utf-8")
        self.assertNotIn('"--collect"', source)
        self.assertIn('"--property=RemainAfterExit=yes"', source)
        self.assertIn("supervise_retained_unit=True", source)

    def run_mocked_preflight(self, directory, mode="success", profile="standard"):
        root = Path(directory)
        (root / "runtime/director").mkdir(parents=True, exist_ok=True)
        staging_parent = root / "tmp"
        staging_parent.mkdir(exist_ok=True)
        runner = FakeRunner(mode)
        def launch(unused_launcher, stage, argv, unit, timeout=30, stage_tmp=False):
            temporary = isolation.prepare_stage_temporary(stage) if stage_tmp else None
            try:
                bwrap = isolation.build_bwrap_command(
                    root, stage, argv, stage_tmp=stage_tmp,
                    verified_temporary=temporary,
                )
            finally:
                if temporary is not None:
                    temporary.close()
            command = [
                "systemd-run", "--property=RestrictAddressFamilies=AF_UNIX",
                *bwrap,
            ]
            try:
                value = runner(command, timeout=timeout)
                return isolation.LaunchResult(unit, value.returncode, value.stdout, value.stderr)
            except subprocess.TimeoutExpired:
                return isolation.LaunchResult(unit, 124, "", "", timed_out=True)
        with patch.object(isolation, "STAGING_PARENT", staging_parent), patch.object(
            isolation, "check_invocation_context", return_value=(True, "test SSH context")
        ), patch.object(isolation.IsolationLauncher, "run", autospec=True, side_effect=launch), patch.object(
            isolation.subprocess, "run", side_effect=runner
        ):
            report, json_path, text_path = isolation.run_preflight(root, profile=profile)
        return report, json_path, text_path, runner, staging_parent

    def test_native_single_run_profile_creates_stage_tmp_before_bind_and_cleans_it(self):
        with tempfile.TemporaryDirectory() as directory:
            report, unused_json, unused_text, runner, staging_parent = self.run_mocked_preflight(
                directory, profile="native-single-run"
            )
            self.assertEqual(report["result"], "PASS")
            self.assertEqual(report["profile"], "native-single-run")
            systemd_call = next(call for call in runner.calls if call[0] == "systemd-run")
            transient_sources = [
                systemd_call[index + 1]
                for index, value in enumerate(systemd_call[:-2])
                if value == "--bind" and systemd_call[index + 2] == "/tmp"
            ]
            self.assertEqual(len(transient_sources), 1)
            self.assertTrue(transient_sources[0].endswith("/transient"))
            self.assertFalse(any(staging_parent.iterdir()))

    def test_native_single_run_launch_failure_still_cleans_stage_tmp(self):
        with tempfile.TemporaryDirectory() as directory:
            report, unused_json, unused_text, unused_runner, staging_parent = self.run_mocked_preflight(
                directory, mode="failure", profile="native-single-run"
            )
            self.assertEqual(report["result"], "FAIL")
            self.assertTrue(report["cleanup"]["staging"]["passed"])
            self.assertFalse(any(staging_parent.iterdir()))

    def test_success_requires_launcher_probe_reports_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            report, json_path, text_path, runner, staging_parent = self.run_mocked_preflight(directory)
            self.assertEqual(report["result"], "PASS")
            self.assertEqual(report["transient_unit"]["exit_status"], 0)
            self.assertEqual(report["bubblewrap"]["exit_status"], 0)
            self.assertTrue(all(item["passed"] for item in report["probe_results"].values()))
            self.assertTrue(report["cleanup"]["unit"]["passed"])
            self.assertTrue(report["cleanup"]["staging"]["passed"])
            self.assertTrue(report["retained_reports"]["json_complete"])
            self.assertTrue(report["retained_reports"]["text_complete"])
            self.assertTrue(json_path.is_file())
            self.assertTrue(text_path.is_file())
            self.assertFalse(any(staging_parent.iterdir()))
            systemd_call = next(call for call in runner.calls if call[0] == "systemd-run")
            self.assertIn("--property=RestrictAddressFamilies=AF_UNIX", systemd_call)
            self.assertNotIn("--unshare-net", systemd_call)
            self.assertIn("--clearenv", systemd_call)
            self.assertIn("PYTHONDONTWRITEBYTECODE", systemd_call)
            self.assertNotIn("/run", systemd_call)
            self.assertNotIn("/home", systemd_call)

    def test_nonzero_bubblewrap_status_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            report, unused_json, unused_text, unused_runner, unused_stage = self.run_mocked_preflight(directory, "failure")
            self.assertEqual(report["result"], "FAIL")
            self.assertFalse(report["transient_unit"]["passed"])
            self.assertFalse(report["bubblewrap"]["passed"])

    def test_timeout_fails_and_still_cleans(self):
        with tempfile.TemporaryDirectory() as directory:
            report, unused_json, unused_text, unused_runner, staging_parent = self.run_mocked_preflight(directory, "timeout")
            self.assertEqual(report["result"], "FAIL")
            self.assertTrue(report["transient_unit"]["timed_out"])
            self.assertTrue(report["cleanup"]["unit"]["passed"])
            self.assertFalse(any(staging_parent.iterdir()))

    def test_malformed_incomplete_and_contradictory_probe_output_fail(self):
        for mode in ("malformed", "incomplete", "contradictory"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                report, unused_json, unused_text, unused_runner, unused_stage = self.run_mocked_preflight(directory, mode)
                self.assertEqual(report["result"], "FAIL")
                self.assertTrue(report["probe_output_error"])

    def test_real_cleanup_error_quarantines_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            report, unused_json, unused_text, unused_runner, staging_parent = self.run_mocked_preflight(directory, "cleanup_failure")
            self.assertEqual(report["result"], "FAIL")
            self.assertFalse(report["cleanup"]["unit"]["passed"])
            self.assertFalse(report["cleanup"]["staging"]["passed"])
            retained = list(staging_parent.iterdir())
            self.assertEqual(len(retained), 1)

    def test_stale_cleanup_checks_shape_owner_symlinks_and_active_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            stale = parent / "fs42-i-aaaaaaaaaaaa"
            active = parent / "fs42-i-bbbbbbbbbbbb"
            wrong_name = parent / "fs42-i-nothex"
            stale.mkdir(); active.mkdir(); wrong_name.mkdir()
            (stale / ".active").write_text("")
            active_marker = (active / ".active").open("w")
            fcntl.flock(active_marker.fileno(), fcntl.LOCK_EX)
            link = parent / "fs42-i-cccccccccccc"
            link.symlink_to(stale, target_is_directory=True)
            old = time.time() - isolation.STAGING_MAX_AGE_SECONDS - 60
            os.utime(stale, (old, old)); os.utime(active, (old, old)); os.utime(wrong_name, (old, old))
            with patch.object(isolation, "STAGING_PARENT", parent):
                ok, failures = isolation.cleanup_stale_directories(now=time.time())
            active_marker.close()
            self.assertTrue(ok, failures)
            self.assertFalse(stale.exists())
            self.assertTrue(active.exists())
            self.assertTrue(wrong_name.exists())
            self.assertTrue(link.is_symlink())

    def test_validation_staging_directories_are_unique_short_and_locked(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            with patch.object(isolation, "STAGING_PARENT", parent):
                first_token, first, first_lock = isolation.create_staging_directory()
                second_token, second, second_lock = isolation.create_staging_directory()
                try:
                    self.assertNotEqual(first_token, second_token)
                    self.assertRegex(first.name, r"^fs42-i-[0-9a-f]{12}$")
                    self.assertRegex(second.name, r"^fs42-i-[0-9a-f]{12}$")
                    self.assertEqual(first.stat().st_mode & 0o777, 0o700)
                    self.assertTrue(isolation._staging_is_locked(first))
                    self.assertTrue(isolation._staging_is_locked(second))
                finally:
                    first_lock.close()
                    second_lock.close()
                    isolation.cleanup_staging_directory(first)
                    isolation.cleanup_staging_directory(second)

    def test_collected_unit_is_a_successful_cleanup(self):
        runner = FakeRunner()
        with patch.object(isolation.subprocess, "run", side_effect=runner):
            ok, detail = isolation.cleanup_unit("already-collected.service")
        self.assertTrue(ok, detail)

    def test_report_directories_are_unique_and_do_not_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            first = self.run_mocked_preflight(directory)[1].parent
            second = self.run_mocked_preflight(directory)[1].parent
            self.assertNotEqual(first, second)
            self.assertTrue((first / "preflight.json").is_file())
            self.assertTrue((second / "preflight.json").is_file())

    def test_expected_pwd_is_accepted_and_reported(self):
        checked = isolation_probe.check_environment(
            isolation_probe.EXPECTED_ENVIRONMENT
        )
        self.assertTrue(checked["passed"])
        self.assertIn('"PWD": "/stage"', checked["detail"])

    def test_wrong_pwd_is_rejected(self):
        environment = dict(isolation_probe.EXPECTED_ENVIRONMENT, PWD="/project")
        self.assertFalse(isolation_probe.check_environment(environment)["passed"])

    def test_unexpected_environment_key_is_rejected_without_reporting_its_value(self):
        environment = dict(
            isolation_probe.EXPECTED_ENVIRONMENT,
            UNEXPECTED_SECRET="do-not-report",
        )
        checked = isolation_probe.check_environment(environment)
        self.assertFalse(checked["passed"])
        self.assertIn("UNEXPECTED_SECRET", checked["detail"])
        self.assertNotIn("do-not-report", checked["detail"])

    def test_wrong_allowlisted_value_is_rejected(self):
        environment = dict(
            isolation_probe.EXPECTED_ENVIRONMENT,
            PATH="/usr/local/bin",
        )
        self.assertFalse(isolation_probe.check_environment(environment)["passed"])

    def test_preflight_and_validation_share_the_exact_probe_definition(self):
        self.assertEqual(isolation.PROBE_RESULTS, isolation_probe.PROBE_RESULTS)
        payload = isolation_probe.build_probe_payload
        with patch.object(
            isolation_probe,
            "collect_probe_results",
            return_value={
                name: {"passed": True, "detail": name}
                for name in isolation.PROBE_RESULTS
            },
        ):
            built = payload("shared-run", environment={}, stage_root=Path("/stage"))
        results, error = isolation.validate_probe_payload(built, "shared-run")
        self.assertIsNone(error)
        self.assertEqual(tuple(results), isolation.PROBE_RESULTS)

    def test_direct_shell_context_is_accepted(self):
        verified, detail = isolation.check_invocation_context(
            environ={"SSH_CONNECTION": "test"},
            ancestry=["python3", "bash", "sshd", "systemd"],
        )
        self.assertTrue(verified, detail)

    def test_codex_ancestry_and_context_are_rejected(self):
        cases = (
            ({"CODEX_FUTURE_MARKER": "test"}, ["python3", "bash", "sshd"]),
            ({}, ["python3", "codex-linux-sandbox"]),
        )
        for environ, ancestry in cases:
            with self.subTest(environ=environ, ancestry=ancestry):
                verified, detail = isolation.check_invocation_context(
                    environ=environ,
                    ancestry=ancestry,
                )
                self.assertFalse(verified)
                if environ:
                    self.assertIn("CODEX_FUTURE_MARKER", detail)
                    self.assertNotIn("test", detail)

    def test_rejected_context_fails_without_launching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "runtime/director").mkdir(parents=True)
            runner = FakeRunner()
            with patch.object(
                isolation, "check_invocation_context", return_value=(False, "Codex detected")
            ):
                report, unused_json, unused_text = isolation.run_preflight(root)
            self.assertEqual(report["result"], "FAIL")
            self.assertFalse(report["verified_outside_codex"])
            self.assertFalse(any(call[0] == "systemd-run" for call in runner.calls))


if __name__ == "__main__":
    unittest.main()
