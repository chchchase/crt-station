import copy
import ctypes
import errno
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from station_director.dual_run import (
    CAPTURE_FAILURE_KINDS, _base_result, _validate_result_semantics,
)
from station_director.c1_diagnostics import make_diagnostic
import station_director.reporting as reporting
from station_director.reporting import (
    LATEST_SCHEMA,
    REPORT_SCHEMA,
    REPORT_SCHEMA_V1,
    REPORT_SCHEMA_V2,
    ReportError,
    build_validation_report,
    create_validation_run_id,
    publish_validation_report,
    render_validation_text,
    validate_report_document,
    _canonical_json,
    _rename_noreplace,
    _reject_unsafe_paths,
    _structured_finding,
)
from station_director.schedule_comparison import (
    MAX_COMPARISON_SPOOL_BYTES,
    ScheduleComparisonError,
    _time_us,
    _new_spool,
    comparison_query_plans,
    source_query_plans,
    canonical_requested_configuration_effects,
    compare_baseline_summaries,
    compare_baseline_to_proposed,
)
from station_director.single_run_protocol import (
    ProtocolError, strict_json_loads, validate_document,
)
from test.test_station_director_milestone_c1 import passing_guide
from test.test_station_director_milestone_b2 import (
    catalog_row,
    create_database,
    insert_block,
)


PROPOSAL = {
    "schema_version": 2,
    "proposal_id": "p-20260913T120000Z-deadbeef",
    "week_start": "2026-09-14T00:00:00-07:00",
    "week_end": "2026-09-21T00:00:00-07:00",
    "assignment_changes": [{"action": "move", "series": "Show",
                            "from_channel": 2, "to_channel": 3}],
    "directives": [], "exclusions": [],
}
CHANNEL = {"number": 2, "name": "Action",
           "channel_seed": 9,
           "regeneration_start": "2026-09-14 01:00:00.000001",
           "effective_horizon": "2026-09-14 04:00:00.000001"}


def retained_v2_from_v3(report):
    retained = copy.deepcopy(report)
    retained["schema_version"] = 2
    retained["software"]["report_schema_version"] = 2
    for group in ("baseline_findings", "unexpected_differences", "warnings", "errors"):
        for finding in retained["findings"][group]:
            finding.pop("capture_run")
            finding.pop("finalization_subphase")
            finding.pop("c1_diagnostic")
    for cleanup in retained["cleanup"].values():
        if cleanup and cleanup.get("diagnostic"):
            cleanup["diagnostic"].pop("capture_run")
            cleanup["diagnostic"].pop("finalization_subphase")
            cleanup["diagnostic"].pop("c1_diagnostic")
    return retained


def add_catalog(connection, catalog_id, path, title="Show", **changes):
    row = catalog_row("Action", path, title, "show")
    row.update(changes)
    columns = ["id", *row]
    connection.execute(
        f"INSERT INTO catalog_entries ({','.join(columns)}) VALUES "
        f"({','.join('?' for unused in columns)})", [catalog_id, *row.values()],
    )


def fixture(root, blocks):
    baseline = root / "baseline.db"
    connection = create_database(baseline)
    paths = sorted({block[3] for block in blocks})
    for index, path in enumerate(paths, 1):
        add_catalog(connection, index, path, title=Path(path).stem)
    for start, end, title, path in blocks:
        insert_block(connection, "Action", start, end, paths.index(path) + 1, path, title)
    connection.commit(); connection.close()
    proposed = root / "proposed.db"
    shutil.copy2(baseline, proposed)
    os.chmod(baseline, 0o600); os.chmod(proposed, 0o600)
    stage = root / "stage"; stage.mkdir(mode=0o700)
    return baseline, proposed, stage


def compare(root, baseline, proposed, channel=CHANNEL):
    return compare_baseline_to_proposed(
        root / "stage", baseline, proposed, [channel],
        "2026-09-14 00:00:00.000001", PROPOSAL,
    )


def success_c2(summary):
    result = _base_result("comparison")
    result.update({
        "status": "success", "phase_reached": "complete",
        "scheduler_invoked": {"run_1": True, "run_2": True},
        "validation_context": {"input_fingerprint": "1" * 64,
                               "requested_seed": 42, "effective_seed": 7,
                               "reference_clock": "2026-09-14 00:00:00-07:00",
                               "start_time": "2026-09-14 00:00:00",
                               "end_time": "2026-09-21 00:00:00",
                               "timezone": "America/Los_Angeles"},
        "affected_channels": [{"number": 2, "name": "Action"}],
        "source_checks": [{"checkpoint": name, "passed": True,
                           "changed_categories": []} for name in
                          ("after_capture", "between_runs", "after_run_2", "before_success")],
        "runs": [{"index": index, "run_id": f"comparison.run-{index}",
                  "status": "success", "normalization_digest": "a" * 64,
                  "record_count": 10, "provisional_catalog_count": 1,
                  "channels": [], "guide_validation": passing_guide(),
                  "preservation_summary": {"retained_history": "pass",
                      "protected_channels": "pass", "sequence_tables_restored": "pass",
                      "foreign_key_baseline": "pass", "path_validation_passed": True,
                      "mapping_count": 1, "scheduled_path_checks": 1,
                      "fingerprint_summary_digest": "c" * 64},
                  "warnings": [], "timings_ms": {}} for index in (1, 2)],
        "reproducibility": {"passed": True, "run_1_digest": "a" * 64,
            "run_2_digest": "a" * 64, "run_1_record_count": 10,
            "run_2_record_count": 10, "changed_records": 0,
            "added_records": 0, "removed_records": 0,
            "differences": [], "differences_truncated": False},
        "baseline_comparison": {**summary, "runs_matched": True,
                                "run_1_digest": summary["digest"],
                                "run_2_digest": summary["digest"]},
        "cleanup": [{"run": 2, "passed": True, "quarantined": False, "detail": "clean"},
                    {"run": 1, "passed": True, "quarantined": False, "detail": "clean"}],
        "failure": None, "timings_ms": {"total": 1},
    })
    return result


class ScheduleComparisonTests(unittest.TestCase):
    def test_exact_microseconds_and_touching_unchanged_blocks(self):
        self.assertEqual(_time_us("2026-09-14 00:00:00.000002", "time")
                         - _time_us("2026-09-14 00:00:00.000001", "time"), 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 02:00:00.000001", "A", "/media/a.mp4"),
                ("2026-09-14 02:00:00.000001", "2026-09-14 04:00:00.000001", "B", "/media/b.mp4"),
            ])
            result = compare(root, baseline, proposed)
            counts = result["channels"][0]["counts"]
            self.assertEqual(counts["unchanged_blocks"], 2)
            self.assertEqual(counts["components"], 2)
            self.assertEqual(result["channels"][0]["coverage"]["proposed"]["overlap_count"], 0)

    def test_replaced_semantics_and_selected_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "A", "/media/a.mp4")])
            connection = sqlite3.connect(proposed)
            add_catalog(connection, 2, "/media/different.mp4", title="Different")
            connection.execute("UPDATE liquid_blocks SET title='B', content_json='2', "
                               "plan_json=replace(plan_json,'a.mp4','different.mp4')")
            connection.commit(); connection.close()
            counts = compare(root, baseline, proposed)["channels"][0]["counts"]
            self.assertEqual(counts["replaced_pairs"], 1)
            self.assertEqual(counts["unchanged_blocks"], 0)
            self.assertEqual(counts["title_changes"], 1)
            self.assertEqual(counts["selected_media_changes"], 1)
            self.assertEqual(counts["playback_plan_changes"], 1)

    def test_shift_split_merge_nested_and_partial_are_reshaped(self):
        variants = [
            [("2026-09-14 01:30:00.000001", "2026-09-14 04:00:00.000001")],
            [("2026-09-14 01:00:00.000001", "2026-09-14 02:00:00.000001"),
             ("2026-09-14 02:00:00.000001", "2026-09-14 04:00:00.000001")],
            [("2026-09-14 01:30:00.000001", "2026-09-14 03:00:00.000001")],
        ]
        for intervals in variants:
            with self.subTest(intervals=intervals), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                baseline, proposed, unused = fixture(root, [
                    ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "A", "/media/a.mp4")])
                connection = sqlite3.connect(proposed)
                connection.execute("DELETE FROM liquid_blocks")
                for start, end in intervals:
                    insert_block(connection, "Action", start, end, 1, "/media/a.mp4")
                connection.commit(); connection.close()
                counts = compare(root, baseline, proposed)["channels"][0]["counts"]
                self.assertEqual(counts["reshaped_components"], 1)
                self.assertEqual(counts["reshaped_baseline_blocks"], 1)
                self.assertEqual(counts["reshaped_proposed_blocks"], len(intervals))

    def test_removed_and_generated_components_are_not_reshaped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 02:00:00.000001", "A", "/media/a.mp4")])
            connection = sqlite3.connect(proposed)
            connection.execute("DELETE FROM liquid_blocks")
            insert_block(connection, "Action", "2026-09-14 03:00:00.000001",
                         "2026-09-14 04:00:00.000001", 1, "/media/a.mp4")
            connection.commit(); connection.close()
            counts = compare(root, baseline, proposed)["channels"][0]["counts"]
            self.assertEqual((counts["removed_blocks"], counts["generated_blocks"]), (1, 1))
            self.assertEqual(counts["reshaped_components"], 0)

    def test_duplicate_interval_mixed_unchanged_and_replaced_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            blocks = [("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", title, path)
                      for title, path in (("A", "/media/a.mp4"), ("B", "/media/b.mp4"))]
            baseline, proposed, unused = fixture(root, blocks)
            connection = sqlite3.connect(proposed)
            connection.execute("UPDATE liquid_blocks SET title='C' WHERE title='B'")
            connection.commit(); connection.close()
            first = compare(root, baseline, proposed)
            second = compare(root, baseline, proposed)
            counts = first["channels"][0]["counts"]
            self.assertEqual((counts["unchanged_blocks"], counts["replaced_pairs"]), (1, 1))
            self.assertEqual(first, second)

    def test_retained_crossing_ends_at_seam_and_anomalous_overlap_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-13 23:00:00.000001", "2026-09-14 01:00:00.000001", "R", "/media/a.mp4"),
                ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "F", "/media/b.mp4"),
            ])
            channel = compare(root, baseline, proposed)["channels"][0]
            self.assertEqual(channel["retained_pre_seam_blocks"], 1)
            self.assertEqual(channel["counts"]["baseline_blocks"], 1)
            bad = dict(CHANNEL, regeneration_start="2026-09-14 00:30:00.000001")
            with self.assertRaisesRegex(ScheduleComparisonError, "anomalously overlaps"):
                compare(root, baseline, proposed, bad)

    def test_baseline_gaps_are_findings_and_new_proposed_gap_is_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 02:00:00.000001", "A", "/media/a.mp4"),
                ("2026-09-14 03:00:00.000001", "2026-09-14 04:00:00.000001", "B", "/media/b.mp4"),
            ])
            initial = compare(root, baseline, proposed)["channels"][0]
            self.assertEqual(initial["baseline_findings"][0]["code"], "baseline_gap")
            self.assertEqual(initial["errors"], [])
            connection = sqlite3.connect(proposed)
            connection.execute("DELETE FROM liquid_blocks WHERE title='B'")
            connection.commit(); connection.close()
            changed = compare(root, baseline, proposed)
            self.assertEqual(changed["unexpected_differences"][0]["code"], "proposal_created_gap")
            channel = changed["channels"][0]
            self.assertGreater(channel["coverage"]["proposed"]["gap_us"],
                               channel["coverage"]["baseline"]["gap_us"])

    def test_requested_effects_are_only_canonical_declared_operations(self):
        effects = canonical_requested_configuration_effects(PROPOSAL)
        self.assertEqual(len(effects), 1)
        self.assertEqual(effects[0]["kind"], "assignment")
        self.assertIn("move", effects[0]["canonical_operation"])
        self.assertNotIn("episode", json.dumps(effects))

    def test_historical_catalog_identity_cannot_be_hidden_by_equal_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "A", "/media/a.mp4")])
            connection = sqlite3.connect(proposed)
            connection.execute("UPDATE catalog_entries SET duration=3599.0 WHERE id=1")
            connection.commit(); connection.close()
            with self.assertRaisesRegex(ScheduleComparisonError, "historical catalog"):
                compare(root, baseline, proposed)

    def test_summary_match_uses_complete_canonical_structure(self):
        self.assertTrue(compare_baseline_summaries({"a": [1]}, {"a": [1]})["passed"])
        self.assertFalse(compare_baseline_summaries({"a": [1]}, {"a": [2]})["passed"])

    def test_spool_limit_is_checked_during_writes(self):
        with patch("station_director.schedule_comparison.MAX_COMPARISON_SPOOL_BYTES", 1):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                baseline, proposed, unused = fixture(root, [
                    ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "A", "/media/a.mp4")])
                with self.assertRaisesRegex(ScheduleComparisonError, "spool limit"):
                    compare(root, baseline, proposed)


    def test_comparison_queries_need_no_temp_btree_or_host_temp_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "stage"
            stage.mkdir(mode=0o700)
            spool_directory, spool_path, spool = _new_spool(stage)
            try:
                plans = comparison_query_plans(spool)
                self.assertTrue(plans)
                self.assertFalse(any(
                    "USE TEMP B-TREE" in detail.upper()
                    for details in plans.values() for detail in details
                ), plans)
            finally:
                spool.close()
                spool_path.unlink()
                spool_directory.rmdir()
            forbidden = root / "forbidden-host-tmp"
            forbidden.mkdir(mode=0o700)
            fixture_root = root / "fixture"
            fixture_root.mkdir(mode=0o700)
            baseline, proposed, unused = fixture(fixture_root, [
                ("2026-09-14 01:00:00", "2026-09-14 02:00:00", "A", "/media/a.mp4")
            ])
            source = sqlite3.connect(baseline)
            try:
                source_plans = source_query_plans(
                    source, "Action", "2026-09-14 00:00:00.000001"
                )
                self.assertTrue(source_plans)
                self.assertFalse(any(
                    "USE TEMP B-TREE" in detail.upper()
                    for details in source_plans.values() for detail in details
                ), source_plans)
            finally:
                source.close()
            with patch.dict(os.environ, {"TMPDIR": str(forbidden)}):
                compare(fixture_root, baseline, proposed)
            self.assertEqual(list(forbidden.iterdir()), [])

    def test_genuine_merge_and_transitive_overlap_are_single_reshaped_components(self):
        cases = (
            ([
                ("2026-09-14 01:00:00", "2026-09-14 02:00:00", "A", "/media/a.mp4"),
                ("2026-09-14 02:00:00", "2026-09-14 03:00:00", "B", "/media/b.mp4"),
             ], [("2026-09-14 01:00:00", "2026-09-14 03:00:00")], 2, 1),
            ([
                ("2026-09-14 01:00:00", "2026-09-14 02:30:00", "A", "/media/a.mp4"),
                ("2026-09-14 02:00:00", "2026-09-14 03:30:00", "B", "/media/b.mp4"),
                ("2026-09-14 03:00:00", "2026-09-14 04:30:00", "C", "/media/c.mp4"),
             ], [("2026-09-14 01:15:00", "2026-09-14 04:15:00")], 3, 1),
        )
        for baseline_blocks, proposed_intervals, left_count, right_count in cases:
            with self.subTest(left_count=left_count), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                baseline, proposed, unused = fixture(root, baseline_blocks)
                connection = sqlite3.connect(proposed)
                connection.execute("DELETE FROM liquid_blocks")
                for start, end in proposed_intervals:
                    insert_block(connection, "Action", start, end, 1, "/media/a.mp4")
                connection.commit(); connection.close()
                counts = compare(root, baseline, proposed)["channels"][0]["counts"]
                self.assertEqual(counts["reshaped_components"], 1)
                self.assertEqual(counts["reshaped_baseline_blocks"], left_count)
                self.assertEqual(counts["reshaped_proposed_blocks"], right_count)

    def test_enlarged_proposed_gap_is_not_excused_by_baseline_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00", "2026-09-14 02:00", "A", "/media/a.mp4"),
                ("2026-09-14 03:00:00", "2026-09-14 04:00:00", "B", "/media/b.mp4")])
            connection = sqlite3.connect(proposed)
            connection.execute("UPDATE liquid_blocks SET start_time='2026-09-14 03:30:00' WHERE title='B'")
            connection.commit(); connection.close()
            channel = compare(root, baseline, proposed)["channels"][0]
            self.assertTrue(any(item["code"] == "baseline_gap" for item in channel["baseline_findings"]))
            self.assertTrue(any(item["code"] == "proposal_created_gap" for item in channel["errors"]))

    def test_all_comparison_queries_are_covering_index_backed_without_temp_btrees(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            spool_directory, spool_path, connection = _new_spool(stage)
            try:
                plans = comparison_query_plans(connection)
                self.assertTrue(plans)
                for name, details in plans.items():
                    self.assertFalse(any("TEMP B-TREE" in detail.upper() for detail in details),
                                     (name, details))
            finally:
                connection.close(); spool_path.unlink(); spool_directory.rmdir()

    def test_forbidden_host_tmp_receives_no_comparison_artifact(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as forbidden:
            root = Path(directory)
            baseline, proposed, unused = fixture(root, [
                ("2026-09-14 01:00:00", "2026-09-14 02:00:00", "A", "/media/a.mp4")])
            with patch.dict(os.environ, {"TMPDIR": forbidden}):
                compare(root, baseline, proposed)
            self.assertEqual(list(Path(forbidden).iterdir()), [])


class ReportTests(unittest.TestCase):
    def test_retained_v4_report_remains_schema_valid(self):
        retained = copy.deepcopy(self.report)
        retained["schema_version"] = 4
        retained["software"]["report_schema_version"] = 4
        validate_report_document(retained, retained=True)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name) / "project"
        self.project.mkdir(mode=0o755)
        self.root = self.project / "runtime/director/validations"
        self.project_patch = patch("station_director.reporting.PROJECT_ROOT", self.project)
        self.root_patch = patch("station_director.reporting.VALIDATIONS_ROOT", self.root)
        self.project_patch.start(); self.root_patch.start()
        self.addCleanup(self.root_patch.stop); self.addCleanup(self.project_patch.stop)
        self.addCleanup(self.temp.cleanup)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            baseline, proposed, unused = fixture(path, [
                ("2026-09-14 01:00:00.000001", "2026-09-14 04:00:00.000001", "A", "/media/a.mp4")])
            self.summary = compare(path, baseline, proposed)
        self.run_id = "v-20260913T120000000001Z-" + "a" * 32
        self.report = build_validation_report(
            PROPOSAL, success_c2(self.summary), self.run_id, "2026-09-13T12:00:00Z"
        )

    def _prepare_parent(self):
        runtime = self.project / "runtime"
        runtime.mkdir(mode=0o755, exist_ok=True); runtime.chmod(0o755)
        director = runtime / "director"
        director.mkdir(mode=0o700, exist_ok=True); director.chmod(0o700)
        self.root.mkdir(mode=0o700, exist_ok=True); self.root.chmod(0o700)
        parent = self.root / PROPOSAL["proposal_id"]
        parent.mkdir(mode=0o700, exist_ok=True); parent.chmod(0o700)
        return parent

    def test_software_identity_schema_and_deterministic_text(self):
        self.assertEqual(self.report["schema_version"], 5)
        self.assertEqual(self.report["software"]["report_schema_version"], 5)
        self.assertTrue(self.report["software"]["director_version"])
        self.assertLessEqual(len(self.report["software"]["director_version"]), 100)
        validate_document(self.report, REPORT_SCHEMA)
        validate_document({"schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
                           "run_id": self.run_id, "validation_json_digest": "a" * 64},
                          LATEST_SCHEMA)
        self.assertEqual(render_validation_text(self.report), render_validation_text(copy.deepcopy(self.report)))
        self.assertEqual(_canonical_json(self.report), _canonical_json(copy.deepcopy(self.report)))
        self.assertTrue(_canonical_json(self.report).endswith(b"\n"))

    def test_c1_diagnostic_propagates_to_v4_without_hostile_text(self):
        result = _base_result("comparison")
        result["phase_reached"] = "run_1"
        result["source_checks"] = [{
            "checkpoint": "after_capture", "passed": True,
            "changed_categories": []}]
        result["failure"] = {
            "phase": "run_1", "code": "c1_run_failed",
            "category": "original_database_verification_failed",
            "message": "password=hunter2 /etc/shadow ENVIRONMENT=secret",
            "c1_diagnostic": {"run": 1, "detail": make_diagnostic(
                "original_database_verification_failed", "snapshot",
                fingerprint_category="original_logical_database")},
            "launcher_summary": {
                "outcome": "nonzero_exit", "stdout_bytes": 123,
                "stderr_bytes": 456, "stdout_truncated": True,
                "stderr_truncated": True, "termination_kind": "nonzero_exit",
                "exit_status": 1, "signal": None},
        }
        report = build_validation_report(
            PROPOSAL, result, self.run_id, "2026-09-13T12:00:00Z")
        finding = report["findings"]["errors"][0]
        self.assertEqual(finding["c1_diagnostic"]["run"], 1)
        self.assertEqual(finding["c1_diagnostic"]["detail"]["domain"],
                         "worker_verification")
        self.assertEqual(set(finding["c1_diagnostic"]["launcher_summary"]), {
            "outcome", "stdout_bytes", "stderr_bytes", "stdout_truncated",
            "stderr_truncated", "termination_kind", "exit_status", "signal"})
        serialized = _canonical_json(report)
        for forbidden in (b"hunter2", b"/etc/shadow", b"ENVIRONMENT"):
            self.assertNotIn(forbidden, serialized)
        changed_message = copy.deepcopy(result)
        changed_message["failure"]["message"] = "token=other /var/private"
        second = build_validation_report(
            PROPOSAL, changed_message, self.run_id,
            "2026-09-13T12:00:00Z")
        self.assertEqual(
            finding["diagnostic_digest"],
            second["findings"]["errors"][0]["diagnostic_digest"])
        validate_report_document(report)
        tampered = copy.deepcopy(report)
        tampered["findings"]["errors"][0]["c1_diagnostic"]["detail"][
            "template"] = "Different safe-looking template."
        with self.assertRaisesRegex(ReportError, "invalid C1"):
            validate_report_document(tampered)

    def test_unknown_scheduler_state_survives_c2_report_digest_and_text(self):
        result = _base_result("comparison")
        result["phase_reached"] = "run_1"
        result["scheduler_invoked"] = {"run_1": "unknown", "run_2": False}
        result["source_checks"] = [{
            "checkpoint": "after_capture", "passed": True,
            "changed_categories": [],
        }]
        detail = make_diagnostic("worker_response_missing", "response")
        detail["scheduler_invoked"] = "unknown"
        result["failure"] = {
            "phase": "run_1", "code": "c1_run_failed",
            "category": "worker_response_missing",
            "message": "An isolated native scheduling run failed.",
            "c1_diagnostic": {"run": 1, "detail": detail},
            "launcher_summary": {
                "outcome": "nonzero_exit", "stdout_bytes": 0,
                "stderr_bytes": 0, "stdout_truncated": False,
                "stderr_truncated": False, "termination_kind": "nonzero_exit",
                "exit_status": 1, "signal": None,
            },
        }
        report = build_validation_report(
            PROPOSAL, result, self.run_id, "2026-09-13T12:00:00Z")
        self.assertEqual(report["validation"]["scheduler_invoked"]["run_1"],
                         "unknown")
        self.assertIn(b"run 1=unknown", render_validation_text(report))

    def test_autobump_diagnostics_are_value_free_through_report_digest_and_text(self):
        for code, phase, invoked in (
            ("autobump_subprocess_required", "configuration", False),
            ("autobump_subprocess_blocked", "scheduler", True),
            ("autobump_selected", "scheduler", True),
            ("invalid_playback_descriptor", "scheduler", True),
            ("catalog_metadata_unavailable", "catalog", False),
        ):
            digests = []
            for poison in (
                "descriptor-one-private-value", "descriptor-two-private-value"
            ):
                result = _base_result("comparison")
                result["phase_reached"] = "run_1"
                result["scheduler_invoked"]["run_1"] = invoked
                result["source_checks"] = [{
                    "checkpoint": "after_capture", "passed": True,
                    "changed_categories": [],
                }]
                result["failure"] = {
                    "phase": "run_1", "code": "c1_run_failed",
                    "category": code, "message": poison,
                    "c1_diagnostic": {"run": 1, "detail": make_diagnostic(
                        code, phase, scheduler_invoked=invoked,
                        channel_number=2,
                    )},
                    "launcher_summary": {
                        "outcome": "nonzero_exit", "stdout_bytes": 0,
                        "stderr_bytes": 0, "stdout_truncated": False,
                        "stderr_truncated": False,
                        "termination_kind": "nonzero_exit",
                        "exit_status": 1, "signal": None,
                    },
                }
                report = build_validation_report(
                    PROPOSAL, result, self.run_id, "2026-09-13T12:00:00Z"
                )
                raw = _canonical_json(report)
                rendered = render_validation_text(report)
                self.assertNotIn(poison.encode(), raw)
                self.assertNotIn(poison.encode(), rendered)
                self.assertEqual(
                    report["findings"]["errors"][0]["c1_diagnostic"]["detail"]["code"],
                    code,
                )
                digests.append(
                    report["findings"]["errors"][0]["diagnostic_digest"]
                )
            self.assertEqual(digests[0], digests[1])

    def test_capture_kind_survives_v4_projection_publication_latest_and_text(self):
        run_id = "v-20260913T120002000001Z-" + "c" * 32
        report = None
        for kind in sorted(CAPTURE_FAILURE_KINDS):
            result = _base_result("comparison")
            result["failure"] = {
                "phase": "capture", "code": "source_capture_failed",
                "category": None, "message": "Source capture failed.",
                "capture_failure_kind": kind,
            }
            if kind == "single_run_finalization":
                result["failure"].update({
                    "capture_run": 2,
                    "finalization_subphase": "proposal_projection",
                })
            captured_digest_inputs = []
            real_digest = reporting._diagnostic_digest
            def observe_digest(value):
                captured_digest_inputs.append(copy.deepcopy(value))
                return real_digest(value)
            with self.subTest(kind=kind), patch.object(
                    reporting, "_diagnostic_digest", side_effect=observe_digest):
                candidate = build_validation_report(
                    PROPOSAL, result, run_id, "2026-09-13T12:02:00Z")
                finding = candidate["findings"]["errors"][0]
                self.assertEqual(finding["capture_failure_kind"], kind)
                self.assertEqual(
                    finding["capture_run"],
                    2 if kind == "single_run_finalization" else None)
                self.assertIn({
                    "code": "source_capture_failed", "phase": "capture",
                    "capture_failure_kind": kind,
                    "capture_run": (2 if kind == "single_run_finalization" else None),
                    "finalization_subphase": (
                        "proposal_projection"
                        if kind == "single_run_finalization" else None),
                    "template": "Source capture failed.",
                }, captured_digest_inputs)
                validate_report_document(candidate)
                self.assertIn(kind.encode("ascii"), _canonical_json(candidate))
                self.assertIn(kind.encode("ascii"), render_validation_text(candidate))
                if kind == "media_manifest_capture":
                    report = candidate
        self.assertIsNotNone(report)
        finding = report["findings"]["errors"][0]
        self.assertEqual(finding["capture_failure_kind"], "media_manifest_capture")
        self.assertEqual(finding["template"], "Source capture failed.")
        raw = _canonical_json(report)
        text = render_validation_text(report)
        self.assertIn(b'"capture_failure_kind":"media_manifest_capture"', raw)
        self.assertIn(b'media_manifest_capture', text)
        publication = publish_validation_report(report)
        self.assertEqual(publication["latest"]["status"], "updated")
        parent = self.root / PROPOSAL["proposal_id"]
        stored = strict_json_loads((parent / run_id / "validation.json").read_bytes())
        self.assertEqual(
            stored["findings"]["errors"][0]["capture_failure_kind"],
            "media_manifest_capture",
        )
        latest = strict_json_loads((parent / "latest.json").read_bytes())
        self.assertEqual(latest["run_id"], run_id)

    def test_v3_finalization_finding_requires_exact_run_and_subphase(self):
        result = _base_result("comparison")
        result["failure"] = {
            "phase": "capture", "code": "source_capture_failed",
            "category": None, "message": "Source capture failed.",
            "capture_failure_kind": "single_run_finalization",
            "capture_run": 1,
            "finalization_subphase": "request_binding",
        }
        report = build_validation_report(
            PROPOSAL, result, self.run_id, "2026-09-13T12:00:00Z")
        for field in ("capture_run", "finalization_subphase"):
            invalid = copy.deepcopy(report)
            invalid["findings"]["errors"][0].pop(field)
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                validate_report_document(invalid)
        invalid = copy.deepcopy(report)
        invalid["findings"]["errors"][0]["finalization_subphase"] = "unknown"
        with self.assertRaises(ProtocolError):
            validate_report_document(invalid)

    def test_explicit_capture_codes_survive_report_projection(self):
        for code, template in (
                ("backup_mismatch",
                 "A staged database backup differs from its pinned source."),
                ("duplicate_stage_file",
                 "A staged validation file already exists."),
                ("proposal_has_no_effects",
                 "Proposal has no effects eligible for schedule validation."),
                ("source_changed",
                 "A staged scheduling input changed during preparation.")):
            result = _base_result("comparison")
            result["failure"] = {
                "phase": "capture", "code": code, "category": None,
                "message": "password=hunter2 /etc/shadow",
            }
            if code in {"duplicate_stage_file", "proposal_has_no_effects",
                        "source_changed"}:
                result["failure"].update({
                    "capture_run": 1,
                    "finalization_subphase": (
                        "request_publication" if code == "duplicate_stage_file"
                        else "affected_channel_resolution"
                        if code == "proposal_has_no_effects"
                        else "staged_logical_configuration_fingerprint"),
                })
            with self.subTest(code=code):
                report = build_validation_report(
                    PROPOSAL, result, self.run_id, "2026-09-13T12:00:00Z")
                finding = report["findings"]["errors"][0]
                self.assertEqual(finding["code"], code)
                self.assertEqual(finding["template"], template)
                self.assertNotIn("hunter2", _canonical_json(report).decode())
                self.assertNotIn("/etc", render_validation_text(report).decode())
                validate_report_document(report)

    def test_retained_v1_capture_failure_remains_valid_and_replaceable(self):
        result = _base_result("comparison")
        result["failure"] = {
            "phase": "capture", "code": "source_capture_failed",
            "category": None, "message": "Source capture failed.",
            "capture_failure_kind": "database_snapshot",
        }
        old_run = "v-20260913T110000000001Z-" + "d" * 32
        v3 = build_validation_report(
            PROPOSAL, result, old_run, "2026-09-13T11:00:00Z")
        v2 = retained_v2_from_v3(v3)
        validate_document(v2, REPORT_SCHEMA_V2)
        validate_report_document(v2, retained=True)

        v1 = copy.deepcopy(v2)
        v1["schema_version"] = 1
        v1["software"]["report_schema_version"] = 1
        for group in ("baseline_findings", "unexpected_differences", "warnings", "errors"):
            for finding in v1["findings"][group]:
                finding.pop("capture_failure_kind")
        for cleanup in v1["cleanup"].values():
            if cleanup and cleanup.get("diagnostic"):
                cleanup["diagnostic"].pop("capture_failure_kind")
        validate_document(v1, REPORT_SCHEMA_V1)
        validate_report_document(v1, retained=True)

        parent = self._prepare_parent()
        old_directory = parent / old_run
        old_directory.mkdir(mode=0o700)
        old_raw = _canonical_json(v1)
        (old_directory / "validation.json").write_bytes(old_raw)
        (old_directory / "validation.txt").write_bytes(b"retained v1 failure\n")
        for path in old_directory.iterdir():
            path.chmod(0o600)
        pointer = {
            "schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
            "run_id": old_run,
            "validation_json_digest": hashlib.sha256(old_raw).hexdigest(),
        }
        (parent / "latest.json").write_bytes(_canonical_json(pointer))
        (parent / "latest.json").chmod(0o600)

        new_run = "v-20260913T120003000001Z-" + "e" * 32
        new_report = build_validation_report(
            PROPOSAL, result, new_run, "2026-09-13T12:03:00Z")
        publication = publish_validation_report(new_report)
        self.assertEqual(publication["latest"]["status"], "updated")
        self.assertEqual((old_directory / "validation.json").read_bytes(), old_raw)
        self.assertEqual(
            strict_json_loads((parent / "latest.json").read_bytes())["run_id"], new_run)

    def test_retained_v2_pointer_target_is_valid_and_replaceable_by_v4(self):
        result = _base_result("comparison")
        result["failure"] = {
            "phase": "capture", "code": "source_capture_failed",
            "category": None, "message": "Source capture failed.",
            "capture_failure_kind": "database_snapshot",
        }
        old_run = "v-20260913T110001000001Z-" + "f" * 32
        v3 = build_validation_report(
            PROPOSAL, result, old_run, "2026-09-13T11:00:01Z")
        v2 = retained_v2_from_v3(v3)
        validate_report_document(v2, retained=True)
        parent = self._prepare_parent()
        old_directory = parent / old_run
        old_directory.mkdir(mode=0o700)
        old_raw = _canonical_json(v2)
        (old_directory / "validation.json").write_bytes(old_raw)
        (old_directory / "validation.txt").write_bytes(b"retained v2 failure\n")
        for path in old_directory.iterdir():
            path.chmod(0o600)
        pointer = {
            "schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
            "run_id": old_run,
            "validation_json_digest": hashlib.sha256(old_raw).hexdigest(),
        }
        (parent / "latest.json").write_bytes(_canonical_json(pointer))
        (parent / "latest.json").chmod(0o600)

        new_run = "v-20260913T120004000001Z-" + "9" * 32
        new_report = build_validation_report(
            PROPOSAL, result, new_run, "2026-09-13T12:04:00Z")
        publication = publish_validation_report(new_report)
        self.assertEqual(publication["latest"]["status"], "updated")
        self.assertEqual((old_directory / "validation.json").read_bytes(), old_raw)
        self.assertEqual(
            strict_json_loads((parent / "latest.json").read_bytes())["run_id"],
            new_run)

    def test_retained_v3_pointer_target_is_replaceable_by_v4(self):
        old_run = "v-20260913T110002000001Z-" + "1" * 32
        retained = copy.deepcopy(self.report)
        retained["schema_version"] = 3
        retained["software"]["report_schema_version"] = 3
        retained["validation"]["run_id"] = old_run
        for group in ("baseline_findings", "unexpected_differences", "warnings", "errors"):
            for finding in retained["findings"][group]:
                finding.pop("c1_diagnostic")
        for cleanup in retained["cleanup"].values():
            if cleanup and cleanup.get("diagnostic"):
                cleanup["diagnostic"].pop("c1_diagnostic")
        validate_report_document(retained, retained=True)
        parent = self._prepare_parent()
        old_directory = parent / old_run
        old_directory.mkdir(mode=0o700)
        old_raw = _canonical_json(retained)
        (old_directory / "validation.json").write_bytes(old_raw)
        (old_directory / "validation.txt").write_bytes(b"retained v3\n")
        for path in old_directory.iterdir():
            path.chmod(0o600)
        pointer = {"schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
                   "run_id": old_run,
                   "validation_json_digest": hashlib.sha256(old_raw).hexdigest()}
        (parent / "latest.json").write_bytes(_canonical_json(pointer))
        (parent / "latest.json").chmod(0o600)
        new_run = "v-20260913T120005000001Z-" + "2" * 32
        new_report = copy.deepcopy(self.report)
        new_report["validation"]["run_id"] = new_run
        publication = publish_validation_report(new_report)
        self.assertEqual(publication["latest"]["status"], "updated")
        self.assertEqual((old_directory / "validation.json").read_bytes(), old_raw)

    def test_immutable_two_file_publication_modes_and_latest(self):
        result = publish_validation_report(self.report)
        directory = self.root / PROPOSAL["proposal_id"] / self.run_id
        self.assertEqual(result["publication_state"], "published_durable")
        self.assertEqual(result["latest"]["status"], "updated")
        self.assertEqual(set(item.name for item in directory.iterdir()),
                         {"validation.json", "validation.txt"})
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertTrue(all(stat.S_IMODE(item.stat().st_mode) == 0o600
                            for item in directory.iterdir()))
        latest = json.loads((directory.parent / "latest.json").read_text())
        self.assertEqual(latest["run_id"], self.run_id)
        with self.assertRaisesRegex(ReportError, "publication"):
            publish_validation_report(self.report)
        self.assertTrue(directory.is_dir())

    def test_invalid_latest_is_not_overwritten_and_report_remains(self):
        parent = self._prepare_parent()
        latest = parent / "latest.json"; latest.write_text("not json"); latest.chmod(0o600)
        result = publish_validation_report(self.report)
        self.assertEqual(result["latest"]["status"], "warning")
        self.assertEqual(latest.read_text(), "not json")
        self.assertTrue((parent / self.run_id / "validation.json").exists())

    def test_symlink_root_and_preexisting_temp_fail_without_modification(self):
        target = Path(self.temp.name) / "target"; target.mkdir(mode=0o700)
        runtime = self.project / "runtime"; runtime.mkdir(mode=0o755); runtime.chmod(0o755)
        (runtime / "director").mkdir(mode=0o700)
        self.root.symlink_to(target, target_is_directory=True)
        with self.assertRaises(ReportError):
            publish_validation_report(self.report)
        self.assertEqual(list(target.iterdir()), [])

    def test_durability_failure_reports_published_not_durable_and_keeps_directory(self):
        real_fsync = os.fsync
        calls = 0
        def fail_parent(descriptor):
            nonlocal calls
            calls += 1
            if calls == 4:
                raise OSError("synthetic")
            return real_fsync(descriptor)
        with patch("station_director.reporting.os.fsync", side_effect=fail_parent):
            with self.assertRaises(ReportError) as caught:
                publish_validation_report(self.report)
        self.assertEqual(caught.exception.publication_state, "published_not_durable")
        self.assertTrue((self.root / PROPOSAL["proposal_id"] / self.run_id).is_dir())

    def test_text_escapes_controls_and_path_fields_reject_host_paths(self):
        altered = copy.deepcopy(self.report)
        altered["findings"]["warnings"].append(_structured_finding(
            {"code": "unknown", "message": "line\n\x1b[31m"},
            "internal_warning", "report"))
        validate_document(altered, REPORT_SCHEMA)
        rendered = render_validation_text(altered)
        self.assertNotIn(b"\x1b", rendered)
        unsafe = {"phase": "report", "code": "unsafe", "category": None,
                  "message": "/home/private"}
        with self.assertRaises(Exception):
            build_validation_report(PROPOSAL, {**success_c2(self.summary),
                "failure": unsafe, "status": "failed"},
                self.run_id, "2026-09-13T12:00:00Z")

    def test_failure_schema_cannot_claim_success_invariants(self):
        broken = copy.deepcopy(self.report)
        broken["validation"]["scheduler_invoked"]["run_2"] = False
        with self.assertRaises(Exception): validate_document(broken, REPORT_SCHEMA)
        failed_c2 = _base_result("comparison")
        failed_c2["failure"] = {"phase": "capture", "code": "source_capture_failed",
                                "category": None, "message": "Source capture failed.",
                                "capture_failure_kind": "database_snapshot"}
        failed = build_validation_report(PROPOSAL, failed_c2, self.run_id,
                                         "2026-09-13T12:00:00Z")
        self.assertEqual(failed["validation"]["status"], "failed")
        self.assertFalse(failed["validation"]["scheduler_invoked"]["run_1"])

    def test_run_ids_are_bounded_and_unpredictable(self):
        one = create_validation_run_id()
        two = create_validation_run_id()
        self.assertNotEqual(one, two)
        self.assertRegex(one, r"^v-\d{8}T\d{12}Z-[a-f0-9]{32}$")

    def test_preexisting_hidden_temporary_directory_is_never_modified(self):
        parent = self._prepare_parent()
        existing = parent / ".tmp-fixed"; existing.mkdir(mode=0o700)
        marker = existing / "marker"; marker.write_text("keep"); marker.chmod(0o600)
        with patch("station_director.reporting.secrets.token_hex", return_value="fixed"):
            with self.assertRaises(ReportError): publish_validation_report(self.report)
        self.assertEqual(marker.read_text(), "keep")
        self.assertFalse((parent / self.run_id).exists())

    def test_partial_two_file_write_leaves_no_final_directory(self):
        import station_director.reporting as reporting
        real_write = reporting._write_file
        calls = 0
        def fail_second(*arguments):
            nonlocal calls
            calls += 1
            if calls == 2: raise OSError("synthetic partial write")
            return real_write(*arguments)
        with patch.object(reporting, "_write_file", side_effect=fail_second):
            with self.assertRaises(ReportError): publish_validation_report(self.report)
        parent = self.root / PROPOSAL["proposal_id"]
        self.assertFalse((parent / self.run_id).exists())
        self.assertFalse(any(item.name.startswith(".tmp-") for item in parent.iterdir()))

    def test_no_replace_unavailable_fails_closed_and_cleans_owned_temp(self):
        with patch("station_director.reporting._rename_noreplace",
                   side_effect=ReportError("noreplace_unavailable", "unavailable")):
            with self.assertRaisesRegex(ReportError, "unavailable"):
                publish_validation_report(self.report)
        parent = self.root / PROPOSAL["proposal_id"]
        self.assertFalse((parent / self.run_id).exists())
        self.assertEqual(list(parent.iterdir()), [])

    def test_latest_means_last_completed_attempt_including_failure(self):
        publish_validation_report(self.report)
        failed_c2 = _base_result("comparison")
        failed_c2["failure"] = {"phase": "capture", "code": "source_capture_failed",
                                "category": None, "message": "Source capture failed.",
                                "capture_failure_kind": "database_snapshot"}
        older_timestamp_run = "v-20260912T120000000001Z-" + "b" * 32
        failed = build_validation_report(PROPOSAL, failed_c2, older_timestamp_run,
                                         "2026-09-13T12:01:00Z")
        publish_validation_report(failed)
        pointer = json.loads((self.root / PROPOSAL["proposal_id"] / "latest.json").read_text())
        self.assertEqual(pointer["run_id"], older_timestamp_run)

    def test_unsafe_hardlinked_latest_is_left_unchanged(self):
        parent = self._prepare_parent()
        source = self.root.parent / "source"; source.write_text("unsafe"); source.chmod(0o600)
        os.link(source, parent / "latest.json")
        result = publish_validation_report(self.report)
        self.assertEqual(result["latest"]["status"], "warning")
        self.assertEqual(source.read_text(), "unsafe")

    def test_concurrent_publishers_produce_two_immutable_runs(self):
        second = copy.deepcopy(self.report)
        second["validation"]["run_id"] = "v-20260913T120001000001Z-" + "b" * 32
        outcomes = []
        def publish(value):
            try: outcomes.append(publish_validation_report(value))
            except Exception as exc: outcomes.append(exc)
        threads = [threading.Thread(target=publish, args=(value,))
                   for value in (self.report, second)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(len(outcomes), 2)
        self.assertTrue(all(isinstance(item, dict) for item in outcomes), outcomes)
        parent = self.root / PROPOSAL["proposal_id"]
        self.assertTrue((parent / self.run_id).is_dir())
        self.assertTrue((parent / second["validation"]["run_id"]).is_dir())

    def test_success_schema_rejects_baseline_mismatch_and_unexpected_findings(self):
        for mutate in (
            lambda item: item["baseline_comparison"].update(runs_matched=False),
            lambda item: item["findings"]["unexpected_differences"].append({
                "category": "unexpected", "code": "x", "message": "",
                "channel": 2, "count": 1}),
        ):
            broken = copy.deepcopy(self.report); mutate(broken)
            with self.assertRaises(Exception): validate_document(broken, REPORT_SCHEMA)


    def test_named_report_structures_reject_duplicate_and_partial_success(self):
        duplicate_array = copy.deepcopy(self.report)
        duplicate_array["guide_validation"] = [
            duplicate_array["guide_validation"]["run_1"],
            duplicate_array["guide_validation"]["run_1"],
        ]
        with self.assertRaises(Exception):
            validate_document(duplicate_array, REPORT_SCHEMA)
        partial = copy.deepcopy(self.report)
        partial["guide_validation"]["comparison"]["status"] = "unavailable"
        with self.assertRaises(Exception):
            validate_document(partial, REPORT_SCHEMA)
        duplicate_json = b'{"schema_version":1,"schema_version":1}'
        with self.assertRaises(Exception):
            strict_json_loads(duplicate_json)

    def test_early_failure_cannot_claim_later_completed_phases(self):
        failed_c2 = _base_result("comparison")
        failed_c2["failure"] = {"phase": "capture", "code": "source_capture_failed",
                                "category": None, "message": "Source capture failed.",
                                "capture_failure_kind": "database_snapshot"}
        failed = build_validation_report(PROPOSAL, failed_c2, self.run_id,
                                         "2026-09-13T12:00:00Z")
        failed["phases"]["reproducibility"] = "completed"
        with self.assertRaises(Exception):
            validate_document(failed, REPORT_SCHEMA)

    def test_existing_latest_pointer_target_is_fully_validated(self):
        first = publish_validation_report(self.report)
        parent = self.root / PROPOSAL["proposal_id"]
        pointer_path = parent / "latest.json"
        valid_pointer = json.loads(pointer_path.read_text())
        invalid_values = []
        cross = dict(valid_pointer, proposal_id="p-20260913T120000Z-feedface")
        invalid_values.append(cross)
        invalid_values.append(dict(valid_pointer,
                                   run_id="v-20260913T120001000001Z-" + "b" * 32))
        invalid_values.append(dict(valid_pointer, validation_json_digest="0" * 64))
        for index, value in enumerate(invalid_values, 1):
            with self.subTest(index=index):
                pointer_path.write_bytes(_canonical_json(value)); pointer_path.chmod(0o600)
                before = pointer_path.read_bytes()
                report = copy.deepcopy(self.report)
                report["validation"]["run_id"] = (
                    f"v-20260913T12000{index}000001Z-" + format(index, "032x"))
                outcome = publish_validation_report(report)
                self.assertEqual(outcome["latest"]["status"], "warning")
                self.assertEqual(pointer_path.read_bytes(), before)
                self.assertTrue((parent / report["validation"]["run_id"]).is_dir())
        self.assertEqual(first["publication_state"], "published_durable")

    def test_latest_rejects_report_internal_identity_mismatch(self):
        publish_validation_report(self.report)
        parent = self.root / PROPOSAL["proposal_id"]
        copied_run = "v-20260913T120009000001Z-" + "9" * 32
        source = parent / self.run_id
        target = parent / copied_run
        shutil.copytree(source, target); target.chmod(0o700)
        for item in target.iterdir(): item.chmod(0o600)
        raw = (target / "validation.json").read_bytes()
        pointer = {"schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
                   "run_id": copied_run,
                   "validation_json_digest": __import__("hashlib").sha256(raw).hexdigest()}
        pointer_path = parent / "latest.json"
        pointer_path.write_bytes(_canonical_json(pointer)); pointer_path.chmod(0o600)
        report = copy.deepcopy(self.report)
        report["validation"]["run_id"] = "v-20260913T120010000001Z-" + "a" * 32
        outcome = publish_validation_report(report)
        self.assertEqual(outcome["latest"]["status"], "warning")
        self.assertEqual(pointer_path.read_bytes(), _canonical_json(pointer))

    def test_symlinked_latest_and_malformed_referenced_report_are_preserved(self):
        publish_validation_report(self.report)
        parent = self.root / PROPOSAL["proposal_id"]
        pointer = parent / "latest.json"
        pointer.unlink()
        external = self.project / "external-pointerbots"
        external.write_text("unsafe"); external.chmod(0o600)
        pointer.symlink_to(external)
        second = copy.deepcopy(self.report)
        second["validation"]["run_id"] = "v-20260913T120020000001Z-" + "b" * 32
        outcome = publish_validation_report(second)
        self.assertEqual(outcome["latest"]["status"], "warning")
        self.assertTrue(pointer.is_symlink())
        pointer.unlink()
        first_pointer = {"schema_version": 1, "proposal_id": PROPOSAL["proposal_id"],
                         "run_id": self.run_id, "validation_json_digest": "0" * 64}
        damaged = parent / self.run_id / "validation.json"
        damaged.write_text("{malformed"); damaged.chmod(0o600)
        first_pointer["validation_json_digest"] = __import__("hashlib").sha256(
            damaged.read_bytes()).hexdigest()
        pointer.write_bytes(_canonical_json(first_pointer)); pointer.chmod(0o600)
        third = copy.deepcopy(self.report)
        third["validation"]["run_id"] = "v-20260913T120021000001Z-" + "c" * 32
        outcome = publish_validation_report(third)
        self.assertEqual(outcome["latest"]["status"], "warning")
        self.assertEqual(damaged.read_text(), "{malformed")

    def test_renameat2_retries_one_eintr_and_bounds_retries(self):
        class Function:
            def __init__(self, failures): self.failures = failures; self.calls = 0
            def __call__(self, *unused):
                self.calls += 1
                if self.calls <= self.failures:
                    ctypes.set_errno(errno.EINTR); return -1
                return 0
        class Library: pass
        one = Function(1); library = Library(); library.renameat2 = one
        with patch("station_director.reporting.ctypes.CDLL", return_value=library):
            _rename_noreplace(1, "source", 2, "target")
        self.assertEqual(one.calls, 2)
        repeated = Function(reporting.RENAME_EINTR_RETRIES)
        library.renameat2 = repeated
        with patch("station_director.reporting.ctypes.CDLL", return_value=library):
            with self.assertRaisesRegex(ReportError, "retry"):
                _rename_noreplace(1, "source", 2, "target")
        self.assertEqual(repeated.calls, reporting.RENAME_EINTR_RETRIES)

    def test_each_immutable_fsync_failure_is_fail_closed(self):
        for fail_call, expected_state in ((1, "not_published"), (2, "not_published"),
                                          (3, "not_published"), (4, "published_not_durable")):
            with self.subTest(fail_call=fail_call), tempfile.TemporaryDirectory() as directory:
                project = Path(directory) / "project"; project.mkdir(mode=0o755)
                root = project / "runtime/director/validations"
                real_fsync = os.fsync; calls = 0
                def fail_selected(descriptor):
                    nonlocal calls
                    calls += 1
                    if calls == fail_call: raise OSError("synthetic")
                    return real_fsync(descriptor)
                with patch("station_director.reporting.PROJECT_ROOT", project), \
                     patch("station_director.reporting.VALIDATIONS_ROOT", root), \
                     patch("station_director.reporting.os.fsync", side_effect=fail_selected):
                    with self.assertRaises(ReportError) as caught:
                        publish_validation_report(self.report)
                self.assertEqual(caught.exception.publication_state, expected_state)
                final = root / PROPOSAL["proposal_id"] / self.run_id
                self.assertEqual(final.exists(), fail_call == 4)
                if root.exists():
                    self.assertFalse(any(item.name.startswith(".tmp-")
                                         for item in (root / PROPOSAL["proposal_id"]).iterdir()))

    def test_latest_write_replace_and_fsync_failures_preserve_immutable_report(self):
        for mode in ("file_fsync", "replace", "parent_fsync"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                project = Path(directory) / "project"; project.mkdir(mode=0o755)
                root = project / "runtime/director/validations"
                real_fsync = os.fsync; calls = 0
                def fsync(descriptor):
                    nonlocal calls
                    calls += 1
                    target = 5 if mode == "file_fsync" else 6
                    if mode != "replace" and calls == target: raise OSError("synthetic")
                    return real_fsync(descriptor)
                replace_patch = patch("station_director.reporting.os.replace",
                                      side_effect=OSError("synthetic")) if mode == "replace" else patch(
                                          "station_director.reporting.os.replace", wraps=os.replace)
                with patch("station_director.reporting.PROJECT_ROOT", project), \
                     patch("station_director.reporting.VALIDATIONS_ROOT", root), \
                     patch("station_director.reporting.os.fsync", side_effect=fsync), replace_patch:
                    outcome = publish_validation_report(self.report)
                self.assertEqual(outcome["publication_state"], "published_durable")
                self.assertEqual(outcome["latest"]["status"], "warning")
                expected_state = ("latest_replaced_not_durable"
                                  if mode == "parent_fsync" else "latest_not_replaced")
                self.assertEqual(outcome["latest"]["publication_state"], expected_state)
                final = root / PROPOSAL["proposal_id"] / self.run_id
                self.assertTrue(final.is_dir())
                self.assertEqual((final / "validation.json").read_bytes(), _canonical_json(self.report))

    def test_json_and_text_size_limits_fail_before_publication(self):
        with patch("station_director.reporting.MAX_REPORT_JSON_BYTES", 1):
            with self.assertRaisesRegex(ReportError, "size|large|limit"):
                publish_validation_report(self.report)
        self.assertFalse(self.root.exists())
        with patch("station_director.reporting.MAX_REPORT_TEXT_BYTES", 1):
            with self.assertRaisesRegex(ReportError, "text|size|large"):
                publish_validation_report(self.report)
        self.assertFalse(self.root.exists())

    def test_diagnostics_never_retain_paths_secrets_or_raw_exception_text(self):
        failed_c2 = _base_result("comparison")
        failed_c2["failure"] = {
            "phase": "capture", "code": "unknown_failure", "category": "ValueError",
            "message": "password=hunter2 /etc/passwd C:\\private Authorization: Bearer token"}
        report = build_validation_report(PROPOSAL, failed_c2, self.run_id,
                                         "2026-09-13T12:00:00Z")
        raw = _canonical_json(report)
        for secret in (b"hunter2", b"/etc/passwd", b"C:\\\\private", b"Authorization"):
            self.assertNotIn(secret, raw)
        error = report["findings"]["errors"][0]
        self.assertEqual(error["code"], "internal_error")
        self.assertRegex(error["diagnostic_digest"], r"^[a-f0-9]{64}$")
        for value in ("/var/lib/private", "C:\\private\\file", "\\\\server\\share"):
            with self.assertRaises(ReportError):
                _reject_unsafe_paths({"diagnostic_message": value})
        _reject_unsafe_paths({"title": "AC/DC: The Secret Files",
                              "selected_media_identity": "crt-media:/Shows/AC-DC.mp4"})

    def test_intermediate_permissions_symlinks_case_and_entry_limits_fail_closed(self):
        cases = ("permissions", "symlink", "case", "limit")
        for mode in cases:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                project = Path(directory) / "project"; project.mkdir(mode=0o755)
                runtime = project / "runtime"
                if mode == "case":
                    (project / "Runtime").mkdir(mode=0o700)
                else:
                    runtime.mkdir(mode=0o755); runtime.chmod(0o755)
                    if mode == "permissions":
                        director = runtime / "director"; director.mkdir(mode=0o777); director.chmod(0o777)
                    elif mode == "symlink":
                        target = project / "target"; target.mkdir(mode=0o700)
                        (runtime / "director").symlink_to(target, target_is_directory=True)
                    elif mode == "limit":
                        (project / "extra-a").mkdir(); (project / "extra-b").mkdir()
                root = project / "runtime/director/validations"
                limit = 1 if mode == "limit" else reporting.MAX_REPORT_ENTRIES
                with patch("station_director.reporting.PROJECT_ROOT", project), \
                     patch("station_director.reporting.VALIDATIONS_ROOT", root), \
                     patch("station_director.reporting.MAX_REPORT_ENTRIES", limit):
                    with self.assertRaises(ReportError):
                        publish_validation_report(self.report)
                self.assertFalse((root / PROPOSAL["proposal_id"] / self.run_id).exists())

    def test_directory_replacement_between_scan_and_open_is_detected(self):
        runtime = self.project / "runtime"; runtime.mkdir(mode=0o755); runtime.chmod(0o755)
        director = runtime / "director"; director.mkdir(mode=0o700)
        real_open = os.open; swapped = False
        def replacing_open(path, *arguments, **keywords):
            nonlocal swapped
            if path == "director" and not swapped:
                swapped = True
                director.rename(runtime / "director-old")
                director.mkdir(mode=0o700)
            return real_open(path, *arguments, **keywords)
        with patch("station_director.reporting.os.open", side_effect=replacing_open):
            with self.assertRaisesRegex(ReportError, "changed"):
                publish_validation_report(self.report)
        self.assertFalse((self.root / PROPOSAL["proposal_id"] / self.run_id).exists())

    def test_concurrent_latest_points_to_last_serialized_completion(self):
        second = copy.deepcopy(self.report)
        second["validation"]["run_id"] = "v-20260913T120001000001Z-" + "b" * 32
        failed = copy.deepcopy(second)
        failed["validation"]["status"] = "failed"
        failed["validation"]["phase_reached"] = "cleanup"
        failed["phases"]["cleanup"] = "failed"
        failed["findings"]["errors"] = [_structured_finding(
            {"code": "cleanup_failed", "phase": "cleanup"}, "cleanup_failed", "cleanup")]
        real_latest = reporting._publish_latest
        serialized = []
        def observed(*arguments):
            result = real_latest(*arguments)
            serialized.append(arguments[2])
            return result
        outcomes = []
        def publish(value): outcomes.append(publish_validation_report(value))
        with patch("station_director.reporting._publish_latest", side_effect=observed):
            threads = [threading.Thread(target=publish, args=(value,))
                       for value in (self.report, failed)]
            for thread in threads: thread.start()
            for thread in threads: thread.join()
        self.assertEqual(len(outcomes), 2)
        pointer = json.loads((self.root / PROPOSAL["proposal_id"] / "latest.json").read_text())
        self.assertEqual(pointer["run_id"], serialized[-1])


class BoundaryTests(unittest.TestCase):
    def test_public_gate_and_import_boundary_remain_unchanged(self):
        import station_director.stage_runner as stage_runner
        import station_director.validation as validation
        self.assertEqual(stage_runner.DISABLED_MESSAGE, "Phase 3 validation is not yet enabled")
        self.assertEqual(validation.PHASE_3_DISABLED, "Phase 3 validation is not yet enabled")
        result = validation._disabled_report({"proposal_id": "test"})
        self.assertFalse(result["scheduler_invoked"])

    def test_c2_success_requires_baseline_comparison(self):
        result = _base_result("comparison")
        result["status"] = "success"; result["phase_reached"] = "complete"
        result["scheduler_invoked"] = {"run_1": True, "run_2": True}
        result["runs"] = [{"index": 1}, {"index": 2}]
        result["source_checks"] = [{"checkpoint": value, "passed": True}
                                   for value in ("after_capture", "between_runs", "after_run_2", "before_success")]
        result["reproducibility"] = {"passed": True}
        result["cleanup"] = [{"run": 2, "passed": True, "quarantined": False},
                             {"run": 1, "passed": True, "quarantined": False}]
        result["failure"] = None
        with self.assertRaisesRegex(Exception, "baseline comparison"):
            _validate_result_semantics(result)

    def test_clean_public_imports_cannot_reach_c2_c3_or_fs42(self):
        script = (
            "import sys; import station_director.cli, station_director.validation, "
            "station_director.stage_runner; "
            "blocked=[n for n in sys.modules if n.startswith('fs42') or "
            "n in ('station_director.dual_run','station_director.schedule_comparison',"
            "'station_director.reporting')]; assert not blocked, blocked"
        )
        subprocess.run([sys.executable, "-c", script], check=True, cwd=Path(__file__).parents[1])


if __name__ == "__main__":
    unittest.main()
