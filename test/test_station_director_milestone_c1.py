import ast
import copy
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
import warnings
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
    RESPONSE_SCHEMA_V4 as RESPONSE_SCHEMA,
    RESPONSE_SCHEMA_V2,
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
    logical_configuration_values_fingerprint,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
)
from station_director.worker_bootstrap import (
    BootstrapError,
    HeldSchedulingInputs,
    VerifiedWorkerAttestation,
    _configuration_inventory,
    finalize_work_tree,
    projected_configuration_documents,
)
from station_director.worker_checkpoint import (
    CHECKPOINT_DIRECTORY,
    CheckpointWriter,
    WorkerCheckpointError,
    read_checkpoint_evidence,
    _validate_transitions,
)
from test.test_station_director_schedule import base_proposal
from test.test_station_director_milestone_b2 import (
    catalog_row,
    create_database,
    insert_block,
)


ROOT = Path(__file__).parents[1]


class SyntheticSequenceRestoreTests(unittest.TestCase):
    """Exercise only restoration helpers; never enter a native scheduling run."""

    def setUp(self):
        from station_director import native_single_run as native
        from station_director import staged_schedule as staged
        self.native, self.staged = native, staged
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "synthetic.sqlite"
        self.connection = create_database(self.database)
        self.addCleanup(self.connection.close)
        self.connection.execute("PRAGMA foreign_keys=ON")

    def populate(self, *, entries=True, groups=True):
        for identifier, station in ((1, "Action"), (2, "Watch In Order")):
            self.connection.execute(
                "INSERT INTO named_sequence VALUES (?,?,?,?,?,?,?,?,?)",
                (identifier, station, "synthetic", "tag", 0.125, 0.875,
                 3, 1, None if identifier == 1 else b"synthetic-blob"))
            if entries:
                self.connection.execute(
                    "INSERT INTO sequence_entries VALUES (?,?,?,?)",
                    (identifier, "synthetic-entry", 0, identifier))
            if groups:
                self.connection.execute(
                    "INSERT INTO sequence_group_state VALUES (?,?,?,?)",
                    (station, "synthetic", "parent", "active"))
        self.connection.commit()

    def snapshot(self):
        return self.staged.capture_protected_state(
            self.connection, ["Action"], ["Watch In Order"])

    def test_round_trips_types_order_foreign_keys_and_protected_channel(self):
        self.populate()
        snapshot = self.snapshot()
        operations = []
        # Retain only fixed operation/table identifiers, never expanded SQL.
        def trace(statement):
            for operation in ("DELETE FROM", "INSERT INTO"):
                for table in ("sequence_entries", "sequence_group_state", "named_sequence"):
                    if statement.startswith(f'{operation} "{table}"'):
                        marker = (operation, table)
                        if not operations or operations[-1] != marker:
                            operations.append(marker)
        self.connection.set_trace_callback(trace)
        for unused in range(4):
            operations.clear()
            self.connection.execute(
                "UPDATE named_sequence SET current_index=99 WHERE station='Action'")
            self.connection.commit()
            self.staged.restore_sequence_state(self.connection, snapshot)
            self.connection.commit()
            self.staged.assert_protected_state(self.connection, snapshot)
            self.assertEqual(self.connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(operations, [
                ("DELETE FROM", "sequence_entries"),
                ("DELETE FROM", "sequence_group_state"),
                ("DELETE FROM", "named_sequence"),
                ("INSERT INTO", "named_sequence"),
                ("INSERT INTO", "sequence_entries"),
                ("INSERT INTO", "sequence_group_state"),
            ])
        self.native._restore_sequences(self.database, snapshot)
        self.staged.assert_protected_state(self.connection, snapshot)

    def test_empty_and_partially_populated_tables(self):
        for entries, groups in ((False, False), (False, True), (True, False), (True, True)):
            with self.subTest(entries=entries, groups=groups):
                for table in ("sequence_entries", "sequence_group_state", "named_sequence"):
                    self.connection.execute(f'DELETE FROM "{table}"')
                self.connection.commit()
                empty = self.snapshot()
                self.native._restore_sequences(self.database, empty)
                self.staged.assert_protected_state(self.connection, empty)
                self.populate(entries=entries, groups=groups)
                snapshot = self.snapshot()
                self.native._restore_sequences(self.database, snapshot)
                self.staged.assert_protected_state(self.connection, snapshot)

    def test_delete_and_insert_failures_roll_back(self):
        self.populate()
        snapshot = self.snapshot()
        for operation in ("DELETE", "INSERT"):
            with self.subTest(operation=operation):
                self.connection.execute(
                    f"CREATE TRIGGER synthetic_failure BEFORE {operation} ON sequence_entries "
                    "BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END")
                self.connection.commit()
                with self.assertRaises(self.native.NativeRunError) as caught:
                    self.native._restore_sequences(self.database, snapshot)
                self.assertEqual(caught.exception.code, "sequence_restore_failure")
                self.assertEqual(caught.exception.preservation_detail, {
                    "helper": "restore_sequence_state", "category": operation.lower() + "_failed",
                    "content_scope": None})
                self.assertIsInstance(caught.exception.__cause__, sqlite3.IntegrityError)
                self.staged.assert_protected_state(self.connection, snapshot)
                self.connection.execute("DROP TRIGGER synthetic_failure")
                self.connection.commit()

    def test_schema_mismatch_rolls_back(self):
        self.populate()
        snapshot = self.snapshot()
        self.connection.execute("ALTER TABLE named_sequence ADD COLUMN synthetic TEXT")
        self.connection.commit()
        before = self.snapshot()
        with self.assertRaises(self.native.NativeRunError) as caught:
            self.native._restore_sequences(self.database, snapshot)
        self.assertIsInstance(caught.exception.__cause__, self.staged.StagedScheduleError)
        self.assertEqual(caught.exception.preservation_detail["category"], "schema_mismatch")
        self.staged.assert_protected_state(self.connection, before)

    def test_commit_failure_rolls_back(self):
        self.populate()
        snapshot = self.snapshot()
        connection = sqlite3.connect(self.database)
        proxy = Mock(wraps=connection)
        proxy.commit.side_effect = sqlite3.OperationalError("synthetic commit failure")
        try:
            with patch.object(self.native.sqlite3, "connect", return_value=proxy):
                with self.assertRaises(self.native.NativeRunError) as caught:
                    self.native._restore_sequences(self.database, snapshot)
            self.assertEqual(caught.exception.code, "sequence_restore_failure")
            self.assertEqual(caught.exception.preservation_detail["category"], "commit_failed")
            proxy.rollback.assert_called_once()
            proxy.close.assert_called_once()
            self.staged.assert_protected_state(self.connection, snapshot)
        finally:
            connection.close()

    def test_exact_typed_row_verification_failure(self):
        self.populate()
        snapshot = self.snapshot()
        verify = self.staged.assert_protected_state
        def mismatch(connection, state, **kwargs):
            connection.execute("UPDATE named_sequence SET parent_tag=CAST('synthetic-blob' AS TEXT) WHERE id=2")
            verify(connection, state, **kwargs)
        with patch.object(self.native, "assert_protected_state", side_effect=mismatch):
            with self.assertRaises(self.native.NativeRunError) as caught:
                self.native._restore_sequences(self.database, snapshot)
        self.assertIsInstance(caught.exception.__cause__, self.staged.StagedScheduleError)
        self.assertEqual(caught.exception.preservation_detail, {
            "helper": "_restore_sequences", "category": "verification_failed", "content_scope": None})
        self.staged.assert_protected_state(self.connection, snapshot)

    def test_primary_failure_is_not_replaced_by_restoration_failure(self):
        self.populate()
        snapshot = self.snapshot()
        primary = self.native.NativeRunError(
            "scheduler_failure", "synthetic primary", phase="scheduler",
            scheduler_invoked=True)
        @self.native._sequence_restored
        def synthetic_operation(request, attestation, restoration):
            restoration.update(database=self.database, protected=snapshot,
                               scheduler_invoked=True)
            self.connection.execute(
                "CREATE TRIGGER synthetic_failure BEFORE INSERT ON sequence_entries "
                "BEGIN SELECT RAISE(ABORT, 'synthetic secondary'); END")
            self.connection.commit()
            raise primary
        with self.assertRaises(self.native.NativeRunError) as caught:
            synthetic_operation({}, None)
        self.assertIs(caught.exception, primary)
        self.assertEqual(caught.exception.restoration_failure, "sequence_restore_failure")
        self.assertEqual(primary.preservation_detail["category"], "insert_failed")

    def test_primary_cause_and_cancellation_survive_secondary_failure(self):
        from station_director.c1_diagnostics import attach_preservation_detail
        for kind in (RuntimeError, KeyboardInterrupt, SystemExit):
            with self.subTest(kind=kind.__name__):
                primary = kind("synthetic primary")
                cause = ValueError("synthetic original cause")
                primary.__cause__ = cause
                attach_preservation_detail(primary, "coverage_report", "gap")
                secondary = attach_preservation_detail(RuntimeError("synthetic secondary"), "restore_sequence_state", "insert_failed")
                @self.native._sequence_restored
                def operation(request, attestation, restoration):
                    restoration.update(database=self.database, protected={})
                    raise primary
                with patch.object(self.native, "_restore_sequences",
                                  side_effect=secondary):
                    with self.assertRaises(kind) as caught:
                        operation({}, None)
                self.assertIs(caught.exception, primary)
                self.assertIs(caught.exception.__cause__, cause)
                self.assertEqual(primary.restoration_failure, "sequence_restore_failure")
                self.assertEqual(primary.preservation_detail["category"], "gap")
                self.assertTrue(primary.__suppress_context__)


def publish_completed_worker_checkpoints(stage):
    with CheckpointWriter(stage) as writer:
        for state in (
            "worker_started", "probes_passed", "snapshot_verified",
            "seed_verified", "request_verified", "native_import_completed",
            "configuration_completed", "catalog_entered", "catalog_completed",
            "scheduler_entry", "scheduler_completed",
            "response_publication_attempted", "response_publication_completed",
        ):
            writer.publish(state)


class PreservationDetailTests(unittest.TestCase):
    def test_all_allowlisted_pairs_and_redaction(self):
        from station_director.c1_diagnostics import (
            PRESERVATION_CATEGORIES, PLAYBACK_HELPERS, attach_preservation_detail,
            copy_preservation_detail, validate_preservation_detail,
        )
        from jsonschema import Draft202012Validator
        def detail_schemas(value):
            if isinstance(value, dict):
                if "preservation_detail" in value.get("properties", {}):
                    yield value["properties"]["preservation_detail"]
                for child in value.values():
                    yield from detail_schemas(child)
            elif isinstance(value, list):
                for child in value:
                    yield from detail_schemas(child)
        validators = [Draft202012Validator(detail) for name in (
            "native-single-run.response.v4.schema.json", "native-dual-run.result.v4.schema.json",
            "validation-report.v6.schema.json") for detail in detail_schemas(
                json.loads((RESPONSE_SCHEMA.parent / name).read_text()))]
        self.assertEqual(len(validators), 2)  # Report v6 references C2 v4's definition.
        for helper, categories in PRESERVATION_CATEGORIES.items():
            scopes = (None, "retained", "generated") if helper in PLAYBACK_HELPERS else (None,)
            for category in categories:
                for scope in scopes:
                    with self.subTest(helper=helper, category=category, scope=scope):
                        error = attach_preservation_detail(
                            RuntimeError("private SQL /secret/media value"), helper, category, scope)
                        validate_preservation_detail(error.preservation_detail)
                        for validator in validators:
                            validator.validate(error.preservation_detail)
                        primary = copy_preservation_detail(error, RuntimeError("private primary"))
                        self.assertEqual(primary.preservation_detail, error.preservation_detail)
                        attach_preservation_detail(primary, "coverage_report", "gap")
                        self.assertEqual(primary.preservation_detail, error.preservation_detail)
                        raw = json.dumps(primary.preservation_detail)
                        self.assertNotIn("private", raw)
                        self.assertNotIn("/secret", raw)
        for bad in (False, [], {}, {"helper": "private", "category": "gap", "content_scope": None},
                    {"helper": "coverage_report", "category": "private", "content_scope": None},
                    {"helper": "coverage_report", "category": "gap", "content_scope": "retained"},
                    {"helper": "coverage_report", "category": "gap", "content_scope": None, "raw": "secret"}):
            with self.assertRaises(ValueError):
                validate_preservation_detail(bad)
            for validator in validators:
                self.assertFalse(validator.is_valid(bad))

    def test_cleanup_keeps_primary_first_detail_and_attempts_close(self):
        from station_director import native_single_run as native
        from station_director.c1_diagnostics import attach_preservation_detail
        for has_detail in (False, True):
            primary = native.NativeRunError("scheduler_failure", "secret", phase="scheduler", scheduler_invoked=True)
            if has_detail:
                attach_preservation_detail(primary, "coverage_report", "gap")
            connection = Mock()
            connection.rollback.side_effect = RuntimeError("secret rollback")
            connection.close.side_effect = RuntimeError("secret close")
            native._preservation_cleanup(connection, primary, "_restore_sequences", rollback=True)
            self.assertEqual(primary.code, "scheduler_failure")
            self.assertEqual(primary.preservation_detail["category"], "gap" if has_detail else "rollback_failed")
            connection.rollback.assert_called_once()
            connection.close.assert_called_once()
        connection = Mock()
        connection.close.side_effect = RuntimeError("secret")
        with self.assertRaises(native.NativeRunError) as caught:
            native._preservation_cleanup(connection, None, "_verify_final_preservation", scheduler_invoked=True)
        self.assertTrue(caught.exception.scheduler_invoked)
        self.assertEqual(caught.exception.code, "preservation_failure")
        self.assertEqual(caught.exception.preservation_detail["category"], "close_failed")

    def test_final_foreign_key_predicate_and_query_failure(self):
        from station_director import native_single_run as native
        for finding, error, category in ((["synthetic"], None, "foreign_key_mismatch"),
                                         (None, RuntimeError("private query"), "foreign_key_check_failed")):
            with patch.object(native, "restore_sequence_state"), patch.object(native, "assert_protected_state"), \
                    patch.object(native, "_validate_final_cross_channel_exclusions"), \
                    patch.object(native, "canonical_foreign_key_findings", return_value=finding, side_effect=error):
                with self.assertRaises(Exception) as caught:
                    native._verify_final_preservation(Mock(), {}, {}, [], {}, [], None)
            self.assertEqual(caught.exception.preservation_detail["category"], category)

    def test_media_failures_distinguish_retained_generated(self):
        from station_director import native_single_run as native
        from station_director import staged_schedule as staged
        from fs42 import autobump_descriptor as descriptor
        for generated in (False, True):
            scope = "generated" if generated else "retained"
            for kind in ("plan", "reference", "missing_media"):
                connection = Mock()
                if kind == "plan":
                    representations = staged._playback_representations(connection, [(1, "show", None, "not-json")])
                    expected = "plan_invalid"
                elif kind == "reference":
                    representations = staged._playback_representations(connection, [(1, "show", "not-json", "[]")])
                    expected = "reference_failure"
                else:
                    representations = [{"block_id": 1, "liquid_type": "show", "content_missing": False,
                                        "plan": [], "catalog": [{"path": "/media/synthetic", "realpath": None}]}]
                    expected = "media_validation_failed"
                with patch.object(descriptor, "classify_catalog_entry", return_value="ordinary"), \
                        patch.object(native, "canonical_media_mapping", side_effect=RuntimeError("secret missing media")):
                    with self.assertRaises(Exception) as caught:
                        native._validate_playback_representations(descriptor, representations, "Synthetic", generated=generated, media_root=Path("/unused"))
                self.assertEqual(caught.exception.preservation_detail["category"], expected)
                self.assertEqual(caught.exception.preservation_detail["content_scope"], scope)

    def test_retained_catalog_protected_and_coverage_predicates(self):
        from station_director import staged_schedule as staged
        history = types.SimpleNamespace(channel="Synthetic", proposal_boundary="2026-09-21 06:00:00",
                                        proposal_end="2026-09-21 09:00:00", effective_horizon="2026-09-21 09:00:00",
                                        retained_rows=[(1,)], protected_catalog_ids={1}, retained_catalog_rows={1: (1, "old")})
        for rows, category in (([], "schedule_mismatch"), ([(1,)], "catalog_mismatch")):
            with patch.object(staged, "_rows", side_effect=[(["id"], rows), (["id"], [])]), \
                    patch.object(staged, "_columns", return_value=["id"]):
                with self.assertRaises(staged.StagedScheduleError) as caught:
                    staged.assert_retained_history(Mock(), history)
            self.assertEqual(caught.exception.preservation_detail["category"], category)
        with patch.object(staged, "_rows", side_effect=[(["id"], [(1,)]), RuntimeError("private")]), \
                patch.object(staged, "_columns", return_value=["id"]):
            with self.assertRaises(RuntimeError) as caught:
                staged.assert_retained_history(Mock(), history)
        self.assertEqual(caught.exception.preservation_detail["category"], "catalog_check_failed")
        state = {"synthetic": {"columns": ["id"], "rows": [(1,)], "where": "", "parameters": ()}}
        with patch.object(staged, "_rows", return_value=(["id"], [(2,)])):
            with self.assertRaises(staged.StagedScheduleError) as caught:
                staged.assert_protected_state(Mock(), state)
        self.assertEqual(caught.exception.preservation_detail["helper"], "assert_protected_state")
        self.assertEqual(caught.exception.preservation_detail["category"], "mismatch")
        for intervals, category in (([(6, 7), (8, 9)], "gap"), ([(6, 8), (7, 9)], "overlap"),
                                    ([(6, 7), (6, 7), (8, 9)], "gap_and_overlap")):
            rows = [(index, f"2026-09-21 {start:02}:00:00", f"2026-09-21 {end:02}:00:00")
                    for index, (start, end) in enumerate(intervals)]
            with patch.object(staged, "_rows", return_value=(["id", "start_time", "end_time"], rows)):
                with self.assertRaises(staged.StagedScheduleError) as caught:
                    staged.coverage_report(Mock(), history)
            self.assertEqual(caught.exception.preservation_detail["category"], category)


class FinalExclusionInitializationTests(unittest.TestCase):
    """Real LiquidIO initialization; every SQLite open is fixture-confined."""

    def setUp(self):
        from fs42.station_manager import StationManager
        from station_director import native_single_run as native
        self.native = native
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "runtime").mkdir()
        (self.root / "confs").mkdir()
        self.database = self.root / "runtime/fs42_fluid.db"
        self.connection = create_database(self.database)
        self.addCleanup(self.connection.close)
        stack = ExitStack()
        self.addCleanup(stack.close)
        previous = os.getcwd()
        os.chdir(self.root)
        stack.callback(os.chdir, previous)
        stack.enter_context(patch.object(StationManager, "_StationManager__we_are_all_one", {}))
        stack.enter_context(patch.object(StationManager, "_initialized", False))
        stack.enter_context(patch.object(StationManager, "stations", []))
        real_connect = sqlite3.connect
        self.opens = []
        def confined_connect(path, *args, **kwargs):
            self.assertEqual(Path(path).resolve(), self.database)
            self.opens.append(Path(path).resolve())
            return real_connect(path, *args, **kwargs)
        stack.enter_context(patch.object(sqlite3, "connect", side_effect=confined_connect))
        self.projected = {}
        for number, name in ((2, "Action"), (8, "Watch In Order")):
            conf = {"network_name": name, "channel_number": number, "network_type": "standard",
                    "content_dir": str(self.root / "media"), "day_templates": {"daily": {"6": {"tags": "Shared"}}}}
            for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
                conf[day] = "daily"
            self.projected[name] = {"station_conf": conf}
        self.histories = {"Action": types.SimpleNamespace(
            regeneration_start="2026-09-21 06:00:00", effective_horizon="2026-09-22 06:00:00")}

    def add_block(self, identifier, name, start, end, *, sequence=False):
        path = str(self.root / "media/synthetic.mp4")
        row = catalog_row(name, path, "Synthetic", "Shared")
        self.connection.execute(
            "INSERT INTO catalog_entries (id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (identifier, *row.values()))
        insert_block(self.connection, name, start, end, identifier, path)
        if sequence:
            self.connection.execute("UPDATE liquid_blocks SET sequence_key=? WHERE station=?", ('["synthetic"]', name))
        self.connection.commit()

    def test_actual_liquid_io_initializes_only_fixture_database(self):
        liquid_io = self.native.LiquidIO()
        self.assertEqual(Path(liquid_io.db_path).resolve(), self.database)
        self.assertTrue(self.opens)

    def test_valid_raw_day_templates_and_single_channel_empty_pairs(self):
        self.projected.pop("Watch In Order")
        before = copy.deepcopy(self.projected)
        self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertEqual(self.projected, before)
        self.assertEqual(self.opens, [])

    def test_unaffected_standard_sibling_collision_is_not_skipped(self):
        self.add_block(1, "Action", "2026-09-21 08:00:00", "2026-09-21 10:00:00")
        self.add_block(2, "Watch In Order", "2026-09-21 09:00:00", "2026-09-21 11:00:00")
        # Processed weekdays isolate the pair-selection defect from template expansion.
        from fs42.config_processor import ConfigProcessor
        self.projected = {name: {"station_conf": ConfigProcessor.preprocess(copy.deepcopy(data["station_conf"]))}
                          for name, data in self.projected.items()}
        with self.assertRaises(Exception) as caught:
            self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertEqual(caught.exception.preservation_detail["category"], "collision")

    def test_raw_templates_collide_without_mutating_config_or_rows(self):
        self.add_block(1, "Action", "2026-09-21 08:00:00", "2026-09-21 10:00:00")
        self.add_block(2, "Watch In Order", "2026-09-21 09:00:00", "2026-09-21 11:00:00")
        before_config = copy.deepcopy(self.projected)
        before_rows = self.connection.execute("SELECT * FROM liquid_blocks ORDER BY id").fetchall()
        with self.assertRaises(Exception) as caught:
            self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertEqual(caught.exception.preservation_detail, {
            "helper": "_validate_final_cross_channel_exclusions", "category": "collision", "content_scope": None})
        self.assertTrue(self.opens)
        self.assertEqual(self.projected, before_config)
        self.assertEqual(self.connection.execute("SELECT * FROM liquid_blocks ORDER BY id").fetchall(), before_rows)

    def test_genuinely_empty_exclusion_sets_do_not_initialize_io(self):
        for case in ("no_affected", "different_tags", "different_directory", "nonstandard_sibling", "empty_range"):
            with self.subTest(case=case):
                projected, histories = copy.deepcopy(self.projected), copy.deepcopy(self.histories)
                sibling = projected["Watch In Order"]["station_conf"]
                if case == "no_affected":
                    histories = {}
                elif case == "different_tags":
                    sibling["day_templates"]["daily"]["6"]["tags"] = "Other"
                elif case == "different_directory":
                    sibling["content_dir"] = str(self.root / "other-media")
                elif case == "nonstandard_sibling":
                    sibling["network_type"] = "web"
                else:
                    histories["Action"].effective_horizon = histories["Action"].regeneration_start
                self.native._validate_final_cross_channel_exclusions(projected, histories)
                self.assertEqual(self.opens, [])

    def test_invalid_template_still_fails_closed_before_io(self):
        self.projected["Action"]["station_conf"]["monday"] = "undefined"
        with self.assertRaises(Exception) as caught:
            self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertEqual(caught.exception.preservation_detail["category"], "check_failed")
        self.assertEqual(self.opens, [])

    def test_existing_sequence_exemption_is_preserved_on_either_side(self):
        self.add_block(1, "Action", "2026-09-21 08:00:00", "2026-09-21 10:00:00", sequence=True)
        self.add_block(2, "Watch In Order", "2026-09-21 09:00:00", "2026-09-21 11:00:00")
        self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.connection.execute("UPDATE liquid_blocks SET sequence_key=NULL WHERE station='Action'")
        self.connection.execute("UPDATE liquid_blocks SET sequence_key=? WHERE station='Watch In Order'", ('["synthetic"]',))
        self.connection.commit()
        self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertTrue(self.opens)

    def test_historical_collision_outside_affected_range_is_not_rejected(self):
        self.add_block(1, "Action", "2026-09-20 08:00:00", "2026-09-20 10:00:00")
        self.add_block(2, "Watch In Order", "2026-09-20 09:00:00", "2026-09-20 11:00:00")
        self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertTrue(self.opens)

    def test_affected_range_also_checks_other_affected_channels_retained_rows(self):
        self.add_block(1, "Action", "2026-09-21 08:00:00", "2026-09-21 10:00:00")
        self.add_block(2, "Watch In Order", "2026-09-21 09:00:00", "2026-09-21 11:00:00")
        # The second history is later: taking the intersection would miss this.
        self.histories["Watch In Order"] = types.SimpleNamespace(
            regeneration_start="2026-09-22 06:00:00", effective_horizon="2026-09-23 06:00:00")
        with self.assertRaises(Exception) as caught:
            self.native._validate_final_cross_channel_exclusions(self.projected, self.histories)
        self.assertEqual(caught.exception.preservation_detail["category"], "collision")


class WorkerCheckpointTests(unittest.TestCase):
    def test_canonical_sequence_and_exact_pending_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            stage.chmod(0o700)
            with CheckpointWriter(stage) as writer:
                for state in (
                    "worker_started", "probes_passed", "snapshot_verified",
                    "seed_verified", "request_verified", "native_import_completed",
                    "configuration_completed", "catalog_entered", "catalog_completed",
                    "scheduler_entry", "scheduler_completed",
                    "response_publication_attempted", "response_publication_completed",
                ):
                    writer.publish(state)
            evidence = read_checkpoint_evidence(stage)
            self.assertTrue(evidence.worker_started)
            self.assertTrue(evidence.scheduler_entered)
            self.assertTrue(evidence.response_completed)

            tail = stage / CHECKPOINT_DIRECTORY / "pending-14.json"
            tail.write_bytes(b'{"partial"')
            tail.chmod(0o600)
            evidence = read_checkpoint_evidence(stage)
            self.assertTrue(evidence.ignored_pending_tail)
            self.assertEqual(len(evidence.states), 13)

    def test_checkpoint_gaps_replacements_and_bad_transitions_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory); stage.chmod(0o700)
            with CheckpointWriter(stage) as writer:
                writer.publish("worker_started")
                with self.assertRaises(WorkerCheckpointError):
                    writer.publish("seed_verified")
            root = stage / CHECKPOINT_DIRECTORY
            wrong = root / "pending-03.json"
            wrong.write_text("", encoding="utf-8"); wrong.chmod(0o600)
            with self.assertRaises(WorkerCheckpointError):
                read_checkpoint_evidence(stage)
            wrong.unlink()
            target = root / "checkpoint-01.json"
            alias = root / "pending-02.json"
            os.link(target, alias)
            with self.assertRaises(WorkerCheckpointError):
                read_checkpoint_evidence(stage)

    def test_worker_started_is_required_first_and_seventh_cycle_prefixes_fail(self):
        with self.assertRaises(WorkerCheckpointError):
            _validate_transitions(("response_publication_attempted",))
        prefix = (
            "worker_started", "probes_passed", "snapshot_verified",
            "seed_verified", "request_verified", "native_import_completed",
            "configuration_completed",
        )
        cycle = ("catalog_entered", "catalog_completed", "scheduler_entry",
                 "scheduler_completed")
        for length in range(1, len(cycle) + 1):
            with self.subTest(length=length), self.assertRaises(WorkerCheckpointError):
                _validate_transitions(prefix + cycle * 6 + cycle[:length])

    def test_worker_checkpoint_module_has_no_runtime_or_fs42_dependency(self):
        source = (ROOT / "station_director/worker_checkpoint.py").read_text()
        tree = ast.parse(source)
        imported = {node.module for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom) and node.module}
        imported.update(alias.name for node in ast.walk(tree)
                        if isinstance(node, ast.Import) for alias in node.names)
        self.assertFalse(any(name.startswith("fs42") for name in imported))
        self.assertFalse(imported & {"subprocess", "socket", "urllib", "requests"})

    def test_scheduler_attestation_precedence_for_abnormal_termination(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory); stage.chmod(0o700)
            media = stage / "media"; media.mkdir()
            request = request_for(stage, media)
            lifecycle = SingleRunLifecycle(
                ROOT, stage, types.SimpleNamespace(closed=False), "token",
                request["run_id"], "unit", request,
                launcher_result=LaunchResult(
                    "unit", 1, "", "", termination_kind="external_signal",
                    signal=9, main_process_started=True, unit_state_valid=True),
            )
            with CheckpointWriter(stage) as writer:
                for state in (
                    "worker_started", "probes_passed", "snapshot_verified",
                    "seed_verified", "request_verified", "native_import_completed",
                    "configuration_completed", "catalog_entered", "catalog_completed",
                    "scheduler_entry",
                ):
                    writer.publish(state)
            with self.assertRaises(SingleRunError) as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"],
                             "worker_external_signal")
            self.assertIs(caught.exception.scheduler_state, True)

    def test_started_missing_response_is_unknown_but_never_started_is_false(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); root.chmod(0o700)
            media = root / "media"; media.mkdir()
            request = request_for(root, media)
            for started, expected in ((True, "unknown"), (False, False)):
                lifecycle = SingleRunLifecycle(
                    ROOT, root, types.SimpleNamespace(closed=False), "token",
                    request["run_id"], "unit", request,
                    launcher_result=LaunchResult(
                        "unit", 1, "", "",
                        termination_kind=("nonzero_exit" if started else "launcher_failure"),
                        exit_status=(1 if started else None),
                        main_process_started=started, unit_state_valid=True),
                )
                with self.subTest(started=started), self.assertRaises(SingleRunError) as caught:
                    inspect_single_run(lifecycle)
                self.assertEqual(caught.exception.scheduler_state, expected)

    def test_missing_unit_after_launcher_execution_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); root.chmod(0o700)
            media = root / "media"; media.mkdir()
            request = request_for(root, media)
            lifecycle = SingleRunLifecycle(
                ROOT, root, types.SimpleNamespace(closed=False), "token",
                request["run_id"], "unit", request,
                launcher_result=LaunchResult(
                    "unit", 1, "", "", termination_kind="launcher_state_invalid",
                    main_process_started=None, unit_state_valid=False,
                    launcher_executed=True),
            )
            with self.assertRaises(SingleRunError) as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"],
                             "launcher_state_invalid")
            self.assertEqual(caught.exception.scheduler_state, "unknown")

    def test_validation_media_guards_precede_probe_decoder_and_subprocess(self):
        import datetime
        from fs42 import media_processor
        from fs42.scheduling_context import (
            ValidationCatalogMetadataUnavailable,
            ValidationSchedulingContext,
            activate_validation_context,
        )
        context = ValidationSchedulingContext(
            datetime.datetime(2026, 9, 14), datetime.datetime(2026, 9, 14),
            datetime.datetime(2026, 9, 15), 1,
        )
        with activate_validation_context(context), patch.object(
            media_processor.ffmpeg, "probe"
        ) as probe, patch.object(
            media_processor, "VideoFileClip"
        ) as decoder, patch("subprocess.run") as process:
            with self.assertRaises(ValidationCatalogMetadataUnavailable):
                media_processor.MediaProcessor._get_duration("opaque")
            with self.assertRaises(ValidationCatalogMetadataUnavailable):
                media_processor.MediaProcessor.black_detect("opaque", 1)
            with self.assertRaises(ValidationCatalogMetadataUnavailable):
                media_processor.MediaProcessor.chapter_detect("opaque", 1)
        probe.assert_not_called()
        decoder.assert_not_called()
        process.assert_not_called()

    def test_ordinary_media_probe_keeps_its_native_argument(self):
        from fs42 import media_processor
        with patch.object(
            media_processor.ffmpeg, "probe",
            return_value={"format": {"duration": "12.5"}},
        ) as probe:
            duration, error = media_processor.MediaProcessor._get_duration(
                "ordinary-native-argument")
        self.assertEqual((duration, error), (12.5, None))
        probe.assert_called_once_with("ordinary-native-argument")

    def test_validation_direct_cache_lookup_requires_current_typed_metadata(self):
        import datetime
        from fs42.fluid_statements import FluidStatements
        from fs42.scheduling_context import (
            ValidationCatalogMetadataUnavailable,
            ValidationSchedulingContext,
            activate_validation_context,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"; media.mkdir()
            item = media / "item.mp4"; item.write_bytes(b"fixture")
            info = item.stat()
            database = root / "cache.db"
            connection = sqlite3.connect(database)
            FluidStatements.init_db(connection)
            connection.execute(
                "INSERT INTO file_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("/mnt/t7/CRT-Media/item.mp4", 12.0, info.st_size, 1,
                 info.st_mtime, 1, 1, '{"type":"movie"}', "video"),
            )
            connection.commit()
            context = ValidationSchedulingContext(
                datetime.datetime(2026, 9, 14), datetime.datetime(2026, 9, 14),
                datetime.datetime(2026, 9, 15), 1, media_root=str(media),
            )
            with activate_validation_context(context):
                self.assertEqual(
                    FluidStatements.check_file_cache(connection, str(item)).duration,
                    12.0)
                connection.execute(
                    "UPDATE file_meta SET meta=? WHERE path=?",
                    ("{}", "/mnt/t7/CRT-Media/item.mp4"),
                )
                connection.commit()
                with self.assertRaises(ValidationCatalogMetadataUnavailable):
                    FluidStatements.check_file_cache(connection, str(item))
                connection.execute(
                    "UPDATE file_meta SET meta=?, size=? WHERE path=?",
                    ('{"type":"movie"}', info.st_size + 1,
                     "/mnt/t7/CRT-Media/item.mp4"),
                )
                connection.commit()
                with self.assertRaises(ValidationCatalogMetadataUnavailable):
                    FluidStatements.check_file_cache(connection, str(item))
            connection.close()


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
    def _projected_configuration_fixture(self, root, *, main_config=None):
        source = root / "stage/source"
        (source / "confs").mkdir(parents=True)
        (source / "runtime").mkdir()
        media = root / "media"
        (media / "shared").mkdir(parents=True)
        policy = load_policy()
        for channel in policy["channels"]:
            filename = channel["name"].lower().replace(" ", "_") + ".json"
            (source / "confs" / filename).write_text(json.dumps({
                "station_conf": {
                    "network_name": channel["name"],
                    "channel_number": channel["number"],
                    "content_dir": "catalog/crt_media/shared",
                },
            }) + "\n", encoding="utf-8")
        if main_config is not None:
            (source / "confs/main_config.json").write_text(
                main_config, encoding="utf-8")
        (source / "runtime/watch_in_order_state.json").write_text(
            "{}\n", encoding="utf-8")
        sqlite3.connect(source / "runtime/fs42_fluid.db").close()
        proposal = base_proposal()
        proposal["directives"] = [{
            "type": "date_slot", "channel": 2, "date": "2026-09-14",
            "hour": 0, "series": "Synthetic",
        }]
        request = {
            "proposal": proposal,
            "policy": policy,
            "affected_channels": [{"number": 2, "name": "Action"}],
        }
        return source, media, request

    def test_missing_optional_main_config_uses_native_defaults_and_round_trips(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, media, request = self._projected_configuration_fixture(root)
            work = root / "stage/work"
            documents, affected, unused_sources, unused_mappings = (
                projected_configuration_documents(source, request, media, work)
            )
            self.assertEqual(len(documents), 8)
            self.assertEqual(affected, ("Action",))
            self.assertNotIn("confs/main_config.json", documents)
            serialized = json.loads(json.dumps(documents, sort_keys=True))
            self.assertEqual(
                logical_configuration_values_fingerprint(documents),
                logical_configuration_values_fingerprint(serialized),
            )

            project = root / "project"
            (project / "fs42").mkdir(parents=True)
            shutil.copy(
                ROOT / "fs42/station_config_schema.json",
                project / "fs42/station_config_schema.json",
            )
            snapshot, held = finalize_work_tree(
                request, root / "stage", media, project)
            try:
                self.assertFalse((work / "confs/main_config.json").exists())
                self.assertEqual(
                    snapshot["projected_configuration_fingerprint"],
                    logical_configuration_values_fingerprint(documents)["digest"],
                )
            finally:
                held.close()

    def test_exact_main_config_is_filtered_and_published(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, media, request = self._projected_configuration_fixture(
                root,
                main_config=json.dumps({
                    "normalize_titles": False,
                    "tmdb_api_key": "must-not-be-staged",
                }),
            )
            documents, unused_affected, unused_sources, unused_mappings = (
                projected_configuration_documents(
                    source, request, media, root / "stage/work")
            )
            main = documents["confs/main_config.json"]
            self.assertEqual(main, {
                "normalize_titles": False,
                "db_path": "runtime/fs42_fluid.db",
            })
            self.assertNotIn("must-not-be-staged", json.dumps(documents))

            project = root / "project"
            (project / "fs42").mkdir(parents=True)
            shutil.copy(
                ROOT / "fs42/station_config_schema.json",
                project / "fs42/station_config_schema.json",
            )
            unused_snapshot, held = finalize_work_tree(
                request, root / "stage", media, project)
            try:
                published = json.loads(
                    (root / "stage/work/confs/main_config.json").read_text(
                        encoding="utf-8"))
                self.assertEqual(published, main)
            finally:
                held.close()

    def test_optional_main_config_inventory_failures_are_closed(self):
        cases = (
            "malformed", "symlink", "hard_link", "directory",
            "case_confusable", "disappearing", "replacement", "unreadable",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, media, request = self._projected_configuration_fixture(root)
                main = source / "confs/main_config.json"
                if case == "malformed":
                    main.write_text("{", encoding="utf-8")
                elif case == "symlink":
                    target = root / "main-target.json"
                    target.write_text("{}", encoding="utf-8")
                    main.symlink_to(target)
                elif case == "hard_link":
                    target = root / "main-target.json"
                    target.write_text("{}", encoding="utf-8")
                    os.link(target, main)
                elif case == "directory":
                    main.mkdir()
                elif case == "case_confusable":
                    main = source / "confs/Main_Config.json"
                    main.write_text("{}", encoding="utf-8")
                else:
                    main.write_text("{}", encoding="utf-8")

                if case in {"disappearing", "replacement"}:
                    original_inventory = _configuration_inventory

                    def changed_inventory(value):
                        result = original_inventory(value)
                        if case == "disappearing":
                            main.unlink()
                        else:
                            replacement = root / "replacement.json"
                            replacement.write_text("{}", encoding="utf-8")
                            os.replace(replacement, main)
                        return result

                    context = patch(
                        "station_director.worker_bootstrap._configuration_inventory",
                        side_effect=changed_inventory,
                    )
                elif case == "unreadable":
                    original_open = os.open

                    def denied_open(path, flags, *args, **kwargs):
                        if Path(path) == main:
                            raise PermissionError("synthetic denial")
                        return original_open(path, flags, *args, **kwargs)

                    context = patch(
                        "station_director.worker_bootstrap.os.open",
                        side_effect=denied_open,
                    )
                else:
                    context = ExitStack()
                expected = {
                    "malformed": json.JSONDecodeError,
                    "symlink": BootstrapError,
                    "hard_link": BootstrapError,
                    "directory": BootstrapError,
                    "case_confusable": BootstrapError,
                    "disappearing": FileNotFoundError,
                    "replacement": BootstrapError,
                    "unreadable": PermissionError,
                }[case]
                with context, self.assertRaises(expected):
                    projected_configuration_documents(
                        source, request, media, root / "stage/work")

    def test_host_staged_schedule_has_no_fs42_descriptor_dependency(self):
        tree = ast.parse((ROOT / "station_director/staged_schedule.py").read_text())
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertNotIn("fs42.autobump_descriptor", imported)

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
    def _verify_paths_only(self, native, connection, history, media):
        with patch.object(native, "restore_sequence_state"), patch.object(
            native, "_validate_final_cross_channel_exclusions"
        ), patch.object(native, "assert_retained_history"), patch.object(
            native, "coverage_report", return_value={}
        ), patch.object(native, "assert_protected_state"), patch.object(
            native, "canonical_foreign_key_findings", return_value=[]
        ):
            return native._verify_final_preservation(
                connection, {}, {history.channel: history},
                [{"name": history.channel}], {}, [], media,
            )

    def test_generated_playback_descriptor_forms_and_retained_seam(self):
        from station_director import native_single_run as native
        from station_director.staged_schedule import (
            StagedScheduleError,
            capture_channel_history,
            validate_all_catalog_reference_shapes,
        )

        marker = ":autobump:="
        tag = ":autobump:"
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "fixture.db"
            connection = create_database(database)
            ordinary = catalog_row("Action", "/media/ordinary.mp4", "Ordinary", "show")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (1, *[ordinary[key] for key in ordinary]),
            )
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (2, "Action", marker + "catalog", "opaque-title", 7, tag, 0,
                 None, None, None, None, "feature", "video"),
            )
            insert_block(
                connection, "Action", "2026-09-14 05:00:00",
                "2026-09-14 07:00:00", 1, marker + "retained",
            )
            connection.execute(
                "UPDATE liquid_blocks SET liquid_type='LiquidWebBlock',content_json='null' "
                "WHERE station='Action' AND start_time='2026-09-14 05:00:00'"
            )
            connection.commit()
            self.assertEqual(validate_all_catalog_reference_shapes(connection), 1)
            history = capture_channel_history(
                connection, "Action", "2026-09-14 06:00:00",
                "2026-09-15 06:00:00",
            )
            retained_before = connection.execute(
                "SELECT * FROM liquid_blocks WHERE start_time<? ORDER BY id",
                (history.proposal_boundary,),
            ).fetchall()
            insert_block(
                connection, "Action", "2026-09-14 07:00:00",
                "2026-09-15 06:00:00", 1, "/media/ordinary.mp4",
            )
            connection.commit()
            native._inspect_generated_playback(connection, history, "Action")
            retained_after = connection.execute(
                "SELECT * FROM liquid_blocks WHERE start_time<? ORDER BY id",
                (history.proposal_boundary,),
            ).fetchall()
            self.assertEqual(retained_after, retained_before)

            autobump_plan = {
                "path": marker + "opaque", "skip": 0, "duration": 7,
                "is_stream": False, "content_type": "bump", "media_type": "video",
            }
            ordinary_plan = {
                "path": "/media/ordinary.mp4", "skip": 0, "duration": 7,
                "is_stream": False, "content_type": "feature", "media_type": "video",
            }
            for position in range(3):
                plan = [dict(ordinary_plan) for unused in range(3)]
                plan[position] = autobump_plan
                connection.execute(
                    "UPDATE liquid_blocks SET plan_json=? WHERE start_time>=?",
                    (json.dumps(plan), history.proposal_boundary),
                )
                with self.assertRaises(native.NativeRunError) as caught:
                    native._inspect_generated_playback(connection, history, "Action")
                self.assertEqual(caught.exception.code, "autobump_selected")
                self.assertTrue(caught.exception.scheduler_invoked)
                self.assertNotIn("opaque", str(caught.exception))

            invalid = json.dumps([{
                "path": marker + "poison", "skip": 0, "duration": 7,
                "is_stream": False, "content_type": "feature", "media_type": "video",
            }])
            connection.execute(
                "UPDATE liquid_blocks SET plan_json=? WHERE start_time>=?",
                (invalid, history.proposal_boundary),
            )
            with self.assertRaises(native.NativeRunError) as caught:
                native._inspect_generated_playback(connection, history, "Action")
            self.assertEqual(caught.exception.code, "invalid_playback_descriptor")
            self.assertNotIn("poison", str(caught.exception))

            connection.execute(
                "UPDATE liquid_blocks SET liquid_type='LiquidWebBlock',content_json='null',plan_json=? "
                "WHERE start_time>=?",
                (json.dumps([dict(autobump_plan, content_type="feature")]),
                 history.proposal_boundary),
            )
            with self.assertRaises(native.NativeRunError) as caught:
                native._inspect_generated_playback(connection, history, "Action")
            self.assertEqual(caught.exception.code, "autobump_selected")

            for reference in (json.dumps(2), json.dumps([1, 2])):
                connection.execute(
                    "UPDATE liquid_blocks SET liquid_type='LiquidBlock',content_json=?,plan_json=? "
                    "WHERE start_time>=?",
                    (reference, json.dumps([ordinary_plan]), history.proposal_boundary),
                )
                with self.assertRaises(native.NativeRunError) as caught:
                    native._inspect_generated_playback(connection, history, "Action")
                self.assertEqual(caught.exception.code, "autobump_selected")
            connection.execute(
                "UPDATE liquid_blocks SET liquid_type='LiquidBlock',content_json='null' "
                "WHERE start_time>=?",
                (history.regeneration_start,),
            )
            with self.assertRaises(StagedScheduleError):
                native._inspect_generated_playback(connection, history, "Action")
            connection.close()

    def test_final_preservation_is_provenance_aware_for_retained_autobump(self):
        from station_director import native_single_run as native
        from station_director.staged_schedule import (
            generated_playback_representations,
            retained_playback_representations,
        )

        marker = ":autobump:="
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            (media / "ordinary.mp4").write_bytes(b"ordinary")
            connection = create_database(root / "fixture.db")
            ordinary = catalog_row(
                "Action", "/media/ordinary.mp4", "Ordinary", "show"
            )
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (1, *[ordinary[key] for key in ordinary]),
            )
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (2,'Action',?,'retained',7,':autobump:',0,NULL,NULL,NULL,NULL,'feature','video')",
                (marker + "catalog",),
            )
            for start, end in (
                ("2026-09-14 04:00:00", "2026-09-14 05:00:00"),
                ("2026-09-14 05:00:00", "2026-09-14 07:00:00"),
            ):
                insert_block(connection, "Action", start, end, 1, marker + "retained")
                connection.execute(
                    "UPDATE liquid_blocks SET liquid_type='LiquidWebBlock',content_json='null' "
                    "WHERE station='Action' AND start_time=?",
                    (start,),
                )
            insert_block(
                connection, "Action", "2026-09-14 06:30:00",
                "2026-09-14 06:45:00", 2, "/media/ordinary.mp4",
            )
            insert_block(
                connection, "Action", "2026-09-14 07:00:00",
                "2026-09-14 09:00:00", 1, "/media/ordinary.mp4",
            )
            connection.commit()
            history = types.SimpleNamespace(
                channel="Action", proposal_boundary="2026-09-14 06:00:00",
                regeneration_start="2026-09-14 07:00:00",
                effective_horizon="2026-09-14 09:00:00",
            )
            before = connection.execute(
                "SELECT * FROM liquid_blocks ORDER BY id"
            ).fetchall()
            native._inspect_generated_playback(connection, history, "Action")
            between_id = connection.execute(
                "SELECT id FROM liquid_blocks WHERE start_time='2026-09-14 06:30:00'"
            ).fetchone()[0]
            retained_ids = {
                block["block_id"]
                for block in retained_playback_representations(connection, history)
            }
            generated_ids = {
                block["block_id"]
                for block in generated_playback_representations(connection, history)
            }
            self.assertIn(between_id, retained_ids)
            self.assertNotIn(between_id, generated_ids)
            self.assertGreater(
                self._verify_paths_only(native, connection, history, media), 0
            )
            self.assertEqual(
                connection.execute("SELECT * FROM liquid_blocks ORDER BY id").fetchall(),
                before,
            )
            connection.close()

    def test_shared_autobump_catalog_generated_use_fails_at_seam(self):
        from station_director import native_single_run as native

        marker = ":autobump:="
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "media"
            media.mkdir()
            (media / "ordinary.mp4").write_bytes(b"ordinary")
            connection = create_database(root / "fixture.db")
            connection.execute(
                "INSERT INTO catalog_entries "
                "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                "VALUES (2,'Action',?,'shared',7,':autobump:',0,NULL,NULL,NULL,NULL,'feature','video')",
                (marker + "shared",),
            )
            for start, end in (
                ("2026-09-14 05:00:00", "2026-09-14 07:00:00"),
                ("2026-09-14 07:00:00", "2026-09-14 09:00:00"),
            ):
                insert_block(connection, "Action", start, end, 2, "/media/ordinary.mp4")
            connection.commit()
            history = types.SimpleNamespace(
                channel="Action", regeneration_start="2026-09-14 07:00:00",
                effective_horizon="2026-09-14 09:00:00",
            )
            with self.assertRaises(native.NativeRunError) as caught:
                self._verify_paths_only(native, connection, history, media)
            self.assertEqual(caught.exception.code, "autobump_selected")
            connection.close()

    def test_malformed_retained_and_generated_descriptors_fail_closed(self):
        from station_director import native_single_run as native

        marker = ":autobump:="
        for provenance, start in (
            ("retained", "2026-09-14 05:00:00"),
            ("generated", "2026-09-14 07:00:00"),
        ):
            with self.subTest(provenance=provenance), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                media = root / "media"
                media.mkdir()
                connection = create_database(root / "fixture.db")
                ordinary = catalog_row(
                    "Action", "/media/ordinary.mp4", "Ordinary", "show"
                )
                connection.execute(
                    "INSERT INTO catalog_entries "
                    "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (1, *[ordinary[key] for key in ordinary]),
                )
                insert_block(
                    connection, "Action", start, "2026-09-14 09:00:00",
                    1, marker + "malformed",
                )
                connection.commit()
                history = types.SimpleNamespace(
                    channel="Action", regeneration_start="2026-09-14 07:00:00",
                    effective_horizon="2026-09-14 09:00:00",
                )
                with self.assertRaises(native.NativeRunError) as caught:
                    self._verify_paths_only(native, connection, history, media)
                self.assertEqual(caught.exception.code, "invalid_playback_descriptor")
                connection.close()

    def test_final_preservation_confines_ordinary_retained_and_generated_media(self):
        from station_director import native_single_run as native

        for provenance, start in (
            ("retained", "2026-09-14 05:00:00"),
            ("generated", "2026-09-14 07:00:00"),
        ):
            with self.subTest(provenance=provenance), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                media = root / "media"
                media.mkdir()
                connection = create_database(root / "fixture.db")
                missing = catalog_row(
                    "Action", "/media/missing.mp4", "Missing", "show"
                )
                connection.execute(
                    "INSERT INTO catalog_entries "
                    "(id,station,path,title,duration,tag,count,hints,created_at,updated_at,realpath,content_type,media_type) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (1, *[missing[key] for key in missing]),
                )
                insert_block(
                    connection, "Action", start, "2026-09-14 09:00:00",
                    1, "/media/missing.mp4",
                )
                connection.commit()
                history = types.SimpleNamespace(
                    channel="Action", regeneration_start="2026-09-14 07:00:00",
                    effective_horizon="2026-09-14 09:00:00",
                )
                with self.assertRaises(Exception):
                    self._verify_paths_only(native, connection, history, media)
                connection.close()

    def test_exact_descriptor_contract_covers_offair_and_rejects_partial_markers(self):
        from fs42 import autobump_descriptor as descriptor

        base = {
            "path": descriptor.AUTOBUMP_PATH_PREFIX + "opaque",
            "skip": 0, "duration": 7, "is_stream": False,
            "content_type": "feature", "media_type": "video",
        }
        self.assertEqual(
            descriptor.classify_plan_entry(
                base, liquid_type="LiquidWebBlock", plan_size=1,
                content_missing=True,
            ),
            descriptor.DESCRIPTOR_SELECTED,
        )
        self.assertEqual(
            descriptor.classify_plan_entry(
                base, liquid_type="LiquidBlock", plan_size=1,
                content_missing=False,
            ),
            descriptor.DESCRIPTOR_INVALID,
        )
        ordinary = dict(base, path="/media/title-with-autobump-text.mp4")
        self.assertEqual(
            descriptor.classify_plan_entry(
                ordinary, liquid_type="LiquidBlock", plan_size=1,
                content_missing=False,
            ),
            descriptor.DESCRIPTOR_NONE,
        )
        catalog = {
            "path": descriptor.AUTOBUMP_PATH_PREFIX + "opaque",
            "realpath": None, "tag": descriptor.AUTOBUMP_CATALOG_TAG,
            "duration": 7, "content_type": "feature", "media_type": "video",
        }
        self.assertEqual(
            descriptor.classify_catalog_entry(catalog),
            descriptor.DESCRIPTOR_SELECTED,
        )
        for mutation in (
            {"tag": "ordinary"},
            {"path": "/media/ordinary.mp4"},
            {"realpath": "/media/ordinary.mp4"},
            {"content_type": "bump"},
            {"media_type": "audio"},
        ):
            with self.subTest(mutation=mutation):
                self.assertEqual(
                    descriptor.classify_catalog_entry(dict(catalog, **mutation)),
                    descriptor.DESCRIPTOR_INVALID,
                )
        self.assertEqual(
            descriptor.classify_catalog_entry({
                "path": "/media/autobump-title.mp4", "realpath": None,
                "tag": "ordinary-autobump-text", "duration": 7,
                "content_type": "feature", "media_type": "video",
                "title": descriptor.AUTOBUMP_PATH_PREFIX + "ignored",
            }),
            descriptor.DESCRIPTOR_NONE,
        )

    def test_diagnostic_codes_have_exact_scheduler_state_contract(self):
        cases = (
            ("autobump_subprocess_required", "configuration", False),
            ("autobump_subprocess_blocked", "scheduler", True),
            ("autobump_selected", "scheduler", True),
            ("invalid_playback_descriptor", "scheduler", True),
        )
        for code, phase, invoked in cases:
            diagnostic = make_diagnostic(
                code, phase, scheduler_invoked=invoked, channel_number=2
            )
            validate_diagnostic(diagnostic)
            self.assertEqual(diagnostic["scheduler_invoked"], invoked)

    def test_static_native_autobump_reachability_excludes_presentation(self):
        files = (
            ROOT / "station_director/native_single_run.py",
            ROOT / "station_director/staged_schedule.py",
            ROOT / "fs42/autobump_descriptor.py",
        )
        forbidden = {
            "fs42.station_player", "fs42.webrender", "requests", "socket",
            "urllib.request", "http.client", "multiprocessing",
        }
        for path in files:
            tree = ast.parse(path.read_text())
            imported = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module)
            self.assertFalse(imported & forbidden, path)
        descriptor_tree = ast.parse(
            (ROOT / "fs42/autobump_descriptor.py").read_text()
        )
        self.assertFalse(any(
            isinstance(node, (ast.Import, ast.ImportFrom))
            for node in ast.walk(descriptor_tree)
        ))

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
            (root / "confs/synthetic.json").write_text(json.dumps({"station_conf": {
                "network_name": "Synthetic Loop", "channel_number": 2,
                "network_type": "loop", "content_dir": str(media),
                "commercial_free": True, "shuffle_loop": False,
                "autobump": {"title": "Synthetic", "duration": 7},
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
                manager = StationManager()
                self.assertEqual(manager.server_conf["db_path"], "runtime/fs42_fluid.db")
                self.assertTrue(manager.server_conf["start_mpv"])
                self.assertEqual(manager.server_conf["custom_holidays"], {})
                config = manager.station_by_name("Synthetic Loop")
                self.assertIsNotNone(config)
                ShowCatalog(config, rebuild_catalog=True, load=False)
                with activate_validation_context(context):
                    schedule = LiquidSchedule(config)
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore", category=DeprecationWarning,
                        message="The default datetime adapter is deprecated.*")
                    schedule.generate_validation_range(
                        context.start_time, context.end_time, context)
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
            "context", "configuration", "allocation", "catalog", "reconciliation", "retained_history",
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
                    error = RuntimeError(f"injected-{phase}")
                    if phase == "retained_history":
                        from station_director.c1_diagnostics import attach_preservation_detail
                        attach_preservation_detail(error, "assert_retained_history", "schedule_mismatch")
                    raise error

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
                        "retained_history": "assert_retained_history",
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
                from fs42.scheduling_context import active_catalog_ids
                self.assertIsNone(active_catalog_ids('Action'))
                if phase == "retained_history":
                    self.assertEqual(caught.exception.code, "catalog_reconciliation")
                    self.assertEqual(caught.exception.preservation_detail["category"], "schedule_mismatch")
                self.assertTrue(restore.called)
                self.assertEqual(self._sequence_rows(database), before)

    def test_restoration_failure_preserves_original_and_fixed_secondary_outcome(self):
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
                with self.assertRaises(RuntimeError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(str(caught.exception), "original-phase")
            self.assertEqual(caught.exception.restoration_failure, "sequence_restore_failure")
            self.assertTrue(caught.exception.__suppress_context__)

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
        from fs42.scheduling_context import active_catalog_ids

        owner = self

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
                    self.assertIsNotNone(active_catalog_ids('Action'))
                    self.assertEqual(active_catalog_ids('Other'), ())
                    schedule = native_schedule_class.__new__(native_schedule_class)
                    schedule.conf = config
                    schedule.catalog = types.SimpleNamespace(clip_index={})

                    def fluid(self, start, end):
                        connection = sqlite3.connect("runtime/fs42_fluid.db")
                        catalog_id = connection.execute(
                            "SELECT id FROM catalog_entries WHERE station='Action' AND path='/media/a.mp4'"
                        ).fetchone()[0]
                        owner.assertIn(catalog_id, active_catalog_ids('Action'))
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
                self.assertIsNone(active_catalog_ids('Action'))
                self.assertTrue(result["scheduler_invoked"])
                self.assertEqual(result["preservation"]["sequence_tables_restored"], "pass")
        self.assertEqual(results[0], results[1])

    def test_autobump_probe_requirement_rejects_before_scheduler_entry(self):
        from station_director import native_single_run as native

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            projected["Action"]["station_conf"]["autobump"] = {
                "title": "Synthetic", "bg_video": "opaque"
            }
            catalog = Mock()
            schedule = Mock()
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(
                native, "ShowCatalog", catalog
            ), patch.object(native, "LiquidSchedule", schedule):
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "autobump_subprocess_required")
            self.assertEqual(caught.exception.channel, "Action")
            self.assertFalse(caught.exception.scheduler_invoked)
            catalog.assert_not_called()
            schedule.assert_not_called()

    def test_native_validation_defense_maps_blocked_probe_after_scheduler_entry(self):
        from fs42.autobump_agent import AutoBumpAgent
        from station_director import native_single_run as native

        class Catalog:
            def __init__(self, *unused, **unused_kwargs):
                pass

        class Schedule:
            def __init__(self, unused_config):
                self.catalog = types.SimpleNamespace(clip_index={})
                AutoBumpAgent.get_bg_video_duration("opaque")

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            projected["Action"]["station_conf"]["autobump"] = {
                "title": "Synthetic", "duration": 7
            }
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(
                native, "ShowCatalog", Catalog
            ), patch.object(native, "LiquidSchedule", Schedule), patch(
                "fs42.autobump_agent.subprocess.run"
            ) as runner:
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "autobump_subprocess_blocked")
            self.assertEqual(caught.exception.channel, "Action")
            self.assertTrue(caught.exception.scheduler_invoked)
            runner.assert_not_called()

    def test_cache_metadata_absence_has_its_distinct_fixed_catalog_diagnostic(self):
        from fs42.scheduling_context import ValidationCatalogMetadataUnavailable
        from station_director import native_single_run as native

        class Catalog:
            def __init__(self, *unused, **unused_kwargs):
                raise ValidationCatalogMetadataUnavailable(
                    "private path and media identity")

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(
                native, "ShowCatalog", Catalog
            ), patch.object(native, "LiquidSchedule") as schedule:
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "catalog_metadata_unavailable")
            self.assertEqual(caught.exception.phase, "catalog")
            self.assertFalse(caught.exception.scheduler_invoked)
            self.assertNotIn("private", str(caught.exception))
            schedule.assert_not_called()

    def test_malformed_autobump_remains_native_configuration_error(self):
        from station_director import native_single_run as native

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            projected["Action"]["station_conf"]["autobump"] = "malformed"
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(
                native, "ShowCatalog"
            ) as catalog, patch.object(native, "LiquidSchedule") as schedule:
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "native_configuration")
            self.assertFalse(caught.exception.scheduler_invoked)
            catalog.assert_not_called()
            schedule.assert_not_called()

    def test_selected_autobump_rejects_after_scheduler_before_media_validation(self):
        from station_director import native_single_run as native

        class Catalog:
            def __init__(self, *unused, **unused_kwargs):
                pass

        class Schedule:
            def __init__(self, unused_config):
                self.catalog = types.SimpleNamespace(clip_index={})

            def generate_validation_range(self, start, end, unused_context):
                connection = sqlite3.connect("runtime/fs42_fluid.db")
                insert_block(connection, "Action", str(start), str(end), 1, "/media/a.mp4")
                connection.execute(
                    "UPDATE liquid_blocks SET plan_json=? WHERE station='Action' AND start_time>=?",
                    (json.dumps([{
                        "path": ":autobump:=opaque", "skip": 0, "duration": 7,
                        "is_stream": False, "content_type": "bump",
                        "media_type": "video",
                    }]), str(start)),
                )
                connection.commit()
                connection.close()

        with tempfile.TemporaryDirectory() as directory:
            work, media, request, projected = self._fixture(Path(directory))
            projected["Action"]["station_conf"]["autobump"] = {
                "title": "Synthetic", "duration": 7
            }
            with patch.object(
                native, "_verified_work_tree",
                return_value=(work, projected, ["Action"], {"Action": 2}, 0),
            ), patch.object(
                native, "_native_station_config",
                side_effect=lambda channel, unused_context: projected[channel]["station_conf"],
            ), patch.object(native, "MEDIA_ROOT", media), patch.object(
                native, "ShowCatalog", Catalog
            ), patch.object(native, "LiquidSchedule", Schedule), patch.object(
                native, "_verify_final_preservation"
            ) as media_validation:
                with self.assertRaises(native.NativeRunError) as caught:
                    native.execute_native_single_run(request, test_attestation(request))
            self.assertEqual(caught.exception.code, "autobump_selected")
            self.assertTrue(caught.exception.scheduler_invoked)
            self.assertNotIn("opaque", str(caught.exception))
            media_validation.assert_not_called()

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
            "schema_version": 4, "operation": "native_single_run",
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

    def test_response_construction_and_publication_failures_leave_fixed_checkpoint(self):
        for target in ("_base_response", "write_private_json_exclusive"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                request, request_path = self._request_file(root)
                patches = [patch(
                    "station_director.single_run_worker.attest_before_native_import",
                    return_value=(probes(request["run_id"]), test_attestation(request)),
                )]
                if target == "_base_response":
                    patches.append(patch(
                        "station_director.single_run_worker._base_response",
                        side_effect=RuntimeError("opaque")))
                else:
                    fake = types.SimpleNamespace(execute_native_single_run=lambda *unused: {
                        "channels": [passing_channel()],
                        "preservation": {"retained_history": "pass", "protected_channels": "pass",
                                         "sequence_tables_restored": "pass", "foreign_key_baseline": "pass"},
                        "path_validation": {"passed": True, "mapping_count": 0, "scheduled_path_checks": 0},
                        "guide_validation": passing_guide(), "timings_ms": {},
                        "verification": passing_verification(), "scheduler_invoked": True,
                    })
                    patches.extend((
                        patch("station_director.single_run_worker.write_private_json_exclusive",
                              side_effect=OSError("opaque")),
                        patch("station_director.single_run_worker.importlib.import_module",
                              return_value=fake),
                    ))
                with ExitStack() as stack:
                    for active in patches:
                        stack.enter_context(active)
                    with self.assertRaises((RuntimeError, OSError)):
                        run_worker(request_path, root / "response.json")
                evidence = read_checkpoint_evidence(root)
                self.assertTrue(evidence.response_attempted)
                self.assertFalse(evidence.response_completed)

    def test_dependency_free_rules_are_strict_and_value_free(self):
        source = (ROOT / "station_director/c1_diagnostics.py").read_text()
        tree = ast.parse(source)
        imports = {alias.name for node in ast.walk(tree)
                   if isinstance(node, ast.Import) for alias in node.names}
        imports.update(node.module for node in ast.walk(tree)
                       if isinstance(node, ast.ImportFrom))
        self.assertEqual(imports, {"contextlib", "functools"})
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
                self.assertEqual(result["schema_version"], 4)
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
            ("autobump_subprocess_required", "configuration", False),
            ("catalog_failure", "catalog", False),
            ("catalog_metadata_unavailable", "catalog", False),
            ("scheduler_failure", "scheduler", True),
            ("autobump_subprocess_blocked", "scheduler", True),
            ("autobump_selected", "scheduler", True),
            ("invalid_playback_descriptor", "scheduler", True),
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
                detail = {"helper": "restore_sequence_state", "category": "insert_failed", "content_scope": None}
                failure.preservation_detail = detail
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
                self.assertEqual(result["failure"]["preservation_detail"], detail)
                stored = json.loads((root / "response.json").read_text())
                self.assertEqual(stored["failure"]["preservation_detail"], detail)
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

    def test_frozen_v2_remains_readable_but_production_rejects_downgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, unused = self._request_file(root)
            response = self._success_response(request)
            response["schema_version"] = 2
            path = root / "native-single-run.response.json"
            write_private_json_exclusive(path, response, RESPONSE_SCHEMA_V2)
            with HeldDocument(path, RESPONSE_SCHEMA_V2) as retained:
                self.assertEqual(retained.payload["schema_version"], 2)
            lifecycle = SingleRunLifecycle(
                ROOT, root, types.SimpleNamespace(closed=False), "token",
                request["run_id"], "unit", request,
                launcher_result=LaunchResult("unit", 0, "", ""))
            with self.assertRaises(SingleRunError) as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"],
                             "worker_response_invalid")

    def test_frozen_v3_diagnostic_readable_current_worker_requires_v4(self):
        from station_director.single_run_protocol import RESPONSE_SCHEMA_V3
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, unused = self._request_file(root)
            response = self._success_response(request)
            response["schema_version"] = 3
            warning = make_diagnostic("native_warning", "configuration")
            warning.pop("preservation_detail")
            response["warnings"] = [warning]
            path = root / "native-single-run.response.json"
            write_private_json_exclusive(path, response, RESPONSE_SCHEMA_V3)
            with HeldDocument(path, RESPONSE_SCHEMA_V3) as retained:
                self.assertEqual(retained.payload, response)
            with self.assertRaises(ProtocolError):
                with HeldDocument(path, RESPONSE_SCHEMA):
                    pass

    def test_valid_v3_response_cannot_bypass_missing_checkpoint_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, unused = self._request_file(root)
            response = self._success_response(request)
            write_private_json_exclusive(
                root / "native-single-run.response.json", response, RESPONSE_SCHEMA)
            lifecycle = SingleRunLifecycle(
                ROOT, root, types.SimpleNamespace(closed=False), "token",
                request["run_id"], "unit", request,
                launcher_result=LaunchResult("unit", 0, "", ""))
            with self.assertRaises(SingleRunError) as caught:
                inspect_single_run(lifecycle)
            self.assertEqual(caught.exception.c1_diagnostic["code"],
                             "worker_checkpoint_mismatch")
            self.assertEqual(caught.exception.scheduler_state, "unknown")

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
                    publish_completed_worker_checkpoints(root)
                with self.assertRaises(SingleRunError) as caught:
                    inspect_single_run(lifecycle)
                self.assertEqual(caught.exception.c1_diagnostic["code"], code)
                self.assertEqual(set(caught.exception.launcher_summary), {
                    "outcome", "stdout_bytes", "stderr_bytes", "stdout_truncated",
                    "stderr_truncated", "termination_kind", "exit_status", "signal"})
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

            def launch(command, unit, timeout, *, supervise_retained_unit):
                self.assertTrue(supervise_retained_unit)
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

            def replace_during_launch(
                unused_command, unit, unused_timeout, *, supervise_retained_unit,
            ):
                self.assertTrue(supervise_retained_unit)
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

            def launch(command, unit, timeout, *, supervise_retained_unit):
                self.assertTrue(supervise_retained_unit)
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
                "schema_version": 4, "operation": "native_single_run",
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
            publish_completed_worker_checkpoints(root)
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
                with self.assertRaisesRegex(SingleRunError, "could not be proven absent"):
                    lifecycle.cleanup()
            self.assertFalse(lifecycle.lock.closed)


if __name__ == "__main__":
    unittest.main()
