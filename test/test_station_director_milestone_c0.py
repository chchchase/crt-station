import ast
import asyncio
import contextvars
import datetime
import importlib.util
import io
import json
import os
import random
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from contextlib import contextmanager, nullcontext
from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import Mock, patch

from fs42.scheduling_context import (
    ValidationSchedulingContext,
    activate_validation_context,
    scheduling_now,
    scheduling_random,
    validation_order,
)
from station_director import cli, isolation, isolation_probe, validation
from station_director.stage_runner import run_worker
from station_director.validation_context import (
    canonical_seed_inputs,
    derive_validation_context,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
    proposal_boundary_to_db,
)


START = datetime.datetime(2026, 9, 14)
END = datetime.datetime(2026, 9, 21)


def context(seed=123):
    return ValidationSchedulingContext(START, START, END, seed)


def context_payload(seed=123):
    proposal = proposal_payload(seed)
    return derive_validation_context(proposal, {}, seed_input_payload())


def proposal_payload(seed=42, start="2026-09-14T00:00:00-07:00", end="2026-09-21T00:00:00-07:00"):
    return {
        "proposal_id": "p",
        "seed": seed,
        "week_start": start,
        "week_end": end,
    }


def seed_input_payload(config="configs", database="database", media="media"):
    return canonical_seed_inputs(config, database, media)


def probe_payload(run_id, passed=True):
    results = {
        name: {"passed": passed, "detail": name}
        for name in isolation_probe.PROBE_RESULTS
    }
    return {
        "schema_version": isolation_probe.PROBE_SCHEMA_VERSION,
        "run_id": run_id,
        "overall_pass": passed,
        "results": results,
    }


def load_api_module(name):
    class Router:
        def __init__(self, **unused):
            pass

        def get(self, unused_path):
            return lambda function: function

    fastapi = types.ModuleType("fastapi")
    fastapi.APIRouter = Router
    path = Path(__file__).parents[1] / f"fs42/fs42_server/api/{name}.py"
    spec = importlib.util.spec_from_file_location(f"c0_{name}", path)
    module = importlib.util.module_from_spec(spec)
    with native_import_dependencies(), patch.dict(sys.modules, {"fastapi": fastapi}):
        spec.loader.exec_module(module)
    return module


@contextmanager
def native_import_dependencies():
    ffmpeg = types.ModuleType("ffmpeg")
    ffmpeg.probe = Mock()
    moviepy = types.ModuleType("moviepy")
    moviepy.VideoFileClip = Mock()
    with patch.dict(sys.modules, {"ffmpeg": ffmpeg, "moviepy": moviepy}):
        yield


class SchedulingContextTests(unittest.TestCase):
    def test_context_rejects_invalid_clock_range_seed_and_timezone(self):
        invalid = (
            ((None, START, END, 1), TypeError),
            ((START, END, START, 1), ValueError),
            ((START, START, END, True), TypeError),
        )
        for arguments, error in invalid:
            with self.subTest(arguments=arguments), self.assertRaises(error):
                ValidationSchedulingContext(*arguments)
        with self.assertRaises(ValueError):
            ValidationSchedulingContext(START, START, END, 1, timezone="UTC")

    def test_rng_clock_and_order_are_deterministic_only_in_context(self):
        values = [3, 1, 2]
        self.assertIs(validation_order(values), values)
        self.assertIs(scheduling_random(), random)
        before = datetime.datetime.now()
        actual = scheduling_now()
        after = datetime.datetime.now()
        self.assertLessEqual(before, actual)
        self.assertLessEqual(actual, after)

        generated = []
        for unused in range(2):
            with activate_validation_context(context(99)):
                self.assertEqual(scheduling_now(), START)
                self.assertEqual(validation_order(values), [1, 2, 3])
                generated.append([scheduling_random().randrange(1000) for _ in range(5)])
        self.assertEqual(generated[0], generated[1])

    def test_context_is_immutable_and_activation_expires_on_normal_exit(self):
        ctx = context()
        with self.assertRaises(FrozenInstanceError):
            ctx.seed = 999
        with self.assertRaises(AttributeError):
            unused = ctx.rng
        with activate_validation_context(ctx):
            proxy = scheduling_random()
            proxy.randrange(10)
        with self.assertRaises(RuntimeError):
            proxy.randrange(10)
        self.assertIs(scheduling_random(), random)

    def test_activation_resets_and_proxy_expires_after_exception(self):
        proxy = None
        with self.assertRaisesRegex(ValueError, "boom"):
            with activate_validation_context(context()):
                proxy = scheduling_random()
                raise ValueError("boom")
        with self.assertRaises(RuntimeError):
            proxy.random()
        self.assertIs(scheduling_random(), random)

    def test_nested_activation_fails_closed_without_disturbing_outer(self):
        with activate_validation_context(context(7)):
            first = scheduling_random().randrange(100)
            with self.assertRaises(RuntimeError):
                with activate_validation_context(context(8)):
                    pass
            second = scheduling_random().randrange(100)
        expected = random.Random(7)
        self.assertEqual((first, second), (expected.randrange(100), expected.randrange(100)))

    def test_inherited_async_task_cannot_use_parent_activation(self):
        async def exercise():
            with activate_validation_context(context()):
                async def child():
                    scheduling_now()

                with self.assertRaises(RuntimeError):
                    await asyncio.create_task(child())

        asyncio.run(exercise())

    def test_copied_context_cannot_retain_activation_after_exit(self):
        with activate_validation_context(context()):
            copied = contextvars.copy_context()
        with self.assertRaises(RuntimeError):
            copied.run(scheduling_now)

    def test_inherited_context_cannot_cross_threads(self):
        errors = []
        with activate_validation_context(context()):
            copied = contextvars.copy_context()
            thread = threading.Thread(
                target=lambda: self._capture_error(
                    errors, lambda: copied.run(scheduling_now)
                )
            )
            thread.start()
            thread.join()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)

    @staticmethod
    def _capture_error(errors, callback):
        try:
            callback()
        except Exception as exc:
            errors.append(exc)

    def test_default_slot_randomness_matches_module_global_random(self):
        from fs42.slot_reader import SlotReader

        slot = {"tags": ["a", "b", "c"], "random_tags": True}
        random.seed(451)
        expected_index = random.randrange(3)
        random.seed(451)
        selected, index = SlotReader.get_tag_from_slot(slot, START)
        self.assertEqual(index, expected_index)
        self.assertEqual(selected, slot["tags"][expected_index])

    def test_filesystem_order_changes_only_in_validation_context(self):
        with native_import_dependencies():
            import fs42.media_processor as media_processor
        MediaProcessor = media_processor.MediaProcessor

        with patch.object(MediaProcessor, "VIDEO_FORMATS", ["mp4"]), patch.object(
            media_processor.glob, "glob", return_value=["z.mp4", "a.mp4"]
        ):
            self.assertEqual(MediaProcessor._find_media("unused"), ["z.mp4", "a.mp4"])
            with activate_validation_context(context()):
                self.assertEqual(MediaProcessor._find_media("unused"), ["a.mp4", "z.mp4"])

    def test_deleted_child_order_changes_only_in_validation_context(self):
        with native_import_dependencies():
            import fs42.sequence_api as sequence_api
        children = ["show/z", "show/a", "show/m"]
        station = {
            "network_name": "Action",
            "content_dir": "/media",
            "clip_shows": {},
        }
        slot = {"sequence": "episodes", "sequence_strategy": "random_show"}

        def run_once(validation_context=None):
            sequence_io = Mock()
            sequence_io.get_sequence.return_value = None
            sequence_io.get_child_sequences.return_value = children
            manager = (
                activate_validation_context(validation_context)
                if validation_context is not None else nullcontext()
            )
            with patch.object(sequence_api, "SequenceIO", return_value=sequence_io), patch.object(
                sequence_api.SequenceAPI, "_find_show_dirs", return_value=[]
            ), manager:
                sequence_api.SequenceAPI._build_sequence(station, "show", slot)
            return [call.args[2] for call in sequence_io.delete_sequence.call_args_list]

        self.assertEqual(run_once(), list(set(children)))
        self.assertEqual(run_once(context()), sorted(children))

    def test_catalog_tie_order_changes_only_in_validation_context(self):
        from fs42.catalog_io import CatalogIO

        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "catalog.db")
            manager = Mock(server_conf={"db_path": database})
            with patch("fs42.catalog_io.StationManager", return_value=manager):
                catalog = CatalogIO()
                connection = sqlite3.connect(database)
                connection.executemany(
                    "INSERT INTO catalog_entries "
                    "(station,path,title,duration,tag,count) VALUES (?,?,?,?,?,?)",
                    [
                        ("Action", "z.mp4", "Same", 1.0, "show", 0),
                        ("Action", "a.mp4", "Same", 1.0, "show", 0),
                    ],
                )
                connection.commit()
                connection.close()
                self.assertEqual(
                    [entry.path for entry in catalog.get_catalog_entries("Action")],
                    ["z.mp4", "a.mp4"],
                )
                with activate_validation_context(context()):
                    self.assertEqual(
                        [entry.path for entry in catalog.get_catalog_entries("Action")],
                        ["a.mp4", "z.mp4"],
                    )

    def test_explicit_range_requires_matching_context_and_bypasses_increment(self):
        with native_import_dependencies():
            import fs42.liquid_schedule as liquid_schedule
        LiquidSchedule = liquid_schedule.LiquidSchedule

        schedule = LiquidSchedule.__new__(LiquidSchedule)
        schedule.conf = {"network_type": "standard"}
        schedule._fluid = Mock(return_value="generated")
        schedule._increment = Mock(side_effect=AssertionError("must not be used"))
        ctx = context()
        self.assertEqual(
            schedule.generate_validation_range(START, END, ctx), "generated"
        )
        schedule._fluid.assert_called_once_with(START, END)
        schedule._increment.assert_not_called()
        for arguments in ((None, END, ctx), (START, None, ctx), (START, END, None)):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                schedule.generate_validation_range(*arguments)
        with self.assertRaises(ValueError):
            schedule.generate_validation_range(START + datetime.timedelta(hours=1), END, ctx)
        with self.assertRaises(ValueError):
            schedule.generate_validation_range(END, START, ctx)
        aware = START.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
        with self.assertRaises(ValueError):
            schedule.generate_validation_range(aware, END, ctx)

    def test_exclusion_failure_is_warning_by_default_and_fatal_in_validation(self):
        with native_import_dependencies():
            import fs42.liquid_schedule as liquid_schedule
        LiquidSchedule = liquid_schedule.LiquidSchedule

        schedule = LiquidSchedule.__new__(LiquidSchedule)
        schedule.conf = {"content_dir": "/media", "network_name": "Action"}
        schedule._l = Mock()
        failure = sqlite3.OperationalError("unavailable")
        with patch.object(liquid_schedule, "StationManager", side_effect=failure):
            self.assertEqual(schedule._build_exclusion_index(START, END), {})
        schedule._l.warning.assert_called_once()
        with activate_validation_context(context()), patch.object(
            liquid_schedule, "StationManager", side_effect=failure
        ):
            with self.assertRaises(sqlite3.OperationalError):
                schedule._build_exclusion_index(START, END)

    def test_candidate_exclusion_fallback_is_disabled_only_in_validation(self):
        with native_import_dependencies():
            import fs42.liquid_schedule as liquid_schedule
        MatchingContentNotFound = liquid_schedule.MatchingContentNotFound
        LiquidSchedule = liquid_schedule.LiquidSchedule

        candidate = Mock(path="/media/show.mp4", duration=1200, title="Show")
        schedule = LiquidSchedule.__new__(LiquidSchedule)
        schedule.conf = {
            "network_name": "Action",
            "content_dir": "/media",
            "clip_shows": {},
            "break_strategy": "end",
            "schedule_increment": 30,
        }
        schedule._l = Mock()
        schedule._break_info = Mock(return_value=({}, "end", 30))
        schedule.catalog = Mock()
        schedule.catalog.find_candidate.side_effect = [
            MatchingContentNotFound("excluded"),
            candidate,
        ]
        with patch.object(liquid_schedule.PathQuery, "match_any_from_base", return_value=None):
            block, unused_end = schedule._fill(
                {"tags": "show"}, "show", START,
                exclusion_index={"/media/show.mp4": [(START, END)]},
            )
        self.assertEqual(block.content, candidate)
        self.assertEqual(schedule.catalog.find_candidate.call_count, 2)

        schedule.catalog.find_candidate.reset_mock()
        schedule.catalog.find_candidate.side_effect = MatchingContentNotFound("excluded")
        with activate_validation_context(context()), patch.object(
            liquid_schedule.PathQuery, "match_any_from_base", return_value=None
        ):
            with self.assertRaises(MatchingContentNotFound):
                schedule._fill(
                    {"tags": "show"}, "show", START,
                    exclusion_index={"/media/show.mp4": [(START, END)]},
                )
        self.assertEqual(schedule.catalog.find_candidate.call_count, 1)


class EnvironmentAndSeedTests(unittest.TestCase):
    def test_launcher_sets_timezone_and_hash_seed_before_interpreter(self):
        command = isolation.build_bwrap_command(
            Path.cwd(), Path("/tmp/fs42-i-aaaaaaaaaaaa"), ["/usr/bin/python3", "worker.py"]
        )
        separator = command.index("--")
        for key, value in (
            ("TZ", "America/Los_Angeles"),
            ("PYTHONHASHSEED", "0"),
        ):
            index = command.index(key)
            self.assertLess(index, separator)
            self.assertEqual(command[index + 1], value)
        self.assertGreater(command.index("/usr/bin/python3"), separator)

    def test_probe_contract_requires_exact_new_environment(self):
        self.assertEqual(isolation_probe.EXPECTED_ENVIRONMENT["TZ"], "America/Los_Angeles")
        self.assertEqual(isolation_probe.EXPECTED_ENVIRONMENT["PYTHONHASHSEED"], "0")
        self.assertTrue(
            isolation_probe.check_environment(isolation_probe.EXPECTED_ENVIRONMENT)["passed"]
        )
        missing = dict(isolation_probe.EXPECTED_ENVIRONMENT)
        missing.pop("PYTHONHASHSEED")
        self.assertFalse(isolation_probe.check_environment(missing)["passed"])

    def test_seed_is_canonical_and_independently_sensitive_to_five_inputs(self):
        proposal = proposal_payload()
        policy = {"b": 2, "a": 1}
        inputs = seed_input_payload()
        first = derive_validation_context(proposal, policy, inputs)
        reordered = derive_validation_context(
            dict(reversed(list(proposal.items()))), {"a": 1, "b": 2},
            dict(reversed(list(inputs.items()))),
        )
        self.assertEqual(first, reordered)
        variants = [
            ({**proposal, "proposal_id": "changed"}, policy, inputs),
            (proposal, {**policy, "a": 9}, inputs),
            (proposal, policy, seed_input_payload(config="changed")),
            (proposal, policy, seed_input_payload(database="changed")),
            (proposal, policy, seed_input_payload(media="changed")),
        ]
        for proposal_value, policy_value, input_value in variants:
            with self.subTest(variant=(proposal_value, policy_value, input_value)):
                changed = derive_validation_context(
                    proposal_value, policy_value, input_value
                )
                self.assertNotEqual(first["effective_seed"], changed["effective_seed"])
        requested_only = derive_validation_context(
            {**proposal, "seed": 999}, policy, inputs
        )
        self.assertEqual(first["effective_seed"], requested_only["effective_seed"])
        self.assertEqual(first["requested_seed"], 42)
        self.assertEqual(requested_only["requested_seed"], 999)
        self.assertNotIn("seed", first)
        self.assertEqual(len(first["input_fingerprint"]), 64)

    def test_cli_labels_requested_and_effective_seeds(self):
        report = {
            "proposal_id": "p",
            "valid": False,
            "validation_context": {
                "requested_seed": 42,
                "effective_seed": 99,
            },
            "failures": [],
            "warnings": [],
            "comparison": None,
        }
        output = io.StringIO()
        with redirect_stdout(output):
            cli._print_validation(report)
        self.assertIn("Requested seed: 42", output.getvalue())
        self.assertIn("Effective seed: 99", output.getvalue())
        self.assertNotIn("Validation seed:", output.getvalue())

    def test_logical_config_fingerprint_ignores_format_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.json"
            second = root / "second.json"
            first.write_text('{"b": [1, 2.0], "a": true}')
            second.write_text('{\n  "a": true,\n  "b": [1, 2.0]\n}\n')
            before = logical_protected_configuration_fingerprint(
                {"confs/action.json": first}
            )
            os.chmod(first, 0o600)
            os.utime(first, (1_000_000_000, 1_000_000_000))
            after_metadata = logical_protected_configuration_fingerprint(
                {"confs/action.json": first}
            )
            reformatted = logical_protected_configuration_fingerprint(
                {"confs\\action.json": second}
            )
        self.assertEqual(before, after_metadata)
        self.assertEqual(before, reformatted)

    def test_logical_config_fingerprint_rejects_bad_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid.json"
            invalid = root / "invalid.json"
            duplicate = root / "duplicate.json"
            nonfinite = root / "nonfinite.json"
            valid.write_text("{}")
            invalid.write_text("{")
            duplicate.write_text('{"a": 1, "a": 2}')
            nonfinite.write_text('{"a": NaN}')
            for path in (invalid, duplicate, nonfinite):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    logical_protected_configuration_fingerprint({"confs/a.json": path})
            with self.assertRaises(ValueError):
                logical_protected_configuration_fingerprint(
                    {"confs/a.json": valid, "confs\\a.json": valid}
                )
            with self.assertRaises(TypeError):
                derive_validation_context(
                    {**proposal_payload(), "unsupported": (1, 2)}, {}, seed_input_payload()
                )

    def test_logical_media_fingerprint_ignores_physical_metadata(self):
        base = {
            "path_b64": "c2hvdy5tcDQ=",
            "type": 32768,
            "size": 123,
            "symlink_target_b64": None,
            "mode": 0o644,
            "uid": 1000,
            "gid": 1000,
            "device": 1,
            "inode": 2,
            "mtime_ns": 3,
            "ctime_ns": 4,
            "xattrs": {"supported": True, "values": []},
        }

        def fingerprint(record):
            stream = io.BytesIO((json.dumps(record) + "\n").encode())
            manifest = types.SimpleNamespace(stream=stream)
            return logical_media_manifest_fingerprint(manifest)

        changed_metadata = {
            **base, "mode": 0o600, "uid": 9, "gid": 8, "device": 7,
            "inode": 6, "mtime_ns": 5, "ctime_ns": 4,
        }
        self.assertEqual(fingerprint(base), fingerprint(changed_metadata))
        self.assertNotEqual(fingerprint(base), fingerprint({**base, "size": 124}))

    def test_proposal_boundaries_require_los_angeles_offsets(self):
        self.assertEqual(
            proposal_boundary_to_db("2026-01-12T00:00:00-08:00"),
            "2026-01-12 00:00:00",
        )
        self.assertEqual(
            proposal_boundary_to_db("2026-07-12T00:00:00-07:00"),
            "2026-07-12 00:00:00",
        )
        # Both fall-back folds are resolvable because their explicit offsets differ.
        self.assertEqual(
            proposal_boundary_to_db("2026-11-01T01:30:00-07:00"),
            "2026-11-01 01:30:00",
        )
        self.assertEqual(
            proposal_boundary_to_db("2026-11-01T01:30:00-08:00"),
            "2026-11-01 01:30:00",
        )
        rejected = (
            "2026-09-14T00:00:00+00:00",
            "2026-09-14T00:00:00",
            "2026-03-08T02:30:00-08:00",
            "2026-11-01T01:30:00-06:00",
        )
        for value in rejected:
            with self.subTest(value=value), self.assertRaises(ValueError):
                proposal_boundary_to_db(value)

    def test_reversed_proposal_range_is_rejected(self):
        with self.assertRaises(ValueError):
            derive_validation_context(
                proposal_payload(
                    start="2026-09-21T00:00:00-07:00",
                    end="2026-09-14T00:00:00-07:00",
                ),
                {},
                seed_input_payload(),
            )


class ImportBoundaryTests(unittest.TestCase):
    def _request(self, directory):
        proposal = proposal_payload()
        inputs = seed_input_payload()
        request = {
            "schema_version": 1,
            "run_id": "c0-import-test",
            "proposal": proposal,
            "policy": {},
            "seed_inputs": inputs,
            "validation_context": derive_validation_context(proposal, {}, inputs),
        }
        path = Path(directory) / "request.json"
        path.write_text(json.dumps(request))
        return path

    def test_stage_runner_has_no_top_level_native_import(self):
        path = Path(__file__).parents[1] / "station_director/stage_runner.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        native = []
        for node in tree.body:
            if isinstance(node, ast.Import):
                native.extend(name.name for name in node.names if name.name.startswith("fs42"))
            elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith("fs42"):
                native.append(node.module)
        self.assertEqual(native, [])

    def test_failed_probe_prevents_dynamic_native_import(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result.json"
            with patch(
                "station_director.stage_runner._load_native_validation_context"
            ) as loader:
                run_worker(
                    self._request(directory),
                    result,
                    project_root=Path(directory) / "missing",
                    stage_root=directory,
                    media_root=Path(directory) / "missing-media",
                    probe_builder=lambda run_id, **unused: probe_payload(run_id, False),
                )
            loader.assert_not_called()

    def test_dynamic_import_follows_successful_probe(self):
        events = []

        def probes(run_id, **unused):
            events.append("probes")
            return probe_payload(run_id)

        def load_context(unused):
            events.append("native-import")
            raise ValueError("stop after import boundary")

        with tempfile.TemporaryDirectory() as directory, patch(
            "station_director.stage_runner._load_native_validation_context",
            side_effect=load_context,
        ):
            run_worker(
                self._request(directory),
                Path(directory) / "result.json",
                project_root=Path(directory) / "missing",
                stage_root=directory,
                media_root=Path(directory) / "missing-media",
                probe_builder=probes,
            )
        self.assertEqual(events, ["probes", "native-import"])

    def test_tampered_effective_seed_is_rejected_before_native_import(self):
        with tempfile.TemporaryDirectory() as directory:
            request_path = self._request(directory)
            request = json.loads(request_path.read_text())
            request["validation_context"]["effective_seed"] += 1
            request_path.write_text(json.dumps(request))
            with patch(
                "station_director.stage_runner._load_native_validation_context"
            ) as loader:
                payload = run_worker(
                    request_path,
                    Path(directory) / "result.json",
                    project_root=Path(directory) / "missing",
                    stage_root=directory,
                    media_root=Path(directory) / "missing-media",
                    probe_builder=lambda run_id, **unused: probe_payload(run_id),
                )
        loader.assert_not_called()
        self.assertEqual(payload["status"], "failed")
        self.assertFalse(payload["scheduler_invoked"])


class GuidePayloadRefactorTests(unittest.TestCase):
    @staticmethod
    def _block(title, start, end, meta=None):
        block = Mock(title=title, start_time=start, end_time=end, content=None)
        block.meta = meta
        return block

    def test_pure_schedule_builder_fixed_payload_metadata_boundaries_and_order(self):
        schedules = load_api_module("schedules")
        first = self._block(
            "First", START, START + datetime.timedelta(hours=1), {"genre": "Action"}
        )
        second = self._block("Second", START + datetime.timedelta(hours=1), END)
        by_station = {"Channel Z": [first, second], "Channel A": []}
        expected = {
            "start": START.isoformat(),
            "end": END.isoformat(),
            "schedules": {
                "Channel Z": [
                    {
                        "title": "First",
                        "start_time": START.isoformat(),
                        "end_time": (START + datetime.timedelta(hours=1)).isoformat(),
                        "meta": {"genre": "Action"},
                    },
                    {
                        "title": "Second",
                        "start_time": (START + datetime.timedelta(hours=1)).isoformat(),
                        "end_time": END.isoformat(),
                    },
                ],
                "Channel A": [],
            },
        }
        with patch.object(schedules.LiquidAPI, "get_all_blocks", side_effect=AssertionError), patch.object(
            schedules, "StationManager", side_effect=AssertionError
        ):
            built = schedules.build_all_schedules_payload(
                START.isoformat(), END.isoformat(), by_station, include_meta=True
            )
        self.assertEqual(built, expected)
        self.assertEqual(list(built["schedules"]), ["Channel Z", "Channel A"])

    def test_schedule_route_preserves_success_empty_invalid_and_error_behavior(self):
        schedules = load_api_module("schedules")
        block = self._block("Program", START, END)
        expected = {
            "start": START.isoformat(),
            "end": END.isoformat(),
            "schedules": {
                "Action": [{
                    "title": "Program",
                    "start_time": START.isoformat(),
                    "end_time": END.isoformat(),
                }]
            },
        }
        with patch.object(
            schedules.LiquidAPI, "get_all_blocks", return_value={"Action": [block]}
        ) as lookup:
            self.assertEqual(
                schedules.get_all_schedules(START.isoformat(), END.isoformat()), expected
            )
        lookup.assert_called_once_with(START, END)
        with patch.object(schedules.LiquidAPI, "get_all_blocks", return_value={}):
            self.assertEqual(
                schedules.get_all_schedules(START.isoformat(), END.isoformat()),
                {"start": START.isoformat(), "end": END.isoformat(), "schedules": {}},
            )
        self.assertEqual(
            schedules.get_all_schedules(),
            {"error": "start and end are both required."},
        )
        self.assertEqual(
            schedules.get_all_schedules("bad", END.isoformat()),
            {"error": "Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS) for start and end."},
        )
        with patch.object(
            schedules.LiquidAPI, "get_all_blocks", side_effect=RuntimeError("database error")
        ), self.assertRaisesRegex(RuntimeError, "database error"):
            schedules.get_all_schedules(START.isoformat(), END.isoformat())

    def test_schedule_route_metadata_access_precedes_pure_transform(self):
        schedules = load_api_module("schedules")
        block = self._block("Program", START, END)

        def attach(by_station):
            by_station["Action"][0].meta = {"rating": "TV-G"}

        with patch.object(
            schedules.LiquidAPI, "get_all_blocks", return_value={"Action": [block]}
        ), patch.object(schedules, "_attach_meta_batch", side_effect=attach) as metadata:
            payload = schedules.get_all_schedules(
                START.isoformat(), END.isoformat(), include_meta=True
            )
        metadata.assert_called_once()
        self.assertEqual(payload["schedules"]["Action"][0]["meta"], {"rating": "TV-G"})

    def test_channel_builder_is_pure_and_route_payload_is_exact(self):
        summary = load_api_module("summary")
        stations = [{
            "network_name": "Action",
            "network_long_name": "Action Channel",
            "channel_number": 2,
            "hidden": False,
            "_has_schedule": True,
        }]
        manager = Mock(stations=stations)
        expected = {"channels": [{
            "network_name": "Action",
            "network_long_name": "Action Channel",
            "channel_number": 2,
            "hidden": False,
            "has_schedule": True,
        }]}
        with patch.object(summary, "StationManager", side_effect=AssertionError):
            self.assertEqual(summary.build_channels_payload(stations), expected)
        with patch.object(summary, "StationManager", return_value=manager):
            self.assertEqual(summary.get_channels(), expected)
        self.assertEqual(summary.build_channels_payload([]), {"channels": []})


if __name__ == "__main__":
    unittest.main()
