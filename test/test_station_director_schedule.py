import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from station_director.cli import _parse_hours, _plan_data, build_parser
from station_director.policy import load_policy
from station_director.proposals import (
    ProposalError,
    archive_proposal,
    create_proposal,
    load_proposal,
    migrate_v1_to_v2,
    validate_schema,
    week_bounds,
)
from station_director.validation import (
    analyze_database,
    apply_directives,
    clone_database,
    project_configuration,
    semantic_checks,
)


def base_proposal(version=2):
    start, end = week_bounds("2026-09-14")
    return {
        "schema_version": version,
        "proposal_id": "p-20260912T000000Z-1234abcd",
        "created_at": "2026-09-12T00:00:00Z",
        "target_week": "2026-09-14",
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "source": "direct",
        "source_hashes": {
            "configs": {},
            "database": "0" * 64,
            "policy": "0" * 64,
            "inventory": "0" * 64,
            "wio_config": "0" * 64,
            "wio_state": "0" * 64,
        },
        "seed": 42,
        "assignment_changes": [],
        "directives": [],
        "exclusions": [],
    }


def _standard_config(name, number, tags):
    daily = {str(hour): {"tags": copy.deepcopy(tags)} for hour in range(24)}
    conf = {
        "network_name": name,
        "channel_number": number,
        "network_type": "standard",
        "day_templates": {"daily": daily},
    }
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
        conf[day] = "daily"
    return {"station_conf": conf}


def configs(action_tags="Batman Beyond"):
    return {
        "CRT Station Guide": {
            "station_conf": {
                "network_name": "CRT Station Guide",
                "channel_number": 1,
                "network_type": "web",
            }
        },
        "Action": _standard_config("Action", 2, action_tags),
        "After School": _standard_config("After School", 3, "Code Lyoko"),
        "Anime": _standard_config("Anime", 4, "Sailor Moon"),
        "Cartoon Network": _standard_config("Cartoon Network", 5, "Adventure Time"),
        "Disney": _standard_config("Disney", 6, "Recess"),
        "Late Night": _standard_config("Late Night", 7, "Futurama"),
        "Watch In Order": _standard_config("Watch In Order", 8, "Gravity Falls"),
    }


def inventory(*extra):
    names = {
        "Batman Beyond",
        "Code Lyoko",
        "Sailor Moon",
        "Adventure Time",
        "Recess",
        "Futurama",
        "Gravity Falls",
        *extra,
    }
    return {"shows": {name: {} for name in sorted(names)}}


class ProposalSchemaTests(unittest.TestCase):
    def test_v2_accepts_minimal_proposal_and_rejects_unknown_fields(self):
        proposal = base_proposal()
        validate_schema(proposal)
        proposal["shell_command"] = "anything"
        with self.assertRaisesRegex(ProposalError, "schema"):
            validate_schema(proposal)

    def test_assignment_one_of_enforces_action_specific_nullability_and_channels(self):
        valid = (
            {"action": "assign", "series": "Batman Beyond", "from_channel": None, "to_channel": 2},
            {"action": "move", "series": "Batman Beyond", "from_channel": 2, "to_channel": 4},
            {"action": "remove", "series": "Batman Beyond", "from_channel": 2, "to_channel": None},
        )
        for assignment in valid:
            with self.subTest(assignment=assignment):
                proposal = base_proposal()
                proposal["assignment_changes"] = [assignment]
                validate_schema(proposal)

        invalid = (
            {"action": "assign", "series": "Batman Beyond", "from_channel": 2, "to_channel": 4},
            {"action": "move", "series": "Batman Beyond", "from_channel": None, "to_channel": 4},
            {"action": "remove", "series": "Batman Beyond", "from_channel": 2, "to_channel": 4},
            {"action": "assign", "series": "Batman Beyond", "from_channel": None, "to_channel": 1},
            {"action": "assign", "series": "Batman Beyond", "from_channel": None, "to_channel": 8},
        )
        for assignment in invalid:
            with self.subTest(assignment=assignment):
                proposal = base_proposal()
                proposal["assignment_changes"] = [assignment]
                with self.assertRaises(ProposalError):
                    validate_schema(proposal)

    def test_move_must_change_channels(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [
            {"action": "move", "series": "Batman Beyond", "from_channel": 2, "to_channel": 2}
        ]
        with self.assertRaisesRegex(ProposalError, "current channel"):
            validate_schema(proposal)

    def test_marathon_uses_episode_count_and_forbids_legacy_hours(self):
        proposal = base_proposal()
        proposal["directives"] = [{
            "type": "marathon", "channel": 2, "date": "2026-09-15", "hour": 20,
            "count": 4, "series": "Batman Beyond",
        }]
        validate_schema(proposal)
        proposal["directives"][0]["hours"] = 4
        with self.assertRaises(ProposalError):
            validate_schema(proposal)

    def test_directive_one_of_forbids_fields_from_other_directive_types(self):
        proposal = base_proposal()
        proposal["directives"] = [{
            "type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20,
            "series": "Batman Beyond", "count": 2,
        }]
        with self.assertRaises(ProposalError):
            validate_schema(proposal)

    def test_seasonal_and_theme_require_hours_xor_true_all_day(self):
        valid = (
            {
                "type": "seasonal", "channel": 2, "start_date": "2026-09-14",
                "end_date": "2026-09-16", "hours": [6, 7], "series": "Batman Beyond",
            },
            {
                "type": "seasonal", "channel": 2, "start_date": "2026-09-14",
                "end_date": "2026-09-16", "all_day": True, "series": "Batman Beyond",
            },
            {
                "type": "theme", "name": "Hero Night", "channel": 2,
                "start_date": "2026-09-18", "end_date": "2026-09-18",
                "hours": [18, 19, 20], "series": "Batman Beyond",
            },
            {
                "type": "theme", "name": "Hero Day", "channel": 2,
                "start_date": "2026-09-18", "end_date": "2026-09-18",
                "all_day": True, "series": "Batman Beyond",
            },
        )
        for directive in valid:
            with self.subTest(directive=directive):
                proposal = base_proposal()
                proposal["directives"] = [directive]
                validate_schema(proposal)

        invalid = (
            dict(valid[0], all_day=True),
            {key: value for key, value in valid[0].items() if key != "hours"},
            dict(valid[1], all_day=False),
            dict(valid[0], hours=list(range(24))),
        )
        for directive in invalid:
            with self.subTest(directive=directive):
                proposal = base_proposal()
                proposal["directives"] = [directive]
                with self.assertRaises(ProposalError):
                    validate_schema(proposal)

    def test_dates_and_ranges_are_limited_to_target_week(self):
        cases = (
            {"type": "date_slot", "channel": 2, "date": "2026-09-13", "hour": 20, "series": "Batman Beyond"},
            {"type": "marathon", "channel": 2, "date": "2026-09-21", "hour": 1, "count": 2, "series": "Batman Beyond"},
            {"type": "seasonal", "channel": 2, "start_date": "2026-09-14", "end_date": "2026-09-21", "hours": [6], "series": "Batman Beyond"},
            {"type": "theme", "name": "Reverse", "channel": 2, "start_date": "2026-09-18", "end_date": "2026-09-17", "hours": [6], "series": "Batman Beyond"},
        )
        for directive in cases:
            with self.subTest(directive=directive):
                proposal = base_proposal()
                proposal["directives"] = [directive]
                with self.assertRaisesRegex(ProposalError, "outside proposal week|after end_date"):
                    validate_schema(proposal)

    def test_boundaries_must_be_canonical_station_week(self):
        proposal = base_proposal()
        proposal["week_start"] = "2026-09-14T13:00:00+00:00"
        with self.assertRaisesRegex(ProposalError, "canonical"):
            validate_schema(proposal)

    def test_new_proposals_are_v2_and_reject_noncanonical_inventory_names_before_write(self):
        hashes = base_proposal()["source_hashes"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch("station_director.proposals.source_hashes", return_value=hashes),
                patch(
                    "station_director.proposals.latest_inventory",
                    return_value=(Path("inventory.json"), inventory()),
                ),
            ):
                proposal, path = create_proposal(
                    "2026-09-14", "direct", 42, [], [], [], root
                )
                self.assertEqual(proposal["schema_version"], 2)
                self.assertTrue(path.is_file())

                with self.assertRaisesRegex(ProposalError, "exact canonical"):
                    create_proposal(
                        "2026-09-14",
                        "direct",
                        42,
                        [],
                        [],
                        ["batman beyond"],
                        root,
                    )


class MigrationTests(unittest.TestCase):
    def test_v1_minimal_and_date_slot_migrate_in_memory_without_mutating_input(self):
        legacy = base_proposal(version=1)
        legacy["week_start"] = "2026-09-14T06:00:00"
        legacy["week_end"] = "2026-09-21T06:00:00"
        legacy["directives"] = [{
            "type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20,
            "series": "Batman Beyond",
        }]
        original = copy.deepcopy(legacy)
        migrated = migrate_v1_to_v2(legacy)
        self.assertEqual(legacy, original)
        self.assertEqual(migrated["schema_version"], 2)
        self.assertEqual(migrated["week_start"], "2026-09-14T06:00:00-07:00")
        self.assertEqual(migrated["directives"], legacy["directives"])

    def test_legacy_marathon_and_implicit_full_day_directives_are_rejected(self):
        directives = (
            {"type": "marathon", "channel": 2, "date": "2026-09-15", "hour": 20, "hours": 4, "series": "Batman Beyond"},
            {"type": "seasonal", "channel": 2, "start_date": "2026-09-14", "end_date": "2026-09-16", "series": "Batman Beyond"},
            {"type": "theme", "channel": 2, "name": "Hero Week", "series": "Batman Beyond"},
        )
        for directive in directives:
            with self.subTest(directive=directive):
                legacy = base_proposal(version=1)
                legacy["directives"] = [directive]
                with self.assertRaisesRegex(ProposalError, "ambiguous|implicit full-day"):
                    migrate_v1_to_v2(legacy)

    def test_unsafe_v1_assignment_is_not_repaired_silently(self):
        legacy = base_proposal(version=1)
        legacy["assignment_changes"] = [
            {"action": "move", "series": "Batman Beyond", "from_channel": None, "to_channel": 4}
        ]
        with self.assertRaisesRegex(ProposalError, "cannot be safely migrated"):
            migrate_v1_to_v2(legacy)

    def test_loading_v1_returns_v2_without_rewriting_saved_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = base_proposal(version=1)
            legacy["week_start"] = "2026-09-14T06:00:00"
            legacy["week_end"] = "2026-09-21T06:00:00"
            path = root / "runtime/director/proposals" / legacy["proposal_id"] / "proposal.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(legacy))
            before = path.read_bytes()
            loaded, loaded_path = load_proposal(legacy["proposal_id"], root)
            self.assertEqual(loaded["schema_version"], 2)
            self.assertEqual(loaded_path, path)
            self.assertEqual(path.read_bytes(), before)


class CliSemanticsTests(unittest.TestCase):
    def _parse(self, *arguments):
        return build_parser().parse_args(["schedule", "plan", "--week", "2026-09-14", *arguments])

    def test_hours_parser_is_explicit_sorted_and_rejects_duplicates_or_full_day(self):
        self.assertEqual(_parse_hours("20,18,19"), [18, 19, 20])
        values = ("", "1,1", "-1", "24", ",", ",".join(str(hour) for hour in range(24)))
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _parse_hours(value)

    @patch("station_director.cli._owner_map", return_value={})
    def test_cli_emits_count_scoped_hours_and_explicit_all_day(self, unused_owner_map):
        args = self._parse(
            "--marathon", "2", "2026-09-15", "20", "4", "Batman Beyond",
            "--season", "2", "2026-09-14", "2026-09-16", "6,7", "Batman Beyond",
            "--season-all-day", "2", "2026-09-17", "2026-09-17", "Batman Beyond",
            "--theme", "Hero Night", "2", "2026-09-18", "2026-09-18", "18,19", "Batman Beyond",
            "--theme-all-day", "Hero Day", "2", "2026-09-19", "2026-09-19", "Batman Beyond",
        )
        unused_assignments, directives = _plan_data(args, load_policy())
        marathon = next(item for item in directives if item["type"] == "marathon")
        seasonal = [item for item in directives if item["type"] == "seasonal"]
        themes = [item for item in directives if item["type"] == "theme"]
        self.assertEqual(marathon["count"], 4)
        self.assertNotIn("hours", marathon)
        self.assertEqual(seasonal[0]["hours"], [6, 7])
        self.assertTrue(seasonal[1]["all_day"])
        self.assertEqual(themes[0]["hours"], [18, 19])
        self.assertTrue(themes[1]["all_day"])


class ProjectedConfigurationTests(unittest.TestCase):
    def test_directives_use_native_marathon_count_and_exact_scopes(self):
        original = configs()
        projected = copy.deepcopy(original)
        proposal = base_proposal()
        proposal["directives"] = [
            {"type": "marathon", "channel": 2, "date": "2026-09-15", "hour": 20, "count": 4, "series": "Batman Beyond"},
            {"type": "seasonal", "channel": 2, "start_date": "2026-09-16", "end_date": "2026-09-16", "hours": [6, 7], "series": "Batman Beyond"},
            {"type": "theme", "name": "Hero Day", "channel": 2, "start_date": "2026-09-17", "end_date": "2026-09-17", "all_day": True, "series": "Batman Beyond"},
        ]
        apply_directives(projected, proposal, load_policy())
        conf = projected["Action"]["station_conf"]["date_overrides"]
        self.assertEqual(conf["September 15"]["20"]["marathon"], {"count": 4, "chance": 1.0})
        self.assertEqual(set(conf["September 16"]), {"6", "7"})
        self.assertEqual(set(conf["September 17"]), {str(hour) for hour in range(24)})
        self.assertEqual(original, configs())

    def test_conflicting_directives_do_not_silently_replace_a_slot(self):
        proposal = base_proposal()
        proposal["directives"] = [
            {"type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20, "series": "Batman Beyond"},
            {"type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20, "series": "Sailor Moon"},
        ]
        with self.assertRaisesRegex(ProposalError, "conflicts"):
            apply_directives(copy.deepcopy(configs()), proposal, load_policy())

    def test_exact_canonical_unknown_and_case_ambiguous_inventory_names_fail(self):
        cases = (
            ("Missing", inventory(), "unknown inventory identifier"),
            ("batman beyond", inventory(), "exact canonical"),
            ("Batman Beyond", {"shows": {"Batman Beyond": {}, "BATMAN BEYOND": {}}}, "case-ambiguous"),
        )
        for name, inventory_data, expected in cases:
            with self.subTest(name=name):
                proposal = base_proposal()
                proposal["exclusions"] = [name]
                failures, unused_warnings = semantic_checks(proposal, load_policy(), inventory_data, configs())
                self.assertTrue(any(expected in failure for failure in failures), failures)

    def test_move_requires_destination_directive_and_detects_newly_tagless_source_slots(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [
            {"action": "move", "series": "Batman Beyond", "from_channel": 2, "to_channel": 4}
        ]
        failures, unused = semantic_checks(proposal, load_policy(), inventory(), configs())
        self.assertTrue(any("requires a scheduling directive" in item for item in failures))

        proposal["directives"] = [{
            "type": "date_slot", "channel": 4, "date": "2026-09-15", "hour": 20,
            "series": "Batman Beyond",
        }]
        failures, unused = semantic_checks(proposal, load_policy(), inventory(), configs())
        self.assertTrue(any("newly unresolved or tagless" in item for item in failures), failures)

    def test_removal_is_safe_when_another_tag_remains_in_every_source_slot(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [
            {"action": "remove", "series": "Batman Beyond", "from_channel": 2, "to_channel": None}
        ]
        failures, unused = semantic_checks(
            proposal,
            load_policy(),
            inventory("Replacement"),
            configs(action_tags=["Batman Beyond", "Replacement"]),
        )
        self.assertFalse(any("newly unresolved or tagless" in item for item in failures), failures)

    def test_exclusion_is_applied_to_projection_and_detects_source_holes(self):
        proposal = base_proposal()
        proposal["exclusions"] = ["Batman Beyond"]
        projected, unused_affected, source_channels = project_configuration(
            configs(), proposal, load_policy()
        )
        self.assertEqual(source_channels, {"Action"})
        self.assertNotIn("batman beyond", json.dumps(projected["Action"]).casefold())
        failures, unused = semantic_checks(proposal, load_policy(), inventory(), configs())
        self.assertTrue(any("newly unresolved or tagless" in item for item in failures), failures)

    def test_assignment_exclusion_and_directive_conflicts_are_rejected(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [
            {"action": "remove", "series": "Batman Beyond", "from_channel": 2, "to_channel": None}
        ]
        proposal["directives"] = [{
            "type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20,
            "series": "Batman Beyond",
        }]
        proposal["exclusions"] = ["Batman Beyond"]
        failures, unused = semantic_checks(proposal, load_policy(), inventory(), configs())
        self.assertTrue(any("Removed series is also scheduled" in item for item in failures))
        self.assertTrue(any("Excluded series is also scheduled or assigned" in item for item in failures))

    def test_channels_one_eight_and_wio_series_are_protected_in_projection(self):
        proposal = base_proposal()
        proposal["exclusions"] = ["Gravity Falls"]
        before = configs()
        failures, unused = semantic_checks(proposal, load_policy(), inventory(), before)
        self.assertTrue(any("Watch In Order series cannot be excluded" in item for item in failures))
        projected, unused_affected, unused_sources = project_configuration(
            before, proposal, load_policy()
        )
        self.assertEqual(projected["CRT Station Guide"], before["CRT Station Guide"])
        self.assertEqual(projected["Watch In Order"], before["Watch In Order"])

    def test_unassigned_series_requires_assign_action(self):
        proposal = base_proposal()
        proposal["directives"] = [{
            "type": "date_slot", "channel": 2, "date": "2026-09-15", "hour": 20,
            "series": "New Show",
        }]
        failures, unused = semantic_checks(
            proposal, load_policy(), inventory("New Show"), configs()
        )
        self.assertTrue(any("requires an assign action" in item for item in failures))


class ExistingScheduleHelperTests(unittest.TestCase):
    def test_staging_database_clone_preserves_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.db"
            target = Path(directory) / "target.db"
            with sqlite3.connect(source) as connection:
                connection.execute("CREATE TABLE value (number INTEGER)")
                connection.execute("INSERT INTO value VALUES(7)")
            before = source.read_bytes()
            clone_database(source, target)
            with sqlite3.connect(target) as connection:
                connection.execute("UPDATE value SET number=8")
            self.assertEqual(source.read_bytes(), before)

    def test_gap_overlap_and_boundary_spanning_reporting(self):
        proposal = base_proposal()
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "db.sqlite"
            with sqlite3.connect(db) as connection:
                connection.execute("CREATE TABLE catalog_entries(id INTEGER, duration REAL, tag TEXT, station TEXT, path TEXT, realpath TEXT)")
                connection.execute("CREATE TABLE liquid_blocks(station TEXT,start_time TEXT,end_time TEXT,title TEXT,content_json TEXT)")
                connection.executemany("INSERT INTO liquid_blocks VALUES(?,?,?,?,?)", [
                    ("Action", "2026-09-14 05:55:00", "2026-09-14 08:00:00", "A", None),
                    ("Action", "2026-09-14 07:00:00", "2026-09-14 09:00:00", "B", None),
                    ("Action", "2026-09-14 10:00:00", "2026-09-14 11:00:00", "C", None),
                ])
            report = analyze_database(db, proposal, {"channels": [{"number": 2, "name": "Action", "has_schedule": True}]})
            self.assertTrue(report["channels"]["2"]["gaps"])
            self.assertTrue(report["channels"]["2"]["overlaps"])

    def test_rejected_context_precedes_stale_checks_and_staging(self):
        from station_director.validation import validate_proposal

        with patch(
            "station_director.validation.check_invocation_context",
            return_value=(False, "Codex detected"),
        ), patch("station_director.validation.stale_sources") as stale, patch(
            "station_director.validation.create_staging_directory"
        ) as create_stage:
            report = validate_proposal(base_proposal(), Path("/does/not/matter"), load_policy())
        self.assertFalse(report["valid"])
        self.assertEqual(report["failures"][0], "Phase 3 validation is not yet enabled")
        self.assertIn("before staging", report["failures"][1])
        stale.assert_not_called()
        create_stage.assert_not_called()

    def test_archive_requires_exact_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proposal = base_proposal()
            proposal_id = proposal["proposal_id"]
            location = root / "runtime/director/proposals" / proposal_id
            location.mkdir(parents=True)
            (location / "proposal.json").write_text(json.dumps(proposal))
            with self.assertRaisesRegex(ProposalError, "exact proposal ID"):
                archive_proposal(proposal_id, "no", root)
            target = archive_proposal(proposal_id, proposal_id, root)
            self.assertTrue((target / "proposal.json").is_file())


if __name__ == "__main__":
    unittest.main()
