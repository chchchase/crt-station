import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from station_director.policy import load_policy
from station_director.proposals import ProposalError, archive_proposal, validate_schema, week_bounds
from station_director.validation import (
    analyze_database,
    apply_assignment_changes,
    apply_directives,
    clone_database,
    semantic_checks,
)


def base_proposal():
    start, end = week_bounds("2026-09-14")
    return {
        "schema_version": 1,
        "proposal_id": "p-20260912T000000Z-1234abcd",
        "created_at": "2026-09-12T00:00:00Z",
        "target_week": "2026-09-14",
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "source": "direct",
        "source_hashes": {"configs": {}, "database": "0"*64, "policy": "0"*64, "inventory": "0"*64, "wio_config": "0"*64, "wio_state": "0"*64},
        "seed": 42,
        "assignment_changes": [], "directives": [], "exclusions": [],
    }


def configs():
    return {
        "Anime": {"station_conf": {"network_name": "Anime", "channel_number": 4, "day_templates": {"daily": {"0": {"tags": "Sailor Moon"}}}}},
        "Late Night": {"station_conf": {"network_name": "Late Night", "channel_number": 7, "day_templates": {"daily": {"0": {"tags": "Futurama"}}}}},
        "Watch In Order": {"station_conf": {"network_name": "Watch In Order", "channel_number": 8, "day_templates": {"daily": {"0": {"tags": "Gravity Falls"}}}}},
    }


class ScheduleProposalTests(unittest.TestCase):
    def test_schema_accepts_minimal_proposal_and_rejects_unknown_or_command_fields(self):
        proposal = base_proposal(); validate_schema(proposal)
        proposal["shell_command"] = "rm anything"
        with self.assertRaisesRegex(ProposalError, "schema"):
            validate_schema(proposal)

    def test_schema_rejects_channel_renumbering_and_paths(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [{"action":"assign","series":"../Show","from_channel":None,"to_channel":9}]
        with self.assertRaises(ProposalError): validate_schema(proposal)

    def test_nonexistent_series_is_rejected(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [{"action":"assign","series":"Missing","from_channel":None,"to_channel":4}]
        failures, unused = semantic_checks(proposal, load_policy(), {"shows":{"Sailor Moon":{}}}, configs())
        self.assertTrue(any("Unknown inventory" in item for item in failures))

    def test_watch_in_order_series_and_channel_are_rejected(self):
        proposal = base_proposal()
        proposal["assignment_changes"] = [{"action":"move","series":"Gravity Falls","from_channel":8,"to_channel":6}]
        failures, unused = semantic_checks(proposal, load_policy(), {"shows":{"Gravity Falls":{}}}, configs())
        self.assertTrue(any("Watch In Order conflict" in item for item in failures))

    def test_cross_channel_ownership_is_detectable(self):
        duplicate = configs()
        duplicate["Late Night"]["station_conf"]["day_templates"]["daily"]["1"] = {"tags":"Sailor Moon"}
        from station_director.validation import configured_tags_from_data
        assignments, unused = configured_tags_from_data(duplicate)
        self.assertEqual(assignments["sailor moon"], {"Anime", "Late Night"})
        failures, unused = semantic_checks(base_proposal(), load_policy(), {"shows":{"Sailor Moon":{},"Futurama":{},"Gravity Falls":{}}}, duplicate)
        self.assertTrue(any("Exclusive ownership violation" in item for item in failures))

    def test_staging_helpers_preserve_source_database_and_configs(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)/"source.db"; target = Path(directory)/"target.db"
            with sqlite3.connect(source) as con: con.execute("CREATE TABLE value (number INTEGER)"); con.execute("INSERT INTO value VALUES(7)")
            before = source.read_bytes(); clone_database(source, target)
            with sqlite3.connect(target) as con: con.execute("UPDATE value SET number=8"); con.commit()
            self.assertEqual(source.read_bytes(), before)
        original = configs(); staged = copy.deepcopy(original)
        proposal = base_proposal(); proposal["directives"] = [{"type":"date_slot","channel":4,"date":"2026-09-15","hour":20,"series":"Sailor Moon"}]
        apply_directives(staged, proposal, load_policy())
        self.assertEqual(original, configs())

    def test_assignment_transfer_removes_source_and_requires_destination_directive(self):
        original = configs(); staged = copy.deepcopy(original)
        proposal = base_proposal()
        proposal["assignment_changes"] = [{"action":"move","series":"Sailor Moon","from_channel":4,"to_channel":7}]
        failures, unused = semantic_checks(proposal, load_policy(), {"shows":{"Sailor Moon":{}}}, original)
        self.assertTrue(any("requires a scheduling directive" in item for item in failures))
        proposal["directives"] = [{"type":"date_slot","channel":7,"date":"2026-09-15","hour":20,"series":"Sailor Moon"}]
        failures, unused = semantic_checks(proposal, load_policy(), {"shows":{"Sailor Moon":{}}}, original)
        self.assertFalse(any("Sailor Moon" in item for item in failures))
        affected = apply_assignment_changes(staged, proposal, load_policy())
        apply_directives(staged, proposal, load_policy())
        self.assertEqual(affected, {"Anime"})
        self.assertNotIn("sailor moon", json.dumps(staged["Anime"]).casefold())
        self.assertIn("sailor moon", json.dumps(staged["Late Night"]).casefold())

    def test_excluded_series_cannot_also_be_scheduled(self):
        proposal = base_proposal()
        proposal["exclusions"] = ["Sailor Moon"]
        proposal["directives"] = [{"type":"date_slot","channel":4,"date":"2026-09-15","hour":20,"series":"Sailor Moon"}]
        failures, unused = semantic_checks(proposal, load_policy(), {"shows":{"Sailor Moon":{}}}, configs())
        self.assertTrue(any("Excluded series" in item for item in failures))

    def test_expired_incomplete_gap_and_overlap_reporting(self):
        proposal = base_proposal()
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory)/"db.sqlite"
            with sqlite3.connect(db) as con:
                con.execute("CREATE TABLE catalog_entries(id INTEGER, duration REAL, tag TEXT, station TEXT, path TEXT, realpath TEXT)")
                con.execute("CREATE TABLE liquid_blocks(station TEXT,start_time TEXT,end_time TEXT,title TEXT,content_json TEXT)")
                con.executemany("INSERT INTO liquid_blocks VALUES(?,?,?,?,?)", [
                    ("Action","2026-09-14 06:00:00","2026-09-14 08:00:00","A",None),
                    ("Action","2026-09-14 07:00:00","2026-09-14 09:00:00","B",None),
                    ("Action","2026-09-14 10:00:00","2026-09-14 11:00:00","C",None),
                ])
            report = analyze_database(db, proposal, {"channels":[{"number":2,"name":"Action","has_schedule":True}]})
            self.assertTrue(report["channels"]["2"]["gaps"])
            self.assertTrue(report["channels"]["2"]["overlaps"])
            self.assertLess(report["channels"]["2"]["last"], proposal["week_end"].replace("T"," "))

    def test_boundary_spanning_block_is_not_an_overlap(self):
        proposal = base_proposal()
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory)/"db.sqlite"
            with sqlite3.connect(db) as con:
                con.execute("CREATE TABLE catalog_entries(id INTEGER, duration REAL, tag TEXT, station TEXT, path TEXT, realpath TEXT)")
                con.execute("CREATE TABLE liquid_blocks(station TEXT,start_time TEXT,end_time TEXT,title TEXT,content_json TEXT)")
                con.execute("INSERT INTO liquid_blocks VALUES('Action','2026-09-14 05:55:00','2026-09-21 06:05:00','A',NULL)")
            report = analyze_database(db, proposal, {"channels":[{"number":2,"name":"Action","has_schedule":True}]})
            self.assertEqual(report["channels"]["2"]["gaps"], [])
            self.assertEqual(report["channels"]["2"]["overlaps"], [])

    def test_same_seed_is_reproducible(self):
        import random
        random.seed(4242); first = [random.randrange(1000) for unused in range(20)]
        random.seed(4242); second = [random.randrange(1000) for unused in range(20)]
        self.assertEqual(first, second)

    def test_stale_hash_rejection_path(self):
        from station_director.validation import validate_proposal
        with patch("station_director.validation.stale_sources", return_value=["database"]):
            report = validate_proposal(base_proposal(), Path("/does/not/matter"), load_policy())
        self.assertFalse(report["valid"])
        self.assertIn("database", report["failures"][0])

    def test_archive_requires_exact_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); proposal = base_proposal(); proposal_id = proposal["proposal_id"]
            location = root/"runtime/director/proposals"/proposal_id
            location.mkdir(parents=True); (location/"proposal.json").write_text(json.dumps(proposal))
            with self.assertRaisesRegex(ProposalError, "exact proposal ID"):
                archive_proposal(proposal_id, "no", root)
            target = archive_proposal(proposal_id, proposal_id, root)
            self.assertTrue((target/"proposal.json").is_file())


if __name__ == "__main__": unittest.main()
