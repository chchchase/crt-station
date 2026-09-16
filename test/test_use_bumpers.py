"""Synthetic-only channel bumper policy tests: no media, database, or scheduler runs."""
import copy
import datetime
import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fs42.autobump_agent import AutoBumpAgent
from fs42.catalog import ShowCatalog
from fs42.catalog_entry import CatalogEntry
from fs42.config_processor import ConfigProcessor, ConfigurationError
from fs42.liquid_blocks import LiquidBlock, LiquidClipBlock
from fs42.liquid_schedule import LiquidSchedule
from fs42.scheduling_context import ValidationSchedulingContext, activate_validation_context
from fs42.station_io import StationIO
from fs42.timings import DAYS
from station_director.native_config_checks import validate_processed_configurations
from station_director.path_safety import map_station_config
from station_director.policy import load_policy
from station_director.validation import project_configuration
from station_director.validation_context import (
    logical_configuration_values_fingerprint, logical_protected_configuration_fingerprint,
)
from station_director.worker_bootstrap import projected_configuration_documents
from test.test_station_director_schedule import base_proposal


NOW = datetime.datetime(2026, 9, 15, 20)


class BumperPolicyTests(unittest.TestCase):
    def catalog(self, **options):
        catalog = object.__new__(ShowCatalog)
        catalog.config = dict(commercial_free=False, break_duration=60,
                              break_strategy="end", **options)
        catalog._l = logging.getLogger("synthetic-bumpers")
        catalog.min_gap = 1
        catalog.find_bump = Mock(side_effect=AssertionError("bumper lookup forbidden"))
        catalog.find_commercial = Mock(return_value=CatalogEntry("synthetic-commercial", 10, "commercial"))
        return catalog

    def test_boolean_validation_in_both_configuration_paths(self):
        schema = StationIO().load_schema()
        for value in (None, 0, 1, "false", "true", [], {}):
            with self.subTest(value=value):
                doc = {"station_conf": dict(network_name="Synthetic", channel_number=2,
                                            use_bumpers=value)}
                self.assertFalse(StationIO().validate_station_config(doc)[0])
                self.assertTrue(validate_processed_configurations({"Synthetic": doc}, schema))
                with self.assertRaises(ConfigurationError):
                    ConfigProcessor.preprocess(doc["station_conf"])
        for value in (False, True):
            self.assertTrue(StationIO().validate_station_config({"station_conf": {
                "network_name": "Synthetic", "channel_number": 2, "use_bumpers": value}})[0])

    def test_omitted_and_true_preserve_ordinary_and_auto_bumpers(self):
        for option in ({}, {"use_bumpers": True}):
            for auto in (False, True):
                with self.subTest(option=option, auto=auto):
                    catalog = self.catalog(**option)
                    bump = CatalogEntry("synthetic-bump", 5, "bump")
                    catalog.find_bump = Mock(return_value=bump)
                    if auto:
                        catalog.config["autobump"] = {"duration": 5}
                    with patch.object(AutoBumpAgent, "gen_bumps", return_value={
                            "message_bump": bump, "next_bump": bump}) as generate:
                        reel = catalog.make_reel_block(NOW, target_duration=60)
                    self.assertIs(reel.start_bump, bump)
                    self.assertIs(reel.end_bump, bump)
                    self.assertEqual(generate.call_count, int(auto))
                    self.assertEqual(catalog.find_bump.call_count, 0 if auto else 2)
                    self.assertTrue(reel.comms)

    def test_disabled_reels_fill_commercials_without_bump_files_or_autobump(self):
        for auto in (False, True):
            for strategy in ("end", "standard"):
                with self.subTest(auto=auto, strategy=strategy):
                    catalog = self.catalog(use_bumpers=False)
                    catalog.config["break_strategy"] = strategy
                    if auto:
                        catalog.config["autobump"] = {"fill_break": 1, "bg_video": "forbidden"}
                    with patch.object(AutoBumpAgent, "gen_bumps", side_effect=AssertionError), \
                         patch.object(AutoBumpAgent, "fill_block", side_effect=AssertionError), \
                         patch("fs42.catalog.SlotReader.get_break_info", return_value={"break_strategy": None}):
                        reels = catalog.make_reel_fill(NOW, 120, use_bumpers=True)
                    self.assertTrue(any(reel.comms for reel in reels))
                    self.assertTrue(all(reel.start_bump is None and reel.end_bump is None for reel in reels))
                    catalog.find_bump.assert_not_called()
                    self.assertTrue(catalog.find_commercial.called)

    def test_commercial_free_stays_commercial_free_and_terminates(self):
        catalog = self.catalog(use_bumpers=False)
        catalog.config.update(commercial_free=True, break_strategy="standard")
        with patch("fs42.catalog.SlotReader.get_break_info", return_value={"break_strategy": None}):
            reels = catalog.make_reel_fill(NOW, 120)
        self.assertEqual(sum(reel.duration for reel in reels), 0)
        catalog.find_commercial.assert_not_called()
        catalog.find_bump.assert_not_called()

    def test_all_autobump_entry_points_disabled_in_both_contexts(self):
        config = {"use_bumpers": False, "autobump": {"fill_break": 1, "bg_video": "forbidden"},
                  "off_air_autobump": {"bg_video": "forbidden"}}
        def check():
            with patch.object(AutoBumpAgent, "get_bg_video_duration", side_effect=AssertionError), \
                 patch.object(AutoBumpAgent, "message_bump", side_effect=AssertionError), \
                 patch.object(AutoBumpAgent, "next_up_bump", side_effect=AssertionError):
                self.assertFalse(AutoBumpAgent.validation_subprocess_required(config))
                self.assertFalse(AutoBumpAgent.do_fill(config))
                self.assertIsNone(AutoBumpAgent.fill_block(config, 60))
                self.assertTrue(all(value is None for value in AutoBumpAgent.gen_bumps(config).values()))
        check()
        with activate_validation_context(ValidationSchedulingContext(
                NOW, NOW, NOW + datetime.timedelta(hours=1), 42)):
            check()
        del config["autobump"]
        check()

    def test_explicit_slot_and_tag_bumpers_cannot_override_false(self):
        schedule = object.__new__(LiquidSchedule)
        schedule.conf = {"use_bumpers": False, "break_strategy": "end", "schedule_increment": 1,
                         "content_dir": "synthetic", "tag_overrides": {
                             "show": {"start_bump": "missing", "end_bump": "missing"}}}
        schedule.catalog = Mock()
        with patch("fs42.liquid_schedule.PathQuery.match_any_from_base", return_value=None):
            info, unused_strategy, unused_increment = schedule._break_info(
                {"start_bump": "missing", "end_bump": "missing", "use_bumpers": True},
                "show", "synthetic/show")
        self.assertIsNone(info["start_bump"])
        self.assertIsNone(info["end_bump"])
        schedule.catalog.get_start_bump.assert_not_called()
        schedule.catalog.get_end_bump.assert_not_called()

    def test_both_block_call_sites_pass_policy_and_suppress_explicit_bumps(self):
        for cls in (LiquidBlock, LiquidClipBlock):
            for option in ({}, {"use_bumpers": True}, {"use_bumpers": False}):
                with self.subTest(block=cls.__name__, option=option):
                    entry = CatalogEntry("synthetic-show", 60, "show")
                    block = cls(entry if cls is LiquidBlock else [entry], NOW,
                                NOW + datetime.timedelta(seconds=120), "Synthetic",
                                break_info={"start_bump": {"duration": 5}, "end_bump": {"duration": 5}})
                    catalog = Mock(config=option)
                    catalog.make_reel_fill.return_value = []
                    with patch("fs42.liquid_blocks.FluidBuilder") as fluid, \
                         patch("fs42.liquid_blocks.ReelCutter.cut_reels_into_base"), \
                         patch("fs42.liquid_blocks.ReelCutter.cut_reels_into_clips"):
                        fluid.return_value.get_chapters.return_value = []
                        fluid.return_value.get_breaks.return_value = []
                        block.make_plan(catalog)
                    enabled = option.get("use_bumpers", True)
                    self.assertIs(catalog.make_reel_fill.call_args.kwargs["use_bumpers"], enabled)
                    self.assertEqual(block.start_bump is None, not enabled)
                    self.assertEqual(block.end_bump is None, not enabled)

    def test_disabled_catalog_does_not_collect_bumper_files(self):
        catalog = self.catalog(use_bumpers=False)
        catalog.config.update(content_dir="nonexistent-synthetic", commercial_dir="commercials",
                              bump_dir="missing", clip_shows={}, tag_overrides={
                                  "show": {"bump_dir": "missing", "start_bump": "missing", "end_bump": "missing"}})
        catalog.config.update({day: {"0": {"tags": "show", "start_bump": "missing"}} for day in DAYS})
        catalog._scan_directory = Mock(return_value=0)
        with patch("fs42.catalog.SequenceAPI.scan_sequences"), \
             patch.object(catalog, "_ShowCatalog__bump_collector", side_effect=AssertionError), \
             patch.object(catalog, "_build_tags"), patch.object(catalog, "_write_catalog"):
            catalog._build_standard()
        self.assertEqual([call.args[0] for call in catalog._scan_directory.call_args_list], ["show", "commercials"])

    def test_director_projection_mapping_and_serialized_fingerprints(self):
        proposal = base_proposal()
        proposal["directives"] = [{"type": "date_slot", "channel": 2, "date": "2026-09-15",
                                   "hour": 20, "series": "Synthetic"}]
        policy = load_policy()
        digests = []
        for option in ({}, {"use_bumpers": True}, {"use_bumpers": False}):
            config = {"Action": {"station_conf": dict(network_name="Action", channel_number=2, **option)}}
            original = copy.deepcopy(config)
            projected, affected, unused = project_configuration(config, proposal, policy)
            self.assertEqual(config, original)
            self.assertEqual(affected, {"Action"})
            mapped, unused = map_station_config(projected["Action"], "synthetic.json")
            self.assertEqual(mapped["station_conf"].get("use_bumpers", "absent"), option.get("use_bumpers", "absent"))
            documents = {"confs/synthetic.json": mapped}
            expected = logical_configuration_values_fingerprint(documents)
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "confs").mkdir()
                source = root / "confs/synthetic.json"
                source.write_text(json.dumps(original["Action"]))
                staged, staged_affected, unused, unused_count = projected_configuration_documents(
                    root, {"proposal": proposal, "policy": policy}, root / "media", root / "work")
                self.assertEqual(staged, documents)
                self.assertEqual(staged_affected, ("Action",))
                path = root / "serialized.json"
                path.write_text(json.dumps(mapped))
                actual = logical_protected_configuration_fingerprint({"confs/synthetic.json": path})
            self.assertEqual(actual, expected)
            digests.append(expected["digest"])
        self.assertEqual(len(set(digests)), 3)
