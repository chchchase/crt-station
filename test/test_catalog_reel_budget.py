"""Reel budgets: production helpers with synthetic entries and private SQLite only."""
import json
import tempfile
import unittest
from collections import Counter
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fs42.catalog import ShowCatalog, MatchingContentNotFound
from fs42.catalog_api import CatalogAPI
from fs42.catalog_entry import CatalogEntry
from fs42.liquid_blocks import LiquidBlock
from fs42.liquid_io import LiquidIO
from fs42.liquid_schedule import LiquidSchedule
from fs42.scheduling_context import ValidationSchedulingContext, activate_validation_context
from station_director import schedule_artifact as artifact


NOW = datetime(2026, 9, 22, 20)


class ReelBudgetTests(unittest.TestCase):
    def entry(self, duration, name='ad', tag='commercial', content_type='commercial'):
        entry = CatalogEntry('/mnt/t7/CRT-Media/synthetic-' + name + '.mp4', duration, tag,
                             content_type=content_type)
        entry.realpath = entry.path
        return entry

    def catalog(self, durations=(150.348729,), strategy='end', bumpers=False, commercial_free=False):
        config = dict(network_name='Synthetic', network_type='standard', commercial_free=commercial_free,
                      use_bumpers=bumpers, commercial_dir='commercial', bump_dir='bump',
                      break_strategy=strategy, break_duration=120)
        catalog = ShowCatalog(config, load=False)
        catalog.clip_index = {'commercial': [self.entry(d, 'ad-' + str(i)) for i, d in enumerate(durations)]}
        if bumpers:
            catalog.clip_index['bump'] = [self.entry(5, 'bump', 'bump', 'bump')]
        return catalog

    def context(self, seed=42):
        return activate_validation_context(ValidationSchedulingContext(NOW, NOW, NOW + timedelta(days=7), seed))

    def block(self, catalog):
        feature = self.entry(1500, 'feature', 'synthetic-series', 'feature')
        feature.dbid = 1
        schedule = object.__new__(LiquidSchedule)
        schedule.conf = {'schedule_increment': 30}
        block = LiquidBlock(feature, NOW, NOW + timedelta(seconds=schedule._calc_target_duration(feature.duration)),
                            break_strategy=catalog.config['break_strategy'],
                            break_info={'start_bump': None, 'end_bump': None})
        # Substitute only cached chapter/break reads; no media analysis or live SQLite.
        fluid = SimpleNamespace(get_chapters=lambda path: [], get_breaks=lambda path: [])
        with patch('fs42.liquid_blocks.FluidBuilder', return_value=fluid):
            block.make_plan(catalog)
        return block

    def effect(self, block):
        row = dict(id=1, station='Synthetic', liquid_type='LiquidBlock', content_json='1',
                   start_time=block.start_time.isoformat(' '), end_time=block.end_time.isoformat(' '),
                   plan_json=json.dumps([p.toJSON() for p in block.plan]), break_info=None, sequence_key=None)
        mapping = {1: {'semantics': {'row': dict(tag='synthetic-series', station='Synthetic',
                                               path='crt-media:/synthetic-feature.mp4')}}}
        directive = dict(date='2026-09-22', hour=20, series='synthetic-series', channel=2)
        artifact._live_plan(row)
        return artifact.effect_evidence([row], mapping, directive)

    def endpoint(self, block):
        mark = block.start_time
        for entry in block.plan:
            mark += timedelta(seconds=entry.duration)
        return mark

    def test_exact_previous_three_entry_overrun_and_corrected_export(self):
        legacy = self.catalog()
        select = legacy.find_commercial
        build_reel = legacy.make_reel_block
        # Reproduce only the previous budget argument; selection, plan building,
        # arithmetic and exporter predicates remain production implementations.
        def old_reel(*args, **kwargs):
            with patch.object(legacy, 'find_commercial', side_effect=lambda seconds, when, commercial_dir:
                              select(300, when, commercial_dir)):
                return build_reel(*args, **kwargs)
        with patch.object(legacy, 'make_reel_block', side_effect=old_reel):
            previous = self.block(legacy)
        self.assertEqual(len(previous.plan), 3)
        self.assertEqual(self.endpoint(previous) - previous.end_time, timedelta(microseconds=697458))
        with self.assertRaises(artifact.ArtifactError) as caught:
            self.effect(previous)
        self.assertEqual(caught.exception.code, 'candidate_timing_unsupported')

        corrected = self.block(self.catalog())
        self.assertEqual(len(corrected.plan), 2)
        self.assertEqual(corrected.end_time, previous.end_time)
        self.assertEqual([p.duration for p in corrected.plan], [1500, 150.348729])
        self.assertLess(self.endpoint(corrected), corrected.end_time)
        self.assertEqual(self.effect(corrected)['rule'], 'all_block_starts_in_requested_hour')
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'synthetic.db'
            with patch('fs42.liquid_io.StationManager', return_value=SimpleNamespace(server_conf={'db_path': str(database)})):
                io = LiquidIO()
                io.put_liquid_blocks('Synthetic', [corrected])
                with io._get_connection() as connection:
                    saved = connection.execute('SELECT * FROM liquid_blocks').fetchall()
                restored = LiquidIO.blocks_from_rows(saved, {1: corrected.content}, normalize_titles=False)[0]
            self.assertEqual([p.toJSON() for p in restored.plan], [p.toJSON() for p in corrected.plan])
            self.assertEqual(restored.end_time, corrected.end_time)
            self.assertEqual(self.effect(restored), self.effect(corrected))

    def test_decreasing_budget_and_bumper_deduction(self):
        for bumpers in (False, True):
            catalog = self.catalog((20,), bumpers=bumpers)
            target = 95 if bumpers else 85
            with patch.object(catalog, 'find_commercial', wraps=catalog.find_commercial) as select:
                reel = catalog.make_reel_block(NOW, target_duration=target)
            self.assertEqual([call.args[0] for call in select.call_args_list], [85, 65, 45, 25])
            self.assertLessEqual(reel.duration, target)

    def test_exact_fit_remains_ineligible_at_initial_and_later_selection(self):
        for target, expected_count in ((50, 0), (100, 1)):
            catalog = self.catalog((50,))
            with self.assertRaises(MatchingContentNotFound):
                catalog.make_reel_block(NOW, target_duration=target)
            self.assertEqual(catalog.clip_index['commercial'][0].count, expected_count)
        catalog = self.catalog((50,))
        reels = catalog.make_reel_fill(NOW, 100)
        self.assertEqual(sum(r.duration for r in reels), 50)

    def test_no_fit_terminates_and_supported_underfill_or_brb(self):
        for brb in (False, True):
            catalog = self.catalog((150.348729,))
            if brb:
                catalog.config['be_right_back_media'] = '/mnt/t7/CRT-Media/synthetic-brb.mp4'
            with patch.object(catalog, 'find_commercial', wraps=catalog.find_commercial) as select:
                reels = catalog.make_reel_fill(NOW, 300)
            self.assertEqual(select.call_count, 4)
            self.assertEqual(sum(r.duration for r in reels), 300 if brb else 150.348729)
        catalog = self.catalog((400,))
        with patch.object(catalog, 'find_commercial', wraps=catalog.find_commercial) as select:
            self.assertEqual(sum(r.duration for r in catalog.make_reel_fill(NOW, 300)), 0)
        self.assertEqual(select.call_count, 2)

    def test_strict_break_counts_keep_the_existing_budget_guard(self):
        for strategy in ('end', 'standard', 'center', 'spread'):
            for count in (1, 2, 3):
                with self.subTest(strategy=strategy, count=count):
                    catalog = self.catalog((20,), strategy)
                    with patch.object(catalog, 'find_commercial', wraps=catalog.find_commercial) as select:
                        reels = catalog.make_reel_fill(NOW, 300, strict_count=count)
                    self.assertLessEqual(sum(r.duration for r in reels), 300)
                    self.assertLess(select.call_count, 50)

    def test_strategies_and_commercial_free_policy(self):
        for strategy in ('end', 'standard', 'center', 'spread'):
            for bumpers in (False, True):
                for commercial_free in (False, True):
                    with self.subTest(strategy=strategy, bumpers=bumpers, commercial_free=commercial_free):
                        catalog = self.catalog((10,), strategy, bumpers, commercial_free)
                        with patch.object(catalog, 'find_commercial', wraps=catalog.find_commercial) as select:
                            block = self.block(catalog)
                        self.assertLessEqual(self.endpoint(block), block.end_time)
                        self.effect(block)
                        if commercial_free:
                            select.assert_not_called()
                            # ReelBlock labels its filler list as commercials even
                            # when commercial_free selects bumper media instead.
                            self.assertTrue(all(p.path not in {e.path for e in catalog.clip_index['commercial']}
                                                for p in block.plan))
                        if not bumpers:
                            self.assertFalse(any(p.content_type == 'bump' for p in block.plan))

    def test_seeded_choices_and_count_persistence_unchanged_for_fitting_pool(self):
        for seed in (0, 1, 42, 4242):
            results = []
            for old_budget in (False, True):
                catalog = self.catalog((20, 20, 20))
                select = catalog.find_commercial
                adapter = patch.object(catalog, 'find_commercial', side_effect=lambda seconds, when, commercial_dir:
                                       select(85, when, commercial_dir)) if old_budget else nullcontext()
                with self.context(seed), adapter:
                    reel = catalog.make_reel_block(NOW, target_duration=85)
                results.append(([e.path for e in reel.comms], [e.count for e in catalog.clip_index['commercial']]))
                self.assertEqual(Counter(e.path for e in reel.comms),
                                 Counter({e.path: e.count for e in catalog.clip_index['commercial'] if e.count}))
                # Native selection changes in-memory counters, not SQLite.
                # Explicit persistence retains existing station/path increment semantics.
                with tempfile.TemporaryDirectory() as directory:
                    database = Path(directory) / 'synthetic.db'
                    with patch('fs42.catalog_io.StationManager', return_value=SimpleNamespace(server_conf={'db_path': str(database)})), self.context(seed):
                        originals = [self.entry(20, 'ad-' + str(i)) for i in range(3)]
                        CatalogAPI.set_entries(catalog.config, originals)
                        self.assertEqual([e.count for e in CatalogAPI.get_entries(catalog.config)], [0, 0, 0])
                        CatalogAPI.update_play_counts(catalog.config, reel.comms)
                        self.assertEqual({e.path: e.count for e in CatalogAPI.get_entries(catalog.config)},
                                         {e.path: e.count for e in catalog.clip_index['commercial']})
            self.assertEqual(*results)
