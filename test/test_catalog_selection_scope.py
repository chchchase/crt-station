"""Synthetic SQLite only; no scheduling workers, media scanning or live state."""
import asyncio
import contextvars
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fs42.catalog import ShowCatalog
from fs42.catalog_api import CatalogAPI
from fs42.catalog_entry import CatalogEntry
from fs42.catalog_io import CatalogIO
from fs42.liquid_io import LiquidIO
from fs42.scheduling_context import (
    ValidationSchedulingContext, activate_validation_context,
    activate_catalog_selection, active_catalog_ids,
)
from station_director.native_single_run import _map_catalog_in_memory
from station_director.path_safety import canonical_media_mapping
from station_director.staged_schedule import (
    capture_catalog_rows, reconcile_catalog, catalog_allocation_floor,
)
from test.test_station_director_milestone_b2 import create_database, insert_block


class SelectionScopeTests(unittest.TestCase):
    def context(self, seed=42):
        return activate_validation_context(ValidationSchedulingContext(
            datetime(2026, 9, 22, 20), datetime(2026, 9, 22, 20),
            datetime(2026, 9, 29, 20), seed))

    def io(self, side='staged'):
        return patch('fs42.catalog_io.StationManager', return_value=SimpleNamespace(
            server_conf={'db_path': str(self.paths[side])}))

    def entries(self, root):
        result = []
        for name, tag in [('a.mp4', 'series'), ('b.mp4', 'series'), ('a.mp4', 'alternate')]:
            entry = CatalogEntry(root + '/' + name, 60., tag)
            entry.realpath = entry.path
            result.append(entry)
        return result

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.paths = {s: Path(temp.name) / (s + '.db') for s in ('ordinary', 'staged')}
        self.conf = {'network_name': 'Action', 'network_type': 'standard'}
        for side, path in self.paths.items():
            with closing(create_database(path)) as c:
                c.execute("INSERT INTO named_sequence VALUES(1,'Other','ordered','series',0,100,2,1,NULL)")
                c.execute("INSERT INTO sequence_entries VALUES(1,'/media/a.mp4',2,1)")
                c.execute("INSERT INTO sequence_group_state VALUES('Other','ordered','parent','series')")
                c.commit()
            with self.io(side), self.context():
                CatalogAPI.set_entries(self.conf, self.entries('/mnt/t7/CRT-Media'))
                CatalogAPI.set_entries({'network_name': 'Other'}, self.entries('/mnt/t7/CRT-Media'))
        with closing(sqlite3.connect(self.paths['staged'])) as c:
            columns, original = capture_catalog_rows(c, 'Action')
            floor = catalog_allocation_floor(c)
            self.protected = next(r[0] for r in original if r[columns.index('tag')] == 'series'
                                  and r[columns.index('path')].endswith('/a.mp4'))
            insert_block(c, 'Action', '2026-09-22 18:00:00', '2026-09-22 19:00:00',
                         self.protected, '/mnt/t7/CRT-Media/a.mp4')
            c.commit()
            self.before = self.preserved(c)
        for side in self.paths:
            with self.io(side), self.context():
                catalog = ShowCatalog(self.conf, load=False)
                for entry in self.entries('/media' if side == 'staged' else '/mnt/t7/CRT-Media'):
                    catalog.clip_index.setdefault(entry.tag, []).append(entry)
                catalog._write_catalog()
        with closing(sqlite3.connect(self.paths['staged'])) as c:
            _, generated = capture_catalog_rows(c, 'Action')
            self.active = reconcile_catalog(c, 'Action', [dict(zip(columns, r)) for r in generated],
                                            {self.protected}, original_rows=original, allocation_floor=floor)
            c.commit()

    def preserved(self, c):
        return [list(c.execute(sql)) for sql in (
            'SELECT * FROM liquid_blocks',
            'SELECT * FROM catalog_entries WHERE id=' + str(self.protected),
            "SELECT * FROM catalog_entries WHERE station='Other'",
            'SELECT * FROM named_sequence', 'SELECT * FROM sequence_entries',
            'SELECT * FROM sequence_group_state')]

    def test_population_and_seeded_equivalence(self):
        with closing(sqlite3.connect(self.paths['staged'])) as c, self.io():
            self.assertEqual(len(ShowCatalog(self.conf).clip_index['series']), 3)
            with activate_catalog_selection(c, 'Action', self.active):
                catalog = ShowCatalog(self.conf)
                self.assertEqual(len(catalog.clip_index['series']), 2)
                self.assertEqual(len(catalog.clip_index['alternate']), 1)
        for seed in range(32):
            samples = []
            for side in self.paths:
                with closing(sqlite3.connect(self.paths[side])) as c, self.io(side), self.context(seed):
                    ids = self.active if side == 'staged' else {
                        e.dbid for e in CatalogAPI.get_entries(self.conf)}
                    with activate_catalog_selection(c, 'Action', ids):
                        cat = ShowCatalog(self.conf)
                        _map_catalog_in_memory(SimpleNamespace(catalog=cat))
                        samples.append([canonical_media_mapping(cat.find_candidate(
                            'series', 61, datetime(2026, 9, 22, 20)).path, allow_sandbox=True).logical_identity
                            for _ in range(8)])
            self.assertEqual(*samples)

    def test_all_reads_empty_scope_and_historical_ids(self):
        with closing(sqlite3.connect(self.paths['staged'])) as c, self.io(), self.context():
            io = CatalogIO()
            with activate_catalog_selection(c, 'Action', self.active):
                self.assertEqual({e.dbid for e in io.get_catalog_entries('Action')}, self.active)
                self.assertIsNone(io.get_entry_by_path('Action', '/mnt/t7/CRT-Media/a.mp4'))
                self.assertEqual(len(io.get_by_tag('Action', 'series')), 2)
                self.assertEqual(len(io.search_catalog_entries('Action', 'a')), 3)
                self.assertEqual(len(io.find_best_candidates('Action', 'series', 61)), 2)
                self.assertEqual(io.get_catalog_entries('Other'), [])
                self.assertIsNotNone(io.entry_by_id(self.protected))
                self.assertIn(self.protected, io.entries_by_ids([self.protected]))
            with activate_catalog_selection(c, 'Action', set()):
                self.assertEqual(io.get_catalog_entries('Action'), [])
                self.assertEqual(io.get_by_tag('Action', 'series'), [])
                self.assertEqual(io.search_catalog_entries('Action', ''), [])
                self.assertEqual(io.find_best_candidates('Action', 'series', 61), [])
                self.assertIsNone(io.get_entry_by_path('Action', '/media/a.mp4'))
                before = list(c.execute('SELECT * FROM catalog_entries'))
                io.update_entry_count('Action', '/media/a.mp4', 9)
                io.batch_increment_counts('Action', self.entries('/media'))
                self.assertEqual(before, list(c.execute('SELECT * FROM catalog_entries')))
            self.assertEqual(len(io.get_catalog_entries('Action')), 4)

    def test_count_writes_preserve_history_tags_channels_and_sequences(self):
        for validation in (False, True):
            from contextlib import nullcontext
            with closing(sqlite3.connect(self.paths['staged'])) as c, self.io(), \
                    (self.context() if validation else nullcontext()), \
                    activate_catalog_selection(c, 'Action', self.active):
                io = CatalogIO()
                io.update_entry_count('Action', '/media/a.mp4', 3)
                io.batch_increment_counts('Action', [self.entries('/media')[0]])
                io.update_entry_count('Other', '/mnt/t7/CRT-Media/a.mp4', 99)
                io.batch_increment_counts('Other', self.entries('/mnt/t7/CRT-Media'))
                counts = [(e.tag, e.count) for e in io.get_catalog_entries('Action') if e.path.endswith('/a.mp4')]
                self.assertEqual(sorted(counts), [('alternate', 4), ('series', 4)])
                self.assertEqual(self.before, self.preserved(c))
                rows = list(c.execute('SELECT * FROM liquid_blocks'))
                cache = io.entries_by_ids([self.protected])
                block = LiquidIO.blocks_from_rows(rows, cache, normalize_titles=False)[0]
                self.assertEqual(block.content.dbid, self.protected)
                self.assertEqual(block.content.path, '/mnt/t7/CRT-Media/a.mp4')
        with self.io():
            io = CatalogIO()
            io.update_entry_count('Other', '/mnt/t7/CRT-Media/a.mp4', 9)
            self.assertEqual(io.get_entry_by_path('Other', '/mnt/t7/CRT-Media/a.mp4').count, 9)

    def test_validation_and_cleanup(self):
        with closing(sqlite3.connect(self.paths['staged'])) as c:
            other = c.execute("SELECT id FROM catalog_entries WHERE station='Other' LIMIT 1").fetchone()[0]
            for ids in ({True}, {-1}, {0}, {2**63}, {999999}, {other}, [1], None):
                with self.assertRaises(ValueError), activate_catalog_selection(c, 'Action', ids):
                    self.fail('invalid scope entered')
            self.assertIsNone(active_catalog_ids('Action'))
            for error in (None, ValueError(), KeyboardInterrupt(), asyncio.CancelledError()):
                with activate_catalog_selection(c, 'Action', self.active):
                    prior = active_catalog_ids('Action')
                    try:
                        with activate_catalog_selection(c, 'Action', set()):
                            self.assertEqual(active_catalog_ids('Action'), ())
                            copied = contextvars.copy_context()
                            if error: raise error
                    except BaseException as caught:
                        self.assertIs(caught, error)
                    self.assertEqual(active_catalog_ids('Action'), prior)
                    with self.assertRaises(RuntimeError):
                        copied.run(active_catalog_ids, 'Action')
                self.assertIsNone(active_catalog_ids('Action'))

    def test_async_boundary(self):
        async def exercise():
            with closing(sqlite3.connect(self.paths['staged'])) as c, activate_catalog_selection(c, 'Action', self.active):
                async def child():
                    with self.assertRaises(RuntimeError): active_catalog_ids('Action')
                await asyncio.create_task(child())
        asyncio.run(exercise())

    def test_scope_snapshot_thread_boundary_and_bound(self):
        import concurrent.futures
        with closing(sqlite3.connect(self.paths['staged'])) as c:
            with self.assertRaises(ValueError), activate_catalog_selection(c, 'Action', set(range(1, 50002))):
                self.fail('oversized scope entered')
            ids = set(self.active)
            with activate_catalog_selection(c, 'Action', ids):
                ids.clear()
                self.assertEqual(active_catalog_ids('Action'), tuple(sorted(self.active)))
                copied = contextvars.copy_context()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(copied.run, active_catalog_ids, 'Action')
                    with self.assertRaises(RuntimeError): future.result()


if __name__ == '__main__':
    unittest.main()
