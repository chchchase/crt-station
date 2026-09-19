"""Disposable databases, real candidate/export/transaction helpers, fake services.

Nothing in this suite can start a native worker or contact the user manager.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from station_director import schedule_application as application, schedule_artifact as artifact
from test import test_station_director_selection_state as selection_tests
from test.test_station_director_schedule_artifact import PROPOSAL, POLICY, INPUTS, RUN


class Crash(BaseException):
    pass


class FakeBindings:
    def __init__(self):
        self.calls = 0
        self.fail_at = None
        self.bad_code = False

    def code(self, document):
        application.require(not self.bad_code, 'binding_changed')

    def inputs(self, document):
        self.calls += 1
        application.require(self.calls != self.fail_at, 'binding_changed')


class FakeServices:
    def __init__(self, root, prior=('active', 'active')):
        self.root = root
        self.prior = dict(zip(application.UNITS, prior))
        self.current = dict(self.prior)
        self.events = []
        self.guard_valid = True
        self.stop_failure = False
        self.start_failure = None
        self.populated = False

    @property
    def marker(self):
        return self.root / 'runtime/director' / application.MARKER

    def guards(self):
        application.require(self.guard_valid, 'guard_invalid')

    def states(self):
        application.require(tuple(self.current[u] for u in application.UNITS)
                            in (('active', 'active'), ('active', 'inactive'), ('inactive', 'inactive')),
                            'service_state_unsupported')
        return dict(self.current)

    def stop(self):
        application.require(self.marker.exists(), 'guard_invalid')
        for unit in reversed(application.UNITS):
            self.events.append('stop-' + unit)
            application.require(not self.stop_failure, 'service_stop_failed')
            self.current[unit] = 'inactive'
        self.quiet()

    def quiet(self):
        application.require(not self.populated and all(v == 'inactive' for v in self.current.values()), 'service_stop_failed')

    def activate(self, unit):
        if self.marker.exists():
            return False
        if unit == 'crtstream.service':
            self.current['fs42.service'] = 'active'
        self.current[unit] = 'active'
        return True

    def reboot(self):
        self.current = dict.fromkeys(application.UNITS, 'inactive')
        for unit in application.UNITS:
            self.activate(unit)

    def restore(self, prior, checkpoint):
        application.require(not self.marker.exists(), 'service_restore_failed')
        for unit in application.UNITS:
            application.require(unit != self.start_failure, 'service_restore_failed')
            if prior[unit] == 'active':
                self.activate(unit)
                self.events.append('start-' + unit)
            else:
                application.require(self.current[unit] == 'inactive', 'service_restore_failed')
            checkpoint('service-' + unit)
        application.require(self.current == prior, 'service_restore_failed')


class ApplicationTests(unittest.TestCase):
    def setUp(self):
        # Production catalog writer/reconciliation and selection recorder fixture.
        helper = selection_tests.SelectionEvidenceTests()
        self.addCleanup(helper.doCleanups)
        f, request, active, recorder = helper.fixture(3)
        helper.increment(f, active, recorder, 2)
        with closing(sqlite3.connect(f.databases[1])) as connection:
            channel = recorder.finish(connection)
        schedule = artifact.export_candidate(f.stage, f.response, PROPOSAL, POLICY,
                    request=request, selection={'channels': [channel]})
        self.root = f.root / 'application-root'
        (self.root / 'runtime').mkdir(parents=True)
        os.chmod(self.root, 0o700)
        os.chmod(self.root / 'runtime', 0o700)
        self.database = self.root / 'runtime/fs42_fluid.db'
        shutil.copyfile(f.databases[0], self.database)
        os.chmod(self.database, 0o600)
        with closing(sqlite3.connect(self.database)) as c:
            c.execute("INSERT INTO named_sequence VALUES(100,'Watch In Order','sequence','tag',0,1,7,1,NULL)")
            c.execute("INSERT INTO sequence_entries VALUES(100,'synthetic',7,100)")
            c.execute("INSERT INTO sequence_group_state VALUES('Watch In Order','sequence','parent','tag')")
            c.commit()
            self.before = application.fingerprint(c)['digest']
        self.document = {'schema_version': 2, 'proposal_id': PROPOSAL['proposal_id'],
            'proposal_digest': 'a'*64, 'policy_digest': 'b'*64, 'code_revision': 'c'*40,
            'validation_run': RUN, 'validation_report_digest': 'd'*64, 'normalized_digest': 'e'*64,
            'inputs': dict(INPUTS, original_logical_database_fingerprint=self.before),
            'directive': PROPOSAL['directives'][0], 'schedule': schedule}
        self.rebind()
        self.bindings = FakeBindings()
        self.services = FakeServices(self.root)
        self.now = datetime(2026, 9, 21, tzinfo=ZoneInfo('America/Los_Angeles'))
        self.checkpoint = lambda name: None
        # A test accident must not contact systemd or run a worker/probe.
        self.forbid = patch('station_director.schedule_application.subprocess.run', side_effect=AssertionError('no subprocess in synthetic application tests'))
        self.forbid.start()
        self.addCleanup(self.forbid.stop)

    def rebind(self):
        artifact.validate_candidate(self.document)
        self.digest = hashlib.sha256(artifact._json(self.document)).hexdigest()

    def app(self):
        return application.Application(self.root, self.document, self.digest,
            self.services, self.bindings, application.Budget(), self.checkpoint, lambda: self.now)

    def current_digest(self):
        with closing(sqlite3.connect(self.database)) as connection:
            return application.fingerprint(connection)['digest']

    def crash_at(self, point):
        def crash(name):
            if name == point:
                raise Crash()
        self.checkpoint = crash

    def expect_code(self, code, action):
        with self.assertRaises(application.ApplicationError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)

    def test_apply_repeat_and_exact_rollback(self):
        applied = self.app().run('apply')
        self.assertEqual(applied['database_outcome'], 'applied')
        self.assertEqual(applied['service_outcome'], 'restored')
        self.assertNotEqual(self.current_digest(), self.before)
        self.assertFalse(self.services.marker.exists())
        self.assertEqual(self.services.events[:2], ['stop-crtstream.service','stop-fs42.service'])
        events = list(self.services.events)
        self.assertEqual(self.app().run('apply'), applied)
        self.assertEqual(self.services.events, events)
        rolled = self.app().run('rollback')
        self.assertEqual(rolled['database_outcome'], 'rolled_back')
        self.assertEqual(self.current_digest(), self.before)
        self.assertEqual(self.app().run('rollback'), rolled)
        self.expect_code('receipt_invalid', lambda: self.app().run('apply'))

    def test_stale_before_and_during_transaction(self):
        self.bindings.fail_at = 3  # inside BEGIN IMMEDIATE, immediately before commit
        self.expect_code('binding_changed', lambda: self.app().run('apply'))
        self.assertEqual(self.current_digest(), self.before)
        self.assertTrue(self.services.marker.exists())
        self.assertEqual(self.app().run('recover')['database_outcome'], 'not_applied')

    def test_rollback_refuses_subsequent_state(self):
        self.app().run('apply')
        with closing(sqlite3.connect(self.database)) as c:
            c.execute("UPDATE named_sequence SET current_index=current_index+1")
            c.commit()
        changed = self.current_digest()
        self.expect_code('database_changed', lambda: self.app().run('rollback'))
        self.assertEqual(self.current_digest(), changed)
        # Refused before a prepared transaction: recovery can safely abort and
        # restore services without overwriting the legitimate subsequent change.
        self.assertEqual(self.app().run('recover')['database_outcome'], 'applied')
        self.assertEqual(self.current_digest(), changed)
        self.assertFalse(self.services.marker.exists())

    def test_approval_revision_window_guard_and_prior_states(self):
        self.assertEqual(application.execute('apply', self.digest, 'wrong')['code'], 'approval_invalid')
        self.bindings.bad_code = True
        self.expect_code('binding_changed', lambda: self.app().run('apply'))
        self.bindings.bad_code = False
        self.now = datetime(2026,9,22,19,31,tzinfo=ZoneInfo('America/Los_Angeles'))
        self.expect_code('window_closed', lambda: self.app().run('apply'))
        self.now -= timedelta(days=1)
        self.services.guard_valid = False
        self.expect_code('guard_invalid', lambda: self.app().run('apply'))
        self.services.guard_valid = True
        self.services.current = dict(zip(application.UNITS, ('inactive','active')))
        self.expect_code('service_state_unsupported', lambda: self.app().run('apply'))
        self.assertEqual(self.current_digest(), self.before)

    def test_supported_inactive_and_mixed_states(self):
        for prior in (('inactive','inactive'), ('active','inactive')):
            with self.subTest(prior=prior):
                other=ApplicationTests();other.setUp()
                try:
                    other.services.current = dict(zip(application.UNITS, prior))
                    other.app().run('apply')
                    self.assertEqual(other.services.current, dict(zip(application.UNITS, prior)))
                    other.app().run('rollback')
                    self.assertEqual(other.services.current, dict(zip(application.UNITS, prior)))
                finally:other.doCleanups()

    def test_preserved_history_catalog_and_sequences(self):
        with closing(sqlite3.connect(self.database)) as c:
            before_catalog = artifact._rows(c,'catalog_entries')[1]
            before_blocks = artifact._rows(c,'liquid_blocks')[1]
            seq = application.fingerprint(c)['sequence_tables']
            allocator = c.execute('SELECT * FROM sqlite_sequence ORDER BY name').fetchall()
        self.app().run('apply')
        with closing(sqlite3.connect(self.database)) as c:
            self.assertEqual(application.fingerprint(c)['sequence_tables'], seq)
            for old in before_blocks:
                row = c.execute('SELECT * FROM liquid_blocks WHERE id=?',(old['id'],)).fetchone()
                self.assertEqual(tuple(old.values()),row)
            new = {r['id']: r for r in artifact._rows(c,'catalog_entries')[1]}
            for old in before_catalog:
                self.assertEqual({k:v for k,v in old.items() if k not in ('count','updated_at')},
                                 {k:v for k,v in new[old['id']].items() if k not in ('count','updated_at')})
        self.app().run('rollback')
        with closing(sqlite3.connect(self.database)) as c:
            self.assertEqual(c.execute('SELECT * FROM sqlite_sequence ORDER BY name').fetchall(),allocator)

    def test_row_collision_preserves_database(self):
        row = self.document['schedule']['rows'][0]
        row['id'] = 1  # retained historical block in production-style fixture
        mapping = {m['live_id']: m for m in self.document['schedule']['catalog_mapping']}
        self.document['schedule']['effect'] = artifact.effect_evidence([row], mapping,self.document['directive'])
        self.rebind()
        self.expect_code('row_collision',lambda:self.app().run('apply'))
        self.assertEqual(self.current_digest(),self.before)

    def test_foreign_marker_is_preserved(self):
        with self.app().store.open() as store:
            application.publish(store.parent,application.MARKER,b'foreign-operation')
        self.expect_code('unsafe_state',lambda:self.app().run('apply'))
        self.assertEqual(self.services.marker.read_bytes(),b'foreign-operation')
        self.assertEqual(self.services.events,[])

    def test_stop_failure_and_cgroup_population_inhibit(self):
        self.services.populated=True
        self.expect_code('service_stop_failed',lambda:self.app().run('apply'))
        self.assertTrue(self.services.marker.exists())
        self.assertEqual(self.current_digest(),self.before)
        self.services.reboot()
        self.assertTrue(all(s=='inactive' for s in self.services.current.values()))
        self.assertFalse(self.services.activate('crtstream.service'))
        self.services.populated=False
        self.assertEqual(self.app().run('recover')['database_outcome'],'not_applied')

    def test_restore_failure_recovery_never_replays_database(self):
        self.services.start_failure='crtstream.service'
        self.expect_code('service_restore_failed',lambda:self.app().run('apply'))
        self.assertFalse(self.services.marker.exists())
        # Playback may change state after the database outcome was sealed.
        with closing(sqlite3.connect(self.database)) as c:
            c.execute('UPDATE named_sequence SET current_index=99');c.commit()
        changed=self.current_digest()
        self.services.start_failure=None
        result=self.app().run('recover')
        self.assertEqual(result['database_outcome'],'applied')
        self.assertEqual(self.current_digest(),changed)

    def test_all_apply_interruption_boundaries(self):
        # Each subcase is a fresh disposable production-helper fixture.
        points = ('apply-intent','marker-created','services-stopped','apply-prepared',
                  'before-commit','after-commit','apply-database','apply-restoring',
                  'marker-removed','service-fs42.service','service-crtstream.service','apply-complete')
        for point in points:
            with self.subTest(point=point):
                other=ApplicationTests();other.setUp()
                try:
                    other.crash_at(point)
                    with self.assertRaises(Crash):other.app().run('apply')
                    other.checkpoint=lambda name:None
                    result=other.app().run('recover')
                    expected='not_applied' if points.index(point)<points.index('after-commit') else 'applied'
                    self.assertEqual(result['database_outcome'],expected)
                    self.assertEqual(result['service_outcome'],'restored')
                    if expected=='not_applied':self.assertEqual(other.current_digest(),other.before)
                finally:other.doCleanups()

    def test_all_rollback_interruption_boundaries(self):
        points=('rollback-intent','marker-created','services-stopped','rollback-prepared',
                'before-commit','after-commit','rollback-database','rollback-restoring',
                'marker-removed','service-fs42.service','service-crtstream.service','rollback-complete')
        for point in points:
            with self.subTest(point=point):
                other=ApplicationTests();other.setUp()
                try:
                    other.app().run('apply');other.crash_at(point)
                    with self.assertRaises(Crash):other.app().run('rollback')
                    other.checkpoint=lambda name:None
                    result=other.app().run('recover')
                    expected='applied' if points.index(point)<points.index('after-commit') else 'rolled_back'
                    self.assertEqual(result['database_outcome'],expected)
                    if expected=='rolled_back':self.assertEqual(other.current_digest(),other.before)
                finally:other.doCleanups()

    def test_durable_write_failure_keeps_inhibition(self):
        real=application.publish
        def fail(parent,name,raw):
            if name=='apply.database.json':raise OSError('synthetic private failure')
            return real(parent,name,raw)
        with patch.object(application,'publish',side_effect=fail),self.assertRaises(OSError):self.app().run('apply')
        self.assertTrue(self.services.marker.exists())
        self.assertEqual(self.app().run('recover')['database_outcome'],'applied')

    def test_each_transition_publication_failure(self):
        for phase in ('intent','prepared','restoring','complete'):
            with self.subTest(phase=phase):
                other=ApplicationTests();other.setUp()
                try:
                    real=application.publish
                    def fail(parent,name,raw):
                        if name=='apply.'+phase+'.json':raise OSError('synthetic write failure')
                        return real(parent,name,raw)
                    with patch.object(application,'publish',side_effect=fail),self.assertRaises(OSError):other.app().run('apply')
                    if phase=='intent':
                        self.assertEqual(other.current_digest(),other.before)
                        self.assertFalse(other.services.marker.exists())
                    else:
                        result=other.app().run('recover')
                        self.assertEqual(result['database_outcome'],'not_applied' if phase=='prepared' else 'applied')
                finally:other.doCleanups()

    def test_backup_tamper_and_unknown_database(self):
        self.crash_at('apply-prepared')
        with self.assertRaises(Crash):self.app().run('apply')
        self.checkpoint=lambda name:None
        backup=self.app().store.path/'baseline.sqlite3'
        with backup.open('ab') as stream:stream.write(b'tampering')
        self.expect_code('backup_invalid',lambda:self.app().run('recover'))
        self.assertTrue(self.services.marker.exists())

    def test_third_database_state_after_preparation_remains_inhibited(self):
        self.crash_at('apply-prepared')
        with self.assertRaises(Crash):self.app().run('apply')
        self.checkpoint=lambda name:None
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute('UPDATE named_sequence SET current_index=99');connection.commit()
        changed=self.current_digest()
        self.expect_code('database_unknown',lambda:self.app().run('recover'))
        self.assertTrue(self.services.marker.exists())
        self.assertEqual(self.current_digest(),changed)

    def test_redacted_cli_and_no_invocation_in_codex(self):
        with patch('station_director.isolation.check_invocation_context',return_value=(False,'private reason')):
            result=application.execute('apply',self.digest,self.digest)
        self.assertEqual(result['code'],'unsafe_state')
        self.assertNotIn('private',json.dumps(result))
        self.assertNotIn(str(self.root),json.dumps(result))

    def test_budget_and_commit_window_are_rechecked(self):
        budget=application.Budget()
        budget.deadline=0
        self.expect_code('budget_exceeded',budget.check)
        def advance(name):
            if name=='apply-prepared':
                self.now=datetime(2026,9,22,19,59,tzinfo=ZoneInfo('America/Los_Angeles'))
        self.checkpoint=advance
        self.expect_code('window_closed',lambda:self.app().run('apply'))
        self.assertEqual(self.current_digest(),self.before)
        # Resolving a noncommitted transaction is still allowed after the window.
        self.checkpoint=lambda name:None
        self.assertEqual(self.app().run('recover')['database_outcome'],'not_applied')

    def test_both_locks_exclude_writer(self):
        import fcntl
        with self.app().store.open() as store:
            for name in ('.schedule-validation.lock','.chapter-cache-maintenance.lock'):
                fd=os.open(name,os.O_RDWR,dir_fd=store.parent)
                try:
                    with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(fd)

    def test_full_receipt_capacity_rejects_before_creating_an_unrecoverable_entry(self):
        directory=self.root/'runtime/director/applications'
        directory.mkdir(parents=True,mode=0o700)
        os.chmod(directory.parent,0o700)
        for index in range(100):
            (directory/('%064x'%index)).mkdir(mode=0o700)
        self.expect_code('unsafe_state',lambda:self.app().run('apply'))
        self.assertFalse(self.app().store.path.exists())
        self.assertEqual(self.services.events,[])

    def test_unaffected_channel_and_crossing_history(self):
        from test.test_station_director_milestone_b2 import insert_block
        with closing(sqlite3.connect(self.database)) as c:
            insert_block(c,'Watch In Order','2026-09-22 20:00:00','2026-09-22 21:00:00',1,'/mnt/t7/CRT-Media/a.mp4')
            # Use a noncolliding approved replacement ID; the exporter binds it.
            c.execute("UPDATE liquid_blocks SET id=100 WHERE station='Watch In Order'")
            c.commit()
            protected=c.execute("SELECT * FROM liquid_blocks WHERE station='Watch In Order'").fetchall()
            self.before=application.fingerprint(c)['digest']
        self.document['inputs']['original_logical_database_fingerprint']=self.before
        self.rebind()
        self.app().run('apply')
        with closing(sqlite3.connect(self.database)) as c:
            self.assertEqual(c.execute("SELECT * FROM liquid_blocks WHERE station='Watch In Order'").fetchall(),protected)
        self.app().run('rollback')
        self.assertEqual(self.current_digest(),self.before)

    def test_failure_receipt_redaction(self):
        self.bindings.fail_at=3
        self.expect_code('binding_changed',lambda:self.app().run('apply'))
        failures=list(self.app().store.path.glob('failure-*.json'))
        self.assertEqual(len(failures),1)
        raw=failures[0].read_text()
        self.assertEqual(json.loads(raw)['code'],'binding_changed')
        for secret in ('synthetic-series','/mnt','private reason','a.mp4',str(self.root)):
            self.assertNotIn(secret,raw)

    def test_interrupted_abort_recovery_is_repeatable(self):
        self.crash_at('services-stopped')
        with self.assertRaises(Crash):self.app().run('apply')
        self.crash_at('apply-prepared')
        with self.assertRaises(Crash):self.app().run('recover')
        self.checkpoint=lambda name:None
        self.assertEqual(self.app().run('recover')['database_outcome'],'not_applied')

    def test_another_pending_operation_blocks_even_after_marker_removal(self):
        self.crash_at('marker-removed')
        with self.assertRaises(Crash):self.app().run('apply')
        self.assertFalse(self.services.marker.exists())
        other=application.Store(self.root,'f'*64)
        with self.assertRaises(application.ApplicationError) as caught:
            with other.open():self.fail('pending operation admitted')
        self.assertEqual(caught.exception.code,'unsafe_state')
        self.checkpoint=lambda name:None
        self.app().run('recover')
        with other.open():pass

    def test_prepared_sqlite_hot_journal_recovery(self):
        # A synthetic child exits without close/rollback, unlike an exception
        # fixture whose Python cleanup would silently do the rollback for us.
        import time
        import signal
        self.crash_at('apply-prepared')
        with self.assertRaises(Crash):self.app().run('apply')
        child=os.fork()
        if child==0:
            try:
                c=sqlite3.connect(self.database)
                c.execute('PRAGMA cache_size=1')
                c.execute('PRAGMA cache_spill=ON')
                c.execute('BEGIN IMMEDIATE')
                application.replace_rows(c,self.document)
                os._exit(0)
            except BaseException:
                os._exit(1)
        deadline=time.monotonic()+10
        try:
            while True:
                pid,status=os.waitpid(child,os.WNOHANG)
                if pid:
                    child=None
                    self.assertEqual(os.waitstatus_to_exitcode(status),0)
                    break
                if time.monotonic()>deadline:self.fail('synthetic child timeout')
                time.sleep(0.01)
        finally:
            if child is not None:
                os.kill(child,signal.SIGKILL);os.waitpid(child,0)
        self.assertTrue(Path(str(self.database)+'-journal').exists())
        self.checkpoint=lambda name:None
        self.assertEqual(self.app().run('recover')['database_outcome'],'not_applied')
        self.assertEqual(self.current_digest(),self.before)

    def test_write_ahead_log_apply_and_rollback(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
        self.app().run('apply')
        self.app().run('rollback')
        self.assertEqual(self.current_digest(),self.before)

    def test_database_identity_and_symlink_rejected(self):
        self.crash_at('apply-prepared')
        with self.assertRaises(Crash):self.app().run('apply')
        self.checkpoint=lambda name:None
        replaced=self.database.with_name('replacement.sqlite3')
        shutil.copyfile(self.database,replaced);os.chmod(replaced,0o600)
        os.replace(replaced,self.database)
        self.expect_code('database_unknown',lambda:self.app().run('recover'))
        self.assertTrue(self.services.marker.exists())

    def test_catalog_mapping_tamper_rejected_against_backup(self):
        # The digest approval alone cannot turn inconsistent mapping semantics
        # into permission to write otherwise well-formed schedule references.
        for item in self.document['schedule']['catalog_mapping']:
            item['semantics']['row']['title']='changed synthetic title'
        # selection-state carries its own protected semantics and validates this
        # tampering earlier; direct backup verification additionally binds rows.
        app=self.app()
        with app.store.open():
            shutil.copyfile(self.database,app.store.path/'baseline.sqlite3')
            os.chmod(app.store.path/'baseline.sqlite3',0o600)
            self.expect_code('binding_changed',app.verify_backup)

    def test_database_link_rejection(self):
        other=self.database.with_name('hardlink.sqlite3')
        os.link(self.database,other)
        self.expect_code('unsafe_state',self.app().db_identity)
        other.unlink()
        self.database.rename(other)
        self.database.symlink_to(other)
        with self.assertRaises(OSError):self.app().db_identity()

    def test_cli_dispatch_is_lazy_and_redacted(self):
        from station_director import cli
        import io
        result={'code':'ok','database_outcome':'applied','service_outcome':'restored'}
        output=io.StringIO()
        with patch.object(application,'execute',return_value=result) as execute,patch('sys.stdout',output):
            self.assertEqual(cli.main(['schedule','recover',self.digest,'--approve',self.digest]),0)
        execute.assert_called_once_with('recover',self.digest,self.digest)
        self.assertEqual(json.loads(output.getvalue()),result)


class ServiceAdapterTests(unittest.TestCase):
    def test_failed_restoration_can_retry_without_failed_prior_admission(self):
        service=application.Services(Path('/tmp/synthetic'),application.Budget())
        states=dict.fromkeys(application.UNITS,'failed')
        def properties(unit):
            state=states[unit]
            return {'ActiveState':state,'SubState':'running' if state=='active' else 'failed','Job':''}
        def command(argv,timeout=10):
            self.assertEqual(argv[:3],['systemctl','--user','start'])
            states[argv[-1]]='active'
            return ''
        with patch.object(service,'properties',side_effect=properties),patch.object(service,'command',side_effect=command):
            with self.assertRaises(application.ApplicationError):service.states()
            service.restore(dict.fromkeys(application.UNITS,'active'),lambda name:None)
            self.assertEqual(service.states(),dict.fromkeys(application.UNITS,'active'))

    def test_effective_guards_and_dependency_activation_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            home=Path(temp); root=home/'FieldStation42'
            service=application.Services(root,application.Budget())
            marker=str(root/'runtime/director'/application.MARKER)
            for unit in application.UNITS:
                name='50-director-maintenance.conf'
                src=artifact.ROOT/'station_director/systemd'/(unit+'.d')/name
                template=root/'station_director/systemd'/(unit+'.d')/name
                template.parent.mkdir(parents=True,mode=0o700)
                template.write_bytes(src.read_bytes())
                installed=home/'.config/systemd/user'/(unit+'.d')
                installed.mkdir(parents=True,mode=0o700)
                (installed/name).write_bytes(src.read_bytes())
                os.chmod(installed/name,0o600)
            condition=['ConditionPathExists',False,True,marker,0]
            objects={unit:'/org/freedesktop/systemd1/unit/'+unit.replace('.', '_2e') for unit in application.UNITS}
            replies={unit:{'type':'a(sbbsi)','data':[condition]} for unit in application.UNITS}
            checked=[]
            def command(argv,timeout=10):
                if 'GetUnit' in argv:return json.dumps({'type':'o','data':[objects[argv[-1]]]})
                self.assertIn('Conditions',argv)
                unit=next(unit for unit,obj in objects.items() if obj in argv)
                checked.append(unit)
                return json.dumps(replies[unit])
            def properties(unit):
                return {'LoadState':'loaded','NeedDaemonReload':'no','KillMode':'control-group',
                    'SendSIGKILL':'yes','TimeoutStopUSec':'1min 30s',
                    'DropInPaths':str(home/'.config/systemd/user'/(unit+'.d')/'50-director-maintenance.conf')}
            with patch.object(Path,'home',return_value=home),patch.object(service,'properties',side_effect=properties), \
                    patch.object(service,'command',side_effect=command):
                service.guards()
                self.assertEqual(checked,list(application.UNITS))
                # Last evaluation is an int32, not evidence of current inhibition.
                for result in (-1,0,1):
                    condition[4]=result
                    service.guards()
                condition[4]=0
                invalid=[None,[],{}, {'type':'a(sbbsi)'},
                    {'type':'as','data':[condition]}]
                malformed_arrays=[None,False,0,'',{},[],condition,[[condition]],
                    [None],[False],[1],['condition'],[{}],
                    [condition[:-1]],[condition+[0]],
                    [condition,condition],
                    [condition,['ConditionUser',False,True,'root',1]],
                    [['ConditionUser',False,True,'root',1]]]
                for index,values in ((0,(None,False,1,[],{},'ConditionUser')),
                        (1,(None,0,1,'false',[],{},True)),
                        (2,(None,0,1,'true',[],{},False)),
                        (3,(None,False,1,[],{},'wrong',marker+'/')),
                        (4,(None,False,True,1.0,'1',[],{},-(2**31)-1,2**31))):
                    for value in values:
                        bad=list(condition);bad[index]=value
                        malformed_arrays.append([bad])
                invalid.extend({'type':'a(sbbsi)','data':data} for data in malformed_arrays)
                for unit in application.UNITS:
                    good=replies[unit]
                    for number,bad in enumerate(invalid):
                        with self.subTest(unit=unit,case=number):
                            replies[unit]=bad
                            checked.clear()
                            with self.assertRaises(application.ApplicationError) as caught:service.guards()
                            self.assertEqual(caught.exception.code,'guard_invalid')
                            self.assertEqual(checked,list(application.UNITS[:application.UNITS.index(unit)+1]))
                    replies[unit]=good
                with patch.object(service,'command',return_value='{}'),self.assertRaises(application.ApplicationError):service.guards()
                installed=home/'.config/systemd/user/fs42.service.d/50-director-maintenance.conf'
                installed.write_text('[Unit]\nConditionPathExists=!wrong\n')
                with self.assertRaises(application.ApplicationError):service.guards()

    def test_cgroup_descendant_and_state_checks(self):
        service=application.Services(Path('/tmp/synthetic'),application.Budget())
        props={'ActiveState':'inactive','SubState':'dead','MainPID':'0','Job':'','ControlGroup':''}
        with patch.object(service,'properties',return_value=props):
            with patch.object(Path,'read_text',return_value='populated 0\nfrozen 0\n'):
                service.empty('fs42.service','/user.slice/synthetic')
            with patch.object(Path,'read_text',return_value='populated 1\nfrozen 0\n'),self.assertRaises(application.ApplicationError):
                service.empty('fs42.service','/user.slice/synthetic')
            props['MainPID']='123'
            with self.assertRaises(application.ApplicationError):service.empty('fs42.service')

    def test_signal_and_alarm_restore_handlers(self):
        import signal
        previous=signal.getsignal(signal.SIGTERM)
        budget=application.Budget()
        with self.assertRaises(KeyboardInterrupt):
            with budget.alarm():os.kill(os.getpid(),signal.SIGTERM)
        self.assertEqual(signal.getsignal(signal.SIGTERM),previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))
        with self.assertRaises(application.ApplicationError) as caught:
            with budget.alarm():os.kill(os.getpid(),signal.SIGALRM)
        self.assertEqual(caught.exception.code,'budget_exceeded')


class BindingTests(unittest.TestCase):
    def test_actual_configuration_media_and_context_binding_helpers(self):
        from station_director import preservation, validation_context as context, secure_validation_inputs as secure
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            (root/'confs').mkdir();(root/'runtime').mkdir()
            configuration=root/'confs/action.json'
            configuration.write_text('{"station_conf":{"network_name":"Action"}}')
            wio=root/'runtime/watch_in_order_state.json';wio.write_text('{}')
            media=root/'synthetic-media';media.mkdir()
            sample=media/'synthetic.bin';sample.write_bytes(b'synthetic fixture')
            proposal=dict(PROPOSAL,seed=4242,week_end='2026-09-29T20:00:00-07:00')
            paths=preservation.protected_json_paths(root)
            logical=context.logical_protected_configuration_fingerprint(paths)['digest']
            physical=preservation.fingerprint_json_files(paths)['digest']
            capture=preservation.capture_media_manifest
            manifest=capture(media)
            try:
                logical_media=context.logical_media_manifest_fingerprint(manifest)['digest']
                physical_media=manifest.summary['digest']
            finally:manifest.close()
            inputs=dict(INPUTS,original_logical_configuration_fingerprint=logical,
                live_physical_configuration_fingerprint=physical,logical_media_manifest_fingerprint=logical_media,
                physical_media_manifest_fingerprint=physical_media)
            inputs['validation_context_fingerprint']=artifact.digest(context.derive_validation_context(
                proposal,POLICY,context.canonical_seed_inputs(logical,inputs['original_logical_database_fingerprint'],logical_media)))
            doc={'proposal_id':proposal['proposal_id'],'proposal_digest':application.sha(artifact._json(proposal)+b'\n'),
                'policy_digest':artifact.digest(POLICY),'inputs':inputs}
            checker=application.Bindings(root)
            with patch.object(secure,'load_canonical_proposal',return_value=proposal), \
                    patch.object(secure,'load_canonical_policy',return_value=POLICY), \
                    patch.object(preservation,'capture_media_manifest',side_effect=lambda unused:capture(media)):
                checker.inputs(doc)
                for field in ('live_physical_configuration_fingerprint','original_logical_configuration_fingerprint',
                        'logical_media_manifest_fingerprint','physical_media_manifest_fingerprint','validation_context_fingerprint'):
                    changed=copy.deepcopy(doc);changed['inputs'][field]='f'*64
                    with self.subTest(field=field),self.assertRaises(application.ApplicationError):checker.inputs(changed)
                sample.write_bytes(b'changed synthetic fixture')
                with self.assertRaises(application.ApplicationError):checker.inputs(doc)


if __name__=='__main__':
    unittest.main()
