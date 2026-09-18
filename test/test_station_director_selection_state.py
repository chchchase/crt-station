"""Selection attribution and candidate v2: disposable synthetic SQLite only."""
import copy
import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from station_director import schedule_artifact as artifact, selection_state as selection


def make_evidence(stage, request, revision, *, active=None):
    """Only for synthetic already-rebuilt zero-count fixtures; no worker stub."""
    request.setdefault('run_id', 'synthetic-selection.run-1')
    with closing(sqlite3.connect(stage / 'work/runtime/fs42_fluid.db')) as connection:
        catalog = artifact._catalog(connection)
        if active is None:
            active = {i for i, (r, _) in catalog.items() if r['station'] == 'Action' and r['path'].startswith('/media/')}
        rebuilt = [catalog[i][0] for i in sorted(active)]
        clock = rebuilt[0]['updated_at']
        request.setdefault('validation_context', {}).setdefault('reference_clock', clock)
        recorder = selection.Recorder(connection, 'Action', active, rebuilt, clock)
        evidence = recorder.finish(connection)
    marker = {'request_digest': artifact.digest(request), 'code_revision': revision}
    selection.publish(stage, marker, request, [evidence])
    return recorder


class SelectionEvidenceTests(unittest.TestCase):
    def fixture(self, baseline_count=0, *, commercial=False):
        from test.test_station_director_schedule_artifact import ArtifactTests, PROPOSAL, POLICY
        f = ArtifactTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        if commercial:
            request = f.commercial_rebuild()
        else:
            f.production_clock_rebuild(protected=True)
            request = {'proposal': PROPOSAL, 'policy': POLICY}
        request.update(run_id='synthetic.run-1', validation_context={'reference_clock':'2026-09-22T20:00:00'})
        f.update("UPDATE catalog_entries SET count=? WHERE tag='synthetic-series'", (baseline_count,), sides=(0,1))
        # The retained historical row keeps its exact baseline count; active rebuild starts at zero.
        f.update("UPDATE catalog_entries SET count=0 WHERE path LIKE '/media/%'")
        with closing(sqlite3.connect(f.databases[1])) as c:
            catalog = artifact._catalog(c)
            active = {i for i,(r,s) in catalog.items() if r['path'].startswith('/media/')}
            recorder = selection.Recorder(c,'Action',active,[catalog[i][0] for i in sorted(active)],'2026-09-22 20:00:00')
        return f,request,active,recorder

    def increment(self, f, active, recorder, times):
        from fs42.catalog_api import CatalogAPI
        from fs42.scheduling_context import activate_catalog_selection, activate_validation_context, ValidationSchedulingContext
        with closing(sqlite3.connect(f.databases[1])) as c, \
                patch('fs42.catalog_io.StationManager',return_value=SimpleNamespace(server_conf={'db_path':str(f.databases[1])})), \
                activate_validation_context(ValidationSchedulingContext(datetime(2026,9,22,20),datetime(2026,9,22,20),datetime(2026,9,29,20),42)), \
                activate_catalog_selection(c,'Action',active,count_observer=recorder.observe):
            entry=next(e for e in CatalogAPI.get_entries({'network_name':'Action'}) if e.tag=='synthetic-series')
            CatalogAPI.update_play_counts({'network_name':'Action'},[entry]*times)

    def test_reset_selection_combined_net_zero(self):
        from test.test_station_director_schedule_artifact import PROPOSAL,POLICY
        for baseline,increments in ((3,0),(0,2),(3,2),(2,2),(0,0)):
            with self.subTest(baseline=baseline,increments=increments):
                f,request,active,recorder=self.fixture(baseline)
                before=f.databases[0].read_bytes()
                self.increment(f,active,recorder,increments)
                with closing(sqlite3.connect(f.databases[1])) as c: channel=recorder.finish(c)
                schedule=artifact.export_candidate(f.stage,f.response,PROPOSAL,POLICY,request=request,selection={'channels':[channel]})
                doc={'schema_version':2,'proposal_id':PROPOSAL['proposal_id'],'proposal_digest':'a'*64,
                     'policy_digest':'b'*64,'code_revision':'c'*40,'validation_run':'v-20260916T075149568345Z-'+'a'*32,
                     'validation_report_digest':'d'*64,'normalized_digest':'e'*64,'inputs':__import__('test.test_station_director_schedule_artifact',fromlist=['INPUTS']).INPUTS,
                     'directive':PROPOSAL['directives'][0],'schedule':schedule}
                artifact.validate_candidate(doc)
                entry=next(e for e in schedule['selection_state']['entries'] if e['validated_semantics']['row']['tag']=='synthetic-series')
                self.assertEqual((entry['baseline_count'],entry['proposed_count'],entry['reset_delta'],entry['selection_increments']),
                                 (baseline,increments,-baseline,increments))
                self.assertEqual(entry['timestamp_disposition'],'native_selection' if increments else 'native_reset' if baseline else 'preserve_baseline')
                self.assertEqual(f.databases[0].read_bytes(),before)
                summary=artifact.summary(doc,'f'*64)['selection_state']
                self.assertEqual(summary['unaffected_channel_mutations'],0)
                self.assertNotIn('synthetic-series',json.dumps(summary))
                for field in ('proposed_count','baseline_count','selection_increments','reset_delta'):
                    bad=copy.deepcopy(doc); bad['schedule']['selection_state']['entries'][0][field]+=1
                    with self.assertRaises(artifact.ArtifactError): artifact.validate_candidate(bad)

    def test_unobserved_changes_and_absolute_writes_rejected(self):
        f,request,active,recorder=self.fixture()
        f.update("UPDATE catalog_entries SET count=count+1 WHERE path LIKE '/media/%'")
        with closing(sqlite3.connect(f.databases[1])) as c,self.assertRaises(artifact.ArtifactError): recorder.finish(c)
        with self.assertRaises(artifact.ArtifactError): recorder.observe('Action','/media/a.mp4','set',[],[])

    def test_missing_wrong_request_and_unsafe_evidence(self):
        f,request,active,recorder=self.fixture()
        with self.assertRaises((FileNotFoundError,artifact.ArtifactError)): selection.load(f.stage,request,'a'*40)
        with closing(sqlite3.connect(f.databases[1])) as c: channel=recorder.finish(c)
        marker={'request_digest':artifact.digest(request),'code_revision':'a'*40}
        selection.publish(f.stage,marker,request,[channel])
        self.assertEqual(selection.load(f.stage,request,'a'*40)['channels'],[channel])
        with self.assertRaises(artifact.ArtifactError): selection.load(f.stage,dict(request,run_id='different'),'a'*40)
        with self.assertRaises(artifact.ArtifactError): selection.load(f.stage,request,'b'*40)
        import os
        os.chmod(f.stage/selection.OUTPUT,0o644)
        with self.assertRaises(artifact.ArtifactError): selection.load(f.stage,request,'a'*40)

    def test_shared_path_groups_and_unexplained_timestamps(self):
        from test.test_catalog_selection_scope import SelectionScopeTests
        from fs42.catalog_api import CatalogAPI
        from fs42.scheduling_context import activate_catalog_selection
        f=SelectionScopeTests(); f.setUp(); self.addCleanup(f.doCleanups)
        with closing(sqlite3.connect(f.paths['staged'])) as c:
            catalog=artifact._catalog(c)
            recorder=selection.Recorder(c,'Action',f.active,[catalog[i][0] for i in sorted(f.active)],'2026-09-22 20:00:00')
            with f.io(),f.context(),activate_catalog_selection(c,'Action',f.active,count_observer=recorder.observe):
                entry=next(e for e in CatalogAPI.get_entries(f.conf) if e.tag=='series' and e.path.endswith('/a.mp4'))
                CatalogAPI.update_play_counts(f.conf,[entry])
            evidence=recorder.finish(c)
            self.assertEqual(len(evidence['events'][0]['ids']),2)
            selection.verify_channel(evidence,artifact._catalog(c))
            self.assertEqual(f.preserved(c),f.before)
            bad=copy.deepcopy(evidence); bad['events'][0]['ids'].pop()
            with self.assertRaises(artifact.ArtifactError): selection.verify_channel(bad,artifact._catalog(c))
            c.execute('UPDATE catalog_entries SET updated_at=? WHERE id=?',('2026-09-23 00:00:00',entry.dbid)); c.commit()
            with self.assertRaises(artifact.ArtifactError): recorder.finish(c)

    def test_evidence_bounds_and_rebuild_rejections(self):
        f,request,active,recorder=self.fixture()
        with closing(sqlite3.connect(f.databases[1])) as c:
            catalog=artifact._catalog(c)
            generated=[catalog[i][0] for i in sorted(active)]
            for change in ('count','station','duration','missing','duplicate'):
                rows=copy.deepcopy(generated)
                if change=='missing': rows.pop()
                elif change=='duplicate': rows.append(copy.deepcopy(rows[0]))
                elif change=='station': rows[0]['station']='Other'
                else: rows[0][change]+=1
                with self.assertRaises(artifact.ArtifactError):
                    selection.Recorder(c,'Action',active,rows,'2026-09-22 20:00:00')
        with patch.object(selection,'MAX_EVENTS',0),self.assertRaises(artifact.ArtifactError):
            self.increment(f,active,recorder,1)
        for value in (True,None,-1,2**63,1.5):
            with self.assertRaises(artifact.ArtifactError): selection.count(value)

    def test_commit_failure_cannot_produce_finished_evidence(self):
        from fs42.catalog_io import CatalogIO
        from fs42.scheduling_context import activate_catalog_selection,activate_validation_context,ValidationSchedulingContext
        f,request,active,recorder=self.fixture()
        fail=[False]; connect=sqlite3.connect
        class Connection(sqlite3.Connection):
            def commit(self):
                if fail[0]: raise sqlite3.OperationalError('synthetic commit failure')
                return super().commit()
        with closing(connect(f.databases[1])) as c, \
                patch('fs42.catalog_io.StationManager',return_value=SimpleNamespace(server_conf={'db_path':str(f.databases[1])})), \
                patch('fs42.catalog_io.sqlite3.connect',side_effect=lambda p:connect(p,factory=Connection)), \
                activate_validation_context(ValidationSchedulingContext(datetime(2026,9,22,20),datetime(2026,9,22,20),datetime(2026,9,29,20),42)), \
                activate_catalog_selection(c,'Action',active,count_observer=recorder.observe):
            io=CatalogIO(); entry=next(e for e in io.get_catalog_entries('Action') if e.tag=='synthetic-series')
            fail[0]=True
            with self.assertRaises(sqlite3.OperationalError): io.batch_increment_counts('Action',[entry])
            with self.assertRaises(artifact.ArtifactError): recorder.finish(c)

    def test_export_rejects_missing_active_evidence_semantics_and_collisions(self):
        from test.test_station_director_schedule_artifact import PROPOSAL,POLICY
        f,request,active,recorder=self.fixture()
        with closing(sqlite3.connect(f.databases[1])) as c:
            channel=recorder.finish(c)
        def export(evidence):
            return artifact.export_candidate(f.stage,f.response,PROPOSAL,POLICY,request=request,selection={'channels':[evidence]})
        for change in ('missing','duplicate','metadata','duration','count','timestamp'):
            bad=copy.deepcopy(channel)
            if change=='missing': bad['rows'].pop()
            elif change=='duplicate': bad['rows'].append(copy.deepcopy(bad['rows'][0]))
            elif change=='count': bad['rows'][0]['final_count']+=1
            elif change=='timestamp': bad['rows'][0]['final_updated_at']='2026-09-23 00:00:00'
            else:
                # Mutation after recording must fail before mapping.
                sql=('UPDATE file_meta SET size=size+1' if change=='metadata' else
                     "UPDATE catalog_entries SET duration=3599 WHERE path LIKE '/media/%'")
                f.update(sql)
            with self.assertRaises(artifact.ArtifactError): export(bad)
            if change=='metadata': f.update('UPDATE file_meta SET size=size-1')
            if change=='duration': f.update("UPDATE catalog_entries SET duration=3600 WHERE path LIKE '/media/%'")
        valid=export(channel)
        mapping={m['validated_id']:m for m in valid['catalog_mapping']}
        proposal=copy.deepcopy(valid['selection_state'])
        proposal['entries'][1]['live_id']=proposal['entries'][0]['live_id']
        with self.assertRaises(artifact.ArtifactError): selection.validate_proposal(proposal,mapping,'Action')

    def test_no_setup_means_no_native_evidence_request(self):
        f,request,active,recorder=self.fixture()
        self.assertIsNone(selection.requested(f.stage,request))
        selection.prepare(SimpleNamespace(stage=f.stage,request=request),'a'*40)
        self.assertEqual(selection.requested(f.stage,request)['code_revision'],'a'*40)
        with self.assertRaises(artifact.ArtifactError): selection.requested(f.stage,dict(request,run_id='wrong'))
        with self.assertRaises(FileExistsError): selection.prepare(SimpleNamespace(stage=f.stage,request=request),'a'*40)


if __name__=='__main__': unittest.main()
