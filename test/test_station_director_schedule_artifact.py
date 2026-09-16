"""Disposable fixtures only; no native workers or live resources."""
import copy
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from station_director import schedule_artifact as artifact
from station_director import cli
from test.test_station_director_milestone_b2 import create_database, insert_block
from test.test_station_director_milestone_c2 import insert_catalog, response

RUN = 'v-20260916T075149568345Z-' + 'a' * 32
PROPOSAL = {'proposal_id': 'p-20260916T063645Z-1bc88c72',
            'assignment_changes': [], 'exclusions': [],
            'week_start': '2026-09-22T20:00:00-07:00',
            'directives': [{'type': 'date_slot', 'channel': 2, 'date': '2026-09-22',
                            'hour': 20, 'series': 'synthetic-series'}]}
POLICY = {'channels': [{'number': 2, 'name': 'Action'}]}
INPUTS = {name: 'a' * 64 for name in (
    'original_logical_configuration_fingerprint', 'original_logical_database_fingerprint',
    'logical_media_manifest_fingerprint', 'live_physical_configuration_fingerprint',
    'physical_media_manifest_fingerprint', 'projected_configuration_fingerprint',
    'working_database_fingerprint', 'validation_context_fingerprint')}


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stage = self.root / 'stage'
        self.databases = []
        for side in ('source', 'work'):
            directory = self.stage / side / 'runtime'
            directory.mkdir(parents=True)
            database = directory / 'fs42_fluid.db'
            connection = create_database(database)
            try:
                insert_catalog(connection, 1, '/media/a.mp4', tag='synthetic-series')
                insert_catalog(connection, 2, '/media/ad.mp4', tag='commercial', content_type='commercial')
                if side == 'work':
                    insert_block(connection, 'Action', '2026-09-22 20:00:00',
                                 '2026-09-22 21:00:00', 1, '/media/a.mp4')
                connection.commit()
            finally:
                connection.close()
            self.databases.append(database)
        self.response = response('a')
        self.response['run_id'] = RUN
        self.response['schema_version'] = 4
        self.response['channels'][0].update(regeneration_start='2026-09-22 20:00:00',
                                            effective_horizon='2026-09-22 21:00:00')
        from station_director.single_run_protocol import RESPONSE_SCHEMA_V4, validate_document, validate_response_semantics
        validate_document(self.response, RESPONSE_SCHEMA_V4)
        validate_response_semantics(self.response)

    def update(self, sql, parameters=(), sides=(1,)):
        for side in sides:
            connection = sqlite3.connect(self.databases[side])
            try:
                connection.execute(sql, parameters)
                connection.commit()
            finally:
                connection.close()

    def export(self):
        return artifact.export_candidate(self.stage, self.response, PROPOSAL, POLICY)

    def document(self):
        return {'schema_version': 1, 'proposal_id': PROPOSAL['proposal_id'],
                'proposal_digest': hashlib.sha256(artifact._json(PROPOSAL) + b'\n').hexdigest(), 'policy_digest': artifact.digest(POLICY),
                'code_revision': 'a' * 40, 'validation_run': RUN,
                'validation_report_digest': 'b' * 64, 'normalized_digest': 'c' * 64,
                'directive': PROPOSAL['directives'][0], 'inputs': INPUTS, 'schedule': self.export()}

    def test_export_translate_and_effect(self):
        doc = self.document()
        artifact.validate_candidate(doc)
        self.assertEqual(doc['schedule']['rows'][0]['plan_json'].count('/mnt/t7/CRT-Media/a.mp4'), 1)
        self.assertEqual(doc['schedule']['effect']['blocks'][0]['features'],
                         [{'start': '2026-09-22 20:00:00', 'end': '2026-09-22 21:00:00'}])
        summary = json.dumps(artifact.summary(doc, 'e' * 64))
        for secret in ('a.mp4', 'synthetic-series', '/media', '/mnt', 'Show'):
            self.assertNotIn(secret, summary)

    def test_catalog_id_mapping(self):
        self.update('UPDATE catalog_entries SET id=100 WHERE id=1')
        self.update("UPDATE liquid_blocks SET content_json='100'")
        doc = self.document()
        artifact.validate_candidate(doc)
        self.assertEqual(doc['schedule']['rows'][0]['content_json'], '1')
        self.assertIn((100, 1), [(r['validated_id'], r['live_id']) for r in doc['schedule']['catalog_mapping']])

    def test_noop_detected_despite_json_formatting_and_aliases(self):
        source = sqlite3.connect(self.databases[0])
        try:
            insert_block(source, 'Action', '2026-09-22 20:00:00',
                         '2026-09-22 21:00:00', 1, 'catalog/crt_media/a.mp4')
            source.commit()
        finally:
            source.close()
        with self.assertRaisesRegex(artifact.ArtifactError, 'candidate_no_change'):
            self.export()

    def test_catalog_or_metadata_changes_rejected(self):
        for statement in ("UPDATE catalog_entries SET duration=3599 WHERE id=1",
                          "INSERT INTO file_meta(path,duration) VALUES('/media/a.mp4',1)"):
            with self.subTest(statement=statement):
                self.update(statement)
                with self.assertRaises(artifact.ArtifactError):
                    self.export()
                self.update('UPDATE catalog_entries SET duration=3600')
                self.update('DELETE FROM file_meta')

    def test_negative_attestation_unchanged(self):
        from fs42.chapter_analysis import encode_trusted_duration
        envelope = {'attestation_version': 2, 'method': 'ffprobe_show_chapters_v1',
                    'outcome': 'unusable_chapters', 'reason': 'final_endpoint_exceeds_cached_duration',
                    'trusted_duration': encode_trusted_duration(3600.0),
                    'media_identity': {'size': 1, 'mtime_ns': 1}}
        self.update("INSERT INTO file_meta(path,duration,size) VALUES('/media/a.mp4',3600,1)", sides=(0, 1))
        self.update("INSERT INTO chapter_points(path,points) VALUES('/media/a.mp4',?)", (json.dumps(envelope),), sides=(0, 1))
        artifact.validate_candidate(self.document())

    def test_retained_web_descriptor_not_exported(self):
        entry = {'path': ':autobump:=synthetic-private', 'duration': 3600, 'skip': 0,
                 'is_stream': False, 'content_type': 'feature', 'media_type': 'video'}
        self.update("INSERT INTO liquid_blocks(station,liquid_type,start_time,end_time,break_strategy,title,content_json,plan_json) VALUES('Action','LiquidWebBlock','2026-09-22 19:00:00','2026-09-22 20:00:00','end','private','null',?)",
                    (json.dumps([entry]),), sides=(0, 1))
        artifact.validate_candidate(self.document())
        self.assertNotIn('synthetic-private', json.dumps(self.document()))
        self.update('UPDATE liquid_blocks SET plan_json=? WHERE start_time=?',
                    (json.dumps([entry]), '2026-09-22 20:00:00'))
        with self.assertRaises(Exception):
            self.export()

    def test_retained_catalog_descriptor(self):
        from fs42.autobump_descriptor import AUTOBUMP_CATALOG_TAG
        self.update("UPDATE catalog_entries SET path=?,realpath=NULL,tag=?,content_type='feature' WHERE id=2",
                    (':autobump:=synthetic-private', AUTOBUMP_CATALOG_TAG), sides=(0, 1))
        artifact.validate_candidate(self.document())
        self.assertNotIn('synthetic-private', json.dumps(self.document()))

    def test_delayed_block_start_is_supported_not_exact_start(self):
        self.response['channels'][0]['regeneration_start'] = '2026-09-22 20:16:00'
        self.update("UPDATE liquid_blocks SET start_time='2026-09-22 20:16:00',end_time='2026-09-22 21:16:00'")
        doc = self.document()
        artifact.validate_candidate(doc)
        self.assertEqual(doc['schedule']['effect']['blocks'][0]['features'][0]['start'], '2026-09-22 20:16:00')

    def test_actual_feature_after_opening_commercial(self):
        plan = [{'path': '/media/ad.mp4', 'duration': 60, 'skip': 0, 'is_stream': False,
                 'content_type': 'commercial', 'media_type': 'video'},
                {'path': '/media/a.mp4', 'duration': 3540, 'skip': 0, 'is_stream': False,
                 'content_type': 'feature', 'media_type': 'video'}]
        self.update('UPDATE liquid_blocks SET plan_json=?,break_info=?',
                    (json.dumps(plan), json.dumps({'commercial_dir': '/media/ads'})))
        doc = self.document()
        artifact.validate_candidate(doc)
        self.assertEqual(doc['schedule']['effect']['blocks'][0]['features'][0]['start'], '2026-09-22 20:01:00')
        self.assertIn('/mnt/t7/CRT-Media/ads', doc['schedule']['rows'][0]['break_info'])

    def test_wrong_feature_or_tag_or_no_decision_is_not_proof(self):
        for statement in ("UPDATE liquid_blocks SET content_json='2'",
                          "UPDATE catalog_entries SET tag='wrong' WHERE id=1",
                          "UPDATE liquid_blocks SET start_time='2026-09-22 19:59:00'"):
            with self.subTest(statement=statement):
                self.update(statement)
                with self.assertRaises(artifact.ArtifactError):
                    self.export()
                self.update("UPDATE liquid_blocks SET content_json='1',start_time='2026-09-22 20:00:00'")
                self.update("UPDATE catalog_entries SET tag='synthetic-series' WHERE id=1")

    def test_every_decision_in_hour_must_match(self):
        self.update("UPDATE liquid_blocks SET end_time='2026-09-22 20:30:00',plan_json=replace(plan_json,'3600','1800')")
        self.update("INSERT INTO liquid_blocks(station,liquid_type,start_time,end_time,break_strategy,title,content_json,plan_json) SELECT station,liquid_type,'2026-09-22 20:30:00','2026-09-22 21:00:00',break_strategy,title,'2',replace(plan_json,'a.mp4','ad.mp4') FROM liquid_blocks")
        with self.assertRaises(artifact.ArtifactError):
            self.export()

    def test_tamper_schema_and_evidence(self):
        for change in (lambda d: d.update(extra=True),
                       lambda d: d['schedule']['effect'].update(hour=19),
                       lambda d: d['schedule']['rows'][0].update(content_json='999'),
                       lambda d: d['schedule']['rows'][0].update(plan_json='null'),
                       lambda d: d['schedule']['range'].update(replacement_end='2026-09-22 22:00:00')):
            doc = self.document()
            change(doc)
            with self.assertRaises(artifact.ArtifactError):
                artifact.validate_candidate(doc)

    def test_invalid_duration_and_confinement(self):
        for value in (True, None, -1, 1e308, float('inf')):
            plan = [{'path': '/media/a.mp4', 'duration': value, 'skip': 0, 'is_stream': False,
                     'content_type': 'feature', 'media_type': 'video'}]
            self.update('UPDATE liquid_blocks SET plan_json=?', (json.dumps(plan),))
            with self.assertRaises(Exception):
                self.export()

    def test_dst_ambiguity_rejected(self):
        for value in ('2026-11-01 01:30:00', '2026-03-08 02:30:00'):
            with self.assertRaises(artifact.ArtifactError):
                artifact._time(value)

    def test_secure_publication_and_tampering(self):
        doc = self.document()
        identity = artifact.publish_candidate(doc, self.root)
        target = self.root / 'runtime/director/candidates' / (identity + '.json')
        self.assertEqual(target.stat().st_mode & 0o777, 0o600)
        self.assertEqual(identity, hashlib.sha256(target.read_bytes()).hexdigest())
        with self.assertRaises(FileExistsError):
            artifact.publish_candidate(doc, self.root)
        self.assertFalse(list(target.parent.glob('.pending-*')))
        target.write_bytes(b'{}')
        with self.assertRaises(artifact.ArtifactError):
            artifact.inspect_candidate(identity, self.root)

    def test_symlink_and_hardlink_rejected(self):
        for hard in (False, True):
            with self.subTest(hard=hard):
                identity = artifact.publish_candidate(self.document(), self.root)
                target = self.root / 'runtime/director/candidates' / (identity + '.json')
                if hard:
                    os.link(target, self.root / 'link')
                else:
                    target.unlink()
                    target.symlink_to(self.databases[0])
                with self.assertRaises((OSError, artifact.ArtifactError)):
                    artifact.inspect_candidate(identity, self.root)
                target.unlink()

    def test_inspection_requires_bound_success_report(self):
        identity = artifact.publish_candidate(self.document(), self.root)
        with self.assertRaises(artifact.ArtifactError):
            artifact.inspect_candidate(identity, self.root)

    def test_bound_report_inspection_and_cli_redaction(self):
        from station_director import reporting
        from test.test_station_director_milestone_c3b1 import valid_success_result
        result = valid_success_result(self.root)
        result['comparison_id'] = RUN
        proposal = dict(PROPOSAL, schema_version=2, week_end='2026-09-29T20:00:00-07:00')
        report = reporting.build_validation_report(proposal, result, RUN, '2026-09-16T08:00:00Z')
        with patch.object(reporting, 'PROJECT_ROOT', self.root), \
                patch.object(reporting, 'VALIDATIONS_ROOT', self.root / 'runtime/director/validations'):
            publication = reporting.publish_validation_report(report)
        document = self.document()
        document['validation_report_digest'] = publication['validation_json_digest']
        document['proposal_digest'] = report['proposal']['digest']
        document['normalized_digest'] = report['reproducibility']['run_1_digest']
        identity = artifact.publish_candidate(document, self.root)
        summary = artifact.inspect_candidate(identity, self.root)
        self.assertEqual(summary['candidate_digest'], identity)
        out = io.StringIO()
        real_inspect = artifact.inspect_candidate
        with patch.object(artifact, 'inspect_candidate', side_effect=lambda value: real_inspect(value, self.root)), \
                patch.object(cli, 'load_policy', side_effect=AssertionError('must not read config')), \
                patch('sys.stdout', out):
            self.assertEqual(cli.main(['schedule', 'inspect-candidate', identity]), 0)
        for secret in ('synthetic-series', 'a.mp4', '/media', '/mnt'):
            self.assertNotIn(secret, out.getvalue())

    def test_failed_publication_leaves_no_candidate_or_pending(self):
        for failure in (OSError('private exception'), KeyboardInterrupt()):
            with patch('station_director.reporting._rename_noreplace', side_effect=failure), \
                    self.assertRaises(type(failure)):
                artifact.publish_candidate(self.document(), self.root)
            self.assertEqual(list((self.root / 'runtime/director/candidates').iterdir()), [])

    def test_size_limit_and_unsafe_directory(self):
        with patch.object(artifact, 'MAX_BYTES', 100), self.assertRaises(artifact.ArtifactError):
            artifact.publish_candidate(self.document(), self.root)
        (self.root / 'runtime').symlink_to(self.stage, target_is_directory=True)
        with self.assertRaises(Exception):
            artifact.publish_candidate(self.document(), self.root)

    def test_only_prepare_routes_to_exporter(self):
        from station_director.validation_coordinator import disabled_outcome
        from station_director import validation_control
        for command in ('prepare', 'validate'):
            with patch.object(validation_control, 'SCHEDULE_VALIDATION_ENABLED', True), \
                    patch('station_director.validation_coordinator.validate_saved_proposal', return_value=disabled_outcome()) as validate, \
                    patch('sys.stdout', io.StringIO()):
                self.assertEqual(cli.main(['schedule', command, PROPOSAL['proposal_id']]), 1)
            self.assertEqual('candidate' in validate.call_args.kwargs, command == 'prepare')

    def test_dual_export_occurs_twice_before_cleanup(self):
        from test import test_station_director_milestone_c2 as c2
        fixture = c2.DualRunLifecycleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        original = c2.run_dual_comparison
        calls = []
        def run(*args, **kwargs):
            def export(lifecycle, accepted, captured):
                self.assertEqual(accepted['status'], 'success')
                calls.append(lifecycle.run_id)
            result = original(*args, **kwargs, candidate_exporter=export)
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(item['passed'] for item in result['cleanup']))
            return result
        with patch.object(c2, 'run_dual_comparison', side_effect=run):
            fixture.test_sequential_independent_success_and_reverse_cleanup()

    def test_export_failure_cleans_stages_and_redacts_exception(self):
        from test import test_station_director_milestone_c2 as c2
        fixture = c2.DualRunLifecycleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        events = []
        scope = fixture._scope(self.root, events)
        with patch('station_director.dual_run._prepare_scope', return_value=scope), \
                patch('station_director.dual_run.launch_single_run'), \
                patch('station_director.dual_run.inspect_single_run', return_value=response('a')), \
                patch('station_director.dual_run._assert_inputs_stable', side_effect=lambda a,b,c,k:
                      {'checkpoint': k, 'passed': True, 'changed_categories': []}), \
                patch('station_director.dual_run.normalize_completed_run', return_value=SimpleNamespace()), \
                patch.object(artifact, 'publish_candidate') as publisher:
            def reject(*unused):
                raise RuntimeError('private-media-name /private/path')
            result = c2.run_dual_comparison(self.root, self.root, self.root,
                                           {'week_start': '2026-09-14T00:00:00-07:00'}, {},
                                           'comparison', candidate_exporter=reject)
        self.assertEqual(result['status'], 'failed')
        self.assertIn('cleanup', events)
        self.assertEqual(result['failure']['code'], 'normalization_failed')
        self.assertNotIn('private-media-name', json.dumps(result))
        publisher.assert_not_called()

    def test_no_apply_or_recovery_commands(self):
        parser = cli.build_parser()
        for command in ('apply', 'recover', 'rollback'):
            with patch('sys.stderr', io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(['schedule', command])

    def test_preparation_publication_gates(self):
        candidate = artifact.CandidatePreparation(self.root)
        with patch.object(artifact, 'code_revision', return_value='a' * 40):
            candidate.begin(PROPOSAL, POLICY, RUN)
            life = SimpleNamespace(stage=self.stage, request={'input_fingerprints': INPUTS,
                                                             'validation_context': {}})
            captured = SimpleNamespace(media_manifest=SimpleNamespace(summary={'digest': 'a' * 64}))
            candidate.capture(life, self.response, captured)
            candidate.capture(life, self.response, captured)
            result = {'status': 'success', 'reproducibility': {'passed': True, 'run_1_digest': 'c' * 64},
                      'cleanup': [{'passed': True, 'quarantined': False}] * 2,
                      'source_checks': [{'checkpoint': k, 'passed': True} for k in
                                        ('after_capture', 'between_runs', 'after_run_2', 'before_success')]}
            publication = {'publication_state': 'published_durable', 'validation_json_digest': 'b' * 64}
            for field in ('cleanup', 'source_checks', 'status'):
                bad = copy.deepcopy(result)
                bad[field] = 'failed' if field == 'status' else []
                with self.assertRaises(artifact.ArtifactError):
                    candidate.publish(bad, publication)
                self.assertFalse((self.root / 'runtime/director/candidates').exists())
            for change in (lambda r: r['cleanup'][0].update(passed=False),
                           lambda r: r['cleanup'][0].update(quarantined=True),
                           lambda r: r['source_checks'][-1].update(passed=False),
                           lambda r: r['reproducibility'].update(passed=False)):
                bad = copy.deepcopy(result)
                change(bad)
                with self.assertRaises(artifact.ArtifactError):
                    candidate.publish(bad, publication)
            with self.assertRaises(artifact.ArtifactError):
                candidate.publish(result, dict(publication, publication_state='published_not_durable'))
            with patch.object(artifact, 'code_revision', return_value='b' * 40), self.assertRaises(artifact.ArtifactError):
                candidate.publish(result, publication)
            candidate.exports[1]['schedule']['rows'][0]['title'] = 'different'
            with self.assertRaises(artifact.ArtifactError):
                candidate.publish(result, publication)
            candidate.exports[1] = copy.deepcopy(candidate.exports[0])
            candidate.publish(result, publication)
            self.assertIsNotNone(candidate.summary)
            self.assertEqual(candidate.exports, [])

    def test_coordinator_never_publishes_candidate_on_failed_finalization(self):
        from test.test_station_director_milestone_c3b1 import CoordinatorFlowTests, valid_success_result, VALID_PROPOSAL, RUN_ID
        from unittest.mock import Mock
        fixture = CoordinatorFlowTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result = valid_success_result(fixture.root)
        result['comparison_id'] = RUN_ID
        candidate = Mock()
        with patch('station_director.dual_run.run_dual_comparison', return_value=result), \
                patch.object(fixture.reporting, 'create_validation_run_id', return_value=RUN_ID), \
                patch.object(fixture.coordinator, '_finalization_budget_exhausted', return_value=True):
            outcome = fixture.coordinator.validate_saved_proposal(VALID_PROPOSAL['proposal_id'], candidate=candidate)
        self.assertNotEqual(outcome.state, 'passed')
        candidate.publish.assert_not_called()

    def test_coordinator_publishes_after_report_and_retains_validate_default(self):
        from test.test_station_director_milestone_c3b1 import CoordinatorFlowTests, valid_success_result, VALID_PROPOSAL, RUN_ID
        from unittest.mock import Mock
        fixture = CoordinatorFlowTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result = valid_success_result(fixture.root)
        result['comparison_id'] = RUN_ID
        candidate = Mock()
        def publish(accepted, publication):
            self.assertEqual(publication['publication_state'], 'published_durable')
            path = fixture.root / 'runtime/director/validations' / VALID_PROPOSAL['proposal_id'] / RUN_ID / 'validation.json'
            self.assertTrue(path.is_file())
        candidate.publish.side_effect = publish
        with patch('station_director.dual_run.run_dual_comparison', return_value=result) as dual, \
                patch.object(fixture.reporting, 'create_validation_run_id', return_value=RUN_ID):
            outcome = fixture.coordinator.validate_saved_proposal(VALID_PROPOSAL['proposal_id'], candidate=candidate)
        self.assertEqual(outcome.state, 'passed')
        self.assertEqual(dual.call_args.kwargs['candidate_exporter'], candidate.capture)
        candidate.publish.assert_called_once()


# End-to-end regression: production projection, reconciliation and fingerprints.
import copy
import json
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace

from station_director.schedule_artifact import CandidatePreparation, ArtifactError
from station_director.schedule_normalization import normalize_completed_run
from station_director.preservation import (capture_media_manifest, fingerprint_database,
    fingerprint_json_files, protected_json_paths)
from station_director.validation_context import (canonical_seed_inputs, derive_validation_context,
    logical_media_manifest_fingerprint, logical_protected_configuration_fingerprint)
from station_director.single_run_protocol import (bind_request, validate_document,
    validate_response_semantics, REQUEST_SCHEMA, RESPONSE_SCHEMA_V4)
from station_director.worker_bootstrap import finalize_work_tree
from station_director.staged_schedule import (capture_catalog_rows,
    capture_catalog_media_metadata, reconcile_catalog)
from test.test_station_director_milestone_c1 import request_for
from test.test_station_director_milestone_c2 import insert_catalog, response, write_test_guide
from test.test_station_director_milestone_b2 import create_database, insert_block

ROOT = Path('/home/chaseanderegg/FieldStation42')


def production_alias_fixture(root, revision_root):
    stage = root / 'stage'; stage.mkdir()
    media = root / 'media'; media.mkdir()
    request = request_for(stage, media)
    (media / 'ad.mp4').write_bytes(b'synthetic-commercial')
    source = stage / 'source'
    database = source / 'runtime/fs42_fluid.db'
    con = create_database(database)
    try:
        insert_catalog(con, 1, '/mnt/t7/CRT-Media/show.mp4', tag='Synthetic')
        insert_catalog(con, 2, 'catalog/crt_media/ad.mp4', realpath='/mnt/t7/CRT-Media/ad.mp4',
                       tag='commercial', content_type='commercial')
        for name in ('show.mp4', 'ad.mp4'):
            info = (media / name).stat()
            con.execute('INSERT INTO file_meta(path,duration,size,last_mod,meta) VALUES(?,?,?,?,?)',
                        ('/mnt/t7/CRT-Media/' + name, 3600.0, info.st_size, info.st_mtime, '{}'))
        insert_block(con, 'Action', '2026-09-14 05:00:00', '2026-09-14 06:00:00',
                     1, '/mnt/t7/CRT-Media/show.mp4')
        con.commit()
    finally:
        con.close()
    proposal = request['proposal']
    proposal['directives'] = [{'type':'date_slot', 'channel':2, 'date':'2026-09-14',
                               'hour':6, 'series':'Synthetic'}]
    policy = request['policy']
    manifest = capture_media_manifest(media, spool_directory=stage)
    try:
        configuration = logical_protected_configuration_fingerprint(protected_json_paths(source))['digest']
        database_digest = fingerprint_database(database)['logical']['digest']
        logical_media = logical_media_manifest_fingerprint(manifest)['digest']
        physical = fingerprint_json_files(protected_json_paths(source))['digest']
        request['seed_inputs'] = canonical_seed_inputs(configuration, database_digest, logical_media)
        request['validation_context'] = derive_validation_context(proposal, policy, request['seed_inputs'])
        request['input_fingerprints'] = {
            'original_logical_configuration_fingerprint': configuration,
            'original_logical_database_fingerprint': database_digest,
            'logical_media_manifest_fingerprint': logical_media,
            'live_physical_configuration_fingerprint': physical,
            'staged_source_physical_configuration_fingerprint': physical}
        request = bind_request(request)
        validate_document(request, REQUEST_SCHEMA)
        finalized, held = finalize_work_tree(request, stage, media, ROOT)
        held.close()
        work = stage / 'work/runtime/fs42_fluid.db'
        con = sqlite3.connect(work)
        try:
            columns, originals = capture_catalog_rows(con, 'Action')
            metadata = capture_catalog_media_metadata(con, originals, columns)
            generated = [dict(zip(columns, row)) for row in originals]
            from station_director.path_safety import canonical_media_mapping
            for row in generated:
                path = canonical_media_mapping(row['realpath'] or row['path']).sandbox_path
                row['path'] = row['realpath'] = path
            generated_metadata = capture_catalog_media_metadata(
                con, [tuple(row[c] for c in columns) for row in generated], columns)
            # Production reconciliation itself creates the /media alias for a
            # protected host-path row; unchanged metadata stays host-keyed.
            active = reconcile_catalog(con, 'Action', generated, {1},
                original_rows=originals, original_media_metadata=metadata,
                generated_media_metadata=generated_metadata)
            feature = con.execute("SELECT id FROM catalog_entries WHERE path='/media/show.mp4'").fetchone()[0]
            from datetime import datetime, timedelta
            for hour in range(168):
                start = datetime(2026, 9, 14, 6) + timedelta(hours=hour)
                insert_block(con, 'Action', start.isoformat(' '),
                             (start + timedelta(hours=1)).isoformat(' '), feature, '/media/show.mp4')
            plan = [{'path':'/media/ad.mp4', 'duration':60, 'skip':0, 'is_stream':False,
                     'content_type':'commercial', 'media_type':'video'},
                    {'path':'/media/show.mp4', 'duration':3540, 'skip':0, 'is_stream':False,
                     'content_type':'feature', 'media_type':'video'}]
            con.execute('UPDATE liquid_blocks SET plan_json=? WHERE start_time=?',
                        (json.dumps(plan), '2026-09-14 06:00:00'))
            con.commit()
        finally:
            con.close()
        accepted = response('a'); accepted['schema_version'] = 4
        accepted['run_id'] = request['run_id']; accepted['proposal_id'] = proposal['proposal_id']
        accepted['validation_context'] = {k:request['validation_context'][k]
                                         for k in ('input_fingerprint','requested_seed','effective_seed')}
        accepted['verification']['fingerprints'].update(request['input_fingerprints'])
        accepted['verification']['fingerprints'].update({k:finalized[k] for k in
            ('projected_configuration_fingerprint','working_database_fingerprint')})
        accepted['channels'][0].update(effective_horizon='2026-09-21 06:00:00',
                                     regeneration_start='2026-09-14 06:00:00',
                                     retained_blocks=1, generated_blocks=168, final_blocks=169)
        validate_document(accepted, RESPONSE_SCHEMA_V4)
        validate_response_semantics(accepted)
        write_test_guide(stage)
        normalize_completed_run(stage, database, accepted, '2026-09-14 06:00:00')
        normalized = True
        preparation = CandidatePreparation(revision_root)
        preparation.begin(proposal, policy, 'v-20260916T000000000000Z-' + 'a'*32)
        life = SimpleNamespace(stage=stage, request=request)
        # Same shared-capture fields consumed by production; manifest and all
        # fingerprints were computed by the real helpers above.
        from station_director.dual_run import SharedCapture, _space_requirement
        capture = SharedCapture(
            configuration, database_digest, logical_media,
            fingerprint_json_files(protected_json_paths(source)),
            fingerprint_database(database), manifest, logical_media_manifest_fingerprint(manifest),
            _space_requirement(database, 0), stage)
        preparation.capture(life, accepted, capture)
        return preparation, stage, normalized, accepted
    finally:
        manifest.close()


class ProductionAliasExportTests(unittest.TestCase):
    def test_protected_host_catalog_and_generated_alias_export(self):
        import subprocess
        with tempfile.TemporaryDirectory(prefix='fs42-alias-regression-', dir='/tmp') as temp:
            root = Path(temp)
            revision = root / 'revision'
            # A clean local clone supplies real revision checks without committing
            # or mocking code/input fingerprint helpers. No remote is contacted.
            subprocess.run(['git', 'clone', '--quiet', '--shared', str(ROOT), str(revision)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            preparation, stage, normalized, accepted = production_alias_fixture(root, revision)
            self.assertTrue(normalized)
            schedule = preparation.exports[0]['schedule']
            self.assertEqual(schedule['rows'][0]['content_json'], '1')
            aliases = [m for m in schedule['catalog_mapping'] if m['live_id'] == 1]
            self.assertEqual(len(aliases), 2)
            self.assertEqual(aliases[0]['semantics'], aliases[1]['semantics'])
            self.assertIsNotNone(aliases[0]['semantics']['media_records']['file_meta'])
            self.assertEqual(schedule['effect']['blocks'][0]['features'][0]['start'],
                             '2026-09-14 06:01:00')
            document = {'schema_version': 1, 'proposal_id': preparation.proposal['proposal_id'],
                        'proposal_digest': artifact.digest(preparation.proposal),
                        'policy_digest': artifact.digest(preparation.policy),
                        'code_revision': preparation.revision, 'validation_run': preparation.run_id,
                        'validation_report_digest': 'a' * 64, 'normalized_digest': 'b' * 64,
                        'directive': preparation.proposal['directives'][0], **preparation.exports[0]}
            artifact.validate_candidate(document)
            duplicate = copy.deepcopy(document)
            duplicate['schedule']['catalog_mapping'].append(copy.deepcopy(aliases[-1]))
            with self.assertRaises(artifact.ArtifactError):
                artifact.validate_candidate(duplicate)
            altered = copy.deepcopy(document)
            pair = [m for m in altered['schedule']['catalog_mapping'] if m['live_id'] == 1]
            pair[-1]['semantics']['row']['duration'] += 1
            with self.assertRaises(artifact.ArtifactError):
                artifact.validate_candidate(altered)
            from contextlib import closing
            with closing(sqlite3.connect(stage / 'work/runtime/fs42_fluid.db')) as writer:
                writer.execute('UPDATE file_meta SET size=size+1')
                writer.commit()
            try:
                with self.assertRaisesRegex(artifact.ArtifactError, 'candidate_metadata_changed'):
                    artifact.export_candidate(stage, accepted, preparation.proposal, preparation.policy)
            finally:
                writer = sqlite3.connect(stage / 'work/runtime/fs42_fluid.db')
                try:
                    writer.execute('UPDATE file_meta SET size=size-1')
                    writer.commit()
                finally:
                    writer.close()
            with closing(sqlite3.connect(stage / 'work/runtime/fs42_fluid.db')) as writer:
                writer.execute("INSERT INTO file_meta(path,duration,size) VALUES('/media/show.mp4',3600,9)")
                writer.commit()
                with self.assertRaisesRegex(artifact.ArtifactError, 'candidate_metadata_ambiguous'):
                    artifact.export_candidate(stage, accepted, preparation.proposal, preparation.policy)
                writer.execute("DELETE FROM file_meta WHERE path='/media/show.mp4'")
                writer.execute("UPDATE catalog_entries SET duration=duration+1 WHERE path='/media/show.mp4'")
                writer.commit()
                with self.assertRaisesRegex(artifact.ArtifactError, 'candidate_catalog_changed'):
                    artifact.export_candidate(stage, accepted, preparation.proposal, preparation.policy)


if __name__ == '__main__':
    unittest.main()
