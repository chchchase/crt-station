"""Private, immutable schedule candidates. This module has no live writer."""

import copy
import hashlib
import json
import math
import os
import re
import secrets
import stat
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from station_director.path_safety import canonical_media_mapping
from station_director.preservation import readonly_database
from station_director.schedule_normalization import (
    _block_playback, _canonical, _catalog_descriptor, _columns, _parse_json,
    _quote, _metadata_for_path, canonical_catalog_semantics,
)
from station_director.single_run_protocol import strict_json_loads, validate_document

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = Path(__file__).with_name('schemas') / 'schedule-artifact.v1.schema.json'
MAX_BYTES = 16 * 1024 * 1024
MAX_ROWS = 10000
MAX_CATALOG = 50000
BLOCK_COLUMNS = ('id', 'station', 'liquid_type', 'start_time', 'end_time',
                 'break_strategy', 'title', 'sequence_key', 'break_info',
                 'content_json', 'plan_json')
HEX = re.compile(r'[a-f0-9]{64}\Z')


class ArtifactError(RuntimeError):
    def __init__(self, code='candidate_invalid'):
        super().__init__(code)
        self.code = code


def digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _file_identity(info):
    # Reading may update atime. Identity checks must not treat that as mutation.
    return (info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def code_revision(root):
    def git(*args):
        result = subprocess.run(['git', '-C', str(root), *args], check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                timeout=10)
        if len(result.stdout) > MAX_BYTES:
            raise ArtifactError('candidate_code_invalid')
        return result.stdout.decode('utf-8').strip()
    try:
        revision = git('rev-parse', 'HEAD')
        if (not re.fullmatch('[a-f0-9]{40}', revision)
                or git('status', '--porcelain', '--untracked-files=no')
                or git('ls-files', '--others', '--exclude-standard',
                       'station_director/*.py', 'fs42/*.py')):
            raise ArtifactError('candidate_code_invalid')
        return revision
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise ArtifactError('candidate_code_invalid') from None


def _time(value):
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is not None or dt.isoformat(' ') != value:
            raise ValueError()
        zone = ZoneInfo('America/Los_Angeles')
        first, second = dt.replace(tzinfo=zone, fold=0), dt.replace(tzinfo=zone, fold=1)
        if first.utcoffset() != second.utcoffset():
            raise ValueError()  # ambiguous and nonexistent civil times fail closed
        return dt
    except (TypeError, ValueError):
        raise ArtifactError('candidate_timing_unsupported') from None


def _rows(connection, table, limit=MAX_CATALOG):
    columns = _columns(connection, table)
    rows = []
    size = 0
    for row in connection.execute('SELECT * FROM ' + _quote(table)):
        encoded_size = len(_canonical(list(row)))
        size += encoded_size
        if len(rows) >= limit or encoded_size > 2 * 1024 * 1024 or size > MAX_BYTES:
            raise ArtifactError('candidate_limit')
        rows.append(dict(zip(columns, row)))
    return columns, rows


def _catalog(connection):
    columns, rows = _rows(connection, 'catalog_entries')
    metadata_paths = {}
    for table in ('file_meta', 'break_points', 'chapter_points'):
        unused, metadata_rows = _rows(connection, table)
        for metadata in metadata_rows:
            path = metadata['path']
            identity = canonical_media_mapping(path, allow_sandbox=True).logical_identity
            if identity in metadata_paths and metadata_paths[identity] != path:
                raise ArtifactError('candidate_metadata_ambiguous')
            metadata_paths[identity] = path
    result = {}
    for row in rows:
        semantics = canonical_catalog_semantics(connection, columns, tuple(row[c] for c in columns))
        if not _catalog_descriptor(row):
            identity = canonical_media_mapping(row.get('realpath') or row['path'], allow_sandbox=True).logical_identity
            if identity in metadata_paths:
                semantics['media_records'] = _metadata_for_path(connection, metadata_paths[identity])
        result[row['id']] = (row, semantics)
    return result


def _semantic_descriptor(row):
    # canonical_catalog_semantics has already validated opaque descriptors and
    # replaced their text with a domain-separated digest, not a filesystem path.
    return any(isinstance(row.get(key), str) and row[key].startswith('autobump-sha256:')
               for key in ('path', 'realpath'))


def _metadata(connection):
    result = {}
    for table in ('file_meta', 'break_points', 'chapter_points'):
        columns, rows = _rows(connection, table)
        encoded = []
        for row in rows:
            row['path'] = canonical_media_mapping(row['path'], allow_sandbox=True).logical_identity
            encoded.append(_canonical(row).hex())
        result[table] = digest([columns, sorted(encoded)])
    return result


def _auxiliary(value, *, logical=False):
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            if key in ('path', 'realpath', 'bump_dir', 'commercial_dir', 'start_bump', 'end_bump') and isinstance(child, str):
                mapped = canonical_media_mapping(child, allow_sandbox=True)
                result[key] = mapped.logical_identity if logical else mapped.canonical_host_path
            else:
                result[key] = _auxiliary(child, logical=logical)
        return result
    if isinstance(value, list):
        return [_auxiliary(child, logical=logical) for child in value]
    if isinstance(value, str) and ('/media/' in value or '/stage/' in value):
        raise ArtifactError('candidate_reference_unsupported')
    return value


def _live_plan(row):
    refs, normalized, unused = _block_playback(row, allow_descriptors=False)
    plan = _parse_json(row['plan_json'], 'plan')
    for entry in plan:
        if entry['is_stream']:
            raise ArtifactError('candidate_reference_invalid')
        mapping = canonical_media_mapping(entry['path'], allow_sandbox=True)
        entry['path'] = mapping.canonical_host_path
        for key in ('duration', 'skip'):
            value = entry[key]
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or value < 0 or value > 86400 or key == 'duration' and value == 0):
                raise ArtifactError('candidate_timing_unsupported')
    result = dict(row, plan_json=_json(plan).decode())
    if _canonical(_block_playback(result, allow_descriptors=False)[1]) != _canonical(normalized):
        raise ArtifactError('candidate_translation_failed')
    for key in ('break_info', 'sequence_key'):
        if row[key] not in (None, ''):
            original = _parse_json(row[key], key)
            translated = _auxiliary(original)
            if _canonical(_auxiliary(original, logical=True)) != _canonical(_auxiliary(translated, logical=True)):
                raise ArtifactError('candidate_translation_failed')
            result[key] = _json(translated).decode()
    return result, refs


def _schedule_semantics(rows):
    result = []
    for row in rows:
        value = {k: v for k, v in row.items() if k != 'id'}
        unused, value['plan_json'], unused_descriptor = _block_playback(row, allow_descriptors=True)
        value['content_json'] = _parse_json(row['content_json'], 'content')
        for key in ('break_info', 'sequence_key'):
            if row[key] not in (None, ''):
                value[key] = _auxiliary(_parse_json(row[key], key), logical=True)
        result.append(value)
    return _canonical(result)


def effect_evidence(rows, mapping, directive):
    """Prove every scheduler decision in the requested hour, not mere overlap."""
    start = _time(f"{directive['date']} {directive['hour']:02d}:00:00")
    end = start + timedelta(hours=1)
    selected = [r for r in rows if start <= _time(r['start_time']) < end]
    if not selected:
        raise ArtifactError('candidate_effect_unproven')
    evidence = []
    for row in selected:
        if row['liquid_type'] != 'LiquidBlock':
            raise ArtifactError('candidate_effect_unsupported')
        refs, unused, unused_descriptor = _block_playback(row, allow_descriptors=False)
        if len(refs) != 1 or refs[0] not in mapping:
            raise ArtifactError('candidate_effect_unproven')
        catalog = mapping[refs[0]]['semantics']['row']
        if catalog['tag'] != directive['series'] or catalog['station'] != row['station']:
            raise ArtifactError('candidate_effect_unproven')
        identity = catalog.get('realpath') or catalog['path']
        mark = _time(row['start_time'])
        features = []
        for entry in _parse_json(row['plan_json'], 'plan'):
            duration, skip = entry['duration'], entry['skip']
            if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0
                   for v in (duration, skip)) or duration <= 0 or duration > 86400:
                raise ArtifactError('candidate_timing_unsupported')
            finish = mark + timedelta(seconds=duration)
            if entry['content_type'] == 'feature':
                if canonical_media_mapping(entry['path']).logical_identity != identity:
                    raise ArtifactError('candidate_effect_unproven')
                features.append({'start': mark.isoformat(' '), 'end': finish.isoformat(' ')})
            mark = finish
        if not features or mark > _time(row['end_time']):
            raise ArtifactError('candidate_timing_unsupported')
        evidence.append({'block_id': row['id'], 'block_start': row['start_time'],
                         'block_end': row['end_time'], 'features': features})
    return {'rule': 'all_block_starts_in_requested_hour', 'channel': directive['channel'],
            'date': directive['date'], 'hour': directive['hour'],
            'series_digest': digest(directive['series']), 'blocks': evidence}


def export_candidate(stage, response, proposal, policy):
    directives = proposal['directives']
    if (proposal['assignment_changes'] or proposal['exclusions'] or len(directives) != 1
            or directives[0]['type'] != 'date_slot' or len(response['channels']) != 1):
        raise ArtifactError('candidate_scope_unsupported')
    directive = directives[0]
    channel = next(c['name'] for c in policy['channels'] if c['number'] == directive['channel'])
    history = response['channels'][0]
    if history['name'] != channel or history['number'] != directive['channel']:
        raise ArtifactError('candidate_scope_unsupported')
    with readonly_database(Path(stage) / 'source/runtime/fs42_fluid.db') as baseline, \
            readonly_database(Path(stage) / 'work/runtime/fs42_fluid.db') as proposed:
        old, new = _catalog(baseline), _catalog(proposed)
        if _metadata(baseline) != _metadata(proposed):
            raise ArtifactError('candidate_metadata_changed')
        # Require unambiguous complete existing semantics, including metadata.
        # The narrowly checked historical/staging alias pair below is the only
        # permitted exception to one-to-one physical catalog IDs.
        by_semantic = {}
        for ident, (row, semantics) in old.items():
            by_semantic.setdefault(_canonical(semantics), []).append(ident)
        mapping = {}
        used = set()
        for ident, (row, semantics) in new.items():
            matches = by_semantic.get(_canonical(semantics), [])
            target = ident if ident in matches else matches[0] if len(matches) == 1 else None
            if target is None:
                raise ArtifactError('candidate_catalog_changed')
            used.add(target)
            mapping[ident] = {'validated_id': ident, 'live_id': target, 'semantics': semantics}
        if used != set(old):
            raise ArtifactError('candidate_catalog_changed')
        # Reconciliation preserves exact historical host rows and may add one
        # /media alias for generated playback. Only that proven pair may share
        # a live ID; do not admit arbitrary duplicate/ambiguous catalog changes.
        boundary = datetime.fromisoformat(proposal['week_start']).replace(tzinfo=None).isoformat(' ')
        unused_columns, baseline_rows = _rows(baseline, 'liquid_blocks', MAX_CATALOG)
        protected = set()
        for retained in baseline_rows:
            if retained['station'] == channel and retained['start_time'] < boundary:
                refs, unused_plan, unused_descriptor = _block_playback(retained, allow_descriptors=True)
                protected.update(refs)
        groups = {}
        allocation_floor = max(old, default=0)
        for ident, entry in mapping.items():
            groups.setdefault(entry['live_id'], []).append(ident)
        for target, ids in groups.items():
            if len(ids) == 1:
                continue
            aliases = [ident for ident in ids if ident != target]
            if (len(ids) != 2 or target not in ids or target not in protected
                    or _canonical(new[target][0]) != _canonical(old[target][0])
                    or len(aliases) != 1 or aliases[0] <= allocation_floor
                    or _catalog_descriptor(new[aliases[0]][0])):
                raise ArtifactError('candidate_catalog_changed')
            alias = new[aliases[0]][0]
            expected = canonical_media_mapping(old[target][0].get('realpath') or old[target][0]['path'], allow_sandbox=True).sandbox_path
            if (alias['path'] != expected or alias['realpath'] != expected
                    or (old[target][0].get('realpath') or old[target][0]['path']).startswith('/media/')):
                raise ArtifactError('candidate_catalog_changed')
        columns, all_rows = _rows(proposed, 'liquid_blocks', MAX_CATALOG)
        if tuple(columns) != BLOCK_COLUMNS:
            raise ArtifactError('candidate_schema_unsupported')
        boundary = datetime.fromisoformat(proposal['week_start']).replace(tzinfo=None).isoformat(' ')
        seam, horizon = history['regeneration_start'], history['effective_horizon']
        rows = sorted((r for r in all_rows if r['station'] == channel and r['start_time'] >= boundary),
                      key=lambda r: (r['start_time'], r['id']))
        if not rows or len(rows) > MAX_ROWS:
            raise ArtifactError('candidate_limit')
        mark = _time(seam)
        translated, referenced = [], set()
        for row in rows:
            if _time(row['start_time']) != mark or mark >= _time(horizon):
                raise ArtifactError('candidate_range_invalid')
            mark = _time(row['end_time'])
            if mark <= _time(row['start_time']):
                raise ArtifactError('candidate_range_invalid')
            live, refs = _live_plan(row)
            for ref in refs:
                if ref not in mapping or _catalog_descriptor(new[ref][0]):
                    raise ArtifactError('candidate_reference_invalid')
                referenced.add(ref)
            content = _parse_json(row['content_json'], 'content')
            live['content_json'] = _json([mapping[r]['live_id'] for r in refs]
                                        if isinstance(content, list) else mapping[refs[0]]['live_id']).decode()
            translated.append(live)
        if mark < _time(horizon):
            raise ArtifactError('candidate_range_invalid')
        # Include plan-only commercial references in the semantic proof.
        paths = {canonical_media_mapping(e['path']).logical_identity
                 for r in translated for e in _parse_json(r['plan_json'], 'plan')}
        known_paths = {(sem['row'].get('realpath') or sem['row']['path'])
                       for unused, sem in old.values() if not _semantic_descriptor(sem['row'])}
        if not paths <= known_paths:
            raise ArtifactError('candidate_reference_invalid')
        live_mapping = {v['live_id']: v for v in mapping.values()}
        evidence = effect_evidence(translated, live_mapping, directive)
        earlier = sorted((r for r in baseline_rows if r['station'] == channel and r['start_time'] >= boundary),
                         key=lambda r: (r['start_time'], r['id']))
        if _schedule_semantics(earlier) == _schedule_semantics(translated):
            raise ArtifactError('candidate_no_change')
        return {'range': {'channel': directive['channel'], 'station': channel,
                          'proposal_boundary': boundary, 'regeneration_start': seam,
                          'effective_horizon': horizon, 'replacement_end': mark.isoformat(' ')},
                'rows': translated, 'catalog_mapping': sorted(mapping.values(), key=lambda v: v['validated_id']),
                'metadata_digests': _metadata(baseline), 'effect': evidence}


def validate_candidate(document):
    try:
        validate_document(document, SCHEMA)
        if len(_json(document)) > MAX_BYTES:
            raise ArtifactError('candidate_limit')
        payload = document['schedule']
        rows = payload['rows']
        mapping = {m['live_id']: m for m in payload['catalog_mapping']}
        if (len({m['validated_id'] for m in payload['catalog_mapping']}) != len(payload['catalog_mapping'])
                or len({r['id'] for r in rows}) != len(rows)):
            raise ArtifactError()
        groups = {}
        for member in payload['catalog_mapping']:
            groups.setdefault(member['live_id'], []).append(member)
        allocation_floor = max(mapping, default=0)
        for live_id, members in groups.items():
            if len(members) > 1 and (len(members) != 2
                    or sum(m['validated_id'] == live_id for m in members) != 1
                    or any(m['validated_id'] != live_id and m['validated_id'] <= allocation_floor for m in members)
                    or _canonical(members[0]['semantics']) != _canonical(members[1]['semantics'])):
                raise ArtifactError()
        span = payload['range']
        mark = _time(span['regeneration_start'])
        if _time(span['proposal_boundary']) > mark or mark >= _time(span['effective_horizon']):
            raise ArtifactError()
        known_paths = {(m['semantics']['row'].get('realpath') or m['semantics']['row']['path'])
                       for m in mapping.values() if not _semantic_descriptor(m['semantics']['row'])}
        for row in rows:
            if row['station'] != span['station'] or _time(row['start_time']) != mark:
                raise ArtifactError()
            mark = _time(row['end_time'])
            if mark <= _time(row['start_time']) or _time(row['start_time']) >= _time(span['effective_horizon']):
                raise ArtifactError()
            unused, refs = _live_plan(row)
            if any(ref not in mapping or _semantic_descriptor(mapping[ref]['semantics']['row']) for ref in refs):
                raise ArtifactError()
            for entry in _parse_json(row['plan_json'], 'plan'):
                if canonical_media_mapping(entry['path']).logical_identity not in known_paths:
                    raise ArtifactError()
            for key in ('break_info', 'sequence_key'):
                if row[key] not in (None, '') and _parse_json(row[key], key) != _parse_json(unused[key], key):
                    raise ArtifactError()
        if mark != _time(span['replacement_end']) or mark < _time(span['effective_horizon']):
            raise ArtifactError()
        directive = document['directive']
        if (directive['channel'] != span['channel']
                or _canonical(payload['effect']) != _canonical(effect_evidence(rows, mapping, directive))):
            raise ArtifactError()
    except ArtifactError:
        raise
    except Exception:
        raise ArtifactError() from None


@contextmanager
def _artifact_root(root, *, create, components=None):
    from station_director.reporting import _open_component, _validate_directory_info
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        _validate_directory_info(os.fstat(descriptor), 'project', private=False)
        for name, private in (components or (('runtime', False), ('director', True), ('candidates', True))):
            child = _open_component(descriptor, name, create=create, private=private)
            if create:
                os.fsync(descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def publish_candidate(document, root=ROOT):
    validate_candidate(document)
    raw = _json(document)
    identity = hashlib.sha256(raw).hexdigest()
    name = identity + '.json'
    temporary = '.pending-' + secrets.token_hex(16)
    from station_director.reporting import _rename_noreplace
    with _artifact_root(root, create=True) as parent:
        from station_director.reporting import _bounded_entries
        if sum(1 for unused in _bounded_entries(parent)) >= 100:
            raise ArtifactError('candidate_capacity')
        fd = None
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                fd = None
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            _rename_noreplace(parent, temporary, parent, name)
            os.fsync(parent)
        finally:
            if fd is not None:
                os.close(fd)
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass
    return identity


def inspect_candidate(identity, root=ROOT):
    if not isinstance(identity, str) or not HEX.fullmatch(identity):
        raise ArtifactError()
    with _artifact_root(root, create=False) as parent:
        fd = os.open(identity + '.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_size > MAX_BYTES):
                raise ArtifactError()
            raw = stream.read(MAX_BYTES + 1)
            after = os.fstat(stream.fileno())
            named = os.stat(identity + '.json', dir_fd=parent, follow_symlinks=False)
            if (_file_identity(after) != _file_identity(before)
                    or _file_identity(named) != _file_identity(before)
                    or hashlib.sha256(raw).hexdigest() != identity):
                raise ArtifactError()
    try:
        document = strict_json_loads(raw)
        validate_candidate(document)
        from station_director.reporting import _read_private_file, validate_report_document, MAX_REPORT_JSON_BYTES
        components = (('runtime', False), ('director', True), ('validations', True),
                      (document['proposal_id'], True), (document['validation_run'], True))
        with _artifact_root(root, create=False, components=components) as parent:
            report_raw = _read_private_file(parent, 'validation.json', MAX_REPORT_JSON_BYTES)
        report = strict_json_loads(report_raw)
        validate_report_document(report)
        if (hashlib.sha256(report_raw).hexdigest() != document['validation_report_digest']
                or report['validation']['status'] != 'success'
                or report['validation']['run_id'] != document['validation_run']
                or report['proposal']['id'] != document['proposal_id']
                or report['proposal']['digest'] != document['proposal_digest']
                or report['reproducibility']['run_1_digest'] != document['normalized_digest']):
            raise ArtifactError()
    except Exception:
        raise ArtifactError() from None
    return summary(document, identity)


def summary(document, identity):
    return {'candidate_digest': identity, 'proposal_id': document['proposal_id'],
            'validation_run': document['validation_run'], 'code_revision': document['code_revision'],
            'replacement_range': {k: v for k, v in document['schedule']['range'].items() if k != 'station'},
            'requested_effect': document['schedule']['effect'], 'application_supported': False}


class CandidatePreparation:
    """Memory-only export until the coordinator attests successful finalization."""
    def __init__(self, root=ROOT):
        self.root = Path(root)
        self.exports = []
        self.summary = None

    def begin(self, proposal, policy, run_id):
        if (proposal['assignment_changes'] or proposal['exclusions']
                or len(proposal['directives']) != 1
                or proposal['directives'][0]['type'] != 'date_slot'
                or proposal['directives'][0]['channel'] not in range(2, 8)):
            raise ArtifactError('candidate_scope_unsupported')
        self.proposal, self.policy, self.run_id = copy.deepcopy(proposal), copy.deepcopy(policy), run_id
        self.revision = code_revision(self.root)

    def capture(self, lifecycle, response, capture):
        if code_revision(self.root) != self.revision or len(self.exports) >= 2:
            raise ArtifactError('candidate_code_changed')
        schedule = export_candidate(lifecycle.stage, response, self.proposal, self.policy)
        if len(_json(schedule)) > MAX_BYTES:
            raise ArtifactError('candidate_limit')
        inputs = {k: v for k, v in lifecycle.request['input_fingerprints'].items()
                  if k != 'staged_source_physical_configuration_fingerprint'}
        inputs['physical_media_manifest_fingerprint'] = capture.media_manifest.summary['digest']
        for key in ('projected_configuration_fingerprint', 'working_database_fingerprint'):
            inputs[key] = response['verification']['fingerprints'][key]
        inputs['validation_context_fingerprint'] = digest(lifecycle.request['validation_context'])
        self.exports.append({'schedule': schedule, 'inputs': inputs})

    def publish(self, result, publication):
        if (result['status'] != 'success' or publication.get('publication_state') != 'published_durable'
                or len(self.exports) != 2 or _canonical(self.exports[0]) != _canonical(self.exports[1])
                or code_revision(self.root) != self.revision
                or not result['reproducibility']['passed']
                or len(result['cleanup']) != 2
                or any(not c['passed'] or c['quarantined'] for c in result['cleanup'])
                or {c['checkpoint'] for c in result['source_checks']} !=
                   {'after_capture', 'between_runs', 'after_run_2', 'before_success'}
                or any(not c['passed'] for c in result['source_checks'])):
            raise ArtifactError('candidate_not_approvable')
        document = {'schema_version': 1, 'proposal_id': self.proposal['proposal_id'],
                    'proposal_digest': hashlib.sha256(_json(self.proposal) + b'\n').hexdigest(),
                    'policy_digest': digest(self.policy),
                    'code_revision': self.revision, 'validation_run': self.run_id,
                    'validation_report_digest': publication['validation_json_digest'],
                    'normalized_digest': result['reproducibility']['run_1_digest'],
                    'directive': self.proposal['directives'][0], **self.exports[0]}
        identity = publish_candidate(document, self.root)
        self.summary = summary(document, identity)
        self.exports.clear()
