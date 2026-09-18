"""Private, preparation-only native selection evidence. No live writer."""
import copy
import os
import stat
from datetime import datetime
from pathlib import Path

from station_director.schedule_artifact import (
    ArtifactError, _catalog, _canonical, _json, _file_identity, digest,
    _catalog_descriptor, _catalog_reference_semantics,
)
from station_director.path_safety import canonical_media_mapping
from station_director.single_run_protocol import strict_json_loads, validate_document

REQUEST = 'selection-evidence.request.json'
OUTPUT = 'selection-evidence.json'
SCHEMA = Path(__file__).with_name('schemas') / 'selection-state.v1.schema.json'
LIMIT = 8 * 1024 * 1024
MAX_EVENTS = 10000


def require(value):
    if not value:
        raise ArtifactError('candidate_invalid')


def count(value):
    require(type(value) is int and 0 <= value <= 2**63-1)
    return value


def _directory(directory):
    path = Path(directory)
    require(path.is_absolute() and '..' not in path.parts)
    parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        require(os.fstat(parent).st_uid == os.geteuid())
        return parent
    except BaseException:
        os.close(parent)
        raise


def _read(directory, name):
    parent = _directory(directory)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                    and before.st_uid == os.geteuid() and stat.S_IMODE(before.st_mode) == 0o600
                    and before.st_size <= LIMIT)
            raw = stream.read(LIMIT + 1)
            require(len(raw) <= LIMIT and _file_identity(before) == _file_identity(os.fstat(stream.fileno()))
                    and _file_identity(before) == _file_identity(os.stat(name, dir_fd=parent, follow_symlinks=False)))
        return strict_json_loads(raw)
    finally:
        os.close(parent)


def _write(directory, name, document):
    raw = _json(document)
    require(len(raw) <= LIMIT)
    parent = _directory(directory)
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(parent)
    finally:
        os.close(parent)


def prepare(lifecycle, revision):
    _write(lifecycle.stage, REQUEST, {'request_digest': digest(lifecycle.request), 'code_revision': revision})


def requested(stage, request):
    try:
        marker = _read(stage, REQUEST)
    except FileNotFoundError:
        return None
    require(type(marker) is dict and set(marker) == {'request_digest', 'code_revision'}
            and marker['request_digest'] == digest(request))
    return marker


def identity(row):
    return (row['station'], row['tag'], canonical_media_mapping(
        row['realpath'] or row['path'], allow_sandbox=True).logical_identity)


def group_key(station, path):
    return digest([station, canonical_media_mapping(path, allow_sandbox=True).logical_identity])


def stable(sem):
    value = copy.deepcopy(sem)
    for field in ('count', 'updated_at'):
        value['row'].pop(field)
    return digest(value)


class Recorder:
    def __init__(self, connection, station, active_ids, rebuilt, clock):
        require(len(active_ids) <= 50000 and len(rebuilt) <= 50000)
        self.station = station
        self.clock = str(datetime.fromisoformat(clock))
        self.catalog = {i: pair for i, pair in _catalog(connection).items() if i in active_ids}
        require(set(self.catalog) == set(active_ids))
        self.rows, self.events, self.current, self.groups = [], [], {}, {}
        self.total_updates = 0
        generated = {}
        for row in rebuilt:
            key = identity(row)
            require(key not in generated)
            generated[key] = row
        used = set()
        for ident, (row, sem) in sorted(self.catalog.items()):
            require(row['station'] == station and not _catalog_descriptor(row))
            _catalog_reference_semantics(sem)
            key = identity(row)
            require(key in generated)
            original = generated[key]
            # Reconciliation may translate paths and allocate IDs, nothing else.
            left, right = dict(original), dict(row)
            for value in (left, right):
                value.pop('id', None)
                for field in ('path', 'realpath'):
                    value[field] = canonical_media_mapping(value[field], allow_sandbox=True).logical_identity
            require(_canonical(left) == _canonical(right))
            require(count(original['count']) == 0 and count(row['count']) == 0
                    and row['updated_at'] == self.clock and row['created_at'] == self.clock)
            used.add(key)
            self.current[ident] = (0, row['updated_at'])
            group = group_key(station, row['path'])
            self.groups.setdefault(group, []).append(ident)
            self.rows.append({'validated_id': ident, 'rebuild_count': 0, 'reconciled_count': 0,
                              'initial_updated_at': row['updated_at'], 'stable_digest': stable(sem),
                              'group_digest': group})
        require(used == set(generated))

    def observe(self, station, path, operation, before, after):
        require(station == self.station and operation == 'increment' and len(self.events) < MAX_EVENTS)
        group = group_key(station, path)
        ids = self.groups.get(group, [])
        require([r[0] for r in before] == ids == [r[0] for r in after])
        for old, new in zip(before, after):
            ident, value, timestamp = old
            require((count(value), timestamp) == self.current[ident])
            require(new == (ident, count(value + 1), self.clock))
            self.current[ident] = (value + 1, self.clock)
        self.events.append({'group_digest': group, 'ids': ids})
        self.total_updates += len(ids)
        require(self.total_updates <= 100000)

    def finish(self, connection):
        catalog = _catalog(connection)
        rows = []
        for initial in self.rows:
            ident = initial['validated_id']
            require(ident in catalog)
            raw, sem = catalog[ident]
            require(stable(sem) == initial['stable_digest']
                    and (raw['count'], raw['updated_at']) == self.current[ident])
            rows.append(dict(initial, final_count=count(raw['count']), final_updated_at=raw['updated_at']))
        return {'station': self.station, 'clock': self.clock, 'rows': rows, 'events': self.events}


def publish(stage, marker, request, channels):
    document = dict(schema_version=1, **marker, run_id=request['run_id'], channels=channels)
    validate_document(document, SCHEMA)
    _write(stage, OUTPUT, document)


def load(stage, request, revision):
    document = _read(stage, OUTPUT)
    validate_document(document, SCHEMA)
    require(document['request_digest'] == digest(request) and document['code_revision'] == revision
            and document['run_id'] == request['run_id'])
    clock = str(datetime.fromisoformat(request['validation_context']['reference_clock']))
    require(all(channel['clock'] == clock for channel in document['channels']))
    return document


def verify_channel(channel, catalog):
    """Replay actual write membership, checking exact final staged state."""
    rows = {row['validated_id']: row for row in channel['rows']}
    require(len(rows) == len(channel['rows']))
    counts = {}
    groups = {}
    for ident, row in rows.items():
        require(ident in catalog and not _catalog_descriptor(catalog[ident][0]))
        raw, sem = catalog[ident]
        require(raw['station'] == channel['station'] and stable(sem) == row['stable_digest']
                and raw['count'] == row['final_count'] and raw['updated_at'] == row['final_updated_at']
                and row['initial_updated_at'] == channel['clock'] == row['final_updated_at']
                and raw['created_at'] == channel['clock']
                and row['group_digest'] == group_key(raw['station'], raw['path']))
        require(count(row['rebuild_count']) == count(row['reconciled_count']) == 0)
        counts[ident] = 0
        groups.setdefault(row['group_digest'], []).append(ident)
    total = 0
    for event in channel['events']:
        ids = event['ids']
        require(ids == sorted(groups.get(event['group_digest'], [])))
        total += len(ids)
        require(total <= 100000)
        for ident in ids:
            counts[ident] = count(counts[ident] + 1)
    require(all(count(row['final_count']) == counts[ident] for ident, row in rows.items()))
    return rows


def proposal(channel, mapping, catalog, directories):
    rows = verify_channel(channel, catalog)
    changes = []
    for ident, phase in sorted(rows.items()):
        member = mapping[ident]
        before = member['semantics']['row']
        after = catalog[ident][1]['row']
        increments = phase['final_count'] - phase['reconciled_count']
        changed = count(before['count']) != count(after['count'])
        disposition = 'native_selection' if increments else 'native_reset' if changed else 'preserve_baseline'
        changes.append({
            'validated_id': ident, 'live_id': member['live_id'],
            'baseline_count': before['count'], 'proposed_count': after['count'],
            'baseline_updated_at': before['updated_at'],
            'proposed_updated_at': before['updated_at'] if disposition == 'preserve_baseline' else after['updated_at'],
            'timestamp_disposition': disposition, 'selection_increments': increments,
            'reset_delta': -before['count'], 'validated_semantics': catalog[ident][1],
            'commercial_directory': directories.get(ident),
        })
    return {'phase_evidence': channel, 'entries': changes, 'unaffected_channel_mutations': 0}


def validate_proposal(selection, mapping, station):
    from station_director.schedule_artifact import _selection_reference_semantics, _commercial_reference_key
    evidence = selection['phase_evidence']
    require(evidence['station'] == station and selection['unaffected_channel_mutations'] == 0)
    entries = selection['entries']
    require(len({e['validated_id'] for e in entries}) == len(entries)
            and len({e['live_id'] for e in entries}) == len(entries))
    catalog = {}
    for entry in entries:
        ident = entry['validated_id']
        require(ident in mapping and mapping[ident]['live_id'] == entry['live_id'])
        sem = entry['validated_semantics']
        raw = dict(sem['row'], id=ident)
        for field in ('path', 'realpath'):
            logical = raw[field]
            require(type(logical) is str and logical.startswith('crt-media:/'))
            mapped = canonical_media_mapping('catalog/crt_media' + logical[len('crt-media:'):])
            require(mapped.logical_identity == logical)
            raw[field] = mapped.sandbox_path
        catalog[ident] = raw, sem
        before = mapping[ident]['semantics']
        require(before['row']['station'] == station == raw['station'])
        directory = entry['commercial_directory']
        if directory is None:
            require(_selection_reference_semantics(before) == _selection_reference_semantics(sem))
        else:
            require(before['row']['tag'] != sem['row']['tag'])
            base = canonical_media_mapping(before['row']['tag'])
            require(base.logical_identity == directory and base.sandbox_path == sem['row']['tag'])
            left = _commercial_reference_key(before, directory, selection=True)
            require(left is not None and left == _commercial_reference_key(sem, directory, selection=True))
        require(entry['baseline_count'] == count(before['row']['count'])
                and entry['baseline_updated_at'] == before['row']['updated_at'])
    rows = verify_channel(evidence, catalog)
    require(set(rows) == set(catalog))
    expected = proposal(evidence, mapping, catalog,
                        {e['validated_id']: e['commercial_directory'] for e in entries})
    require(_canonical(expected) == _canonical(selection))
