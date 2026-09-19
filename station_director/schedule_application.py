"""Supervised writer. No scheduling, probes, service actions or DB opens on import.

The supported boundary is one cooperative operator, not hostile same-user/root
writers. Durable inhibition is deliberately not a lease. Unknown outcomes stay
inhibited until an operator runs recovery; there is no force path.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import stat
import subprocess
import tempfile
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from station_director import schedule_artifact as artifact
from station_director.preservation import logical_database_fingerprint

ROOT = artifact.ROOT
UNITS = ('fs42.service', 'crtstream.service')
MARKER = 'application-maintenance'
SCHEMA = Path(__file__).with_name('schemas') / 'schedule-application.v1.schema.json'
LIMIT = 65536
SECONDS = 900
PHASES = ('intent', 'prepared', 'database', 'restoring', 'complete')
CODES = frozenset(('ok', 'approval_invalid', 'candidate_unsupported', 'binding_changed',
    'window_closed', 'budget_exceeded', 'lock_busy', 'unsafe_state', 'guard_invalid',
    'service_state_unsupported', 'service_stop_failed', 'service_restore_failed',
    'database_changed', 'database_unknown', 'backup_invalid', 'row_collision',
    'preservation_failed', 'receipt_invalid', 'interrupted', 'application_failed'))


class ApplicationError(RuntimeError):
    def __init__(self, code):
        self.code = code if code in CODES else 'application_failed'
        super().__init__(self.code)


def require(condition, code):
    if not condition:
        raise ApplicationError(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(info):
    return [info.st_dev, info.st_ino, info.st_uid, info.st_gid,
            info.st_mode, info.st_nlink]


def safe_file(parent, name, limit=LIMIT):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and before.st_uid == os.geteuid() and stat.S_IMODE(before.st_mode) == 0o600
                and before.st_size <= limit, 'unsafe_state')
        raw = stream.read(limit + 1)
        require(artifact._file_identity(before) == artifact._file_identity(os.fstat(stream.fileno()))
                == artifact._file_identity(os.stat(name, dir_fd=parent, follow_symlinks=False))
                and len(raw) <= limit, 'unsafe_state')
        return raw


def publish(parent, name, raw):
    """Never replace a durable transition (or another operation's marker)."""
    from station_director.reporting import _rename_noreplace
    require(len(raw) <= LIMIT, 'unsafe_state')
    temporary = '.pending-' + os.urandom(16).hex()
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                 0o600, dir_fd=parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        _rename_noreplace(parent, temporary, parent, name)
        os.fsync(parent)
    finally:
        try:
            os.unlink(temporary, dir_fd=parent)
        except FileNotFoundError:
            pass


class Budget:
    def __init__(self):
        self.deadline = time.monotonic() + SECONDS

    def check(self, reserve=0):
        require(time.monotonic() + reserve < self.deadline, 'budget_exceeded')

    @contextlib.contextmanager
    def alarm(self):
        # A bounded main-thread CLI, including file enumeration and SQL work.
        require(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), 'unsafe_state')
        previous = signal.getsignal(signal.SIGALRM)
        def expired(signum, frame):
            raise ApplicationError('budget_exceeded')
        def interrupted(signum, frame):
            raise KeyboardInterrupt()
        old_term = signal.getsignal(signal.SIGTERM)
        old_hup = signal.getsignal(signal.SIGHUP)
        signal.signal(signal.SIGALRM, expired)
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGHUP, interrupted)
        signal.setitimer(signal.ITIMER_REAL, max(0.001, self.deadline - time.monotonic()))
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            signal.signal(signal.SIGTERM, old_term)
            signal.signal(signal.SIGHUP, old_hup)


class Store:
    def __init__(self, root, digest):
        self.root, self.digest = Path(root), digest
        self.path = self.root / 'runtime/director/applications' / digest

    @contextlib.contextmanager
    def open(self):
        with artifact._artifact_root(self.root, create=True, components=(
                ('runtime', False), ('director', True))) as parent:
            self.parent = parent
            with contextlib.ExitStack() as locks:
                # These are the same files used by validation and chapter maintenance.
                for name in ('.schedule-validation.lock', '.chapter-cache-maintenance.lock'):
                    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=parent)
                    locks.callback(os.close, fd)
                    info = os.fstat(fd)
                    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                            and info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600
                            and identity(info) == identity(os.stat(name, dir_fd=parent, follow_symlinks=False)), 'unsafe_state')
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        raise ApplicationError('lock_busy') from None
                # A prior operation may already have removed its marker while
                # restoration is unfinished. Do not admit a different candidate
                # into that recovery window merely because the marker is absent.
                self.exclude_other_pending()
                with artifact._artifact_root(self.root, create=True, components=(
                        ('runtime', False), ('director', True), ('applications', True),
                        (self.digest, True))) as directory:
                    self.directory = directory
                    # Bound recovery scanning, including abandoned pending files.
                    with os.scandir(directory) as entries:
                        require(sum(1 for _ in zip(range(65), entries)) < 65, 'unsafe_state')
                    yield self

    def exclude_other_pending(self):
        from station_director.reporting import _validate_directory_info
        with artifact._artifact_root(self.root, create=True, components=(
                ('runtime', False), ('director', True), ('applications', True))) as directory:
            with os.scandir(directory) as scan:
                names = [entry.name for _, entry in zip(range(101), scan)]
            require(len(names) <= 100, 'unsafe_state')
            require(self.digest in names or len(names) < 100, 'unsafe_state')
            for name in names:
                require(artifact.HEX.fullmatch(name), 'unsafe_state')
                if name == self.digest:
                    continue
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                try:
                    _validate_directory_info(os.fstat(fd), 'application', private=True)
                    other = Store(self.root, name)
                    other.directory = fd
                    for operation in ('apply', 'rollback'):
                        state = other.chain(operation)
                        require(state is None or state['phase'] == 'complete', 'unsafe_state')
                finally:
                    os.close(fd)

    def read(self, operation, phase):
        try:
            raw = safe_file(self.directory, operation + '.' + phase + '.json')
        except FileNotFoundError:
            return None
        from station_director.single_run_protocol import strict_json_loads, validate_document
        value = strict_json_loads(raw)
        validate_document(value, SCHEMA)
        require(raw == artifact._json(value) and value['candidate_digest'] == self.digest
                and value['operation'] == operation and value['phase'] == phase, 'receipt_invalid')
        return value

    def chain(self, operation):
        previous, gap, last = None, False, None
        for phase in PHASES:
            item = self.read(operation, phase)
            if item is None:
                gap = True
                continue
            require(not gap and item['previous'] == previous, 'receipt_invalid')
            if last:
                for field in ('candidate_digest', 'code_revision', 'prior_services', 'database_identity', 'before'):
                    require(item[field] == last[field], 'receipt_invalid')
                if last['phase'] != 'intent':
                    for field in ('after', 'backup_digest'):
                        require(item[field] == last[field], 'receipt_invalid')
                if last['phase'] in ('database', 'restoring'):
                    require(item['database_outcome'] == last['database_outcome'], 'receipt_invalid')
            previous, last = sha(artifact._json(item)), item
        return last

    def write(self, state, phase, **changes):
        from station_director.single_run_protocol import validate_document
        value = dict(state, **changes)
        value.update(phase=phase, previous=None if phase == 'intent' else sha(artifact._json(state)))
        validate_document(value, SCHEMA)
        publish(self.directory, value['operation'] + '.' + phase + '.json', artifact._json(value))
        return value

    def marker_value(self, operation):
        intent = self.read(operation, 'intent')
        require(intent is not None, 'receipt_invalid')
        return artifact._json({'candidate_digest': self.digest, 'intent_digest': sha(artifact._json(intent))})

    def marker(self, operation, *, create=False, remove=False):
        expected = self.marker_value(operation)
        try:
            raw = safe_file(self.parent, MARKER)
        except FileNotFoundError:
            if create:
                publish(self.parent, MARKER, expected)
            elif remove:
                os.fsync(self.parent)
            elif not remove:
                raise ApplicationError('unsafe_state') from None
            return
        require(raw == expected, 'unsafe_state')
        if remove:
            os.unlink(MARKER, dir_fd=self.parent)
            os.fsync(self.parent)

    def absent_marker(self):
        try:
            os.stat(MARKER, dir_fd=self.parent, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise ApplicationError('unsafe_state')

    def failure(self, state, code):
        # Failure receipts are separate from the irreversible transition chain.
        # A failed receipt write must never replace the original failure.
        value = {'schema_version': 1, 'candidate_digest': self.digest,
                 'operation': state['operation'], 'last_transition': sha(artifact._json(state)),
                 'code': code,
                 'database_outcome': state['database_outcome'] if state['phase'] in ('database', 'restoring', 'complete') else 'unknown',
                 'service_outcome': 'restored' if state['phase'] == 'complete' else 'recovery_required'}
        from station_director.single_run_protocol import validate_document
        validate_document(value, SCHEMA)
        for index in range(16):
            name = 'failure-%02d.json' % index
            try:
                os.stat(name, dir_fd=self.directory, follow_symlinks=False)
            except FileNotFoundError:
                publish(self.directory, name, artifact._json(value))
                return
        raise ApplicationError('receipt_invalid')


class Services:
    """Fixed two-unit adapter. Tests replace this object, never user units."""
    def __init__(self, root, budget):
        self.root, self.budget = Path(root), budget

    def command(self, argv, timeout=10):
        self.budget.check()
        # Output is spooled, not buffered without a bound in memory.
        with tempfile.TemporaryFile() as output:
            result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=output,
                                    stderr=subprocess.DEVNULL, timeout=timeout, check=False)
            require(result.returncode == 0 and output.tell() <= LIMIT, 'guard_invalid')
            output.seek(0)
            return output.read(LIMIT + 1).decode('utf-8')

    def properties(self, unit):
        require(unit in UNITS, 'guard_invalid')
        names = ('LoadState', 'ActiveState', 'SubState', 'MainPID', 'ControlGroup', 'Job',
                 'KillMode', 'SendSIGKILL', 'NeedDaemonReload', 'DropInPaths', 'TimeoutStopUSec')
        text = self.command(['systemctl', '--user', 'show', unit, '--no-pager',
                             '--property=' + ','.join(names)])
        values = dict(line.split('=', 1) for line in text.splitlines() if '=' in line)
        require(set(values) == set(names), 'guard_invalid')
        return values

    def guards(self):
        marker = str(self.root / 'runtime/director' / MARKER)
        require(self.root == Path.home() / 'FieldStation42', 'guard_invalid')
        for unit in UNITS:
            installed = Path.home() / '.config/systemd/user' / (unit + '.d')
            name = '50-director-maintenance.conf'
            # Installed drop-ins need not be private, but must not be writable by others.
            fd = os.open(installed, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                d = os.fstat(fd)
                require(d.st_uid == os.geteuid() and not d.st_mode & 0o022, 'guard_invalid')
                f = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                with os.fdopen(f, 'rb') as stream:
                    info = os.fstat(stream.fileno())
                    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                            and info.st_uid == os.geteuid() and not info.st_mode & 0o022
                            and info.st_size < LIMIT, 'guard_invalid')
                    raw = stream.read(LIMIT)
                expected = (self.root / 'station_director/systemd' / (unit + '.d') / name).read_bytes()
                require(raw == expected, 'guard_invalid')
            finally:
                os.close(fd)
            props = self.properties(unit)
            require(props['LoadState'] == 'loaded' and props['NeedDaemonReload'] == 'no'
                    and props['KillMode'] == 'control-group' and props['SendSIGKILL'] == 'yes'
                    and str(installed / name) in props['DropInPaths'].split()
                    and props['TimeoutStopUSec'] in ('1min 30s', '90s'), 'guard_invalid')
            # Inspect the *effective* unit condition, not just a file that could
            # have been reset/OR-ed by another drop-in. Reject additional conditions.
            object_reply = json.loads(self.command(['busctl', '--user', '--json=short', 'call',
                'org.freedesktop.systemd1', '/org/freedesktop/systemd1',
                'org.freedesktop.systemd1.Manager', 'GetUnit', 's', unit]))
            require(object_reply.get('type') == 'o' and len(object_reply.get('data', [])) == 1, 'guard_invalid')
            obj = object_reply['data'][0]
            require(isinstance(obj, str) and obj.startswith('/org/freedesktop/systemd1/unit/'), 'guard_invalid')
            reply = json.loads(self.command(['busctl', '--user', '--json=short', 'get-property',
                'org.freedesktop.systemd1', obj, 'org.freedesktop.systemd1.Unit', 'Conditions']))
            require(reply.get('type') == 'a(sbbsi)' and isinstance(reply.get('data'), list)
                    and len(reply['data']) == 1, 'guard_invalid')
            conditions = reply['data'][0]
            require(isinstance(conditions, list) and len(conditions) == 1
                    and isinstance(conditions[0], list) and len(conditions[0]) == 5
                    and type(conditions[0][1]) is bool and type(conditions[0][2]) is bool
                    and conditions[0][:4] == ['ConditionPathExists', False, True, marker], 'guard_invalid')

    def states(self):
        states = {}
        for unit in UNITS:
            props = self.properties(unit)
            state = (props['ActiveState'], props['SubState'])
            require(props['Job'] in ('', '0') and state in (('active', 'running'), ('inactive', 'dead')),
                    'service_state_unsupported')
            states[unit] = state[0]
        require(tuple(states[u] for u in UNITS) != ('inactive', 'active'), 'service_state_unsupported')
        return states

    def empty(self, unit, group=''):
        props = self.properties(unit)
        require(props['ActiveState'] == 'inactive' and props['SubState'] == 'dead'
                and props['MainPID'] == '0' and props['Job'] in ('', '0'), 'service_stop_failed')
        for value in {group, props['ControlGroup']} - {''}:
            require(value.startswith('/user.slice/') and '..' not in Path(value).parts, 'service_stop_failed')
            path = Path('/sys/fs/cgroup') / value.lstrip('/') / 'cgroup.events'
            try:
                events = dict(line.split() for line in path.read_text().splitlines())
                require(events.get('populated') == '0', 'service_stop_failed')
            except FileNotFoundError:
                pass  # a removed cgroup has no remaining members

    def stop(self):
        for unit in reversed(UNITS):
            group = self.properties(unit)['ControlGroup']
            try:
                self.command(['systemctl', '--user', 'stop', unit], timeout=100)
                self.empty(unit, group)
            except Exception:
                raise ApplicationError('service_stop_failed') from None
        self.quiet()

    def quiet(self):
        for unit in UNITS:
            self.empty(unit)

    def restore(self, prior, checkpoint):
        for unit in UNITS:  # fs42 first; crtstream Wants=fs42 cannot alter supported prior states
            props = self.properties(unit)
            if prior[unit] == 'inactive':
                self.empty(unit)
            else:
                # A previous restoration start may itself have failed. Retrying
                # start is safe only here, after the database outcome seal; a
                # failed unit is still forbidden as an *admission* prior state.
                require(props['ActiveState'] in ('inactive', 'active', 'failed'), 'service_restore_failed')
                self.command(['systemctl', '--user', 'start', unit], timeout=100)
            checkpoint('service-' + unit)
        require(self.states() == prior, 'service_restore_failed')


class Bindings:
    def __init__(self, root):
        self.root = Path(root)

    def code(self, document):
        require(artifact.code_revision(self.root) == document['code_revision'], 'binding_changed')

    def inputs(self, document):
        from station_director import secure_validation_inputs as secure
        from station_director.preservation import protected_json_paths, fingerprint_json_files, capture_media_manifest
        from station_director.validation_context import (logical_protected_configuration_fingerprint,
            logical_media_manifest_fingerprint, derive_validation_context, canonical_seed_inputs)
        proposal, policy = secure.load_canonical_proposal(document['proposal_id']), secure.load_canonical_policy()
        require(sha(artifact._json(proposal) + b'\n') == document['proposal_digest']
                and artifact.digest(policy) == document['policy_digest'], 'binding_changed')
        paths = protected_json_paths(self.root)
        physical = fingerprint_json_files(paths)['digest']
        logical = logical_protected_configuration_fingerprint(paths)['digest']
        manifest = capture_media_manifest(Path('/mnt/t7/CRT-Media'))
        try:
            media = logical_media_manifest_fingerprint(manifest)['digest']
            physical_media = manifest.summary['digest']
        finally:
            manifest.close()
        expected = document['inputs']
        require(physical == expected['live_physical_configuration_fingerprint']
                and logical == expected['original_logical_configuration_fingerprint']
                and media == expected['logical_media_manifest_fingerprint']
                and physical_media == expected['physical_media_manifest_fingerprint'], 'binding_changed')
        context = derive_validation_context(proposal, policy, canonical_seed_inputs(
            logical, expected['original_logical_database_fingerprint'], media))
        require(artifact.digest(context) == expected['validation_context_fingerprint'], 'binding_changed')


def fingerprint(connection):
    result = logical_database_fingerprint(connection)
    require(result['integrity'] == ['ok'], 'preservation_failed')
    return result


def window(document, now, minutes=30):
    start = artifact._time(document['schedule']['range']['regeneration_start']).replace(tzinfo=ZoneInfo('America/Los_Angeles'))
    boundary = artifact._time(document['schedule']['range']['proposal_boundary']).replace(tzinfo=start.tzinfo)
    require(min(start, boundary).timestamp() - now.timestamp() >= minutes * 60, 'window_closed')


def replace_rows(connection, document, baseline=None):
    """The only DML implementation, also used to simulate and verify the backup.

    baseline=None applies; a read-only backup connection supplies exact inverse
    values, including AUTOINCREMENT state. No catalog row is added or deleted.
    """
    span = document['schedule']['range']
    station, boundary, end = span['station'], span['proposal_boundary'], span['replacement_end']
    original = fingerprint(connection)
    allocator_before = connection.execute("SELECT name,seq FROM sqlite_sequence WHERE name!='liquid_blocks' ORDER BY name").fetchall()
    require(not connection.execute("SELECT 1 FROM sqlite_master WHERE type='trigger'").fetchone(), 'preservation_failed')
    columns, old_rows = artifact._rows(connection, 'liquid_blocks')
    require(tuple(columns) == artifact.BLOCK_COLUMNS, 'preservation_failed')
    in_range = lambda r: r['station'] == station and boundary <= r['start_time'] < end
    protected = [r for r in old_rows if not in_range(r)]
    # A block starting beyond the approved replacement remains untouched; reject
    # overlaps at the far edge, not just ID collisions.
    require(not any(r['station'] == station and r['start_time'] < end and r['end_time'] > end
                    for r in old_rows), 'preservation_failed')
    seam = span['regeneration_start']
    require(not any(r['station'] == station and r['start_time'] < boundary
                    and r['end_time'] > seam for r in protected), 'preservation_failed')
    rows = document['schedule']['rows'] if baseline is None else [r for r in artifact._rows(baseline, 'liquid_blocks')[1] if in_range(r)]
    require(not ({r['id'] for r in rows} & {r['id'] for r in protected}), 'row_collision')
    before_catalog = artifact._rows(connection, 'catalog_entries')[1]
    by_id = {r['id']: r for r in before_catalog}
    mutations = document['schedule']['selection_state']['entries']
    expected_catalog = {i: dict(r) for i, r in by_id.items()}
    for entry in mutations:
        row = by_id.get(entry['live_id'])
        before_prefix, after_prefix = ('baseline', 'proposed') if baseline is None else ('proposed', 'baseline')
        require(row is not None and row['station'] == station
                and artifact._canonical([row['count'], row['updated_at']]) == artifact._canonical(
                    [entry[before_prefix + '_count'], entry[before_prefix + '_updated_at']]), 'database_changed')
        expected_catalog[entry['live_id']].update(count=entry[after_prefix + '_count'], updated_at=entry[after_prefix + '_updated_at'])
    connection.execute('DELETE FROM liquid_blocks WHERE station=? AND start_time>=? AND start_time<?', (station, boundary, end))
    sql = 'INSERT INTO liquid_blocks (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in columns) + ')'
    connection.executemany(sql, [tuple(r[c] for c in columns) for r in rows])
    for entry in mutations:
        row = expected_catalog[entry['live_id']]
        connection.execute('UPDATE catalog_entries SET count=?, updated_at=? WHERE id=?', (row['count'], row['updated_at'], row['id']))
    if baseline is not None:
        saved = baseline.execute("SELECT seq FROM sqlite_sequence WHERE name='liquid_blocks'").fetchall()
        require(len(saved) <= 1, 'preservation_failed')
        connection.execute("DELETE FROM sqlite_sequence WHERE name='liquid_blocks'")
        if saved:
            connection.execute("INSERT INTO sqlite_sequence(name,seq) VALUES('liquid_blocks',?)", saved[0])
    # Typed full-row comparisons protect all other fields and all historical rows.
    order = lambda values: sorted(artifact._canonical(r) for r in values)
    require(order(artifact._rows(connection, 'liquid_blocks')[1]) == order(protected + rows)
            and order(artifact._rows(connection, 'catalog_entries')[1]) == order(expected_catalog.values()), 'preservation_failed')
    final = fingerprint(connection)
    require(final['schema_digest'] == original['schema_digest']
            and connection.execute("SELECT name,seq FROM sqlite_sequence WHERE name!='liquid_blocks' ORDER BY name").fetchall() == allocator_before
            and final['foreign_key_check'] == original['foreign_key_check']
            and {k: v for k, v in final['tables'].items() if k not in ('liquid_blocks', 'catalog_entries', 'sqlite_sequence')}
            == {k: v for k, v in original['tables'].items() if k not in ('liquid_blocks', 'catalog_entries', 'sqlite_sequence')}, 'preservation_failed')
    return final['digest']


class Application:
    def __init__(self, root, document, candidate_digest, services, bindings, budget,
                 checkpoint=lambda name: None, now=lambda: datetime.now(ZoneInfo('America/Los_Angeles'))):
        self.root, self.doc, self.digest = Path(root), document, candidate_digest
        self.services, self.bindings, self.budget = services, bindings, budget
        self.checkpoint, self.now = checkpoint, now
        self.database = self.root / 'runtime/fs42_fluid.db'
        self.store = Store(root, candidate_digest)
        self.state = None

    def db_identity(self):
        fd = os.open(self.database, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and info.st_nlink == 1 and not info.st_mode & 0o022
                    and identity(info) == identity(os.stat(self.database, follow_symlinks=False)), 'unsafe_state')
            for suffix in ('-wal', '-shm', '-journal'):
                try:
                    side = os.stat(str(self.database) + suffix, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                require(stat.S_ISREG(side.st_mode) and side.st_uid == os.geteuid()
                        and side.st_nlink == 1 and not side.st_mode & 0o022, 'unsafe_state')
            return identity(info)
        finally:
            os.close(fd)

    @contextlib.contextmanager
    def connect(self, path, *, writable=False):
        connection = sqlite3.connect(Path(path).as_uri() + ('?mode=rw' if writable else '?mode=ro'), uri=True, timeout=2)
        try:
            connection.set_progress_handler(lambda: int(time.monotonic() >= self.budget.deadline), 1000)
            if writable:
                connection.execute('PRAGMA synchronous=FULL')
            else:
                connection.execute('PRAGMA query_only=ON')
            yield connection
        finally:
            connection.close()

    def backup(self):
        from station_director.chapter_cache_warmup import stable_private_generation
        name = 'baseline.sqlite3'
        # Only intent state can create this file. A crash leaving a partial file
        # causes verification failure, never replacement of a possible backup.
        path = self.store.path / name
        if not path.exists():
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.store.directory)
            os.close(fd)
            with stable_private_generation(self.database, temporary_parent=self.store.path,
                                           deadline=self.budget.deadline) as generation:
                with self.connect(generation.database) as source, self.connect(path, writable=True) as dest:
                    source.backup(dest, pages=256, progress=lambda *args: self.budget.check())
                    dest.execute('PRAGMA journal_mode=DELETE')
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self.store.directory)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.fsync(self.store.directory)
        return self.verify_backup()

    def verify_backup(self, expected=None):
        # Bounded streaming hash: backups can be much larger than JSON records.
        fd = os.open('baseline.sqlite3', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self.store.directory)
        h = hashlib.sha256()
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                    and before.st_nlink == 1 and stat.S_IMODE(before.st_mode) == 0o600
                    and 0 < before.st_size <= 4 * 1024**3, 'backup_invalid')
            while True:
                self.budget.check()
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
            require(artifact._file_identity(before) == artifact._file_identity(os.fstat(stream.fileno()))
                    == artifact._file_identity(os.stat('baseline.sqlite3', dir_fd=self.store.directory, follow_symlinks=False)), 'backup_invalid')
        require(expected is None or h.hexdigest() == expected, 'backup_invalid')
        with self.connect(self.store.path / 'baseline.sqlite3') as source:
            require(fingerprint(source)['digest'] == self.doc['inputs']['original_logical_database_fingerprint'], 'backup_invalid')
            baseline = artifact._catalog(source)
            mapping = {item['live_id']: item['semantics'] for item in self.doc['schedule']['catalog_mapping']}
            require(set(mapping) == set(baseline) and all(
                artifact._canonical(mapping[key]) == artifact._canonical(baseline[key][1])
                for key in baseline), 'binding_changed')
            require(artifact._metadata(source) == self.doc['schedule']['metadata_digests'], 'binding_changed')
        return h.hexdigest()

    def preview(self):
        with tempfile.TemporaryDirectory(prefix='preview-', dir=self.store.path) as temp:
            path = Path(temp) / 'expected.sqlite3'
            with contextlib.closing(sqlite3.connect(path)) as preview:
                with self.connect(self.store.path / 'baseline.sqlite3') as source:
                    source.backup(preview)
                preview.execute('BEGIN IMMEDIATE')
                result = replace_rows(preview, self.doc)
                preview.rollback()
                return result

    def transition(self, state, phase, **changes):
        state = self.store.write(state, phase, **changes)
        self.state = state
        self.checkpoint(state['operation'] + '-' + phase)
        return state

    def finish(self, state):
        # restoring is a durable database-outcome seal. Never open/mutate the DB
        # again after this transition: resumed playback may already have written.
        if state['phase'] == 'database':
            self.store.marker(state['operation'])
            self.services.quiet()
            require(self.db_identity() == state['database_identity'], 'database_unknown')
            expected = (state['after'] if state['database_outcome'] ==
                        ('applied' if state['operation'] == 'apply' else 'rolled_back') else state['before'])
            if state['backup_digest'] is None:
                expected = state['after']  # verified abort, no write was authorized
            with self.connect(self.database) as connection:
                require(fingerprint(connection)['digest'] == expected, 'database_unknown')
            state = self.transition(state, 'restoring')
        require(state['phase'] == 'restoring', 'receipt_invalid')
        self.services.guards()
        self.store.marker(state['operation'], remove=True)
        self.checkpoint('marker-removed')
        try:
            self.services.restore(state['prior_services'], self.checkpoint)
        except Exception:
            raise ApplicationError('service_restore_failed') from None
        return self.transition(state, 'complete', service_outcome='restored')

    def prepare_state(self, operation, apply_state):
        self.store.absent_marker()
        window(self.doc, self.now())
        self.budget.check(300)
        self.bindings.inputs(self.doc)
        prior = self.services.states()
        if operation == 'rollback':
            require(self.db_identity() == apply_state['database_identity'], 'database_changed')
        before = self.doc['inputs']['original_logical_database_fingerprint'] if operation == 'apply' else apply_state['after']
        state = self.transition({'schema_version': 1, 'candidate_digest': self.digest,
            'code_revision': self.doc['code_revision'], 'operation': operation,
            'prior_services': prior, 'database_identity': self.db_identity(),
            'before': before, 'after': None, 'backup_digest': None,
            'database_outcome': 'not_applied' if operation == 'apply' else 'applied',
            'service_outcome': 'inhibited'}, 'intent')
        self.store.marker(operation, create=True)
        self.checkpoint('marker-created')
        self.services.stop()
        self.checkpoint('services-stopped')
        require(self.db_identity() == state['database_identity'], 'database_changed')
        with self.connect(self.database) as source:
            require(fingerprint(source)['digest'] == before, 'database_changed')
        if operation == 'apply':
            backup_digest = self.backup()
            after = self.preview()
        else:
            backup_digest = self.verify_backup(apply_state['backup_digest'])
            after = self.doc['inputs']['original_logical_database_fingerprint']
        return self.transition(state, 'prepared', after=after, backup_digest=backup_digest)

    def mutate(self, state):
        self.verify_backup(state['backup_digest'])
        self.services.guards()
        self.services.quiet()
        self.store.marker(state['operation'])
        require(self.db_identity() == state['database_identity'], 'database_changed')
        self.bindings.inputs(self.doc)
        window(self.doc, self.now())
        self.budget.check(300)
        with self.connect(self.database, writable=True) as connection, contextlib.ExitStack() as stack:
            connection.execute('BEGIN IMMEDIATE')
            require(fingerprint(connection)['digest'] == state['before'], 'database_changed')
            baseline = stack.enter_context(self.connect(self.store.path / 'baseline.sqlite3')) if state['operation'] == 'rollback' else None
            require(replace_rows(connection, self.doc, baseline) == state['after'], 'preservation_failed')
            self.bindings.code(self.doc)
            self.bindings.inputs(self.doc)
            self.services.guards()
            self.services.quiet()
            self.store.marker(state['operation'])
            require(self.db_identity() == state['database_identity'], 'database_changed')
            window(self.doc, self.now(), 5)
            self.budget.check(300)
            self.checkpoint('before-commit')
            connection.commit()
            self.checkpoint('after-commit')
            require(fingerprint(connection)['digest'] == state['after'], 'database_unknown')
        return self.transition(state, 'database', database_outcome='applied' if state['operation'] == 'apply' else 'rolled_back')

    def recover(self, state):
        if state['phase'] == 'complete':
            return state
        if state['phase'] in ('database', 'restoring'):
            return self.finish(state)
        self.store.marker(state['operation'], create=True)
        self.services.stop()
        require(self.db_identity() == state['database_identity'], 'database_unknown')
        # No prepared record means no transaction was authorized. Recovery aborts
        # instead of attempting to finish admission using possibly stale inputs.
        if state['phase'] == 'intent':
            with self.connect(self.database) as connection:
                current = fingerprint(connection)['digest']
            # Stale admission (including a service's final write while stopping)
            # is not an uncertain application commit: no prepared record exists.
            # Seal the observed state unchanged, without pretending it is the
            # approved baseline or allowing a later transaction to use it.
            state = self.transition(state, 'prepared', after=current)
        elif state['backup_digest'] is not None:
            self.verify_backup(state['backup_digest'])
            expected = self.preview() if state['operation'] == 'apply' else self.doc['inputs']['original_logical_database_fingerprint']
            require(state['after'] == expected, 'receipt_invalid')
        # A prepared transaction may have left a hot SQLite journal. After the
        # verified backup/identity and writer-exclusion checks, let SQLite do its
        # own bounded atomic recovery; never copy a backup over an unknown DB.
        with self.connect(self.database, writable=state['backup_digest'] is not None) as connection:
            current = fingerprint(connection)['digest']
        require(current in ((state['after'],) if state['backup_digest'] is None
                            else (state['before'], state['after'])), 'database_unknown')
        applied = state['backup_digest'] is not None and current == state['after'] and state['after'] != state['before']
        outcome = ('applied' if applied else 'not_applied') if state['operation'] == 'apply' else ('rolled_back' if applied else 'applied')
        state = self.transition(state, 'database', database_outcome=outcome)
        return self.finish(state)

    def run(self, operation):
        require(operation in ('apply', 'recover', 'rollback'), 'approval_invalid')
        artifact.validate_candidate(self.doc)
        require(self.doc['schema_version'] == 2 and sha(artifact._json(self.doc)) == self.digest, 'candidate_unsupported')
        self.bindings.code(self.doc)
        with self.store.open():
            try:
                return self.run_locked(operation)
            except BaseException as exc:
                if self.state is not None:
                    code = exc.code if isinstance(exc, ApplicationError) else 'interrupted' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else 'application_failed'
                    try:
                        self.store.failure(self.state, code)
                    except BaseException:
                        pass  # Never turn a journal failure into permission to resume.
                raise

    def run_locked(self, operation):
        self.services.guards()
        applied, rolled = self.store.chain('apply'), self.store.chain('rollback')
        for item in (applied, rolled):
            if item:
                require(item['code_revision'] == self.doc['code_revision'], 'receipt_invalid')
        if applied:
            require(applied['before'] == self.doc['inputs']['original_logical_database_fingerprint'], 'receipt_invalid')
        if rolled:
            require(applied is not None and applied['phase'] == 'complete'
                    and applied['database_outcome'] == 'applied'
                    and rolled['before'] == applied['after']
                    and rolled['database_identity'] == applied['database_identity'], 'receipt_invalid')
        self.state = rolled or applied
        if operation == 'recover':
            state = rolled or applied
            require(state is not None, 'receipt_invalid')
            return self.recover(state)
        if operation == 'apply' and applied:
            require(rolled is None, 'receipt_invalid')
            require(applied['phase'] == 'complete', 'receipt_invalid')
            return applied
        if operation == 'rollback':
            require(applied is not None and applied['phase'] == 'complete'
                    and applied['database_outcome'] == 'applied', 'receipt_invalid')
            if rolled:
                require(rolled['phase'] == 'complete', 'receipt_invalid')
                return rolled
        state = self.prepare_state(operation, applied)
        state = self.mutate(state)
        return self.finish(state)


def execute(operation, candidate_digest, approval):
    """The CLI exposes only fixed outcomes; never raw exceptions or private rows."""
    database, services = 'unknown', 'not_attempted'
    app = None
    try:
        require(isinstance(candidate_digest, str) and artifact.HEX.fullmatch(candidate_digest)
                and approval == candidate_digest, 'approval_invalid')
        from station_director.isolation import check_invocation_context
        require(check_invocation_context()[0], 'unsafe_state')
        budget = Budget()
        with budget.alarm():
            document = artifact.load_verified_candidate(candidate_digest)
            app = Application(ROOT, document, candidate_digest, Services(ROOT, budget), Bindings(ROOT), budget)
            result = app.run(operation)
        return {'code': 'ok', 'candidate_digest': candidate_digest,
                'database_outcome': result['database_outcome'], 'service_outcome': result['service_outcome']}
    except BaseException as exc:
        if isinstance(exc, (SystemExit, GeneratorExit)):
            raise
        code = exc.code if isinstance(exc, ApplicationError) else 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'application_failed'
        if app is not None and app.state is not None:
            state = app.state
            if state['phase'] in ('database', 'restoring', 'complete'):
                database = state['database_outcome']
            services = 'restored' if state['phase'] == 'complete' else 'recovery_required'
        # Failure is not a guessed transaction outcome. Durable phase records,
        # marker and backup are the recovery authority; never clear them here.
        return {'code': code, 'database_outcome': database, 'service_outcome': services,
                'recovery_required': True}
