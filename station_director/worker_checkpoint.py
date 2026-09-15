"""Private, value-free lifecycle checkpoints for the isolated native worker."""

import ctypes
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path


CHECKPOINT_DIRECTORY = "native-single-run.checkpoints"
MAX_CHECKPOINTS = 40
MAX_CHANNELS = 6
SCHEMA_VERSION = 1
_PENDING_RE = re.compile(r"pending-(0[1-9]|[1-3][0-9]|40)\.json\Z")
_PREFIX = (
    "worker_started",
    "probes_passed",
    "snapshot_verified",
    "seed_verified",
    "request_verified",
    "native_import_completed",
    "configuration_completed",
)
_CYCLE = (
    "catalog_entered",
    "catalog_completed",
    "scheduler_entry",
    "scheduler_completed",
)
_FINAL = ("response_publication_attempted", "response_publication_completed")
STATES = frozenset(_PREFIX + _CYCLE + _FINAL)


class WorkerCheckpointError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckpointEvidence:
    states: tuple
    ignored_pending_tail: bool = False

    @property
    def worker_started(self):
        return bool(self.states) and self.states[0] == "worker_started"

    @property
    def scheduler_entered(self):
        return "scheduler_entry" in self.states

    @property
    def response_attempted(self):
        return "response_publication_attempted" in self.states

    @property
    def response_completed(self):
        return bool(self.states) and self.states[-1] == "response_publication_completed"


def _expected_names(prefix):
    return tuple(f"{prefix}-{sequence:02d}.json" for sequence in range(1, MAX_CHECKPOINTS + 1))


_PUBLISHED_NAMES = _expected_names("checkpoint")
_PENDING_NAMES = _expected_names("pending")
_ALLOWED_NAMES = frozenset(_PUBLISHED_NAMES + _PENDING_NAMES)


def _safe_directory(path, *, create=False):
    path = Path(path)
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    try:
        info = path.lstat()
    except OSError as exc:
        raise WorkerCheckpointError("checkpoint directory is unavailable") from exc
    if (
        not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
        or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise WorkerCheckpointError("checkpoint directory identity is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    held = os.fstat(descriptor)
    if (held.st_dev, held.st_ino) != (info.st_dev, info.st_ino):
        os.close(descriptor)
        raise WorkerCheckpointError("checkpoint directory changed")
    return descriptor


def _safe_file_info(directory_fd, name):
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
        or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
    ):
        raise WorkerCheckpointError("checkpoint file identity is unsafe")
    return info


def _validate_transitions(states):
    if not states:
        return
    if states[0] != "worker_started":
        raise WorkerCheckpointError("worker_started must be checkpoint sequence 1")
    phase = "prefix"
    prefix_index = 0
    cycle_index = 0
    cycles = 0
    final_index = 0
    for state in states:
        if state not in STATES:
            raise WorkerCheckpointError("checkpoint state is not allowlisted")
        if phase == "prefix":
            if state == "response_publication_attempted":
                if prefix_index == 0:
                    raise WorkerCheckpointError("checkpoint transition is invalid")
                phase, final_index = "final", 1
                continue
            if prefix_index >= len(_PREFIX) or state != _PREFIX[prefix_index]:
                raise WorkerCheckpointError("checkpoint transition is invalid")
            prefix_index += 1
            if prefix_index == len(_PREFIX):
                phase = "cycle"
            continue
        if phase == "cycle":
            if state == "response_publication_attempted":
                phase, final_index = "final", 1
                continue
            if state != _CYCLE[cycle_index]:
                raise WorkerCheckpointError("checkpoint transition is invalid")
            if cycle_index == 0 and cycles >= MAX_CHANNELS:
                raise WorkerCheckpointError("checkpoint channel bound exceeded")
            cycle_index += 1
            if cycle_index == len(_CYCLE):
                cycles += 1
                cycle_index = 0
                if cycles > MAX_CHANNELS:
                    raise WorkerCheckpointError("checkpoint channel bound exceeded")
            continue
        if phase == "final":
            if final_index != 1 or state != "response_publication_completed":
                raise WorkerCheckpointError("checkpoint transition is invalid")
            final_index = 2
            phase = "done"
            continue
        raise WorkerCheckpointError("checkpoint follows terminal state")


def _canonical_record(sequence, state):
    return {"schema_version": SCHEMA_VERSION, "sequence": sequence, "state": state}


def _read_published(directory_fd, name, sequence):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(descriptor)
        _safe_file_info(directory_fd, name)
        raw = os.read(descriptor, 4097)
        if len(raw) > 4096 or os.read(descriptor, 1):
            raise WorkerCheckpointError("checkpoint record is oversized")
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                after.st_ctime_ns):
            raise WorkerCheckpointError("checkpoint record changed while read")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise WorkerCheckpointError("checkpoint record is malformed") from exc
        if value != _canonical_record(sequence, value.get("state") if isinstance(value, dict) else None):
            raise WorkerCheckpointError("checkpoint record shape is invalid")
        canonical = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if raw != canonical or value["sequence"] != sequence or value["state"] not in STATES:
            raise WorkerCheckpointError("checkpoint record is not canonical")
        return value["state"]
    finally:
        os.close(descriptor)


def read_checkpoint_evidence(stage):
    directory = Path(stage) / CHECKPOINT_DIRECTORY
    if not os.path.lexists(directory):
        return CheckpointEvidence(())
    descriptor = _safe_directory(directory)
    try:
        present = set()
        # Enumerate through the verified, held directory descriptor so a path
        # replacement cannot redirect checkpoint inspection.
        with os.scandir(descriptor) as entries:
            for index, entry in enumerate(entries):
                if index >= len(_ALLOWED_NAMES):
                    raise WorkerCheckpointError("checkpoint directory entry bound exceeded")
                if entry.name not in _ALLOWED_NAMES:
                    raise WorkerCheckpointError("checkpoint directory contains an unexpected name")
                present.add(entry.name)
        states = []
        for sequence, name in enumerate(_PUBLISHED_NAMES, 1):
            if name not in present:
                if any(later in present for later in _PUBLISHED_NAMES[sequence:]):
                    raise WorkerCheckpointError("checkpoint sequence has a gap")
                break
            _safe_file_info(descriptor, name)
            states.append(_read_published(descriptor, name, sequence))
        next_sequence = len(states) + 1
        pending = sorted(name for name in present if _PENDING_RE.fullmatch(name))
        expected_pending = f"pending-{next_sequence:02d}.json" if next_sequence <= MAX_CHECKPOINTS else None
        if pending:
            if len(pending) != 1 or pending[0] != expected_pending:
                raise WorkerCheckpointError("checkpoint pending tail is invalid")
            _safe_file_info(descriptor, pending[0])
        _validate_transitions(states)
        return CheckpointEvidence(tuple(states), bool(pending))
    finally:
        os.close(descriptor)


def _rename_noreplace(directory_fd, source, target):
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise WorkerCheckpointError("atomic checkpoint publication is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    if renameat2(directory_fd, os.fsencode(source), directory_fd, os.fsencode(target), 1) != 0:
        code = ctypes.get_errno()
        raise WorkerCheckpointError("atomic checkpoint publication failed") from OSError(code, os.strerror(code))


class CheckpointWriter:
    def __init__(self, stage):
        self.directory = Path(stage) / CHECKPOINT_DIRECTORY
        self._descriptor = _safe_directory(self.directory, create=True)
        self._states = []

    def close(self):
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None

    @property
    def states(self):
        return tuple(self._states)

    def publish(self, state):
        candidate = self._states + [state]
        _validate_transitions(candidate)
        sequence = len(candidate)
        if sequence > MAX_CHECKPOINTS:
            raise WorkerCheckpointError("checkpoint record bound exceeded")
        pending = f"pending-{sequence:02d}.json"
        published = f"checkpoint-{sequence:02d}.json"
        raw = (json.dumps(_canonical_record(sequence, state), sort_keys=True,
                          separators=(",", ":")) + "\n").encode()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(pending, flags, 0o600, dir_fd=self._descriptor)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                raise WorkerCheckpointError("pending checkpoint identity is unsafe")
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise WorkerCheckpointError("checkpoint write made no progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _safe_file_info(self._descriptor, pending)
        _rename_noreplace(self._descriptor, pending, published)
        os.fsync(self._descriptor)
        self._states.append(state)

    def __enter__(self):
        return self

    def __exit__(self, unused_type, unused_value, unused_traceback):
        self.close()
