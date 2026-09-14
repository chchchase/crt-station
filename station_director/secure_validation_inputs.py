"""Descriptor-confined acquisition for public validation inputs."""

import os
import re
import stat
from pathlib import Path

from station_director.policy import validate_policy_document
from station_director.proposals import migrate_v1_to_v2, validate_schema
from station_director.single_run_protocol import strict_json_loads


TRUSTED_PROJECT_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_POLICY_NAME = "channel_identities.v2.json"
PROPOSAL_ID_RE = re.compile(r"p-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}\Z")
MAX_DIRECTORY_ENTRIES = 10_000
MAX_PROPOSAL_BYTES = 2 * 1024 * 1024
MAX_POLICY_BYTES = 512 * 1024


class SecureInputError(RuntimeError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def validate_proposal_id(proposal_id):
    if not isinstance(proposal_id, str) or not PROPOSAL_ID_RE.fullmatch(proposal_id):
        raise SecureInputError("invalid_proposal_id")
    return proposal_id


def _bounded_entries(directory_fd):
    scan_fd = os.open(
        ".", os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NOATIME", 0), dir_fd=directory_fd,
    )
    iterator = os.scandir(scan_fd)
    try:
        for count, entry in enumerate(iterator, 1):
            if count > MAX_DIRECTORY_ENTRIES:
                raise SecureInputError("directory_entry_limit")
            yield entry
    finally:
        iterator.close()
        os.close(scan_fd)


def _validate_directory(info):
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o022):
        raise SecureInputError("unsafe_directory")


def _find_exact(directory_fd, name):
    found = None
    folded = name.casefold()
    for entry in _bounded_entries(directory_fd):
        if entry.name.casefold() == folded:
            if entry.name != name or found is not None:
                raise SecureInputError("case_ambiguous_path")
            found = entry.stat(follow_symlinks=False)
    if found is None:
        raise SecureInputError("missing_path")
    return found


def _open_directory(parent_fd, name):
    before = _find_exact(parent_fd, name)
    descriptor = os.open(
        name, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NOATIME", 0), dir_fd=parent_fd,
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise SecureInputError("path_replaced")
        _validate_directory(opened)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_project_root():
    root = Path(TRUSTED_PROJECT_ROOT)
    if not root.is_absolute():
        raise SecureInputError("unsafe_project_root")
    descriptor = os.open(
        root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NOATIME", 0)
    )
    try:
        _validate_directory(os.fstat(descriptor))
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _descend(descriptor, names):
    current = descriptor
    try:
        for name in names:
            following = _open_directory(current, name)
            os.close(current)
            current = following
        return current
    except BaseException:
        os.close(current)
        raise


def _read_regular(directory_fd, name, limit):
    before = _find_exact(directory_fd, name)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NOATIME", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    try:
        opened = os.fstat(descriptor)
        if ((opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                or not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) & 0o022
                or opened.st_size > limit):
            raise SecureInputError("unsafe_input_file")
        chunks = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise SecureInputError("input_too_large")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                after.st_ctime_ns) != (
                opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns,
                opened.st_ctime_ns):
            raise SecureInputError("input_replaced")
        path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (path_after.st_dev, path_after.st_ino) != (opened.st_dev, opened.st_ino):
            raise SecureInputError("input_replaced")
        return b"".join(chunks), opened
    finally:
        os.close(descriptor)


def _validate_optional_legacy_report(directory_fd):
    try:
        before = _find_exact(directory_fd, "validation.json")
    except SecureInputError as exc:
        if exc.code == "missing_path":
            return
        raise
    descriptor = os.open(
        "validation.json", os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NOATIME", 0), dir_fd=directory_fd,
    )
    try:
        opened = os.fstat(descriptor)
        after = os.stat("validation.json", dir_fd=directory_fd, follow_symlinks=False)
        if (len({(before.st_dev, before.st_ino), (opened.st_dev, opened.st_ino),
             (after.st_dev, after.st_ino)}) != 1
                or not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) & 0o022):
            raise SecureInputError("unsafe_legacy_validation")
    finally:
        os.close(descriptor)


def _proposal_contents(directory_fd):
    names = set()
    folded_names = set()
    for entry in _bounded_entries(directory_fd):
        folded = entry.name.casefold()
        if folded in folded_names:
            raise SecureInputError("case_ambiguous_path")
        folded_names.add(folded)
        names.add(entry.name)
    allowed = {"proposal.json", "validation.json"}
    if "proposal.json" not in names or not names <= allowed:
        raise SecureInputError("unexpected_proposal_contents")
    _validate_optional_legacy_report(directory_fd)


def load_canonical_proposal(proposal_id):
    validate_proposal_id(proposal_id)
    descriptor = _descend(
        _open_project_root(), ("runtime", "director", "proposals", proposal_id)
    )
    try:
        _proposal_contents(descriptor)
        raw, unused_info = _read_regular(descriptor, "proposal.json", MAX_PROPOSAL_BYTES)
        try:
            proposal = strict_json_loads(raw)
        except Exception as exc:
            raise SecureInputError("invalid_proposal_json") from exc
        if not isinstance(proposal, dict) or proposal.get("proposal_id") != proposal_id:
            raise SecureInputError("proposal_identity_mismatch")
        try:
            version = proposal.get("schema_version")
            if version == 1:
                proposal = migrate_v1_to_v2(proposal)
            elif version == 2:
                validate_schema(proposal)
            else:
                raise SecureInputError("unsupported_proposal_schema")
        except SecureInputError:
            raise
        except Exception as exc:
            raise SecureInputError("proposal_validation_failed") from exc
        return proposal
    finally:
        os.close(descriptor)


def load_canonical_policy():
    descriptor = _descend(_open_project_root(), ("director_conf",))
    try:
        raw, unused_info = _read_regular(descriptor, CANONICAL_POLICY_NAME, MAX_POLICY_BYTES)
        try:
            policy = strict_json_loads(raw)
            return validate_policy_document(policy)
        except Exception as exc:
            raise SecureInputError("policy_validation_failed") from exc
    finally:
        os.close(descriptor)
