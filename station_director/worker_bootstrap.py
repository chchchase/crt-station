"""Pre-native-import attestation and pure staged-input finalization."""

import copy
import hashlib
import json
import os
import shutil
import stat
from pathlib import Path

from station_director.isolation_probe import build_probe_payload
from station_director.isolation import validate_probe_payload
from station_director.path_safety import map_station_config
from station_director.preservation import (
    capture_media_manifest,
    fingerprint_and_clone_database,
    fingerprint_database,
    fingerprint_json_files,
    protected_json_paths,
)
from station_director.validation import project_configuration
from station_director.validation_context import (
    derive_validation_context,
    logical_configuration_values_fingerprint,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
    verify_validation_context,
)


SCHEDULING_MAIN_CONFIG_KEYS = {
    "day_parts", "custom_holidays", "normalize_titles", "title_patterns"
}


class BootstrapError(RuntimeError):
    def __init__(self, message, *, phase="snapshot", code=None, probe=None,
                 fingerprint_category=None):
        super().__init__(message)
        self.phase = phase
        self.code = code
        self.probe = probe
        self.fingerprint_category = fingerprint_category


def _classified(code, operation, *, phase="snapshot", fingerprint_category=None):
    try:
        return operation()
    except BootstrapError as exc:
        if exc.code is None:
            exc.code = code
            exc.phase = phase
            exc.fingerprint_category = fingerprint_category
        raise
    except Exception as exc:
        raise BootstrapError(
            "classified worker verification failure", phase=phase, code=code,
            fingerprint_category=fingerprint_category) from exc


def scheduling_main_config(source_main):
    """Copy only main-config fields used by native catalog/scheduling paths."""
    if not isinstance(source_main, dict):
        raise BootstrapError("main_config must be an object", phase="configuration")
    main = {
        key: copy.deepcopy(source_main[key])
        for key in SCHEDULING_MAIN_CONFIG_KEYS if key in source_main
    }
    if (
        ("day_parts" in main and not isinstance(main["day_parts"], dict))
        or ("custom_holidays" in main and not isinstance(main["custom_holidays"], dict))
        or ("normalize_titles" in main and not isinstance(main["normalize_titles"], bool))
        or ("title_patterns" in main and not isinstance(main["title_patterns"], list))
    ):
        raise BootstrapError(
            "scheduling main_config fields have invalid types", phase="configuration"
        )
    main["db_path"] = "runtime/fs42_fluid.db"
    return main


def _load_json(path, expected_identity=None):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or expected_identity is not None
            and (before.st_dev, before.st_ino) != expected_identity
        ):
            raise BootstrapError(f"input is not a single-link regular file: {path}")
        with os.fdopen(os.dup(descriptor), "r", encoding="utf-8") as handle:
            value = json.load(handle)
        after = os.fstat(descriptor)
        current = Path(path).lstat()
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
             before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                after.st_ctime_ns)
            or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise BootstrapError(f"input changed while it was being read: {path}")
        return value
    finally:
        os.close(descriptor)


def _write_private_json(path, value):
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view):]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _configuration_inventory(source):
    """Inspect configuration names and identities once without following links."""
    confs = Path(source) / "confs"
    inventory = {}
    folded = {}
    with os.scandir(confs) as entries:
        for entry in entries:
            name = entry.name
            folded_name = name.casefold()
            if folded_name == "main_config.json" and name != "main_config.json":
                raise BootstrapError("main_config.json has a case-confusable name")
            if not name.endswith(".json"):
                continue
            if folded_name in folded:
                raise BootstrapError("configuration inventory has case-confusable names")
            info = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise BootstrapError(
                    f"configuration is not a single-link regular file: {name}"
                )
            folded[folded_name] = name
            inventory[name] = (confs / name, (info.st_dev, info.st_ino))
    return inventory


def _source_configurations(source, inventory=None):
    if inventory is None:
        inventory = _configuration_inventory(source)
    configs = {}
    filenames = {}
    for filename in sorted(inventory):
        if filename == "main_config.json":
            continue
        path, identity = inventory[filename]
        value = _load_json(path, identity)
        name = value.get("station_conf", {}).get("network_name")
        if not isinstance(name, str) or not name or name in configs:
            raise BootstrapError(f"invalid or duplicate station in {filename}")
        configs[name] = value
        filenames[name] = filename
    return configs, filenames


def projected_configuration_documents(source, request, media_root, work):
    """Derive the exact JSON documents native StationManager will read."""
    inventory = _configuration_inventory(source)
    original, filenames = _source_configurations(source, inventory)
    projected, affected, source_channels = project_configuration(
        original, request["proposal"], request["policy"]
    )
    by_name = {item["name"]: item["number"] for item in request["policy"]["channels"]}
    unknown = sorted(set(projected) - set(by_name))
    if unknown:
        raise BootstrapError("projected configuration has unknown channels: " + ", ".join(unknown))
    documents = {}
    mappings = []
    for name in sorted(projected, key=lambda item: by_name[item]):
        mapped, rows = map_station_config(
            projected[name], filenames[name], sandbox_media_root=media_root,
            stage_root=work,
        )
        documents[f"confs/{filenames[name]}"] = mapped
        mappings.extend(rows)
    main_entry = inventory.get("main_config.json")
    if main_entry is not None:
        main_path, main_identity = main_entry
        documents["confs/main_config.json"] = scheduling_main_config(
            _load_json(main_path, main_identity)
        )
    return documents, tuple(sorted(affected, key=lambda item: by_name[item])), source_channels, len(mappings)


class HeldSchedulingInputs:
    """Hold and continuously identify the exact native input paths."""

    def __init__(self, paths, mutable_identity):
        self._items = []
        self._mutable_identity = Path(mutable_identity)
        try:
            for path in paths:
                path = Path(path)
                descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    os.close(descriptor)
                    raise BootstrapError(f"verified input is not a single-link regular file: {path}")
                digest = hashlib.sha256()
                while True:
                    chunk = os.read(descriptor, 65536)
                    if not chunk:
                        break
                    digest.update(chunk)
                os.lseek(descriptor, 0, os.SEEK_SET)
                self._items.append((path, descriptor, (info.st_dev, info.st_ino), digest.digest()))
        except Exception:
            self.close()
            raise

    def assert_ready(self):
        for path, descriptor, identity, digest in self._items:
            held = os.fstat(descriptor)
            current = path.lstat()
            if (
                not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                or (current.st_dev, current.st_ino) != identity
                or (held.st_dev, held.st_ino) != identity
            ):
                raise BootstrapError(f"verified scheduling input was replaced: {path}")
            os.lseek(descriptor, 0, os.SEEK_SET)
            actual = hashlib.sha256()
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                actual.update(chunk)
            if path != self._mutable_identity and actual.digest() != digest:
                raise BootstrapError(f"verified scheduling input was modified: {path}")

    def close(self):
        for unused_path, descriptor, unused_identity, unused_digest in getattr(self, "_items", []):
            try:
                os.close(descriptor)
            except OSError:
                pass
        self._items = []


def finalize_work_tree(request, stage_root, media_root, project_root):
    """Publish, fingerprint, and hold final native inputs before fs42 import."""
    stage = Path(stage_root)
    source = stage / "source"
    work = stage / "work"
    confs = work / "confs"
    runtime = work / "runtime"
    confs.mkdir(mode=0o700, parents=True, exist_ok=False)
    runtime.mkdir(mode=0o700, exist_ok=False)
    documents, affected, source_channels, mapping_count = _classified(
        "projected_configuration_verification_failed",
        lambda: projected_configuration_documents(source, request, media_root, work),
        fingerprint_category="projected_logical_configuration")
    if not affected:
        raise BootstrapError("native scheduling requires at least one affected channel", phase="configuration")
    expected_channels = tuple(item["name"] for item in request["affected_channels"])
    if affected != expected_channels:
        raise BootstrapError("affected channel set differs from the canonical request")
    for identity, value in documents.items():
        target = work / identity
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write_private_json(target, value)
    schema_dir = work / "fs42"
    schema_dir.mkdir(mode=0o700)
    schema_target = schema_dir / "station_config_schema.json"
    source_schema = Path(project_root) / "fs42/station_config_schema.json"
    descriptor = os.open(schema_target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with source_schema.open("rb") as source_handle, os.fdopen(os.dup(descriptor), "wb") as target:
            shutil.copyfileobj(source_handle, target)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    working_database = runtime / "fs42_fluid.db"
    cloned = _classified(
        "working_database_verification_failed",
        lambda: fingerprint_and_clone_database(
            source / "runtime/fs42_fluid.db", working_database),
        fingerprint_category="working_logical_database")
    os.chmod(working_database, 0o600)
    actual_projected = _classified(
        "projected_configuration_verification_failed",
        lambda: logical_protected_configuration_fingerprint(
            {identity: work / identity for identity in documents}),
        fingerprint_category="projected_logical_configuration")
    expected_projected = logical_configuration_values_fingerprint(documents)
    if actual_projected != expected_projected:
        raise BootstrapError(
            "published projection differs from deterministic transformation",
            code="projected_configuration_verification_failed",
            fingerprint_category="projected_logical_configuration")
    source_database = _classified(
        "original_database_verification_failed",
        lambda: fingerprint_database(source / "runtime/fs42_fluid.db")["logical"],
        fingerprint_category="original_logical_database")
    if cloned["logical"]["digest"] != source_database["digest"]:
        raise BootstrapError(
            "working database differs from verified source database",
            code="working_database_verification_failed",
            fingerprint_category="working_logical_database")
    held = HeldSchedulingInputs(
        [work / identity for identity in sorted(documents)] + [schema_target, working_database],
        working_database,
    )
    return {
        "projected_configuration_fingerprint": actual_projected["digest"],
        "working_database_fingerprint": cloned["logical"]["digest"],
        "affected_channels": list(affected),
        "source_channels": sorted(source_channels),
        "mapping_count": mapping_count,
    }, held


_ATTESTATION_TOKEN = object()


class VerifiedWorkerAttestation:
    __slots__ = ("run_id", "request_digest", "snapshot", "inputs", "checkpoint", "_active")

    def __init__(self, token, request, snapshot, inputs, checkpoint=None):
        if token is not _ATTESTATION_TOKEN:
            raise BootstrapError("worker attestation cannot be constructed directly")
        self.run_id = request["run_id"]
        self.request_digest = request["request_digest"]
        self.snapshot = snapshot
        self.inputs = inputs
        self.checkpoint = checkpoint
        self._active = True

    def verify(self, request):
        if (
            not self._active or self.run_id != request.get("run_id")
            or self.request_digest != request.get("request_digest")
        ):
            raise BootstrapError("worker attestation is absent, expired, or mismatched")
        self.inputs.assert_ready()

    def invalidate(self):
        self._active = False
        self.inputs.close()


def verify_original_snapshot(request, stage_root, media_root, project_root):
    source = Path(stage_root) / "source"
    protected = protected_json_paths(source)
    protected["runtime/fs42_fluid.db"] = source / "runtime/fs42_fluid.db"
    for identity, path in protected.items():
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
            or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise BootstrapError(f"staged {identity} is not a private regular file")
    configuration = _classified(
        "original_configuration_verification_failed",
        lambda: logical_protected_configuration_fingerprint(protected_json_paths(source)),
        fingerprint_category="original_logical_configuration")
    database = _classified(
        "original_database_verification_failed",
        lambda: fingerprint_database(source / "runtime/fs42_fluid.db")["logical"],
        fingerprint_category="original_logical_database")
    staged_physical = _classified(
        "physical_transition_verification_failed",
        lambda: fingerprint_json_files(protected_json_paths(source))["digest"],
        fingerprint_category="staged_source_physical_configuration")
    live_physical = _classified(
        "physical_transition_verification_failed",
        lambda: fingerprint_json_files(protected_json_paths(project_root))["digest"],
        fingerprint_category="live_physical_configuration")
    manifest = _classified(
        "media_manifest_verification_failed",
        lambda: capture_media_manifest(media_root, spool_directory=Path(stage_root)),
        fingerprint_category="logical_media_manifest")
    try:
        media = _classified(
            "media_manifest_verification_failed",
            lambda: logical_media_manifest_fingerprint(manifest),
            fingerprint_category="logical_media_manifest")
    finally:
        try:
            manifest.close()
        except Exception as exc:
            raise BootstrapError(
                "media manifest verification failed", code="media_manifest_verification_failed",
                fingerprint_category="logical_media_manifest") from exc
    actual = {
        "original_logical_configuration_fingerprint": configuration["digest"],
        "original_logical_database_fingerprint": database["digest"],
        "logical_media_manifest_fingerprint": media["digest"],
        "live_physical_configuration_fingerprint": live_physical,
        "staged_source_physical_configuration_fingerprint": staged_physical,
    }
    if actual != request["input_fingerprints"]:
        changed = sorted(key for key in actual if actual[key] != request["input_fingerprints"].get(key))
        categories = {
            "original_logical_configuration_fingerprint": ("original_configuration_verification_failed", "original_logical_configuration"),
            "original_logical_database_fingerprint": ("original_database_verification_failed", "original_logical_database"),
            "logical_media_manifest_fingerprint": ("media_manifest_verification_failed", "logical_media_manifest"),
            "live_physical_configuration_fingerprint": ("physical_transition_verification_failed", "live_physical_configuration"),
            "staged_source_physical_configuration_fingerprint": ("physical_transition_verification_failed", "staged_source_physical_configuration"),
        }
        code, category = categories[changed[0]]
        raise BootstrapError("original snapshot fingerprint mismatch", code=code,
                             fingerprint_category=category)
    seed_actual = {
        "logical_protected_configuration_fingerprint": configuration["digest"],
        "logical_database_fingerprint": database["digest"],
        "logical_media_manifest_fingerprint": media["digest"],
    }
    if seed_actual != request["seed_inputs"]:
        raise BootstrapError("seed inputs differ from verified original snapshot",
                             phase="seed", code="effective_seed_verification_failed")
    return actual


def attest_before_native_import(request, stage_root, media_root, project_root,
                                checkpoint=None):
    probes = build_probe_payload(request["run_id"], stage_root=stage_root, stage_tmp=True)
    normalized, probe_error = validate_probe_payload(probes, request["run_id"])
    if probe_error or not all(item["passed"] for item in normalized.values()):
        failed = next((name for name in sorted(normalized)
                       if not normalized[name]["passed"]), None)
        raise BootstrapError(
            "isolation probe attestation failed", phase="probes",
            code="isolation_probe_failed", probe=failed,
        )
    if checkpoint is not None:
        checkpoint.publish("probes_passed")
    try:
        original = _classified(
            "original_configuration_verification_failed",
            lambda: verify_original_snapshot(request, stage_root, media_root, project_root),
            fingerprint_category="original_logical_configuration")
        projected, inputs = _classified(
            "projected_configuration_verification_failed",
            lambda: finalize_work_tree(request, stage_root, media_root, project_root),
            fingerprint_category="projected_logical_configuration")
    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError("staged snapshot verification failed",
                             code="projected_configuration_verification_failed",
                             fingerprint_category="projected_logical_configuration") from exc
    if checkpoint is not None:
        checkpoint.publish("snapshot_verified")
    try:
        try:
            verify_validation_context(
                request["proposal"], request["policy"], request["seed_inputs"],
                request["validation_context"],
            )
        except Exception as exc:
            try:
                expected_context = derive_validation_context(
                    request["proposal"], request["policy"], request["seed_inputs"])
                code = ("effective_seed_verification_failed"
                        if request["validation_context"].get("effective_seed")
                        != expected_context.get("effective_seed")
                        else "validation_context_failed")
            except Exception:
                code = "validation_context_failed"
            raise BootstrapError("validation context verification failed", phase="seed",
                                 code=code) from exc
        context = request["validation_context"]
        if os.environ.get("TZ") != context["timezone"]:
            raise BootstrapError("worker timezone does not match validation context", phase="seed",
                                 code="timezone_verification_failed")
        if os.environ.get("PYTHONHASHSEED") != context["python_hash_seed"]:
            raise BootstrapError("worker hash seed does not match validation context", phase="seed",
                                 code="hash_seed_verification_failed")
    except Exception:
        inputs.close()
        raise
    if checkpoint is not None:
        checkpoint.publish("seed_verified")
    snapshot = {**original, **projected}
    return probes, VerifiedWorkerAttestation(
        _ATTESTATION_TOKEN, request, snapshot, inputs, checkpoint)
