"""Private C2 orchestration for two independently isolated C1 executions."""

import json
import os
import re
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from station_director.isolation import (
    STAGING_PARENT,
    check_invocation_context,
    cleanup_staging_directory,
    create_staging_directory,
)
from station_director.preservation import (
    MAX_PROTECTED_JSON_FILES,
    MAX_PROTECTED_JSON_FILE_BYTES,
    MAX_PROTECTED_JSON_TOTAL_BYTES,
    capture_media_manifest,
    compare_media_manifests,
    fingerprint_and_clone_database_targets,
    fingerprint_database,
    fingerprint_json_files,
    protected_json_paths,
)
from station_director.schedule_normalization import (
    NormalizationError,
    compare_normalized_runs,
    normalize_completed_run,
)
from station_director.single_run import (
    SingleRunError,
    _finalize_prepared_single_run,
    inspect_single_run,
    launch_single_run,
)
from station_director.single_run_protocol import validate_document
from station_director.validation_context import (
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
    proposal_boundary_to_db,
)


COMPARISON_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,100}\Z")
MINIMUM_FREE_BYTES = 512 * 1024 * 1024
PER_RUN_TIMEOUT_SECONDS = 30 * 60
TOTAL_TIMEOUT_SECONDS = 75 * 60
RESULT_SCHEMA = Path(__file__).with_name("schemas") / "native-dual-run.result.v1.schema.json"


class DualRunError(RuntimeError):
    def __init__(self, phase, code, message, *, category=None):
        super().__init__(message)
        self.phase = phase
        self.code = code
        self.category = category


def _bounded_detail(value, *private_paths):
    text = str(value)
    for path in private_paths:
        if path is not None:
            text = text.replace(str(path), "<private-path>")
    text = re.sub(r"/tmp/fs42-i-[0-9a-f]{12}", "<stage>", text)
    return text[:1000]


@dataclass
class SharedCapture:
    configuration_digest: str
    database_digest: str
    media_logical_digest: str
    configuration_physical: dict
    database_fingerprint: dict
    media_manifest: object
    media_logical: dict
    required_free_bytes: int
    spool_directory: Path
    closed: bool = False

    def close(self):
        if not self.closed:
            self.media_manifest.close()
            self.closed = True


@dataclass
class DualRunScope:
    lifecycles: list
    capture: SharedCapture
    cleanup_results: list

    stages_cleaned: bool = False

    def cleanup_stages(self):
        if self.stages_cleaned:
            return
        failures = []
        for index, lifecycle in reversed(list(enumerate(self.lifecycles, 1))):
            try:
                result = lifecycle.cleanup()
                self.cleanup_results.append({
                    "run": index, "passed": result["passed"],
                    "detail": _bounded_detail(result["detail"], lifecycle.stage),
                })
            except Exception as exc:
                self.cleanup_results.append({
                    "run": index, "passed": False,
                    "detail": _bounded_detail(exc, lifecycle.stage),
                })
                failures.append(f"run {index}: {exc}")
        self.stages_cleaned = True
        if failures:
            raise DualRunError(
                "cleanup", "cleanup_failed", _bounded_detail("; ".join(failures))
            )

    def close_capture(self):
        try:
            self.capture.close()
        except Exception as exc:
            raise DualRunError(
                "cleanup", "cleanup_failed", f"shared capture close failed: {type(exc).__name__}"
            ) from exc

    def cleanup(self):
        primary = None
        try:
            self.cleanup_stages()
        except Exception as exc:
            primary = exc
        try:
            self.close_capture()
        except Exception as exc:
            if primary is None:
                primary = exc
        if primary is not None:
            raise primary


def _write_private_bytes(path, raw):
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view):]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _captured_configuration_bytes(root):
    values = {}
    paths = protected_json_paths(root)
    if len(paths) > MAX_PROTECTED_JSON_FILES:
        raise DualRunError("capture", "oversized_config", "protected file-count limit exceeded")
    aggregate = 0
    for identity, path in paths.items():
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise DualRunError("capture", "unsafe_source", f"unsafe protected file: {identity}")
            chunks = []
            total = 0
            while True:
                chunk = os.read(
                    descriptor,
                    min(65536, MAX_PROTECTED_JSON_FILE_BYTES + 1 - total),
                )
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_PROTECTED_JSON_FILE_BYTES:
                    raise DualRunError("capture", "oversized_config", identity)
            raw = b"".join(chunks)
            aggregate += len(raw)
            if aggregate > MAX_PROTECTED_JSON_TOTAL_BYTES:
                raise DualRunError(
                    "capture", "oversized_config", "protected aggregate limit exceeded"
                )
            json.loads(raw)
            values[identity] = raw
        finally:
            os.close(descriptor)
    return values


def _publish_sources(stages, configuration_bytes):
    database_targets = []
    for stage in stages:
        source = stage / "source"
        (source / "confs").mkdir(mode=0o700, parents=True)
        (source / "runtime").mkdir(mode=0o700)
        for identity, raw in configuration_bytes.items():
            target = source / identity
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _write_private_bytes(target, raw)
        database_targets.append(source / "runtime/fs42_fluid.db")
    return database_targets


def _space_requirement(source_database, config_size, manifest_size=0):
    info = Path(source_database).stat()
    allocated = max(info.st_size, getattr(info, "st_blocks", 0) * 512)
    # Four database copies, WAL/temp growth, two canonical streams, and room to
    # retain both stages if cleanup must quarantine them.
    return max(
        MINIMUM_FREE_BYTES,
        allocated * 8 + config_size * 4 + manifest_size * 4 + 64 * 1024 * 1024,
    )


def _require_space(required):
    free = shutil.disk_usage(STAGING_PARENT).free
    if free < required:
        raise DualRunError(
            "capture", "insufficient_space",
            f"C2 requires {required} free bytes but only {free} are available",
        )


def _capture_shared_inputs(source_root, media_root, stages):
    config_paths = protected_json_paths(source_root)
    physical_before = fingerprint_json_files(config_paths)
    logical_before = logical_protected_configuration_fingerprint(config_paths)
    configuration_bytes = _captured_configuration_bytes(source_root)
    config_size = sum(len(raw) for raw in configuration_bytes.values())
    source_database = Path(source_root) / "runtime/fs42_fluid.db"
    _require_space(_space_requirement(source_database, config_size))
    targets = _publish_sources(stages, configuration_bytes)
    database = fingerprint_and_clone_database_targets(source_database, targets)
    for target in targets:
        os.chmod(target, 0o600)
    for backup in database["backups"]:
        if backup["logical"]["digest"] != database["logical"]["digest"]:
            raise DualRunError("capture", "backup_mismatch", "database backups differ from pinned source")
    manifest = capture_media_manifest(media_root, spool_directory=stages[0])
    try:
        logical_media = logical_media_manifest_fingerprint(manifest)
        manifest_position = manifest.stream.tell()
        manifest.stream.seek(0, os.SEEK_END)
        manifest_size = manifest.stream.tell()
        manifest.stream.seek(manifest_position)
        required = _space_requirement(source_database, config_size, manifest_size)
        _require_space(required)
        capture = SharedCapture(
            logical_before["digest"], database["logical"]["digest"],
            logical_media["digest"], physical_before, database, manifest,
            logical_media, required, Path(stages[0]),
        )
        _assert_inputs_stable(source_root, media_root, capture, "after_capture")
    except Exception:
        manifest.close()
        raise
    return capture


def _assert_inputs_stable(source_root, media_root, capture, checkpoint):
    changed = []
    def checked(category, operation):
        try:
            return operation()
        except Exception as exc:
            raise DualRunError(
                checkpoint, "input_changed",
                f"{category} could not be fingerprinted ({type(exc).__name__})",
                category=[category],
            ) from exc

    try:
        physical = fingerprint_json_files(protected_json_paths(source_root))
    except Exception as exc:
        category = (
            "logical_configuration"
            if isinstance(exc, (ValueError, json.JSONDecodeError))
            or "JSON is invalid" in str(exc)
            else "physical_configuration"
        )
        raise DualRunError(
            checkpoint, "input_changed",
            f"{category} could not be fingerprinted ({type(exc).__name__})",
            category=[category],
        ) from exc
    if physical["digest"] != capture.configuration_physical["digest"]:
        changed.append("physical_configuration")
    logical_config = checked(
        "logical_configuration",
        lambda: logical_protected_configuration_fingerprint(
            protected_json_paths(source_root)
        ),
    )
    if logical_config["digest"] != capture.configuration_digest:
        changed.append("logical_configuration")
    database = checked(
        "logical_database",
        lambda: fingerprint_database(Path(source_root) / "runtime/fs42_fluid.db"),
    )
    if database["logical"]["digest"] != capture.database_digest:
        changed.append("logical_database")
    current_manifest = checked(
        "physical_media_metadata",
        lambda: capture_media_manifest(
            media_root,
            spool_directory=(
                capture.spool_directory
                if capture.spool_directory.is_dir() else STAGING_PARENT
            ),
        ),
    )
    try:
        comparison = checked(
            "physical_media_metadata",
            lambda: compare_media_manifests(capture.media_manifest, current_manifest),
        )
        current_logical = checked(
            "logical_media",
            lambda: logical_media_manifest_fingerprint(current_manifest),
        )
    finally:
        current_manifest.close()
    if not comparison["preserved"]:
        changed.append("physical_media_metadata")
    if current_logical["digest"] != capture.media_logical_digest:
        changed.append("logical_media")
    if changed:
        raise DualRunError(
            checkpoint, "input_changed",
            f"validation inputs changed at {checkpoint}: {', '.join(changed)}",
            category=changed,
        )
    return {"checkpoint": checkpoint, "passed": True, "changed_categories": []}


def _prepare_scope(project_root, source_root, media_root, proposal, policy, comparison_id):
    if not COMPARISON_ID_RE.fullmatch(comparison_id):
        raise DualRunError("capture", "invalid_comparison_id", "invalid comparison ID")
    project_root = Path(project_root).resolve()
    source_root = Path(source_root).resolve()
    if project_root != source_root:
        raise DualRunError("capture", "source_root_mismatch", "source root must be project root")
    allowed, detail = check_invocation_context()
    if not allowed:
        raise DualRunError("capture", "invocation_context_rejected", detail)
    created = []
    capture = None
    try:
        for unused in range(2):
            token, stage, lock = create_staging_directory()
            created.append((token, stage, lock))
        capture = _capture_shared_inputs(
            source_root, Path(media_root).resolve(), [item[1] for item in created]
        )
        physical_live = capture.configuration_physical["digest"]
        lifecycles = []
        for index, (token, stage, lock) in enumerate(created, 1):
            lifecycle = _finalize_prepared_single_run(
                project_root, stage, lock, token,
                f"{comparison_id}.run-{index}", proposal, policy,
                configuration_digest=capture.configuration_digest,
                database_digest=capture.database_digest,
                media_digest=capture.media_logical_digest,
                live_physical_digest=physical_live,
            )
            lifecycles.append(lifecycle)
        contexts = [item.request["validation_context"] for item in lifecycles]
        if contexts[0] != contexts[1]:
            raise DualRunError("capture", "context_mismatch", "run contexts differ")
        return DualRunScope(lifecycles, capture, [])
    except Exception as primary:
        cleanup_failures = []
        if capture is not None:
            try:
                capture.close()
            except Exception as exc:
                cleanup_failures.append(f"shared capture: {exc}")
        for unused_token, stage, lock in reversed(created):
            lock.close()
            if stage.exists():
                cleaned, detail = cleanup_staging_directory(stage)
                if not cleaned:
                    cleanup_failures.append(
                        "staged run: " + _bounded_detail(detail, stage)
                    )
        if cleanup_failures:
            raise DualRunError(
                "cleanup", "cleanup_failed",
                _bounded_detail(
                    f"preparation failed ({type(primary).__name__}); cleanup failed: "
                    + "; ".join(cleanup_failures),
                    *(item[1] for item in created),
                ),
            ) from primary
        raise


def _base_result(comparison_id):
    return {
        "schema_version": 1,
        "operation": "native_dual_run_comparison",
        "comparison_id": comparison_id,
        "status": "failed",
        "phase_reached": "capture",
        "scheduler_invoked": {"run_1": False, "run_2": False},
        "validation_context": {},
        "affected_channels": [],
        "source_checks": [],
        "runs": [],
        "reproducibility": None,
        "failure": None,
        "cleanup": [],
        "timings_ms": {"total": 0},
    }


def _validate_result_semantics(result):
    """Enforce outcome relationships that JSON Schema cannot express safely."""
    if result["status"] == "success":
        if result["phase_reached"] != "complete" or result["failure"] is not None:
            raise DualRunError("protocol", "invalid_result", "incoherent C2 success")
        if result["scheduler_invoked"] != {"run_1": True, "run_2": True}:
            raise DualRunError("protocol", "invalid_result", "successful C2 did not run both schedulers")
        if [item["index"] for item in result["runs"]] != [1, 2]:
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks both ordered runs")
        if [item["checkpoint"] for item in result["source_checks"]] != [
            "after_capture", "between_runs", "after_run_2", "before_success"
        ] or not all(item["passed"] for item in result["source_checks"]):
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks source attestations")
        if result["reproducibility"] is None or not result["reproducibility"]["passed"]:
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks reproducibility proof")
        if [item["run"] for item in result["cleanup"]] != [2, 1] or not all(
            item["passed"] for item in result["cleanup"]
        ):
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks cleanup proof")
    elif result["failure"] is None:
        raise DualRunError("protocol", "invalid_result", "failed C2 result omits failure")


def run_dual_comparison(
    project_root, source_root, media_root, proposal, policy, comparison_id,
):
    """Execute C2 internally. This function is intentionally not CLI-routed."""
    started = time.monotonic()
    deadline = started + TOTAL_TIMEOUT_SECONDS
    result = _base_result(comparison_id)
    scope = None
    comparison_succeeded = False
    try:
        scope = _prepare_scope(
            project_root, source_root, media_root, proposal, policy, comparison_id
        )
        result["validation_context"] = {
            key: scope.lifecycles[0].request["validation_context"][key]
            for key in ("input_fingerprint", "requested_seed", "effective_seed")
        }
        result["affected_channels"] = scope.lifecycles[0].request["affected_channels"]
        result["source_checks"].append(
            {"checkpoint": "after_capture", "passed": True, "changed_categories": []}
        )
        normalized = []
        for index, lifecycle in enumerate(scope.lifecycles, 1):
            result["phase_reached"] = f"run_{index}"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DualRunError(
                    f"run_{index}", "c1_run_failed", "C2 total timeout expired",
                    category="total_timeout",
                )
            try:
                launch_single_run(
                    lifecycle, timeout=max(1, min(PER_RUN_TIMEOUT_SECONDS, int(remaining)))
                )
                response = inspect_single_run(lifecycle)
            except Exception as exc:
                raise DualRunError(
                    f"run_{index}", "c1_run_failed",
                    f"C1 run {index} failed ({type(exc).__name__})",
                    category=(getattr(exc, "category", None)
                              or getattr(exc, "code", None)),
                ) from exc
            result["scheduler_invoked"][f"run_{index}"] = response["scheduler_invoked"]
            if response["status"] != "success":
                raise DualRunError(
                    f"run_{index}", "c1_run_failed",
                    f"C1 run {index} returned failure",
                    category=(response.get("failure") or {}).get("code"),
                )
            try:
                lifecycle.settle_unit()
            except Exception as exc:
                raise DualRunError(
                    "cleanup", "cleanup_failed",
                    f"run {index} unit could not be proven absent",
                    category=f"run_{index}",
                ) from exc
            if index == 1:
                result["source_checks"].append(
                    _assert_inputs_stable(source_root, media_root, scope.capture, "between_runs")
                )
            else:
                result["source_checks"].append(
                    _assert_inputs_stable(source_root, media_root, scope.capture, "after_run_2")
                )
            try:
                normalized_run = normalize_completed_run(
                    lifecycle.stage,
                    lifecycle.stage / "source/runtime/fs42_fluid.db",
                    response,
                    proposal_boundary_to_db(proposal["week_start"], "week_start"),
                )
            except Exception as exc:
                raise DualRunError(
                    "normalization", "normalization_failed",
                    f"run {index} normalization failed ({type(exc).__name__})",
                    category=f"run_{index}",
                ) from exc
            normalized.append(normalized_run)
            result["runs"].append({
                "index": index,
                "run_id": lifecycle.run_id,
                "status": response["status"],
                "normalization_digest": normalized_run.digest,
                "record_count": normalized_run.record_count,
                "provisional_catalog_count": normalized_run.provisional_count,
                "channels": response["channels"],
                "guide_validation": response["guide_validation"],
                "warnings": response["warnings"],
                "timings_ms": response["timings_ms"],
            })
        result["phase_reached"] = "comparison"
        if time.monotonic() >= deadline:
            raise DualRunError(
                "comparison", "comparison_failed", "C2 total timeout expired",
                category="total_timeout",
            )
        try:
            comparison = compare_normalized_runs(normalized[0], normalized[1])
        except Exception as exc:
            raise DualRunError(
                "comparison", "comparison_failed",
                f"structural comparison failed ({type(exc).__name__})",
            ) from exc
        result["reproducibility"] = comparison
        if not comparison["passed"]:
            guide_digests = [
                item["guide_validation"]["digest"] for item in result["runs"]
            ]
            guide_difference = len(set(guide_digests)) != 1 or any(
                item["field_path"].startswith("/guide/")
                or "/guide_validation" in item["field_path"]
                for item in comparison["differences"]
            )
            raise DualRunError(
                "comparison",
                ("guide_reproducibility_mismatch" if guide_difference
                 else "reproducibility_mismatch"),
                ("normalized guide outputs differ" if guide_difference
                 else "normalized native runs differ"),
            )
        comparison_succeeded = True
    except Exception as exc:
        result["status"] = "failed"
        result["phase_reached"] = getattr(exc, "phase", result["phase_reached"])
        fallback_code = {
            "capture": "source_capture_failed",
            "run_1": "c1_run_failed",
            "run_2": "c1_run_failed",
            "normalization": "normalization_failed",
            "comparison": "comparison_failed",
            "cleanup": "cleanup_failed",
        }.get(result["phase_reached"], "comparison_failed")
        code = getattr(exc, "code", fallback_code)
        if code == "cleanup_failure":
            code = "cleanup_failed"
        result["failure"] = {
            "phase": result["phase_reached"],
            "code": code,
            "category": getattr(exc, "category", None),
            "message": _bounded_detail(exc, project_root, source_root, media_root),
        }
    finally:
        if scope is not None:
            try:
                scope.cleanup_stages()
            except Exception as cleanup_error:
                if result["failure"] is None:
                    result["failure"] = {
                        "phase": "cleanup", "code": "cleanup_failed",
                        "category": None,
                        "message": f"stage cleanup failed ({type(cleanup_error).__name__})",
                    }
                result["status"] = "failed"
            result["cleanup"] = scope.cleanup_results
            # The final source check intentionally follows both stage cleanup
            # attempts and is the last possible prerequisite for success.
            try:
                final_check = _assert_inputs_stable(
                    source_root, media_root, scope.capture, "before_success"
                )
                result["source_checks"].append(final_check)
            except Exception as final_error:
                categories = getattr(final_error, "category", None) or []
                result["source_checks"].append({
                    "checkpoint": "before_success", "passed": False,
                    "changed_categories": categories,
                })
                if result["failure"] is None:
                    result["failure"] = {
                        "phase": "before_success", "code": "input_changed",
                        "category": categories,
                        "message": _bounded_detail(
                            final_error, project_root, source_root, media_root
                        ),
                    }
                result["status"] = "failed"
            try:
                scope.close_capture()
            except Exception as cleanup_error:
                if result["failure"] is None:
                    result["failure"] = {
                        "phase": "cleanup", "code": "cleanup_failed",
                        "category": None,
                        "message": f"capture cleanup failed ({type(cleanup_error).__name__})",
                    }
                result["status"] = "failed"
            if comparison_succeeded and result["failure"] is None:
                result["status"] = "success"
                result["phase_reached"] = "complete"
        result["timings_ms"]["total"] = round((time.monotonic() - started) * 1000)
    validate_document(result, RESULT_SCHEMA)
    _validate_result_semantics(result)
    return result
