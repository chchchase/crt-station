"""Private C2 orchestration for two independently isolated C1 executions."""

import hashlib
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
from station_director.schedule_comparison import (
    COMPARISON_SPOOL_RESERVE_BYTES,
    compare_baseline_summaries,
    compare_baseline_to_proposed,
)
from station_director.single_run import (
    FINALIZATION_SUBPHASES,
    SingleRunError,
    SingleRunFinalizationError,
    _finalize_prepared_single_run,
    inspect_single_run,
    launch_single_run,
)
from station_director.c1_diagnostics import launcher_summary, make_diagnostic
from station_director.single_run_protocol import validate_document
from station_director.validation_context import (
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
    proposal_boundary_to_db,
)
from station_director.validation_coordinator import (
    CONTROL_DEADLINE_SECONDS,
    EXECUTION_ADMISSION_CUTOFF_SECONDS,
    MANDATORY_FINALIZATION_RESERVE_SECONDS as FINALIZATION_RESERVE_SECONDS,
)


COMPARISON_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,100}\Z")
MINIMUM_FREE_BYTES = 512 * 1024 * 1024
PER_RUN_TIMEOUT_SECONDS = 2850
UNIT_HARD_LIMIT_SECONDS = 2700
OUTER_WATCHDOG_SECONDS = 2730
UNIT_INSPECTION_SECONDS = 15
RUN_CLEANUP_SECONDS = 95
INTERNAL_PHASE_BUDGET_SECONDS = 2520
INTERNAL_OVERHEAD_SECONDS = 120
CAPTURE_ALLOWANCE_SECONDS = 360
NORMALIZATION_ALLOWANCE_SECONDS = 120
BASELINE_COMPARISON_ALLOWANCE_SECONDS = 120
COMPARISON_ALLOWANCE_SECONDS = 120
ORDINARY_NON_RUN_ALLOWANCE_SECONDS = (
    CAPTURE_ALLOWANCE_SECONDS
    + 2 * NORMALIZATION_ALLOWANCE_SECONDS
    + 2 * BASELINE_COMPARISON_ALLOWANCE_SECONDS
    + COMPARISON_ALLOWANCE_SECONDS
)
ORDINARY_WORK_ESTIMATE_SECONDS = (
    2 * PER_RUN_TIMEOUT_SECONDS + ORDINARY_NON_RUN_ALLOWANCE_SECONDS)
RESULT_SCHEMA = Path(__file__).with_name("schemas") / "native-dual-run.result.v4.schema.json"

CAPTURE_FAILURE_KINDS = frozenset({
    "source_path_resolution", "invocation_verification", "stage_allocation",
    "configuration_inventory", "configuration_physical_fingerprint",
    "configuration_logical_fingerprint", "configuration_snapshot",
    "free_space_check", "stage_source_publication", "database_snapshot",
    "database_backup_verification", "media_manifest_capture",
    "media_logical_fingerprint", "post_capture_stability",
    "capture_artifact_initialization", "single_run_finalization",
    "context_consistency",
})
SOURCE_CAPTURE_FAILURE_MESSAGE = "Source capture failed."
SINGLE_RUN_CAPTURE_MESSAGES = {
    "duplicate_stage_file": "A staged validation file already exists.",
    "proposal_has_no_effects": "Proposal has no effects eligible for schedule validation.",
    "source_changed": "A staged scheduling input changed during preparation.",
}


class DualRunError(RuntimeError):
    def __init__(self, phase, code, message, *, category=None,
                 capture_failure_kind=None, capture_run=None,
                 finalization_subphase=None, c1_diagnostic=None,
                 launcher_summary_value=None):
        super().__init__(message)
        self.phase = phase
        self.code = code
        self.category = category
        if capture_failure_kind is not None and capture_failure_kind not in CAPTURE_FAILURE_KINDS:
            raise ValueError("invalid capture failure kind")
        if (code == "source_capture_failed") != (capture_failure_kind is not None):
            raise ValueError("source capture failures require exactly one classified kind")
        self.capture_failure_kind = capture_failure_kind
        if capture_run not in (None, 1, 2):
            raise ValueError("invalid capture run")
        if (finalization_subphase is not None
                and finalization_subphase not in FINALIZATION_SUBPHASES):
            raise ValueError("invalid finalization subphase")
        self.capture_run = capture_run
        self.finalization_subphase = finalization_subphase
        self.c1_diagnostic = c1_diagnostic
        self.launcher_summary = launcher_summary_value


def _raise_if_cancelled(exc):
    if isinstance(exc, KeyboardInterrupt) or getattr(exc, "is_validation_cancellation", False):
        raise exc


def _bounded_detail(value, *private_paths):
    text = str(value)
    for path in private_paths:
        if path is not None:
            text = text.replace(str(path), "<private-path>")
    text = re.sub(r"/tmp/fs42-i-[0-9a-f]{12}", "<stage>", text)
    return text[:1000]


def _capture_step(kind, operation):
    """Run one capture subphase and classify only otherwise-uncoded failures."""
    if kind not in CAPTURE_FAILURE_KINDS:
        raise ValueError("invalid capture failure kind")
    try:
        return operation()
    except SingleRunFinalizationError as exc:
        if kind != "single_run_finalization":
            raise
        raise DualRunError(
            "capture", "source_capture_failed", SOURCE_CAPTURE_FAILURE_MESSAGE,
            capture_failure_kind=kind,
            finalization_subphase=exc.finalization_subphase,
        ) from exc
    except (DualRunError, SingleRunError):
        raise
    except Exception as exc:
        _raise_if_cancelled(exc)
        raise DualRunError(
            "capture", "source_capture_failed", SOURCE_CAPTURE_FAILURE_MESSAGE,
            capture_failure_kind=kind,
        ) from exc


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
                    "quarantined": False,
                    "detail": _bounded_detail(result["detail"], lifecycle.stage),
                })
            except Exception as exc:
                self.cleanup_results.append({
                    "run": index, "passed": False,
                    "quarantined": (lifecycle.stage / ".quarantine").exists(),
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
        allocated * 8 + config_size * 4 + manifest_size * 4
        + COMPARISON_SPOOL_RESERVE_BYTES + 2 * 1024 * 1024,
    )


def _require_space(required):
    free = shutil.disk_usage(STAGING_PARENT).free
    if free < required:
        raise DualRunError(
            "capture", "insufficient_space",
            f"C2 requires {required} free bytes but only {free} are available",
        )


def _capture_shared_inputs(source_root, media_root, stages):
    config_paths = _capture_step(
        "configuration_inventory", lambda: protected_json_paths(source_root))
    physical_before = _capture_step(
        "configuration_physical_fingerprint",
        lambda: fingerprint_json_files(config_paths),
    )
    logical_before = _capture_step(
        "configuration_logical_fingerprint",
        lambda: logical_protected_configuration_fingerprint(config_paths),
    )
    configuration_bytes = _capture_step(
        "configuration_snapshot", lambda: _captured_configuration_bytes(source_root))
    config_size, source_database = _capture_step(
        "configuration_snapshot",
        lambda: (sum(len(raw) for raw in configuration_bytes.values()),
                 Path(source_root) / "runtime/fs42_fluid.db"),
    )
    _capture_step(
        "free_space_check",
        lambda: _require_space(_space_requirement(source_database, config_size)),
    )
    targets = _capture_step(
        "stage_source_publication",
        lambda: _publish_sources(stages, configuration_bytes),
    )
    database = _capture_step(
        "database_snapshot",
        lambda: fingerprint_and_clone_database_targets(source_database, targets),
    )

    def verify_database_backups():
        for target in targets:
            os.chmod(target, 0o600)
        for backup in database["backups"]:
            if backup["logical"]["digest"] != database["logical"]["digest"]:
                raise DualRunError(
                    "capture", "backup_mismatch",
                    "database backups differ from pinned source",
                )

    _capture_step("database_backup_verification", verify_database_backups)
    manifest = _capture_step(
        "media_manifest_capture",
        lambda: capture_media_manifest(media_root, spool_directory=stages[0]),
    )
    try:
        logical_media = _capture_step(
            "media_logical_fingerprint",
            lambda: logical_media_manifest_fingerprint(manifest),
        )

        def check_final_space():
            manifest_position = manifest.stream.tell()
            manifest.stream.seek(0, os.SEEK_END)
            manifest_size = manifest.stream.tell()
            manifest.stream.seek(manifest_position)
            required_bytes = _space_requirement(
                source_database, config_size, manifest_size)
            _require_space(required_bytes)
            return required_bytes

        required = _capture_step("free_space_check", check_final_space)
        capture = _capture_step(
            "capture_artifact_initialization",
            lambda: SharedCapture(
                logical_before["digest"], database["logical"]["digest"],
                logical_media["digest"], physical_before, database, manifest,
                logical_media, required, Path(stages[0]),
            ),
        )
        _capture_step(
            "post_capture_stability",
            lambda: _assert_inputs_stable(
                source_root, media_root, capture, "after_capture"),
        )
    except BaseException as primary:
        try:
            manifest.close()
        except BaseException:
            # Preparation owns the staged directories. Preserve the primary
            # capture failure and let its cleanup accounting record this
            # separate close failure.
            try:
                primary._manifest_close_failed = True
            except Exception:
                pass
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


def _preparation_stage_exists(stage):
    try:
        info = Path(stage).lstat()
    except FileNotFoundError:
        return False
    if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.getuid()):
        raise OSError("unsafe staged directory identity")
    return True


def _quarantine_preparation_stage(stage):
    descriptor = os.open(
        stage, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            return False
        marker = os.open(
            ".quarantine", os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=descriptor)
        os.close(marker)
        return True
    except FileExistsError:
        try:
            marker_info = os.stat(
                ".quarantine", dir_fd=descriptor, follow_symlinks=False)
        except OSError:
            return False
        return (stat.S_ISREG(marker_info.st_mode) and marker_info.st_nlink == 1
                and marker_info.st_uid == os.getuid()
                and stat.S_IMODE(marker_info.st_mode) == 0o600)
    except OSError:
        return False
    finally:
        os.close(descriptor)


def _cleanup_failed_preparation(created, capture, primary):
    """Attempt every safe cleanup and return bounded per-run status."""
    manifest_close_failed = bool(
        getattr(primary, "_manifest_close_failed", False))
    if capture is not None:
        try:
            capture.close()
        except BaseException:
            manifest_close_failed = True

    results = []
    for index, (unused_token, stage, lock) in reversed(
            list(enumerate(created, 1))):
        failures = []
        if index == 1 and manifest_close_failed:
            failures.append("capture_close_failed")
        try:
            lock.close()
        except BaseException:
            failures.append("lock_close_failed")

        stage_present = True
        try:
            stage_present = _preparation_stage_exists(stage)
        except BaseException:
            failures.append("stage_inspection_failed")

        if stage_present and "stage_inspection_failed" not in failures:
            try:
                cleaned, unused_detail = cleanup_staging_directory(stage)
            except BaseException:
                cleaned = False
            if not cleaned:
                failures.append("stage_removal_failed")
            else:
                stage_present = False

        quarantined = False
        if stage_present:
            try:
                quarantined = _quarantine_preparation_stage(stage)
            except BaseException:
                quarantined = False
            if not quarantined:
                failures.append("stage_quarantine_failed")

        results.append({
            "run": index,
            "passed": not failures,
            "quarantined": quarantined,
            "detail": "cleaned" if not failures else ",".join(failures),
        })
    return results


def _prepare_scope(project_root, source_root, media_root, proposal, policy, comparison_id):
    if not COMPARISON_ID_RE.fullmatch(comparison_id):
        raise DualRunError("capture", "invalid_comparison_id", "invalid comparison ID")
    project_root, source_root, media_root = _capture_step(
        "source_path_resolution",
        lambda: (Path(project_root).resolve(), Path(source_root).resolve(),
                 Path(media_root).resolve()),
    )
    if project_root != source_root:
        raise DualRunError("capture", "source_root_mismatch", "source root must be project root")
    allowed, detail = _capture_step(
        "invocation_verification", check_invocation_context)
    if not allowed:
        raise DualRunError("capture", "invocation_context_rejected", detail)
    created = []
    capture = None
    try:
        for unused in range(2):
            token, stage, lock = _capture_step(
                "stage_allocation", create_staging_directory)
            created.append((token, stage, lock))
        capture = _capture_shared_inputs(
            source_root, media_root, [item[1] for item in created]
        )
        physical_live = _capture_step(
            "capture_artifact_initialization",
            lambda: capture.configuration_physical["digest"],
        )
        lifecycles = []
        for index, (token, stage, lock) in enumerate(created, 1):
            try:
                lifecycle = _capture_step(
                    "single_run_finalization",
                    lambda index=index, token=token, stage=stage, lock=lock:
                    _finalize_prepared_single_run(
                        project_root, stage, lock, token,
                        f"{comparison_id}.run-{index}", proposal, policy,
                        configuration_digest=capture.configuration_digest,
                        database_digest=capture.database_digest,
                        media_digest=capture.media_logical_digest,
                        live_physical_digest=physical_live,
                    ),
                )
            except (DualRunError, SingleRunError) as exc:
                exc.capture_run = index
                raise
            lifecycles.append(lifecycle)

        def verify_contexts():
            contexts = [item.request["validation_context"] for item in lifecycles]
            if contexts[0] != contexts[1]:
                raise DualRunError("capture", "context_mismatch", "run contexts differ")

        _capture_step("context_consistency", verify_contexts)
        return DualRunScope(lifecycles, capture, [])
    except BaseException as primary:
        cleanup_results = _cleanup_failed_preparation(created, capture, primary)
        try:
            primary.preparation_cleanup = cleanup_results
        except Exception:
            pass
        raise


def _base_result(comparison_id):
    return {
        "schema_version": 4,
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
        "baseline_comparison": None,
        "failure": None,
        "cleanup": [],
        "timings_ms": {"total": 0},
    }


def _run_preservation_summary(response):
    fingerprints = response["verification"]["fingerprints"]
    encoded = json.dumps(
        fingerprints, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "retained_history": response["preservation"]["retained_history"],
        "protected_channels": response["preservation"]["protected_channels"],
        "sequence_tables_restored": response["preservation"]["sequence_tables_restored"],
        "foreign_key_baseline": response["preservation"]["foreign_key_baseline"],
        "path_validation_passed": response["path_validation"]["passed"],
        "mapping_count": response["path_validation"]["mapping_count"],
        "scheduled_path_checks": response["path_validation"]["scheduled_path_checks"],
        "fingerprint_summary_digest": hashlib.sha256(encoded).hexdigest(),
    }


def _validate_result_semantics(result):
    """Enforce outcome relationships that JSON Schema cannot express safely."""
    failure = result.get("failure")
    if failure is not None:
        c1 = failure.get("c1_diagnostic")
        launcher = failure.get("launcher_summary")
        if failure.get("code") == "c1_run_failed":
            from station_director.c1_diagnostics import validate_host_diagnostic
            if not isinstance(c1, dict) or c1.get("run") not in (1, 2):
                raise DualRunError("protocol", "invalid_result", "C1 failure lacks run identity")
            validate_host_diagnostic(c1.get("detail"))
            run_key = f"run_{c1['run']}"
            if failure.get("phase") != run_key:
                raise DualRunError("protocol", "invalid_result", "C1 run/phase mismatch")
            if c1["detail"]["scheduler_invoked"] != result["scheduler_invoked"][run_key]:
                raise DualRunError("protocol", "invalid_result", "C1 scheduler state mismatch")
            if not isinstance(launcher, dict):
                raise DualRunError("protocol", "invalid_result", "C1 failure lacks launcher summary")
        elif c1 is not None or launcher is not None:
            raise DualRunError("protocol", "invalid_result", "unexpected C1 diagnostic")
        capture_run = failure.get("capture_run")
        subphase = failure.get("finalization_subphase")
        finalization_failure = (
            failure.get("capture_failure_kind") == "single_run_finalization"
            or failure.get("code") in {
                "proposal_has_no_effects", "source_changed",
                "duplicate_stage_file",
            }
        )
        if finalization_failure != (
                capture_run in (1, 2)
                and subphase in FINALIZATION_SUBPHASES
                and failure.get("phase") == "capture"):
            raise DualRunError(
                "protocol", "invalid_result",
                "incoherent finalization diagnostic fields")
        if not finalization_failure and (
                capture_run is not None or subphase is not None):
            raise DualRunError(
                "protocol", "invalid_result",
                "unexpected finalization diagnostic fields")
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
        if (result["baseline_comparison"] is None
                or result["baseline_comparison"]["status"] != "pass"
                or not result["baseline_comparison"]["runs_matched"]
                or result["baseline_comparison"]["unexpected_differences"]):
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks baseline comparison proof")
        if [item["run"] for item in result["cleanup"]] != [2, 1] or not all(
            item["passed"] for item in result["cleanup"]
        ):
            raise DualRunError("protocol", "invalid_result", "successful C2 lacks cleanup proof")
    elif result["failure"] is None:
        raise DualRunError("protocol", "invalid_result", "failed C2 result omits failure")


def run_dual_comparison(
    project_root, source_root, media_root, proposal, policy, comparison_id, *,
    control_started=None, admission_cutoff=None, control_deadline=None,
):
    """Execute C2 internally. This function is intentionally not CLI-routed."""
    started = time.monotonic() if control_started is None else control_started
    admission_cutoff = (started + EXECUTION_ADMISSION_CUTOFF_SECONDS
                        if admission_cutoff is None else admission_cutoff)
    deadline = (started + CONTROL_DEADLINE_SECONDS
                if control_deadline is None else control_deadline)
    if not started < admission_cutoff < deadline:
        raise ValueError("invalid coordinator control deadline")
    result = _base_result(comparison_id)
    scope = None
    comparison_succeeded = False
    def admit(phase, required_seconds=0):
        if time.monotonic() + required_seconds >= admission_cutoff:
            raise DualRunError(
                phase, "comparison_failed",
                "C2 execution admission cutoff reached",
                category="execution_admission_cutoff")
    try:
        admit("capture", CAPTURE_ALLOWANCE_SECONDS)
        scope = _prepare_scope(
            project_root, source_root, media_root, proposal, policy, comparison_id
        )
        def copy_verified_context():
            result["validation_context"] = {
                key: scope.lifecycles[0].request["validation_context"][key]
                for key in ("input_fingerprint", "requested_seed", "effective_seed",
                            "reference_clock", "start_time", "end_time", "timezone")
            }
            result["affected_channels"] = scope.lifecycles[0].request["affected_channels"]

        _capture_step("context_consistency", copy_verified_context)
        result["source_checks"].append(
            {"checkpoint": "after_capture", "passed": True, "changed_categories": []}
        )
        normalized = []
        baseline_summaries = []
        for index, lifecycle in enumerate(scope.lifecycles, 1):
            result["phase_reached"] = f"run_{index}"
            admit(f"run_{index}", PER_RUN_TIMEOUT_SECONDS)
            remaining = admission_cutoff - time.monotonic()
            if remaining <= 0:
                raise DualRunError(
                    f"run_{index}", "c1_run_failed", "C2 total timeout expired",
                    category="launcher_timeout",
                    c1_diagnostic={"run": index, "detail": make_diagnostic(
                        "launcher_timeout", "launch")},
                    launcher_summary_value={
                        "outcome": "timed_out", "stdout_bytes": 0,
                        "stderr_bytes": 0, "stdout_truncated": False,
                        "stderr_truncated": False},
                )
            try:
                result["scheduler_invoked"][f"run_{index}"] = "unknown"
                launch_single_run(
                    lifecycle, timeout=OUTER_WATCHDOG_SECONDS
                )
                response = inspect_single_run(lifecycle)
            except Exception as exc:
                _raise_if_cancelled(exc)
                if isinstance(exc, SingleRunError) and exc.c1_diagnostic is not None:
                    diagnostic = exc.c1_diagnostic
                    launch_summary = exc.launcher_summary
                    result["scheduler_invoked"][f"run_{index}"] = exc.scheduler_state
                elif getattr(lifecycle, "launcher_result", None) is not None:
                    diagnostic = make_diagnostic(
                        "worker_response_invalid", "response")
                    launch_summary = launcher_summary(lifecycle.launcher_result)
                else:
                    diagnostic = make_diagnostic("launcher_failed", "launch")
                    result["scheduler_invoked"][f"run_{index}"] = False
                    launch_summary = {
                        "outcome": "launch_error", "stdout_bytes": 0,
                        "stderr_bytes": 0, "stdout_truncated": False,
                        "stderr_truncated": False,
                        "termination_kind": "launcher_failure",
                        "exit_status": None, "signal": None}
                raise DualRunError(
                    f"run_{index}", "c1_run_failed",
                    "An isolated native scheduling run failed.",
                    category=diagnostic["code"],
                    c1_diagnostic={"run": index, "detail": diagnostic},
                    launcher_summary_value=launch_summary,
                ) from exc
            result["scheduler_invoked"][f"run_{index}"] = response["scheduler_invoked"]
            if response["status"] != "success":
                launch_summary = launcher_summary(lifecycle.launcher_result)
                raise DualRunError(
                    f"run_{index}", "c1_run_failed",
                    "An isolated native scheduling run failed.",
                    category=response["failure"]["code"],
                    c1_diagnostic={"run": index, "detail": response["failure"]},
                    launcher_summary_value=launch_summary,
                )
            try:
                lifecycle.settle_unit()
            except Exception as exc:
                _raise_if_cancelled(exc)
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
                admit("normalization", NORMALIZATION_ALLOWANCE_SECONDS)
                normalized_run = normalize_completed_run(
                    lifecycle.stage,
                    lifecycle.stage / "source/runtime/fs42_fluid.db",
                    response,
                    proposal_boundary_to_db(proposal["week_start"], "week_start"),
                )
            except Exception as exc:
                _raise_if_cancelled(exc)
                raise DualRunError(
                    "normalization", "normalization_failed",
                    f"run {index} normalization failed ({type(exc).__name__})",
                    category=f"run_{index}",
                ) from exc
            normalized.append(normalized_run)
            try:
                admit("baseline_comparison", BASELINE_COMPARISON_ALLOWANCE_SECONDS)
                baseline_summaries.append(compare_baseline_to_proposed(
                    lifecycle.stage,
                    lifecycle.stage / "source/runtime/fs42_fluid.db",
                    lifecycle.stage / "work/runtime/fs42_fluid.db",
                    response["channels"],
                    proposal_boundary_to_db(proposal["week_start"], "week_start"),
                    proposal,
                ))
            except Exception as exc:
                _raise_if_cancelled(exc)
                raise DualRunError(
                    "baseline_comparison", "baseline_comparison_failed",
                    f"run {index} baseline comparison failed ({type(exc).__name__})",
                    category=f"run_{index}",
                ) from exc
            result["runs"].append({
                "index": index,
                "run_id": lifecycle.run_id,
                "status": response["status"],
                "normalization_digest": normalized_run.digest,
                "record_count": normalized_run.record_count,
                "provisional_catalog_count": normalized_run.provisional_count,
                "channels": response["channels"],
                "guide_validation": response["guide_validation"],
                "preservation_summary": _run_preservation_summary(response),
                "warnings": response["warnings"],
                "timings_ms": response["timings_ms"],
            })
        result["phase_reached"] = "comparison"
        admit("comparison", COMPARISON_ALLOWANCE_SECONDS)
        try:
            comparison = compare_normalized_runs(normalized[0], normalized[1])
        except Exception as exc:
            _raise_if_cancelled(exc)
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
        baseline_match = compare_baseline_summaries(
            baseline_summaries[0], baseline_summaries[1]
        )
        result["baseline_comparison"] = {
            **baseline_summaries[0],
            "runs_matched": baseline_match["passed"],
            "run_1_digest": baseline_match["run_1_digest"],
            "run_2_digest": baseline_match["run_2_digest"],
        }
        if not baseline_match["passed"]:
            raise DualRunError(
                "baseline_comparison", "baseline_summary_mismatch",
                "independent baseline-versus-proposed summaries differ",
            )
        if result["baseline_comparison"]["unexpected_differences"]:
            raise DualRunError(
                "baseline_comparison", "unexpected_schedule_difference",
                "proposed schedule introduced a new coverage defect",
            )
        comparison_succeeded = True
    except BaseException as exc:
        if result["phase_reached"] == "run_1":
            result["scheduler_invoked"]["run_2"] = False
        result["status"] = "failed"
        validated_error = isinstance(exc, (DualRunError, SingleRunError))
        exception_phase = (exc.phase if validated_error
                           else result["phase_reached"])
        # C1 preparation errors retain their validated code, while the C2
        # lifecycle still records that they occurred during source capture.
        if isinstance(exc, SingleRunError) and result["phase_reached"] == "capture":
            exception_phase = "capture"
        result["phase_reached"] = exception_phase
        fallback_code = {
            # Every expected capture operation has an explicit _capture_step.
            # Anything else is a programming failure, never an unclassified
            # source-capture result.
            "capture": "internal_error",
            "run_1": "c1_run_failed",
            "run_2": "c1_run_failed",
            "normalization": "normalization_failed",
            "comparison": "comparison_failed",
            "baseline_comparison": "baseline_comparison_failed",
            "cleanup": "cleanup_failed",
        }.get(result["phase_reached"], "comparison_failed")
        interrupted = (isinstance(exc, KeyboardInterrupt)
                       or getattr(exc, "is_validation_cancellation", False))
        code = ("validation_interrupted" if interrupted else
                exc.code if validated_error else fallback_code)
        if code == "cleanup_failure":
            code = "cleanup_failed"
        capture_failure_kind = getattr(exc, "capture_failure_kind", None)
        capture_run = getattr(exc, "capture_run", None)
        finalization_subphase = getattr(exc, "finalization_subphase", None)
        safe_message = (
            SINGLE_RUN_CAPTURE_MESSAGES.get(code, "Native preparation failed.")
            if isinstance(exc, SingleRunError) and result["phase_reached"] == "capture"
            else _bounded_detail(exc, project_root, source_root, media_root)
        )
        result["failure"] = {
            "phase": result["phase_reached"],
            "code": code,
            "category": (exc.category if isinstance(exc, DualRunError) else None),
            "message": (SOURCE_CAPTURE_FAILURE_MESSAGE
                        if code == "source_capture_failed" else
                        safe_message),
        }
        if isinstance(exc, DualRunError) and exc.code == "c1_run_failed":
            result["failure"]["c1_diagnostic"] = exc.c1_diagnostic
            result["failure"]["launcher_summary"] = exc.launcher_summary
        if capture_failure_kind is not None:
            result["failure"]["capture_failure_kind"] = capture_failure_kind
        if capture_run is not None:
            result["failure"]["capture_run"] = capture_run
        if finalization_subphase is not None:
            result["failure"]["finalization_subphase"] = finalization_subphase
        preparation_cleanup = getattr(exc, "preparation_cleanup", None)
        if preparation_cleanup is not None:
            result["cleanup"] = preparation_cleanup
    finally:
        if scope is not None:
            try:
                scope.cleanup_stages()
            except BaseException as cleanup_error:
                if result["failure"] is None:
                    interrupted = (isinstance(cleanup_error, KeyboardInterrupt)
                                   or getattr(cleanup_error, "is_validation_cancellation", False))
                    result["failure"] = {
                        "phase": "cleanup",
                        "code": "validation_interrupted" if interrupted else "cleanup_failed",
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
            except BaseException as final_error:
                categories = getattr(final_error, "category", None) or []
                result["source_checks"].append({
                    "checkpoint": "before_success", "passed": False,
                    "changed_categories": categories,
                })
                if result["failure"] is None:
                    interrupted = (isinstance(final_error, KeyboardInterrupt)
                                   or getattr(final_error, "is_validation_cancellation", False))
                    result["failure"] = {
                        "phase": "before_success",
                        "code": "validation_interrupted" if interrupted else "input_changed",
                        "category": categories,
                        "message": _bounded_detail(
                            final_error, project_root, source_root, media_root
                        ),
                    }
                result["status"] = "failed"
            try:
                scope.close_capture()
            except BaseException as cleanup_error:
                if result["failure"] is None:
                    interrupted = (isinstance(cleanup_error, KeyboardInterrupt)
                                   or getattr(cleanup_error, "is_validation_cancellation", False))
                    result["failure"] = {
                        "phase": "cleanup",
                        "code": "validation_interrupted" if interrupted else "cleanup_failed",
                        "category": None,
                        "message": f"capture cleanup failed ({type(cleanup_error).__name__})",
                    }
                result["status"] = "failed"
            if comparison_succeeded and result["failure"] is None:
                result["status"] = "success"
                result["phase_reached"] = "complete"
        result["timings_ms"]["total"] = round((time.monotonic() - started) * 1000)
        if time.monotonic() > deadline:
            if result["failure"] is None:
                result["failure"] = {
                    "phase": "cleanup", "code": "finalization_deadline_overrun",
                    "category": None,
                    "message": "Mandatory finalization exceeded its reserved deadline.",
                }
            elif result["failure"].get("category") is None:
                result["failure"]["category"] = "finalization_deadline_overrun"
            result["status"] = "failed"
    validate_document(result, RESULT_SCHEMA)
    _validate_result_semantics(result)
    return result
