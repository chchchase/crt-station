"""Disabled-by-default public schedule-validation coordinator."""

import fcntl
import os
import re
import signal
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from station_director import validation_control
from station_director.c1_diagnostics import (
    DIAGNOSTIC_RULES, MAX_LAUNCH_BYTE_COUNT,
)


TRUSTED_PROJECT_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_MEDIA_ROOT = Path("/mnt/t7/CRT-Media")
LOCK_NAME = ".schedule-validation.lock"
LOCK_WAIT_SECONDS = 5.0
LOCK_POLL_SECONDS = 0.05
STALE_SCAN_LIMIT = 10_000
EXECUTION_ADMISSION_CUTOFF_SECONDS = 7020
UNIT_CLEANUP_SEQUENCE_COUNT = 2
UNIT_CLEANUP_SEQUENCE_ALLOWANCE_SECONDS = 95
REPORT_PUBLICATION_ALLOWANCE_SECONDS = 120
STAGE_CAPTURE_FINALIZATION_ALLOWANCE_SECONDS = 10
SIGNAL_RESTORATION_ALLOWANCE_SECONDS = 5
LOCK_RELEASE_ALLOWANCE_SECONDS = 5
MANDATORY_FINALIZATION_REQUIRED_SECONDS = (
    UNIT_CLEANUP_SEQUENCE_COUNT * UNIT_CLEANUP_SEQUENCE_ALLOWANCE_SECONDS
    + REPORT_PUBLICATION_ALLOWANCE_SECONDS
    + STAGE_CAPTURE_FINALIZATION_ALLOWANCE_SECONDS
    + SIGNAL_RESTORATION_ALLOWANCE_SECONDS
    + LOCK_RELEASE_ALLOWANCE_SECONDS
)
MANDATORY_FINALIZATION_RESERVE_SECONDS = 340
CONTROL_DEADLINE_SECONDS = (
    EXECUTION_ADMISSION_CUTOFF_SECONDS
    + MANDATORY_FINALIZATION_RESERVE_SECONDS
)
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
FINALIZATION_SUBPHASES = frozenset({
    "staged_physical_fingerprint",
    "staged_logical_configuration_fingerprint",
    "seed_input_construction",
    "validation_context_derivation",
    "staged_channel_configuration_loading",
    "proposal_projection",
    "affected_channel_resolution",
    "request_binding",
    "request_publication",
    "lifecycle_construction",
})


class CoordinatorError(RuntimeError):
    def __init__(self, code, phase="coordinator"):
        super().__init__(code)
        self.code = code
        self.phase = phase


class ValidationCancellation(Exception):
    is_validation_cancellation = True

    def __init__(self, signum=None):
        super().__init__("validation_interrupted")
        self.code = "validation_interrupted"
        self.signum = signum


@dataclass(frozen=True)
class CoordinatorOutcome:
    state: str
    proposal_id: str | None = None
    run_id: str | None = None
    validation_status: str | None = None
    phase: str | None = None
    failure_code: str | None = None
    capture_failure_kind: str | None = None
    capture_run: int | None = None
    finalization_subphase: str | None = None
    c1_run: int | None = None
    c1_domain: str | None = None
    c1_phase: str | None = None
    c1_code: str | None = None
    c1_probe: str | None = None
    c1_fingerprint_category: str | None = None
    c1_channel_number: int | None = None
    c1_launcher_outcome: str | None = None
    c1_stdout_bytes: int | None = None
    c1_stderr_bytes: int | None = None
    c1_stdout_truncated: bool | None = None
    c1_stderr_truncated: bool | None = None
    scheduler_invoked: tuple = (False, False)
    report_published: bool = False
    report_durable: bool = False
    publication_code: str | None = None
    latest_status: str | None = None
    latest_code: str | None = None
    affected_channels: int = 0
    changed_channels: int = 0
    cleanup_passed: bool | None = None
    quarantined: bool = False

    def __post_init__(self):
        states = {"disabled", "rejected", "passed", "failed", "interrupted"}
        scheduler_values = (True, False, "unknown")
        if (self.state not in states or len(self.scheduler_invoked) != 2
                or any(value not in scheduler_values for value in self.scheduler_invoked)):
            raise ValueError("invalid coordinator outcome")
        if self.state == "disabled" and any((
                self.proposal_id, self.run_id, self.validation_status, self.phase,
                self.failure_code, self.scheduler_invoked != (False, False),
                self.capture_failure_kind, self.capture_run,
                self.finalization_subphase,
                self.c1_run, self.c1_domain, self.c1_phase, self.c1_code,
                self.c1_probe, self.c1_fingerprint_category,
                self.c1_channel_number, self.c1_launcher_outcome,
                self.c1_stdout_bytes, self.c1_stderr_bytes,
                self.c1_stdout_truncated, self.c1_stderr_truncated,
                self.report_published, self.report_durable, self.publication_code,
                self.latest_status, self.latest_code, self.affected_channels,
                self.changed_channels, self.cleanup_passed is not None,
                self.quarantined)):
            raise ValueError("contradictory disabled outcome")
        if self.state == "rejected" and (self.run_id is not None or self.report_published
                or self.scheduler_invoked != (False, False)):
            raise ValueError("contradictory rejected outcome")
        if self.state == "passed" and not (
                self.validation_status == "success" and self.report_published
                and self.report_durable and self.failure_code is None
                and self.scheduler_invoked == (True, True)
                and self.cleanup_passed is True and not self.quarantined):
            raise ValueError("contradictory successful outcome")
        if self.report_durable and not self.report_published:
            raise ValueError("durable report is not published")
        if min(self.affected_channels, self.changed_channels) < 0:
            raise ValueError("negative coordinator count")
        safe_codes = {None, "phase_3_disabled", "invocation_context_rejected",
                      "invalid_proposal_id", "validation_lock_busy",
                      "validation_lock_unsafe", "proposal_rejected",
                      "policy_rejected", "source_capture_failed",
                      "proposal_has_no_effects",
                      "duplicate_stage_file", "source_changed",
                      "backup_mismatch", "context_mismatch",
                      "insufficient_space", "invalid_comparison_id",
                      "oversized_config", "source_root_mismatch",
                      "unsafe_source",
                      "c1_run_failed", "normalization_failed",
                      "comparison_failed", "reproducibility_mismatch",
                      "guide_reproducibility_mismatch",
                      "baseline_comparison_failed", "baseline_summary_mismatch",
                      "unexpected_schedule_difference", "input_changed",
                      "cleanup_failed", "validation_interrupted",
                      "finalization_deadline_overrun",
                      "stale_stage_cleanup_failed", "stale_unit_not_absent",
                      "stale_stage_scan_limit", "stale_stage_ambiguous",
                      "internal_error", "publication_failed",
                      "publication_durability_failed", "validation_interrupted",
                      "run_id_collision", "run_id_collision_exhausted",
                      "report_too_large", "report_verification_failed",
                      "unsafe_report_value", "noreplace_unavailable",
                      "rename_interrupted", "unsafe_report_root",
                      "unsafe_report_directory", "case_ambiguous_path",
                      "stale_report_temporary", "unexpected_report_entry",
                      "unsafe_report_file", "invalid_immutable_report",
                      "report_path_replaced", "invalid_latest", "invalid_identity",
                      "invalid_report_input"}
        if self.failure_code not in safe_codes:
            raise ValueError("unsafe coordinator failure code")
        c1_present = self.c1_run is not None
        if (self.failure_code == "c1_run_failed") != c1_present:
            raise ValueError("C1 outcome lacks a classified diagnostic")
        if c1_present:
            detail = {
                "domain": self.c1_domain, "phase": self.c1_phase,
                "code": self.c1_code,
                "template": DIAGNOSTIC_RULES[self.c1_code][2],
                "scheduler_invoked": self.scheduler_invoked[self.c1_run - 1],
                "probe": self.c1_probe,
                "fingerprint_category": self.c1_fingerprint_category,
                "channel_number": self.c1_channel_number,
            }
            from station_director.c1_diagnostics import validate_host_diagnostic
            validate_host_diagnostic(detail)
            if self.c1_launcher_outcome not in {
                    "completed", "nonzero_exit", "timed_out", "launch_error"}:
                raise ValueError("invalid C1 launcher outcome")
            if (isinstance(self.c1_stdout_bytes, bool)
                    or isinstance(self.c1_stderr_bytes, bool)
                    or not isinstance(self.c1_stdout_bytes, int)
                    or not isinstance(self.c1_stderr_bytes, int)
                    or min(self.c1_stdout_bytes, self.c1_stderr_bytes) < 0
                    or max(self.c1_stdout_bytes, self.c1_stderr_bytes)
                    > MAX_LAUNCH_BYTE_COUNT
                    or not isinstance(self.c1_stdout_truncated, bool)
                    or not isinstance(self.c1_stderr_truncated, bool)):
                raise ValueError("invalid C1 launcher counters")
        if ((self.failure_code == "source_capture_failed")
                != (self.capture_failure_kind in CAPTURE_FAILURE_KINDS)):
            raise ValueError("source capture outcome lacks a classified kind")
        if (self.capture_failure_kind is not None
                and self.capture_failure_kind not in CAPTURE_FAILURE_KINDS):
            raise ValueError("unsafe capture failure kind")
        detailed_finalization = (
            self.capture_failure_kind == "single_run_finalization"
            or (self.failure_code in {
                "proposal_has_no_effects", "source_changed",
                "duplicate_stage_file",
            } and self.state != "rejected")
        )
        if detailed_finalization != (
                self.capture_run in (1, 2)
                and self.finalization_subphase in FINALIZATION_SUBPHASES):
            raise ValueError("incoherent finalization diagnostic")
        if not detailed_finalization and (
                self.capture_run is not None
                or self.finalization_subphase is not None):
            raise ValueError("unexpected finalization diagnostic")
        if self.state in {"failed", "interrupted"} and self.failure_code is None:
            raise ValueError("failed outcome lacks failure code")
        if self.state == "interrupted" and self.failure_code != "validation_interrupted":
            raise ValueError("contradictory interrupted outcome")
        if self.state == "rejected" and any((
                self.validation_status is not None, self.report_durable,
                self.publication_code is not None, self.latest_status is not None,
                self.latest_code is not None, self.cleanup_passed is not None,
                self.quarantined, self.affected_channels, self.changed_channels)):
            raise ValueError("contradictory rejected outcome")
        if self.report_published and (self.proposal_id is None or self.run_id is None):
            raise ValueError("published report lacks identity")
        if self.proposal_id is not None and not re.fullmatch(
                r"p-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", self.proposal_id):
            raise ValueError("unsafe outcome proposal identity")
        if self.run_id is not None and not re.fullmatch(
                r"v-[0-9]{8}T[0-9]{12}Z-[a-f0-9]{32}", self.run_id):
            raise ValueError("unsafe outcome run identity")
        if self.phase not in {None, "invocation", "proposal", "policy", "lock",
                              "capture", "run_1", "run_2", "normalization",
                              "comparison", "baseline_comparison", "cleanup",
                              "before_success", "complete", "report", "coordinator"}:
            raise ValueError("unsafe outcome phase")
        if (self.publication_code is not None
                and self.publication_code not in safe_codes):
            raise ValueError("unsafe publication code")
        if (self.latest_code is not None
                and not re.fullmatch(r"[a-z][a-z0-9_]{0,99}", self.latest_code)):
            raise ValueError("unsafe latest code")
        if self.latest_status not in {None, "updated", "warning"}:
            raise ValueError("unsafe latest status")


def disabled_outcome():
    return CoordinatorOutcome(state="disabled")


def _rejected(code, phase, proposal_id=None):
    return CoordinatorOutcome(
        state="interrupted" if code == "validation_interrupted" else "rejected",
        proposal_id=proposal_id, phase=phase, failure_code=code,
    )


@contextmanager
def _validation_lock(secure_inputs):
    root_fd = secure_inputs._open_project_root()
    director_fd = None
    lock_fd = None
    try:
        consumed = root_fd
        root_fd = None
        director_fd = secure_inputs._descend(consumed, ("runtime", "director"))
        try:
            before = secure_inputs._find_exact(director_fd, LOCK_NAME)
        except secure_inputs.SecureInputError as exc:
            if exc.code != "missing_path":
                raise CoordinatorError("validation_lock_unsafe", "lock") from exc
            try:
                lock_fd = os.open(
                    LOCK_NAME,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=director_fd,
                )
            except FileExistsError:
                before = secure_inputs._find_exact(director_fd, LOCK_NAME)
            else:
                before = os.fstat(lock_fd)
        if lock_fd is None:
            lock_fd = os.open(
                LOCK_NAME, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=director_fd,
            )
        opened = os.fstat(lock_fd)
        after = os.stat(LOCK_NAME, dir_fd=director_fd, follow_symlinks=False)
        identities = {(item.st_dev, item.st_ino) for item in (before, opened, after)}
        if (len(identities) != 1 or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1 or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o600):
            raise CoordinatorError("validation_lock_unsafe", "lock")
        deadline = time.monotonic() + LOCK_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise CoordinatorError("validation_lock_busy", "lock") from exc
                time.sleep(LOCK_POLL_SECONDS)
        yield
    except KeyboardInterrupt:
        raise CoordinatorError("validation_interrupted", "lock") from None
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)
        if director_fd is not None:
            os.close(director_fd)
        if root_fd is not None:
            os.close(root_fd)


class _CancellationScope:
    def __init__(self):
        self.previous = {}
        self.requested = False

    def _handle(self, signum, unused_frame):
        if self.requested:
            return
        self.requested = True
        raise ValidationCancellation(signum)

    def __enter__(self):
        if threading.current_thread() is not threading.main_thread():
            raise CoordinatorError("coordinator_requires_main_thread")
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            self.previous[signum] = signal.getsignal(signum)
            signal.signal(signum, self._handle)
        return self

    def checkpoint(self):
        if self.requested:
            raise ValidationCancellation()

    def __exit__(self, unused_type, unused_value, unused_traceback):
        for signum, handler in self.previous.items():
            signal.signal(signum, handler)


def _recover_stale_stages(isolation):
    now = time.time()
    seen = 0
    folded = set()
    with os.scandir(isolation.STAGING_PARENT) as entries:
        for entry in entries:
            seen += 1
            if seen > STALE_SCAN_LIMIT:
                raise CoordinatorError("stale_stage_scan_limit", "capture")
            name = entry.name
            if not name.casefold().startswith("fs42-i-"):
                continue
            if name.casefold() in folded:
                raise CoordinatorError("stale_stage_ambiguous", "capture")
            folded.add(name.casefold())
            if not isolation.STAGING_RE.fullmatch(name):
                continue
            path = isolation.STAGING_PARENT / name
            info = entry.stat(follow_symlinks=False)
            try:
                (path / ".quarantine").lstat()
            except FileNotFoundError:
                quarantined = False
            except OSError:
                continue
            else:
                quarantined = True
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o700
                    or now - info.st_mtime < isolation.STAGING_MAX_AGE_SECONDS
                    or quarantined or isolation._staging_is_locked(path)):
                continue
            marker = path / ".active"
            request = path / "native-single-run.request.json"
            try:
                marker_info = marker.lstat()
                request_info = request.lstat()
            except OSError:
                continue
            if (not stat.S_ISREG(marker_info.st_mode) or marker_info.st_nlink != 1
                    or marker_info.st_uid != os.geteuid()
                    or stat.S_IMODE(marker_info.st_mode) != 0o600
                    or not stat.S_ISREG(request_info.st_mode) or request_info.st_nlink != 1
                    or request_info.st_uid != os.geteuid()
                    or stat.S_IMODE(request_info.st_mode) != 0o600):
                continue
            token = name.removeprefix("fs42-i-")
            unit = f"fs42-native-{token}.service"
            absent, unused_detail = isolation.cleanup_unit(unit)
            if not absent:
                try:
                    (path / ".quarantine").touch(mode=0o600, exist_ok=True)
                except OSError:
                    pass
                raise CoordinatorError("stale_unit_not_absent", "capture")
            cleaned, unused_detail = isolation.cleanup_staging_directory(path)
            if not cleaned:
                raise CoordinatorError("stale_stage_cleanup_failed", "capture")


def _minimal_c2_failure(run_id, code, phase="capture", source=None, *,
                        capture_failure_kind=None, capture_run=None,
                        finalization_subphase=None):
    from station_director.dual_run import _base_result
    from station_director.single_run_protocol import validate_document
    from station_director.dual_run import RESULT_SCHEMA

    result = _base_result(run_id)
    if source is not None:
        validate_document(source, RESULT_SCHEMA)
        for name in ("scheduler_invoked", "validation_context", "affected_channels",
                     "source_checks", "runs", "reproducibility",
                     "baseline_comparison", "cleanup", "timings_ms"):
            result[name] = source[name]
    result["phase_reached"] = phase if phase in (
        "capture", "run_1", "run_2", "normalization", "comparison",
        "baseline_comparison", "cleanup", "before_success",
    ) else "capture"
    if code == "source_capture_failed":
        if capture_failure_kind not in CAPTURE_FAILURE_KINDS:
            raise ValueError("source capture failure requires a precise kind")
    elif capture_failure_kind is not None:
        raise ValueError("capture failure kind is only valid for source capture")
    result["failure"] = {
        "phase": result["phase_reached"], "code": code,
        "category": None,
        "message": ("Source capture failed." if code == "source_capture_failed"
                    else "validation failed"),
    }
    if capture_failure_kind is not None:
        result["failure"]["capture_failure_kind"] = capture_failure_kind
    if capture_run is not None:
        result["failure"]["capture_run"] = capture_run
    if finalization_subphase is not None:
        result["failure"]["finalization_subphase"] = finalization_subphase
    validate_document(result, RESULT_SCHEMA)
    return result


def _outcome_from_result(proposal_id, run_id, result, publication=None,
                         publication_error=None):
    schedulers = result.get("scheduler_invoked", {})
    scheduler_tuple = (schedulers.get("run_1", "unknown"),
                       schedulers.get("run_2", "unknown"))
    cleanup = result.get("cleanup", [])
    cleanup_passed = (all(item.get("passed") for item in cleanup)
                      if cleanup else None)
    quarantined = any(item.get("quarantined") for item in cleanup)
    comparison = result.get("baseline_comparison") or {}
    changed = (comparison.get("resulting_schedule_changes") or {}).get(
        "changed_channels", 0)
    failure = result.get("failure") or {}
    c1 = failure.get("c1_diagnostic") or {}
    c1_detail = c1.get("detail") or {}
    c1_launcher = failure.get("launcher_summary") or {}
    if publication is not None:
        if (not isinstance(publication, dict)
                or publication.get("publication_state") != "published_durable"
                or publication.get("proposal_id") != proposal_id
                or publication.get("run_id") != run_id
                or not isinstance(publication.get("latest"), dict)
                or publication["latest"].get("status") not in {"updated", "warning"}):
            raise ValueError("invalid publication outcome")
    publication_state = (None if publication_error is None
                         else publication_error.publication_state)
    published = publication is not None or publication_state in (
        "published_not_durable", "published_durable")
    durable = publication is not None or publication_state == "published_durable"
    publication_code = (None if publication_error is None
                        else publication_error.code)
    latest = {} if publication is None else publication.get("latest", {})
    passed = (result.get("status") == "success" and durable
              and publication_error is None)
    failure_code = (publication_code or failure.get("code")
                    or (None if passed else "publication_failed"))
    interrupted = failure_code == "validation_interrupted"
    return CoordinatorOutcome(
        state="interrupted" if interrupted else "passed" if passed else "failed",
        proposal_id=proposal_id, run_id=run_id,
        validation_status=result.get("status"),
        phase=("report" if publication_error is not None
               else result.get("phase_reached")),
        failure_code=failure_code, scheduler_invoked=scheduler_tuple,
        capture_failure_kind=(failure.get("capture_failure_kind")
                              if failure_code == "source_capture_failed" else None),
        capture_run=failure.get("capture_run"),
        finalization_subphase=failure.get("finalization_subphase"),
        c1_run=c1.get("run") if failure_code == "c1_run_failed" else None,
        c1_domain=c1_detail.get("domain") if failure_code == "c1_run_failed" else None,
        c1_phase=c1_detail.get("phase") if failure_code == "c1_run_failed" else None,
        c1_code=c1_detail.get("code") if failure_code == "c1_run_failed" else None,
        c1_probe=c1_detail.get("probe") if failure_code == "c1_run_failed" else None,
        c1_fingerprint_category=(c1_detail.get("fingerprint_category")
                                if failure_code == "c1_run_failed" else None),
        c1_channel_number=(c1_detail.get("channel_number")
                           if failure_code == "c1_run_failed" else None),
        c1_launcher_outcome=(c1_launcher.get("outcome")
                             if failure_code == "c1_run_failed" else None),
        c1_stdout_bytes=(c1_launcher.get("stdout_bytes")
                         if failure_code == "c1_run_failed" else None),
        c1_stderr_bytes=(c1_launcher.get("stderr_bytes")
                         if failure_code == "c1_run_failed" else None),
        c1_stdout_truncated=(c1_launcher.get("stdout_truncated")
                             if failure_code == "c1_run_failed" else None),
        c1_stderr_truncated=(c1_launcher.get("stderr_truncated")
                             if failure_code == "c1_run_failed" else None),
        report_published=published, report_durable=durable,
        publication_code=publication_code,
        latest_status=latest.get("status"), latest_code=latest.get("code"),
        affected_channels=len(result.get("affected_channels", [])),
        changed_channels=changed, cleanup_passed=cleanup_passed,
        quarantined=quarantined,
    )



def _outcome_without_report(proposal_id, run_id, result, code):
    schedulers = (result or {}).get("scheduler_invoked", {})
    cleanup = (result or {}).get("cleanup", [])
    failure = ((result or {}).get("failure") or {})
    c1 = failure.get("c1_diagnostic") or {}
    detail = c1.get("detail") or {}
    launcher = failure.get("launcher_summary") or {}
    return CoordinatorOutcome(
        state="interrupted" if code == "validation_interrupted" else "failed",
        proposal_id=proposal_id, run_id=run_id,
        validation_status=(result or {}).get("status"), phase="report",
        failure_code=code,
        capture_failure_kind=(
            ((result or {}).get("failure") or {}).get("capture_failure_kind")
            if code == "source_capture_failed" else None),
        capture_run=((result or {}).get("failure") or {}).get("capture_run"),
        finalization_subphase=(
            ((result or {}).get("failure") or {}).get("finalization_subphase")),
        c1_run=c1.get("run") if code == "c1_run_failed" else None,
        c1_domain=detail.get("domain") if code == "c1_run_failed" else None,
        c1_phase=detail.get("phase") if code == "c1_run_failed" else None,
        c1_code=detail.get("code") if code == "c1_run_failed" else None,
        c1_probe=detail.get("probe") if code == "c1_run_failed" else None,
        c1_fingerprint_category=(detail.get("fingerprint_category")
                                if code == "c1_run_failed" else None),
        c1_channel_number=(detail.get("channel_number")
                           if code == "c1_run_failed" else None),
        c1_launcher_outcome=(launcher.get("outcome")
                             if code == "c1_run_failed" else None),
        c1_stdout_bytes=(launcher.get("stdout_bytes")
                         if code == "c1_run_failed" else None),
        c1_stderr_bytes=(launcher.get("stderr_bytes")
                         if code == "c1_run_failed" else None),
        c1_stdout_truncated=(launcher.get("stdout_truncated")
                             if code == "c1_run_failed" else None),
        c1_stderr_truncated=(launcher.get("stderr_truncated")
                             if code == "c1_run_failed" else None),
        scheduler_invoked=(schedulers.get("run_1", "unknown"),
                           schedulers.get("run_2", "unknown")),
        cleanup_passed=(all(item.get("passed") for item in cleanup)
                        if cleanup else None),
        quarantined=any(item.get("quarantined") for item in cleanup),
    )


def _finalization_budget_exhausted(result, admission_cutoff, control_deadline, *, now=None):
    current = time.monotonic() if now is None else now
    return (result.get("status") == "success"
            and (current >= admission_cutoff
                 or current + MANDATORY_FINALIZATION_REQUIRED_SECONDS
                 >= control_deadline))

def validate_saved_proposal(proposal_id):
    """Run the complete public flow only when the checked-in gate is true."""
    if not validation_control.SCHEDULE_VALIDATION_ENABLED:
        return disabled_outcome()

    from station_director.isolation import check_invocation_context

    allowed, unused_detail = check_invocation_context()
    if not allowed:
        return _rejected("invocation_context_rejected", "invocation")

    from station_director import secure_validation_inputs as secure_inputs
    try:
        secure_inputs.validate_proposal_id(proposal_id)
    except secure_inputs.SecureInputError:
        return _rejected("invalid_proposal_id", "proposal")

    try:
        with _validation_lock(secure_inputs):
            try:
                proposal = secure_inputs.load_canonical_proposal(proposal_id)
            except secure_inputs.SecureInputError:
                return _rejected("proposal_rejected", "proposal", proposal_id)
            if not any(proposal[name] for name in (
                    "assignment_changes", "directives", "exclusions")):
                return _rejected(
                    "proposal_has_no_effects", "proposal", proposal_id)
            try:
                policy = secure_inputs.load_canonical_policy()
            except secure_inputs.SecureInputError:
                return _rejected("policy_rejected", "policy", proposal_id)

            from station_director.reporting import (
                ReportError, build_validation_report, create_validation_run_id,
                publish_validation_report,
            )
            run_id = create_validation_run_id()
            control_started = time.monotonic()
            admission_cutoff = (
                control_started + EXECUTION_ADMISSION_CUTOFF_SECONDS)
            control_deadline = control_started + CONTROL_DEADLINE_SECONDS
            result = None
            try:
                with _CancellationScope() as cancellation:
                    cancellation.checkpoint()
                    from station_director import isolation
                    try:
                        _recover_stale_stages(isolation)
                    except CoordinatorError as exc:
                        result = _minimal_c2_failure(run_id, exc.code, "capture")
                    if result is None:
                        cancellation.checkpoint()
                        from station_director.dual_run import (
                            RESULT_SCHEMA, _validate_result_semantics,
                            run_dual_comparison,
                        )
                        from station_director.single_run_protocol import validate_document
                        result = run_dual_comparison(
                            TRUSTED_PROJECT_ROOT, TRUSTED_PROJECT_ROOT,
                            CANONICAL_MEDIA_ROOT, proposal, policy, run_id,
                            control_started=control_started,
                            admission_cutoff=admission_cutoff,
                            control_deadline=control_deadline,
                        )
                        validate_document(result, RESULT_SCHEMA)
                        _validate_result_semantics(result)
                    if result.get("comparison_id") != run_id:
                        return _outcome_without_report(
                            proposal_id, run_id, result, "internal_error")
                    if _finalization_budget_exhausted(
                            result, admission_cutoff, control_deadline):
                        result["status"] = "failed"
                        result["phase_reached"] = "cleanup"
                        result["failure"] = {
                            "phase": "cleanup",
                            "code": "finalization_deadline_overrun",
                            "category": None,
                            "message": "Mandatory finalization exceeded its reserved deadline.",
                        }
                    completed_at = datetime.now(timezone.utc).isoformat().replace(
                        "+00:00", "Z")
                    try:
                        report = build_validation_report(
                            proposal, result, run_id, completed_at)
                    except ValidationCancellation:
                        raise
                    except Exception:
                        return _outcome_without_report(
                            proposal_id, run_id, result, "internal_error")
                    try:
                        publication = publish_validation_report(report)
                    except ReportError as exc:
                        return _outcome_from_result(
                            proposal_id, run_id, result,
                            publication_error=exc)
                    return _outcome_from_result(
                        proposal_id, run_id, result, publication)
            except (ValidationCancellation, KeyboardInterrupt):
                if result is not None:
                    return _outcome_without_report(
                        proposal_id, run_id, result, "validation_interrupted")
                interrupted = _minimal_c2_failure(
                    run_id, "validation_interrupted", "capture")
                completed_at = datetime.now(timezone.utc).isoformat().replace(
                    "+00:00", "Z")
                try:
                    report = build_validation_report(
                        proposal, interrupted, run_id, completed_at)
                    publication = publish_validation_report(report)
                except ReportError as exc:
                    return _outcome_from_result(
                        proposal_id, run_id, interrupted, publication_error=exc)
                except Exception:
                    publication = None
                return _outcome_from_result(
                    proposal_id, run_id, interrupted, publication)
    except CoordinatorError as exc:
        return _rejected(exc.code, exc.phase, proposal_id)
    except (secure_inputs.SecureInputError, OSError):
        return _rejected("validation_lock_unsafe", "lock", proposal_id)
    except KeyboardInterrupt:
        return _rejected("validation_interrupted", "coordinator", proposal_id)
    except Exception:
        return CoordinatorOutcome(
            state="failed", proposal_id=proposal_id, phase="coordinator",
            failure_code="internal_error")


def render_cli_outcome(outcome):
    if not isinstance(outcome, CoordinatorOutcome):
        raise TypeError("CoordinatorOutcome required")
    if outcome.state == "disabled":
        return validation_control.DISABLED_MESSAGE + "\n"
    lines = []
    if outcome.state == "rejected":
        lines.append(f"Validation rejected: {outcome.failure_code}")
    else:
        if outcome.proposal_id is not None:
            lines.append(f"Proposal: {outcome.proposal_id}")
        lines.append("Validation: " + ("PASS" if outcome.state == "passed" else "FAIL"))
        if outcome.phase is not None:
            lines.append(f"Phase: {outcome.phase}")
        if outcome.failure_code is not None:
            lines.append(f"Failure: {outcome.failure_code}")
        if outcome.capture_failure_kind is not None:
            lines.append(f"Capture failure kind: {outcome.capture_failure_kind}")
        if outcome.capture_run is not None:
            lines.append(f"Capture run: {outcome.capture_run}")
        if outcome.finalization_subphase is not None:
            lines.append(
                f"Finalization subphase: {outcome.finalization_subphase}")
        if outcome.c1_run is not None:
            lines.append(f"C1 run: {outcome.c1_run}")
            lines.append(f"C1 domain: {outcome.c1_domain}")
            lines.append(f"C1 phase: {outcome.c1_phase}")
            lines.append(f"C1 code: {outcome.c1_code}")
            if outcome.c1_probe is not None:
                lines.append(f"C1 probe: {outcome.c1_probe}")
            if outcome.c1_fingerprint_category is not None:
                lines.append(
                    f"C1 fingerprint category: {outcome.c1_fingerprint_category}")
            if outcome.c1_channel_number is not None:
                lines.append(f"C1 channel: {outcome.c1_channel_number}")
            lines.append(f"C1 launcher: {outcome.c1_launcher_outcome}")
            lines.append(
                f"C1 output bytes: stdout={outcome.c1_stdout_bytes} "
                f"stderr={outcome.c1_stderr_bytes}")
            lines.append(
                "C1 output truncated: "
                f"stdout={str(outcome.c1_stdout_truncated).lower()} "
                f"stderr={str(outcome.c1_stderr_truncated).lower()}")
        if outcome.run_id is not None:
            lines.append(f"Run: {outcome.run_id}")
        if outcome.report_durable:
            lines.append(
                f"Report: validations/{outcome.proposal_id}/{outcome.run_id}/")
        else:
            lines.append("Report: not published")
        lines.append(f"Channels: {outcome.affected_channels}")
        lines.append(f"Changed channels: {outcome.changed_channels}")
        if outcome.cleanup_passed is not None:
            lines.append("Cleanup: " + ("passed" if outcome.cleanup_passed else "failed"))
        lines.append("Quarantine: " + ("yes" if outcome.quarantined else "no"))
        if outcome.latest_status is not None:
            lines.append("Latest: " + outcome.latest_status)
    return "\n".join(lines) + "\n"
