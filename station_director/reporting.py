"""Disconnected C3a2 immutable validation-report infrastructure.

Nothing in the public CLI imports this module.  Publication is an explicit
internal operation for C3b to invoke only after it trusts invocation and
proposal identity.
"""

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from station_director import __version__
from station_director.single_run_protocol import strict_json_loads, validate_document


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALIDATIONS_ROOT = PROJECT_ROOT / "runtime/director/validations"
REPORT_SCHEMA_V1 = Path(__file__).with_name("schemas") / "validation-report.v1.schema.json"
REPORT_SCHEMA_V2 = Path(__file__).with_name("schemas") / "validation-report.v2.schema.json"
REPORT_SCHEMA_V3 = Path(__file__).with_name("schemas") / "validation-report.v3.schema.json"
REPORT_SCHEMA_V4 = Path(__file__).with_name("schemas") / "validation-report.v4.schema.json"
REPORT_SCHEMA_V5 = Path(__file__).with_name("schemas") / "validation-report.v5.schema.json"
REPORT_SCHEMA_V6 = Path(__file__).with_name("schemas") / "validation-report.v6.schema.json"
REPORT_SCHEMA_V7 = Path(__file__).with_name("schemas") / "validation-report.v7.schema.json"
REPORT_SCHEMA_V8 = Path(__file__).with_name("schemas") / "validation-report.v8.schema.json"
# New reports always use v8. Older paths remain frozen for retained reports.
REPORT_SCHEMA = REPORT_SCHEMA_V8
LATEST_SCHEMA = Path(__file__).with_name("schemas") / "latest-validation-pointer.v1.schema.json"
DUAL_RESULT_SCHEMA = Path(__file__).with_name("schemas") / "native-dual-run.result.v4.schema.json"
PROPOSAL_ID_RE = re.compile(r"p-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}\Z")
RUN_ID_RE = re.compile(r"v-[0-9]{8}T[0-9]{6}[0-9]{6}Z-[a-f0-9]{32}\Z")
MAX_REPORT_JSON_BYTES = 512 * 1024
MAX_REPORT_TEXT_BYTES = 128 * 1024
MAX_REPORT_TEXT_FIELD = 2000
MAX_RUN_ID_ATTEMPTS = 16
MAX_REPORT_ENTRIES = 10_000
RENAME_NOREPLACE = 1
RENAME_EINTR_RETRIES = 8

SAFE_PHASES = {
    "capture", "run_1", "run_2", "normalization", "comparison",
    "baseline_comparison", "cleanup", "before_success", "complete", "report",
}
DIAGNOSTIC_TEMPLATES = {
    "baseline_gap": "The captured baseline contains a coverage gap.",
    "baseline_overlap": "The captured baseline contains overlapping blocks.",
    "proposal_created_gap": "The proposed schedule introduces a coverage gap.",
    "proposal_created_overlap": "The proposed schedule introduces overlapping blocks.",
    "source_capture_failed": "Source capture failed.",
    "proposal_has_no_effects": "Proposal has no effects eligible for schedule validation.",
    "duplicate_stage_file": "A staged validation file already exists.",
    "source_changed": "A staged scheduling input changed during preparation.",
    "backup_mismatch": "A staged database backup differs from its pinned source.",
    "context_mismatch": "Independent scheduling contexts differ.",
    "insufficient_space": "Insufficient private staging space is available.",
    "invalid_comparison_id": "The internal comparison identity is invalid.",
    "oversized_config": "Protected configuration exceeds its safety limit.",
    "source_root_mismatch": "The protected source root is not canonical.",
    "unsafe_source": "A protected source object is unsafe.",
    "c1_run_failed": "An isolated native scheduling run failed.",
    "normalization_failed": "Schedule normalization failed.",
    "comparison_failed": "Structural reproducibility comparison failed.",
    "reproducibility_mismatch": "The independent native runs differ.",
    "guide_reproducibility_mismatch": "The independent guide results differ.",
    "baseline_comparison_failed": "Baseline comparison failed.",
    "baseline_summary_mismatch": "The independent baseline summaries differ.",
    "unexpected_schedule_difference": "The proposal introduced an unauthorized schedule difference.",
    "input_changed": "A validated source input changed during validation.",
    "cleanup_failed": "Validation cleanup failed.",
    "finalization_deadline_overrun": "Mandatory finalization exceeded its reserved deadline.",
    "validation_interrupted": "Validation was interrupted.",
    "stale_stage_cleanup_failed": "A verified stale Director stage could not be cleaned.",
    "stale_unit_not_absent": "A stale Director transient unit could not be proven absent.",
    "stale_stage_scan_limit": "The stale-stage scan exceeded its safety limit.",
    "stale_stage_ambiguous": "Stale Director stage identities are ambiguous.",
    "guide_loading_failed": "Read-only guide loading failed.",
    "guide_validation_failed": "Guide validation failed.",
    "internal_error": "An internal validation error occurred.",
    "internal_warning": "A validation warning occurred.",
}
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
SAFE_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,99}\Z")
SAFE_EXCEPTION_CLASSES = {
    "DualRunError", "SingleRunError", "ProtocolError", "NormalizationError",
    "ScheduleComparisonError", "ReportError", "TimeoutError", "OSError",
    "RuntimeError", "ValueError", "SQLiteError",
}
POSIX_ABSOLUTE_RE = re.compile(r"(?:^|[\s\"'=:(])/(?:[^/\s]+(?:/|\Z))")
WINDOWS_ABSOLUTE_RE = re.compile(r"(?:^|[\s\"'=:(])(?:[A-Za-z]:[\\/]|\\\\)")
SECRET_RE = re.compile(
    r"(?i)(authorization|bearer|cookie|password|passwd|private[ _-]?key|api[ _-]?key|"
    r"access[ _-]?token|refresh[ _-]?token|secret|environment(?: variables?)?)"
)


class ReportError(RuntimeError):
    def __init__(self, code, message, *, publication_state="not_published"):
        super().__init__(message)
        self.code = code
        self.publication_state = publication_state


def _raise_if_cancelled(exc):
    if isinstance(exc, KeyboardInterrupt) or getattr(exc, "is_validation_cancellation", False):
        raise exc


def create_validation_run_id():
    current = datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ReportError("invalid_completion_time", "completion clock must be aware")
    stamp = current.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"v-{stamp}-{secrets.token_hex(16)}"


def _canonical_json(value):
    return (json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                       separators=(",", ":")) + "\n").encode("utf-8")


def _safe_text(value):
    text = str(value)
    result = []
    for character in text[:MAX_REPORT_TEXT_FIELD]:
        code = ord(character)
        if character == "\n": result.append("\\n")
        elif character == "\r": result.append("\\r")
        elif character == "\t": result.append("\\t")
        elif unicodedata.category(character).startswith("C") or code in (0x2028, 0x2029):
            result.append(f"\\u{code:04x}" if code <= 0xffff else f"\\U{code:08x}")
        else: result.append(character)
    return "".join(result)


def _diagnostic_digest(value):
    try:
        raw = _canonical_json(value)
    except Exception:
        raw = type(value).__name__.encode("ascii", "replace")
    return hashlib.sha256(b"FS42-C3A2-DIAGNOSTIC\0" + raw).hexdigest()


def _safe_identifier(value):
    value = str(value or "")
    return value if SAFE_IDENTIFIER_RE.fullmatch(value) else None


def validate_report_document(report, *, retained=False):
    """Validate a new v8 report or a retained immutable v1-v7 report."""
    if not isinstance(report, dict):
        raise ReportError("invalid_report", "validation report must be an object")
    version = report.get("schema_version")
    if version == 8:
        schema = REPORT_SCHEMA_V8
    elif retained and version in (1, 2, 3, 4, 5, 6, 7):
        schema = {1: REPORT_SCHEMA_V1, 2: REPORT_SCHEMA_V2,
                  3: REPORT_SCHEMA_V3, 4: REPORT_SCHEMA_V4, 5: REPORT_SCHEMA_V5, 6: REPORT_SCHEMA_V6,
                  7: REPORT_SCHEMA_V7}[version]
    else:
        raise ReportError("invalid_report_version", "unsupported validation report version")
    validate_document(report, schema)
    if version in (5, 6, 7, 8):
        from station_director.c1_diagnostics import validate_host_diagnostic
        for group in ("baseline_findings", "unexpected_differences", "warnings", "errors"):
            for finding in report["findings"][group]:
                if finding["template"] != DIAGNOSTIC_TEMPLATES.get(finding["code"]):
                    raise ReportError("invalid_report", "report diagnostic template mismatch")
                c1 = finding["c1_diagnostic"]
                if finding["code"] == "c1_run_failed":
                    try:
                        validate_host_diagnostic(c1["detail"], legacy=version == 5)
                    except (KeyError, TypeError, ValueError) as exc:
                        raise ReportError("invalid_report", "invalid C1 report diagnostic") from exc
                    if finding["phase"] != f"run_{c1['run']}":
                        raise ReportError("invalid_report", "C1 report run/phase mismatch")
                    if c1["detail"]["scheduler_invoked"] != report["validation"][
                            "scheduler_invoked"][f"run_{c1['run']}"]:
                        raise ReportError("invalid_report", "C1 scheduler state mismatch")
                elif c1 is not None:
                    raise ReportError("invalid_report", "unexpected C1 report diagnostic")


def _structured_finding(value, default_code, default_phase):
    """Project an untrusted diagnostic without retaining its free text."""
    value = value if isinstance(value, dict) else {}
    supplied_code = _safe_identifier(value.get("code"))
    code = supplied_code if supplied_code in DIAGNOSTIC_TEMPLATES else default_code
    if code not in DIAGNOSTIC_TEMPLATES:
        code = "internal_error"
    phase = _safe_identifier(value.get("phase"))
    if phase not in SAFE_PHASES:
        phase = default_phase if default_phase in SAFE_PHASES else "report"
    channel = value.get("channel")
    if isinstance(channel, bool) or not isinstance(channel, (int, type(None))):
        channel = None
    count = value.get("count")
    if isinstance(count, bool) or not isinstance(count, (int, type(None))) or (
            isinstance(count, int) and count < 0):
        count = None
    exception_class = _safe_identifier(value.get("exception_class") or value.get("category"))
    if exception_class not in SAFE_EXCEPTION_CLASSES:
        exception_class = None
    capture_failure_kind = _safe_identifier(value.get("capture_failure_kind"))
    if code == "source_capture_failed":
        if capture_failure_kind not in CAPTURE_FAILURE_KINDS:
            raise ReportError(
                "invalid_report_input", "source capture failure lacks a classified kind")
        channel = None
        count = None
        exception_class = None
    else:
        capture_failure_kind = None
    capture_run = value.get("capture_run")
    finalization_subphase = _safe_identifier(value.get("finalization_subphase"))
    detailed_finalization = (
        capture_failure_kind == "single_run_finalization"
        or code in {"proposal_has_no_effects", "source_changed",
                    "duplicate_stage_file"}
    )
    if detailed_finalization:
        if (capture_run not in (1, 2)
                or finalization_subphase not in FINALIZATION_SUBPHASES):
            raise ReportError(
                "invalid_report_input",
                "finalization failure lacks safe run and subphase")
    else:
        capture_run = None
        finalization_subphase = None
    c1_diagnostic = None
    if code == "c1_run_failed":
        from station_director.c1_diagnostics import validate_host_diagnostic
        wrapped = value.get("c1_diagnostic")
        launcher = value.get("launcher_summary")
        if (not isinstance(wrapped, dict) or wrapped.get("run") not in (1, 2)
                or not isinstance(launcher, dict)):
            raise ReportError("invalid_report_input", "C1 failure lacks safe diagnostics")
        validate_host_diagnostic(wrapped.get("detail"))
        c1_diagnostic = {
            "run": wrapped["run"], "detail": dict(wrapped["detail"]),
            "launcher_summary": {
                key: launcher[key] for key in (
                    "outcome", "stdout_bytes", "stderr_bytes",
                    "stdout_truncated", "stderr_truncated",
                    "termination_kind", "exit_status", "signal")},
        }
    from station_director.schedule_artifact import candidate_export_category
    finding = {
        "candidate_export_category": candidate_export_category(value),
        "code": code,
        "phase": phase,
        "template": DIAGNOSTIC_TEMPLATES[code],
        "channel": channel,
        "count": count,
        "exception_class": exception_class,
        "capture_failure_kind": capture_failure_kind,
        "capture_run": capture_run,
        "finalization_subphase": finalization_subphase,
        "c1_diagnostic": c1_diagnostic,
    }
    digest_input = ({
        "code": finding["code"], "phase": finding["phase"],
        "capture_failure_kind": finding["capture_failure_kind"],
        "capture_run": finding["capture_run"],
        "finalization_subphase": finding["finalization_subphase"],
        "template": finding["template"],
    } if code == "source_capture_failed" or detailed_finalization else (
        {"code": code, "phase": phase, "template": finding["template"],
         "c1_diagnostic": c1_diagnostic} if code == "c1_run_failed" else value))
    finding["diagnostic_digest"] = _diagnostic_digest(digest_input)
    return finding


def _contains_unsafe_diagnostic(value):
    scrubbed = value.replace("crt-media:/", "crt-media:")
    return bool(POSIX_ABSOLUTE_RE.search(scrubbed) or WINDOWS_ABSOLUTE_RE.search(scrubbed)
                or SECRET_RE.search(scrubbed))


def _reject_unsafe_paths(value, field="report"):
    if isinstance(value, dict):
        for key, child in value.items():
            _reject_unsafe_paths(child, f"{field}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_unsafe_paths(child, f"{field}[{index}]")
    elif isinstance(value, str):
        lowered = field.casefold()
        if "media_identity" in lowered:
            if not value.startswith("crt-media:/"):
                raise ReportError("unsafe_report_value", f"unsafe media identity in {field}")
            return
        if any(token in lowered for token in ("template", "message", "detail")):
            if _contains_unsafe_diagnostic(value):
                raise ReportError("unsafe_report_value", f"unsafe diagnostic in {field}")
        if "path" in lowered or "artifact" in lowered:
            if value.startswith("crt-media:/"):
                return
            if _contains_unsafe_diagnostic(value) or value.startswith("/"):
                raise ReportError("unsafe_report_value", f"unsafe path in {field}")


def _named_by(values, key, accepted):
    result = {name: None for name in accepted}
    for value in values:
        identity = value.get(key) if isinstance(value, dict) else None
        name = f"run_{identity}" if key == "index" or key == "run" else identity
        if name not in result or result[name] is not None:
            raise ReportError("invalid_report_input", f"duplicate or unexpected {key}")
        result[name] = value
    return result


def _phase_states(c2_result, runs, guide, reproducibility, comparison, cleanup, stability):
    states = {name: "unavailable" for name in (
        "capture", "run_1", "run_2", "guide_validation", "reproducibility",
        "baseline_comparison", "cleanup", "source_stability",
    )}
    after_capture = stability["after_capture"]
    states["capture"] = ("completed" if after_capture and after_capture.get("passed")
                         else "failed" if c2_result.get("failure") else "not_started")
    for name in ("run_1", "run_2"):
        if runs[name] is not None:
            states[name] = "completed" if runs[name].get("status") == "success" else "failed"
        elif c2_result.get("phase_reached") == name or c2_result.get("scheduler_invoked", {}).get(name):
            states[name] = "failed" if c2_result.get("failure") else "attempted"
    if guide["comparison"]["status"] == "passed":
        states["guide_validation"] = "completed"
    elif guide["comparison"]["status"] == "failed":
        states["guide_validation"] = "failed"
    if reproducibility is not None:
        states["reproducibility"] = "completed" if reproducibility.get("passed") else "failed"
    if comparison is not None:
        states["baseline_comparison"] = (
            "completed" if comparison.get("status") == "pass" and comparison.get("runs_matched")
            else "failed")
    cleanup_values = [item for item in cleanup.values() if item is not None]
    if cleanup_values:
        states["cleanup"] = ("completed" if len(cleanup_values) == 2 and
                             all(item.get("passed") for item in cleanup_values) else "failed")
    stability_values = [item for item in stability.values() if item is not None]
    if stability_values:
        states["source_stability"] = (
            "completed" if len(stability_values) == 4 and
            all(item.get("passed") for item in stability_values) else "failed")
    return states


def build_validation_report(proposal, c2_result, run_id, completed_at):
    """Pure bounded projection. It performs no filesystem/database access."""
    validate_document(c2_result, DUAL_RESULT_SCHEMA)
    if not PROPOSAL_ID_RE.fullmatch(proposal.get("proposal_id", "")):
        raise ReportError("invalid_proposal_id", "invalid canonical proposal ID")
    if not RUN_ID_RE.fullmatch(run_id):
        raise ReportError("invalid_run_id", "invalid validation run ID")
    comparison = c2_result.get("baseline_comparison")
    reproducibility = c2_result.get("reproducibility")
    runs = _named_by(c2_result.get("runs", []), "index", ("run_1", "run_2"))
    cleanup_source = _named_by(c2_result.get("cleanup", []), "run", ("run_1", "run_2"))
    source_source = _named_by(c2_result.get("source_checks", []), "checkpoint", (
        "after_capture", "between_runs", "after_run_2", "before_success"))

    def guide_projection(item, expected_run):
        if item is None:
            return None
        source = item.get("guide_validation") or {}
        return {"run": expected_run, "status": source.get("status"),
                "digest": source.get("digest"), "record_count": source.get("record_count"),
                "byte_count": source.get("byte_count")}

    failure_code = (c2_result.get("failure") or {}).get("code")
    guide_runs = {
        "run_1": guide_projection(runs["run_1"], 1),
        "run_2": guide_projection(runs["run_2"], 2),
    }
    guide_passed = all(item is not None and item.get("status") == "pass"
                       for item in guide_runs.values())
    guide_comparison = "passed" if guide_passed and reproducibility and reproducibility.get("passed") else (
        "failed" if failure_code == "guide_reproducibility_mismatch" else "unavailable")
    guide = {**guide_runs, "comparison": {"status": guide_comparison}}

    cleanup = {}
    for name, item in cleanup_source.items():
        cleanup[name] = None if item is None else {
            "run": int(name[-1]), "passed": item.get("passed"),
            "quarantined": item.get("quarantined"),
            "diagnostic": _structured_finding(
                {"code": "cleanup_failed" if not item.get("passed") else "internal_warning",
                 "phase": "cleanup", "category": item.get("detail")},
                "cleanup_failed" if not item.get("passed") else "internal_warning", "cleanup")
                if not item.get("passed") else None,
        }
    stability = {}
    for name, item in source_source.items():
        stability[name] = None if item is None else {
            "checkpoint": name, "passed": item.get("passed"),
            "changed_categories": [identifier for identifier in (
                _safe_identifier(value) for value in item.get("changed_categories", []))
                if identifier is not None][:5],
        }
    phases = _phase_states(c2_result, runs, guide, reproducibility, comparison, cleanup, stability)
    success = c2_result.get("status") == "success"
    empty_resulting = {key: 0 for key in (
        "changed_channels", "unchanged_blocks", "replaced_pairs", "removed_blocks",
        "generated_blocks", "reshaped_components", "title_changes",
        "selected_media_changes", "playback_plan_changes", "block_type_changes",
        "sequence_changes", "break_changes",
    )}
    report = {
        "schema_version": 8,
        "report_type": "station_director_validation",
        "software": {"director_version": __version__, "report_schema_version": 8},
        "proposal": {"id": proposal["proposal_id"], "schema_version": proposal["schema_version"],
                     "digest": hashlib.sha256(_canonical_json(proposal)).hexdigest()},
        "validation": {
            "run_id": run_id, "status": "success" if success else "failed",
            "phase_reached": c2_result.get("phase_reached", "capture"),
            "scheduler_invoked": c2_result.get("scheduler_invoked",
                                                {"run_1": "unknown", "run_2": "unknown"}),
            "live_inputs_changed": any(item is not None and not item["passed"]
                                       for item in stability.values()),
            "completed_at": completed_at,
        },
        "phases": phases,
        "context": {
            "requested_seed": c2_result.get("validation_context", {}).get("requested_seed"),
            "effective_seed": c2_result.get("validation_context", {}).get("effective_seed"),
            "reference_clock": c2_result.get("validation_context", {}).get("reference_clock"),
            "timezone": c2_result.get("validation_context", {}).get("timezone", "America/Los_Angeles"),
            "proposal_start": proposal.get("week_start"), "proposal_end": proposal.get("week_end"),
        },
        "channels": [] if comparison is None else [
            {key: value for key, value in channel.items() if key != "channel_seed"}
            for channel in comparison.get("channels", [])],
        "channel_seeds": [] if comparison is None else [
            {"number": channel["number"], "channel_seed": channel["channel_seed"]}
            for channel in comparison.get("channels", [])],
        "requested_configuration_effects": [] if comparison is None else comparison.get(
            "requested_configuration_effects", []),
        "resulting_schedule_changes": empty_resulting if comparison is None else comparison.get(
            "resulting_schedule_changes", empty_resulting),
        "baseline_comparison": None if comparison is None else {
            "status": comparison.get("status"), "digest": comparison.get("digest"),
            "runs_matched": comparison.get("runs_matched"),
            "run_1_digest": comparison.get("run_1_digest"),
            "run_2_digest": comparison.get("run_2_digest")},
        "reproducibility": None if reproducibility is None else {
            key: reproducibility[key] for key in (
                "passed", "run_1_digest", "run_2_digest", "run_1_record_count",
                "run_2_record_count", "changed_records", "added_records", "removed_records",
                "differences_truncated")},
        "guide_validation": guide,
        "preservation": {
            name: None if item is None else {"run": int(name[-1]), **item.get("preservation_summary", {})}
            for name, item in runs.items()},
        "source_stability": stability,
        "findings": {
            "baseline_findings": [_structured_finding(item, item.get("code", "internal_error"), "baseline_comparison")
                                  for channel in ([] if comparison is None else comparison.get("channels", []))
                                  for item in channel.get("baseline_findings", [])],
            "requested_configuration_effects": [] if comparison is None else comparison.get("requested_configuration_effects", []),
            "resulting_schedule_changes": [] if comparison is None else [comparison.get("resulting_schedule_changes", {})],
            "unexpected_differences": [_structured_finding(item, item.get("code", "internal_error"), "baseline_comparison")
                                       for item in ([] if comparison is None else comparison.get("unexpected_differences", []))],
            "warnings": [_structured_finding(item, "internal_warning", f"run_{index}")
                         for index, run in enumerate((runs["run_1"], runs["run_2"]), 1)
                         if run is not None for item in run.get("warnings", [])],
            "errors": [] if c2_result.get("failure") is None else [
                _structured_finding(c2_result["failure"], "internal_error",
                                    c2_result.get("phase_reached", "report"))],
        },
        "timings_ms": {"total": c2_result.get("timings_ms", {}).get("total", 0)},
        "cleanup": cleanup,
        "publication": {"immutable": "pending", "latest": "pending"},
    }
    _reject_unsafe_paths(report)
    validate_report_document(report)
    raw = _canonical_json(report)
    if len(raw) > MAX_REPORT_JSON_BYTES:
        raise ReportError("report_too_large", "validation report exceeds size limit")
    return report


def render_validation_text(report):
    """Deterministically render only an already validated report object."""
    validate_report_document(report)
    lines = [
        "Station Director validation",
        f"Run: {_safe_text(report['validation']['run_id'])}",
        f"Proposal: {_safe_text(report['proposal']['id'])}",
        f"Status: {_safe_text(report['validation']['status'])}",
        f"Phase: {_safe_text(report['validation']['phase_reached'])}",
        f"Director version: {_safe_text(report['software']['director_version'])}",
        f"Scheduler invoked: run 1={str(report['validation']['scheduler_invoked']['run_1']).lower()}, run 2={str(report['validation']['scheduler_invoked']['run_2']).lower()}",
        f"Live inputs changed: {str(report['validation']['live_inputs_changed']).lower()}",
        "",
        "Channels:",
    ]
    for channel in report["channels"]:
        counts = channel["counts"]
        lines.append(
            f"  {channel['number']} {_safe_text(channel['name'])}: "
            f"unchanged={counts['unchanged_blocks']} replaced={counts['replaced_pairs']} "
            f"removed={counts['removed_blocks']} generated={counts['generated_blocks']} "
            f"reshaped={counts['reshaped_components']}"
        )
    for category in ("baseline_findings", "unexpected_differences", "warnings", "errors"):
        values = report["findings"][category]
        lines.extend(("", category.replace("_", " ").title() + f" ({len(values)}):"))
        for value in values[:50]:
            lines.append("  " + _safe_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                                       separators=(",", ":"))))
        if len(values) > 50:
            lines.append("  [truncated]")
    if any(((item.get("c1_diagnostic") or {}).get("detail") or {}).get("preservation_detail")
           for group in report["findings"].values() for item in group):
        lines.append("Preservation detail identifies the first preservation failure; it may be secondary to the primary diagnostic.")
    raw = ("\n".join(lines) + "\n").encode("utf-8")
    if len(raw) > MAX_REPORT_TEXT_BYTES:
        raise ReportError("text_report_too_large", "text report exceeds size limit")
    return raw


def _bounded_entries(directory_fd):
    # Open a fresh file description so repeated scans never share/consume the
    # held directory descriptor's seek position.
    scan_fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                      dir_fd=directory_fd)
    iterator = os.scandir(scan_fd)
    try:
        for count, entry in enumerate(iterator, 1):
            if count > MAX_REPORT_ENTRIES:
                raise ReportError("report_entry_limit", "report directory entry limit exceeded")
            yield entry
    finally:
        iterator.close()
        os.close(scan_fd)


def _validate_directory_info(info, label, *, private):
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise ReportError("unsafe_report_directory", f"unsafe report directory: {label}")
    if mode & 0o022 or (private and mode != 0o700):
        raise ReportError("unsafe_report_directory", f"unsafe report directory mode: {label}")


def _open_component(parent_fd, name, *, create, private):
    found = None
    folded = name.casefold()
    for entry in _bounded_entries(parent_fd):
        if entry.name.casefold() == folded:
            if entry.name != name:
                raise ReportError("case_ambiguous_path", f"case-ambiguous report path: {name}")
            found = entry.stat(follow_symlinks=False)
    if found is None:
        if not create:
            raise ReportError("missing_report_path", f"missing report path: {name}")
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        except FileExistsError:
            # A concurrent Director publisher may have created the same fixed
            # component. The no-follow open and full identity checks below are
            # still authoritative.
            pass
        found = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                         dir_fd=parent_fd)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (found.st_dev, found.st_ino):
            raise ReportError("report_path_replaced", f"report path changed: {name}")
        _validate_directory_info(opened, name, private=private)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_report_parent(proposal_id):
    if not PROPOSAL_ID_RE.fullmatch(proposal_id):
        raise ReportError("invalid_proposal_id", "invalid canonical proposal ID")
    project = Path(PROJECT_ROOT)
    expected = project / "runtime" / "director" / "validations"
    if Path(VALIDATIONS_ROOT) != expected or not project.is_absolute():
        raise ReportError("unsafe_report_root", "report root is not internally derived")
    descriptor = os.open(project, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        _validate_directory_info(os.fstat(descriptor), "project", private=False)
        for name, private in (("runtime", False), ("director", True), ("validations", True)):
            next_descriptor = _open_component(
                descriptor, name, create=True, private=private)
            os.close(descriptor)
            descriptor = next_descriptor
        proposal_fd = _open_component(
            descriptor, proposal_id, create=True, private=True)
        os.close(descriptor)
        return proposal_fd
    except Exception:
        try: os.close(descriptor)
        except OSError: pass
        raise


def _write_file(directory_fd, name, raw):
    descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=directory_fd)
    try:
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view):]
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed = bytearray()
        while len(observed) < len(raw):
            chunk = os.read(descriptor, min(65536, len(raw) - len(observed)))
            if not chunk:
                break
            observed.extend(chunk)
        if bytes(observed) != raw or os.read(descriptor, 1):
            raise ReportError("report_verification_failed", f"report file changed: {name}")
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise ReportError("unsafe_report_file", f"unsafe report file: {name}")
    finally:
        os.close(descriptor)


def _validate_report_parent(parent_fd):
    seen = set()
    for entry in _bounded_entries(parent_fd):
        name = entry.name
        folded = name.casefold()
        if folded in seen:
            raise ReportError("case_ambiguous_path", "case-ambiguous report entry")
        seen.add(folded)
        info = entry.stat(follow_symlinks=False)
        if name == "latest.json":
            continue
        if name.startswith(".tmp-") or name.startswith(".latest-"):
            raise ReportError("stale_report_temporary", "stale report temporary exists")
        if not RUN_ID_RE.fullmatch(name) or not stat.S_ISDIR(info.st_mode):
            raise ReportError("unexpected_report_entry", "unexpected report entry")
        child = _open_component(parent_fd, name, create=False, private=True)
        try:
            names = []
            for child_entry in _bounded_entries(child):
                names.append(child_entry.name)
                child_info = child_entry.stat(follow_symlinks=False)
                if (not stat.S_ISREG(child_info.st_mode) or child_info.st_nlink != 1
                        or child_info.st_uid != os.geteuid()
                        or stat.S_IMODE(child_info.st_mode) != 0o600):
                    raise ReportError("unsafe_report_file", "immutable report file is unsafe")
            if names != ["validation.json", "validation.txt"] and set(names) != {
                    "validation.json", "validation.txt"}:
                raise ReportError("invalid_immutable_report", "immutable report contents invalid")
        finally:
            os.close(child)


def _rename_noreplace(source_fd, source, target_fd, target):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "renameat2", None)
    if function is None:
        raise ReportError("noreplace_unavailable", "renameat2(RENAME_NOREPLACE) unavailable")
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
                         ctypes.c_uint]
    function.restype = ctypes.c_int
    for unused in range(RENAME_EINTR_RETRIES):
        if function(source_fd, os.fsencode(source), target_fd, os.fsencode(target),
                    RENAME_NOREPLACE) == 0:
            return
        error = ctypes.get_errno()
        if error != errno.EINTR:
            raise OSError(error, os.strerror(error))
    raise ReportError("rename_interrupted", "renameat2 retry limit exceeded")



def _safe_remove_temp(parent_fd, name):
    try:
        child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=parent_fd)
    except FileNotFoundError:
        return
    removable = []
    try:
        info = os.fstat(child)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            return
        for entry in _bounded_entries(child):
            if entry.name not in ("validation.json", "validation.txt"):
                return
            entry_info = entry.stat(follow_symlinks=False)
            if (not stat.S_ISREG(entry_info.st_mode) or entry_info.st_nlink != 1
                    or entry_info.st_uid != os.geteuid()
                    or stat.S_IMODE(entry_info.st_mode) != 0o600):
                return
            removable.append(entry.name)
        for entry in removable:
            os.unlink(entry, dir_fd=child)
    finally:
        os.close(child)
    os.rmdir(name, dir_fd=parent_fd)


def _read_private_file(directory_fd, name, limit):
    before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > limit):
        raise ReportError("unsafe_report_file", f"unsafe report file: {name}")
    descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                         dir_fd=directory_fd)
    try:
        opened = os.fstat(descriptor)
        if ((opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                or opened.st_size > limit):
            raise ReportError("report_path_replaced", f"report file changed: {name}")
        chunks = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65536, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > limit:
                raise ReportError("report_too_large", f"report file is oversized: {name}")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size) != (
                opened.st_dev, opened.st_ino, opened.st_size):
            raise ReportError("report_path_replaced", f"report file changed: {name}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _validate_pointer_target(parent_fd, pointer, proposal_id):
    if pointer["proposal_id"] != proposal_id:
        raise ReportError("invalid_latest", "latest proposal identity mismatch")
    run_id = pointer["run_id"]
    if not RUN_ID_RE.fullmatch(run_id):
        raise ReportError("invalid_latest", "latest run identity is invalid")
    run_fd = _open_component(parent_fd, run_id, create=False, private=True)
    try:
        names = set()
        for entry in _bounded_entries(run_fd):
            names.add(entry.name)
        if names != {"validation.json", "validation.txt"}:
            raise ReportError("invalid_latest", "latest run directory is incomplete")
        raw = _read_private_file(run_fd, "validation.json", MAX_REPORT_JSON_BYTES)
        _read_private_file(run_fd, "validation.txt", MAX_REPORT_TEXT_BYTES)
        try:
            report = strict_json_loads(raw)
            validate_report_document(report, retained=True)
        except Exception as exc:
            raise ReportError("invalid_latest", "latest report is invalid") from exc
        if report["proposal"]["id"] != proposal_id or report["validation"]["run_id"] != run_id:
            raise ReportError("invalid_latest", "latest report identity mismatch")
        if raw != _canonical_json(report):
            raise ReportError("invalid_latest", "latest report is not canonical")
        if hashlib.sha256(raw).hexdigest() != pointer["validation_json_digest"]:
            raise ReportError("invalid_latest", "latest report digest mismatch")
    finally:
        os.close(run_fd)


def _publish_latest(parent_fd, proposal_id, run_id, digest):
    pointer = {"schema_version": 1, "proposal_id": proposal_id, "run_id": run_id,
               "validation_json_digest": digest}
    validate_document(pointer, LATEST_SCHEMA)
    raw = _canonical_json(pointer)
    try:
        before = os.stat("latest.json", dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        before = None
    if before is not None:
        try:
            existing_raw = _read_private_file(parent_fd, "latest.json", 4096)
            existing_value = strict_json_loads(existing_raw)
            validate_document(existing_value, LATEST_SCHEMA)
            _validate_pointer_target(parent_fd, existing_value, proposal_id)
            after = os.stat("latest.json", dir_fd=parent_fd, follow_symlinks=False)
            if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
                raise ReportError("report_path_replaced", "latest pointer changed")
        except Exception as exc:
            _raise_if_cancelled(exc)
            if isinstance(exc, ReportError):
                raise
            raise ReportError("invalid_latest", "existing latest.json is invalid") from exc
    temp = ".latest-" + secrets.token_hex(16)
    created = False
    replaced = False
    try:
        _write_file(parent_fd, temp, raw)
        created = True
        os.replace(temp, "latest.json", src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        created = False
        replaced = True
        os.fsync(parent_fd)
    except BaseException as exc:
        state = "latest_replaced_not_durable" if replaced else "latest_not_replaced"
        code = ("validation_interrupted" if isinstance(exc, KeyboardInterrupt)
                or getattr(exc, "is_validation_cancellation", False)
                else "latest_publication_failed")
        raise ReportError(code, state, publication_state=state) from exc
    finally:
        if created:
            try: os.unlink(temp, dir_fd=parent_fd)
            except FileNotFoundError: pass


def publish_validation_report(report):
    """Explicitly publish an immutable two-file report and optional latest pointer."""
    validate_report_document(report)
    proposal_id = report["proposal"]["id"]
    run_id = report["validation"]["run_id"]
    if not PROPOSAL_ID_RE.fullmatch(proposal_id) or not RUN_ID_RE.fullmatch(run_id):
        raise ReportError("invalid_identity", "invalid report identity")
    json_bytes = _canonical_json(report)
    text_bytes = render_validation_text(report)
    if len(json_bytes) > MAX_REPORT_JSON_BYTES or len(text_bytes) > MAX_REPORT_TEXT_BYTES:
        raise ReportError("report_too_large", "report output exceeds limit")
    parent_fd = temp_fd = None
    temp_name = ".tmp-" + secrets.token_hex(16)
    published = False
    durable = False
    temp_created = False
    try:
        parent_fd = _open_report_parent(proposal_id)
        fcntl.flock(parent_fd, fcntl.LOCK_EX)
        _validate_report_parent(parent_fd)
        os.mkdir(temp_name, 0o700, dir_fd=parent_fd)
        temp_created = True
        temp_fd = os.open(temp_name, os.O_RDONLY | os.O_DIRECTORY
                          | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)
        temp_info = os.fstat(temp_fd)
        if (not stat.S_ISDIR(temp_info.st_mode) or temp_info.st_uid != os.geteuid()
                or stat.S_IMODE(temp_info.st_mode) != 0o700):
            raise ReportError("unsafe_temporary_directory", "unsafe temporary report directory")
        _write_file(temp_fd, "validation.json", json_bytes)
        _write_file(temp_fd, "validation.txt", text_bytes)
        os.fsync(temp_fd)
        os.close(temp_fd); temp_fd = None
        try:
            _rename_noreplace(parent_fd, temp_name, parent_fd, run_id)
        except OSError as exc:
            code = "run_id_collision" if exc.errno == errno.EEXIST else "publication_failed"
            raise ReportError(code, "immutable directory publication failed") from exc
        published = True
        try:
            os.fsync(parent_fd)
            durable = True
        except OSError as exc:
            raise ReportError("publication_durability_failed",
                              "immutable report published but parent fsync failed",
                              publication_state="published_not_durable") from exc
        digest = hashlib.sha256(json_bytes).hexdigest()
        latest = {"status": "updated", "code": None,
                  "publication_state": "latest_durable"}
        try:
            _publish_latest(parent_fd, proposal_id, run_id, digest)
        except ReportError as exc:
            state = ("latest_not_replaced" if exc.publication_state == "not_published"
                     else exc.publication_state)
            latest = {"status": "warning", "code": exc.code,
                      "publication_state": state}
        except Exception as exc:
            _raise_if_cancelled(exc)
            latest = {"status": "warning", "code": "internal_error",
                      "publication_state": "latest_not_replaced"}
        return {"publication_state": "published_durable", "proposal_id": proposal_id,
                "run_id": run_id, "validation_json_digest": digest, "latest": latest}
    except ReportError:
        raise
    except BaseException as exc:
        interrupted = (isinstance(exc, KeyboardInterrupt)
                       or getattr(exc, "is_validation_cancellation", False))
        state = ("published_durable" if durable else
                 "published_not_durable" if published else "not_published")
        raise ReportError(
            "validation_interrupted" if interrupted else "publication_failed",
            "report publication interrupted" if interrupted else type(exc).__name__,
            publication_state=state,
        ) from exc
    finally:
        if temp_fd is not None: os.close(temp_fd)
        if parent_fd is not None:
            if temp_created and not published: _safe_remove_temp(parent_fd, temp_name)
            try: fcntl.flock(parent_fd, fcntl.LOCK_UN)
            except OSError: pass
            os.close(parent_fd)


def create_and_publish_validation_report(proposal, c2_result):
    """Allocate an unpredictable identity and publish with bounded retries."""
    current = datetime.now(timezone.utc)
    for unused in range(MAX_RUN_ID_ATTEMPTS):
        run_id = create_validation_run_id()
        completed_at = current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        report = build_validation_report(proposal, c2_result, run_id, completed_at)
        try:
            return report, publish_validation_report(report)
        except ReportError as exc:
            if exc.code != "run_id_collision":
                raise
    raise ReportError("run_id_collision_exhausted", "validation run ID retries exhausted")
