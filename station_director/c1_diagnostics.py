"""Dependency-free, allowlisted diagnostics for the isolated C1 protocol."""

C1_RESPONSE_VERSION = 3
C2_RESULT_VERSION = 3
REPORT_VERSION = 5

PROBE_IDENTIFIERS = (
    "environment_sanitized", "host_home_not_exposed", "host_run_not_exposed",
    "user_bus_not_exposed", "proc_private", "dev_private", "tmp_private",
    "project_mount_read_only", "media_mount_read_only", "staging_mount_writable",
    "ipv4_blocked", "ipv6_blocked", "loopback_4242_blocked", "af_unix_path_safe",
    "af_unix_round_trip", "staging_create", "staging_read", "staging_rename",
    "staging_delete",
)

FINGERPRINT_CATEGORIES = (
    "original_logical_configuration", "original_logical_database",
    "logical_media_manifest", "live_physical_configuration",
    "staged_source_physical_configuration", "projected_logical_configuration",
    "working_logical_database",
)

# code: (domain, permitted phases, fixed safe template)
DIAGNOSTIC_RULES = {
    "launcher_failed": ("launcher", ("launch",), "The isolated worker could not be launched."),
    "launcher_timeout": ("launcher", ("launch",), "The isolated worker timed out."),
    "worker_runtime_timeout": ("launcher", ("launch",), "The worker reached its runtime limit."),
    "worker_oom_kill": ("launcher", ("launch",), "The worker was terminated for memory exhaustion."),
    "worker_external_signal": ("launcher", ("launch",), "The worker was terminated by an external signal."),
    "worker_nonzero_exit": ("launcher", ("launch",), "The worker exited unsuccessfully."),
    "launcher_state_invalid": ("launcher", ("launch",), "The launcher state evidence is invalid."),
    "worker_response_missing": ("worker_protocol", ("response",), "The worker response is missing."),
    "worker_response_invalid": ("worker_protocol", ("response",), "The worker response is invalid."),
    "worker_response_publication_failed": ("worker_protocol", ("response",), "Worker response publication failed."),
    "worker_checkpoint_mismatch": ("worker_protocol", ("response",), "Worker checkpoint evidence contradicts the response."),
    "worker_checkpoint_incomplete": ("worker_protocol", ("response",), "Worker checkpoint publication is incomplete."),
    "worker_response_identity_mismatch": ("worker_protocol", ("response",), "The worker response identity does not match."),
    "worker_context_mismatch": ("worker_protocol", ("response",), "The worker validation context does not match."),
    "worker_channels_mismatch": ("worker_protocol", ("response",), "The worker channel set does not match."),
    "worker_exit_status_mismatch": ("worker_protocol", ("response",), "The worker exit status contradicts its response."),
    "isolation_probe_failed": ("isolation_probe", ("probes",), "An isolation probe failed."),
    "original_configuration_verification_failed": ("worker_verification", ("snapshot",), "Original configuration verification failed."),
    "original_database_verification_failed": ("worker_verification", ("snapshot",), "Original database verification failed."),
    "media_manifest_verification_failed": ("worker_verification", ("snapshot",), "Media manifest verification failed."),
    "physical_transition_verification_failed": ("worker_verification", ("snapshot",), "Physical source transition verification failed."),
    "projected_configuration_verification_failed": ("worker_verification", ("snapshot",), "Projected configuration verification failed."),
    "working_database_verification_failed": ("worker_verification", ("snapshot",), "Working database verification failed."),
    "request_integrity_failed": ("worker_verification", ("request",), "The held request failed integrity verification."),
    "validation_context_failed": ("worker_verification", ("seed",), "Validation context verification failed."),
    "timezone_verification_failed": ("worker_verification", ("seed",), "Worker timezone verification failed."),
    "hash_seed_verification_failed": ("worker_verification", ("seed",), "Worker hash-seed verification failed."),
    "effective_seed_verification_failed": ("worker_verification", ("seed",), "Effective-seed verification failed."),
    "native_import_failed": ("native_import", ("native_import",), "Native scheduling imports failed."),
    "native_warning": ("native_preparation", ("configuration", "catalog", "scheduler", "preservation", "guide"), "Native validation emitted a warning."),
    "missing_attestation": ("native_preparation", ("native_import",), "Native attestation is missing."),
    "invalid_configuration": ("native_preparation", ("configuration",), "Native scheduling configuration is invalid."),
    "protected_channel": ("native_preparation", ("configuration",), "A protected channel was selected."),
    "unknown_channel": ("native_preparation", ("configuration",), "An unknown channel was selected."),
    "native_configuration": ("native_preparation", ("configuration",), "Native configuration loading failed."),
    "unsupported_autobump": ("native_preparation", ("configuration",), "AutoBump is unsupported in validation."),
    "autobump_subprocess_required": ("native_preparation", ("configuration",), "AutoBump configuration requires a blocked subprocess."),
    "autobump_subprocess_blocked": ("native_scheduler", ("scheduler",), "AutoBump attempted a blocked subprocess."),
    "autobump_selected": ("native_scheduler", ("scheduler",), "AutoBump content was selected."),
    "invalid_playback_descriptor": ("native_scheduler", ("scheduler",), "Playback descriptor is invalid."),
    "staged_preparation": ("native_preparation", ("configuration", "preservation"), "Staged scheduling preparation failed."),
    "sequence_restore_failure": ("native_preservation", ("configuration", "catalog", "scheduler", "preservation", "guide"), "Sequence restoration failed."),
    "catalog_failure": ("native_catalog", ("catalog",), "Native catalog construction failed."),
    "catalog_reconciliation": ("native_catalog", ("catalog",), "Catalog reconciliation failed."),
    "native_system_exit": ("native_scheduler", ("native_import", "configuration", "catalog", "scheduler", "preservation", "guide"), "Native code exited unexpectedly."),
    "scheduler_failure": ("native_scheduler", ("scheduler",), "Native schedule generation failed."),
    "native_failure": ("native_scheduler", ("configuration", "catalog", "scheduler", "preservation", "guide"), "Native validation failed."),
    "preservation_failure": ("native_preservation", ("preservation",), "Post-scheduling preservation verification failed."),
    "guide_loading_failed": ("native_guide", ("guide",), "Read-only guide loading failed."),
    "guide_validation_failed": ("native_guide", ("guide",), "Guide validation failed."),
}

LAUNCH_OUTCOMES = ("completed", "nonzero_exit", "timed_out", "launch_error")
TERMINATION_KINDS = (
    "completed", "nonzero_exit", "runtime_timeout", "outer_watchdog",
    "oom_kill", "external_signal", "launcher_failure", "launcher_state_invalid",
    "sandbox_restriction",
)
MAX_LAUNCH_BYTE_COUNT = (1 << 63) - 1
WORKER_DIAGNOSTIC_CODES = frozenset(
    code for code, (domain, unused_phases, unused_template) in DIAGNOSTIC_RULES.items()
    if domain not in {"launcher", "worker_protocol"}
)


def make_diagnostic(code, phase, *, scheduler_invoked=False, probe=None,
                    fingerprint_category=None, channel_number=None):
    """Return one strictly validated, value-free diagnostic object."""
    if code not in DIAGNOSTIC_RULES:
        code = "native_failure"
    domain, phases, template = DIAGNOSTIC_RULES[code]
    if phase not in phases:
        raise ValueError("diagnostic phase/code mismatch")
    if probe is not None and (code != "isolation_probe_failed" or probe not in PROBE_IDENTIFIERS):
        raise ValueError("invalid diagnostic probe")
    if fingerprint_category is not None and (
            domain != "worker_verification" or
            fingerprint_category not in FINGERPRINT_CATEGORIES):
        raise ValueError("invalid fingerprint category")
    if channel_number is not None and (
            isinstance(channel_number, bool) or not isinstance(channel_number, int)
            or not 1 <= channel_number <= 8):
        raise ValueError("invalid diagnostic channel")
    return {
        "domain": domain, "phase": phase, "code": code, "template": template,
        "scheduler_invoked": bool(scheduler_invoked), "probe": probe,
        "fingerprint_category": fingerprint_category,
        "channel_number": channel_number,
    }


def validate_diagnostic(value):
    if not isinstance(value, dict) or set(value) != {
            "domain", "phase", "code", "template", "scheduler_invoked", "probe",
            "fingerprint_category", "channel_number"}:
        raise ValueError("invalid C1 diagnostic shape")
    expected = make_diagnostic(
        value["code"], value["phase"], scheduler_invoked=value["scheduler_invoked"],
        probe=value["probe"], fingerprint_category=value["fingerprint_category"],
        channel_number=value["channel_number"])
    if value != expected:
        raise ValueError("invalid C1 diagnostic content")


def launcher_summary(result, outcome=None):
    if outcome is None:
        outcome = ("timed_out" if result.timed_out else
                   "completed" if result.returncode == 0 else "nonzero_exit")
    if outcome not in LAUNCH_OUTCOMES:
        raise ValueError("invalid launcher outcome")
    termination = getattr(result, "termination_kind", "launcher_failure")
    if termination not in TERMINATION_KINDS:
        termination = "launcher_state_invalid"
    exit_status = getattr(result, "exit_status", None)
    signal_number = getattr(result, "signal", None)
    if isinstance(exit_status, bool) or not isinstance(exit_status, (int, type(None))):
        exit_status = None
    if isinstance(signal_number, bool) or not isinstance(signal_number, (int, type(None))):
        signal_number = None
    return {
        "outcome": outcome,
        "stdout_bytes": min(MAX_LAUNCH_BYTE_COUNT,
                            max(0, int(getattr(result, "stdout_bytes", 0)))),
        "stderr_bytes": min(MAX_LAUNCH_BYTE_COUNT,
                            max(0, int(getattr(result, "stderr_bytes", 0)))),
        "stdout_truncated": bool(getattr(result, "stdout_truncated", False)),
        "stderr_truncated": bool(getattr(result, "stderr_truncated", False)),
        "termination_kind": termination,
        "exit_status": exit_status,
        "signal": signal_number,
    }


def host_diagnostic(detail, scheduler_state):
    """Copy a validated value-free C1 diagnostic into the tri-state host shape."""
    validate_diagnostic(detail)
    if scheduler_state not in (True, False, "unknown"):
        raise ValueError("invalid host scheduler state")
    return {**detail, "scheduler_invoked": scheduler_state}


def validate_host_diagnostic(value):
    if not isinstance(value, dict) or value.get("scheduler_invoked") not in (
            True, False, "unknown"):
        raise ValueError("invalid host diagnostic scheduler state")
    worker_shape = {**value, "scheduler_invoked": value["scheduler_invoked"] is True}
    validate_diagnostic(worker_shape)
