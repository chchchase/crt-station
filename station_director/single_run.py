"""Internal C1 lifecycle for one isolated native run; not a public CLI API."""

import os
import json
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from station_director.isolation import (
    IsolationLauncher,
    check_invocation_context,
    cleanup_staging_directory,
    cleanup_unit,
    create_staging_directory,
    sandbox_python,
)
from station_director.preservation import (
    capture_media_manifest,
    fingerprint_and_clone_database,
    fingerprint_json_files,
    protected_json_paths,
)
from station_director.single_run_protocol import (
    HeldDocument,
    ProtocolError,
    RESPONSE_SCHEMA_V3 as RESPONSE_SCHEMA,
    bind_request,
    write_private_json_exclusive,
    REQUEST_SCHEMA,
)
from station_director.c1_diagnostics import launcher_summary, make_diagnostic
from station_director.worker_checkpoint import (
    WorkerCheckpointError,
    read_checkpoint_evidence,
)
from station_director.validation_context import (
    canonical_seed_inputs,
    derive_validation_context,
    logical_media_manifest_fingerprint,
    logical_protected_configuration_fingerprint,
)
from station_director.validation import project_configuration


REQUEST_NAME = "native-single-run.request.json"
RESPONSE_NAME = "native-single-run.response.json"
DEFAULT_TIMEOUT_SECONDS = 2730
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


class SingleRunError(RuntimeError):
    def __init__(self, phase, code, message, *, finalization_subphase=None,
                 c1_diagnostic=None, launcher=None, scheduler_state="unknown"):
        super().__init__(message)
        self.phase = phase
        self.code = code
        if (finalization_subphase is not None
                and finalization_subphase not in FINALIZATION_SUBPHASES):
            raise ValueError("invalid finalization subphase")
        self.finalization_subphase = finalization_subphase
        self.c1_diagnostic = c1_diagnostic
        self.launcher_summary = launcher
        if scheduler_state not in (True, False, "unknown"):
            raise ValueError("invalid scheduler state")
        self.scheduler_state = scheduler_state


class SingleRunFinalizationError(SingleRunError):
    """An uncoded finalization failure with no retained exception detail."""

    def __init__(self, subphase):
        super().__init__(
            "prepare", "single_run_finalization_failed",
            "Native single-run finalization failed.",
            finalization_subphase=subphase,
        )


def _finalization_step(subphase, operation):
    if subphase not in FINALIZATION_SUBPHASES:
        raise ValueError("invalid finalization subphase")
    try:
        return operation()
    except SingleRunError as exc:
        # Preserve an already validated C1 code while attaching the precise
        # finalization boundary at which it arose.  This is safe structured
        # context, not exception text.
        if exc.finalization_subphase is None:
            exc.finalization_subphase = subphase
        raise
    except Exception as exc:
        if getattr(exc, "is_validation_cancellation", False):
            raise
        raise SingleRunFinalizationError(subphase) from exc


@dataclass
class SingleRunLifecycle:
    project_root: Path
    stage: Path
    lock: object
    token: str
    run_id: str
    unit_name: str
    request: dict
    launcher_result: object = None
    response: dict = None
    cleaned: bool = False
    unit_settled: bool = False

    def settle_unit(self):
        """Prove the worker unit absent while retaining the locked stage."""
        if self.unit_settled:
            return {"passed": True, "detail": "unit_absent"}
        try:
            unit_ok, unit_detail = cleanup_unit(self.unit_name)
        except Exception as exc:
            unit_ok, unit_detail = False, str(exc)
        if not unit_ok:
            quarantine = self.stage / ".quarantine"
            quarantine.touch(mode=0o600, exist_ok=True)
            raise SingleRunError(
                "cleanup", "cleanup_failure",
                "worker unit could not be proven absent; staging was quarantined",
            )
        self.unit_settled = True
        return {"passed": True, "detail": "unit_absent"}

    def cleanup(self):
        if self.cleaned:
            return {"passed": True, "detail": "cleanup_complete"}
        details = []
        if (self.stage / ".quarantine").exists():
            raise SingleRunError(
                "cleanup", "cleanup_failure", "staging remains quarantined"
            )
        try:
            settled = self.settle_unit()
            unit_ok, unit_detail = True, settled["detail"]
        except Exception as exc:
            unit_ok, unit_detail = False, str(exc)
        details.append(f"unit: {unit_detail}")
        if not unit_ok:
            quarantine = self.stage / ".quarantine"
            quarantine.touch(mode=0o600, exist_ok=True)
            raise SingleRunError(
                "cleanup", "cleanup_failure",
                "worker unit could not be proven absent; staging was quarantined",
            )
        guide_input = self.stage / "guide-input"
        guide_database = guide_input / "guide.db"
        if guide_input.exists() and not guide_input.is_symlink():
            try:
                if guide_database.exists() and not guide_database.is_symlink():
                    os.chmod(guide_database, 0o600)
                os.chmod(guide_input, 0o700)
            except OSError as exc:
                try:
                    (self.stage / ".quarantine").touch(mode=0o600, exist_ok=True)
                except OSError:
                    pass
                raise SingleRunError(
                    "cleanup", "cleanup_failure",
                    f"guide snapshot could not be prepared for cleanup: {type(exc).__name__}",
                ) from exc
        stage_ok, stage_detail = cleanup_staging_directory(self.stage)
        details.append("stage: removed" if stage_ok else "stage: cleanup_failed")
        self.cleaned = unit_ok and stage_ok
        if self.cleaned:
            self.lock.close()
        if not self.cleaned:
            raise SingleRunError("cleanup", "cleanup_failure", "; ".join(details))
        return {"passed": True, "detail": "cleanup_complete"}


def _copy_private(source, target):
    if target.exists():
        raise SingleRunError("prepare", "duplicate_stage_file", str(target))
    descriptor = os.open(
        target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        with Path(source).open("rb") as handle:
            with os.fdopen(os.dup(descriptor), "wb") as target_handle:
                shutil.copyfileobj(handle, target_handle)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _finalize_prepared_single_run(
    project_root, stage, lock, token, run_id, proposal, policy, *,
    configuration_digest, database_digest, media_digest, live_physical_digest,
):
    """Bind a C1 request to an already-published immutable source snapshot."""
    staged_source = _finalization_step(
        "staged_physical_fingerprint", lambda: Path(stage) / "source")
    staged_physical = _finalization_step(
        "staged_physical_fingerprint",
        lambda: fingerprint_json_files(protected_json_paths(staged_source)),
    )
    staged_configuration = _finalization_step(
        "staged_logical_configuration_fingerprint",
        lambda: logical_protected_configuration_fingerprint(
            protected_json_paths(staged_source)),
    )
    def verify_staged_configuration():
        if staged_configuration["digest"] != configuration_digest:
            raise SingleRunError(
                "prepare", "source_changed",
                "staged configuration differs from captured source",
            )

    _finalization_step(
        "staged_logical_configuration_fingerprint",
        verify_staged_configuration,
    )
    seed_inputs = _finalization_step(
        "seed_input_construction",
        lambda: canonical_seed_inputs(
            configuration_digest, database_digest, media_digest),
    )
    context = _finalization_step(
        "validation_context_derivation",
        lambda: derive_validation_context(proposal, policy, seed_inputs),
    )

    def load_channel_configs():
        configs = {}
        for identity, path in protected_json_paths(staged_source).items():
            if not identity.startswith("confs/") or path.name == "main_config.json":
                continue
            value = json.loads(path.read_text(encoding="utf-8"))
            configs[value["station_conf"]["network_name"]] = value
        return configs

    configs = _finalization_step(
        "staged_channel_configuration_loading", load_channel_configs)
    unused_projected, affected, unused_sources = _finalization_step(
        "proposal_projection",
        lambda: project_configuration(configs, proposal, policy),
    )

    def resolve_affected_channels():
        by_name = {item["name"]: item["number"] for item in policy["channels"]}
        return [
            {"number": by_name[name], "name": name}
            for name in sorted(affected, key=lambda name: by_name[name])
        ]

    affected_channels = _finalization_step(
        "affected_channel_resolution", resolve_affected_channels)
    if not affected_channels:
        raise SingleRunError(
            "prepare", "proposal_has_no_effects",
            "Proposal has no effects eligible for schedule validation.",
            finalization_subphase="affected_channel_resolution",
        )
    def build_request():
        request_payload = {
            "schema_version": 1,
            "operation": "native_single_run",
            "run_id": run_id,
            "proposal": proposal,
            "policy": policy,
            "seed_inputs": seed_inputs,
            "input_fingerprints": {
                "original_logical_configuration_fingerprint": configuration_digest,
                "original_logical_database_fingerprint": database_digest,
                "logical_media_manifest_fingerprint": media_digest,
                "live_physical_configuration_fingerprint": live_physical_digest,
                "staged_source_physical_configuration_fingerprint": staged_physical["digest"],
            },
            "affected_channels": affected_channels,
            "validation_context": context,
        }
        return bind_request(request_payload)

    request = _finalization_step("request_binding", build_request)
    _finalization_step(
        "request_publication",
        lambda: write_private_json_exclusive(
            Path(stage) / REQUEST_NAME, request, REQUEST_SCHEMA),
    )
    return _finalization_step(
        "lifecycle_construction",
        lambda: SingleRunLifecycle(
            Path(project_root), Path(stage), lock, token, run_id,
            f"fs42-native-{token}.service", request,
        ),
    )


def prepare_single_run(
    project_root,
    source_root,
    media_root,
    proposal,
    policy,
    run_id,
):
    project_root = Path(project_root).resolve()
    source_root = Path(source_root).resolve()
    if source_root != project_root:
        raise SingleRunError(
            "prepare", "source_root_mismatch",
            "the protected source root must be the mounted project root",
        )
    allowed, detail = check_invocation_context()
    if not allowed:
        raise SingleRunError(
            "prepare", "invocation_context_rejected",
            f"internal native run requires verified SSH ancestry: {detail}",
        )
    token, stage, lock = create_staging_directory()
    try:
        live_physical_before = fingerprint_json_files(
            protected_json_paths(source_root)
        )
        staged_source = stage / "source"
        (staged_source / "confs").mkdir(mode=0o700, parents=True)
        (staged_source / "runtime").mkdir(mode=0o700)
        for identity, source in protected_json_paths(source_root).items():
            target = staged_source / identity
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _copy_private(source, target)
        database = fingerprint_and_clone_database(
            source_root / "runtime/fs42_fluid.db",
            staged_source / "runtime/fs42_fluid.db",
        )
        os.chmod(staged_source / "runtime/fs42_fluid.db", 0o600)
        live_physical_after = fingerprint_json_files(
            protected_json_paths(source_root)
        )
        if live_physical_before["digest"] != live_physical_after["digest"]:
            raise SingleRunError(
                "prepare", "source_changed", "protected source changed during capture"
            )
        configuration = logical_protected_configuration_fingerprint(
            protected_json_paths(staged_source)
        )
        manifest = capture_media_manifest(media_root, spool_directory=stage)
        try:
            media = logical_media_manifest_fingerprint(manifest)
        finally:
            manifest.close()
        return _finalize_prepared_single_run(
            project_root, stage, lock, token, run_id, proposal, policy,
            configuration_digest=configuration["digest"],
            database_digest=database["logical"]["digest"],
            media_digest=media["digest"],
            live_physical_digest=live_physical_after["digest"],
        )
    except Exception as exc:
        lock.close()
        if stage.exists():
            cleaned, detail = cleanup_staging_directory(stage)
            if not cleaned:
                raise SingleRunError(
                    "cleanup", "cleanup_failure",
                    f"preparation failed: {exc}; staging cleanup failed: {detail}",
                ) from exc
        raise


def launch_single_run(lifecycle, *, timeout=DEFAULT_TIMEOUT_SECONDS):
    if lifecycle.cleaned or lifecycle.launcher_result is not None:
        raise SingleRunError("launch", "invalid_lifecycle", "run is not launchable")
    launcher = IsolationLauncher(lifecycle.project_root)
    try:
        lifecycle.launcher_result = launcher.run(
            lifecycle.stage,
            [
                sandbox_python(lifecycle.project_root),
                "/project/station_director/single_run_worker.py",
                f"/stage/{REQUEST_NAME}",
                f"/stage/{RESPONSE_NAME}",
            ],
            lifecycle.unit_name,
            timeout=timeout,
            stage_tmp=True,
        )
    except Exception as exc:
        launched = getattr(exc, "launcher_result", None)
        if launched is not None:
            lifecycle.launcher_result = launched
        raise
    return lifecycle.launcher_result


def inspect_single_run(lifecycle):
    if lifecycle.launcher_result is None:
        raise SingleRunError("inspect", "not_launched", "run has not been launched")
    launch = lifecycle.launcher_result
    try:
        evidence = read_checkpoint_evidence(lifecycle.stage)
    except WorkerCheckpointError as exc:
        raise SingleRunError(
            "response", "worker_checkpoint_mismatch",
            "worker checkpoint evidence is invalid",
            c1_diagnostic={**make_diagnostic("worker_checkpoint_mismatch", "response"),
                           "scheduler_invoked": "unknown"},
            launcher=launcher_summary(launch), scheduler_state="unknown") from exc
    if (launch.unit_state_valid and launch.main_process_started is False
            and evidence.worker_started):
        raise SingleRunError(
            "response", "worker_checkpoint_mismatch",
            "worker checkpoint evidence contradicts launcher state",
            c1_diagnostic={**make_diagnostic("worker_checkpoint_mismatch", "response"),
                           "scheduler_invoked": "unknown"},
            launcher=launcher_summary(launch), scheduler_state="unknown")
    if evidence.scheduler_entered:
        scheduler_state = True
    elif launch.unit_state_valid and launch.main_process_started is False:
        scheduler_state = False
    else:
        scheduler_state = "unknown"
    termination_codes = {
        "runtime_timeout": ("worker_runtime_timeout", "native worker reached its runtime limit"),
        "oom_kill": ("worker_oom_kill", "native worker was terminated for memory exhaustion"),
        "external_signal": ("worker_external_signal", "native worker was terminated by a signal"),
        "launcher_state_invalid": ("launcher_state_invalid", "launcher state evidence is invalid"),
        "launcher_failure": ("launcher_failed", "native worker did not begin execution"),
        "sandbox_restriction": ("launcher_failed", "native worker was blocked by isolation policy"),
    }
    response_path = lifecycle.stage / RESPONSE_NAME
    response_exists = os.path.lexists(response_path)
    if (not response_exists and launch.timed_out
            and launch.termination_kind not in {"runtime_timeout", "oom_kill", "external_signal"}):
        launch.termination_kind = "outer_watchdog"
        raise SingleRunError(
            "launch", "worker_timeout", "native worker timed out",
            c1_diagnostic={**make_diagnostic("launcher_timeout", "launch"),
                           "scheduler_invoked": scheduler_state},
            launcher=launcher_summary(launch), scheduler_state=scheduler_state)
    if not response_exists and launch.termination_kind in termination_codes:
        code, message = termination_codes[launch.termination_kind]
        raise SingleRunError(
            "launch", code, message,
            c1_diagnostic={**make_diagnostic(code, "launch"),
                           "scheduler_invoked": scheduler_state},
            launcher=launcher_summary(launch), scheduler_state=scheduler_state)
    if not response_exists:
        if launch.termination_kind == "nonzero_exit" and not evidence.response_attempted:
            raise SingleRunError(
                "launch", "worker_nonzero_exit", "native worker exited unsuccessfully",
                c1_diagnostic={**make_diagnostic("worker_nonzero_exit", "launch"),
                               "scheduler_invoked": scheduler_state},
                launcher=launcher_summary(launch), scheduler_state=scheduler_state)
        code = ("worker_response_publication_failed" if evidence.response_attempted
                else "worker_response_missing")
        raise SingleRunError(
            "response", code, "worker response is missing",
            c1_diagnostic={**make_diagnostic(code, "response"),
                           "scheduler_invoked": scheduler_state},
            launcher=launcher_summary(launch), scheduler_state=scheduler_state)
    try:
        document_context = HeldDocument(response_path, RESPONSE_SCHEMA)
    except ProtocolError as exc:
        raise SingleRunError(
            "response", "worker_response_invalid", "worker response is invalid",
            c1_diagnostic={**make_diagnostic("worker_response_invalid", "response"),
                           "scheduler_invoked": scheduler_state},
            launcher=launcher_summary(launch), scheduler_state=scheduler_state) from exc
    with document_context as document:
        response = document.payload
        if response["run_id"] != lifecycle.run_id:
            raise SingleRunError("response", "run_id_mismatch", "worker run ID mismatch",
                                 c1_diagnostic={**make_diagnostic("worker_response_identity_mismatch", "response"),
                                                "scheduler_invoked": scheduler_state},
                                 launcher=launcher_summary(lifecycle.launcher_result),
                                 scheduler_state=scheduler_state)
        if response["proposal_id"] != lifecycle.request["proposal"]["proposal_id"]:
            raise SingleRunError("response", "proposal_id_mismatch", "worker proposal ID mismatch",
                                 c1_diagnostic={**make_diagnostic("worker_response_identity_mismatch", "response"),
                                                "scheduler_invoked": scheduler_state},
                                 launcher=launcher_summary(lifecycle.launcher_result),
                                 scheduler_state=scheduler_state)
        context = response["validation_context"]
        expected = lifecycle.request["validation_context"]
        for name in ("input_fingerprint", "requested_seed", "effective_seed"):
            if context[name] != expected[name]:
                raise SingleRunError("response", "context_mismatch", "worker context mismatch",
                                     c1_diagnostic={**make_diagnostic("worker_context_mismatch", "response"),
                                                    "scheduler_invoked": scheduler_state},
                                     launcher=launcher_summary(lifecycle.launcher_result),
                                     scheduler_state=scheduler_state)
        if response["affected_channels"] != lifecycle.request["affected_channels"]:
            raise SingleRunError("response", "affected_channels_mismatch", "worker affected channels mismatch",
                                 c1_diagnostic={**make_diagnostic("worker_channels_mismatch", "response"),
                                                "scheduler_invoked": scheduler_state},
                                 launcher=launcher_summary(lifecycle.launcher_result),
                                 scheduler_state=scheduler_state)
        document.assert_unchanged()
    response_state = response["scheduler_invoked"]
    if response_state != evidence.scheduler_entered:
        raise SingleRunError(
            "response", "worker_checkpoint_mismatch", "worker response contradicts checkpoints",
            c1_diagnostic={**make_diagnostic("worker_checkpoint_mismatch", "response"),
                           "scheduler_invoked": "unknown"},
            launcher=launcher_summary(launch), scheduler_state="unknown")
    if not evidence.response_completed:
        raise SingleRunError(
            "response", "worker_checkpoint_incomplete", "worker checkpoint publication is incomplete",
            c1_diagnostic={**make_diagnostic("worker_checkpoint_incomplete", "response",
                                             scheduler_invoked=response_state),
                           "scheduler_invoked": response_state},
            launcher=launcher_summary(launch), scheduler_state=response_state)
    if lifecycle.launcher_result.returncode != (0 if response["status"] == "success" else 1):
        raise SingleRunError("response", "exit_status_mismatch", "worker exit status contradicts response",
                             c1_diagnostic={**make_diagnostic("worker_exit_status_mismatch", "response"),
                                            "scheduler_invoked": response_state},
                             launcher=launcher_summary(lifecycle.launcher_result),
                             scheduler_state=response_state)
    lifecycle.response = response
    return response


@contextmanager
def single_run_lifecycle(*args, **kwargs):
    """Hold one internally owned stage until the comparison scope exits."""
    lifecycle = prepare_single_run(*args, **kwargs)
    primary_error = None
    try:
        launch_single_run(lifecycle)
        inspect_single_run(lifecycle)
        yield lifecycle
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            lifecycle.cleanup()
        except Exception as cleanup_error:
            if primary_error is None:
                raise
            raise SingleRunError(
                "cleanup", "cleanup_failure",
                f"{primary_error}; cleanup also failed: {cleanup_error}",
            ) from primary_error


def run_single_run(*args, **kwargs):
    with single_run_lifecycle(*args, **kwargs) as lifecycle:
        return lifecycle.response
