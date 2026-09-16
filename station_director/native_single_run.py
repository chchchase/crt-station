"""One staged native schedule run. Import this module only after attestation."""

import importlib
import json
import os
import sqlite3
import time
from functools import wraps
from datetime import datetime
from pathlib import Path

from fs42.catalog import ShowCatalog
from fs42.liquid_schedule import LiquidSchedule
from fs42.liquid_io import LiquidIO
from fs42.scheduling_context import (
    ValidationCatalogMetadataUnavailable,
    ValidationSchedulingContext,
    activate_validation_context,
)
from fs42.station_manager import StationManager
from fs42.guide_reader import (
    GuideLoadingError,
    GuideValidationError,
    fingerprint_guide_snapshot,
    prepare_guide_snapshot,
    stream_validate_staged_guide,
    verify_guide_snapshot,
)

from station_director.native_config_checks import (
    newly_unresolved_source_slots,
    validate_processed_configurations,
)
from station_director.path_safety import canonical_media_mapping, validate_scheduled_media
from station_director.preservation import canonical_foreign_key_findings
from station_director.c1_diagnostics import (
    attach_preservation_detail, copy_preservation_detail,
    preservation_check, preservation_step,
)
from station_director.staged_schedule import (
    StagedScheduleError,
    assert_protected_state,
    assert_retained_history,
    catalog_allocation_floor,
    capture_catalog_media_metadata,
    capture_catalog_rows,
    capture_channel_history,
    capture_protected_state,
    coverage_report,
    generated_playback_representations,
    inspect_required_schema,
    reconcile_catalog,
    retained_playback_representations,
    restore_sequence_state,
    validate_all_catalog_reference_shapes,
)
from station_director.validation import _db_time
from station_director.validation_context import derive_channel_seed
from station_director.worker_bootstrap import VerifiedWorkerAttestation


PROTECTED_CHANNELS = {"CRT Station Guide", "Watch In Order"}
STAGE_ROOT = Path("/stage")
MEDIA_ROOT = Path("/media")
CONFIGURATION_BUDGET_SECONDS = 120
CATALOG_BUDGET_SECONDS = 1980
SCHEDULER_BUDGET_SECONDS = 300
FINAL_NATIVE_BUDGET_SECONDS = 120
PROJECT_ROOT = Path("/project")


class NativeRunError(RuntimeError):
    def __init__(self, code, message, *, phase, channel=None, scheduler_invoked=False,
                 original_failure=None, restoration_failure=None,
                 guide_validation=None):
        super().__init__(message)
        self.code = code
        self.phase = phase
        self.channel = channel
        self.scheduler_invoked = scheduler_invoked
        self.original_failure = original_failure
        self.restoration_failure = restoration_failure
        self.guide_validation = guide_validation


def _load_json(path):
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


def _load_configs(root):
    configs = {}
    filenames = {}
    for path in sorted(Path(root).glob("*.json")):
        if path.name == "main_config.json":
            continue
        data = _load_json(path)
        name = data.get("station_conf", {}).get("network_name")
        if not isinstance(name, str) or not name or name in configs:
            raise NativeRunError(
                "invalid_configuration", f"invalid or duplicate station in {path.name}",
                phase="configuration",
            )
        configs[name] = data
        filenames[name] = path.name
    return configs, filenames


def _verified_work_tree(request, attestation):
    work = STAGE_ROOT / "work"
    projected, unused_filenames = _load_configs(work / "confs")
    affected = [item["name"] for item in request["affected_channels"]]
    by_name = {item["name"]: item["number"] for item in request["policy"]["channels"]}
    forbidden = sorted(PROTECTED_CHANNELS & set(affected))
    if forbidden:
        raise NativeRunError(
            "protected_channel", "protected channels cannot be affected: " + ", ".join(forbidden),
            phase="configuration",
        )
    unknown = sorted(set(affected) - set(by_name))
    if unknown:
        raise NativeRunError(
            "unknown_channel", "unknown affected channels: " + ", ".join(unknown),
            phase="configuration",
        )

    schema = _load_json(work / "fs42/station_config_schema.json")
    native_failures = validate_processed_configurations(projected, schema)
    original, unused = _load_configs(STAGE_ROOT / "source/confs")
    unresolved, resolution_errors = newly_unresolved_source_slots(
        original, projected, attestation.snapshot["source_channels"], request["proposal"]
    )
    native_failures.extend(unresolved)
    native_failures.extend(resolution_errors)
    if native_failures:
        raise NativeRunError(
            "native_configuration", "; ".join(sorted(set(native_failures))),
            phase="configuration",
        )
    return work, projected, affected, by_name, attestation.snapshot["mapping_count"]


def _native_autobump_contract():
    """Load the fixed FS42 contract only inside the attested native worker."""
    descriptor = importlib.import_module("fs42.autobump_descriptor")
    runtime = importlib.import_module("fs42.autobump_agent")
    return descriptor, runtime


def _playback_descriptor_failure(channel, generated, invalid):
    if invalid:
        raise NativeRunError(
            "invalid_playback_descriptor",
            "Playback descriptor is invalid.",
            phase="scheduler", channel=channel, scheduler_invoked=True,
        )
    if generated:
        raise NativeRunError(
            "autobump_selected", "AutoBump content was selected.",
            phase="scheduler", channel=channel, scheduler_invoked=True,
        )


@preservation_check("_validate_playback_representations", "check_failed", playback=True)
def _validate_playback_representations(
        descriptor, representations, channel, *, generated, media_root=None):
    checked = 0
    for block in representations:
        plan = block["plan"]
        for item in plan:
            classification = descriptor.classify_plan_entry(
                item,
                liquid_type=block["liquid_type"],
                plan_size=len(plan),
                content_missing=block["content_missing"],
            )
            if classification == descriptor.DESCRIPTOR_INVALID:
                with preservation_step("_validate_playback_representations", "descriptor_invalid"):
                    _playback_descriptor_failure(channel, generated, True)
            if classification == descriptor.DESCRIPTOR_SELECTED:
                with preservation_step("_validate_playback_representations", "autobump_selected"):
                    _playback_descriptor_failure(channel, generated, False)
                continue
            if media_root is not None:
                if item["is_stream"]:
                    raise attach_preservation_detail(StagedScheduleError(
                        f"block {block['block_id']} contains stream content "
                        "instead of confined media"
                    ), "_validate_playback_representations", "stream_rejected")
                with preservation_step("_validate_playback_representations", "media_validation_failed"):
                    mapping = canonical_media_mapping(
                        item["path"], "scheduled plan path", allow_sandbox=True
                    )
                    validate_scheduled_media(
                        mapping.sandbox_path, sandbox_media_root=media_root
                    )
                checked += 1
        for entry in block["catalog"]:
            classification = descriptor.classify_catalog_entry(entry)
            if classification == descriptor.DESCRIPTOR_INVALID:
                with preservation_step("_validate_playback_representations", "descriptor_invalid"):
                    _playback_descriptor_failure(channel, generated, True)
            if classification == descriptor.DESCRIPTOR_SELECTED:
                with preservation_step("_validate_playback_representations", "autobump_selected"):
                    _playback_descriptor_failure(channel, generated, False)
                continue
            if media_root is not None:
                with preservation_step("_validate_playback_representations", "media_validation_failed"):
                    mapping = canonical_media_mapping(
                        entry["realpath"] or entry["path"],
                        "scheduled catalog path", allow_sandbox=True,
                    )
                    validate_scheduled_media(
                        mapping.sandbox_path, sandbox_media_root=media_root
                    )
                checked += 1
    return checked


def _inspect_generated_playback(connection, history, channel):
    descriptor, unused_runtime = _native_autobump_contract()
    _validate_playback_representations(
        descriptor, generated_playback_representations(connection, history),
        channel, generated=True,
    )


def _validate_scheduled_playback(connection, histories, media_root):
    descriptor, unused_runtime = _native_autobump_contract()
    checked = 0
    for channel, history in histories.items():
        checked += _validate_playback_representations(
            descriptor, retained_playback_representations(connection, history),
            channel, generated=False, media_root=media_root,
        )
        checked += _validate_playback_representations(
            descriptor, generated_playback_representations(connection, history),
            channel, generated=True, media_root=media_root,
        )
    return checked


def _rows_as_dicts(columns, rows):
    return [dict(zip(columns, row)) for row in rows]


def _map_catalog_in_memory(schedule):
    for entries in schedule.catalog.clip_index.values():
        for entry in entries:
            for field in ("path", "realpath"):
                value = getattr(entry, field, None)
                if value:
                    setattr(
                        entry, field,
                        canonical_media_mapping(value, "native catalog path", allow_sandbox=True).sandbox_path,
                    )


@preservation_check("_validate_final_cross_channel_exclusions", "check_failed")
def _validate_final_cross_channel_exclusions(projected, histories):
    """Fail if the final schedule violates native sibling exclusion semantics."""
    liquid_io = LiquidIO()
    ordered = sorted(histories)
    for index, left_name in enumerate(ordered):
        left = projected[left_name]["station_conf"]
        left_dir = os.path.realpath(left.get("content_dir", ""))
        left_tags = LiquidSchedule._get_station_tags(left)
        for right_name in ordered[index + 1:]:
            right = projected[right_name]["station_conf"]
            if (
                not left_dir
                or os.path.realpath(right.get("content_dir", "")) != left_dir
                or not (left_tags & LiquidSchedule._get_station_tags(right))
            ):
                continue
            start = max(histories[left_name].regeneration_start, histories[right_name].regeneration_start)
            end = min(histories[left_name].effective_horizon, histories[right_name].effective_horizon)
            if start >= end:
                continue
            left_blocks = liquid_io.query_liquid_blocks(left_name, start, end)
            right_blocks = liquid_io.query_liquid_blocks(right_name, start, end)
            for first in left_blocks:
                if getattr(first, "sequence_key", None) or not first.content or isinstance(first.content, list):
                    continue
                first_path = getattr(first.content, "realpath", None)
                if not first_path:
                    continue
                for second in right_blocks:
                    if getattr(second, "sequence_key", None) or not second.content or isinstance(second.content, list):
                        continue
                    if (
                        first_path == getattr(second.content, "realpath", None)
                        and first.start_time < second.end_time
                        and second.start_time < first.end_time
                    ):
                        raise attach_preservation_detail(StagedScheduleError(
                            f"cross-channel exclusion collision: {left_name} and {right_name}"
                        ), "_validate_final_cross_channel_exclusions", "collision")


def _native_station_config(channel, context):
    try:
        with activate_validation_context(context):
            config = StationManager().station_by_name(channel)
    except Exception as exc:
        raise NativeRunError(
            "native_configuration", str(exc), phase="configuration", channel=channel
        ) from exc
    if config is None:
        raise NativeRunError(
            "native_configuration", f"native StationManager did not load {channel}",
            phase="configuration", channel=channel,
        )
    return config


def _preservation_cleanup(connection, primary, helper, *, rollback=False, scheduler_invoked=False):
    """Attempt all cleanup, without replacing a primary failure or its detail."""
    failure = primary
    for category, operation in (("rollback_failed", connection.rollback), ("close_failed", connection.close)):
        if category == "rollback_failed" and not rollback:
            continue
        try:
            with preservation_step(helper, category):
                operation()
        except BaseException as exc:
            if failure is None:
                failure = exc
            else:
                copy_preservation_detail(exc, failure)
    if primary is None and failure is not None:
        if not isinstance(failure, Exception):
            raise failure
        code = "sequence_restore_failure" if helper == "_restore_sequences" else "preservation_failure"
        raise copy_preservation_detail(failure, NativeRunError(
            code, "Preservation cleanup failed.", phase="preservation",
            scheduler_invoked=scheduler_invoked,
        )) from failure


def _restore_sequences(database, protected, *, scheduler_invoked=False):
    with preservation_step("_restore_sequences", "open_failed"):
        connection = sqlite3.connect(database)
    primary = None
    try:
        restore_sequence_state(connection, protected)
        with preservation_step("_restore_sequences", "commit_failed"):
            connection.commit()
        sequence_state = {
            name: protected[name]
            for name in ("named_sequence", "sequence_entries", "sequence_group_state")
        }
        with preservation_step("_restore_sequences", "verification_failed"):
            assert_protected_state(connection, sequence_state, sequence_verification=True)
    except BaseException as exc:
        primary = exc
        if not isinstance(exc, Exception):
            raise
        primary = copy_preservation_detail(exc, NativeRunError(
            "sequence_restore_failure", str(exc), phase="preservation",
            scheduler_invoked=scheduler_invoked,
        ))
        raise primary from exc
    finally:
        _preservation_cleanup(connection, primary, "_restore_sequences", rollback=primary is not None, scheduler_invoked=scheduler_invoked)


def _sequence_restored(function):
    """Guarantee global sequence restoration after every post-snapshot outcome."""
    @wraps(function)
    def guarded(request, attestation):
        restoration = {}
        primary = None
        result = None
        try:
            result = function(request, attestation, restoration)
        except BaseException as exc:
            primary = exc
        if restoration.get("protected") is not None:
            try:
                _restore_sequences(
                    restoration["database"], restoration["protected"],
                    scheduler_invoked=restoration.get("scheduler_invoked", False),
                )
            except BaseException as exc:
                if primary is not None:
                    # Preserve the original diagnostic (including cancellation).
                    # This local marker is fixed and is not a protocol extension.
                    primary.restoration_failure = "sequence_restore_failure"
                    copy_preservation_detail(exc, primary)
                    raise primary from primary.__cause__
                raise copy_preservation_detail(exc, NativeRunError(
                    "sequence_restore_failure", str(exc), phase="preservation",
                    scheduler_invoked=restoration.get("scheduler_invoked", False),
                    restoration_failure=f"{type(exc).__name__}: {exc}",
                )) from exc
        if primary is not None:
            raise primary
        return result
    return guarded


def _build_channel_result(connection, channel, channel_number, channel_seed,
                          history, statistics):
    generated_count = connection.execute(
        "SELECT COUNT(*) FROM liquid_blocks WHERE station=? AND start_time>=?",
        (channel, history.regeneration_start),
    ).fetchone()[0]
    return {
        "name": channel, "number": channel_number, "channel_seed": channel_seed,
        "regeneration_start": history.regeneration_start,
        "effective_horizon": history.effective_horizon,
        "retained_blocks": len(history.retained_rows),
        "generated_blocks": generated_count,
        "final_blocks": len(history.retained_rows) + generated_count,
        "catalog_reused": statistics["reused"], "catalog_new": statistics["new"],
        "catalog_protected": statistics["protected"],
        "new_catalog_ids": "provisional", "coverage": {},
    }


def _verify_final_preservation(connection, projected, histories, channel_results,
                               protected, baseline_foreign_keys, media_root):
    restore_sequence_state(connection, protected)
    with preservation_step("_verify_final_preservation", "commit_failed"):
        connection.commit()
    _validate_final_cross_channel_exclusions(projected, histories)
    for result in channel_results:
        history = histories[result["name"]]
        assert_retained_history(connection, history)
        result["coverage"] = coverage_report(connection, history)
    assert_protected_state(connection, protected)
    with preservation_step("_verify_final_preservation", "foreign_key_check_failed"):
        if canonical_foreign_key_findings(connection) != baseline_foreign_keys:
            raise attach_preservation_detail(StagedScheduleError("foreign-key findings changed from baseline"),
                                             "_verify_final_preservation", "foreign_key_mismatch")
    return _validate_scheduled_playback(connection, histories, media_root)


def _final_input_verification(attestation, request):
    attestation.verify(request)


def _run_guide_validation(database, channel_results, proposal_start, proposal_end):
    state = {
        "status": "failed",
        "primary_failure": None,
        "snapshot_preparation": {"status": "not_run", "message": None},
        "snapshot_verification": {"status": "not_run", "message": None},
        "post_read_verification": {"status": "not_run", "message": None},
        "errors": [], "errors_truncated": False,
    }
    snapshot = None
    try:
        snapshot = prepare_guide_snapshot(database, STAGE_ROOT)
        state["snapshot_preparation"] = {"status": "pass", "message": None}
    except Exception as exc:
        state["snapshot_preparation"] = {
            "status": "failed", "message": f"{type(exc).__name__}: {exc}"[:1000],
        }
        state["primary_failure"] = {
            "phase": "guide_snapshot", "code": "guide_loading_failed",
            "message": f"{type(exc).__name__}: {exc}"[:1000],
        }
        raise NativeRunError(
            "guide_loading_failed", str(exc), phase="guide", scheduler_invoked=True,
            guide_validation=state,
        ) from exc

    primary = None
    failure_code = "guide_loading_failed"
    artifact = None
    summaries = None
    verification_failures = []
    try:
        manager = StationManager()
        stations = manager.stations
        failure_code = "guide_validation_failed"
        artifact, summaries = stream_validate_staged_guide(
            snapshot, STAGE_ROOT, stations, channel_results,
            proposal_start, proposal_end,
            normalize_titles=manager.server_conf.get("normalize_titles", True),
            title_patterns=manager.server_conf.get("title_patterns", []),
        )
    except Exception as exc:
        primary = exc
        failure_code = (
            "guide_validation_failed" if isinstance(exc, GuideValidationError)
            else "guide_loading_failed" if isinstance(exc, GuideLoadingError)
            else failure_code
        )
        state["primary_failure"] = {
            "phase": "guide_read", "code": failure_code,
            "message": f"{type(exc).__name__}: {exc}"[:1000],
        }
    finally:
        try:
            after = fingerprint_guide_snapshot(snapshot)
            if after != snapshot.working_logical_digest:
                raise GuideValidationError("guide snapshot logical fingerprint differs from working baseline")
            state["post_read_verification"] = {"status": "pass", "message": None}
        except Exception as exc:
            verification_failures.append(exc)
            state["post_read_verification"] = {
                "status": "failed", "message": f"{type(exc).__name__}: {exc}"[:1000],
            }
        try:
            verify_guide_snapshot(snapshot, database)
            state["snapshot_verification"] = {"status": "pass", "message": None}
        except Exception as exc:
            verification_failures.append(exc)
            state["snapshot_verification"] = {
                "status": "failed", "message": f"{type(exc).__name__}: {exc}"[:1000],
            }

    if primary is not None or verification_failures:
        cause = primary if primary is not None else verification_failures[0]
        raise NativeRunError(
            failure_code if primary is not None else "guide_validation_failed",
            str(cause), phase="guide", scheduler_invoked=True,
            original_failure=primary, guide_validation=state,
        ) from cause
    state.update({
        "status": "pass", **artifact, "channels": summaries,
        "primary_failure": None,
    })
    return state


def execute_native_single_run(request, attestation=None):
    if not isinstance(attestation, VerifiedWorkerAttestation):
        raise NativeRunError(
            "missing_attestation", "verified worker attestation is required",
            phase="native_import",
        )
    attestation.verify(request)
    original_directory = os.getcwd()
    try:
        return _execute_native_single_run(request, attestation)
    finally:
        os.chdir(original_directory)


@_sequence_restored
def _execute_native_single_run(request, attestation, restoration):
    started = time.monotonic()
    work, projected, affected, channel_numbers, mapping_count = _verified_work_tree(
        request, attestation
    )
    prepared_at = time.monotonic()
    catalog_seconds = 0.0
    scheduler_seconds = 0.0
    os.chdir(work)
    database = work / "runtime/fs42_fluid.db"
    proposal_start = _db_time(request["proposal"]["week_start"])
    proposal_end = _db_time(request["proposal"]["week_end"])
    effective_seed = request["validation_context"]["effective_seed"]
    reference_clock = datetime.fromisoformat(request["validation_context"]["reference_clock"])

    connection = sqlite3.connect(database)
    try:
        try:
            inspect_required_schema(connection)
            validate_all_catalog_reference_shapes(connection)
            histories = {
                channel: capture_channel_history(connection, channel, proposal_start, proposal_end)
                for channel in affected
            }
            protected = capture_protected_state(
                connection, affected, protected_channels=tuple(PROTECTED_CHANNELS)
            )
            restoration.update(database=database, protected=protected, scheduler_invoked=False)
            original_catalogs = {}
            original_metadata = {}
            for channel in affected:
                columns, rows = capture_catalog_rows(connection, channel)
                original_catalogs[channel] = (columns, rows)
                original_metadata[channel] = capture_catalog_media_metadata(connection, rows, columns)
            baseline_foreign_keys = canonical_foreign_key_findings(connection)
            for channel in affected:
                connection.execute(
                    "DELETE FROM liquid_blocks WHERE station=? AND start_time>=?",
                    (channel, proposal_start),
                )
            connection.commit()
        except Exception as exc:
            connection.rollback()
            raise NativeRunError(
                "staged_preparation", str(exc), phase="preservation"
            ) from exc
    finally:
        connection.close()

    channel_results = []
    scheduler_entered = False
    channel_inputs = {}
    unused_descriptor, autobump_runtime = _native_autobump_contract()
    checkpoint = getattr(attestation, "checkpoint", None)
    for channel in affected:
        attestation.verify(request)
        history = histories[channel]
        channel_seed = derive_channel_seed(
            effective_seed, channel_numbers[channel], channel,
            history.regeneration_start, history.effective_horizon,
        )
        context = ValidationSchedulingContext(
            reference_clock=reference_clock,
            start_time=datetime.fromisoformat(history.regeneration_start),
            end_time=datetime.fromisoformat(history.effective_horizon),
            seed=channel_seed,
            media_root=str(MEDIA_ROOT),
        )
        try:
            native_config = _native_station_config(channel, context)
        except SystemExit as exc:
            raise NativeRunError(
                "native_system_exit", f"native configuration exited with {exc.code!r}",
                phase="configuration", channel=channel,
            ) from exc
        try:
            subprocess_required = autobump_runtime.AutoBumpAgent.validation_subprocess_required(
                native_config
            )
        except autobump_runtime.AutoBumpConfigurationError as exc:
            raise NativeRunError(
                "native_configuration", "Native AutoBump configuration is invalid.",
                phase="configuration", channel=channel,
            ) from exc
        if subprocess_required:
            raise NativeRunError(
                "autobump_subprocess_required",
                "AutoBump configuration requires a blocked subprocess.",
                phase="configuration", channel=channel,
            )
        channel_inputs[channel] = (history, context, native_config)
    if time.monotonic() - started > CONFIGURATION_BUDGET_SECONDS:
        raise NativeRunError(
            "native_configuration", "Native configuration budget expired.",
            phase="configuration")
    if checkpoint is not None:
        checkpoint.publish("configuration_completed")

    for channel in affected:
        history, context, native_config = channel_inputs[channel]
        attestation.verify(request)
        allocation_connection = sqlite3.connect(database)
        try:
            allocation_floor = catalog_allocation_floor(allocation_connection)
        finally:
            allocation_connection.close()
        attestation.verify(request)
        try:
            catalog_started = time.monotonic()
            attestation.verify(request)
            if checkpoint is not None:
                checkpoint.publish("catalog_entered")
            with activate_validation_context(context):
                ShowCatalog(
                    native_config, rebuild_catalog=True, load=False
                )
            attestation.verify(request)
            if checkpoint is not None:
                checkpoint.publish("catalog_completed")
        except SystemExit as exc:
            raise NativeRunError(
                "native_system_exit", f"native catalog exited with {exc.code!r}",
                phase="catalog", channel=channel,
            ) from exc
        except ValidationCatalogMetadataUnavailable as exc:
            raise NativeRunError(
                "catalog_metadata_unavailable",
                "Verified catalog metadata is unavailable.",
                phase="catalog", channel=channel,
            ) from exc
        except Exception as exc:
            raise NativeRunError(
                "catalog_failure", str(exc), phase="catalog", channel=channel
            ) from exc

        connection = sqlite3.connect(database)
        statistics = {}
        primary = None
        try:
            try:
                columns, generated_rows = capture_catalog_rows(connection, channel)
                generated_metadata = capture_catalog_media_metadata(
                    connection, generated_rows, columns
                )
                reconcile_catalog(
                    connection, channel, _rows_as_dicts(columns, generated_rows),
                    history.protected_catalog_ids,
                    original_rows=original_catalogs[channel][1],
                    original_media_metadata=original_metadata[channel],
                    generated_media_metadata=generated_metadata,
                    statistics=statistics,
                    allocation_floor=allocation_floor,
                )
                connection.commit()
                assert_retained_history(connection, history)
                attestation.verify(request)
            except Exception as exc:
                primary = copy_preservation_detail(exc, NativeRunError(
                    "catalog_reconciliation", str(exc), phase="catalog",
                    channel=channel,
                ))
                raise primary from exc
            except BaseException as exc:
                primary = exc
                raise
        finally:
            _preservation_cleanup(
                connection, primary, "_execute_native_single_run",
                rollback=primary is not None, scheduler_invoked=scheduler_entered,
            )
        catalog_seconds += time.monotonic() - catalog_started
        if catalog_seconds > CATALOG_BUDGET_SECONDS:
            raise NativeRunError(
                "catalog_failure", "Native catalog budget expired.",
                phase="catalog", channel=channel)

        try:
            scheduler_started = time.monotonic()
            attestation.verify(request)
            with activate_validation_context(context):
                schedule = LiquidSchedule(native_config)
                _map_catalog_in_memory(schedule)
            attestation.verify(request)
            if checkpoint is not None:
                checkpoint.publish("scheduler_entry")
            scheduler_entered = True
            restoration["scheduler_invoked"] = True
            schedule.generate_validation_range(
                context.start_time, context.end_time, context
            )
            attestation.verify(request)
            if checkpoint is not None:
                checkpoint.publish("scheduler_completed")
            scheduler_seconds += time.monotonic() - scheduler_started
            if scheduler_seconds > SCHEDULER_BUDGET_SECONDS:
                raise NativeRunError(
                    "scheduler_failure", "Native scheduler budget expired.",
                    phase="scheduler", channel=channel, scheduler_invoked=True)
        except ValidationCatalogMetadataUnavailable as exc:
            raise NativeRunError(
                "catalog_metadata_unavailable",
                "Verified catalog metadata is unavailable.",
                phase="scheduler" if scheduler_entered else "catalog",
                channel=channel, scheduler_invoked=scheduler_entered,
            ) from exc
        except autobump_runtime.AutoBumpValidationSubprocessBlocked as exc:
            raise NativeRunError(
                "autobump_subprocess_blocked",
                "AutoBump attempted a blocked subprocess.",
                phase="scheduler", channel=channel, scheduler_invoked=True,
            ) from exc
        except SystemExit as exc:
            raise NativeRunError(
                "native_system_exit", f"native scheduler exited with {exc.code!r}",
                phase="scheduler", channel=channel, scheduler_invoked=True,
            ) from exc
        except Exception as exc:
            raise NativeRunError(
                "scheduler_failure", str(exc), phase="scheduler", channel=channel,
                scheduler_invoked=True,
            ) from exc

        connection = sqlite3.connect(database)
        try:
            _inspect_generated_playback(connection, history, channel)
            channel_results.append(_build_channel_result(
                connection, channel, channel_numbers[channel], channel_seed,
                history, statistics,
            ))
        finally:
            connection.close()

    preservation_started = time.monotonic()
    connection = sqlite3.connect(database)
    primary = None
    try:
        try:
            checked_paths = _verify_final_preservation(
                connection, projected, histories, channel_results, protected,
                baseline_foreign_keys, MEDIA_ROOT,
            )
        except NativeRunError as exc:
            primary = exc
            raise
        except Exception as exc:
            primary = copy_preservation_detail(exc, NativeRunError(
                "preservation_failure", str(exc), phase="preservation",
                scheduler_invoked=scheduler_entered,
            ))
            raise primary from exc
        except BaseException as exc:
            primary = exc
            raise
    finally:
        _preservation_cleanup(connection, primary, "_verify_final_preservation", rollback=primary is not None, scheduler_invoked=scheduler_entered)
    finished = time.monotonic()
    guide_started = time.monotonic()
    guide_validation = _run_guide_validation(
        database, channel_results,
        datetime.fromisoformat(proposal_start), datetime.fromisoformat(proposal_end),
    )
    guide_finished = time.monotonic()
    if guide_finished - preservation_started > FINAL_NATIVE_BUDGET_SECONDS:
        raise NativeRunError(
            "guide_validation_failed", "Native finalization budget expired.",
            phase="guide", scheduler_invoked=scheduler_entered)
    _final_input_verification(attestation, request)
    return {
        "scheduler_invoked": scheduler_entered,
        "channels": channel_results,
        "preservation": {
            "retained_history": "pass",
            "protected_channels": "pass",
            "sequence_tables_restored": "pass",
            "foreign_key_baseline": "pass",
        },
        "path_validation": {"passed": True, "mapping_count": mapping_count, "scheduled_path_checks": checked_paths},
        "guide_validation": guide_validation,
        "timings_ms": {
            "prepare": round((prepared_at - started) * 1000),
            "catalog": round(catalog_seconds * 1000),
            "scheduler": round(scheduler_seconds * 1000),
            "preservation": round((finished - preservation_started) * 1000),
            "guide": round((guide_finished - guide_started) * 1000),
            "total": round((guide_finished - started) * 1000),
        },
        "verification": {
            "original_snapshot": "pass",
            "deterministic_projection": "pass",
            "projected_configuration": "pass",
            "working_database": "pass",
            "logical_media": "pass",
            "physical_transition": "pass",
            "fingerprints": {
                key: value for key, value in attestation.snapshot.items()
                if key.endswith("_fingerprint")
            },
        },
        "workspace": str(work),
    }
