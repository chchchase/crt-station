"""One staged native schedule run. Import this module only after attestation."""

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
from fs42.scheduling_context import ValidationSchedulingContext, activate_validation_context
from fs42.station_manager import StationManager

from station_director.native_config_checks import (
    newly_unresolved_source_slots,
    validate_processed_configurations,
)
from station_director.path_safety import canonical_media_mapping
from station_director.preservation import canonical_foreign_key_findings
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
    inspect_required_schema,
    reconcile_catalog,
    restore_sequence_state,
    validate_scheduled_paths,
    validate_all_catalog_reference_shapes,
)
from station_director.validation import _db_time
from station_director.validation_context import derive_channel_seed
from station_director.worker_bootstrap import VerifiedWorkerAttestation


PROTECTED_CHANNELS = {"CRT Station Guide", "Watch In Order"}
STAGE_ROOT = Path("/stage")
MEDIA_ROOT = Path("/media")
PROJECT_ROOT = Path("/project")


class NativeRunError(RuntimeError):
    def __init__(self, code, message, *, phase, channel=None, scheduler_invoked=False,
                 original_failure=None, restoration_failure=None):
        super().__init__(message)
        self.code = code
        self.phase = phase
        self.channel = channel
        self.scheduler_invoked = scheduler_invoked
        self.original_failure = original_failure
        self.restoration_failure = restoration_failure


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


def _autobump_fields(value, location="station_conf"):
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            field = f"{location}.{key}"
            if key in ("autobump", "off_air_autobump"):
                found.append(field)
            found.extend(_autobump_fields(child, field))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_autobump_fields(child, f"{location}[{index}]"))
    return found


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
                        raise StagedScheduleError(
                            f"cross-channel exclusion collision: {left_name} and {right_name}"
                        )


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


def _restore_sequences(database, protected, *, scheduler_invoked=False):
    connection = sqlite3.connect(database)
    try:
        restore_sequence_state(connection, protected)
        connection.commit()
        sequence_state = {
            name: protected[name]
            for name in ("named_sequence", "sequence_entries", "sequence_group_state")
        }
        assert_protected_state(connection, sequence_state)
    except Exception as exc:
        connection.rollback()
        raise NativeRunError(
            "sequence_restore_failure", str(exc), phase="preservation",
            scheduler_invoked=scheduler_invoked,
        ) from exc
    finally:
        connection.close()


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
                original = (
                    f"{type(primary).__name__}: {primary}" if primary is not None else None
                )
                raise NativeRunError(
                    "sequence_restore_failure", str(exc), phase="preservation",
                    scheduler_invoked=restoration.get("scheduler_invoked", False),
                    original_failure=original,
                    restoration_failure=f"{type(exc).__name__}: {exc}",
                ) from (primary or exc)
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
    connection.commit()
    _validate_final_cross_channel_exclusions(projected, histories)
    for result in channel_results:
        history = histories[result["name"]]
        assert_retained_history(connection, history)
        result["coverage"] = coverage_report(connection, history)
    assert_protected_state(connection, protected)
    if canonical_foreign_key_findings(connection) != baseline_foreign_keys:
        raise StagedScheduleError("foreign-key findings changed from baseline")
    return validate_scheduled_paths(connection, list(histories.values()), media_root)


def _final_input_verification(attestation, request):
    attestation.verify(request)


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

    for channel in affected:
        fields = _autobump_fields(projected[channel]["station_conf"])
        if fields:
            raise NativeRunError(
                "unsupported_autobump",
                "AutoBump is unsupported in isolated validation: " + ", ".join(fields[:8]),
                phase="configuration", channel=channel,
            )

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
        )
        try:
            native_config = _native_station_config(channel, context)
        except SystemExit as exc:
            raise NativeRunError(
                "native_system_exit", f"native configuration exited with {exc.code!r}",
                phase="configuration", channel=channel,
            ) from exc
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
            with activate_validation_context(context):
                ShowCatalog(
                    native_config, rebuild_catalog=True, load=False
                )
            attestation.verify(request)
        except SystemExit as exc:
            raise NativeRunError(
                "native_system_exit", f"native catalog exited with {exc.code!r}",
                phase="catalog", channel=channel,
            ) from exc
        except Exception as exc:
            raise NativeRunError(
                "catalog_failure", str(exc), phase="catalog", channel=channel
            ) from exc

        connection = sqlite3.connect(database)
        statistics = {}
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
                connection.rollback()
                raise NativeRunError(
                    "catalog_reconciliation", str(exc), phase="catalog",
                    channel=channel,
                ) from exc
        finally:
            connection.close()
        catalog_seconds += time.monotonic() - catalog_started

        try:
            scheduler_started = time.monotonic()
            attestation.verify(request)
            with activate_validation_context(context):
                schedule = LiquidSchedule(native_config)
                _map_catalog_in_memory(schedule)
            attestation.verify(request)
            scheduler_entered = True
            restoration["scheduler_invoked"] = True
            schedule.generate_validation_range(
                context.start_time, context.end_time, context
            )
            attestation.verify(request)
            scheduler_seconds += time.monotonic() - scheduler_started
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
            channel_results.append(_build_channel_result(
                connection, channel, channel_numbers[channel], channel_seed,
                history, statistics,
            ))
        finally:
            connection.close()

    preservation_started = time.monotonic()
    connection = sqlite3.connect(database)
    try:
        try:
            checked_paths = _verify_final_preservation(
                connection, projected, histories, channel_results, protected,
                baseline_foreign_keys, MEDIA_ROOT,
            )
        except Exception as exc:
            connection.rollback()
            raise NativeRunError(
                "preservation_failure", str(exc), phase="preservation",
                scheduler_invoked=scheduler_entered,
            ) from exc
    finally:
        connection.close()
    finished = time.monotonic()
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
        "timings_ms": {
            "prepare": round((prepared_at - started) * 1000),
            "catalog": round(catalog_seconds * 1000),
            "scheduler": round(scheduler_seconds * 1000),
            "preservation": round((finished - preservation_started) * 1000),
            "total": round((finished - started) * 1000),
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
