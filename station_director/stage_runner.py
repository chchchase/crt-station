#!/usr/bin/env python3
import json
import importlib
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from station_director.isolation_probe import build_probe_payload
from station_director.path_safety import PathSafetyError, map_station_config
from station_director.staged_schedule import (
    StagedScheduleError,
    capture_channel_history,
    inspect_required_schema,
)
from station_director.validation_context import verify_validation_context
from station_director.validation import _db_time, project_configuration


REQUEST_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
DISABLED_MESSAGE = "Phase 3 validation is not yet enabled"
PROJECT_ROOT = Path("/project")
STAGE_ROOT = Path("/stage")
MEDIA_ROOT = Path("/media")


def _write_result(path, payload):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _load_request(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    expected = {
        "schema_version", "run_id", "proposal", "policy", "seed_inputs",
        "validation_context"
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("validation request fields are incomplete or unexpected")
    if raw["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise ValueError("validation request schema mismatch")
    if not isinstance(raw["run_id"], str) or not raw["run_id"]:
        raise ValueError("validation request run ID is invalid")
    if (
        not isinstance(raw["proposal"], dict)
        or not isinstance(raw["policy"], dict)
        or not isinstance(raw["seed_inputs"], dict)
        or not isinstance(raw["validation_context"], dict)
    ):
        raise ValueError("validation request proposal or policy is invalid")
    return raw


def _load_native_validation_context(raw):
    """Import native validation support only after probes have passed."""
    module = importlib.import_module("fs42.scheduling_context")
    expected = {
        "input_fingerprint",
        "requested_seed",
        "effective_seed",
        "reference_clock",
        "start_time",
        "end_time",
        "timezone",
        "python_hash_seed",
        "validation_mode",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("validation scheduling context is malformed")
    if raw["python_hash_seed"] != "0":
        raise ValueError("validation Python hash seed is not fixed at zero")
    if raw["timezone"] != "America/Los_Angeles" or raw["validation_mode"] is not True:
        raise ValueError("validation scheduling environment is invalid")
    if os.environ.get("PYTHONHASHSEED") != raw["python_hash_seed"]:
        raise ValueError("worker Python hash seed does not match the attested context")
    if os.environ.get("TZ") != raw["timezone"]:
        raise ValueError("worker timezone does not match the attested context")
    context = module.ValidationSchedulingContext(
        reference_clock=datetime.fromisoformat(raw["reference_clock"]),
        start_time=datetime.fromisoformat(raw["start_time"]),
        end_time=datetime.fromisoformat(raw["end_time"]),
        seed=raw["effective_seed"],
        timezone=raw["timezone"],
        validation_mode=raw["validation_mode"],
    )
    return context


def _load_configurations(config_root):
    configs = {}
    filenames = {}
    for path in sorted(Path(config_root).glob("*.json")):
        if path.name == "main_config.json":
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        name = data.get("station_conf", {}).get("network_name")
        if not isinstance(name, str) or not name or name in configs:
            raise ValueError(f"invalid or duplicate network name in {path.name}")
        configs[name] = data
        filenames[name] = path.name
    return configs, filenames


def run_worker(
    request_path,
    result_path,
):
    project_root = PROJECT_ROOT
    stage_root = STAGE_ROOT
    media_root = MEDIA_ROOT
    request = _load_request(request_path)
    run_id = request["run_id"]
    proposal = request["proposal"]
    payload = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "run_id": run_id,
        "proposal_id": proposal.get("proposal_id"),
        "status": "failed",
        "failure": DISABLED_MESSAGE,
        "scheduler_invoked": False,
        "probe_attestation": None,
        "path_validation": {"passed": False, "mapping_count": 0, "mappings": []},
        "b2_preparation": None,
        "validation_context": request["validation_context"],
    }
    try:
        probes = build_probe_payload(run_id, stage_root=stage_root)
        payload["probe_attestation"] = probes
        if not probes.get("overall_pass"):
            payload["failure"] = "Isolation probe attestation failed; " + DISABLED_MESSAGE
            return payload

        verify_validation_context(
            proposal,
            request["policy"],
            request["seed_inputs"],
            request["validation_context"],
        )

        # This is intentionally the first native FieldStation42 import. The
        # context remains unused while the Phase 3 scheduler gate is active.
        _load_native_validation_context(request["validation_context"])

        configs, filenames = _load_configurations(Path(project_root) / "confs")
        projected, affected, unused_sources = project_configuration(
            configs, proposal, request["policy"]
        )
        native_checks = importlib.import_module(
            "station_director.native_config_checks"
        )
        station_schema = json.loads(
            (Path(project_root) / "fs42/station_config_schema.json").read_text(
                encoding="utf-8"
            )
        )
        native_failures = native_checks.validate_processed_configurations(
            projected, station_schema
        )
        unresolved, resolution_errors = native_checks.newly_unresolved_source_slots(
            configs, projected, unused_sources, proposal
        )
        native_failures.extend(unresolved)
        native_failures.extend(resolution_errors)
        if native_failures:
            raise ValueError("; ".join(sorted(set(native_failures))))
        mapping_rows = []
        for name in sorted(projected):
            unused_mapped, mappings = map_station_config(
                projected[name],
                filenames[name],
                sandbox_media_root=media_root,
                stage_root=stage_root,
            )
            for mapping in mappings:
                row = mapping.as_dict()
                row["configuration"] = filenames[name]
                mapping_rows.append(row)
        payload["path_validation"] = {
            "passed": True,
            "mapping_count": len(mapping_rows),
            "mappings": mapping_rows,
            "affected_channels": sorted(affected),
        }
        database = Path(stage_root) / "runtime/fs42_fluid.db"
        connection = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
                raise StagedScheduleError("staged SQLite query_only could not be established")
            connection.execute("BEGIN")
            schema = inspect_required_schema(connection)
            histories = [
                capture_channel_history(
                    connection,
                    name,
                    _db_time(proposal["week_start"]),
                    _db_time(proposal["week_end"]),
                )
                for name in sorted(affected)
            ]
            payload["b2_preparation"] = {
                "schema_tables": sorted(schema["tables"]),
                "channels": [history.summary() for history in histories],
                "scheduler_gate": "disabled",
            }
            connection.rollback()
        finally:
            connection.close()
        payload["status"] = "disabled"
        return payload
    except (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        KeyError,
        sqlite3.Error,
        json.JSONDecodeError,
        PathSafetyError,
        StagedScheduleError,
    ) as exc:
        payload["failure"] = f"Validation worker failed closed: {exc}; {DISABLED_MESSAGE}"
        return payload
    finally:
        _write_result(result_path, payload)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        return 2
    payload = run_worker(argv[0], argv[1])
    return 0 if payload["status"] == "disabled" else 1


if __name__ == "__main__":
    raise SystemExit(main())
