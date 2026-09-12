#!/usr/bin/env python3
import json
import os
import sqlite3
import sys
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
from station_director.validation import _db_time, project_configuration


REQUEST_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
DISABLED_MESSAGE = "Phase 3 validation is not yet enabled"


def _write_result(path, payload):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _load_request(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    expected = {"schema_version", "run_id", "proposal", "policy"}
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("validation request fields are incomplete or unexpected")
    if raw["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise ValueError("validation request schema mismatch")
    if not isinstance(raw["run_id"], str) or not raw["run_id"]:
        raise ValueError("validation request run ID is invalid")
    if not isinstance(raw["proposal"], dict) or not isinstance(raw["policy"], dict):
        raise ValueError("validation request proposal or policy is invalid")
    return raw


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
    *,
    project_root=Path("/project"),
    stage_root=Path("/stage"),
    media_root=Path("/media"),
    probe_builder=build_probe_payload,
):
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
    }
    try:
        probes = probe_builder(run_id, stage_root=stage_root)
        payload["probe_attestation"] = probes
        if not probes.get("overall_pass"):
            payload["failure"] = "Isolation probe attestation failed; " + DISABLED_MESSAGE
            return payload

        configs, filenames = _load_configurations(Path(project_root) / "confs")
        projected, affected, unused_sources = project_configuration(
            configs, proposal, request["policy"]
        )
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
