#!/usr/bin/env python3
"""Isolated worker for one internal native scheduling run."""

import importlib
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
STAGE_ROOT = Path("/stage")
MEDIA_ROOT = Path("/media")
PROJECT_ROOT = Path("/project")

from station_director.single_run_protocol import (
    HeldDocument,
    REQUEST_SCHEMA,
    RESPONSE_SCHEMA_V3 as RESPONSE_SCHEMA,
    ProtocolError,
    write_private_json_exclusive,
)
from station_director.worker_bootstrap import BootstrapError, attest_before_native_import
from station_director.c1_diagnostics import make_diagnostic
from station_director.worker_checkpoint import CheckpointWriter


class _BoundedWarningHandler(logging.Handler):
    def __init__(self, limit=50):
        super().__init__(logging.WARNING)
        self.limit = limit
        self.items = []
        self.truncated = False

    def emit(self, record):
        if len(self.items) >= self.limit:
            self.truncated = True
            return
        # Native warnings are classified without copying arbitrary config/media
        # values into the protocol response.
        self.items.append(make_diagnostic("native_warning", "configuration"))


def _diagnostic(phase, code, *, scheduler_invoked=False, probe=None,
                fingerprint_category=None, channel_number=None):
    return make_diagnostic(
        code, phase, scheduler_invoked=scheduler_invoked, probe=probe,
        fingerprint_category=fingerprint_category,
        channel_number=channel_number)


def _base_response(request):
    context = request["validation_context"]
    return {
        "schema_version": 3,
        "operation": "native_single_run",
        "run_id": request["run_id"],
        "proposal_id": request["proposal"]["proposal_id"],
        "status": "failed",
        "phase_reached": "request",
        "scheduler_invoked": False,
        "validation_context": {
            "input_fingerprint": context["input_fingerprint"],
            "requested_seed": context["requested_seed"],
            "effective_seed": context["effective_seed"],
        },
        "affected_channels": request["affected_channels"],
        "channels": [],
        "verification": {},
        "preservation": {},
        "path_validation": {},
        "guide_validation": {},
        "warnings": [],
        "failure": None,
        "timings_ms": {},
        "diagnostics": {"messages": [], "truncated": False},
    }


def run_worker(
    request_path,
    response_path,
):
    with CheckpointWriter(Path(request_path).parent) as checkpoint:
        checkpoint.publish("worker_started")
        with HeldDocument(request_path, REQUEST_SCHEMA) as document:
            request = document.payload
            try:
                response = _base_response(request)
            except BaseException:
                checkpoint.publish("response_publication_attempted")
                raise
            attestation = None
            warning_handler = None
            try:
                response["phase_reached"] = "probes"
                unused_probes, attestation = attest_before_native_import(
                    request, STAGE_ROOT, MEDIA_ROOT, PROJECT_ROOT, checkpoint
                )
                completed = set(checkpoint.states)
                for state in ("probes_passed", "snapshot_verified", "seed_verified"):
                    if state not in completed:
                        checkpoint.publish(state)
                response["phase_reached"] = "request"
                try:
                    document.assert_unchanged()
                except ProtocolError:
                    response["failure"] = _diagnostic("request", "request_integrity_failed")
                    raise
                checkpoint.publish("request_verified")

                # This is intentionally the first FieldStation42 import in this worker.
                response["phase_reached"] = "native_import"
                try:
                    native = importlib.import_module("station_director.native_single_run")
                except Exception:
                    response["failure"] = _diagnostic("native_import", "native_import_failed")
                    raise
                checkpoint.publish("native_import_completed")
                warning_handler = _BoundedWarningHandler()
                logging.getLogger().addHandler(warning_handler)
                result = native.execute_native_single_run(request, attestation)
                response.update(
                    channels=result["channels"],
                    preservation=result["preservation"],
                    path_validation=result["path_validation"],
                    guide_validation=result["guide_validation"],
                    timings_ms=result["timings_ms"],
                    verification=result["verification"],
                    scheduler_invoked=result["scheduler_invoked"],
                    phase_reached="complete",
                    status="success",
                )
            except BootstrapError as exc:
                response["phase_reached"] = getattr(
                    exc, "phase", response["phase_reached"])
                if response["failure"] is None:
                    fallback = {
                        "probes": "isolation_probe_failed",
                        "snapshot": "projected_configuration_verification_failed",
                        "seed": "validation_context_failed",
                        "configuration": "invalid_configuration",
                    }.get(response["phase_reached"], "native_failure")
                    category = exc.fingerprint_category
                    if exc.code is None and response["phase_reached"] == "snapshot":
                        category = "projected_logical_configuration"
                    response["failure"] = _diagnostic(
                        response["phase_reached"], exc.code or fallback,
                        probe=exc.probe, fingerprint_category=category)
            except ProtocolError:
                if response["failure"] is None:
                    response["phase_reached"] = "request"
                    response["failure"] = _diagnostic(
                        "request", "request_integrity_failed")
            except SystemExit:
                response["failure"] = _diagnostic(
                    response["phase_reached"], "native_system_exit",
                    scheduler_invoked=response["scheduler_invoked"])
            except Exception as exc:
                response["phase_reached"] = getattr(exc, "phase", response["phase_reached"])
                response["scheduler_invoked"] = bool(
                    getattr(exc, "scheduler_invoked", False))
                if response["failure"] is None:
                    channel_name = getattr(exc, "channel", None)
                    channel_number = next((item["number"] for item in request["affected_channels"]
                                           if item["name"] == channel_name), None)
                    response["failure"] = _diagnostic(
                        response["phase_reached"], getattr(exc, "code", "native_failure"),
                        scheduler_invoked=response["scheduler_invoked"],
                        channel_number=channel_number)
                guide_validation = getattr(exc, "guide_validation", None)
                if guide_validation is not None:
                    response["guide_validation"] = guide_validation
            finally:
                if warning_handler is not None:
                    try:
                        logging.getLogger().removeHandler(warning_handler)
                        response["warnings"] = warning_handler.items
                        response["diagnostics"]["truncated"] = warning_handler.truncated
                    except Exception:
                        response["status"] = "failed"
                        if response["scheduler_invoked"]:
                            response["phase_reached"] = "preservation"
                            response["failure"] = _diagnostic(
                                "preservation", "native_failure",
                                scheduler_invoked=True)
                        else:
                            response["phase_reached"] = "native_import"
                            response["failure"] = _diagnostic(
                                "native_import", "native_import_failed")
                if attestation is not None:
                    try:
                        attestation.invalidate()
                    except Exception:
                        response["status"] = "failed"
                        phase = response["phase_reached"]
                        if phase not in {
                            "configuration", "catalog", "scheduler",
                            "preservation", "guide",
                        }:
                            phase = ("preservation" if response["scheduler_invoked"]
                                     else "configuration")
                        response["phase_reached"] = phase
                        response["failure"] = _diagnostic(
                            phase, "native_failure",
                            scheduler_invoked=response["scheduler_invoked"])
            checkpoint.publish("response_publication_attempted")
            write_private_json_exclusive(response_path, response, RESPONSE_SCHEMA)
            checkpoint.publish("response_publication_completed")
            return response


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        return 2
    try:
        result = run_worker(argv[0], argv[1])
    except Exception:
        return 2
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
