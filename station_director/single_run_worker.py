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
    RESPONSE_SCHEMA,
    ProtocolError,
    write_private_json_exclusive,
)
from station_director.worker_bootstrap import BootstrapError, attest_before_native_import


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
        message = f"{record.name}: native warning emitted"[:1000]
        self.items.append(
            {
                "phase": "native",
                "channel": None,
                "code": "native_warning",
                "type": record.levelname,
                "message": message or record.levelname,
            }
        )


def _diagnostic(phase, code, exc, channel=None):
    message = str(exc).replace("/mnt/t7/CRT-Media", "crt-media:")[:1000]
    return {
        "phase": phase,
        "channel": channel,
        "code": code,
        "type": type(exc).__name__,
        "message": message or type(exc).__name__,
    }


def _base_response(request):
    context = request["validation_context"]
    return {
        "schema_version": 1,
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
    with HeldDocument(request_path, REQUEST_SCHEMA) as document:
        request = document.payload
        response = _base_response(request)
        attestation = None
        warning_handler = None
        try:
            response["phase_reached"] = "probes"
            unused_probes, attestation = attest_before_native_import(
                request, STAGE_ROOT, MEDIA_ROOT, PROJECT_ROOT
            )
            response["phase_reached"] = "snapshot"
            document.assert_unchanged()
            response["phase_reached"] = "seed"

            # This is intentionally the first FieldStation42 import in this worker.
            native = importlib.import_module("station_director.native_single_run")
            response["phase_reached"] = "native_import"
            warning_handler = _BoundedWarningHandler()
            logging.getLogger().addHandler(warning_handler)
            result = native.execute_native_single_run(
                request, attestation
            )
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
        except (BootstrapError, ProtocolError) as exc:
            response["phase_reached"] = getattr(
                exc, "phase", response["phase_reached"]
            )
            response["failure"] = _diagnostic(
                response["phase_reached"], "attestation_failure", exc
            )
        except SystemExit as exc:
            response["failure"] = _diagnostic(
                response["phase_reached"], "native_system_exit", exc
            )
        except Exception as exc:
            response["phase_reached"] = getattr(exc, "phase", response["phase_reached"])
            response["scheduler_invoked"] = bool(
                getattr(exc, "scheduler_invoked", False)
            )
            response["failure"] = _diagnostic(
                response["phase_reached"], getattr(exc, "code", "native_failure"),
                exc, getattr(exc, "channel", None),
            )
            guide_validation = getattr(exc, "guide_validation", None)
            if guide_validation is not None:
                response["guide_validation"] = guide_validation
            for label in ("original_failure", "restoration_failure"):
                detail = getattr(exc, label, None)
                if detail:
                    response["diagnostics"]["messages"].append(
                        f"{label}: {detail}"[:1000]
                    )
        finally:
            if warning_handler is not None:
                logging.getLogger().removeHandler(warning_handler)
                response["warnings"] = warning_handler.items
                response["diagnostics"]["truncated"] = warning_handler.truncated
            if attestation is not None:
                attestation.invalidate()
        document.assert_unchanged()
        write_private_json_exclusive(response_path, response, RESPONSE_SCHEMA)
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
