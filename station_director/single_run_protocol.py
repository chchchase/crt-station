"""Strict, private-file protocol for one isolated native scheduling run."""

import hashlib
import json
import os
import stat
from pathlib import Path

from station_director.validation_context import derive_request_digest


PROTOCOL_VERSION = 1
RESPONSE_PROTOCOL_VERSION = 4
OPERATION = "native_single_run"
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
SCHEMA_DIR = Path(__file__).with_name("schemas")
REQUEST_SCHEMA = SCHEMA_DIR / "native-single-run.request.v1.schema.json"
RESPONSE_SCHEMA_V1 = SCHEMA_DIR / "native-single-run.response.v1.schema.json"
# Compatibility name for callers that explicitly validate the frozen v1 shape.
RESPONSE_SCHEMA = RESPONSE_SCHEMA_V1
RESPONSE_SCHEMA_V2 = SCHEMA_DIR / "native-single-run.response.v2.schema.json"
RESPONSE_SCHEMA_V3 = SCHEMA_DIR / "native-single-run.response.v3.schema.json"
RESPONSE_SCHEMA_V4 = SCHEMA_DIR / "native-single-run.response.v4.schema.json"


class ProtocolError(RuntimeError):
    pass


def strict_json_loads(raw):
    """Decode protocol JSON while rejecting duplicates at every object level."""
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ProtocolError(f"protocol JSON contains duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value):
        raise ProtocolError(f"protocol JSON contains non-finite number: {value}")

    try:
        return json.loads(
            raw, object_pairs_hook=reject_duplicates, parse_constant=reject_constant
        )
    except ProtocolError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"protocol JSON is malformed: {exc}") from exc


def request_digest(payload):
    material = dict(payload)
    material.pop("request_digest", None)
    return derive_request_digest(material)


def bind_request(payload):
    result = dict(payload)
    result["request_digest"] = request_digest(result)
    validate_document(result, REQUEST_SCHEMA)
    validate_request_semantics(result)
    return result


def validate_request_semantics(payload):
    expected = [
        (1, "CRT Station Guide"), (2, "Action"), (3, "After School"),
        (4, "Anime"), (5, "Cartoon Network"), (6, "Disney"),
        (7, "Late Night"), (8, "Watch In Order"),
    ]
    actual = [(item["number"], item["name"]) for item in payload["policy"]["channels"]]
    if actual != expected:
        raise ProtocolError("request policy does not contain the canonical channel lineup")
    affected = [(item["number"], item["name"]) for item in payload["affected_channels"]]
    canonical = [item for item in expected if 2 <= item[0] <= 7]
    if affected != sorted(affected) or any(item not in canonical for item in affected):
        raise ProtocolError("affected channels are not a canonical ordered subset")
    if len({number for number, unused in affected}) != len(affected) or len({name for unused, name in affected}) != len(affected):
        raise ProtocolError("affected channels contain a duplicate identity")
    fingerprints = payload["input_fingerprints"]
    seeds = payload["seed_inputs"]
    if seeds["logical_protected_configuration_fingerprint"] != fingerprints["original_logical_configuration_fingerprint"]:
        raise ProtocolError("configuration seed input does not match original snapshot")
    if seeds["logical_database_fingerprint"] != fingerprints["original_logical_database_fingerprint"]:
        raise ProtocolError("database seed input does not match original snapshot")
    if seeds["logical_media_manifest_fingerprint"] != fingerprints["logical_media_manifest_fingerprint"]:
        raise ProtocolError("media seed input does not match original snapshot")
    if payload["request_digest"] != request_digest(payload):
        raise ProtocolError("request digest mismatch")


def validate_response_semantics(payload):
    if payload.get("schema_version") in (2, 3, RESPONSE_PROTOCOL_VERSION):
        from station_director.c1_diagnostics import (
            WORKER_DIAGNOSTIC_CODES, validate_diagnostic,
        )
        try:
            for diagnostic in payload.get("warnings", []):
                validate_diagnostic(diagnostic, legacy=payload["schema_version"] < 4)
                if diagnostic["code"] not in WORKER_DIAGNOSTIC_CODES:
                    raise ValueError("host-only diagnostic")
                if diagnostic.get("preservation_detail") is not None:
                    raise ValueError("preservation detail requires a failure")
            if payload.get("failure") is not None:
                validate_diagnostic(payload["failure"], legacy=payload["schema_version"] < 4)
                if payload["failure"]["code"] not in WORKER_DIAGNOSTIC_CODES:
                    raise ValueError("host-only diagnostic")
                if payload["failure"]["phase"] != payload["phase_reached"]:
                    raise ValueError("phase mismatch")
                if payload["failure"]["scheduler_invoked"] != payload["scheduler_invoked"]:
                    raise ValueError("scheduler state mismatch")
        except ValueError as exc:
            raise ProtocolError("worker response diagnostic is invalid") from exc
    if payload["status"] == "success" and payload["failure"] is not None:
        raise ProtocolError("successful response contains a failure")
    if payload["status"] == "failed" and payload["failure"] is None:
        raise ProtocolError("failed response omits its failure")
    if payload["status"] == "success" and (
        not payload["scheduler_invoked"]
        or not payload["verification"]
        or not payload["preservation"]
        or not payload["path_validation"]
        or not payload["guide_validation"]
        or not payload["timings_ms"]
        or payload["phase_reached"] != "complete"
    ):
        raise ProtocolError("successful response is incomplete")
    expected = [(item["number"], item["name"]) for item in payload["affected_channels"]]
    actual = [(item["number"], item["name"]) for item in payload["channels"]]
    if payload["status"] == "success" and actual != expected:
        raise ProtocolError("successful response does not contain exactly every affected channel")
    if len(set(actual)) != len(actual):
        raise ProtocolError("response contains duplicate channel results")
    if payload["status"] == "failed" and payload["channels"]:
        raise ProtocolError("failed response must not expose partial channel results")
    if payload["status"] == "failed" and payload["phase_reached"] == "complete":
        raise ProtocolError("failed response cannot claim the complete phase")
    if payload["scheduler_invoked"] and payload["phase_reached"] in {
        "request", "probes", "snapshot", "seed", "native_import", "configuration", "catalog"
    }:
        raise ProtocolError("response claims scheduling before the scheduler phase")
    guide = payload["guide_validation"]
    if payload["status"] == "success" and (
        guide.get("status") != "pass"
        or any(guide[name].get("status") != "pass" for name in (
            "snapshot_preparation", "snapshot_verification", "post_read_verification"
        ))
    ):
        raise ProtocolError("successful response does not contain complete guide verification")
    if payload["status"] == "failed" and guide and guide.get("status") != "failed":
        raise ProtocolError("failed response contains a successful guide result")


def validate_document(payload, schema_path):
    try:
        import jsonschema

        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        from referencing import Registry, Resource

        proposal = json.loads(
            (SCHEMA_DIR / "proposal.v2.schema.json").read_text(encoding="utf-8")
        )
        registry = Registry().with_resource(
            "https://crt-station.invalid/schemas/proposal.v2.schema.json",
            Resource.from_contents(proposal),
        )
        for candidate in SCHEMA_DIR.glob("*.schema.json"):
            candidate_schema = json.loads(candidate.read_text(encoding="utf-8"))
            identifier = candidate_schema.get("$id")
            if identifier:
                registry = registry.with_resource(
                    identifier, Resource.from_contents(candidate_schema))
        validator = jsonschema.Draft7Validator(
            schema, registry=registry, format_checker=jsonschema.FormatChecker()
        )
        errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"cannot load protocol schema: {exc}") from exc
    if errors:
        first = errors[0]
        location = ".".join(str(item) for item in first.absolute_path) or "document"
        raise ProtocolError(f"protocol schema rejected {location}: {first.message}")


class HeldDocument:
    """A parsed private regular file whose descriptor remains held until close."""

    def __init__(self, path, schema_path, *, expected_digest=None):
        self.path = Path(path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            self.descriptor = os.open(self.path, flags)
        except OSError as exc:
            raise ProtocolError(f"cannot open protocol file safely: {exc}") from exc
        try:
            before = os.fstat(self.descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ProtocolError("protocol file is not a regular file")
            if before.st_nlink != 1:
                raise ProtocolError("protocol file has an unexpected hard-link count")
            if stat.S_IMODE(before.st_mode) != 0o600:
                raise ProtocolError("protocol file permissions must be 0600")
            raw = self._bounded_read()
            after = os.fstat(self.descriptor)
            identity = lambda value: (
                value.st_dev, value.st_ino, value.st_size,
                value.st_mtime_ns, value.st_ctime_ns, value.st_nlink,
            )
            if identity(before) != identity(after):
                raise ProtocolError("protocol file changed while it was read")
            self._identity = identity(after)
            self._raw_digest = hashlib.sha256(raw.encode("utf-8")).digest()
            self.payload = strict_json_loads(raw)
            validate_document(self.payload, schema_path)
            if Path(schema_path) == REQUEST_SCHEMA:
                validate_request_semantics(self.payload)
            elif Path(schema_path) in (RESPONSE_SCHEMA_V1, RESPONSE_SCHEMA_V2,
                                       RESPONSE_SCHEMA_V3, RESPONSE_SCHEMA_V4):
                validate_response_semantics(self.payload)
            if expected_digest is not None and request_digest(self.payload) != expected_digest:
                raise ProtocolError("request digest changed")
        except Exception:
            self.close()
            raise

    def _bounded_read(self):
        chunks = []
        total = 0
        while True:
            chunk = os.read(self.descriptor, min(65536, MAX_DOCUMENT_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_DOCUMENT_BYTES:
                raise ProtocolError("protocol document exceeds 2 MiB")
        return b"".join(chunks).decode("utf-8")

    def assert_unchanged(self):
        try:
            path_info = self.path.lstat()
            held_info = os.fstat(self.descriptor)
        except OSError as exc:
            raise ProtocolError(f"protocol file disappeared or changed: {exc}") from exc
        if (
            not stat.S_ISREG(path_info.st_mode)
            or path_info.st_dev != held_info.st_dev
            or path_info.st_ino != held_info.st_ino
            or path_info.st_nlink != 1
            or stat.S_IMODE(path_info.st_mode) != 0o600
        ):
            raise ProtocolError("protocol path was replaced or relinked")
        current_identity = (
            held_info.st_dev, held_info.st_ino, held_info.st_size,
            held_info.st_mtime_ns, held_info.st_ctime_ns, held_info.st_nlink,
        )
        if current_identity != self._identity:
            raise ProtocolError("protocol file was modified while held")
        os.lseek(self.descriptor, 0, os.SEEK_SET)
        if hashlib.sha256(self._bounded_read().encode("utf-8")).digest() != self._raw_digest:
            raise ProtocolError("protocol file contents changed while held")

    def close(self):
        descriptor = getattr(self, "descriptor", None)
        if descriptor is not None:
            os.close(descriptor)
            self.descriptor = None

    def __enter__(self):
        return self

    def __exit__(self, unused_type, unused_value, unused_traceback):
        self.close()


def write_private_json_exclusive(path, payload, schema_path):
    validate_document(payload, schema_path)
    if Path(schema_path) == REQUEST_SCHEMA:
        validate_request_semantics(payload)
    elif Path(schema_path) in (RESPONSE_SCHEMA_V1, RESPONSE_SCHEMA_V2,
                               RESPONSE_SCHEMA_V3, RESPONSE_SCHEMA_V4):
        validate_response_semantics(payload)
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    if os.path.lexists(path) or os.path.lexists(temporary):
        raise ProtocolError("refusing duplicate protocol output")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(encoded) > MAX_DOCUMENT_BYTES:
            raise ProtocolError("protocol document exceeds 2 MiB")
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise ProtocolError("temporary protocol output is not private and regular")
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        final = path.lstat()
        if final.st_dev != info.st_dev or final.st_ino != info.st_ino or final.st_nlink != 1:
            raise ProtocolError("protocol output publication was not exclusive")
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if os.path.lexists(temporary):
            os.unlink(temporary)
        raise
    finally:
        os.close(descriptor)
