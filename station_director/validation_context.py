"""Pure canonical input, seed, and local-time helpers for Director validation."""

import hashlib
import json
import math
import os
import stat
import unicodedata
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo


VALIDATION_TIMEZONE = "America/Los_Angeles"
VALIDATION_PYTHON_HASH_SEED = "0"
SEED_INPUT_FIELDS = {
    "logical_protected_configuration_fingerprint",
    "logical_database_fingerprint",
    "logical_media_manifest_fingerprint",
}


def _typed_value(value):
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical values cannot contain non-finite numbers")
        return ["real", value.hex()]
    if isinstance(value, str):
        return ["text", value]
    if isinstance(value, list):
        return ["array", [_typed_value(item) for item in value]]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical object keys must be strings")
        return ["object", [[key, _typed_value(value[key])] for key in sorted(value)]]
    raise TypeError(f"unsupported canonical value type: {type(value).__name__}")


def _canonical_bytes(value):
    return json.dumps(
        _typed_value(value), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


def _reject_constant(value):
    raise ValueError(f"protected JSON contains non-finite number {value}")


def _object_without_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"protected JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def _protected_identity(value):
    if not isinstance(value, str) or not value:
        raise ValueError("protected configuration identity must be a non-empty string")
    normalized = unicodedata.normalize("NFC", value.replace("\\", "/"))
    identity = PurePosixPath(normalized)
    if identity.is_absolute() or not identity.parts or any(
        part in ("", ".", "..") for part in identity.parts
    ):
        raise ValueError(f"invalid protected configuration identity: {value!r}")
    return identity.as_posix()


def logical_protected_configuration_fingerprint(paths):
    """Hash protected JSON identities and logical values, never file metadata."""
    entries = []
    identities = set()
    for supplied_identity, supplied_path in paths.items():
        identity = _protected_identity(supplied_identity)
        if identity in identities:
            raise ValueError(f"duplicate protected configuration identity: {identity}")
        identities.add(identity)
        path = Path(supplied_path)
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            with os.fdopen(os.dup(descriptor), "r", encoding="utf-8") as handle:
                parsed = json.load(
                    handle,
                    parse_constant=_reject_constant,
                    object_pairs_hook=_object_without_duplicate_keys,
                )
        finally:
            os.close(descriptor)
        _typed_value(parsed)
        entries.append([identity, parsed])
    entries.sort(key=lambda item: item[0])
    return {
        "digest": hashlib.sha256(_canonical_bytes(entries)).hexdigest(),
        "file_count": len(entries),
    }


def logical_media_manifest_fingerprint(manifest):
    """Hash stable media identities/types/sizes without physical metadata."""
    stream = manifest.stream
    position = stream.tell()
    digest = hashlib.sha256()
    count = 0
    try:
        stream.seek(0)
        for raw_line in stream:
            record = json.loads(raw_line)
            file_type = record.get("type")
            logical = {
                "path_b64": record.get("path_b64"),
                "type": file_type,
                "size": record.get("size") if file_type == stat.S_IFREG else None,
                "symlink_target_b64": (
                    record.get("symlink_target_b64")
                    if file_type == stat.S_IFLNK else None
                ),
            }
            if not isinstance(logical["path_b64"], str) or not isinstance(file_type, int):
                raise ValueError("media manifest contains malformed logical identity")
            encoded = _canonical_bytes(logical)
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1
    finally:
        stream.seek(position)
    return {"digest": digest.hexdigest(), "entry_count": count}


def _local_boundary(value, field_name):
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be an aware ISO timestamp")
    try:
        supplied = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid aware ISO timestamp") from exc
    if supplied.tzinfo is None or supplied.utcoffset() is None:
        raise ValueError(f"{field_name} must include an America/Los_Angeles UTC offset")

    wall = supplied.replace(tzinfo=None)
    zone = ZoneInfo(VALIDATION_TIMEZONE)
    matches = []
    for fold in (0, 1):
        candidate = wall.replace(tzinfo=zone, fold=fold)
        round_trip = candidate.astimezone(timezone.utc).astimezone(zone)
        if (
            round_trip.replace(tzinfo=None) == wall
            and round_trip.fold == fold
            and candidate.utcoffset() == supplied.utcoffset()
        ):
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError(
            f"{field_name} is nonexistent, ambiguous, or has an offset incompatible "
            f"with {VALIDATION_TIMEZONE}"
        )
    return matches[0]


def proposal_local_range(proposal):
    start = _local_boundary(proposal.get("week_start"), "week_start")
    end = _local_boundary(proposal.get("week_end"), "week_end")
    if start.astimezone(timezone.utc) >= end.astimezone(timezone.utc):
        raise ValueError("proposal week_start must precede week_end")
    return start.replace(tzinfo=None), end.replace(tzinfo=None)


def proposal_boundary_to_db(value, field_name="proposal boundary"):
    return _local_boundary(value, field_name).replace(tzinfo=None).isoformat(sep=" ")


def canonical_seed_inputs(config_fingerprint, database_fingerprint, media_fingerprint):
    values = {
        "logical_protected_configuration_fingerprint": config_fingerprint,
        "logical_database_fingerprint": database_fingerprint,
        "logical_media_manifest_fingerprint": media_fingerprint,
    }
    for name, value in values.items():
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")
    return values


def derive_validation_context(proposal, policy, seed_inputs):
    if not isinstance(proposal, dict) or not isinstance(policy, dict):
        raise TypeError("proposal and policy must be objects")
    if not isinstance(seed_inputs, dict) or set(seed_inputs) != SEED_INPUT_FIELDS:
        raise ValueError("validation seed inputs are malformed")
    requested_seed = proposal.get("seed")
    if isinstance(requested_seed, bool) or not isinstance(requested_seed, int):
        raise TypeError("proposal requested seed must be an integer")
    start, end = proposal_local_range(proposal)

    # The requested seed is recorded but excluded from the approved five-input
    # effective-seed contract.
    canonical_proposal = {key: value for key, value in proposal.items() if key != "seed"}
    material = {"proposal": canonical_proposal, "policy": policy, **seed_inputs}
    digest = hashlib.sha256(_canonical_bytes(material)).hexdigest()
    return {
        "input_fingerprint": digest,
        "requested_seed": requested_seed,
        "effective_seed": int(digest[:16], 16),
        "reference_clock": start.isoformat(sep=" "),
        "start_time": start.isoformat(sep=" "),
        "end_time": end.isoformat(sep=" "),
        "timezone": VALIDATION_TIMEZONE,
        "python_hash_seed": VALIDATION_PYTHON_HASH_SEED,
        "validation_mode": True,
    }


def verify_validation_context(proposal, policy, seed_inputs, supplied_context):
    expected = derive_validation_context(proposal, policy, seed_inputs)
    if supplied_context != expected:
        raise ValueError("validation scheduling context does not match canonical seed inputs")
    return expected
