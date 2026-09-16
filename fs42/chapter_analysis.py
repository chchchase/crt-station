"""Strict, bounded chapter analysis shared by native maintenance callers."""

from __future__ import annotations

import json
import math
import os
import select
import signal
import subprocess
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re


NATIVE_SHORT_MEDIA_SECONDS = 5 * 60
DEFAULT_TIMEOUT_SECONDS = 30.0
TERMINATE_GRACE_SECONDS = 2.0
KILL_GRACE_SECONDS = 2.0
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
METHOD_FFPROBE = "ffprobe_show_chapters_v1"
METHOD_SHORT = "native_short_media_v1"
COMPLETED_METHODS = frozenset({METHOD_FFPROBE, METHOD_SHORT})
NEGATIVE_METHODS = frozenset({METHOD_FFPROBE})
ACTIVE_NEGATIVE_METHOD = METHOD_FFPROBE
UNUSABLE_REASON = "final_endpoint_exceeds_cached_duration"
TRUSTED_CHAPTER_STATES = frozenset({"trusted_v1", "trusted_negative_v2"})


class ChapterAnalysisError(RuntimeError):
    """A fixed-category failure that must never be persisted as an empty scan."""

    def __init__(self, category):
        if category not in {
            "probe_launch_failed", "probe_timeout", "probe_nonzero",
            "probe_signaled", "probe_output_too_large", "probe_output_invalid",
            "chapter_data_invalid", "probe_cleanup_failed",
        }:
            category = "chapter_data_invalid"
        super().__init__(category)
        self.category = category


@dataclass(frozen=True)
class CompletedChapterAnalysis:
    """A successful analysis, including the successful no-chapter outcome."""

    method: str
    chapters: tuple
    unusable_reason: str | None = None
    trusted_duration: str | None = None

    def __post_init__(self):
        if self.method not in COMPLETED_METHODS:
            raise ValueError("unknown completed chapter-analysis method")
        if not isinstance(self.chapters, tuple):
            raise TypeError("completed chapters must be a tuple")
        if self.unusable_reason is not None:
            if (self.unusable_reason != UNUSABLE_REASON or self.chapters
                    or self.method != ACTIVE_NEGATIVE_METHOD):
                raise ValueError("invalid negative chapter analysis")
            decode_trusted_duration(self.trusted_duration)
        elif self.trusted_duration is not None:
            raise ValueError("unexpected trusted duration")

    def as_list(self):
        return [dict(item) for item in self.chapters]


def _number(value):
    try:
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value))
    except OverflowError:
        return False


def encode_trusted_duration(value):
    """Canonical binary64 hexadecimal string; never coerce bool/string inputs."""
    if not _number(value) or value <= 0:
        raise ValueError("invalid trusted duration")
    return float(value).hex()


def decode_trusted_duration(value):
    if not isinstance(value, str) or len(value) > 32:
        raise ValueError("invalid trusted duration")
    try:
        number = float.fromhex(value)
        if encode_trusted_duration(number) != value:
            raise ValueError("noncanonical trusted duration")
        return number
    except (OverflowError, ValueError):
        raise ValueError("invalid trusted duration") from None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate chapter key")
        result[key] = value
    return result


def _invalid_constant(unused):
    raise ValueError("invalid chapter number")


def load_chapter_json(payload):
    return json.loads(payload, object_pairs_hook=_unique_object,
                      parse_constant=_invalid_constant)


def classify_attestation(value, duration, size, mtime_ns):
    """One envelope validator for playback, validation and maintenance."""
    if not isinstance(value, dict) or type(value.get("attestation_version")) is not int:
        raise ValueError("invalid chapter attestation")
    version = value["attestation_version"]
    expected = {"attestation_version", "method", "media_identity"}
    expected |= {"chapters"} if version == 1 else {
        "outcome", "reason", "trusted_duration"}
    if version not in {1, 2} or set(value) != expected:
        raise ValueError("invalid chapter attestation")
    identity = value["media_identity"]
    if (not isinstance(identity, dict) or set(identity) != {"size", "mtime_ns"}
            or type(identity["size"]) is not int or identity["size"] < 0
            or type(identity["mtime_ns"]) is not int):
        raise ValueError("invalid chapter identity")
    matches = identity == {"size": size, "mtime_ns": mtime_ns}
    if version == 2:
        if (value["method"] not in NEGATIVE_METHODS
                or value["outcome"] != "unusable_chapters"
                or value["reason"] != UNUSABLE_REASON):
            raise ValueError("invalid negative chapter attestation")
        decode_trusted_duration(value["trusted_duration"])
        current = encode_trusted_duration(duration)
        status = ("trusted_negative_v2" if matches
                  and value["trusted_duration"] == current
                  and value["method"] == ACTIVE_NEGATIVE_METHOD
                  else "re_attestation_required")
        return status, ()
    if value["method"] not in COMPLETED_METHODS or not matches:
        raise ValueError("invalid chapter attestation")
    chapters = validate_chapters(value["chapters"], duration)
    if value["method"] == METHOD_SHORT and (chapters or duration >= NATIVE_SHORT_MEDIA_SECONDS):
        raise ValueError("invalid short-media chapter attestation")
    return "trusted_v1", chapters


def validate_chapters(chapters, duration):
    """Return a canonical immutable chapter sequence or raise a fixed failure."""
    if (not _number(duration) or duration <= 0
            or not isinstance(chapters, list) or len(chapters) > 100_000):
        raise ChapterAnalysisError("chapter_data_invalid")
    canonical = []
    previous_start = -1.0
    previous_end = 0.0
    for item in chapters:
        if not isinstance(item, dict):
            raise ChapterAnalysisError("chapter_data_invalid")
        if set(item) - {"chapter_start", "chapter_end", "segment_duration", "title"}:
            raise ChapterAnalysisError("chapter_data_invalid")
        start = item.get("chapter_start")
        end = item.get("chapter_end")
        segment = item.get("segment_duration", end - start if _number(start) and _number(end) else None)
        if not all(_number(value) for value in (start, end, segment)):
            raise ChapterAnalysisError("chapter_data_invalid")
        start, end, segment = float(start), float(end), float(segment)
        if (
            start < 0 or end < start or end > float(duration)
            or start < previous_start or start < previous_end
            or segment < 0
        ):
            raise ChapterAnalysisError("chapter_data_invalid")
        normalized = {
            "chapter_start": start,
            "chapter_end": end,
            "segment_duration": segment,
        }
        if "title" in item:
            try:
                valid_title = isinstance(item["title"], str) and len(item["title"].encode("utf-8")) <= 4096
            except UnicodeError:
                valid_title = False
            if not valid_title:
                raise ChapterAnalysisError("chapter_data_invalid")
            normalized["title"] = item["title"]
        canonical.append(normalized)
        previous_start, previous_end = start, end
    return tuple(canonical)


def _probe_number(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("invalid chapter number")
    if isinstance(value, str) and (len(value) > 128 or not re.fullmatch(
            r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value)):
        raise ValueError("invalid chapter number")
    exact = Decimal(value)
    if not exact.is_finite() or exact < 0:
        raise ValueError("invalid chapter number")
    number = float(exact)
    if not math.isfinite(number) or (number == 0 and exact != 0):
        raise ValueError("invalid chapter number")
    return exact


def _parse_probe_output(payload, duration, *, allow_negative=False):
    if not _number(duration) or duration <= 0:
        raise ChapterAnalysisError("chapter_data_invalid")
    exact_duration = Decimal.from_float(float(duration))
    try:
        document = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object,
                              parse_float=Decimal, parse_constant=_invalid_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ChapterAnalysisError("probe_output_invalid") from exc
    if not isinstance(document, dict) or set(document) - {"chapters"}:
        raise ChapterAnalysisError("probe_output_invalid")
    raw = document.get("chapters", [])
    if not isinstance(raw, list) or len(raw) > 100_000:
        raise ChapterAnalysisError("probe_output_invalid")
    chapters = []
    exact_chapters = []
    for chapter in raw:
        if not isinstance(chapter, dict):
            raise ChapterAnalysisError("chapter_data_invalid")
        try:
            exact_start = _probe_number(chapter["start_time"])
            exact_end = _probe_number(chapter["end_time"])
            if (exact_end < exact_start
                    or exact_chapters and exact_start < exact_chapters[-1][1]):
                raise ValueError("invalid chapter geometry")
            exact_chapters.append((exact_start, exact_end))
            start, end = float(exact_start), float(exact_end)
        except (KeyError, TypeError, ValueError, OverflowError, InvalidOperation) as exc:
            raise ChapterAnalysisError("chapter_data_invalid") from exc
        item = {"chapter_start": start, "chapter_end": end}
        tags = chapter.get("tags")
        if tags is not None:
            if not isinstance(tags, dict):
                raise ChapterAnalysisError("chapter_data_invalid")
            if "title" in tags:
                item["title"] = tags["title"]
        chapters.append(item)
    if chapters and chapters[0]["chapter_start"] > 0:
        chapters.insert(0, {
            "chapter_start": 0.0,
            "chapter_end": chapters[0]["chapter_start"],
        })
    for index, chapter in enumerate(chapters):
        next_start = (
            chapters[index + 1]["chapter_start"]
            if index + 1 < len(chapters) else float(duration)
        )
        chapter["segment_duration"] = next_start - chapter["chapter_start"]
    overrun = bool(exact_chapters and exact_chapters[-1][1] > exact_duration)
    if exact_chapters and (exact_chapters[-1][0] > exact_duration
                          or any(end > exact_duration for start, end in exact_chapters[:-1])):
        raise ChapterAnalysisError("chapter_data_invalid")
    if allow_negative and overrun:
        # Validate every other invariant against the unmodified final endpoint.
        # The computed last segment still uses the trusted cached duration.
        validate_chapters(chapters, chapters[-1]["chapter_end"])
        return CompletedChapterAnalysis(
            METHOD_FFPROBE, (), UNUSABLE_REASON, encode_trusted_duration(duration))
    if overrun:
        raise ChapterAnalysisError("chapter_data_invalid")
    return validate_chapters(chapters, duration)


def _terminate(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        return process.wait(timeout=TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            return process.wait(timeout=KILL_GRACE_SECONDS)
        except subprocess.TimeoutExpired as exc:
            raise ChapterAnalysisError("probe_cleanup_failed") from exc


def analyze_chapters(input_path, duration, *, timeout=DEFAULT_TIMEOUT_SECONDS,
                     pass_fds=(), popen=subprocess.Popen, clock=time.monotonic):
    """Analyze one held local file path without accepting failure as success."""
    from fs42.scheduling_context import block_validation_media_runtime
    block_validation_media_runtime()
    if not _number(duration) or duration <= 0:
        raise ChapterAnalysisError("chapter_data_invalid")
    if duration < NATIVE_SHORT_MEDIA_SECONDS:
        return CompletedChapterAnalysis(METHOD_SHORT, ())

    arguments = [
        "/usr/bin/ffprobe", "-v", "quiet", "-protocol_whitelist", "file,pipe",
        "-print_format", "json", "-show_chapters", os.fspath(input_path),
    ]
    try:
        process = popen(
            arguments,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=tuple(pass_fds),
            start_new_session=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ChapterAnalysisError("probe_launch_failed") from exc

    output = bytearray()
    deadline = clock() + float(timeout)
    descriptor = process.stdout.fileno()
    os.set_blocking(descriptor, False)
    eof = False
    try:
        while not eof or process.poll() is None:
            remaining = deadline - clock()
            if remaining <= 0:
                _terminate(process)
                raise ChapterAnalysisError("probe_timeout")
            readable, unused_write, unused_error = select.select(
                [descriptor] if not eof else [], [], [], min(0.1, remaining)
            )
            if readable:
                chunk = os.read(descriptor, 64 * 1024)
                if not chunk:
                    eof = True
                else:
                    output.extend(chunk)
                    if len(output) > MAX_OUTPUT_BYTES:
                        _terminate(process)
                        raise ChapterAnalysisError("probe_output_too_large")
            elif process.poll() is not None and not eof:
                chunk = os.read(descriptor, 64 * 1024)
                if chunk:
                    output.extend(chunk)
                    if len(output) > MAX_OUTPUT_BYTES:
                        raise ChapterAnalysisError("probe_output_too_large")
                else:
                    eof = True
        status = process.wait(timeout=0)
    except ChapterAnalysisError:
        raise
    except Exception as exc:
        _terminate(process)
        raise ChapterAnalysisError("probe_cleanup_failed") from exc
    finally:
        if process.stdout is not None:
            process.stdout.close()

    if status < 0:
        raise ChapterAnalysisError("probe_signaled")
    if status != 0:
        raise ChapterAnalysisError("probe_nonzero")
    chapters = _parse_probe_output(bytes(output), float(duration), allow_negative=True)
    if isinstance(chapters, CompletedChapterAnalysis):
        return chapters
    return CompletedChapterAnalysis(METHOD_FFPROBE, chapters)
