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


NATIVE_SHORT_MEDIA_SECONDS = 5 * 60
DEFAULT_TIMEOUT_SECONDS = 30.0
TERMINATE_GRACE_SECONDS = 2.0
KILL_GRACE_SECONDS = 2.0
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
METHOD_FFPROBE = "ffprobe_show_chapters_v1"
METHOD_SHORT = "native_short_media_v1"
COMPLETED_METHODS = frozenset({METHOD_FFPROBE, METHOD_SHORT})


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

    def __post_init__(self):
        if self.method not in COMPLETED_METHODS:
            raise ValueError("unknown completed chapter-analysis method")
        if not isinstance(self.chapters, tuple):
            raise TypeError("completed chapters must be a tuple")

    def as_list(self):
        return [dict(item) for item in self.chapters]


def _number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def validate_chapters(chapters, duration):
    """Return a canonical immutable chapter sequence or raise a fixed failure."""
    if not isinstance(chapters, list) or len(chapters) > 100_000:
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
            if not isinstance(item["title"], str) or len(item["title"].encode("utf-8")) > 4096:
                raise ChapterAnalysisError("chapter_data_invalid")
            normalized["title"] = item["title"]
        canonical.append(normalized)
        previous_start, previous_end = start, end
    return tuple(canonical)


def _parse_probe_output(payload, duration):
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ChapterAnalysisError("probe_output_invalid") from exc
    if not isinstance(document, dict) or set(document) - {"chapters"}:
        raise ChapterAnalysisError("probe_output_invalid")
    raw = document.get("chapters", [])
    if not isinstance(raw, list) or len(raw) > 100_000:
        raise ChapterAnalysisError("probe_output_invalid")
    chapters = []
    for chapter in raw:
        if not isinstance(chapter, dict):
            raise ChapterAnalysisError("chapter_data_invalid")
        try:
            start = float(chapter["start_time"])
            end = float(chapter["end_time"])
        except (KeyError, TypeError, ValueError) as exc:
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
    chapters = _parse_probe_output(bytes(output), float(duration))
    return CompletedChapterAnalysis(METHOD_FFPROBE, chapters)
