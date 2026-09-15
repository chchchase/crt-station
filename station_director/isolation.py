import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid
import fcntl
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from station_director.isolation_probe import PROBE_RESULTS, PROBE_SCHEMA_VERSION


MEDIA_ROOT = Path("/mnt/t7/CRT-Media")
STAGING_PARENT = Path("/tmp")
STAGING_RE = re.compile(r"fs42-i-[0-9a-f]{12}\Z")
STAGING_MAX_AGE_SECONDS = 6 * 60 * 60
UNIT_TIMEOUT_SECONDS = 30
UNIT_INSPECTION_TIMEOUT_SECONDS = 15
UNIT_POLL_SECONDS = 1.0
CLEANUP_TIMEOUT_SECONDS = 10
MAX_CAPTURE_BYTES = 64 * 1024
PROBE_OUTPUT = "preflight-probe.json"
class IsolationError(RuntimeError):
    pass


@dataclass
class LaunchResult:
    unit_name: str
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    termination_kind: str = "completed"
    exit_status: object = None
    signal: object = None
    main_process_started: object = None
    unit_state_valid: bool = False
    launcher_executed: bool = True


def _utc_now():
    return datetime.now(timezone.utc)


def _new_run_id():
    return f"{_utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:12]}"


def new_run_id():
    return _new_run_id()


def _process_ancestry(proc_root=Path("/proc"), start_pid=None):
    pid = os.getpid() if start_pid is None else start_pid
    visited = set()
    names = []
    for unused in range(64):
        if pid in visited:
            raise IsolationError("process ancestry contains a cycle")
        visited.add(pid)
        process = Path(proc_root) / str(pid)
        comm = (process / "comm").read_text(encoding="utf-8").strip()
        cmdline = (process / "cmdline").read_bytes().split(b"\0", 1)[0]
        if comm:
            names.append(comm)
        if cmdline:
            names.append(Path(os.fsdecode(cmdline)).name)
        if pid == 1:
            return names
        status = (process / "status").read_text(encoding="utf-8")
        match = re.search(r"^PPid:\s+(\d+)$", status, re.MULTILINE)
        if match is None or int(match.group(1)) == 0:
            raise IsolationError("process ancestry ended before PID 1")
        pid = int(match.group(1))
    raise IsolationError("process ancestry exceeded 64 entries")


def check_invocation_context(environ=None, ancestry=None):
    environ = os.environ if environ is None else environ
    markers = sorted(key for key in environ if key.startswith("CODEX_"))
    if markers:
        return False, f"Codex environment markers present: {','.join(markers)}"
    try:
        ancestry = _process_ancestry() if ancestry is None else ancestry
    except (OSError, UnicodeError, IsolationError) as exc:
        return False, f"could not verify host process ancestry: {exc}"
    names = [name.casefold() for name in ancestry]
    codex_names = sorted({name for name in names if "codex" in name})
    if codex_names:
        return False, f"Codex process ancestry detected: {','.join(codex_names)}"
    if not any(name == "sshd" or name.startswith("sshd:") for name in names):
        return False, "no sshd ancestor found"
    return True, "host ancestry contains sshd and no Codex markers"


def _staging_name(token):
    return f"fs42-i-{token}"


def _is_absent_unit_message(value):
    lowered = value.casefold()
    return any(text in lowered for text in ("not found", "not loaded", "could not be found", "does not exist"))


def _run_cleanup_command(argv):
    try:
        completed = subprocess.run(
            argv, capture_output=True, text=True, timeout=CLEANUP_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    detail = (completed.stderr or completed.stdout).strip()
    return completed.returncode == 0 or _is_absent_unit_message(detail), detail


def _unit_absent(unit_name):
    try:
        shown = subprocess.run(
            ["systemctl", "--user", "show", "--property=LoadState", "--value", unit_name],
            capture_output=True, text=True, timeout=CLEANUP_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    detail = (shown.stderr or shown.stdout).strip()
    absent = shown.returncode != 0 and _is_absent_unit_message(detail)
    absent = absent or (shown.returncode == 0 and shown.stdout.strip() == "not-found")
    return absent, detail or "no output"


def cleanup_unit(unit_name):
    details = []
    for action in ("stop", "reset-failed"):
        ok, detail = _run_cleanup_command(
            ["systemctl", "--user", action, unit_name]
        )
        details.append(f"{action}: {detail or ('ok' if ok else 'failed')}")
    absent, detail = _unit_absent(unit_name)
    details.append(f"show: {detail}")
    for signal in ("TERM", "KILL"):
        if absent:
            break
        ok, detail = _run_cleanup_command(
            ["systemctl", "--user", "kill", "--kill-who=all", f"--signal={signal}", unit_name]
        )
        details.append(f"kill-{signal}: {detail or ('ok' if ok else 'failed')}")
        _run_cleanup_command(["systemctl", "--user", "stop", unit_name])
        absent, detail = _unit_absent(unit_name)
        details.append(f"show-after-{signal}: {detail}")
    return absent, "; ".join(details)


def _safe_staging_directory(path, active_paths=()):
    path = Path(path)
    if path.parent != STAGING_PARENT or not STAGING_RE.fullmatch(path.name):
        return False
    if path in {Path(item) for item in active_paths}:
        return False
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    return (
        stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)
        and info.st_uid == os.getuid() and not (path / ".quarantine").exists()
    )


def cleanup_staging_directory(path, active_paths=()):
    path = Path(path)
    if not _safe_staging_directory(path, active_paths=active_paths):
        return False, f"refused unsafe staging path: {path}"
    try:
        shutil.rmtree(path)
    except OSError as exc:
        return False, str(exc)
    return not os.path.lexists(path), "removed"


def _staging_is_locked(path):
    marker = path / ".active"
    try:
        info = marker.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            return False
        descriptor = os.open(marker, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except (FileNotFoundError, OSError):
        return False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(descriptor)


def cleanup_stale_directories(now=None, active_paths=()):
    now = time.time() if now is None else now
    failures = []
    try:
        candidates = list(STAGING_PARENT.iterdir())
    except OSError as exc:
        return False, [str(exc)]
    for path in candidates:
        if not STAGING_RE.fullmatch(path.name) or path in {Path(item) for item in active_paths}:
            continue
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
            continue
        if now - info.st_mtime < STAGING_MAX_AGE_SECONDS:
            continue
        if _staging_is_locked(path):
            continue
        ok, detail = cleanup_staging_directory(path, active_paths=active_paths)
        if not ok:
            failures.append(f"{path}: {detail}")
    return not failures, failures


def _existing_runtime_mounts():
    paths = [Path("/usr"), Path("/bin"), Path("/lib"), Path("/lib64")]
    paths.extend(
        Path(item) for item in (
            "/etc/ld.so.cache",
            "/etc/ld.so.conf",
            "/etc/ld.so.conf.d",
            "/etc/localtime",
        )
    )
    return [path for path in paths if path.exists()]


def _sandbox_python(project_root):
    project_python = project_root / "env/bin/python3"
    if project_python.is_file() and os.access(project_python, os.X_OK):
        return "/project/env/bin/python3"
    executable = Path(sys.executable).resolve()
    for prefix in (Path("/usr"), Path("/bin")):
        try:
            executable.relative_to(prefix)
            return str(executable)
        except ValueError:
            pass
    raise IsolationError(f"Python executable is outside mounted runtime paths: {executable}")


def sandbox_python(project_root):
    return _sandbox_python(project_root)


class HeldStageTemporary:
    """An exclusively-created stage-local /tmp source held through launch."""

    def __init__(self, staging_path):
        supplied_stage = Path(staging_path)
        try:
            stage_info = supplied_stage.lstat()
            expected_stage = STAGING_PARENT.resolve(strict=True) / supplied_stage.name
            resolved_stage = supplied_stage.resolve(strict=True)
        except OSError as exc:
            raise IsolationError(f"could not verify staging directory: {exc}") from exc
        if (
            supplied_stage.name != resolved_stage.name
            or not STAGING_RE.fullmatch(supplied_stage.name)
            or resolved_stage != expected_stage
            or not stat.S_ISDIR(stage_info.st_mode)
            or stat.S_ISLNK(stage_info.st_mode)
            or stage_info.st_uid != os.getuid()
            or stat.S_IMODE(stage_info.st_mode) != 0o700
        ):
            raise IsolationError(f"refusing unsafe staging directory: {supplied_stage}")
        self.stage = resolved_stage
        self.path = self.stage / "transient"
        self.descriptor = None
        try:
            os.mkdir(self.path, mode=0o700)
            flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            self.descriptor = os.open(self.path, flags)
            info = os.fstat(self.descriptor)
            self.identity = (info.st_dev, info.st_ino)
            self.assert_ready()
        except FileExistsError as exc:
            raise IsolationError(
                f"refusing pre-existing stage temporary path: {self.path}"
            ) from exc
        except Exception:
            self.close()
            try:
                self.path.rmdir()
            except OSError:
                pass
            raise

    def assert_ready(self):
        if self.descriptor is None:
            raise IsolationError("stage temporary descriptor is closed")
        try:
            held = os.fstat(self.descriptor)
            current = self.path.lstat()
        except OSError as exc:
            raise IsolationError(f"stage temporary path changed: {exc}") from exc
        if (
            not stat.S_ISDIR(held.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or (current.st_dev, current.st_ino) != self.identity
            or (held.st_dev, held.st_ino) != self.identity
            or stat.S_IMODE(current.st_mode) != 0o700
            or current.st_uid != os.getuid()
        ):
            raise IsolationError("stage temporary path identity or permissions changed")

    def close(self):
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None


def prepare_stage_temporary(staging_path):
    return HeldStageTemporary(staging_path)


def build_bwrap_command(
    project_root, staging_path, sandbox_argv, *, stage_tmp=False,
    verified_temporary=None,
):
    project_root = Path(project_root).resolve()
    staging_path = Path(staging_path).resolve()
    command = [
        "bwrap",
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--clearenv",
    ]
    for path in _existing_runtime_mounts():
        command.extend(("--ro-bind", str(path), str(path)))
    if stage_tmp:
        if (
            not isinstance(verified_temporary, HeldStageTemporary)
            or verified_temporary.stage != staging_path
        ):
            raise IsolationError("stage-backed /tmp requires a held verified source")
        verified_temporary.assert_ready()
        temporary_mount = ("--bind", str(verified_temporary.path), "/tmp")
    else:
        if verified_temporary is not None:
            raise IsolationError("ordinary tmpfs profile cannot accept a stage temporary source")
        temporary_mount = ("--tmpfs", "/tmp")
    command.extend(
        (
            "--ro-bind", str(project_root), "/project",
            "--ro-bind", str(MEDIA_ROOT), "/media",
            "--bind", str(staging_path), "/stage",
            "--proc", "/proc",
            "--dev", "/dev",
            *temporary_mount,
            "--chdir", "/stage",
            "--setenv", "PATH", "/usr/bin:/bin",
            "--setenv", "HOME", "/nonexistent",
            "--setenv", "TMPDIR", "/tmp",
            "--setenv", "LANG", "C.UTF-8",
            "--setenv", "LC_ALL", "C.UTF-8",
            "--setenv", "TZ", "America/Los_Angeles",
            "--setenv", "PYTHONHASHSEED", "0",
            "--setenv", "PYTHONDONTWRITEBYTECODE", "1",
            "--setenv", "PYTHONPATH", "/project",
            "--",
            *sandbox_argv,
        )
    )
    return command


class IsolationLauncher:
    """The single Bubblewrap launcher intended for preflight and staged validation."""

    def __init__(self, project_root):
        self.project_root = Path(project_root).resolve()

    def run(
        self, staging_path, sandbox_argv, unit_name, timeout=UNIT_TIMEOUT_SECONDS,
        *, stage_tmp=False,
    ):
        temporary = None
        try:
            if stage_tmp:
                temporary = prepare_stage_temporary(staging_path)
            bwrap = build_bwrap_command(
                self.project_root, staging_path, sandbox_argv,
                stage_tmp=stage_tmp, verified_temporary=temporary,
            )
            unit_timeout = max(1, timeout - 5)
            command = [
                "systemd-run",
                "--user",
                "--quiet",
                "--wait",
                "--pipe",
                "--service-type=exec",
                f"--unit={unit_name}",
                "--property=RestrictAddressFamilies=AF_UNIX",
                "--property=RemainAfterExit=yes",
                f"--property=RuntimeMaxSec={int(unit_timeout)}s",
                "--",
                *bwrap,
            ]
            result = _run_bounded(
                command, unit_name, timeout, supervise_retained_unit=True)
            if not result.launcher_executed:
                return result
            if temporary is not None:
                try:
                    temporary.assert_ready()
                except Exception as exc:
                    exc.launcher_result = result
                    raise
            return result
        finally:
            if temporary is not None:
                temporary.close()


def _run_bounded(
    command, unit_name, timeout, limit=MAX_CAPTURE_BYTES, *,
    supervise_retained_unit=False,
):
    """Drain both child streams fully while retaining only bounded prefixes."""
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as exc:
        return LaunchResult(
            unit_name, 127, "", str(exc), stderr_bytes=len(str(exc).encode()),
            termination_kind="launcher_failure", main_process_started=False,
            unit_state_valid=True, launcher_executed=False,
        )
    captures = {"stdout": bytearray(), "stderr": bytearray()}
    totals = {"stdout": 0, "stderr": 0}
    def drain(name, pipe):
        while True:
            chunk = pipe.read(65536)
            if not chunk:
                break
            totals[name] += len(chunk)
            available = max(0, limit - len(captures[name]))
            captures[name].extend(chunk[:available])
        pipe.close()
    threads = [
        threading.Thread(target=drain, args=("stdout", process.stdout)),
        threading.Thread(target=drain, args=("stderr", process.stderr)),
    ]
    for thread in threads:
        thread.start()
    timed_out = False
    evidence = None
    if supervise_retained_unit:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                evidence = inspect_unit_termination(
                    unit_name, launcher_timed_out=True,
                    inspection_timeout=UNIT_INSPECTION_TIMEOUT_SECONDS,
                )
                break
            evidence = inspect_unit_termination(
                unit_name, allow_running=True,
                inspection_timeout=min(
                    UNIT_INSPECTION_TIMEOUT_SECONDS, remaining),
            )
            if (evidence["valid"]
                    and evidence["termination_kind"] != "running"):
                break
            launcher_status = process.poll()
            if launcher_status is not None:
                break
            time.sleep(min(UNIT_POLL_SECONDS, remaining))
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        if evidence is not None and evidence["valid"]:
            kind = evidence["termination_kind"]
            if kind == "completed":
                returncode = 0
            elif kind == "nonzero_exit":
                returncode = evidence["exit_status"]
            elif kind in {"runtime_timeout", "outer_watchdog"}:
                returncode = 124
            else:
                returncode = 1
        else:
            returncode = process.returncode
            if returncode is None:
                returncode = 124 if timed_out else 1
    else:
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
            returncode = 124
    for thread in threads:
        thread.join(timeout=5)
    def decoded(name):
        value = captures[name].decode("utf-8", errors="replace")
        value = value.replace("/mnt/t7/CRT-Media", "crt-media:")
        value = re.sub(r"/home/[^\s:]+", "/project/[redacted]", value)
        value = re.sub(
            r"(?i)\b(token|password|secret|api[_-]?key)\s*[:=]\s*[^\s]+",
            r"\1=[REDACTED]", value,
        )
        return value
    result = LaunchResult(
        unit_name, returncode, decoded("stdout"), decoded("stderr"), timed_out,
        totals["stdout"], totals["stderr"], totals["stdout"] > limit,
        totals["stderr"] > limit,
    )
    if evidence is not None:
        result.termination_kind = evidence["termination_kind"]
        result.exit_status = evidence["exit_status"]
        result.signal = evidence["signal"]
        result.main_process_started = evidence["main_process_started"]
        result.unit_state_valid = evidence["valid"]
    return result


_UNIT_PROPERTIES = (
    "LoadState", "ActiveState", "SubState", "Result", "ExecMainCode",
    "ExecMainStatus", "ExecMainStartTimestampMonotonic",
)
_UNIT_RESULTS = {
    "success", "exit-code", "signal", "core-dump", "timeout", "oom-kill",
    "resources", "protocol", "start-limit-hit", "condition", "assert",
    "watchdog", "exec-condition", "skipped",
}
_EXEC_CODES = {"0": "", "1": "exited", "2": "killed", "3": "dumped",
               "": "", "exited": "exited", "killed": "killed", "dumped": "dumped"}


def _invalid_unit_evidence():
    return {
        "valid": False, "termination_kind": "launcher_state_invalid",
        "exit_status": None, "signal": None, "main_process_started": None,
    }


def inspect_unit_termination(
    unit_name, *, launcher_timed_out=False, allow_running=False,
    inspection_timeout=UNIT_INSPECTION_TIMEOUT_SECONDS,
):
    """Capture fixed systemd properties before cleanup; never expose raw values."""
    argv = [
        "systemctl", "--user", "show", "--no-pager",
        "--property=" + ",".join(_UNIT_PROPERTIES), unit_name,
    ]
    try:
        shown = subprocess.run(
            argv, capture_output=True, text=True,
            timeout=max(0.001, min(
                UNIT_INSPECTION_TIMEOUT_SECONDS, inspection_timeout)),
        )
    except (OSError, subprocess.TimeoutExpired):
        return _invalid_unit_evidence()
    if shown.returncode != 0:
        return _invalid_unit_evidence()
    values = {}
    for line in shown.stdout.splitlines():
        if line.count("=") != 1:
            return _invalid_unit_evidence()
        name, value = line.split("=", 1)
        if name not in _UNIT_PROPERTIES or name in values:
            return _invalid_unit_evidence()
        values[name] = value
    if set(values) != set(_UNIT_PROPERTIES):
        return _invalid_unit_evidence()
    if values["LoadState"] == "not-found":
        return _invalid_unit_evidence()
    if values["LoadState"] != "loaded" or values["Result"] not in _UNIT_RESULTS:
        return _invalid_unit_evidence()
    active_pair = (values["ActiveState"], values["SubState"])
    if active_pair not in {
        ("active", "running"), ("activating", "start"),
        ("active", "exited"), ("inactive", "dead"), ("failed", "failed"),
    }:
        return _invalid_unit_evidence()
    try:
        status = int(values["ExecMainStatus"])
        started_at = int(values["ExecMainStartTimestampMonotonic"])
    except ValueError:
        return _invalid_unit_evidence()
    if (status < 0 or status > 255 or started_at < 0
            or values["ExecMainCode"] not in _EXEC_CODES):
        return _invalid_unit_evidence()
    code = _EXEC_CODES[values["ExecMainCode"]]
    started = started_at > 0
    if not started:
        if (code or status
                or active_pair in {("active", "running"), ("activating", "start"),
                                   ("active", "exited")}
                or values["Result"] == "success"):
            return _invalid_unit_evidence()
        return {
            "valid": True, "termination_kind": "launcher_failure",
            "exit_status": None, "signal": None, "main_process_started": False,
        }
    if active_pair in {("active", "running"), ("activating", "start")}:
        if code or status or values["Result"] != "success":
            return _invalid_unit_evidence()
        if allow_running and not launcher_timed_out:
            return {
                "valid": True, "termination_kind": "running",
                "exit_status": None, "signal": None,
                "main_process_started": True,
            }
        if not launcher_timed_out:
            return _invalid_unit_evidence()
        return {
            "valid": True, "termination_kind": "outer_watchdog",
            "exit_status": None, "signal": None, "main_process_started": True,
        }
    result = values["Result"]
    if result == "timeout":
        if code not in {"killed", "dumped"} or not 1 <= status <= 64:
            return _invalid_unit_evidence()
        kind, exit_status, signal_number = "runtime_timeout", None, status or None
    elif result == "oom-kill":
        if code not in {"killed", "dumped"} or not 1 <= status <= 64:
            return _invalid_unit_evidence()
        kind, exit_status, signal_number = "oom_kill", None, status or None
    elif result in {"resources", "protocol"}:
        if code in {"killed", "dumped"} and 1 <= status <= 64:
            kind, exit_status, signal_number = "sandbox_restriction", None, status
        elif code == "exited" and status:
            kind, exit_status, signal_number = "sandbox_restriction", status, None
        else:
            return _invalid_unit_evidence()
    elif result in {"signal", "core-dump"} and code in {"killed", "dumped"} \
            and 1 <= status <= 64:
        kind, exit_status, signal_number = "external_signal", None, status or None
    elif result == "exit-code" and code == "exited" and status:
        kind, exit_status, signal_number = "nonzero_exit", status, None
    elif result == "success" and code == "exited" and status == 0:
        kind, exit_status, signal_number = "completed", 0, None
    else:
        return _invalid_unit_evidence()
    return {
        "valid": True, "termination_kind": kind,
        "exit_status": exit_status, "signal": signal_number,
        "main_process_started": True,
    }


def _failed_probe_results(detail):
    return {name: {"passed": False, "detail": detail} for name in PROBE_RESULTS}


def validate_probe_payload(raw, run_id):
    if not isinstance(raw, dict) or raw.get("schema_version") != PROBE_SCHEMA_VERSION or raw.get("run_id") != run_id:
        return _failed_probe_results("probe identity or schema mismatch"), "probe identity or schema mismatch"
    results = raw.get("results")
    if not isinstance(results, dict) or set(results) != set(PROBE_RESULTS):
        return _failed_probe_results("probe result set is incomplete or unexpected"), "probe result set is incomplete or unexpected"
    normalized = {}
    for name in PROBE_RESULTS:
        item = results[name]
        if not isinstance(item, dict) or set(item) != {"passed", "detail"}:
            return _failed_probe_results(f"malformed result: {name}"), f"malformed result: {name}"
        if not isinstance(item["passed"], bool) or not isinstance(item["detail"], str) or not item["detail"]:
            return _failed_probe_results(f"malformed result: {name}"), f"malformed result: {name}"
        normalized[name] = item
    actual_pass = all(item["passed"] for item in normalized.values())
    if raw.get("overall_pass") is not actual_pass:
        return _failed_probe_results("contradictory overall probe result"), "contradictory overall probe result"
    return normalized, None


def _load_probe_output(path, run_id):
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return _failed_probe_results(f"missing or malformed probe output: {exc}"), str(exc)
    return validate_probe_payload(raw, run_id)


def _render_text(report):
    lines = [
        "FieldStation42 isolation preflight",
        f"Run: {report['run_id']}",
        f"Result: {report['result']}",
        f"Verified outside Codex: {report['verified_outside_codex']}",
        f"Invocation context: {report['invocation_context_detail']}",
        f"Transient unit exit status 0: {report['transient_unit']['passed']}",
        f"Bubblewrap exit status 0: {report['bubblewrap']['passed']}",
        "",
        "Probe results:",
    ]
    for name in PROBE_RESULTS:
        item = report["probe_results"][name]
        lines.append(f"{'PASS' if item['passed'] else 'FAIL'} {name}: {item['detail']}")
    lines.extend(
        (
            "",
            f"Staging cleanup: {'PASS' if report['cleanup']['staging']['passed'] else 'FAIL'} - {report['cleanup']['staging']['detail']}",
            f"Unit cleanup: {'PASS' if report['cleanup']['unit']['passed'] else 'FAIL'} - {report['cleanup']['unit']['detail']}",
            f"Report JSON complete: {report['retained_reports']['json_complete']}",
            f"Report text complete: {report['retained_reports']['text_complete']}",
        )
    )
    return "\n".join(lines) + "\n"


def _unique_report_directory(project_root, run_id):
    parent = Path(project_root) / "runtime/director/isolation"
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / f"preflight-{run_id}"
    target.mkdir(exist_ok=False)
    return target


def create_staging_directory():
    for unused in range(20):
        token = uuid.uuid4().hex[:12]
        stage = STAGING_PARENT / _staging_name(token)
        try:
            stage.mkdir(mode=0o700)
            marker = (stage / ".active").open("x", encoding="utf-8")
            fcntl.flock(marker.fileno(), fcntl.LOCK_EX)
            return token, stage, marker
        except FileExistsError:
            continue
        except Exception:
            if stage.exists() and _safe_staging_directory(stage):
                shutil.rmtree(stage)
            raise
    raise IsolationError("could not allocate a unique staging directory")


def _create_staging_directory():
    return create_staging_directory()


def _write_reports(report_dir, report):
    json_path = report_dir / "preflight.json"
    text_path = report_dir / "preflight.txt"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    text_path.write_text(_render_text(report), encoding="utf-8")
    try:
        parsed = json.loads(json_path.read_text(encoding="utf-8"))
        json_complete = parsed.get("run_id") == report["run_id"] and set(parsed.get("probe_results", {})) == set(PROBE_RESULTS)
        text = text_path.read_text(encoding="utf-8")
        text_complete = all(name in text for name in PROBE_RESULTS) and f"Run: {report['run_id']}" in text
    except (OSError, UnicodeError, json.JSONDecodeError):
        json_complete = text_complete = False
    return json_path, text_path, json_complete, text_complete


def run_preflight(
    project_root,
    profile="standard",
    now=None,
):
    if profile not in ("standard", "native-single-run"):
        raise IsolationError(f"unknown isolation profile: {profile}")
    project_root = Path(project_root).resolve()
    run_id = _new_run_id()
    report_dir = _unique_report_directory(project_root, run_id)
    outside_codex, invocation_detail = check_invocation_context()
    stale_ok, stale_failures = True, []
    stage = None
    stage_lock = None
    unit_name = None
    launch = LaunchResult("", 127, "", "launcher did not run")
    probe_results = _failed_probe_results("probe did not run")
    probe_error = None
    stage_cleanup = (True, "not created")
    unit_cleanup = (True, "not created")
    try:
        if outside_codex:
            stale_ok, stale_failures = cleanup_stale_directories(now=now)
            try:
                token, stage, stage_lock = _create_staging_directory()
                unit_name = f"fs42-isolation-{token}.service"
                python = _sandbox_python(project_root)
                launcher = IsolationLauncher(project_root)
                probe_argv = [python, "/project/station_director/isolation_probe.py", run_id, f"/stage/{PROBE_OUTPUT}"]
                if profile == "native-single-run":
                    probe_argv.append(profile)
                launch = launcher.run(
                    stage,
                    probe_argv,
                    unit_name,
                    stage_tmp=profile == "native-single-run",
                )
                probe_results, probe_error = _load_probe_output(stage / PROBE_OUTPUT, run_id)
            except (IsolationError, OSError) as exc:
                probe_error = str(exc)
                probe_results = _failed_probe_results(f"launcher failure: {exc}")
        else:
            probe_error = invocation_detail
            probe_results = _failed_probe_results(
                f"host invocation context rejected: {invocation_detail}"
            )
    finally:
        if unit_name is not None:
            unit_cleanup = cleanup_unit(unit_name)
        if stage is not None and not unit_cleanup[0] and stage.exists():
            (stage / ".quarantine").touch(mode=0o600, exist_ok=True)
        if stage_lock is not None:
            stage_lock.close()
        if stage is not None and os.path.lexists(stage):
            stage_cleanup = cleanup_staging_directory(stage)

    unit_pass = launch.returncode == 0 and not launch.timed_out
    bwrap_pass = launch.returncode == 0 and not launch.timed_out
    report = {
        "schema_version": 1,
        "profile": profile,
        "run_id": run_id,
        "created_at": _utc_now().isoformat(),
        "result": "FAIL",
        "verified_outside_codex": outside_codex,
        "invocation_context_detail": invocation_detail,
        "transient_unit": {
            "name": unit_name,
            "exit_status": launch.returncode,
            "timed_out": launch.timed_out,
            "passed": unit_pass,
            "stderr": launch.stderr[-4000:],
        },
        "bubblewrap": {"exit_status": launch.returncode, "passed": bwrap_pass},
        "probe_results": probe_results,
        "probe_output_error": probe_error,
        "stale_cleanup": {"passed": stale_ok, "failures": stale_failures},
        "cleanup": {
            "staging": {"passed": stage_cleanup[0], "detail": stage_cleanup[1]},
            "unit": {"passed": unit_cleanup[0], "detail": unit_cleanup[1]},
        },
        "retained_reports": {
            "directory": str(report_dir.relative_to(project_root)),
            "json_complete": False,
            "text_complete": False,
        },
    }
    pre_report_pass = all(
        (
            unit_pass,
            bwrap_pass,
            outside_codex,
            all(item["passed"] for item in probe_results.values()),
            stale_ok,
            stage_cleanup[0],
            unit_cleanup[0],
        )
    )
    report["retained_reports"]["json_complete"] = True
    report["retained_reports"]["text_complete"] = True
    report["result"] = "PASS" if pre_report_pass else "FAIL"
    json_path, text_path, json_complete, text_complete = _write_reports(report_dir, report)
    report["retained_reports"]["json_complete"] = json_complete
    report["retained_reports"]["text_complete"] = text_complete
    if not (json_complete and text_complete):
        report["result"] = "FAIL"
        _write_reports(report_dir, report)
    return report, json_path, text_path
