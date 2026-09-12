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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


MEDIA_ROOT = Path("/mnt/t7/CRT-Media")
STAGING_PARENT = Path("/tmp")
STAGING_RE = re.compile(r"fs42-i-[0-9a-f]{12}\Z")
STAGING_MAX_AGE_SECONDS = 6 * 60 * 60
UNIT_TIMEOUT_SECONDS = 30
CLEANUP_TIMEOUT_SECONDS = 10
PROBE_OUTPUT = "preflight-probe.json"
PROBE_RESULTS = (
    "environment_sanitized",
    "host_home_not_exposed",
    "host_run_not_exposed",
    "user_bus_not_exposed",
    "proc_private",
    "dev_private",
    "tmp_private",
    "project_mount_read_only",
    "media_mount_read_only",
    "staging_mount_writable",
    "ipv4_blocked",
    "ipv6_blocked",
    "loopback_4242_blocked",
    "af_unix_path_safe",
    "af_unix_round_trip",
    "staging_create",
    "staging_read",
    "staging_rename",
    "staging_delete",
)


class IsolationError(RuntimeError):
    pass


@dataclass
class LaunchResult:
    unit_name: str
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False


def _utc_now():
    return datetime.now(timezone.utc)


def _new_run_id():
    return f"{_utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:12]}"


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


def _run_cleanup_command(argv, runner=subprocess.run):
    try:
        completed = runner(argv, capture_output=True, text=True, timeout=CLEANUP_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    detail = (completed.stderr or completed.stdout).strip()
    return completed.returncode == 0 or _is_absent_unit_message(detail), detail


def cleanup_unit(unit_name, runner=subprocess.run):
    details = []
    for action in ("stop", "reset-failed"):
        ok, detail = _run_cleanup_command(
            ["systemctl", "--user", action, unit_name], runner=runner
        )
        details.append(f"{action}: {detail or ('ok' if ok else 'failed')}")
        if not ok:
            return False, "; ".join(details)
    try:
        shown = runner(
            ["systemctl", "--user", "show", "--property=LoadState", "--value", unit_name],
            capture_output=True,
            text=True,
            timeout=CLEANUP_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, "; ".join(details + [f"show: {exc}"])
    show_text = (shown.stderr or shown.stdout).strip()
    absent = shown.returncode != 0 and _is_absent_unit_message(show_text)
    absent = absent or (shown.returncode == 0 and shown.stdout.strip() == "not-found")
    details.append(f"show: {show_text or 'no output'}")
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
    return stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and info.st_uid == os.getuid()


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


def build_bwrap_command(project_root, staging_path, sandbox_argv):
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
    command.extend(
        (
            "--ro-bind", str(project_root), "/project",
            "--ro-bind", str(MEDIA_ROOT), "/media",
            "--bind", str(staging_path), "/stage",
            "--proc", "/proc",
            "--dev", "/dev",
            "--tmpfs", "/tmp",
            "--chdir", "/stage",
            "--setenv", "PATH", "/usr/bin:/bin",
            "--setenv", "HOME", "/nonexistent",
            "--setenv", "TMPDIR", "/tmp",
            "--setenv", "LANG", "C.UTF-8",
            "--setenv", "LC_ALL", "C.UTF-8",
            "--setenv", "PYTHONDONTWRITEBYTECODE", "1",
            "--setenv", "PYTHONPATH", "/project",
            "--",
            *sandbox_argv,
        )
    )
    return command


class IsolationLauncher:
    """The single Bubblewrap launcher intended for preflight and staged validation."""

    def __init__(self, project_root, runner=subprocess.run):
        self.project_root = Path(project_root).resolve()
        self.runner = runner

    def run(self, staging_path, sandbox_argv, unit_name, timeout=UNIT_TIMEOUT_SECONDS):
        bwrap = build_bwrap_command(self.project_root, staging_path, sandbox_argv)
        command = [
            "systemd-run",
            "--user",
            "--quiet",
            "--wait",
            "--collect",
            "--pipe",
            "--service-type=exec",
            f"--unit={unit_name}",
            "--property=RestrictAddressFamilies=AF_UNIX",
            f"--property=RuntimeMaxSec={max(1, timeout - 5)}s",
            "--",
            *bwrap,
        ]
        try:
            completed = self.runner(
                command, capture_output=True, text=True, timeout=timeout
            )
            return LaunchResult(
                unit_name, completed.returncode, completed.stdout, completed.stderr
            )
        except subprocess.TimeoutExpired as exc:
            return LaunchResult(
                unit_name,
                124,
                exc.stdout or "",
                exc.stderr or "",
                timed_out=True,
            )
        except OSError as exc:
            return LaunchResult(unit_name, 127, "", str(exc))


def _failed_probe_results(detail):
    return {name: {"passed": False, "detail": detail} for name in PROBE_RESULTS}


def _load_probe_output(path, run_id):
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return _failed_probe_results(f"missing or malformed probe output: {exc}"), str(exc)
    if not isinstance(raw, dict) or raw.get("schema_version") != 1 or raw.get("run_id") != run_id:
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


def _create_staging_directory():
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
    runner=subprocess.run,
    now=None,
    context_checker=check_invocation_context,
):
    project_root = Path(project_root).resolve()
    run_id = _new_run_id()
    report_dir = _unique_report_directory(project_root, run_id)
    outside_codex, invocation_detail = context_checker()
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
                launcher = IsolationLauncher(project_root, runner=runner)
                launch = launcher.run(
                    stage,
                    [python, "/project/station_director/isolation_probe.py", run_id, f"/stage/{PROBE_OUTPUT}"],
                    unit_name,
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
            unit_cleanup = cleanup_unit(unit_name, runner=runner)
        if stage_lock is not None:
            stage_lock.close()
        if stage is not None and os.path.lexists(stage):
            stage_cleanup = cleanup_staging_directory(stage)

    unit_pass = launch.returncode == 0 and not launch.timed_out
    bwrap_pass = launch.returncode == 0 and not launch.timed_out
    report = {
        "schema_version": 1,
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
