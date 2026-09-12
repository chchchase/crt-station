#!/usr/bin/env python3
import errno
import json
import os
import socket
import sys
from pathlib import Path


SOCKET_TIMEOUT_SECONDS = 1.0
NETWORK_TIMEOUT_SECONDS = 1.0
DENIED_ERRNOS = {errno.EACCES, errno.EPERM, errno.EAFNOSUPPORT}
EXPECTED_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/nonexistent",
    "PWD": "/stage",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONPATH": "/project",
}


def result(passed, detail):
    return {"passed": bool(passed), "detail": str(detail)}


def check_environment(environment=None):
    environment = dict(os.environ if environment is None else environment)
    expected_keys = set(EXPECTED_ENVIRONMENT)
    actual_keys = set(environment)
    unexpected = sorted(actual_keys - expected_keys)
    missing = sorted(expected_keys - actual_keys)
    incorrect = sorted(
        key
        for key in expected_keys & actual_keys
        if environment[key] != EXPECTED_ENVIRONMENT[key]
    )
    allowlisted_values = {
        key: environment[key]
        for key in sorted(expected_keys & actual_keys)
    }
    detail = (
        f"allowlisted values={json.dumps(allowlisted_values, sort_keys=True)}; "
        f"unexpected keys={json.dumps(unexpected)}; "
        f"missing keys={json.dumps(missing)}; "
        f"incorrect values={json.dumps(incorrect)}"
    )
    return result(not unexpected and not missing and not incorrect, detail)


def mount_info(target):
    for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
        left, right = line.split(" - ", 1)
        fields = left.split()
        if fields[4] != target:
            continue
        return set(fields[5].split(",")), right.split()[0]
    raise RuntimeError(f"mountpoint not found: {target}")


def check_mount(target, expected_flag, expected_type=None):
    try:
        flags, fs_type = mount_info(target)
        passed = expected_flag in flags and (expected_type is None or fs_type == expected_type)
        return result(passed, f"flags={','.join(sorted(flags))}; type={fs_type}")
    except Exception as exc:
        return result(False, exc)


def check_family_blocked(family):
    sock = None
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(NETWORK_TIMEOUT_SECONDS)
        return result(False, "socket creation succeeded")
    except OSError as exc:
        return result(exc.errno in DENIED_ERRNOS, f"socket denied with errno {exc.errno}: {exc}")
    finally:
        if sock is not None:
            sock.close()


def check_loopback_blocked():
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(NETWORK_TIMEOUT_SECONDS)
        code = sock.connect_ex(("127.0.0.1", 4242))
        return result(code in DENIED_ERRNOS, f"connect_ex returned errno {code}")
    except OSError as exc:
        return result(exc.errno in DENIED_ERRNOS, f"loopback denied with errno {exc.errno}: {exc}")
    finally:
        if sock is not None:
            sock.close()


def check_unix_socket():
    path = Path("/stage/u.sock")
    encoded_length = len(os.fsencode(path))
    path_result = result(encoded_length < 100, f"socket path is {encoded_length} bytes")
    server = client = accepted = None
    try:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.settimeout(SOCKET_TIMEOUT_SECONDS)
        server.bind(str(path))
        server.listen(1)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(SOCKET_TIMEOUT_SECONDS)
        client.connect(str(path))
        accepted, unused = server.accept()
        accepted.settimeout(SOCKET_TIMEOUT_SECONDS)
        client.sendall(b"ping")
        payload = accepted.recv(4)
        accepted.sendall(b"pong")
        reply = client.recv(4)
        socket_result = result(payload == b"ping" and reply == b"pong", "AF_UNIX round trip completed")
    except Exception as exc:
        socket_result = result(False, exc)
    finally:
        for sock in (accepted, client, server):
            if sock is not None:
                sock.close()
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    return path_result, socket_result


def staging_operations():
    created = Path("/stage/create.txt")
    renamed = Path("/stage/renamed.txt")
    results = {}
    try:
        with created.open("x", encoding="utf-8") as handle:
            handle.write("isolation-preflight\n")
        results["staging_create"] = result(True, "created /stage/create.txt")
    except Exception as exc:
        results["staging_create"] = result(False, exc)
    try:
        contents = created.read_text(encoding="utf-8")
        results["staging_read"] = result(contents == "isolation-preflight\n", "read staged content")
    except Exception as exc:
        results["staging_read"] = result(False, exc)
    try:
        created.rename(renamed)
        results["staging_rename"] = result(renamed.is_file() and not created.exists(), "renamed staged file")
    except Exception as exc:
        results["staging_rename"] = result(False, exc)
    try:
        renamed.unlink()
        results["staging_delete"] = result(not renamed.exists(), "deleted staged file")
    except Exception as exc:
        results["staging_delete"] = result(False, exc)
    for path in (created, renamed):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    return results


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        return 2
    run_id, output_name = argv
    output = Path(output_name)
    results = {}
    results["environment_sanitized"] = check_environment()
    results["host_home_not_exposed"] = result(not Path("/home").exists(), "/home is absent")
    results["host_run_not_exposed"] = result(not Path("/run").exists(), "/run is absent")
    bus_exposed = "DBUS_SESSION_BUS_ADDRESS" in os.environ or Path(f"/run/user/{os.getuid()}/bus").exists()
    results["user_bus_not_exposed"] = result(not bus_exposed, "host user bus is absent")
    results["proc_private"] = check_mount("/proc", "rw", "proc")
    results["dev_private"] = check_mount("/dev", "rw", "tmpfs")
    results["tmp_private"] = check_mount("/tmp", "rw", "tmpfs")
    results["project_mount_read_only"] = check_mount("/project", "ro")
    results["media_mount_read_only"] = check_mount("/media", "ro")
    results["staging_mount_writable"] = check_mount("/stage", "rw")
    results["ipv4_blocked"] = check_family_blocked(socket.AF_INET)
    results["ipv6_blocked"] = check_family_blocked(socket.AF_INET6)
    results["loopback_4242_blocked"] = check_loopback_blocked()
    results["af_unix_path_safe"], results["af_unix_round_trip"] = check_unix_socket()
    results.update(staging_operations())
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "overall_pass": all(item["passed"] for item in results.values()),
        "results": results,
    }
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if payload["overall_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
