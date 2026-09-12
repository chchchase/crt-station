import json
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


def api_get(base_url, endpoint, timeout=3):
    with urlopen(f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}", timeout=timeout) as response:
        return json.load(response)


def service_status(unit="fs42.service"):
    result = subprocess.run(
        ["systemctl", "--user", "show", unit, "--property=ActiveState,SubState,MainPID"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    values = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    if result.returncode:
        values["error"] = result.stderr.strip() or f"systemctl exited {result.returncode}"
    return values


def readonly_db_summary(db_path, channel_names):
    uri = f"file:{Path(db_path).resolve()}?mode=ro&immutable=1"
    now = datetime.now().isoformat(sep=" ")
    result = {"catalog_entry_count": 0, "schedule_block_count": 0, "channels": []}
    with sqlite3.connect(uri, uri=True) as connection:
        connection.execute("PRAGMA query_only = ON")
        result["catalog_entry_count"] = connection.execute("SELECT COUNT(*) FROM catalog_entries").fetchone()[0]
        result["schedule_block_count"] = connection.execute("SELECT COUNT(*) FROM liquid_blocks").fetchone()[0]
        for channel in channel_names:
            current = connection.execute(
                "SELECT title, start_time, end_time FROM liquid_blocks "
                "WHERE station=? AND start_time<=? AND end_time>? ORDER BY start_time LIMIT 1",
                (channel, now, now),
            ).fetchone()
            final = connection.execute(
                "SELECT MAX(end_time) FROM liquid_blocks WHERE station=?", (channel,)
            ).fetchone()[0]
            result["channels"].append({
                "name": channel,
                "current": ({"title": current[0], "start": current[1], "end": current[2]} if current else None),
                "final_scheduled": final,
            })
        if connection.total_changes:
            raise RuntimeError("Read-only database inspection unexpectedly reported a change")
    return result


def station_status(root, policy, base_url="http://127.0.0.1:4242"):
    errors = []
    service = service_status()
    if service.get("error"):
        errors.append(f"Service status unavailable: {service['error']}")
    elif service.get("ActiveState") != "active" or service.get("SubState") != "running":
        errors.append(
            f"fs42.service is {service.get('ActiveState', 'unknown')}/"
            f"{service.get('SubState', 'unknown')}"
        )
    player = None
    api_summary = None
    try:
        player = api_get(base_url, "/player/status")
        api_summary = api_get(base_url, "/summary/")
        if player.get("error"):
            errors.append(f"Player status error: {player['error']}")
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"FieldStation42 API unavailable: {exc}")

    channel_names = [item["name"] for item in policy["channels"] if item.get("has_schedule", True)]
    try:
        database = readonly_db_summary(Path(root) / "runtime" / "fs42_fluid.db", channel_names)
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        database = None
        errors.append(f"Database status unavailable: {exc}")

    media_path = None
    if player and player.get("file_path"):
        media_path = Path(player["file_path"])
        if not media_path.is_absolute():
            media_path = Path(root) / media_path
        if not media_path.is_file() or not os.access(media_path, os.R_OK):
            errors.append(f"Current media path is missing or unreadable: {media_path}")

    expected = {(item["number"], item["name"]) for item in policy["channels"]}
    if api_summary:
        actual = {
            (item.get("channel_number"), item.get("network_name"))
            for item in api_summary.get("summary_data", [])
        }
        if actual != expected:
            errors.append("Live API channel lineup does not match the Director identity policy")

    return {
        "checked_at": datetime.now().astimezone().isoformat(),
        "service": service,
        "player": player,
        "api_summary": api_summary,
        "database": database,
        "errors": errors,
    }


def watch_in_order_status(root):
    root = Path(root)
    python = root / "env" / "bin" / "python3"
    if not python.exists():
        python = Path(os.sys.executable)
    result = subprocess.run(
        [str(python), str(root / "tools" / "wio.py"), "status"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"wio status exited {result.returncode}")
    return result.stdout.rstrip()
