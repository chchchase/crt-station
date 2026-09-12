import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mpg", ".mpeg", ".avi", ".mov", ".mkv", ".ts", ".m4v", ".webm", ".wmv"}
)
SNAPSHOT_VERSION = 1


class InventoryError(RuntimeError):
    pass


def nearest_mount(path):
    candidate = Path(path).resolve()
    while candidate != candidate.parent and not os.path.ismount(candidate):
        candidate = candidate.parent
    return candidate


def validate_media_mount(media_root, mount_checker=None):
    root = Path(media_root)
    if not root.is_dir():
        raise InventoryError(f"Media library is unavailable: {root}")
    if not os.access(root, os.R_OK | os.X_OK):
        raise InventoryError(f"Media library is unreadable: {root}")

    if mount_checker is not None:
        mounted = mount_checker(root)
        mount = root if mounted else Path("/")
    else:
        mount = nearest_mount(root)
        mounted = mount != Path("/")
    if not mounted:
        raise InventoryError(
            f"Media library is not backed by its expected external mount: {root}"
        )
    return mount


def _skip_reason(name, extension):
    if name.startswith("."):
        return "hidden"
    if extension not in VIDEO_EXTENSIONS:
        return "unsupported_extension"
    return None


def build_snapshot(media_root, mount_checker=None, now=None):
    root = Path(media_root).resolve()
    mount = validate_media_mount(root, mount_checker=mount_checker)
    files = []
    skipped = []
    errors = []
    shows = {}
    visited = set()

    def onerror(error):
        errors.append({"path": str(getattr(error, "filename", root)), "error": str(error)})

    for current, dirs, names in os.walk(root, followlinks=True, onerror=onerror):
        try:
            current_stat = os.stat(current)
            identity = (current_stat.st_dev, current_stat.st_ino)
            if identity in visited:
                dirs[:] = []
                continue
            visited.add(identity)
        except OSError as exc:
            errors.append({"path": str(current), "error": str(exc)})
            dirs[:] = []
            continue

        hidden_dirs = sorted(name for name in dirs if name.startswith("."))
        for name in hidden_dirs:
            skipped.append({"path": str((Path(current) / name).relative_to(root)), "reason": "hidden_directory"})
        dirs[:] = sorted(name for name in dirs if not name.startswith("."))

        for name in sorted(names):
            path = Path(current) / name
            relative = path.relative_to(root)
            reason = _skip_reason(name, path.suffix.lower())
            if reason:
                skipped.append({"path": str(relative), "reason": reason})
                continue
            try:
                stat = path.stat()
                if not os.access(path, os.R_OK):
                    raise PermissionError("not readable")
            except OSError as exc:
                errors.append({"path": str(relative), "error": str(exc)})
                continue

            record = {
                "path": str(relative),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
            files.append(record)
            show = relative.parts[0] if len(relative.parts) > 1 else "(media root)"
            summary = shows.setdefault(show, {"file_count": 0, "total_bytes": 0})
            summary["file_count"] += 1
            summary["total_bytes"] += stat.st_size

    generated = now or datetime.now(timezone.utc)
    return {
        "schema_version": SNAPSHOT_VERSION,
        "generated_at": generated.isoformat(),
        "media_root": str(root),
        "mount_point": str(mount),
        "shows": dict(sorted(shows.items())),
        "files": sorted(files, key=lambda item: item["path"].casefold()),
        "skipped": skipped,
        "errors": errors,
    }


def save_snapshot(snapshot, snapshot_dir):
    directory = Path(snapshot_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    target = directory / f"inventory-{stamp}-{uuid4().hex[:8]}.json"
    temporary = target.with_suffix(".tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, target)
    return target


def load_snapshots(snapshot_dir):
    snapshots = []
    for path in sorted(Path(snapshot_dir).glob("inventory-*.json")):
        try:
            with path.open(encoding="utf-8") as handle:
                value = json.load(handle)
            if value.get("schema_version") == SNAPSHOT_VERSION:
                snapshots.append((path, value))
        except (OSError, json.JSONDecodeError):
            continue
    return snapshots


def compare_snapshots(before, after):
    old_files = {item["path"]: item for item in before.get("files", [])}
    new_files = {item["path"]: item for item in after.get("files", [])}
    old_shows = set(before.get("shows", {}))
    new_shows = set(after.get("shows", {}))
    common = old_files.keys() & new_files.keys()
    old_skipped = {(item["path"], item["reason"]) for item in before.get("skipped", [])}
    new_skipped = {(item["path"], item["reason"]) for item in after.get("skipped", [])}
    modified = sorted(
        path for path in common
        if (old_files[path]["size"], old_files[path]["mtime_ns"])
        != (new_files[path]["size"], new_files[path]["mtime_ns"])
    )
    return {
        "from": before.get("generated_at"),
        "to": after.get("generated_at"),
        "new_shows": sorted(new_shows - old_shows, key=str.casefold),
        "missing_shows": sorted(old_shows - new_shows, key=str.casefold),
        "added_files": sorted(new_files.keys() - old_files.keys(), key=str.casefold),
        "missing_files": sorted(old_files.keys() - new_files.keys(), key=str.casefold),
        "modified_files": modified,
        "skipped_count": len(new_skipped),
        "newly_skipped": [
            {"path": path, "reason": reason}
            for path, reason in sorted(new_skipped - old_skipped, key=lambda item: item[0].casefold())
        ],
        "no_longer_skipped": [
            {"path": path, "reason": reason}
            for path, reason in sorted(old_skipped - new_skipped, key=lambda item: item[0].casefold())
        ],
        "errors": after.get("errors", []),
    }
