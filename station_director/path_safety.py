import copy
import os
import re
import stat
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath


LIVE_MEDIA_ROOT = PurePosixPath("/mnt/t7/CRT-Media")
PROJECT_MEDIA_LINK = PurePosixPath("catalog/crt_media")
SANDBOX_MEDIA_ROOT = PurePosixPath("/media")

MEDIA_DIRECTORY_KEYS = {"content_dir", "bump_dir", "commercial_dir"}
MEDIA_FILE_KEYS = {
    "standby_image",
    "be_right_back_media",
    "sign_off_video",
    "off_air_video",
    "off_air_image",
    "start_bump",
    "end_bump",
    "bg_music",
    "bg_video",
}
MEDIA_LIST_KEYS = {"images", "sound_to_play"}
STAGE_PATH_KEYS = {"runtime_dir", "catalog_path", "schedule_path"}
KNOWN_NON_PATH_KEYS = {
    "media_filter",
    "video_keepaspect",
    "video_scramble_fx",
    "bg_video_loop_count",
    "bg_video_audio",
    "web_url",
    "executable_command",
    "executable_shutdown",
}
PATH_LIKE_KEY = re.compile(r"(?:dir|path|video|image|media|sound)", re.IGNORECASE)


class PathSafetyError(ValueError):
    pass


@dataclass(frozen=True)
class MediaPathMapping:
    logical_identity: str
    canonical_host_path: str
    sandbox_path: str
    sandbox_only: bool

    def as_dict(self):
        return asdict(self)


def _path_text(value, description):
    if not isinstance(value, str) or not value:
        raise PathSafetyError(f"{description} must be a non-empty string")
    if "\0" in value or "\\" in value or any(ord(character) < 32 for character in value):
        raise PathSafetyError(f"{description} contains an unsafe character")
    return value


def _reject_ambiguous_parts(path, description):
    if any(part in ("", ".", "..") for part in path.parts):
        raise PathSafetyError(f"{description} contains ambiguous traversal components")


def _logical_relative_path(value, description):
    raw = _path_text(value, description)
    source = PurePosixPath(raw)
    _reject_ambiguous_parts(source, description)
    if source.is_absolute():
        if source == SANDBOX_MEDIA_ROOT or SANDBOX_MEDIA_ROOT in source.parents:
            raise PathSafetyError(
                f"{description} uses sandbox-only /media as though it were a live path"
            )
        try:
            return source.relative_to(LIVE_MEDIA_ROOT)
        except ValueError as exc:
            raise PathSafetyError(
                f"{description} is outside the approved live media root"
            ) from exc
    try:
        return source.relative_to(PROJECT_MEDIA_LINK)
    except ValueError as exc:
        raise PathSafetyError(
            f"{description} is not rooted at catalog/crt_media"
        ) from exc


def canonical_media_mapping(value, description="media path", *, allow_sandbox=False):
    """Return stable live/sandbox identities without claiming /media is live-safe."""
    raw = _path_text(value, description)
    source = PurePosixPath(raw)
    _reject_ambiguous_parts(source, description)
    if allow_sandbox and source.is_absolute():
        try:
            relative = source.relative_to(SANDBOX_MEDIA_ROOT)
        except ValueError:
            relative = _logical_relative_path(raw, description)
    else:
        relative = _logical_relative_path(raw, description)
    suffix = "" if relative.as_posix() == "." else relative.as_posix()
    parts = () if not suffix else relative.parts
    return MediaPathMapping(
        "crt-media:/" + suffix,
        str(Path(str(LIVE_MEDIA_ROOT)).joinpath(*parts)),
        str(Path("/media").joinpath(*parts)),
        True,
    )


def _resolved_within(candidate, root, description):
    try:
        resolved_root = Path(root).resolve(strict=True)
        resolved = Path(candidate).resolve(strict=True)
        resolved.relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError) as exc:
        raise PathSafetyError(
            f"{description} does not resolve safely beneath {root}: {exc}"
        ) from exc
    return resolved


def map_media_path(value, description, *, sandbox_media_root=Path("/media"), expected="any"):
    relative = _logical_relative_path(value, description)
    sandbox_root = Path(sandbox_media_root)
    sandbox_candidate = sandbox_root.joinpath(*relative.parts)
    resolved = _resolved_within(sandbox_candidate, sandbox_root, description)
    try:
        info = resolved.stat()
    except OSError as exc:
        raise PathSafetyError(f"{description} cannot be inspected: {exc}") from exc
    if expected == "directory" and not stat.S_ISDIR(info.st_mode):
        raise PathSafetyError(f"{description} is not a directory")
    if expected == "file" and not stat.S_ISREG(info.st_mode):
        raise PathSafetyError(f"{description} is not a regular file")
    if expected == "file" and not os.access(resolved, os.R_OK):
        raise PathSafetyError(f"{description} is not readable")
    identity = canonical_media_mapping(value, description)
    return MediaPathMapping(
        identity.logical_identity,
        identity.canonical_host_path,
        str(sandbox_candidate),
        True,
    )


def validate_scheduled_media(value, *, sandbox_media_root=Path("/media")):
    description = "scheduled media path"
    raw = _path_text(value, description)
    source = PurePosixPath(raw)
    _reject_ambiguous_parts(source, description)
    try:
        relative = source.relative_to(SANDBOX_MEDIA_ROOT)
    except ValueError as exc:
        raise PathSafetyError("scheduled media path is not rooted at sandbox-only /media") from exc
    root = Path(sandbox_media_root)
    resolved = _resolved_within(root.joinpath(*relative.parts), root, description)
    info = resolved.stat()
    if not stat.S_ISREG(info.st_mode):
        raise PathSafetyError("scheduled media path is not a regular file")
    if not os.access(resolved, os.R_OK):
        raise PathSafetyError("scheduled media path is not readable")
    return canonical_media_mapping(raw, description, allow_sandbox=True)


def _map_relative_media(value, base_mapping, description, sandbox_media_root, expected):
    raw = _path_text(value, description)
    candidate = PurePosixPath(raw)
    _reject_ambiguous_parts(candidate, description)
    if candidate.is_absolute() or candidate == PROJECT_MEDIA_LINK or PROJECT_MEDIA_LINK in candidate.parents:
        return map_media_path(
            raw,
            description,
            sandbox_media_root=sandbox_media_root,
            expected=expected,
        )
    base = PurePosixPath(base_mapping.canonical_host_path)
    return map_media_path(
        str(base / candidate),
        description,
        sandbox_media_root=sandbox_media_root,
        expected=expected,
    )


def _map_stage_path(value, description, stage_root):
    raw = _path_text(value, description)
    source = PurePosixPath(raw)
    _reject_ambiguous_parts(source, description)
    if source.is_absolute():
        try:
            relative = source.relative_to(PurePosixPath("/stage"))
        except ValueError as exc:
            raise PathSafetyError(f"{description} is outside /stage") from exc
    else:
        relative = source
    root = Path(stage_root).resolve(strict=True)
    candidate = root.joinpath(*relative.parts).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise PathSafetyError(f"{description} escapes /stage") from exc
    return str(Path("/stage").joinpath(*relative.parts))


def map_station_config(data, config_name, *, sandbox_media_root=Path("/media"), stage_root=Path("/stage")):
    """Return an in-memory sandbox config and auditable canonical path mappings."""
    mapped = copy.deepcopy(data)
    station_conf = mapped.get("station_conf")
    if not isinstance(station_conf, dict):
        raise PathSafetyError(f"{config_name} has no station_conf object")
    mappings = []
    content_mapping = None
    if "content_dir" in station_conf:
        content_mapping = map_media_path(
            station_conf["content_dir"],
            f"{config_name}.station_conf.content_dir",
            sandbox_media_root=sandbox_media_root,
            expected="directory",
        )

    def visit(value, location):
        nonlocal content_mapping
        if isinstance(value, dict):
            for key, child in list(value.items()):
                field = f"{location}.{key}"
                if key in MEDIA_DIRECTORY_KEYS:
                    mapping = map_media_path(
                        child, field, sandbox_media_root=sandbox_media_root, expected="directory"
                    )
                    value[key] = mapping.sandbox_path
                    mappings.append(mapping)
                    if key == "content_dir" and location.endswith("station_conf"):
                        content_mapping = mapping
                elif key == "logo_dir":
                    if content_mapping is None:
                        raise PathSafetyError(f"{field} requires a confined content_dir")
                    mapping = _map_relative_media(
                        child, content_mapping, field, sandbox_media_root, "directory"
                    )
                    value[key] = mapping.sandbox_path
                    mappings.append(mapping)
                elif key in MEDIA_FILE_KEYS:
                    if isinstance(child, str) and "://" in child:
                        raise PathSafetyError(f"{field} is a URL and cannot be verified as local media")
                    mapping = map_media_path(
                        child, field, sandbox_media_root=sandbox_media_root, expected="file"
                    )
                    value[key] = mapping.sandbox_path
                    mappings.append(mapping)
                elif key in MEDIA_LIST_KEYS:
                    items = child if isinstance(child, list) else [child]
                    converted = []
                    for index, item in enumerate(items):
                        mapping = map_media_path(
                            item,
                            f"{field}[{index}]",
                            sandbox_media_root=sandbox_media_root,
                            expected="file",
                        )
                        converted.append(mapping.sandbox_path)
                        mappings.append(mapping)
                    value[key] = converted if isinstance(child, list) else converted[0]
                elif key in STAGE_PATH_KEYS:
                    value[key] = _map_stage_path(child, field, stage_root)
                elif key == "default_logo":
                    if not isinstance(child, str) or PurePosixPath(child).name != child:
                        raise PathSafetyError(f"{field} must be a single filename")
                elif key in KNOWN_NON_PATH_KEYS:
                    continue
                elif PATH_LIKE_KEY.search(key) and isinstance(child, (str, list)):
                    raise PathSafetyError(f"{field} is an unclassified path-bearing field")
                else:
                    visit(child, field)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{location}[{index}]")

    visit(station_conf, f"{config_name}.station_conf")
    return mapped, mappings
