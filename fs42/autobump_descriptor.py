"""Pure structural contract for serialized AutoBump descriptors.

This module deliberately performs no URL parsing, I/O, hashing, logging, or
presentation work.  Callers receive only a classification; the opaque suffix
is never returned.
"""

AUTOBUMP_PATH_PREFIX = ":autobump:="
AUTOBUMP_CATALOG_TAG = ":autobump:"
AUTOBUMP_VALIDATION_PATH = AUTOBUMP_PATH_PREFIX + "validation"

DESCRIPTOR_NONE = "none"
DESCRIPTOR_SELECTED = "selected"
DESCRIPTOR_INVALID = "invalid"


def _marked_path(value):
    return isinstance(value, str) and value.startswith(AUTOBUMP_PATH_PREFIX)


def _has_opaque_suffix(value):
    return _marked_path(value) and len(value) > len(AUTOBUMP_PATH_PREFIX)


def _positive_number(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value > 0


def classify_plan_entry(entry, *, liquid_type, plan_size, content_missing):
    """Classify one already shape-validated native playback-plan entry."""
    path = entry["path"]
    if not _marked_path(path):
        return DESCRIPTOR_NONE
    if not _has_opaque_suffix(path):
        return DESCRIPTOR_INVALID
    skip = entry["skip"]
    if (entry["is_stream"] is not False or isinstance(skip, bool)
            or not isinstance(skip, (int, float)) or skip != 0
            or not _positive_number(entry["duration"])
            or entry["media_type"] != "video"):
        return DESCRIPTOR_INVALID
    if entry["content_type"] == "bump":
        return DESCRIPTOR_SELECTED
    if (entry["content_type"] == "feature" and liquid_type == "LiquidWebBlock"
            and plan_size == 1 and content_missing):
        return DESCRIPTOR_SELECTED
    return DESCRIPTOR_INVALID


def classify_catalog_entry(entry):
    """Classify one referenced native catalog row without exposing its path."""
    path_marked = _marked_path(entry["path"])
    realpath_marked = _marked_path(entry["realpath"])
    tag_marked = entry["tag"] == AUTOBUMP_CATALOG_TAG
    if not (path_marked or realpath_marked or tag_marked):
        return DESCRIPTOR_NONE
    if (not path_marked or not tag_marked or realpath_marked
            or entry["realpath"] is not None
            or not _has_opaque_suffix(entry["path"])
            or not _positive_number(entry["duration"])
            or entry["content_type"] != "feature"
            or entry["media_type"] != "video"):
        return DESCRIPTOR_INVALID
    return DESCRIPTOR_SELECTED
