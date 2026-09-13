"""Native configuration checks; import only after worker attestation."""

import copy
from datetime import datetime, timedelta

from fs42.config_processor import ConfigProcessor
from fs42.slot_reader import SlotReader


def _slot_has_tags(slot):
    if not isinstance(slot, dict):
        return False
    tags = slot.get("tags")
    if isinstance(tags, str):
        return bool(tags)
    return isinstance(tags, list) and bool(tags) and all(
        isinstance(tag, str) and tag for tag in tags
    )


def _resolved_week_slots(configs, channel_names, proposal):
    start = datetime.fromisoformat(proposal["week_start"])
    resolved = {}
    errors = []
    for name in sorted(channel_names):
        data = configs.get(name)
        if not data:
            errors.append(
                f"Cannot resolve source slots for missing channel configuration: {name}"
            )
            continue
        try:
            conf = ConfigProcessor.preprocess(copy.deepcopy(data["station_conf"]))
            conf = SlotReader.smooth_tags(conf)
            states = {}
            for offset in range(7):
                current_date = (start + timedelta(days=offset)).date()
                for hour in range(24):
                    when = datetime.combine(current_date, datetime.min.time()).replace(
                        hour=hour
                    )
                    slot, unused_slot_number = SlotReader.get_slot(conf, when)
                    states[f"{current_date.isoformat()}T{hour:02d}:00"] = _slot_has_tags(slot)
            resolved[name] = states
        except Exception as exc:
            errors.append(f"Could not resolve projected slots for {name}: {exc}")
    return resolved, errors


def newly_unresolved_source_slots(original, projected, source_channels, proposal):
    if not source_channels:
        return [], []
    before, before_errors = _resolved_week_slots(original, source_channels, proposal)
    after, after_errors = _resolved_week_slots(projected, source_channels, proposal)
    failures = []
    for name in sorted(source_channels):
        lost = [
            timestamp
            for timestamp, was_tagged in before.get(name, {}).items()
            if was_tagged and not after.get(name, {}).get(timestamp, False)
        ]
        if lost:
            sample = ", ".join(lost[:8])
            remainder = f" and {len(lost) - 8} more" if len(lost) > 8 else ""
            failures.append(
                f"{name} has {len(lost)} newly unresolved or tagless source slot(s): "
                f"{sample}{remainder}"
            )
    return failures, before_errors + after_errors


def validate_processed_configurations(configs, schema):
    import jsonschema

    failures = []
    for name in sorted(configs):
        try:
            jsonschema.validate(configs[name], schema)
            processed = ConfigProcessor.preprocess(
                copy.deepcopy(configs[name]["station_conf"])
            )
            if processed.get("network_type", "standard") == "standard":
                SlotReader.smooth_tags(processed)
        except Exception as exc:
            failures.append(f"FieldStation42 configuration processing failed for {name}: {exc}")
    return failures
