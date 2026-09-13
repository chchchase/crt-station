"""Pure transformations used by the HTTP guide and isolated validation."""


def build_channels_payload(stations):
    channels = [
        {
            "network_name": station["network_name"],
            "network_long_name": station.get("network_long_name", ""),
            "channel_number": station["channel_number"],
            "hidden": station.get("hidden", False),
            "has_schedule": station.get("_has_schedule", False),
        }
        for station in stations
    ]
    return {"channels": channels}


def _listing_projection(blocks, include_meta):
    return list(iter_listing_projection(blocks, include_meta))


def iter_listing_projection(blocks, include_meta):
    """Yield the same guide listings used by the materialized HTTP payload."""
    for block in blocks:
        listing = {
            "title": block.title,
            "start_time": block.start_time.isoformat(),
            "end_time": block.end_time.isoformat(),
        }
        if include_meta:
            meta = getattr(block, "meta", None)
            if meta:
                listing["meta"] = meta
        yield listing


def build_all_schedules_payload(start, end, by_station, include_meta=False):
    schedules = {
        name: _listing_projection(blocks, include_meta)
        for name, blocks in by_station.items()
    }
    return {"start": start, "end": end, "schedules": schedules}
