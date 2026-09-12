import json
from pathlib import Path


def configured_tags(config_dir):
    assignments = {}
    errors = []
    for path in sorted(Path(config_dir).glob("*.json")):
        try:
            with path.open(encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: {exc}")
            continue
        conf = data.get("station_conf", {})
        station = conf.get("network_name")
        if not station:
            continue

        def visit(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key == "tags":
                        values = child if isinstance(child, list) else [child]
                        for tag in values:
                            if isinstance(tag, str):
                                assignments.setdefault(tag.casefold(), set()).add(station)
                    else:
                        visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(conf.get("day_templates", {}))
        visit(conf.get("date_overrides", {}))
        visit(conf.get("week_overrides", {}))
    return assignments, errors


def recommend_shows(show_names, policy, assignments):
    excluded = {name.casefold() for name in policy.get("inventory_excluded_roots", [])}
    eligible = [channel for channel in policy["channels"] if channel.get("accepts_recommendations")]
    results = []
    for show in sorted(show_names, key=str.casefold):
        if show.casefold() in excluded or show == "(media root)":
            continue
        existing = sorted(assignments.get(show.casefold(), []))
        if existing:
            results.append({"show": show, "status": "already_assigned", "channels": existing})
            continue

        words = show.casefold()
        scored = []
        for channel in eligible:
            matches = sorted({word for word in channel.get("keywords", []) if word.casefold() in words})
            scored.append((len(matches), channel["number"], channel, matches))
        scored.sort(key=lambda value: (-value[0], value[1]))
        score, _, winner, matches = scored[0]
        results.append({
            "show": show,
            "status": "advisory",
            "recommended_channel": winner["number"] if score else None,
            "recommended_identity": winner["name"] if score else None,
            "confidence": "low" if score == 1 else ("medium" if score > 1 else "unclassified"),
            "matched_keywords": matches,
            "note": "Manual review required; this recommendation cannot apply itself.",
        })
    return results
