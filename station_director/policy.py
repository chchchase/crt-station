import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "director_conf" / "channel_identities.v2.json"


def load_policy(path=DEFAULT_POLICY):
    with Path(path).open(encoding="utf-8") as handle:
        policy = json.load(handle)
    return validate_policy_document(policy)


def validate_policy_document(policy):
    """Validate an already-loaded policy without performing file access."""
    if not isinstance(policy, dict):
        raise ValueError("Channel identity policy must be an object")
    if policy.get("schema_version") not in (1, 2):
        raise ValueError("Unsupported channel identity policy version")

    channels = policy.get("channels", [])
    expected = [
        (1, "CRT Station Guide"),
        (2, "Action"),
        (3, "After School"),
        (4, "Anime"),
        (5, "Cartoon Network"),
        (6, "Disney"),
        (7, "Late Night"),
        (8, "Watch In Order"),
    ]
    actual = [(item.get("number"), item.get("name")) for item in channels]
    if actual != expected:
        raise ValueError("Channel identity policy does not match canonical Channels 1-8")
    return policy
