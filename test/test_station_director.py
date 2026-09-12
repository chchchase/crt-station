import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from station_director.inventory import InventoryError, build_snapshot, compare_snapshots
from station_director.policy import load_policy
from station_director.readers import readonly_db_summary, watch_in_order_status
from station_director.recommend import configured_tags, recommend_shows


def touch(path, content=b"video"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class StationDirectorTests(unittest.TestCase):
    def test_inventory_uses_fs42_video_and_hidden_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            touch(root / "New Show" / "Season 1" / "Episode 01.MKV")
            touch(root / "New Show" / "notes.txt")
            touch(root / ".hidden" / "secret.mp4")
            touch(root / "New Show" / "._sidecar.mp4")
            snapshot = build_snapshot(
                root,
                mount_checker=lambda unused: True,
                now=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(
                [item["path"] for item in snapshot["files"]],
                ["New Show/Season 1/Episode 01.MKV"],
            )
            self.assertEqual(snapshot["shows"]["New Show"]["file_count"], 1)
            self.assertEqual(
                {item["reason"] for item in snapshot["skipped"]},
                {"hidden", "hidden_directory", "unsupported_extension"},
            )

    def test_unavailable_mount_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(InventoryError, "external mount"):
                build_snapshot(Path(directory), mount_checker=lambda unused: False)

    def test_changes_detect_new_show_and_files(self):
        before = {"generated_at": "one", "shows": {"A": {}}, "files": [{"path": "A/1.mp4", "size": 1, "mtime_ns": 1}]}
        after = {"generated_at": "two", "shows": {"A": {}, "B": {}}, "files": [
            {"path": "A/1.mp4", "size": 2, "mtime_ns": 2},
            {"path": "B/1.mp4", "size": 1, "mtime_ns": 1},
        ]}
        changes = compare_snapshots(before, after)
        self.assertEqual(changes["new_shows"], ["B"])
        self.assertEqual(changes["added_files"], ["B/1.mp4"])
        self.assertEqual(changes["modified_files"], ["A/1.mp4"])
        self.assertEqual(changes["missing_files"], [])
        self.assertEqual(changes["skipped_count"], 0)
        self.assertEqual(changes["newly_skipped"], [])

    def test_policy_is_canonical_and_special_channels_are_ineligible(self):
        policy = load_policy()
        self.assertEqual([(c["number"], c["name"]) for c in policy["channels"]], [
            (1, "CRT Station Guide"), (2, "Action"), (3, "After School"), (4, "Anime"),
            (5, "Cartoon Network"), (6, "Disney"), (7, "Late Night"), (8, "Watch In Order"),
        ])
        self.assertFalse(policy["channels"][0]["accepts_recommendations"])
        self.assertFalse(policy["channels"][7]["accepts_recommendations"])

    def test_recommendations_are_advisory_and_never_use_channels_one_or_eight(self):
        policy = load_policy()
        results = recommend_shows(["New Batman Adventures", "Unknown Program"], policy, {})
        batman = next(item for item in results if item["show"] == "New Batman Adventures")
        unknown = next(item for item in results if item["show"] == "Unknown Program")
        self.assertEqual(batman["recommended_channel"], 2)
        self.assertIn("Manual review required", batman["note"])
        self.assertIsNone(unknown["recommended_channel"])

    def test_configured_tags_only_reads_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "channel.json"
            config.write_text(json.dumps({"station_conf": {"network_name": "Action", "day_templates": {"daily": {"0": {"tags": "Show"}}}}}))
            before = config.read_bytes()
            assignments, errors = configured_tags(root)
            self.assertEqual(assignments["show"], {"Action"})
            self.assertEqual(errors, [])
            self.assertEqual(config.read_bytes(), before)

    def test_database_reader_opens_existing_database_without_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "station.db"
            with sqlite3.connect(db) as connection:
                connection.execute("CREATE TABLE catalog_entries (id INTEGER)")
                connection.execute("INSERT INTO catalog_entries VALUES (1)")
                connection.execute("CREATE TABLE liquid_blocks (station TEXT, title TEXT, start_time TEXT, end_time TEXT)")
                connection.execute("INSERT INTO liquid_blocks VALUES ('Action', 'Show', '2000-01-01', '2999-01-01')")
            before = db.read_bytes()
            summary = readonly_db_summary(db, ["Action"])
            self.assertEqual(summary["catalog_entry_count"], 1)
            self.assertEqual(summary["channels"][0]["current"]["title"], "Show")
            self.assertEqual(db.read_bytes(), before)

    def test_wio_status_preserves_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tools").mkdir()
            state = root / "watch_in_order_state.json"
            state.write_text('{"position": 3}\n')
            (root / "tools" / "wio.py").write_text(
                "from pathlib import Path\n"
                "import sys\n"
                "assert sys.argv[1] == 'status'\n"
                "print(Path('watch_in_order_state.json').read_text().strip())\n"
            )
            before = state.read_bytes()
            output = watch_in_order_status(root)
            self.assertEqual(output, '{"position": 3}')
            self.assertEqual(state.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
