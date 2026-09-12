#!/usr/bin/env python3
import json
import random
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fs42.catalog import ShowCatalog
from fs42.liquid_schedule import LiquidSchedule
from fs42.station_manager import StationManager


def main():
    instructions = json.loads(Path(sys.argv[1]).read_text())
    random.seed(instructions["seed"])
    manager = StationManager()
    db_path = Path(manager.server_conf["db_path"])
    affected = instructions["affected"]
    with sqlite3.connect(db_path) as connection:
        for name in affected:
            connection.execute("DELETE FROM liquid_blocks WHERE station=?", (name,))
            connection.execute("DELETE FROM catalog_entries WHERE station=?", (name,))
        connection.commit()
    target_end = instructions["week_end"]
    for name in affected:
        conf = manager.station_by_name(name)
        if not conf or not conf.get("_has_schedule"):
            raise RuntimeError(f"Cannot stage scheduled channel: {name}")
        ShowCatalog(conf, rebuild_catalog=True, force=True, skip_chapter_scan=True)
        schedule = LiquidSchedule(conf)
        for unused in range(60):
            end = schedule._end_time()
            if end and end.isoformat() >= target_end:
                break
            schedule.add_week()
        else:
            raise RuntimeError(f"Could not reach target boundary for {name}")


if __name__ == "__main__":
    main()
