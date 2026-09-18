import argparse
import json
import os
import sys
from pathlib import Path

from station_director.inventory import (
    InventoryError,
    build_snapshot,
    compare_snapshots,
    load_snapshots,
    save_snapshot,
    validate_media_mount,
)
from station_director.isolation import run_preflight
from station_director.policy import DEFAULT_POLICY, ROOT, load_policy
from station_director.proposals import (
    ProposalError, archive_proposal, create_proposal, load_proposal, proposal_dirs,
)
from station_director.readers import station_status, watch_in_order_status
from station_director.recommend import configured_tags, recommend_shows
from station_director import validation_control


MEDIA_ROOT = Path("/mnt/t7/CRT-Media")
SNAPSHOT_DIR = ROOT / "runtime" / "director" / "inventory"


def print_json(value):
    print(json.dumps(value, indent=2, sort_keys=True))


def build_parser():
    parser = argparse.ArgumentParser(description="Read-only CRT Station Director")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY, help=argparse.SUPPRESS)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("status", help="Report station health and current programming")

    inventory = subcommands.add_parser("inventory", help="Inspect media inventory")
    inventory_commands = inventory.add_subparsers(dest="inventory_command", required=True)
    inventory_commands.add_parser("scan", help="Write a read-only media inventory snapshot")
    inventory_commands.add_parser("changes", help="Compare the two latest inventory snapshots")

    recommend = subcommands.add_parser("recommend", help="Recommend identities for unassigned shows")
    recommend.add_argument("--all", action="store_true", help="Include already assigned shows")

    wio = subcommands.add_parser("wio", help="Inspect Watch In Order")
    wio_commands = wio.add_subparsers(dest="wio_command", required=True)
    wio_commands.add_parser("status", help="Show current Watch In Order batches")

    isolation = subcommands.add_parser("isolation", help="Check the schedule isolation boundary")
    isolation_commands = isolation.add_subparsers(dest="isolation_command", required=True)
    preflight = isolation_commands.add_parser("preflight", help="Exercise the shared Bubblewrap launcher")
    preflight.add_argument(
        "--profile", choices=("standard", "native-single-run"), default="standard",
        help="Isolation mount profile to attest",
    )

    schedule = subcommands.add_parser("schedule", help="Plan and validate schedule proposals")
    schedule_commands = schedule.add_subparsers(dest="schedule_command", required=True)
    plan = schedule_commands.add_parser("plan", help="Create a declarative dry-run proposal")
    plan.add_argument("--week", required=True)
    plan.add_argument("--source", choices=("direct", "recommendation"), default="direct")
    plan.add_argument("--seed", type=int, default=4242)
    plan.add_argument("--assign", nargs=2, action="append", metavar=("SERIES", "CHANNEL"), default=[])
    plan.add_argument("--move", nargs=2, action="append", metavar=("SERIES", "CHANNEL"), default=[])
    plan.add_argument("--remove", action="append", metavar="SERIES", default=[])
    plan.add_argument("--date-slot", nargs=4, action="append", metavar=("CHANNEL", "DATE", "HOUR", "SERIES"), default=[])
    plan.add_argument("--daypart", nargs=3, action="append", metavar=("CHANNEL", "DAYPART", "SERIES"), default=[])
    plan.add_argument("--season", nargs=5, action="append", metavar=("CHANNEL", "START", "END", "HOURS", "SERIES"), default=[])
    plan.add_argument("--season-all-day", nargs=4, action="append", metavar=("CHANNEL", "START", "END", "SERIES"), default=[])
    plan.add_argument("--theme", nargs=6, action="append", metavar=("NAME", "CHANNEL", "START", "END", "HOURS", "SERIES"), default=[])
    plan.add_argument("--theme-all-day", nargs=5, action="append", metavar=("NAME", "CHANNEL", "START", "END", "SERIES"), default=[])
    plan.add_argument("--marathon", nargs=5, action="append", metavar=("CHANNEL", "DATE", "HOUR", "COUNT", "SERIES"), default=[])
    plan.add_argument("--exclude", action="append", default=[])
    schedule_commands.add_parser("list", help="List saved proposals")
    show = schedule_commands.add_parser("show", help="Display a proposal"); show.add_argument("proposal_id")
    validate = schedule_commands.add_parser("validate", help="Validate in an isolated staging workspace"); validate.add_argument("proposal_id")
    prepare = schedule_commands.add_parser("prepare", help="Validate twice and prepare an immutable schedule/selection-state candidate; never apply")
    prepare.add_argument("proposal_id")
    inspect = schedule_commands.add_parser("inspect-candidate", help="Inspect a private candidate without live reads")
    inspect.add_argument("digest")
    compare = schedule_commands.add_parser("compare", help="Display the saved current/proposed comparison"); compare.add_argument("proposal_id")
    archive = schedule_commands.add_parser("archive", help="Archive an unapplied proposal"); archive.add_argument("proposal_id"); archive.add_argument("--confirm", required=True)
    return parser


def _owner_map(policy):
    tags, unused = configured_tags(ROOT / "confs")
    numbers = {c["name"]: c["number"] for c in policy["channels"]}
    return {tag: sorted(numbers[name] for name in names if name in numbers) for tag, names in tags.items()}


def _parse_hours(value):
    try:
        hours = [int(item) for item in value.split(",")]
    except ValueError as exc:
        raise ValueError("HOURS must be a comma-separated list of integers from 0 through 23") from exc
    if not hours or any(hour < 0 or hour > 23 for hour in hours):
        raise ValueError("HOURS must contain integers from 0 through 23")
    if len(hours) != len(set(hours)):
        raise ValueError("HOURS must not contain duplicates")
    if len(hours) == 24:
        raise ValueError("Use the all-day option instead of listing all 24 hours")
    return sorted(hours)


def _plan_data(args, policy):
    owners = _owner_map(policy)
    assignments = []
    for series, channel in args.assign:
        assignments.append({"action": "assign", "series": series, "from_channel": None, "to_channel": int(channel)})
    for series, channel in args.move:
        current = owners.get(series.casefold(), [])
        assignments.append({"action": "move", "series": series, "from_channel": current[0] if len(current) == 1 else None, "to_channel": int(channel)})
    for series in args.remove:
        current = owners.get(series.casefold(), [])
        assignments.append({"action": "remove", "series": series, "from_channel": current[0] if len(current) == 1 else None, "to_channel": None})
    directives = []
    for channel, value, hour, series in args.date_slot: directives.append({"type":"date_slot","channel":int(channel),"date":value,"hour":int(hour),"series":series})
    for channel, part, series in args.daypart: directives.append({"type":"daypart","channel":int(channel),"daypart":part,"series":series})
    for channel, start, end, hours, series in args.season: directives.append({"type":"seasonal","channel":int(channel),"start_date":start,"end_date":end,"hours":_parse_hours(hours),"series":series})
    for channel, start, end, series in args.season_all_day: directives.append({"type":"seasonal","channel":int(channel),"start_date":start,"end_date":end,"all_day":True,"series":series})
    for name, channel, start, end, hours, series in args.theme: directives.append({"type":"theme","channel":int(channel),"name":name,"start_date":start,"end_date":end,"hours":_parse_hours(hours),"series":series})
    for name, channel, start, end, series in args.theme_all_day: directives.append({"type":"theme","channel":int(channel),"name":name,"start_date":start,"end_date":end,"all_day":True,"series":series})
    for channel, value, hour, count, series in args.marathon: directives.append({"type":"marathon","channel":int(channel),"date":value,"hour":int(hour),"count":int(count),"series":series})
    return assignments, directives


def _print_validation(report):
    print(f"Proposal: {report['proposal_id']}")
    print(f"Result: {'PASS' if report['valid'] else 'FAIL'}")
    if report.get("validation_context"):
        print(f"Requested seed: {report['validation_context']['requested_seed']}")
        print(f"Effective seed: {report['validation_context']['effective_seed']}")
    for failure in report.get("failures", []): print(f"FAIL: {failure}")
    for warning in report.get("warnings", []): print(f"WARN: {warning}")
    for number, item in (report.get("comparison") or {}).items():
        print(f"Channel {number} {item['name']}: {item['current_blocks']} current -> {item['proposed_blocks']} proposed blocks")


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "isolation" and args.isolation_command == "preflight":
            report, json_path, text_path = run_preflight(ROOT, profile=args.profile)
            print(f"Result: {report['result']}")
            print(f"JSON: {json_path.relative_to(ROOT)}")
            print(f"Text: {text_path.relative_to(ROOT)}")
            return 0 if report["result"] == "PASS" else 1

        if args.command == "schedule" and args.schedule_command == "inspect-candidate":
            from station_director.schedule_artifact import inspect_candidate
            try:
                print_json(inspect_candidate(args.digest))
                return 0
            except Exception:
                print('error: candidate inspection failed', file=sys.stderr)
                return 1

        if args.command == "schedule" and args.schedule_command in ("validate", "prepare"):
            if not validation_control.SCHEDULE_VALIDATION_ENABLED:
                print(validation_control.DISABLED_MESSAGE)
                return 1
            supplied_policy = os.path.abspath(os.fspath(args.policy))
            if args.policy != DEFAULT_POLICY or supplied_policy != os.fspath(DEFAULT_POLICY):
                print("error: schedule validate requires the canonical policy", file=sys.stderr)
                return 2
            from station_director.validation_coordinator import (
                render_cli_outcome, validate_saved_proposal,
            )
            candidate = None
            if args.schedule_command == 'prepare':
                from station_director.schedule_artifact import CandidatePreparation
                candidate = CandidatePreparation()
            outcome = validate_saved_proposal(args.proposal_id, **(
                {'candidate': candidate} if candidate is not None else {}))
            sys.stdout.write(render_cli_outcome(outcome))
            if candidate is not None:
                if candidate.summary is None:
                    print('Candidate: not published')
                    return 1
                print_json(candidate.summary)
            return 0 if outcome.state == "passed" else 1

        policy = load_policy(args.policy)
        if args.command == "status":
            report = station_status(ROOT, policy)
            print_json(report)
            return 1 if report["errors"] else 0

        if args.command == "inventory" and args.inventory_command == "scan":
            snapshot = build_snapshot(MEDIA_ROOT)
            path = save_snapshot(snapshot, SNAPSHOT_DIR)
            print_json({
                "snapshot": str(path.relative_to(ROOT)),
                "show_count": len(snapshot["shows"]),
                "media_file_count": len(snapshot["files"]),
                "skipped_count": len(snapshot["skipped"]),
                "errors": snapshot["errors"],
            })
            return 1 if snapshot["errors"] else 0

        snapshots = load_snapshots(SNAPSHOT_DIR)
        if args.command == "inventory" and args.inventory_command == "changes":
            validate_media_mount(MEDIA_ROOT)
            if len(snapshots) < 2:
                raise InventoryError("At least two inventory snapshots are required")
            print_json(compare_snapshots(snapshots[-2][1], snapshots[-1][1]))
            return 0

        if args.command == "recommend":
            validate_media_mount(MEDIA_ROOT)
            if not snapshots:
                raise InventoryError("Run 'director inventory scan' before requesting recommendations")
            assignments, errors = configured_tags(ROOT / "confs")
            results = recommend_shows(snapshots[-1][1].get("shows", {}).keys(), policy, assignments)
            if not args.all:
                results = [item for item in results if item["status"] != "already_assigned"]
            print_json({"recommendations": results, "configuration_errors": errors, "advisory_only": True})
            return 1 if errors else 0

        if args.command == "wio" and args.wio_command == "status":
            print(watch_in_order_status(ROOT))
            return 0

        if args.command == "schedule":
            if args.schedule_command == "plan":
                assignments, directives = _plan_data(args, policy)
                proposal, path = create_proposal(args.week, args.source, args.seed, assignments, directives, args.exclude, ROOT, args.policy)
                print_json({"proposal_id": proposal["proposal_id"], "path": str(path.relative_to(ROOT)), "dry_run": True})
                return 0
            if args.schedule_command == "list":
                print_json({"proposals": [path.name for path in proposal_dirs(ROOT)]}); return 0
            proposal, path = load_proposal(args.proposal_id, ROOT)
            if args.schedule_command == "show": print_json(proposal); return 0
            if args.schedule_command == "compare":
                report_path = path.parent / "validation.json"
                if not report_path.exists(): raise ProposalError("Validate the proposal before comparing it")
                print_json(json.loads(report_path.read_text()).get("comparison")); return 0
            if args.schedule_command == "archive":
                target = archive_proposal(args.proposal_id, args.confirm, ROOT)
                print_json({"archived": str(target.relative_to(ROOT))}); return 0
    except (InventoryError, ProposalError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
