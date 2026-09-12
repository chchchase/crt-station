# Station Director MVP

The Station Director is a small, read-only inspection layer for CRT Station. It does not rebuild catalogs or schedules and cannot apply its recommendations.

## Commands

Run commands from the FieldStation42 project directory:

```console
./director status
./director inventory scan
./director inventory changes
./director recommend
./director recommend --all
./director wio status
./director isolation preflight
./director schedule plan --week 2026-09-14
./director schedule list
./director schedule show PROPOSAL_ID
./director schedule validate PROPOSAL_ID
./director schedule compare PROPOSAL_ID
./director schedule archive PROPOSAL_ID --confirm PROPOSAL_ID
```

`status` reads systemd, the local FieldStation42 API, and the SQLite database opened in immutable read-only mode. It reports the player, catalog and schedule totals, current programming, final scheduled timestamps, and detected errors.

`inventory scan` recursively inspects `/mnt/t7/CRT-Media` using FieldStation42's video extensions and hidden-file rules. It writes one JSON snapshot under `runtime/director/inventory/`. It refuses to scan if the library is not backed by an external mount, preventing an unavailable disk from appearing as deleted media.

`inventory changes` compares the two newest valid snapshots. It reports newly seen or missing shows, added, missing, or modified video files, the total skipped-file count, newly skipped or no-longer-skipped paths, and scan errors. The complete skipped-path list remains in each snapshot without flooding an unchanged report. The command also requires the media mount to be available.

`recommend` compares the newest snapshot with tags currently referenced by station configurations. Unassigned shows receive keyword-based advisory suggestions from the versioned channel identity policy. Unclassified results require manual judgment. Channels 1 and 8 never receive ordinary recommendations.

`wio status` invokes only the existing `wio status` operation. It does not call setup, advance, or reset.

## Isolation preflight

`./director isolation preflight` exercises the same isolation launcher used by the in-progress `schedule validate` implementation. It does not run FieldStation42 scheduling.

The launcher starts Bubblewrap as the direct process of a transient user systemd service. The service sets `RestrictAddressFamilies=AF_UNIX`; Bubblewrap deliberately does not create a network namespace. The sandbox receives read-only mounts for the project, media library, and only the existing system runtime paths required by Python (`/usr`, `/bin`, `/lib`, `/lib64`, and selected dynamic-loader/timezone files beneath `/etc`). It receives private `/proc`, `/dev`, and `/tmp` views. The host's full `/home`, `/run`, user bus, and inherited environment are not exposed. The only writable host-backed mount is a unique `/tmp/fs42-i-<token>` directory mounted at `/stage`.

Inside the sandbox, the probe verifies the project and media mount flags are read-only, staging is writable, private runtime mounts exist, and the environment exactly matches a fixed key/value allowlist including `PWD=/stage`. Reports include the non-secret allowlisted environment values. The probe also verifies that IPv4 and IPv6 socket creation and `127.0.0.1:4242` access are denied, and that a bounded AF_UNIX round trip succeeds on a short socket path. Create, read, rename, and delete tests operate only under `/stage`. The preflight never attempts a write to the live project or media paths.

Every run retains `preflight.json` and `preflight.txt` in a new, uniquely named directory beneath `runtime/director/isolation/`. A PASS requires zero transient-unit and Bubblewrap exit statuses, every expected probe result present and true, complete retained reports, and successful unit, current-staging, and stale-staging cleanup. Missing, malformed, incomplete, contradictory, timed-out, or cleanup-failed results fail closed. An already-collected unit is treated as clean only when systemd explicitly reports it absent.

Mocked unit tests verify control flow only; they do not establish that isolation works on this host. Real preflight and validation attempts must be started from a normal SSH shell outside Codex. Before creating staging or entering Bubblewrap, each command checks its host process ancestry and every inherited environment key whose name starts with `CODEX_`. It requires an `sshd` ancestor and fails closed when ancestry cannot be verified or Codex is detected. Consequently, legitimate local-console, detached-tmux, cron, and service invocations are intentionally not recorded as verified. Process ancestry is an operational safeguard, not protection against a privileged actor capable of spoofing process names or environment state.

## Read-only boundary

The Director never writes FieldStation42 configuration, its SQLite database, schedules, media, guide data, persistent service state, or Watch In Order state. Director-owned inventory snapshots, proposals, validation artifacts, and isolation reports remain beneath `runtime/director/`; isolation staging is temporary beneath `/tmp`.

The canonical identity policy is `director_conf/channel_identities.v2.json`. Director policy files intentionally live outside `confs/`, where FieldStation42 would interpret JSON files as station definitions.

## Dry-run schedule proposals

Schedule proposals are versioned JSON data stored at `runtime/director/proposals/<proposal-id>/proposal.json`. The v2 identity policy enforces one active channel per series and protects Channel 8. Each assignment change records its losing and gaining channel; WIO titles stop normal validation.

Planning accepts repeatable structured options: `--assign SERIES CHANNEL`, `--move SERIES CHANNEL`, `--remove SERIES`, `--date-slot CHANNEL DATE HOUR SERIES`, `--daypart CHANNEL DAYPART SERIES`, and `--marathon CHANNEL DATE HOUR COUNT SERIES`. Marathon `COUNT` uses FieldStation42's native episode-count meaning; durations in hours are not accepted.

Seasonal and theme directives must declare their scope explicitly. `--season CHANNEL START END HOURS SERIES` and `--theme NAME CHANNEL START END HOURS SERIES` accept a comma-separated list of distinct hours from 0 through 23. A list may contain at most 23 hours. Full-day programming instead requires `--season-all-day CHANNEL START END SERIES` or `--theme-all-day NAME CHANNEL START END SERIES`, which records `"all_day": true`. An hours list and `all_day` are mutually exclusive. All directive dates and inclusive ranges must fall within the proposal's Monday-through-Sunday target week. These options never edit live configuration.

New proposals use schema v2. Proposal week boundaries are stored as canonical RFC 3339 timestamps in the station's `America/Los_Angeles` timezone. Schema-v1 proposals are validated against the frozen v1 reader schema and safely migrated in memory; their saved JSON is not rewritten. Legacy date-slot and daypart directives can migrate. Legacy marathon `hours` and seasonal/theme directives with implicit full-day scope are rejected rather than guessed.

Assignment actions have strict shapes: assign requires a null source and a Channel 2-7 destination; move requires distinct Channel 2-7 source and destination values; remove requires a Channel 2-7 source and a null destination. An `--assign` or `--move` must be paired with at least one scheduling directive for the same series and destination channel. Moves and removals delete the series from their declared source in the projected configuration. Exclusions remove the series from ordinary projected channel configurations and cannot name a series that the same proposal assigns or schedules. Any change that turns a previously tagged source hour into a tagless or unresolved hour is rejected.

Every proposal series name, including exclusions, must exactly match one canonical identifier in the captured inventory. Unknown names, case-only approximations, and identifiers made ambiguous by case collisions are rejected. Channels 1 and 8 and all Watch In Order series remain protected from normal assignments, directives, removals, and exclusions.

Phase 3 schedule validation is explicitly disabled after Milestone B1. `schedule validate` always returns `Phase 3 validation is not yet enabled` and never invokes FieldStation42's catalog or schedule generator. When invoked from Codex or without verified SSH ancestry, it fails before allocating staging. In an accepted host context it uses only the verified `IsolationLauncher`, holds a lock on a unique `/tmp/fs42-i-<token>` directory, repeats every isolation probe inside the worker, validates staged configuration paths, and then returns the disabled result. Missing or contradictory probe attestations, launcher failures, unsafe paths, and cleanup failures all fail closed; there is no direct or unsandboxed worker fallback.

The host symlink `catalog/crt_media -> /mnt/t7/CRT-Media` is not itself a read-only link or security boundary. Read-only enforcement comes from Bubblewrap mounting the media tree at sandbox-only `/media`. Approved live paths beneath `/mnt/t7/CRT-Media` and `catalog/crt_media` are mapped to `/media` only for isolated execution. Each mapping retains a canonical `crt-media:/...` identity and canonical host path; `/media` paths must never be presented as paths suitable for live application. Lexical traversal, paths outside the approved roots, broken links, symlink escapes, and scheduled content that is not a readable regular file beneath `/media` are rejected.

Milestone B2 will add schedule-history/catalog preservation and complete live-state fingerprints. Milestone C will add deterministic dual scheduler execution, guide validation, and immutable validation report history. No real schedule validation should be run until those milestones are complete and reviewed.

There is deliberately no apply or rollback command. Archiving requires the exact proposal ID as confirmation and only moves Director-owned proposal files.
