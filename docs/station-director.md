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

`./director isolation preflight` exercises the reusable isolation launcher that a future, separately approved `schedule validate` implementation will use. It does not run FieldStation42 scheduling.

The launcher starts Bubblewrap as the direct process of a transient user systemd service. The service sets `RestrictAddressFamilies=AF_UNIX`; Bubblewrap deliberately does not create a network namespace. The sandbox receives read-only mounts for the project, media library, and only the existing system runtime paths required by Python (`/usr`, `/bin`, `/lib`, `/lib64`, and selected dynamic-loader/timezone files beneath `/etc`). It receives private `/proc`, `/dev`, and `/tmp` views. The host's full `/home`, `/run`, user bus, and inherited environment are not exposed. The only writable host-backed mount is a unique `/tmp/fs42-i-<token>` directory mounted at `/stage`.

Inside the sandbox, the probe verifies the project and media mount flags are read-only, staging is writable, private runtime mounts exist, and the environment exactly matches a fixed key/value allowlist including `PWD=/stage`. Reports include the non-secret allowlisted environment values. The probe also verifies that IPv4 and IPv6 socket creation and `127.0.0.1:4242` access are denied, and that a bounded AF_UNIX round trip succeeds on a short socket path. Create, read, rename, and delete tests operate only under `/stage`. The preflight never attempts a write to the live project or media paths.

Every run retains `preflight.json` and `preflight.txt` in a new, uniquely named directory beneath `runtime/director/isolation/`. A PASS requires zero transient-unit and Bubblewrap exit statuses, every expected probe result present and true, complete retained reports, and successful unit, current-staging, and stale-staging cleanup. Missing, malformed, incomplete, contradictory, timed-out, or cleanup-failed results fail closed. An already-collected unit is treated as clean only when systemd explicitly reports it absent.

Mocked unit tests verify control flow only; they do not establish that isolation works on this host. The final real preflight must be run with `./director isolation preflight` from a normal SSH shell outside Codex. Before creating staging or entering Bubblewrap, the command checks its host process ancestry and every inherited environment key whose name starts with `CODEX_`. It requires an `sshd` ancestor and fails closed when ancestry cannot be verified or Codex is detected. Consequently, legitimate local-console, detached-tmux, cron, and service invocations are intentionally not recorded as verified. Process ancestry is an operational safeguard, not protection against a privileged actor capable of spoofing process names or environment state. Until that real command passes, the isolation implementation remains unverified and Phase 3 remains blocked.

## Read-only boundary

The Director never writes FieldStation42 configuration, its SQLite database, schedules, media, guide data, persistent service state, or Watch In Order state. Director-owned inventory snapshots, proposals, validation artifacts, and isolation reports remain beneath `runtime/director/`; isolation staging is temporary beneath `/tmp`.

The canonical identity policy is `director_conf/channel_identities.v2.json`. Director policy files intentionally live outside `confs/`, where FieldStation42 would interpret JSON files as station definitions.

## Dry-run schedule proposals

Schedule proposals are versioned JSON data stored at `runtime/director/proposals/<proposal-id>/proposal.json`. The v2 identity policy enforces one active channel per series and protects Channel 8. Each assignment change records its losing and gaining channel; WIO titles stop normal validation.

Planning accepts repeatable structured options: `--assign SERIES CHANNEL`, `--move SERIES CHANNEL`, `--remove SERIES`, `--date-slot CHANNEL DATE HOUR SERIES`, `--daypart CHANNEL DAYPART SERIES`, `--season CHANNEL START END SERIES`, `--theme NAME CHANNEL SERIES`, and `--marathon CHANNEL DATE HOUR HOURS SERIES`. These options never edit live configuration.

Proposal week boundaries are stored as RFC 3339 timestamps in the station's `America/Los_Angeles` timezone. Legacy proposals without an offset are interpreted in that timezone when read and are not rewritten.

An `--assign` or `--move` must be paired with at least one scheduling directive for the same series and destination channel. During validation, a move removes the series from its losing channel in the staged configuration before adding the proposed overrides; a remove only removes it from the staged configuration. `--exclude` records a planning exclusion and cannot name a series that the same proposal assigns or schedules. A theme name is proposal metadata; its schedule effect is a target-week override for the named series.

Validation rejects stale source hashes, clones the live SQLite database using its backup API, copies configurations, and runs affected FieldStation42 catalog/schedule generation under `runtime/director/staging/`. The media library is linked read-only. Staging is removed afterward and `validation.json` is retained with failures, warnings, schedule coverage, integrity results, a deterministic digest, and a per-channel comparison.

There is deliberately no apply or rollback command. Archiving requires the exact proposal ID as confirmation and only moves Director-owned proposal files.
