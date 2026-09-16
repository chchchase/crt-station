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
./director isolation preflight --profile native-single-run
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

Phase 3 schedule validation has one checked-in master gate. While disabled, `schedule validate` returns `Phase 3 validation is not yet enabled`, reports `scheduler_invoked: false`, and never invokes FieldStation42's catalog or schedule generator. The disabled branch stops before SSH/Codex verification, proposal or policy reads, locking, staging, worker imports, or report publication. When enabled, the reviewed coordinator is the only public path to the C1 single-run and C2 dual-run engines. Those engines retain the fail-closed probe, unique locked stage, path-confinement, protocol, preservation, and cleanup checks; only `IsolationLauncher` can start a production worker and no unsandboxed fallback exists.

Milestone C0 adds an opt-in native validation scheduling context without enabling scheduling. The context fixes FieldStation42's local reference clock and range. Its effective RNG seed is derived from exactly the canonical proposal (excluding its separately reported requested seed), canonical policy, logical protected-configuration fingerprint, logical-database fingerprint, and logical media-manifest fingerprint. The physical protected-file fingerprint remains separate and is used only for preservation checks. Deterministic ordering and strict exclusion errors apply only while that context is active. Existing FieldStation42 callers retain their prior clock, process-global RNG, enumeration order, and warning-only exclusion behavior. The isolated environment now also requires exact `TZ=America/Los_Angeles` and `PYTHONHASHSEED=0` values established before Python starts. Because that changes the probe contract, any pre-C0 isolation preflight is obsolete and a new preflight must pass from a normal SSH shell before validation work continues.

The validation worker performs its isolation probes before dynamically importing any native FieldStation42 scheduling context, configuration processor, catalog, scheduler, station manager, or guide module. C0 also factors the custom guide channel and schedule route bodies into pure payload builders; the HTTP routes delegate directly to them and retain their exact response shapes.

Milestone C1 adds a separate internal worker capability for one native staged run while leaving that public gate intact. Its versioned request embeds strict proposal and policy shapes and is bound by a typed, domain-separated digest to the protocol version, operation, run ID, proposal, policy, validation context, affected channels, and declared input fingerprints. New workers emit Boolean response protocol v3; frozen response protocols v1 and v2 remain retained-data readers only, and host coordination uses result protocol v3. Before its first `fs42` import, the worker holds a private regular request file open, repeats isolation probes, independently verifies the original logical configuration/database/media fingerprints and physical source transition, deterministically constructs the projected configuration and exact working database, fingerprints those final scheduling inputs, recomputes the effective seed, and rejects any mismatch. The final held-request integrity check occurs after seed verification and immediately before native import. The logical original, logical projected configuration, logical working database, logical media manifest, and physical preservation fingerprints remain distinctly named. Request and response reads are bounded to 2 MiB; publication requires private mode-0600 regular files, a single link, and exclusive atomic no-replace publication. C1 additionally binds `/stage/transient` at `/tmp` for this worker only, so native temporary files remain owned by the locked staging lifecycle. The ordinary preflight and disabled validation worker retain their private tmpfs. The C1 worker probe attests that `/tmp` is the exact stage-backed directory rather than merely trusting the launcher option.

C1's pathname threat model covers accidental replacement, stale or reused paths, symlinks, unexpected hard links, untrusted request paths, concurrent Director runs, sandbox path escape, and mutation outside the disposable staged workspace. Stages have random private mode-0700 names and held locks; protocol and staged input files use mode 0600, no-follow opens, regular-file and link-count checks, held descriptors, no-replace publication, and identity/fingerprint checks before and after practical native pathname reopens. The final accepted result verifies the same staged configuration identities and database inode consumed by native code. Any detected change fails closed. A deliberately malicious process already running as the same Unix UID is outside this C1 threat model; C1 does not claim protection against ptrace or a precisely timed same-UID adversary.

Milestone C2 adds a private, still-unrouted reproducibility coordinator. It pins one short read-only SQLite transaction only while calculating the baseline logical fingerprint, creating two independent backups from that exact view, and verifying both backups. The source transaction closes before either native run begins. Each backup then receives its own projected configuration, worker interpreter, transient unit, Bubblewrap process, validation context, writable database, and locked stage. Runs execute sequentially in canonical input order; run 1's unit must be proven absent before normalization and run 2. Neither run is cloned from or reads the other. Both stages remain owned by one private context and are cleaned in reverse order; an unconfirmed unit leaves its stage quarantined and prevents success.

C2 rechecks logical and physical configuration state, the logical live database, and logical and physical media metadata after capture, between runs, after run 2, and after both stage-cleanup attempts immediately before success. Missing, replaced, malformed, unreadable, or otherwise un-fingerprintable input is also reported as `input_changed` in its precise category, never scheduler nondeterminism. This strict check is expected to reject validation if normal live playback changes the database during the two-run window; later external testing must measure that operational risk rather than weaken preservation.

Completed staged databases are normalized read-only into private mode-0600 streams and lookup indexes outside the FieldStation42 database. SQLite values retain distinct null, integer, IEEE-754 real (including negative zero), text, and BLOB encodings. JSON retains types and list order; protocol JSON rejects duplicate keys at every nesting level before schema checks, and non-finite values and unsupported reference shapes fail closed. Media aliases become their `crt-media:/...` identities. Historical catalog IDs and the actual typed `sqlite_sequence` rows stay exact, so allocated-and-deleted IDs and counter advancement remain visible. A newly allocated catalog ID becomes a provisional token only when its complete typed catalog row and file, break, and chapter metadata provide a unique semantic identity. A separate disk-backed ledger inventories and then independently verifies every proven catalog-reference location and token; unrelated equal integers are untouched. Unknown schema objects, tables, columns, values, indexes, and triggers remain comparison inputs. Explicit limits bound protected-configuration files and bytes, schema objects, tables, columns, foreign-key findings, reference-ledger rows, canonical records, and diagnostics.

Only comparison ID, per-run run ID, transient-unit identity, physical stage paths and identities, execution timings, and cleanup timestamps are operational equality exclusions. No broad field-name exclusion exists: schedule and catalog timestamps, counters, warnings, channel seeds, selected media, playback plans, and all logical database values remain equality inputs. Canonical streams are compared structurally through end-of-file even after the bounded diagnostic limit is reached. Equal digests alone never establish success. Failures retain explicit `input_changed`, `normalization_failed`, `comparison_failed`, `reproducibility_mismatch`, `cleanup_failed`, or `c1_run_failed` classification; a primary run/comparison failure and per-stage cleanup results remain independently visible.

The effective seed is the validation-run root. Each affected channel, processed in canonical numeric order, receives a domain-separated channel seed derived from the root seed, canonical channel number and name, regeneration seam, and effective horizon. Native catalog construction and `LiquidSchedule.generate_validation_range()` run only under that channel's immutable validation context. Ordinary FieldStation42 RNG and catalog behavior remain unchanged. Catalog-entry construction problems become fatal only in validation mode; C1 does not infer failures by parsing printed warnings.

FieldStation42 permits `confs/main_config.json` to be absent and then uses `StationManager`'s native defaults. Validation preserves that behavior: an exact, securely inventoried absence publishes no synthetic main configuration. A present exact file is securely loaded and the staged scheduling copy contains only the fields the inspected catalog/scheduler graph reads: `db_path`, `day_parts`, `custom_holidays`, `normalize_titles`, and `title_patterns`. Case-confusable names, malformed or unsafe files, and read or replacement failures fail closed. API keys, PINs, sockets, host/port values, playback/service controls, and unrelated runtime configuration are excluded. All station media paths are translated to `/media`, with `crt-media:/...` retained as the stable identity.

All affected future rows are deleted in one transaction before the first channel is regenerated. Therefore the first affected channel sees retained affected history plus unaffected schedules; each later channel also sees already-regenerated earlier channels when the native sibling-exclusion index is built. This ordering is deterministic and is reported. Retained history and catalog rows remain exact. Reuse of any other catalog ID requires complete typed catalog semantics—including title, duration, tag, hints, content/media type, SQLite storage classes, and cached file/break/chapter metadata—to match after canonical path-alias normalization. New IDs are allocated explicitly above both the current maximum and `sqlite_sequence` and are marked provisional for C2.

Native catalog and scheduling may mutate sequence state in the disposable database, so C1 snapshots all rows, columns, and schemas in `named_sequence`, `sequence_entries`, and `sequence_group_state` before either operation, restores them in foreign-key-safe order, and independently verifies the global snapshot afterward. No intermediate staged database is accepted as a result: retained references are checked immediately after catalog reconciliation, and all B1/B2 preservation, foreign-key, path, seam, coverage, and effective-horizon checks must pass before success.

Configured AutoBump is permitted when native scheduling does not select it. The worker mirrors native duration defaults before the first affected scheduler enters and rejects configurations that would require media probing; a validation-mode guard independently blocks the probe at its subprocess boundary. After each native schedule is generated, exact AutoBump plan and catalog descriptor markers are inspected from the regeneration seam before ordinary `/media` conversion. Final media checks use that same seam as the provenance boundary: structurally valid retained AutoBump descriptors remain untouched and bypass media conversion, while every newly generated use fails with a value-free diagnostic. Contradictory partial markers fail as an invalid playback descriptor regardless of provenance, and all ordinary retained and generated media remain subject to confinement checks. The worker substitutes a fixed marker before presentation construction; descriptor bodies are never parsed, logged, returned, rendered, or loaded as content, and player and web-renderer modules remain unreachable. Host-side comparison preserves descriptor differences as described below. Unrelated native `SystemExit` results remain `native_system_exit`. The internal lifecycle separates preparation, launch, inspection, and cleanup so C2 can nest two internally owned scopes and retain both disposable results for comparison. Ownership is never caller-transferable: the coordinator verifies unit termination before removing its stage. A unit that cannot be proven absent leaves a locked, quarantined stage for manual recovery, and cleanup failure makes the run fail. No persistent validation report is created.

Normalization and baseline comparison reuse the pure AutoBump structural classifier, loaded lazily without the native runtime or player. Valid descriptor paths receive domain-separated SHA-256 tokens over their exact typed text; they are not mapped as media paths or replaced by a constant. Other descriptor fields retain their exact typed values, so body, duration, and numeric-type changes remain comparison inputs. Canonical output and report previews omit opaque descriptor bodies. Existing private baseline lookup data still supports exact retained-row verification and is disposed with the stage. In a completed run, descriptor-bearing blocks must match baseline rows exactly and be outside the affected channel’s generated range; new descriptor catalog entries and generated descriptor use fail closed. The sole no-reference exception is JSON-null content on a valid retained single-feature AutoBump LiquidWebBlock. Null on other blocks, empty/Boolean references, malformed markers, and ordinary paths outside media confinement remain rejected. Baseline-side comparison may describe old AutoBump programming being replaced, but proposed generated AutoBump remains forbidden. Versioned negative chapter attestations remain ordinary typed JSON equality inputs; no chapter endpoint or duration is changed.

New native workers publish response schema v4; production inspection requires that exact version, while response schemas v1 through v3 remain frozen and readable only for retained-protocol compatibility. Missing or stale verified cache metadata produces the fixed `catalog_metadata_unavailable` diagnostic before any validation-time probe, decoder, MoviePy, FFmpeg, or subprocess fallback; other native catalog errors remain `catalog_failure`.

Validation catalog rebuilding is cache-only. Native directory, tag, ordering, multiplicity, hint, content-type, and weighting discovery still constructs the current candidate population, but every selected media file must have current typed `file_meta` data and an existing chapter-scan record where native catalog construction requires one. Missing, stale, malformed, or untyped metadata fails before FFprobe, MoviePy, FFmpeg, decoder, or subprocess entry. The working database is disposable, but the rebuild does not invent metadata. Ordinary non-validation catalog refresh, probing, argument ordering, and serialization remain unchanged.

Each worker publishes a private value-free lifecycle journal at the fixed stage-relative `native-single-run.checkpoints/` identity. At most 40 canonical records encode only a sequence number and an allowlisted state: worker start; probes; snapshot, seed, and final request verification; native import; configuration; up to six catalog/scheduler cycles; and response publication attempt/completion. Records are mode 0600, single-link regular files, exclusively created and fsynced, then published with `RENAME_NOREPLACE` and a directory fsync. One metadata-safe pending record at exactly the next sequence is an ignored unparsed crash tail; gaps, multiple tails, unsafe identities, unknown names, malformed published records, or invalid transitions invalidate the evidence. A durable scheduler-entry record proves `true`. Authoritative proof that the main process never began proves `false`. A started process without a valid response or scheduler-entry record is `unknown`; response/checkpoint contradictions are `worker_checkpoint_mismatch`. Successful validation still requires `true` for both runs.

Transient units retain their state until the launcher captures the exact `LoadState`, `ActiveState`, `SubState`, `Result`, `ExecMainCode`, `ExecMainStatus`, and `ExecMainStartTimestampMonotonic` properties; collection and cleanup follow inspection. The launcher starts `systemd-run` with bounded output draining but supervises the retained unit through bounded property polling instead of waiting for the unit to become inactive. A successfully completed retained oneshot is terminal at `active/exited`, so short preflights return promptly while their properties remain available. `active/running` and `activating/start` remain nonterminal and become outer-watchdog failures only after affirmative local watchdog expiry. This distinguishes unit runtime expiry, OOM kill, external signal, normal nonzero exit, launcher failure, and invalid launcher evidence without journal parsing or process-output projection. A missing unit after launcher execution is unknown, and scheduler state is false only when the launcher never executed or the retained start timestamp is zero. The unit hard limit is 2,700 seconds, the launcher watchdog 2,730 seconds, and the per-run allowance 2,850 seconds, including the 15-second inspection allowance and a 95-second shared-deadline cleanup allowance for at most nine commands. Internal phase budgets total 2,520 seconds plus 120 seconds overhead. Ordinary work is bounded at 6,660 seconds. No new expensive phase is admitted at or after 7,020 seconds, leaving a 360-second ordinary-work margin. Mandatory finalization requires 330 seconds: two 95-second unit-cleanup sequences, 120 seconds for report construction/publication, 10 seconds for stage and capture finalization, 5 seconds for signal restoration, and 5 seconds for lock release. The coordinator reserves 340 seconds and therefore owns a 7,360-second overall control deadline beginning before capture. Cleanup and lock release are still attempted after an overrun, which records only `finalization_deadline_overrun`.

The host symlink `catalog/crt_media -> /mnt/t7/CRT-Media` is not itself a read-only link or security boundary. Read-only enforcement comes from Bubblewrap mounting the media tree at sandbox-only `/media`. Approved live paths beneath `/mnt/t7/CRT-Media` and `catalog/crt_media` are mapped to `/media` only for isolated execution. Each mapping retains a canonical `crt-media:/...` identity and canonical host path; `/media` paths must never be presented as paths suitable for live application. Lexical traversal, paths outside the approved roots, broken links, symlink escapes, and scheduled content that is not a readable regular file beneath `/media` are rejected.

Milestone B2 implements the safety layer for staged history and live-state preservation, but does not call it to regenerate a schedule while validation remains disabled. The live SQLite database is opened explicitly read-only and cloned through SQLite's backup API from the same transactionally consistent logical snapshot. Logical database fingerprints use typed values, deterministic table ordering, schema and persistent pragma digests, row counts, integrity results, and foreign-key-check results; database, WAL, shared-memory, and journal file metadata is diagnostic only. The implementation uses the database's inspected `liquid_blocks`, `catalog_entries`, `named_sequence`, `sequence_entries`, `sequence_group_state`, `file_meta`, `break_points`, and `chapter_points` tables rather than assuming alternate names.

For an affected channel, every schedule row whose start precedes the proposal boundary is retained column-for-column, including a row that crosses the boundary. Regeneration starts after the latest retained crossing block and must provide continuous, non-overlapping coverage through the later of the proposal end or that channel's original schedule horizon. Coverage checks include the proposal boundary, proposal end, and effective horizon. Catalog references accept only the native integer or array-of-integers encodings observed in FieldStation42; every referenced historical catalog row and ID is retained, while a separate sandbox-path catalog row may be allocated for future generation. Unaffected channels and Channels 1 and 8 are exact protected snapshots. Every row and inspected column in `named_sequence`, `sequence_entries`, and `sequence_group_state` is protected globally, including sequence state associated with an affected channel.

Pre/post preservation covers every `confs/*.json` file, Watch In Order JSON state, the complete logical SQLite schema and table contents, and media-tree metadata. Media manifests stream to temporary files with bounded memory, never hash video contents, never traverse directory symlinks, omit access time, use byte-safe relative names, and include type, size, mode, ownership, device/inode, nanosecond modification/change times, symlink targets, and xattr digests. Filesystems returning `ENOTSUP` or `EOPNOTSUPP` for xattrs are recorded as lacking that capability; other inspection errors fail closed. Validation summaries contain aggregate digests, counts, timings, and bounded differences rather than the full manifest.

Milestone C3a1 adds guide validation only to the unreachable internal native-run path. After native writers close, each C1 worker copies the stable working database and any WAL bytes into a private stage capture, verifies that the working file and sidecars did not change, and uses SQLite's backup API on that capture to create `guide-input/guide.db`. The completed snapshot is mode 0400 in a mode-0500 directory, contains no sidecars, and is opened with `mode=ro`, `immutable=1`, verified `PRAGMA query_only`, file-backed temporary storage, an 8 MiB SQLite cache, and a 256 KiB SQLite value limit. Complete logical and physical/sidecar checks bracket guide access. The loader never constructs the table-initializing `LiquidIO`, `CatalogIO`, or `MetadataIO` services. Shared row and metadata transformations feed the same pure channel and incremental schedule projection used by `/summary/channels` and `/schedules/all`. The HTTP routes keep their existing signatures, half-open overlap behavior, payload shapes, field types, ordering, and errors.

Milestone C3a2 adds two still-private capabilities. First, each successful C2 run is compared with its own copy of the same pinned baseline while both stages remain open. Comparison is limited to each channel's half-open `[regeneration_start, effective_horizon)` interval. Retained rows before the seam are counted and verified separately; a retained row ending at the seam is not double-counted, and one that actually overlaps the advanced seam is a preservation error. Exact interval multisets match complete semantic-equal records first and then deterministically pair the remaining canonical semantic bytes as replacements. Changed interval topology is reported as reshaped components with separate baseline/proposed block counts. Coverage uses exact integer microseconds, clips at both boundaries, treats touching intervals as non-overlapping, records baseline gaps/overlaps as baseline findings, and rejects newly introduced coverage defects. Declared proposal operations are reported only as requested configuration effects; episode, commercial, bumper, and playback consequences remain resulting schedule changes.

The comparator streams database rows into a private stage-local disk index and continues through all records after sample limits. Its tables and covering indexes are created while empty, and every ordering/aggregation query has an asserted `EXPLAIN QUERY PLAN` with no SQLite temporary B-tree; it does not configure or use the host SQLite temporary directory. It enforces one-million-block, 100,000-block-per-component, 512 MiB spool, 512-byte sample, and 256 KiB summary limits during writes. The C2 free-space reservation includes comparison and report scratch space. Catalog and playback semantics reuse the C2 typed normalization contract, including `crt-media:/...` identities; no absolute host, `/media`, or staging path is emitted.

Second, C3a2 provides a disconnected immutable report publisher for later C3b use. It is not imported or called by the CLI, validation coordinator, or stage runner. Reports use the checked-in Director semantic version (`station_director.__version__`, currently `0.1.0`) and report schema version; this semantic version is not claimed to identify a Git commit. The internal report root is `runtime/director/validations/<proposal-id>/`. A complete `validation.json` and deterministic `validation.txt` are written in a private hidden directory, fsynced, and published as one immutable `v-<UTC completion time>-<128-bit random token>` directory using Linux `renameat2(RENAME_NOREPLACE)` through held directory descriptors. No overwrite-capable fallback exists. Publication distinguishes not published, published but not durably fsynced, and published/durable outcomes; a published directory is never removed.

`latest.json` means the latest completed attempt, whether successful or failed. An attempt reaches pointer completion only inside the serialized proposal-directory publication lock, so ordering follows completion of that workflow rather than proposal or run-ID time. Before replacement, an existing pointer is strictly parsed and its proposal/run identity, safe immutable directory, canonical report bytes, and report digest are verified. An invalid, dangling, cross-proposal, unsafe, or bad-digest pointer is left untouched; pointer failure is returned with separate not-replaced or replaced-not-durable state and never changes immutable validation status. Existing proposal files, legacy colocated reports, and prior immutable runs are never moved, overwritten, or deleted.

Validation report schemas v1 through v5 are frozen for retained immutable reports. New attempts use validation report schema v6 and native dual-run result v4. The host and report scheduler state is `true`, `false`, or `unknown`, while a valid worker response remains Boolean. C1 diagnostics retain their exact run number, compatible domain/phase/code, optional allowlisted probe or fingerprint category, and a launcher summary limited to fixed outcome/category fields, bounded byte counts, truncation flags, and verified numeric exit status or signal. Diagnostic templates and hashes use only these safe structured fields. Raw exceptions, journal text, process output, unit names, paths, media identities, configuration values, secrets, and environment data are excluded. Existing valid v1–v5 reports and `latest.json` pointers remain readable and may be atomically superseded by a completed v6 attempt without rewriting an earlier report.

Report projection uses named `run_1`/`run_2` guide, preservation, and cleanup fields and named source-stability checkpoints, plus explicit not-started, attempted, completed, failed, and unavailable phase states. Raw exception text and native output are never projected: diagnostics use allowlisted codes, fixed safe templates, typed counts/channel fields, allowlisted exception classes, and a digest. Canonical JSON serialization, deterministic control-escaped text rendering, schema validation, and explicit publication remain separate operations. The trusted project root and every fixed report-tree component are opened through held no-follow directory descriptors with owner, permission, identity, case-ambiguity, and bounded-entry checks. C3a2 tests redirect the internally derived root only by patching both trusted constants.

Guide loading is station-at-a-time and 32 blocks per batch. Raw expected intervals and actual projected intervals are stored in a private stage-local SQLite index rather than retained in Python. Limits include 64 stations, 25,000 blocks per station, 100,000 total blocks, 64 catalog references per block, 128 references per batch, 256 KiB SQLite fields, 512 KiB aggregate rows, 192 KiB encoded JSON, depth 32, 10,000 JSON members, 512 array items, 64 KiB strings, 4 MiB metadata batches, 200,000 distinct boundary probes, 512 KiB canonical records, and a 64 MiB guide stream. A deliberately conservative worker-memory ceiling is approximately 700 MiB: 8 MiB SQLite cache; at most 16 MiB of raw schedule rows and 64 MiB of raw catalog rows; up to about 400 MiB for worst-case Python expansion of bounded schedule, catalog-hint, and metadata JSON; 120 MiB for the bounded record-path set; and under 92 MiB for payload projection, Python/SQLite objects, and writer buffers. Normal inputs should remain far below that ceiling. Sorts and interval indexes are file-backed beneath the stage-backed `/tmp` or stage workspace.

Guide validation uses the real half-open rules: a database block overlaps a request when `start_time < requested_end` and `end_time > requested_start`, and a listing resolves at an instant when `start_time <= instant < end_time`. Zero matches are valid outside generated coverage; multiple matches fail. Proposal, seam, proposal-end, effective-horizon, and every block-transition boundary are checked at one microsecond before, exactly at, and one microsecond after the boundary. Metadata remains optional because the real guide omits it when no cached row exists. The guide does not expose selected-media or catalog identity; those remain C2 playback-plan assertions.

Every C1 worker writes its complete guide result as a versioned, length-framed, mode-0600 stream at the fixed stage-relative identity `guide/guide-v1.records`. Records and the aggregate stream are bounded, exclusively created, fsynced, and identity-checked. The C1 JSON response contains only its version, identity, digest, counts, bounded channel/boundary summaries, and bounded errors. C2 reads and validates both complete streams through EOF and incorporates every guide record into structural reproducibility comparison; digest equality alone is insufficient. A guide-only difference is `guide_reproducibility_mismatch`. Guide loading and validation failures are distinguished as `guide_loading_failed` and `guide_validation_failed`.

C3b1 added the complete public coordinator and immutable-report flow behind the single checked-in `station_director.validation_control.SCHEDULE_VALIDATION_ENABLED` constant. While that gate is false, a syntactically valid `schedule validate PROPOSAL_ID` prints exactly `Phase 3 validation is not yet enabled`, exits 1, and stops before invocation checks, proposal or policy access, the validation lock, stale-stage recovery, C2 imports, staging, workers, or report publication. Normal argparse errors may still exit 2 before that gate. The old object-based `validation.validate_proposal()` entry point is permanently side-effect-free and disabled regardless of the master constant; it cannot become an alternate scheduling route. C3b2 enabled only the checked-in master constant after the reviewed external preflight.

The enabled coordinator, exercised only with synthetic test-time patching in C3b1, verifies SSH ancestry and absence of Codex first, validates the proposal ID lexically, and then holds one persistent private global `flock` through trusted proposal/policy acquisition, stale-stage recovery, dual execution, cleanup, final source checks, immutable publication, and latest-pointer handling. Proposal and policy paths are derived from the checked-in project root and traversed through held no-follow directory descriptors. Proposal schema-v1 migration remains in memory and never rewrites saved bytes. Alternate `--policy` values are rejected for enabled validation without opening them; other Director commands retain their existing policy option behavior. Bounded CLI output contains only canonical IDs, fixed phase/failure codes, counts, booleans, and relative report identity.

After secure proposal loading and in-memory migration, the coordinator rejects a proposal whose assignments, directives, and exclusions are all empty with `proposal_has_no_effects`. This eligibility check occurs before policy loading, validation run-ID allocation, stale-stage recovery, capture, staging, scheduling, and report publication, so an existing saved no-op proposal remains loadable but cannot create another validation attempt. The C1 finalizer repeats the affected-channel check as defense in depth and preserves the explicit error through C2.

Signal handlers are installed only in the main thread after invocation, proposal, policy, and run identity are trusted. SIGINT, SIGTERM, SIGHUP, and `KeyboardInterrupt` enter the same bounded cleanup path; later signals cannot interrupt cleanup, prior handlers are restored, and the lock descriptor is always released. SIGKILL and power loss cannot be handled. A later invocation scans only canonical old unlocked Director stages, reuses the reviewed exact-unit cleanup and absence checks, and never targets `fs42.service`; malformed, young, locked, quarantined, ambiguous, or unrelated entries are left untouched. `tmux` is recommended for the eventual long external dry run but is not required by the software.

C3b2 was deliberately limited to changing the master constant after C3b1 was committed and pushed, a fresh normal-SSH `./director isolation preflight --profile native-single-run` succeeded and its full report was reviewed, and the complete synthetic/static audit was repeated. Real dry runs remain read-only and should be launched from a normal SSH session, preferably inside `tmux`; their immutable reports must be reviewed before relying on the result.

Chapter metadata maintenance is deliberately separate from validation and
schedule generation. The dedicated, externally gated, default-read-only
chapter-cache command, its versioned successful-scan attestations, WAL-safe
backup ordering, resumption rules, and downgrade procedure are specified in
[`chapter-cache-warmup.md`](chapter-cache-warmup.md). It must not be executed
from Codex or as part of a validation run.

Before the first production validation, remove group-write permission from the three trusted path ancestors with these non-recursive commands:

```bash
chmod 0755 -- /home/chaseanderegg/FieldStation42
chmod 0755 -- /home/chaseanderegg/FieldStation42/runtime
chmod 0700 -- /home/chaseanderegg/FieldStation42/runtime/director
```

The validation coordinator intentionally rejects group-writable trusted directories. These changes preserve owner access for `fs42.service`, which runs as `chaseanderegg`, while preventing path replacement by another group member. Do not apply this migration recursively; proposal, policy, report, and unrelated FieldStation42 paths must retain their individually reviewed permissions.

The stage-backed C1 `/tmp` mount is a new isolation variant. The shared launcher first verifies the canonical, owned, mode-0700 `/tmp/fs42-i-*` stage, then exclusively creates `/stage/transient` beneath it as a mode-0700 directory before constructing the Bubblewrap command. It holds the transient directory's no-follow descriptor through the child run and verifies the same device/inode identity and permissions afterward. A missing source is created; any pre-existing file, symlink, or directory fails closed. The outer run lifecycle removes the entire stage after success or launch failure. The ordinary preflight retains its prior tmpfs behavior and does not create `transient`. `./director isolation preflight --profile native-single-run` exercises only the C1 probe and mount profile—never catalog or scheduling code—and must pass from an external SSH shell before the public validation gate may be removed. C1 performs a final native-semantics cross-channel exclusion check and fails closed on a collision; it does not retry, reseed, or relax exclusions, even when a future deterministic conflict-resolution design might find a valid schedule.

There is deliberately no apply or rollback command. Archiving requires the exact proposal ID as confirmation and only moves Director-owned proposal files.

### Immutable application candidates (checkpoint 1)

`./director schedule prepare PROPOSAL_ID` runs the same genuine, isolated two-run
validation as `schedule validate`, with an additional bounded candidate export.
It never applies a schedule, stops playback, or writes the live database. The
normal validation command is unchanged. Preparation has the same enabled gate,
normal-SSH admission, lock, confinement, preservation, and cleanup requirements.
It requires a clean tracked worktree and records the exact Git revision; new
untracked Python implementation files in the native/Director packages are rejected.

This first checkpoint supports one date-slot directive on one ordinary channel,
without assignments or exclusions. It requires an unambiguous mapping to existing catalog
semantics and unchanged file/break/chapter metadata, including negative chapter
attestations. Ambiguous catalog mappings, catalog additions/removals, metadata
changes, and a no-change replacement are rejected. A protected, exact historical
host-path row and its freshly allocated `/media` staging alias may map to the
same existing live ID, only with identical complete semantics. Metadata is
resolved through an unambiguous canonical media identity; missing exact-path
metadata on a staging alias is not treated as missing live metadata. Conflicting
or ambiguous metadata aliases fail closed. Native numeric cache timestamps are
retained as numbers, without coercion or exclusion from equivalence checks.
Catalog IDs are translated to
existing baseline IDs; known media references are translated through the existing
confinement mapper, with logical-equivalence checks. Unknown auxiliary sandbox
references fail closed. Retained AutoBump history remains protected and is not
copied into the replacement range; generated AutoBump remains forbidden.

Each accepted worker's schedule is exported in memory before stage cleanup.
The two exports must match. A candidate is published only after reproducibility,
all source-stability checkpoints, both cleanups, and durable successful report
publication. No failed staging database is retained. Candidate failure never
makes an ordinary validation silently eligible for application.

The date-slot directive is an **hour-of-block-selection rule**, not an exact
feature start appointment. `SlotReader.get_slot()` uses the current block-start
hour; `LiquidSchedule._fluid()` advances to the previous block's end before its
next decision. A crossing block can delay or entirely skip an hour. Evidence
therefore requires at least one generated ordinary `LiquidBlock` starting in the
requested hour, and every block starting in that hour must reference the requested
catalog tag and have matching feature playback paths. Presence in a title, an
earlier block merely overlapping the hour, or one matching block among wrong-tag
blocks is not proof. Fallback output with a different tag is rejected. Clip/loop/
web effect shapes and ambiguous/nonexistent DST times are unsupported for now.

Feature intervals come from the actual generated playback plan: block start plus
the cumulative entry durations, including opening material and intervening
commercials. They are scheduled times, not measurements of player execution.
The inspector lists each feature segment's start/end rather than pretending the
feature starts exactly on the hour or runs uninterrupted. Nonpositive, nonfinite,
unbounded, or block-overrunning plan durations fail closed. No exact-start timing
requirement is introduced; an exact-start product would need a separate operator
decision and is not implemented here.

`./director schedule inspect-candidate SHA256` reads only the private candidate
and its bound immutable validation report. It verifies size, ownership, mode,
single-link regular-file identity, schema, semantic checks, content-addressed
digest, and report binding. It displays the digest, proposal/run/revision, actual
replacement boundary/seam/horizon/end, requested channel/date/hour and tag digest,
and scheduled feature intervals. It does not print media names, paths, titles,
catalog rows, or raw errors. There is no approval or application command yet.

Candidates live at `runtime/director/candidates/SHA256.json`, mode 0600 beneath
private owned directories. Publication uses descriptor-relative no-follow access,
exclusive temporary creation, fsync, and atomic no-replace rename. Limits are
16 MiB per artifact, 10,000 replacement rows, 50,000 scanned rows per table and
catalog mappings, and 100 directory entries; capacity exhaustion rejects rather
than deleting older candidates. Temporary publication files are removed on caught
failure/interruption; an uncatchable crash may leave a private `.pending-*` file,
which inspection cannot accept. There is no automatic crash cleanup/resumption.

Bindings include the exact canonical proposal digest, typed policy digest, clean
code revision, logical configuration/database/media fingerprints, physical
configuration and media-manifest fingerprints, typed schedule/catalog semantics,
metadata digests, replacement range, effect evidence, and successful report and
normalized-run digests. The private artifact contains the necessary schedule data;
only the operator summary is redacted. Media fingerprints retain existing metadata
identity semantics, not full media-byte hashing. A content hash is not a signature
against a malicious process running as the same Unix user.

An artifact is evidence for a future approval, not permission to mutate live state.
This checkpoint implements no writer, stale-input-at-apply check, backup, recovery,
rollback, service coordination, or race-closing mechanism. Those remain separately
reviewed work. A successful historical validation cannot supply missing schedule
rows; preparing a candidate necessarily regenerates and validates both runs.

### First preservation failure detail

C1 response v4, C2 result v4, and immutable validation report v6 carry a required
nullable `preservation_detail` inside the existing C1 diagnostic. A non-null
detail has exactly `helper`, `category`, and `content_scope`. All values are
fixed allowlisted identifiers, never exception text or data. The scope is
`retained` or `generated` for media/reference checks and null for other checks.
Null means no instrumented preservation failure was observed, not that
preservation succeeded.

The original domain/phase/code remains the primary diagnostic. The detail
identifies the **first preservation failure**, which can be secondary: for
example, a scheduler failure followed by a sequence insertion failure. It
does not assert that the primary failure was caused by preservation. Later
restoration, rollback, or close failures do not overwrite either the primary
diagnostic or the first detail. Media scope is added while unwinding the
retained/generated check, without replacing the helper or category.

| Helper | Fixed categories |
| --- | --- |
| `restore_sequence_state` | schema_mismatch, schema_check_failed, delete_failed, insert_failed |
| `_restore_sequences` | open_failed, commit_failed, verification_failed, rollback_failed, close_failed |
| `_verify_final_preservation` | commit_failed, foreign_key_check_failed, foreign_key_mismatch, rollback_failed, close_failed |
| `_execute_native_single_run` | rollback_failed, close_failed (catalog reconciliation cleanup) |
| `assert_retained_history` | schedule_check_failed, schedule_mismatch, catalog_check_failed, catalog_mismatch |
| `assert_protected_state` | check_failed, mismatch |
| `coverage_report` | check_failed, gap, overlap, gap_and_overlap |
| `_validate_final_cross_channel_exclusions` | check_failed, collision |
| `_playback_representations` | plan_invalid, reference_failure |
| `_validate_playback_representations` | check_failed, descriptor_invalid, autobump_selected, stream_rejected, media_validation_failed |

Categories are captured at the operation or failed predicate. Query/check
errors are not interpreted as proven mismatches. Sequence verification uses
the existing exact typed-row comparison. The new versions are strict: old
workers are not admitted as current responses, and malformed, unknown, or
extra detail fields are rejected. Frozen schemas and retained reports are
not rewritten; older reports remain readable with their original schemas.
All preservation checks, media confinement, selected-AutoBump rejection,
isolation, and staging disposal remain in force. No failed staging database
is newly retained, and no new recovery or scheduling behavior is introduced.

## Candidate-export failure diagnostics

Candidate-export failures retain the primary `normalization_failed` diagnostic.
When candidate export is the failing operation, reports v7 and later carry
`candidate_export_category`: an explicitly allowlisted `ArtifactError` code,
or `candidate_export_unknown` for an unexpected exception or unknown code.
Ordinary normalization failures have a null category. The category appears in
the immutable JSON report, its text rendering, and CLI output; exception text,
paths, rows, and metadata values are never included in this diagnostic.
Cancellation remains cancellation, and a later cleanup failure does not replace
the original export failure.

Report schemas v1–v7 remain frozen and retained reports remain readable, including
valid candidates bound to older reports. New reports use v8; older software that
does not support v8 cannot read them. This diagnostic does not change candidate
acceptance, publication gates, cleanup, isolation, or live-application support.

Report v8 distinguishes the existing catalog rejection predicates with fixed
categories:

- `candidate_catalog_no_semantic_match`: no complete match and no matching catalog fields.
- `candidate_catalog_metadata_association_mismatch`: catalog fields match, but associated metadata does not.
- `candidate_catalog_mapping_ambiguous`: multiple complete matches without a same-ID match.
- `candidate_catalog_baseline_id_unmapped`: at least one baseline catalog ID is not represented.
- `candidate_catalog_alias_pair_invalid`: duplicate mappings fail the protected-original/new-alias shape or provenance checks.
- `candidate_catalog_alias_path_invalid`: the alias paths are not the expected sandbox paths, or the original was already a sandbox row.

The old `candidate_catalog_changed` category remains supported for retained
reports. Classification occurs only after an existing rejection predicate
fires; matching catalog fields alone never authorizes a mapping. All baseline
catalog IDs and complete metadata equivalence are still required. These
categories do not prove live data changed and do not permit catalog writes.

### Timestamp-only reference mapping

Candidate export tries exact catalog matches first. Only when none exists may
a rebuilt ordinary-media row map to a unique existing baseline row with every
canonical field and associated metadata equal except catalog `created_at` and
`updated_at`. Rebuilt rows must belong to the affected channel, have IDs above
the baseline catalog maximum, and have both paths in their expected sandbox
form, as production reconciliation creates them. Ambiguous fallback matches
are rejected, without a same-ID preference.

Ordinary catalog timestamps must be valid naive SQLite/ISO datetime text (space
or T separator, optional one-to-six fractional digits); nullable baseline
timestamps remain null. Rebuilt rows using the fallback require non-null
timestamps. Numbers, booleans, blobs, invalid dates, and timezone-bearing text
are rejected, not coerced.

Each candidate mapping contains the baseline row's complete semantics and
original timestamps, never the staged timestamps. Only schedule references are
translated; no catalog updates are exported. Descriptor matching, complete
baseline-ID coverage, exact protected originals, and alias provenance checks
remain unchanged. General normalization, two-run comparisons, database
fingerprints, source-stability checks, metadata equivalence, timing proof, and
approval bindings remain timestamp-sensitive and unchanged. This exception
does not implement or authorize live application.
