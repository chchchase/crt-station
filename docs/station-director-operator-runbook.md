# Supervised Station Director: operator runbook

Reviewed at `6c3c6d3fc638dc2c63294d2626e77a306d13cabd`. Run operational
commands from normal authenticated SSH, in `/home/chaseanderegg/FieldStation42`.
These instructions are not authorization to execute them unattended.

## Plan, prepare, inspect

Replace uppercase placeholders with reviewed values; do not paste them literally.
Use the exact canonical inventory series identifier, not an assumed display title.
The application milestone supports one date-slot on one ordinary channel, without
assignments or exclusions, and requires an actual schedule change.

```sh
./director schedule plan --week WEEK_START_DATE --source direct --seed 4242 \
  --date-slot 2 SLOT_DATE 20 "EXACT_SERIES_IDENTIFIER"
./director schedule show PROPOSAL_ID
./director schedule prepare PROPOSAL_ID
./director schedule inspect-candidate CANDIDATE_SHA256
```

`prepare` runs genuine isolated two-worker validation and publishes a candidate
only after agreement, source-stability checks, and cleanup succeed. Ordinary
`./director schedule validate PROPOSAL_ID` validates but does not prepare an
application candidate. Planning success alone proves neither validation nor effect.
Inspection supports v1/v2; application requires v2. Review the replacement range,
feature intervals, selection resets/increments, count/timestamp changes, and zero
unaffected-channel mutations. No new catalog entries may be applied.

## Timing and bumper semantics

Station time is `America/Los_Angeles`; proposal weeks begin Monday at 06:00.
A date-slot selects the requested series for blocks **starting in that hour**.
At least one such block must exist, and every such block must match. A crossing
block, title mention, or feature merely overlapping the hour is insufficient.
Feature playback need not begin at exactly 20:00: commercials and multiple
segments affect the actual plan intervals shown by inspection. These are planned
times, not measured player execution. Inspect the complete regenerated/replacement
range; the operation is not limited to that one hour.
Application replaces affected-channel blocks whose starts are at or after the
candidate's `proposal_boundary` and before `replacement_end`. Earlier/crossing
blocks remain protected; `regeneration_start` identifies the validated generation
seam. Use the inspected candidate's actual bounds, not an assumed one-hour range.

Channel-wide `use_bumpers=false` suppresses ordinary bumpers and AutoBump, including
filler, while retaining commercials when `commercial_free=false`. Do not change
configuration merely to reuse a prepared candidate; configuration is bound.
Newly selected AutoBump remains forbidden; retained historical descriptors are
handled separately. Do not bypass confinement or render AutoBump for validation.

## Maintenance and approval

During preparation, avoid competing operator changes; normal playback need not
be stopped, and source drift can cause preparation to reject. Before application,
establish a cooperative single-operator freeze: no competing scheduling,
catalog/chapter maintenance, WIO commands, configuration edits, media transfers/
importers, or other writers beyond the services the application quiesces. Not all writers
honor Director locks. Keep exclusion through final service restoration. Recheck
activation/writer inventory after deployment changes; process scans are not proof
of exclusion. Root-cron absence was operator-confirmed for this installation, not
a permanent guarantee.

Both installed maintenance guards must remain intact and effective. Application
checks them; it does not install them. Never remove another operation's marker or
override a failed guard. Supported prior service states are both running, only
fs42 running, or both inactive. The writer locks Director then chapter maintenance,
durably records prior states and a persistent marker, stops crtstream then fs42,
and verifies empty cgroups. Restoration is fs42 first, to the recorded prior states.

Admission needs a clean exact candidate revision and unchanged bound inputs.
A later commit, even documentation-only, cannot use an older candidate for
application at the new revision: prepare a fresh candidate there. Recovery and
rollback also require their candidate's bound revision; never upgrade during an
unresolved operation. Pushing the same commit does not change its binding.
The earlier proposal boundary
or regeneration seam must be at least 30 minutes ahead. Mutation/commit have further
budget and future-time checks. Allow a supervised 15-minute operation window plus
recovery time; failure can leave a longer outage. Markers never expire automatically.

After reviewing the exact digest and separately approving the outage:

```sh
./director schedule apply CANDIDATE_SHA256 --approve CANDIDATE_SHA256
```

Application verifies a durable private baseline backup, then atomically replaces
only the approved schedule range and explicit count/updated_at values. Historical
playback, other channels, other catalog fields, and sequence/WIO state are protected.
Completion requires `database_outcome=applied` and `service_outcome=restored`.
Receipts/backups reside under `runtime/director/applications/CANDIDATE_SHA256/`.
Historical success does not establish current live-database equality.

## Interruption, recovery, rollback

On failure, retain all records and the marker; inspect database and service outcomes
separately. Do not rerun apply, manually start services, delete a marker, replace the
database, upgrade code, or use force overrides. Using the bound revision:

```sh
./director schedule recover CANDIDATE_SHA256 --approve CANDIDATE_SHA256
```

Recovery classifies the transaction and restores services; an unknown database
state remains inhibited. After the durable restoration seal, it resumes restoration
without replaying writes—even after marker removal. An aborted or rolled-back
candidate requires fresh preparation for another application. A completed repeat
returns a receipt; it is not a fresh database verification.

Separately approved rollback is:

```sh
./director schedule rollback CANDIDATE_SHA256 --approve CANDIDATE_SHA256
```

Rollback requires exact applied post-state, unchanged inputs, the verified backup,
and the same future-window/exclusion requirements. Ordinary service writes can
make it unavailable. It must refuse subsequent changes, not overwrite them.
Rollback restores logical state, not byte-identical SQLite pages. Use `recover`
for incomplete restoration, not rollback. A disposable guard test has its own
recovery script; never substitute schedule recovery for guard-test recovery.

## Recorded checkpoint

Candidate `90f42a6efc0a88b25acaad231d0e1cf40b83ed4549e75205d7374f09edb5a734`
completed application and recorded restoration of both services. Read-only closeout
verified its receipt chain and backup byte digest, with no pending application or
maintenance marker. This is historical evidence, not an instruction to reapply.
Older sections of [the implementation history](station-director.md) describe
earlier incomplete milestones; use the current CLI and the restrictions above.
