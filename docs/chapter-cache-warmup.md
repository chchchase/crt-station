# Chapter-cache warm-up

This is a dedicated, resumable maintenance operation for chapter metadata. It
does not generate schedules or rebuild normal catalog rows. Its only logical
database changes are completed `chapter_points` attestations and retirement of
confirmed-unavailable legacy empty rows; SQLite transaction and sidecar changes
are unavoidable bookkeeping.

## Admission and output

Run the module only from a normal SSH session. Any `CODEX_` environment marker,
Codex process ancestor, missing `sshd` ancestor, malformed proposal ID, or
missing exact confirmation count rejects before locks, service inspection,
backup work, media access, or mutable database access. The default invocation
is a read-only provisional plan:

```text
env/bin/python -m station_director.chapter_cache_warmup PROPOSAL_ID
```

Planning never opens the live database with SQLite. It copies a stable main and
WAL generation into a private directory, retains an unopened raw copy, and
opens only a second private working copy. A journal is rejected. SHM without
WAL is rejected. Identities before copying, after copying, and after provisional
counting must agree; otherwise the provisional counts are discarded. Committed
WAL-only rows are therefore included without allowing a read-only SQLite open
to alter live SHM.

Execution requires `--execute` and eight confirmation values printed by the immediately
preceding plan: `eligible`, `missing`, `legacy_empty`, `current_empty`,
`unavailable_empty`, `attestations`, `probes`, and `short_media`. These are
recounted authoritatively after verified service shutdown; any mismatch rejects
before backup or writes. The plan also reports legacy-nonempty and versioned-row
counts. Execution reports every fixed probe-failure category, including zeros.
Output is one bounded JSON object containing only fixed status/category names
and integer counts. It never contains configuration values, media names, paths,
probe output, or exception text.

The reviewed inventory had 8,880 extension-supported files in the media tree
and 4,479 root `file_meta` cache rows. Those counts describe different sets:
the first is a filesystem extension inventory, while the second is existing
cache state and can include neither every projected candidate nor only projected
candidates. The audited Channels 2 through 7 projection contained 7,801
deduplicated eligible identities. Candidate paths are projected and deduplicated
by their canonical media-root-relative identity; when multiple channels refer
to the same identity, it is analyzed once. The approved maintenance population
contains 3,322
missing rows plus 654 ambiguous legacy empty rows, or 3,976 identities requiring
fresh attestation, and 114 confirmed-unavailable legacy empty rows to retire.
Execution still requires the live authoritative totals to match the operator's
explicit confirmations.

## Backup and write protocol

The Director validation lock is acquired first, followed by a separate private
maintenance lock. `fs42.service` is then stopped and proved `inactive/dead` with
no main PID. The live main/WAL/SHM identity is captured without SQLite. A hot
rollback journal is rejected; neither planning nor rollback asks SQLite to
recover, alter, or remove a live journal.

Before the live database is opened, execution performs this exact sequence:

1. Copy the stopped main+WAL generation into an unopened raw private bundle and
   a separate private SQLite working copy.
2. Recount inputs and compare every explicit expected count.
3. Create the logical backup as a private, mode-0600, unpublished pending file.
4. Close and fsync it, then check integrity, foreign keys, required schema,
   counts, and complete logical equivalence against the private generation.
5. Reverify the unopened raw bundle and capture identity D, requiring all
   captured live identities A through D to agree.
6. Publish the verified backup with Linux no-replace rename and directory fsync.
7. On the first envelope migration only, write and fsync a private pending pin,
   publish it with no-replace rename, and fsync the directory. The pin names the
   verified logical backup, its complete logical digest, and a separate bounded
   digest of the exact pre-envelope `chapter_points` rows.
8. Reverify the raw bundle, capture identity E, and require A=B=C=D=E.
9. Only then open the live database as the controlled chapter writer.

The native chapter writer independently refuses to publish any versioned
envelope until the private pin and named baseline backup have safe identities.
Thus an ordinary catalog refresh between code deployment and the first approved
maintenance invocation can leave a chapter row missing, but cannot get ahead of
the permanent pre-envelope rollback point.

Failure before backup publication leaves no permanent pin. A backup published
without a completed pin is an explicitly unreferenced artifact and is never
adopted automatically. The first verified pre-envelope backup remains pinned
across every partial resumption. At normal rest, retention contains that pinned
baseline and at most four unpinned rollback points. Admission at capacity
requires enough free space for one additional database-generation backup.

The additional rollback point is published and directory-fsynced, then reopened
immutably for integrity and schema checks before retention changes. Only after
that verification may the oldest verified unpinned point be unlinked and the
directory fsynced again. The pinned baseline and newest point are never pruning
candidates. A crash between publication and pruning can therefore leave exactly
six recognized points; the next invocation verifies the baseline, newest point,
and deletion candidate and completes that transition before creating another
backup. More than six points, unsafe identities, pending files, SQLite sidecars,
unknown entries, or an over-cap set without a valid pin fail closed. A deletion
or directory-fsync failure prevents chapter writes and destructive rollback.
The same transition permits rollback to publish its required current-generation
point when retention initially contains five points.

Free space covers four database-generation copies, including the temporary
extra rollback point, plus the bounded verification spool and fixed reserve.
No file is overwritten. Pending or otherwise ambiguous artifacts block another
run rather than being silently removed.

## Analysis and resumability

Every media component is traversed relative to a held media-root descriptor
with `O_NOFOLLOW`; the final object must be a regular file. The held descriptor
is passed to one fixed `/usr/bin/ffprobe` invocation through `/proc/self/fd`.
Only local `file,pipe` protocols are allowed. Standard error is discarded,
standard output is capped at 4 MiB, each probe has a 30-second deadline, and its
process group receives bounded TERM/KILL cleanup. There is no networking,
rendering, decoder fallback, MoviePy, or FFmpeg invocation.

Media shorter than five minutes receives a completed empty attestation without
a probe. A successful probe with no chapters also receives an explicit empty
attestation. Launch, timeout, signal, nonzero-exit, malformed-output, invalid
chapter, and cleanup failures write nothing. Each successful result is committed
in its own transaction after the held descriptor's size and nanosecond mtime are
rechecked. The version-1 envelope records only the method, exact size/mtime
identity, and chapter list. Native readers validate and unwrap it, so an envelope
never reaches callers that expect the historical raw list.

Legacy nonempty lists remain readable. Every eligible legacy empty list is
ambiguous and is re-analyzed; every missing eligible row is analyzed; a legacy
empty row is deleted only when descriptor-relative traversal confirms that its
canonical media file is absent. Any other legacy-empty population is rejected.
Future ordinary scans use the same completed-result type, so analysis failures
cannot recreate false empty attestations.

The command stops admitting new analyses after 6,960 seconds and reserves 240
seconds within a 7,200-second control window for post-write integrity and
authorized-change checks plus service-state finalization. Sixteen analysis
failures also end an invocation as a clean partial. A clean verified partial is
resumable and automatically restarts the service if it was initially running.
Any uncertain integrity, unauthorized change, service state, or cleanup leaves
the service stopped for operator review. SIGKILL and power loss cannot execute
cleanup; the durable per-result transactions and pre-write backup make a later
invocation resumable.

At 30 seconds per probe, 3,976 full probe attempts have a strict serial upper
bound near 33.1 hours, so multiple two-hour invocations may be required. Actual
time should be materially lower for quick chapter-only probes and short media.
Temporary space is bounded by the live main+WAL generation's raw copy, working
copy, pending logical backup, one disk-backed verification spool, and 100 MiB
reserve (plus 8 MiB fixed overhead). The published rollback point reuses the
pending backup's space. The admission check rejects insufficient space.

## Rollback

`--rollback-baseline` is a separate execute-style operation and requires the
same canonical proposal and exact current counts. It acquires both locks,
verifies service shutdown, rejects a live journal, validates the immutable pin
and baseline, and publishes a fresh verified rollback point for the current
generation before opening the live database. One controlled transaction then
replaces only `chapter_points` with the pinned pre-envelope rows. All other
tables and schema are checked unchanged. The old backup is opened separately as
immutable input, never installed as a database main file, so it cannot be
combined with newer WAL, SHM, or journal state.

After any interruption or failure, do not remove pending files or restart the
service until the fixed failure category, service state, backup directory, and
database integrity have been reviewed. If those checks are clean, rerun planning
from normal SSH and supply the new exact counts. If integrity or authorized
change verification is uncertain, retain the stopped service and every backup
artifact for manual recovery.
