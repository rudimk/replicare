# replicare — MySQL Engine Implementation Plan (v1 draft)

**Status:** DRAFT — pre-review. Mirrors the structure of `.sisyphus/postgres-plan.md`; to be
hardened through Sisyphus + Momus passes before execution.
**Scope:** A second engine — **MySQL source → MySQL target** — implementing the *existing*
`engine.Source`/`engine.Sink`/`engine.ApplyTx` interfaces (`internal/engine/engine.go`). The
engine-neutral pipeline (`internal/copy`, `apply`, `delta`, `reseed`, `pipeline`, `state`,
`observability`) is **reused unchanged**; MySQL is additive per the CLAUDE.md §5/§11 decoupling.
Section refs like §3.4 point at `CLAUDE.md`. The **single-engine rule (§6)** stands: a MySQL sync's
source and all targets are MySQL — never cross-engine.

## Guiding constraints carried over from CLAUDE.md (unchanged, engine-neutral)
The MySQL engine inherits every §1 non-negotiable **as-is**: no privileged change stream (no
**binlog** access — the MySQL analog of "no WAL"), least privilege, high performance, deep
observability, faithful transport / never transform (§1.7), and old-source tolerance (§1.6). Trigger
CDC, PK-only dirty-key capture, dirty-key-set consumption (§3.1–3.4), at-least-once idempotent apply
(§5), FK connected components as scheduling units (§8.1), Postgres-backed StateStore (§9),
single-active daemon (§9) — all reused.

---

## 0. The crux: three things MySQL does NOT have that the Postgres path assumes

The interface method **signatures** are engine-neutral, but their doc comments and the Postgres
implementation lean on three Postgres features MySQL/InnoDB lacks. Every hard decision in this plan
traces back to one of these. **This section is the review's primary target.**

1. **No `COPY`.** Postgres streams `COPY … TO/FROM STDIN` through an `io.Pipe` for copy, re-read, and
   staging-load. MySQL's streaming bulk path is **`LOAD DATA LOCAL INFILE`** fed from a reader
   (go-sql-driver/mysql's `RegisterReaderHandler` → `LOAD DATA LOCAL INFILE 'Reader::name'`). This
   preserves the `io.Reader`/`io.Writer` streaming contract, but:
   - it uses **MySQL's own tab-delimited escape format** (`\t` sep, `\n` terminator, `\N` = NULL,
     backslash-escaping) — a *different* "faithful text" serialization than Postgres text `COPY`,
     defined and locked in MM4;
   - it requires the server system variable **`local_infile=ON`** and the client capability flag —
     a **configuration prerequisite, not a grant** (documented like "capture writes to the source").
     If `local_infile` is OFF and ungrantable, the fallback is **multi-row batched `INSERT`** (slower,
     still faithful) — decided in MM4.

2. **No deferrable constraints, no `SET CONSTRAINTS`.** Postgres defers FK checks within a component
   transaction (`SET CONSTRAINTS ALL DEFERRED`) so a cyclic component commits atomically. InnoDB
   has **no** deferral; the only lever is session-local **`SET FOREIGN_KEY_CHECKS=0`**, which
   *disables* checking rather than *deferring* it to commit. This forces a real decision (MM5b/MM4):
   topo-ordered apply covers acyclic components with **checks left ON**; cyclic/self-ref components
   either (a) rely on the **engine-neutral retry fallback** (§3.3) — accepting **weaker per-pass
   atomicity** than Postgres for cyclic components — or (b) run the component pass with
   `FOREIGN_KEY_CHECKS=0`, preserving per-pass atomicity but permitting a transiently-inconsistent
   commit. This is a genuine correctness/consistency tradeoff, decided explicitly in MM5b.

3. **No `ctid` / stable physical row locator.** Postgres' `ctid` block-range fallback handles
   pathological key distributions. InnoDB tables are **clustered on the primary key**, so **keyset
   PK-range chunking already scans in physical order** — the ctid fallback has *no MySQL analog and
   needs none* for keyed tables. No-PK/unique tables are **skipped + warned** exactly as in Postgres
   (§3.1), so there is no residual case requiring a physical fallback. Documented, not implemented.

A fourth, softer divergence pervades fidelity (MM1/MM4/MM9): **MySQL transcodes character data and
enforces `sql_mode`/zero-date rules**, so "faithful transport" needs a MySQL-specific session
canonicalization (§4.2 analog) — see MM1.

---

## Goals & non-goals
**Goals (v1 MySQL):** trigger-based CDC (no binlog), PK-only capture, dirty-key-set consumption
(§3.1–3.4); resumable chunked initial copy + delta streaming to a **single target** (fan-out
interface present, not hardened), one-way; **faithful transport, never transform** (§1.7, §4.2) for
MySQL types/charsets; reuse the Postgres-backed StateStore (§9); reuse the full observability
contract (§10); run against an **old source MySQL → modern target MySQL** (§1.6 analog).
**Non-goals (v1 MySQL):** multi-master, HA/leader-election, live DDL, value transforms, cross-engine,
Helm, a MySQL-backed StateStore (state store stays Postgres — see the wrinkle in MM2), **binary
LOAD DATA**, and **MariaDB** unless explicitly folded in at MM0 (fork divergence — scoped there).

## Definition of Done (MySQL engine)
Feature-complete at **end of MM8**; **shippable only after the MM9 matrix gate passes**. Done when:
given a YAML config with a MySQL source + MySQL target(s), replicare can validate the pair
(pre-flight), install least-privilege trigger capture, perform a resumable consistent initial copy of
an FK-connected multi-table schema **including self-referential and cyclic FKs (per the MM5b
decision)**, then stream inserts/updates/deletes/PK-changes to convergence — crash-safe, observable
across all three channels (reusing F2), bounding source footprint under a stalled **InnoDB history
list** (the xmin-horizon analog) — **verified by the automated multi-version MySQL integration
matrix (MM9).**

---

## Cross-cutting foundations (MySQL adaptations of F1–F4)

- **MF1 — Config block (locked in MM0.5).** Add a **typed `mysql:` engine block** to the existing
  neutral envelope via the engine registry — no change to the neutral schema (proves §11's
  extension-point claim). Block owns/validates: connection shape (host/port/**database** — MySQL's
  schema unit — user/password/TLS), **selection as `db.table` globs**, a **`local_infile` capability
  hint**, and MySQL CDC/copy tuning (worker/batch/chunk, LOAD-DATA-vs-INSERT bulk mode). The
  **single-engine rule (§6)** is already enforced by neutral validation; add MySQL to its engine set.
- **MF2 — Observability contract: REUSED AS-IS.** The F2 registry (metrics/spans/log events) is
  engine-neutral; the MySQL engine emits the **same** series against it. No new contract. MM6 verifies
  MySQL emission, exactly as Postgres' M6 did. (This is the cheapest milestone and the strongest
  evidence the core is neutral.)
- **MF3 — Old-source MySQL floor (decided in MM0).** Commit to a specific oldest source MySQL in CI
  and justify it against: generated columns (5.7+), JSON (5.7+), multiple-triggers-per-event (8.0+),
  `utf8mb4` defaults (8.0), and `information_schema` performance (5.7 is slow). Candidate floor:
  **5.7 source → 8.0/8.4 target** (with 5.6 explicitly evaluated). **Decide MariaDB in or out here**
  — if in, list the concrete dialect forks that need handling; if out, say so and why. If the chosen
  floor is unavailable in CI, **list which fidelity claims become unverified** — never silently raise.
- **MF4 — Schema versioning: REUSED runner, MySQL DDL.** Reuse the shared migration-runner
  abstraction (F4) for the **source `replicare` schema** (a MySQL *database*), carrying a
  `schema_version`, idempotent + in-place, **preserving in-flight deltas/cursors** across upgrades
  (source DB we may not own — migrate, never drop-recreate). The **StateStore schema stays Postgres**
  and reuses Postgres' existing F4 runner unchanged.

---

## Testing strategy (every milestone)
- **Multi-version MySQL docker harness (MM0):** the MF3 old source + a modern target. Integration
  tests run on the matrix, gated exactly like the Postgres `REPLICARE_INTEGRATION=1` suite.
- **MySQL type-fidelity corpus (from MM1):** int/decimal/float/double/bit/char/varchar/text/blob/
  binary/enum/set/date/**datetime/timestamp (tz semantics)**/**JSON**/generated(virtual+stored)/
  **`0000-00-00` zero-dates**/**charset mixes (latin1↔utf8mb4)**/auto_increment — with round-trip
  expectations across the version gap. Zero-dates and charset transcoding are the headline fidelity
  risks and get dedicated cases.
- **Named property/race tests (from MM5a):** the same keystone set as Postgres (commit-order hazard,
  delete-by-id read-and-clear race, crash-between-apply-and-track, coalescing).
- **Fault injection (from MM4/MM5*):** kill mid-copy/mid-stream; target down; slow-but-reachable
  target (backpressure); **long-running source transaction inflating the InnoDB history list**
  (xmin-horizon analog).
- A milestone is **not complete** until its acceptance tests pass on the MySQL matrix in CI.

---

## MM0 — Foundations: MySQL engine skeleton, harness, floor
**Goal:** A registered but stubbed MySQL engine, a multi-version harness, and the MF3/MariaDB
decisions — proving the seams before building.
**Deliverables:** `internal/engine/mysql/` package; driver decision (**`go-sql-driver/mysql`**,
pure-Go so the static-binary/CGO-free property holds); `Engine` factory + `init()` registration in
`cmd/replicare/engines.go`; `ServerVersion` probe (5.6/5.7/8.0/8.4 → version int); **docker-compose
MySQL harness** (MF3-old source + modern target, non-default ports, ephemeral); **MF3 floor +
MariaDB-scope decision doc**; confirm (via the MM-inventory) that the neutral pipeline compiles
against a MySQL engine with method stubs.
**Acceptance:** CI green with the MySQL package building; `replicare version` unaffected; harness
brings up old + modern MySQL; floor/MariaDB decision committed in writing; registry `Get("mysql")`
returns the engine.
**Depends on:** none (the neutral core already exists).

## MM0.5 — MySQL config block (MF1)
**Goal:** Lock the MySQL connection/selection/tuning surface before anything consumes it.
**Deliverables:** typed `mysql:` block parser+validator wired through the registry; `db.table`
selection globs; TLS modes (`disable`→`verify-full` mapped to the driver's `tls` param, incl.
old-server TLS note); secret resolution (inline + env) reusing the neutral layer; `local_infile`
capability hint; bulk-mode tuning knob (LOAD DATA vs batched INSERT).
**Acceptance:** golden-file parse+validate of a full MySQL config; env-override precedence test;
invalid MySQL configs rejected with actionable errors; the single-engine rule rejects a MySQL↔
Postgres sync.
**Depends on:** MM0.

## MM1 — Connectivity, introspection & pre-flight (MySQL)
**Goal:** MySQL DB pair → validated, classified replication report; faithful, secure sessions.
**Deliverables:**
- **Session canonicalization (the §4.2 analog), pinned on every source-read and target-write conn:**
  `time_zone='+00:00'` (deterministic `TIMESTAMP`), a **locked `sql_mode`** (notably deciding
  zero-date handling — see below), **charset strategy for byte-faithful transfer** (candidate:
  `SET NAMES utf8mb4` with per-column awareness, or a binary-safe transfer path — the charset
  transcoding risk is called out for oracle in MM4), `NO_AUTO_VALUE_ON_ZERO` (faithful auto_increment
  = the `OVERRIDING SYSTEM VALUE` analog), and `sql_notes`/`unique_checks` left untouched. All are
  **session-level `SET`s, no special privilege**, changing formatting only — never data.
- **Zero-date decision (faithful vs strict):** a very old source may hold `'0000-00-00'`; a modern
  strict-mode target rejects it. Per §1.7 we never transform. Decision (MM1, revisited MM9): pin the
  **target write session's `sql_mode`** to permit the source's zero-dates (canonicalizing the
  *session*, not the *data*) so verbatim values land; if that is impossible, **fail loud** — never
  coerce to NULL/epoch.
- **TLS per connection** (verify-full + disable), secret resolution wired.
- **Introspection via `information_schema`** (COLUMNS incl. `EXTRA` for generated/auto_increment,
  KEY_COLUMN_USAGE + TABLE_CONSTRAINTS + STATISTICS for PK/unique, REFERENTIAL_CONSTRAINTS +
  KEY_COLUMN_USAGE for FK edges), **version-tolerant** behind the MM0 version probe; **`db.table`
  selection** globs.
- **FK connected components** (reuse the neutral component computation; the MySQL engine only supplies
  the FK edge list) + **giant-component + dangling-FK-to-excluded warnings** (§8.1).
- **Cyclic-FK classification, MySQL cases (diverges from Postgres — no DEFERRABLE case exists):**
  **(a) nullable** cyclic/self-ref FK → passes through to MM4's NULL-then-fill; **(b) NOT NULL**
  cyclic/self-ref FK → **the MM5b/MM4 decision applies** (retry-fallback vs `FOREIGN_KEY_CHECKS=0`
  scoped to load) — pre-flight **flags** it and records which strategy will be used; there is no
  `SET CONSTRAINTS DEFERRED` middle case.
- **Pre-flight type-compat check** for MySQL type pairs (identical/widening/risky/incompatible →
  block-on-incompatible, warn-on-risky), including charset/collation-change and zero-date risk
  classification; **no-PK/unique tables skip+warn** (§3.1).
- CLI `validate` works against a MySQL config.
**Acceptance (matrix):** `validate` lists MySQL tables/keys/components correctly; hub table →
giant-component warning; FK-to-excluded → dangling warning; nullable vs NOT NULL cyclic FKs
classified per the MySQL cases; a MySQL mismatch corpus classified correctly (incompatible blocks,
risky/charset/zero-date warns); PK-less table skipped+warned; `verify-full` + `disable` both connect;
env-resolved secret connects; a `'0000-00-00'` value round-trips faithfully under the pinned target
`sql_mode` (or fails loud if impossible); emits the F2 metrics/spans.
**Depends on:** MM0.5.

## MM2 — StateStore integration (REUSED — Postgres) + the operational wrinkle
**Goal:** Confirm a MySQL data sync runs on the existing engine-neutral Postgres StateStore.
**Deliverables:** **no new StateStore code** — the v1 StateStore is Postgres and is orthogonal to the
data engine (copy progress, cursors, events, `pg_advisory_lock` ownership, F4 runner all neutral).
Deliver the **integration wiring + documentation of the wrinkle**: *replicating MySQL still requires a
Postgres for state.* Decide + document whether that Postgres may be a small sidecar and note a
**MySQL-backed StateStore as an explicit future item** (interface already pluggable, §9).
**Acceptance:** a MySQL sync definition + copy progress + cursors + events persist and resume from the
Postgres StateStore across a restart; a second daemon cannot acquire a held MySQL sync (advisory
lock); the wrinkle is documented.
**Depends on:** MM0 (MM0.5 for sync/target config shape). *(Largely verification — kept as its own
milestone only to make the cross-engine-state dependency explicit and tested.)*

## MM3 — Trigger-based CDC capture (MySQL source machinery, + versioning)
**Goal:** Correct least-privilege trigger capture into per-table delta tables, version-upgradable.
**Deliverables:** `replicare` **source database** (MF4-versioned) with **per-table delta tables**
(`delta_id BIGINT AUTO_INCREMENT PK` — the delete-by-id handle and lag-only seq; op char; PK columns;
`captured_at DATETIME(6)`) + **per-(target,table) track tables** created lazily at stream time;
**AFTER INSERT/UPDATE/DELETE triggers** in MySQL syntax that record **PK-only** rows; **PK-update
enqueues BOTH old+new PK** (§3.1) via `OLD.pk`/`NEW.pk`; **pre-8.0 single-trigger-per-event
handling** (one trigger body per event, or the multi-trigger path on 8.0+); capture
**install/uninstall** (note the §12 asymmetry analog: MySQL `CREATE TRIGGER` needs `TRIGGER` priv,
`DROP TRIGGER` also needs `TRIGGER` priv — confirm and document the exact grant matrix, which differs
from Postgres' ownership-for-DROP rule); set-difference consume + **delete-by-delta_id** (§3.3);
**MF4 migration preserving in-flight deltas**; **least-privilege grants** (`TRIGGER`, `SELECT`,
`CREATE`/owner on the `replicare` db, `INSERT`/`DELETE` on delta/track; **no `SUPER`, no
`REPLICATION SLAVE`, no binlog**; §12 analog).
**MySQL bloat note (replaces PG autovacuum reloptions):** InnoDB has **no autovacuum**; delta churn
is reclaimed by InnoDB purge threads but the tablespace does not shrink without `OPTIMIZE TABLE`. The
capture-install step therefore sets **no per-table vacuum reloptions** (none exist); bloat control
moves entirely to MM5c (batched DELETE + optional per-table `OPTIMIZE`/partition-DROP).
**Acceptance (matrix):** INSERT/UPDATE/DELETE produce correct delta rows; PK-change enqueues old+new;
install/uninstall leaves the source clean; works under a non-`SUPER` role with exactly the documented
grants; a source-schema version bump preserves queued deltas; emits F2 metrics/spans. *(The
commit-order hazard test lives in MM5a where consumption exists.)*
**Depends on:** MM1, MM2.

## MM4 — Initial copy (MySQL: LOAD DATA, keyset-only, cyclic-FK by case, + backpressure)
**Goal:** Resumable, consistent, faithful bulk copy of an FK-connected MySQL schema.
**Deliverables:**
- **Chunking:** keyset PK-range default (composite/text/UUID-as-`BINARY(16)`/`CHAR` via row-value
  comparison), index-only boundary discovery over the clustered PK; **NO ctid fallback** (documented
  §0.3 — InnoDB is PK-clustered so keyset is already physical; no-PK tables are skipped+warned);
  native **RANGE/LIST partition-aware** per-partition copy as a free chunking win where present.
  **Boundary-discovery divergence (real, floor-dependent):** Postgres discovers balanced boundaries
  with `row_number() OVER (ORDER BY keys)` — a **window function MySQL lacks until 8.0**. At the
  5.7 floor the boundary scan must be rewritten window-free: candidate is a **user-variable row
  counter** (`SELECT keys FROM (SELECT keys, @rn:=@rn+1 rn FROM t, (SELECT @rn:=0) init ORDER BY keys) s
  WHERE (rn-1) % N = 0`), or **`LIMIT/OFFSET` sampling**, or the 8.0+ window path when the source is
  new enough. Locked in MM4; the user-variable path is the portable default.
- **Faithful text transport via `LOAD DATA LOCAL INFILE`** streamed source→target through `io.Pipe`
  (driver `RegisterReaderHandler`): the source read side (`CopyChunk`/`RereadCurrent`) **writes
  MySQL's tab-delimited escape format** (locked here: field sep `\t`, line term `\n`, `\N` NULL,
  backslash-escape of `\t`/`\n`/`\\`), the target `BulkLoad` issues the matching
  `LOAD DATA LOCAL INFILE … FIELDS ESCAPED BY '\\' TERMINATED BY '\t' LINES TERMINATED BY '\n'`. The
  **charset-fidelity mechanism is decided here** (oracle): transfer bytes without lossy transcoding
  (candidate: `character_set_client` pinned + column charsets honored, or a `_binary`/hex path for
  binary/blob) — never let MySQL silently transcode. **`local_infile`-OFF fallback:** multi-row
  batched `INSERT` (faithful, slower).
- **Target load:** empty-target **direct LOAD DATA** + **DELETE-range resume**; **TEMP staging +
  `INSERT … SELECT … ON DUPLICATE KEY UPDATE`** merge path for a non-empty target (the ON CONFLICT
  analog; upsert on PK — note ON DUPLICATE KEY reacts to *any* unique key, documented). **Never
  `REPLACE INTO`** (it deletes+reinserts, firing cascades/auto_increment — unfaithful).
- **FK-component parents-first** ordering, chunks parallel per table, components parallel (§8.1).
- **Cyclic/self-ref FK strategy (diverges from Postgres — no DEFERRABLE):** **(a) nullable →
  NULL-then-fill two-pass**; **(b) NOT NULL cyclic/self-ref → the MM5b decision** — either the initial
  load runs the component with **`SET FOREIGN_KEY_CHECKS=0` scoped to that load transaction**
  (session-local, does not alter the constraint; the standard MySQL bulk-load practice, cf.
  mysqldump) **or** it is **blocked at MM1 pre-flight**. This plan's **recommendation, pending Momus:
  allow `FOREIGN_KEY_CHECKS=0` scoped strictly to the initial-copy load of a cyclic component**,
  because (i) it is session-local and reversible, (ii) the copy is immediately reconciled by capture
  (§4), and (iii) blocking NOT NULL cyclic FKs would refuse common MySQL schemas. This is a **real
  departure from Postgres' "never disable constraints" stance** and is the plan's most important open
  decision — see Open Questions.
- Per-chunk progress → StateStore; **backpressure** (bounded queues + pool sizing tied to MF1 caps);
  **capture-first then copy** (§4).
**Acceptance (matrix):** copies a multi-table FK schema (composite & binary/text PKs) old→new with
byte-faithful values (fidelity corpus incl. zero-dates, charset mixes, JSON, generated-excluded);
cyclic-FK corpus copies to a **correct result** — nullable via NULL-then-fill; NOT NULL cyclic via the
chosen strategy (scoped `FK_CHECKS=0` load **or** pre-flight block, per the MM5b decision) with **no
silent partial load**; kill mid-copy → resume correct; a row changed during copy reconciles;
`local_infile`-OFF path falls back to batched INSERT and still converges; backpressure holds under a
throttled target; emits F2 metrics/spans.
**Depends on:** MM2, MM3.

## MM5a — Minimal streaming convergence (TRUE FIRST END-TO-END, MySQL)
**Goal:** Earliest honest end-to-end: single-component MySQL streaming with crash-safety.
**Deliverables:** consume `delta MINUS track`; **faithful re-read → staging `ON DUPLICATE KEY UPDATE`**
(§4.2, MySQL text format); single-component apply (no cross-table FK ordering yet); **crash-safe
ordering** apply→track→purge, at-least-once, no 2PC (§3.3); **cutover** copy→streaming.
**Acceptance (matrix) — the keystone correctness tests, identical in spirit to Postgres M5a:**
- **Commit-order hazard:** an uncommitted low-`delta_id` row is not seen/purged by a drain pass; after
  commit, a second pass applies it. (InnoDB auto_increment assigns at insert, visible at commit — same
  skew as PG's `nextval`; asserts §3.3 is defeated for MySQL too.)
- **Delete-by-id read-and-clear race:** X=V1 being processed while source updates X→V2 (new delta row)
  → only observed ids deleted → V2 survives and converges.
- **Crash between apply and track-write:** replays without duplication (idempotent ON DUPLICATE KEY).
- **Coalescing:** N updates to one PK → one re-read, final-value convergence.
- End-to-end insert/update/delete/PK-change converge; emits F2 metrics/spans.
**Depends on:** MM4.

## MM5b — FK-ordered apply + retry fallback (MySQL, no deferral)
**Goal:** Multi-table referential correctness within a component **without deferrable constraints**.
**Deliverables:** **per-component apply** — upserts parent→child, deletes child→parent (§3.3, §8.1) —
in one transaction with **FK checks left ON** for acyclic components (topo order makes them hold).
**Transient-FK error mapping (concrete):** Postgres wraps SQLSTATE **23503** as
`engine.TransientConstraintError` so the neutral drain retry loop handles it; the MySQL engine must
map its FK-violation signal — **errno 1452 (`ER_NO_REFERENCED_ROW_2`), SQLSTATE 23000** — to the same
`engine.TransientConstraintError`, and halt loud on everything else. Without this mapping the retry
fallback (Option A) cannot function.
**Cyclic/self-ref/cross-pass deps — the core MySQL decision:**
- **Option A (recommended default): retry fallback (engine-neutral, §3.3).** A row that hits a
  transient FK violation stays dirty and retries on a later pass; cyclic components converge across
  passes. **Consequence, documented:** MySQL cyclic components get **eventual (cross-pass) referential
  consistency, not Postgres' per-pass atomicity** — a real, disclosed weakening of the §8.1 per-pass
  guarantee for the cyclic case only. Bounded exponential-backoff retries; on exhaustion, **halt the
  component loud + observable** (§4.2), never infinite thrash.
- **Option B (opt-in): `FOREIGN_KEY_CHECKS=0` for the component pass** — restores per-pass atomicity
  for cyclic components but permits a transiently-inconsistent commit; gated behind explicit config.
**Acceptance (matrix):** acyclic multi-table component stays referentially consistent with checks ON;
an FK cycle converges via retry (Option A) — asserted to reach a consistent target even if not
per-pass-atomic; a deliberately unsatisfiable dependency **exhausts bounded retry and halts loud** (no
thrash/skip); per-component transaction bounded by churn/pass; emits F2 metrics/spans.
**Depends on:** MM5a.

## MM5c — Bloat control: purge + bounded retention + forced reseed (MySQL)
**Goal:** Protect a source we may not own; survive a stalled **InnoDB history list** (xmin analog).
**Pre-work (design artifact, delegate to oracle):** confirm the **reseed state machine** (already
specified engine-neutrally in `docs/reseed-state-machine.md`) needs **no MySQL-specific changes** —
or enumerate any (e.g. TEMP-table / `FK_CHECKS` interactions during the re-copy). Reuse the neutral
`internal/reseed` orchestration.
**Deliverables:** **batched consumption-gated purge** (plain `DELETE` — no autovacuum to lean on) with
**optional per-table `OPTIMIZE TABLE`** and/or **RANGE-partition + `DROP PARTITION`** (5.7+/8.0) as
the space-reclamation path (the PG partition+DROP analog, likewise opt-in); **bounded retention
(age+size) + forced reseed** of over-cap targets (reuse neutral reseed). **The MySQL bloat physics
differ:** InnoDB doesn't shrink the tablespace on DELETE, and a long-running source transaction
inflates the **history list length**, blocking purge — the direct analog of the PG xmin horizon —
which must be **surfaced loudly** in telemetry.
**Acceptance (matrix):** convergence **after reseed under continuous source writes**;
**history-list-stall test:** a long-running source transaction inflates the history list → delta
bloat grows AND is surfaced loudly; bounded-retention/reseed still protects the source; over-cap
target → reseed triggers and bounds growth; emits F2 metrics/spans.
**Depends on:** MM5b.

## MM6 — Observability verification (REUSED F2)
**Goal:** Verify the reused F2 contract end-to-end for MySQL and confirm the operator surface.
**Deliverables:** **no new contract** — confirm the MySQL engine emits the declared metrics/spans/log
events (throughput, lag, backlog, per-target reachability, purge rate, reseed events, phase); confirm
the neutral status HTTP API + CLI `status` report MySQL syncs; the cross-channel target-unreachable
signal (§3.4/§10) fires for a downed MySQL target.
**Acceptance:** a scrape asserts the **named** series exist with expected labels for a live MySQL
sync; a drain pass against a downed MySQL target yields the span-error + escalating-log + gauge/backlog
trifecta; status API reports MySQL phase/lag/progress.
**Depends on:** MM5c. *(Verification milestone — strongest proof the core is engine-neutral.)*

## MM7 — Daemon, CLI & multi-sync (REUSED core; MySQL integration)
**Goal:** MySQL syncs run under the real daemon with the full lifecycle.
**Deliverables:** **no new daemon code** — verify the neutral daemon manages MySQL syncs (multiple
named syncs, worker pools, graceful SIGTERM drain+checkpoint); CLI `run`/`validate`/`status`/`capture
install|remove`/`reseed` operate on MySQL configs; a **mixed multi-sync** (a MySQL sync and a Postgres
sync in one daemon) runs concurrently, proving engine coexistence while the single-engine rule holds
per-sync.
**Acceptance:** ≥2 concurrent MySQL syncs on the matrix; a MySQL + Postgres sync coexist in one
daemon; SIGTERM drains+checkpoints cleanly; ungraceful kill resumes; CLI exercises the MySQL
lifecycle.
**Depends on:** MM6, MM5c.

## MM8 — Packaging & docs (MySQL, feature-complete)
**Goal:** MySQL installable and documented.
**Deliverables:** the **existing static pure-Go binary** now links the MySQL driver (still CGO-free);
MySQL **grant SQL** (`deploy/grants-source-mysql.sql`, `deploy/grants-target-mysql.sql`); example
MySQL config; MySQL sections in getting-started/configuration/operations/troubleshooting; a
**MySQL demo** (compose: old source + modern target + replicare) mirroring the Postgres demo; update
README status (Postgres + MySQL shipped).
**Acceptance:** binary runs a MySQL sync via the systemd sample; the MySQL compose demo replicates
end-to-end; MySQL quickstart works.
**Depends on:** MM7.

## MM9 — Hardening & E2E matrix (MySQL shippable gate)
**Goal:** Confidence under faults, scale, version spread. **Passing MM9 = MySQL shippable.**
**Deliverables:** full **MySQL integration matrix** (MF3-old→modern, incl. the chosen 5.7/8.0 pair);
**fault-injection** (crash, target down, slow-but-reachable target, history-list stall);
**throughput/perf benchmarks with recorded thresholds + non-regression assertions**; expanded MySQL
fidelity corpus (zero-dates, charset mixes, JSON, generated, enum/set, `BINARY`/`BLOB`, spatial
noted-or-scoped).
**Acceptance:** matrix green; perf numbers recorded with explicit thresholds; fidelity corpus passes;
retention/reseed proven to bound source growth under a stalled history list at scale; slow-but-
reachable target handled via backpressure without unbounded memory.
**Depends on:** MM7 (parallel with MM8).

---

## Critical path & sequencing
MM0 → MM0.5 → MM1 → {MM2, MM3} → MM4 → **MM5a (true first end-to-end)** → MM5b → MM5c → MM6 → MM7 →
{MM8, MM9}. **F2 is reused, not rebuilt**; MM6 verifies. **MM2/MM6/MM7 are largely
integration-verification** (the neutral core already does the work) and are kept as explicit
milestones only to test the seams. Earliest honest MySQL end-to-end: **end of MM5a**. Feature-
complete: **MM8**. Shippable: **MM9 gate**.

## Reuse ledger (what MySQL does NOT rebuild) — CONFIRMED by the code inventory
The import scan of every non-test source file confirms **every pipeline package a MySQL data sync
flows through is engine-neutral and reused unchanged:** `internal/copy`, `internal/apply` (incl.
`component.go`/`retry.go`), `internal/reseed`, `internal/pipeline` (`drain.go`/`syncer.go`/`stream.go`),
the `internal/state` **interface**, `internal/observability` (F2 contract + prom/status/tracing/
telemetry), and the neutral half of `internal/config`. The daemon (`daemon/build.go`,
`daemon/daemon.go`) dispatches **purely through `engine.Get(name)` + the `engine.*` interfaces — no
per-engine `switch` anywhere** in the pipeline or daemon.

**Two corrections to earlier assumptions:**
- **There is no `internal/delta` or `internal/schema` package.** Delta/track tables are an
  *engine-internal* concept (Postgres keeps `replicare.delta_*`/`track_*` on the source; MySQL will
  own its own equivalent). Their *orchestration* is the neutral `internal/pipeline` drain loop +
  `internal/apply`. "Schema" is the neutral `engine.Schema`/`engine.Table` type set in
  `internal/engine/types.go`, populated by each engine's `Introspect`. So MySQL's delta/track/capture
  is **MySQL-new engine code**, not a reused package.
- **`internal/state/postgres` (+ `internal/pgmigrate`) is deliberately always-Postgres** and
  orthogonal to the data engine (confirmed: `daemon.go` wires `statepg.New(...)` unconditionally,
  driven by the `state_store` block, not `srcEp.Engine`; `config.go` enforces `state_store.engine ==
  postgres`). A MySQL sync uses it as the daemon's control-plane bookkeeping only (MM2).

**MySQL-new:** `internal/engine/mysql/*` (Engine/Source/Sink/ApplyTx/Preflight + MySQL introspection,
capture DDL/triggers, chunking, LOAD DATA transport, consume/purge, type-compat, session
canonicalization), the `mysql:` config block, MySQL grant SQL, and the MySQL harness + tests.

**The three additive registration seams (exact, from the inventory):**
1. `internal/engine/mysql` `init()` → `engine.Register(mysqlEngine{})` (mirrors `postgres/engine.go`).
2. `internal/engine/mysql/config.go` `init()` → `config.RegisterEngine("mysql", parse)` with a `mysql`
   `EngineConn` (Name/Validate/ConnConfig) (mirrors `postgres/config.go`).
3. **Hand-edit** `cmd/replicare/engines.go` to add `_ ".../internal/engine/mysql"` (the file's comment
   already reserves the spot). This is the *only* non-additive edit outside the new package.

## Open questions (decide before/within the noted milestone)
1. **NOT NULL cyclic FKs — `FOREIGN_KEY_CHECKS=0` vs pre-flight block (MM4/MM5b).** The plan's biggest
   philosophical divergence from Postgres. Recommendation: allow `FK_CHECKS=0` **scoped strictly** to
   the initial-copy load (and opt-in for the streaming component pass), because blocking would refuse
   common MySQL schemas and the practice is session-local + reconciled by capture. **Needs Momus.**
2. **Charset fidelity mechanism (MM1/MM4).** How to move character/blob bytes without lossy MySQL
   transcoding across a latin1→utf8mb4 version gap — pinned `character_set_client` + column-charset
   honoring, vs a binary/hex transfer path. **Delegate to oracle.**
3. **Zero-date handling (MM1).** Pin the target write session's `sql_mode` to accept source zero-dates
   (session canonicalization, not data transform) vs fail-loud. Recommendation: pin-then-verbatim,
   fail-loud only if impossible.
4. **MariaDB scope (MM0).** In or out for v1. Recommendation: **out** for v1 (fork dialect drift),
   documented as a future engine variant.
5. **MySQL-backed StateStore (MM2).** v1 keeps the Postgres StateStore for MySQL syncs (operational
   wrinkle: need a Postgres to replicate MySQL). Future item behind the pluggable interface.
6. **`local_infile` unavailability (MM4).** Batched-INSERT fallback is the answer; confirm it meets
   perf floors in MM9 or document the degradation.
7. **Old-source floor 5.6 vs 5.7 (MM0).** 5.7 gives generated columns + JSON + usable
   `information_schema`; 5.6 widens reach but loses those. Recommendation: **5.7 floor**, 5.6 best-effort.

## How we execute (Sisyphus)
Each MMx is a task group; MySQL-dialect SQL, charset/zero-date fidelity, and the `FK_CHECKS` decision
delegate to oracle; docs to document-writer at MM8. A milestone closes only when its acceptance tests
pass on the MySQL matrix. Plan/decision changes are mirrored into `CLAUDE.md` (which already names
MySQL as a first-class future engine throughout — §3.1, §11, §14).

## Momus review disposition
*(To be filled after the review passes.)*
