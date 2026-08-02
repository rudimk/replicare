# replicare — MySQL Engine Implementation Plan (v3)

**Status:** **APPROVED to execute** after one Sisyphus (execution) + **three** Momus (design) passes.
Pass 3 confirmed every substantive fix closed (it verified the pre-commit FK-verify mechanism against
`apply/component.go`'s real rollback flow) and returned REVISE only for two one-line consistency nits
(a stale "minimal/backward-compatible" phrase and an unowned mixed-charset flag) — both now fixed; the
reviewer's standing was "fix those two lines and this is APPROVE-on-sight, do not open a fourth pass."
v1→v2 folded all six Momus-pass-1 blocking §1.7 holes + both Sisyphus blocking items; **v2→v3 folded
Momus pass 2** (verdict REVISE): the FK-cycle verification is corrected to **pre-commit with rollback** (the pass-1
mechanism was specified post-commit → committed corruption), the **byte-faithful WRITE side** (LOAD
DATA charset validation + mixed-charset handling) is now specified and tested, the impossible
cross-charset "byte-identical" corpus is split into same-charset-identical vs cross-charset-halt-loud,
the FK-edge/cyclic-signal **interface plumbing** is scoped as a real (bounded) change, and the
infeasible secondary-unique runtime backstop is dropped in favor of the pre-flight block. The three
load-bearing decisions (byte-faithful transport, strict-safe `sql_mode`, FK-cycle mechanism) are
**decided**. See the **Review disposition** for the item-by-item mapping across all three passes.
**Scope:** A second engine — **MySQL source → MySQL target** — implementing the *existing*
`engine.Source`/`engine.Sink`/`engine.ApplyTx` interfaces (`internal/engine/engine.go`). The
engine-neutral pipeline (`internal/copy`, `apply`, `reseed`, `pipeline`, the `state` interface,
`observability`) is **reused** per the CLAUDE.md §5/§11 decoupling — with one honest caveat about the
cyclic apply path (see §0.2 / MM5b). Section refs like §3.4 point at `CLAUDE.md`. The **single-engine
rule (§6)** stands: a MySQL sync's source and all targets are MySQL — never cross-engine.

## Guiding constraints carried over from CLAUDE.md (unchanged, engine-neutral)
The MySQL engine inherits every §1 non-negotiable **as-is**: no privileged change stream (no
**binlog** access — the MySQL analog of "no WAL"), least privilege (§12), high performance, deep
observability (§10), **faithful transport / never transform (§1.7)**, and old-source tolerance (§1.6).
Trigger CDC, PK-only dirty-key capture, dirty-key-set consumption (§3.1–3.4), at-least-once idempotent
apply (§5), FK connected components as scheduling units (§8.1), Postgres-backed StateStore (§9),
single-active daemon (§9) — all reused. **§1.7 is the review's north star:** every MySQL decision
below is judged by whether it moves values verbatim and fails *loud*, never silently coerces.

---

## 0. The crux: what MySQL lacks that the Postgres path assumes (and the §1.7 traps each opens)

The interface method **signatures** are engine-neutral, but their doc comments and the Postgres
implementation lean on Postgres features MySQL/InnoDB lacks. Every hard decision traces to one of
these. Momus verified that each gap, left naive, is a concrete §1.7 silent-corruption vector — so each
now carries its **decided** mitigation.

### 0.1 No `COPY` → `LOAD DATA LOCAL INFILE`, and it must be **byte-faithful**
Postgres streams `COPY … TO/FROM STDIN` through `io.Pipe`. MySQL's streaming bulk path is
**`LOAD DATA LOCAL INFILE`** fed from a reader (go-sql-driver's `RegisterReaderHandler`). This keeps
the `io.Reader`/`io.Writer` contract but forces three decisions, now made:
- **DECISION (charset READ side, was Open Q2): byte-faithful reads.** Read every column with
  **`character_set_results = binary`** so the server ships **raw column bytes with no wire
  transcoding**, re-emitted via the driver's `RawBytes` (no Go-type round-trip). This is the *only*
  read transport that honors §1.7 across a `latin1 ↔ utf8mb4` version gap (a `SET NAMES utf8mb4` text
  path would silently transcode). The wire format is a **byte stream**, so it *can* be locked
  (resolving the v1 "can't lock a format while transcoding is open" contradiction).
- **DECISION (charset WRITE side — the other half, Momus 2nd-pass B2):** the read side is only half
  the promise; `LOAD DATA` on the target must **validate** those bytes, not silently store garbage.
  Two facts force the design: (i) `LOAD DATA` has **one `CHARACTER SET` clause per statement**, but a
  MySQL table may carry **per-column charsets** — a single statement cannot byte-faithfully land a
  mixed-charset row; (ii) `CHARACTER SET binary` stores raw bytes **without per-column charset
  validation**, which would silently admit invalid-for-column bytes (a §1.7 corruption). Therefore:
  **load with `CHARACTER SET` set to the table's uniform column charset when the table is
  single-charset**, so the target's own input functions validate each value and **halt loud** on an
  invalid sequence; and **pre-flight loud-warns/blocks tables with mixed per-column charsets** (MM1b),
  handling them (if enabled) via **per-column-charset-group loads**. This is specified + tested on the
  **write** side in MM4a — the read-side corpus alone does not prove faithfulness.
- **Cross-charset transfers are loud-fail, not byte-identical (Momus 2nd-pass M1):** a `latin1` byte
  `0xE9` has **no** byte-identical representation in a `utf8mb4` column, and a 4-byte `utf8mb4`
  sequence has none in `utf8mb3`. Under byte-faithful transport these are **halt-loud** cases (the
  target's input function rejects them), *never* silent transcodes. The fidelity corpus therefore
  splits: **same-charset pairs assert byte-identical round-trip; cross-charset narrowing pairs
  (latin1→utf8mb4 non-ASCII, utf8mb4→utf8mb3 4-byte) assert halt-loud.** (Claiming "byte-identical
  round-trip across a charset change" is logically impossible and would force an illegal transcode.)
- **DECISION (escaping, Momus B6): full, fuzz-tested byte-level escape set.** `LOAD DATA … FIELDS
  ESCAPED BY '\\'` interprets more than `\t`/`\n`/`\\`; binary/BLOB bytes include `\0`, `\r` (`\Z`),
  and the **two-byte `\`+`N` that LOAD DATA reads as NULL**. The serializer escapes the complete set
  (`\0 \b \n \r \t \Z \\`) and disambiguates literal `\N` bytes from the NULL sentinel — or, at
  implementer's option, uses hex literals for `BINARY`/`BLOB`. Locked in **MM4a** with a BLOB fuzz
  corpus (values containing `\t \n \\ \0 0x1A` and the literal bytes `\N`).
- **DECISION (`local_infile` OFF fallback, Momus M5): text-literal batched INSERT.** If `local_infile`
  is OFF/ungrantable, fall back to multi-row `INSERT` built from **pre-rendered verbatim byte/text
  literals** (via `character_set_results=binary`), **never** driver-typed parameters (which would
  round-trip through Go's type system — the §4.2/§14 fidelity loss the text path exists to avoid). The
  fallback applies to **both** initial copy (MM4a) **and** streaming apply staging (MM5a), since both
  use LOAD-DATA-into-TEMP. Perf: correctness-first; MM9 records a coarse bound (fallback within Nx of
  LOAD DATA) or explicitly marks it correctness-only.

### 0.2 No deferrable constraints, no `SET CONSTRAINTS` → FK cycles need `FOREIGN_KEY_CHECKS=0` + verify
Postgres defers FK checks in a component transaction (`SET CONSTRAINTS ALL DEFERRED`) so a cyclic
component commits atomically and **still aborts loud at COMMIT** on a real violation. InnoDB has **no**
deferral; the only lever is session-local **`SET FOREIGN_KEY_CHECKS=0`**, which *disables* checking
(never re-checks). **Momus B3 (verified against `internal/apply/component.go` + `retry.go`):** the
neutral component apply is **all-or-nothing per pass** (`BeginApply` → all `StageUpsert` parent→child
→ all `DeleteAbsent` → single `Commit`; any error rolls the whole pass back), and
`DrainComponentRetrying` retries the **identical** pass. With checks ON, a true mutual `NOT NULL`
cycle throws errno 1452 on the first `StageUpsert` → whole pass rolls back → every retry is identical
→ thrash → halt. **Therefore the retry fallback cannot close an intra-pass `NOT NULL` cycle** — it
only ever resolves *cross-pass* deps and acyclic transient violations. Decisions:
- **DECISION (FK cycles, was Open Q1; Momus B3/B5/M9):**
  - **Acyclic components:** apply with **FK checks ON**, topo-ordered (parent→child upserts,
    child→parent deletes). Transient cross-pass FK violations use the **neutral retry fallback**
    (errno **1452 / SQLSTATE 23000** mapped to `engine.TransientConstraintError` — the MySQL analog of
    Postgres' 23503 in `apply_tx.go`).
  - **Cyclic / self-ref `NOT NULL` components (initial copy AND streaming):** the mechanism is
    **`FOREIGN_KEY_CHECKS=0` scoped to that component's load/apply transaction**, **paired with a
    mandatory PRE-COMMIT referential-integrity verification** that converts any residual dangling
    reference into a **loud halt + rollback** (§1.7). **The verification MUST run inside the open
    transaction, before `COMMIT` (Momus 2nd-pass B1 — this is the load-bearing correction):** MySQL's
    `ApplyTx.Commit()` first `SET FOREIGN_KEY_CHECKS=1`, then runs the orphan anti-join over the
    component's FK edges *within the still-open transaction* (it sees its own uncommitted writes); on
    any orphan it returns a loud error and **does not COMMIT** — the existing `defer` in the neutral
    `apply.DrainComponent` rolls the transaction back, so **nothing referentially broken is ever
    visible**, exactly matching Postgres' abort-at-COMMIT deferral. A **post-commit** verification is
    *rejected*: it would make the orphan durable and visible before the halt fires (committed
    corruption, and a crash between COMMIT and verify would skip the only loud step). `FK_CHECKS=0`
    alone is strictly weaker than Postgres deferral (never re-checks); the **pre-commit** verification
    is what restores the loud-*before*-corrupt promise — it is **not optional** and **not post-hoc**.
  - **Verification scope (Momus 2nd-pass m1):** the anti-join covers the **whole component's FK
    edges over all component tables**, not just this pass's dirty/upserted rows — because with
    `FK_CHECKS=0` a `DeleteAbsent` can strand a **non-dirty** child that references a just-deleted
    parent. A dirty-row-scoped check would miss that. (Cost is bounded by component size per pass;
    acceptable, and only paid for cyclic components.)
  - **Cyclic nullable components:** **NULL-then-fill two-pass** (insert with FK cols NULL, then UPDATE)
    — no constraint touching needed; preferred where applicable.
- **Reuse caveat + the plumbing it actually needs (corrects the v1 ledger; Momus B3 + 2nd-pass M2):**
  the neutral apply *orchestration* is reused, but MySQL's `ApplyTx` issues `SET FOREIGN_KEY_CHECKS=0`
  (not `SET CONSTRAINTS ALL DEFERRED`) **for cyclic components only** and runs the **pre-commit**
  verification. This requires threading data the apply layer does **not** carry today (verified: it
  receives only `comp.Order []engine.TableRef` via `stream.go`→`component.go`; `BeginApply(ctx)` has
  no component identity, and the `engine.ForeignKey` edges live on `engine.Table.ForeignKeys` at
  introspection but are **never passed to the sink**). So MM5b must specify a **real, if bounded,
  interface change**, not a "minimal tweak": (a) a **cyclic flag** and (b) the **component FK-edge
  list** must reach `BeginApply`/`ApplyTx` (so MySQL can decide `FK_CHECKS=0` at Begin and build the
  orphan anti-join). This **also changes the Postgres sink's `BeginApply` signature** (it ignores the
  new args), so it is not purely additive. Feasible and small, but **decided and implemented in MM5b
  before the FK_CHECKS acceptance gate**, not treated as an afterthought. The `component.go` doc-
  comment baking in Postgres deferral is genericized (m1).
- **CHANGE DISCIPLINE (Momus B5 — mandatory, blocking):** this **overturns a RESOLVED CLAUDE.md
  decision** (§4.1/§14 "never by touching target constraints"; §15 marks `NOT NULL` non-deferrable
  cyclic **resolved** as "pre-flight fails loud"). The plan **commits** to these exact edits, made when
  **MM4b/MM5b land** (not after): §4.1 and §14 gain a **MySQL exception** — cyclic `NOT NULL`
  components use scoped `FOREIGN_KEY_CHECKS=0` **+ pre-commit orphan verification → loud halt +
  rollback** (never a committed dangling ref); §15 is **re-opened/annotated for the MySQL engine**.
  Postgres behavior is unchanged.

### 0.3 No `ctid` / stable physical row locator → keyset-only, with two MySQL-specific correctness traps
InnoDB tables are **clustered on the primary key**, so keyset PK-range chunking already scans in
physical order — the ctid fallback has **no MySQL analog and needs none** for keyed tables; no-PK
tables are **skipped + warned** (§3.1), leaving no residual case. Documented, not implemented. But
keyset over MySQL introduces two traps the Postgres path never had:
- **DECISION (ci/ai collation, Momus M7): force binary-collation ordering for keyset.** MySQL's modern
  default `utf8mb4_0900_ai_ci` is case/accent-**insensitive**, so `ORDER BY text_key` is **not a total
  order** (`'a' = 'A'`); keyset pagination and `DeleteRange`'s range predicate would skip/duplicate
  boundary rows (data loss). Boundary discovery, chunk range predicates, and `DeleteRange` on text
  keys **force `COLLATE utf8mb4_bin`** (binary ordering). Acceptance in MM4a: a mixed-case text PK
  under a ci collation copies with **no skipped/duplicated boundary rows**.
- **DECISION (boundary scan, Momus M8): version-branched, defined-order only.** MySQL &lt; 8.0 lacks
  `ROW_NUMBER()`; MySQL 8.0 **deprecates and leaves undefined** the evaluation order of `@rn := @rn+1`
  inside `SELECT` — so the v1 "user-variable counter" default is **unreliable on modern targets**. Use
  **`ROW_NUMBER() OVER (ORDER BY key COLLATE utf8mb4_bin)` on 8.0+** and a **deterministic keyset
  `LIMIT n` walk** (repeated bounded scans collecting every Nth key) on 5.7 — never user-variable
  arithmetic. Locked in MM4a.

### 0.4 MySQL enforces `sql_mode`/strict rules and auto-mutates some columns → session canonicalization is subtler than Postgres GUCs
- **DECISION (zero-dates, was Open Q3; Momus B2): strict-safe `sql_mode`.** A very old source may hold
  `'0000-00-00'`; a modern strict target rejects it. The target write session **keeps
  `STRICT_TRANS_TABLES` + `STRICT_ALL_TABLES`** and removes **only** `NO_ZERO_DATE` and
  `NO_ZERO_IN_DATE` (in strict mode, zero dates are permitted iff those two flags are absent). This
  lets a zero-date land **verbatim** while **every other** out-of-range/oversize value still **halts
  loud** — never the blanket STRICT-off that would silently truncate strings and clamp ints. MM1b
  acceptance proves BOTH: (a) a zero-date round-trips verbatim, and (b) an oversize value on a
  *different* column still halts loud under the same pinned mode. If a target version cannot both
  permit zero-dates and keep strict-for-everything-else, **fail loud** — never drop STRICT.
- **DECISION (`ON DUPLICATE KEY UPDATE` on secondary uniques, Momus B1 + 2nd-pass M3): the pre-flight
  block IS the protection — no viable runtime backstop exists.**
  Postgres upserts `ON CONFLICT (pk)` — a secondary-unique collision raises `unique_violation` and
  halts loud. MySQL `INSERT … ON DUPLICATE KEY UPDATE` has **no conflict target**: it fires on the PK
  *or any secondary UNIQUE*, so upserting `(id=5, email='a@x')` onto an existing `(id=7, email='a@x')`
  **silently rewrites row 7 to id=5** — §1.7 corruption of a *different* row. Critically, **ODKU does
  not error on that collision**, and we cannot use plain `INSERT` + catch errno 1062 because
  *legitimate* PK re-upserts (the normal streaming case) also raise 1062 — so **there is no clean
  runtime "halt loud on secondary-unique" backstop.** Protection is therefore entirely **MM1b
  pre-flight: BLOCK any target table with a secondary UNIQUE key beyond the replication key**
  (documented opt-in override). MM1b acceptance tests the block; **MM5a does NOT claim a runtime
  backstop it cannot implement.** Residual risk (a secondary unique added by post-pre-flight DDL
  drift) falls under the §7 "schema drift → re-sync" stance. (Never `REPLACE INTO` — it
  delete+reinserts, firing cascades/auto_increment.)
- **DECISION (`ON UPDATE CURRENT_TIMESTAMP`, Momus M2): override to verbatim.** An old source with
  `explicit_defaults_for_timestamp=OFF` gives TIMESTAMP columns implicit `ON UPDATE CURRENT_TIMESTAMP`;
  on the **target**, any such column is **silently rewritten to "now"** when our upsert touches the
  row. MM1b introspection parses `EXTRA` for `on update CURRENT_TIMESTAMP`; MM5a apply **explicitly
  sets those columns to the verbatim source value** in the UPDATE clause (overriding auto-mutation), or
  pre-flight loud-warns. Fidelity case added.
- **Faithful auto_increment/identity:** pin `NO_AUTO_VALUE_ON_ZERO` (the `OVERRIDING SYSTEM VALUE`
  analog) so a source `0` in an auto_increment column is not turned into next-value.

### 0.5 Storage-engine, trigger-slot, DDL-atomicity, and multi-database assumptions MySQL breaks
- **DECISION (non-InnoDB, Momus M3): detect + block/warn.** MyISAM/MEMORY targets make `BEGIN/COMMIT`
  a **no-op** (§8.1 per-component atomicity silently lost); a non-transactional **source** breaks the
  §3.3 "consume + track + purge in one atomic source transaction" guarantee (delta loss / double
  processing). MM1b reads `information_schema.TABLES.ENGINE` per table and **blocks (or loud-warns with
  documented degradation)** on non-transactional engines, source and target.
- **DECISION (pre-8.0 single-trigger-per-event, Momus M4): detect + block.** MySQL &lt; 8.0 allows only
  **one** trigger per (timing, event) per table; a selected source table that **already has** an
  `AFTER INSERT` trigger blocks `CREATE TRIGGER` on the 5.7 floor. MM1b detects existing triggers on
  selected tables and **blocks with an actionable error** (merging into a trigger we don't own is
  rejected in v1 — documented tradeoff). 8.0+ multi-trigger is used where available.
- **DECISION (implicit COMMIT on DDL, Momus M1): idempotent checkpointed install/migration.** MySQL
  **auto-commits every `CREATE TABLE`/`CREATE TRIGGER`** — the Postgres "six DDL statements in one
  transaction" capture-install and the F4 migration runner's transactional rollback **do not hold**. A
  crash mid-install leaves partial capture; a failed migration leaves a half-applied `schema_version`.
  MM3/MF4 specify **idempotent, individually-checkpointed, re-runnable** install and migration steps
  (each persistent-object DDL recorded/verified so a re-run resumes), and a `RemoveCapture` that
  **tolerates partial state**. (`CREATE TEMPORARY TABLE` is exempt from implicit commit, so the
  apply/merge staging path stays transactionally safe.) Acceptance: kill mid-install → re-run converges
  to complete capture; interrupted migration → re-run completes without re-applying steps.
- **DECISION (schema = database, Momus M6): define multi-DB semantics up front.**
  `engine.ConnConfig.Database` is singular, but MySQL selection is `db.table` and MySQL allows
  **cross-database FK edges** (one FK component can span databases). MF1/MM1a define `database` as the
  **DSN default only** — selection globs may span other databases; MM1a introspection does **not**
  filter `information_schema` by a single `TABLE_SCHEMA`; the **`replicare` capture database is a
  single dedicated database** holding delta/track tables for all captured source databases (keyed by
  source db+table). MM3 grants enumerate `SELECT`/`TRIGGER` across the N selected databases + owner on
  the `replicare` database. Acceptance: a cross-database FK component copies + streams correctly.

---

## Goals & non-goals
**Goals (v1 MySQL):** trigger-based CDC (no binlog), PK-only capture, dirty-key-set consumption
(§3.1–3.4); resumable chunked initial copy + delta streaming to a **single target** (fan-out interface
present, not hardened), one-way; **byte-faithful transport, never transform** (§1.7, §4.2) across
MySQL types/charsets/collations; reuse the Postgres-backed StateStore (§9); reuse the full
observability contract (§10); run against an **old source MySQL (5.7) → modern target MySQL (8.4)**
(§1.6 analog).
**Non-goals (v1 MySQL):** multi-master, HA/leader-election, live DDL, value transforms, cross-engine,
Helm, a MySQL-backed StateStore (state store stays Postgres — MM2), **binary LOAD DATA**, **MariaDB**
(**DECIDED out for v1**, Open Q4 — fork dialect drift; documented as a future engine variant), and
merging capture into a table's pre-existing trigger.

## Definition of Done (MySQL engine)
Feature-complete at **end of MM8**; **shippable only after the MM9 gate passes**. Done when: given a
YAML config with a MySQL source + MySQL target(s), replicare can validate the pair (pre-flight —
including the MySQL-specific blocks: secondary-unique, non-InnoDB, pre-existing-trigger, incompatible
types), install least-privilege trigger capture (idempotently, surviving mid-install crashes),
perform a resumable byte-faithful initial copy of an FK-connected multi-table schema **including
self-referential and cyclic FKs (via the §0.2 mechanism with orphan-verification)**, then stream
inserts/updates/deletes/PK-changes to convergence — crash-safe, observable across all three channels
(reusing F2), bounding source footprint under a stalled **InnoDB history list** (the xmin analog) —
**verified by the automated MySQL integration pair (MM9).**

---

## Cross-cutting foundations (MySQL adaptations of F1–F4)

- **MF1 — Config block (locked in MM0.5).** Add a **typed `mysql:` engine block** to the existing
  neutral envelope via the engine registry — no change to the neutral schema (proves §11's
  extension-point claim). Owns/validates: connection shape (host/port/**database as DSN default**,
  user/password/TLS), **`db.table` selection globs (may span databases)**, a **`local_infile`
  capability hint**, and MySQL CDC/copy tuning (worker/batch/chunk, LOAD-DATA-vs-INSERT bulk mode).
  **TLS mapping (Momus m6): spell out all six `engine.TLSMode` values onto go-sql-driver** —
  `disable→tls=false`, `require→tls=skip-verify`, `verify-full→tls=true`, and `verify-ca` has **no
  direct driver flag** → a **custom `tls.Config` with `VerifyPeerCertificate`** (CA but not hostname);
  note old-server (5.7 default self-signed cert) compatibility. Single-engine rule already enforced by
  neutral validation; add MySQL to its engine set.
- **MF2 — Observability contract: REUSED AS-IS.** The F2 registry (metrics/spans/log events) is
  engine-neutral; the MySQL engine emits the **same** series against it. No new contract. MM6 verifies.
- **MF3 — Old-source MySQL floor (decided in MM0). DECISION: 5.7 source → 8.4 target** (Open Q7). 5.7
  gives generated columns + JSON + usable `information_schema`; 5.6 is best-effort only and, if CI
  can't run the chosen floor, the plan **lists which fidelity claims become unverified** — never
  silently raises. **MariaDB: out (Open Q4).** **The CI "matrix" is one pair** (`mysql:5.7 →
  mysql:8.4`, one `integration` job, amd64/emulation like Postgres) — the word "matrix" is replaced by
  this concrete pair throughout (Sisyphus M4).
- **MF4 — Schema versioning: REUSED runner abstraction, MySQL-safe steps.** Reuse the shared
  migration-runner for the **source `replicare` database** (a MySQL database), carrying `schema_version`,
  **but with idempotent per-step checkpointing because MySQL auto-commits DDL** (§0.5/MM1). StateStore
  schema stays Postgres and reuses Postgres' F4 runner unchanged.

---

## Testing strategy (every milestone)
- **MySQL pair harness (MM0):** `mysql:5.7` source + `mysql:8.4` target, **`local_infile=ON`** and
  appropriate `secure_file_priv` in both images, **ports 3340/3341** (off the Postgres harness'
  5440/5441), ephemeral, non-default. A **dual-harness** compose (both MySQL + both Postgres, 4
  containers) exists for MM7's coexistence test. Integration gated by `REPLICARE_INTEGRATION=1`,
  packages run **`-p 1`** (MySQL integration packages share schemas, like Postgres).
- **MySQL byte-fidelity corpus (from MM1b):** int/decimal/float/double/bit/char/varchar/text/**blob/
  binary (with embedded `\t \n \\ \0 0x1A` and literal `\N` bytes)**/enum/set/date/**datetime/
  timestamp (tz + `ON UPDATE`)**/**JSON**/generated(virtual+stored)/**`0000-00-00` zero-dates**/
  auto_increment. **Same-charset pairs assert byte-identical round-trip across the 5.7→8.4 gap;
  cross-charset narrowing pairs (latin1→utf8mb4 non-ASCII, utf8mb4→utf8mb3 4-byte) assert HALT-LOUD,
  never byte-identity** (Momus 2nd-pass M1 — a byte-identical cross-charset round-trip is impossible
  and would force an illegal transcode). Zero-dates, the write-side charset validation, BLOB escaping,
  and ON-UPDATE-timestamp get dedicated cases.
- **Named property/race tests (from MM5a):** commit-order hazard, delete-by-id read-and-clear race,
  crash-between-apply-and-track, coalescing — plus **secondary-unique-halts-loud** and
  **cyclic-orphan-verification-halts-loud**.
- **Fault injection (from MM3/MM4/MM5*):** kill mid-install; kill mid-copy/mid-stream; target down;
  slow-but-reachable target (backpressure); **long-running source transaction inflating the InnoDB
  history list** (xmin analog).
- A milestone is **not complete** until its acceptance tests pass on the 5.7→8.4 pair in CI.

---

## Milestones (decomposed & resequenced per the reviews)

### MM0 — Foundations: engine skeleton, harness (dual-ready), floor, matrix
**Goal:** Registered-but-stubbed MySQL engine; the concrete harness + CI pair + MF3/MariaDB decisions.
**Deliverables:** `internal/engine/mysql/` package; **`go-sql-driver/mysql`** (pure-Go → static binary
stays CGO-free); `Engine` factory + `init()` `engine.Register`; `ServerVersion` probe (5.6/5.7/8.0/8.4);
**docker-compose MySQL harness** at **ports 3340/3341**, `local_infile=ON`, ephemeral; a
**dual-harness** compose + Taskfile `test:integration:mysql` (and a dual phase for MM7); **`.github/
workflows/ci.yml` gains one `integration-mysql` job** for the 5.7→8.4 pair (state CI-minute cost);
**MF3 floor + MariaDB-out decision doc**.
**Acceptance:** CI green with the MySQL package building + the new integration job wired; harness
brings up 5.7+8.4 with `local_infile` on and non-clashing ports; dual-harness bring-up works;
`Get("mysql")` returns the engine; floor/MariaDB committed in writing.
**Depends on:** none.

### MM0.5 — MySQL config block (MF1)
**Goal:** Lock the MySQL connection/selection/tuning surface, including the full TLS mapping.
**Deliverables:** typed `mysql:` block parser+validator via `config.RegisterEngine`; `db.table`
selection globs (multi-DB semantics per §0.5); **all six TLS modes mapped** (incl. custom
`verify-ca`); secret resolution (inline+env) reusing the neutral layer; `local_infile` hint; bulk-mode
knob.
**Acceptance:** golden-file parse+validate of a full MySQL config; env-override precedence; invalid
configs rejected with actionable errors; single-engine rule rejects a MySQL↔Postgres sync; `verify-ca`
maps to a working custom `tls.Config`.
**Depends on:** MM0.

### MM1a — Connectivity, session canonicalization & introspection (MySQL)
**Goal:** Faithful, secure MySQL sessions and a version-tolerant introspected schema. *(Split from v1
MM1 per Sisyphus M1 — this is the connection/introspection half; classification is MM1b.)*
**Deliverables:** driver conn mgmt; **session canonicalization pinned on every source-read and
target-write conn:** `time_zone='+00:00'`, **`character_set_results=binary`** (byte-faithful reads,
§0.1), the **strict-safe target `sql_mode`** (§0.4: keep `STRICT_*`, drop only `NO_ZERO_DATE`/
`NO_ZERO_IN_DATE`), `NO_AUTO_VALUE_ON_ZERO` — all session-level, no special privilege; **TLS per
connection** wired (all six modes); **`information_schema` introspection** (COLUMNS incl. `EXTRA` for
generated/auto_increment/**`on update CURRENT_TIMESTAMP`**, KEY_COLUMN_USAGE/TABLE_CONSTRAINTS/
STATISTICS for PK/unique/**secondary uniques**, REFERENTIAL_CONSTRAINTS for FK edges incl.
**cross-database**, `TABLES.ENGINE` for **storage engine**, and existing-**trigger** enumeration),
version-tolerant behind the MM0 probe; **`db.table` selection** across databases; supplies the FK edge
list to the neutral component computation. **Note (Momus 2nd-pass m3):** confirm
`character_set_results=binary` does not corrupt result **metadata** the driver relies on (read by
ordinal via `RawBytes`) — test it here, don't assume.
**Acceptance:** `Introspect` returns MySQL tables/columns/keys/FK-edges (incl. cross-DB) correctly on
both 5.7 and 8.4; `EXTRA`-derived generated/auto_increment/on-update flags parsed; storage engine and
existing triggers surfaced; `verify-full` + `disable` connect; env-resolved secret connects; a
zero-date value **round-trips verbatim** under the pinned modes while an oversize value on another
column **halts loud**; a `RawBytes` read under `character_set_results=binary` returns column values
byte-intact with correct metadata; emits F2 spans.
**Depends on:** MM0.5.

### MM1b — Pre-flight classification & the MySQL-specific blocks
**Goal:** A MySQL DB pair → validated, classified replication report with every MySQL §1.7 block.
**Deliverables:** **type-compat classification** for MySQL type pairs (identical/widening/risky/
incompatible; **`utf8mb4→utf8mb3` is risky-lossy/block**, not a generic warn — Momus m4); **FK
connected components** (reuse neutral computation) + giant-component + dangling-FK-to-excluded warnings
(§8.1); **cyclic-FK classification, MySQL cases (single, decided outcome — Momus M9):** nullable →
NULL-then-fill (MM4b); **`NOT NULL` cyclic → scoped `FK_CHECKS=0` + orphan-verification (MM4b/MM5b)**
(no undecided fork; no `DEFERRABLE` middle case exists); **the MySQL pre-flight BLOCKS
(§0.4/§0.5):** target tables with **secondary UNIQUE keys** beyond the replication key (B1),
**non-InnoDB** source/target tables (M3), source tables with a **pre-existing conflicting trigger** on
5.7 (M4); no-PK/unique tables **skip+warn** (§3.1); **detect + flag tables with mixed per-column
charsets** (the LOAD DATA single-`CHARACTER SET` limitation, §0.1/2nd-pass B2 — flagged for the
per-charset-group load path, or blocked if that path is disabled); CLI `validate` for MySQL.
**Acceptance (5.7→8.4):** `validate` lists components correctly; hub → giant-component warning; FK-to-
excluded → dangling warning; nullable vs `NOT NULL` cyclic classified to a **single** strategy each;
MySQL mismatch corpus classified (incompatible/`utf8mb4→utf8mb3` block, risky warn); **a secondary-
unique target table is blocked**; **a MyISAM table is blocked**; **a pre-existing AFTER trigger on 5.7
is blocked** with an actionable error; **a mixed per-column-charset table is flagged** (per-charset-group
load or block); PK-less table skipped+warned; emits F2 metrics/spans.
**Depends on:** MM1a.

### MM2 — StateStore integration (REUSED — Postgres) + operational wrinkles *(shrunk per Sisyphus B2)*
**Goal:** Confirm a MySQL data sync runs on the existing neutral Postgres StateStore — testing only
what exists this early.
**Deliverables:** **no new StateStore code.** Integration wiring + docs of **two wrinkles**:
(1) *replicating MySQL still requires a Postgres for state* (a MySQL-backed StateStore is a future item
behind the pluggable §9 interface); (2) **MySQL syncs are strictly at-least-once — never the §9
exactly-once-per-batch bonus** (that needs the StateStore to be the target DB; a Postgres state store
+ MySQL target can't share a transaction) — Momus m1.
**Acceptance (assertable only):** a MySQL **sync definition** round-trips through the Postgres
StateStore; a second daemon **cannot acquire** a held MySQL sync (`pg_advisory_lock`). *(The
copy-progress/cursor persist-and-resume assertions move to MM4a/MM5a, where the producing code lands —
Sisyphus B2. "Wrinkles documented" is a deliverable, not a test gate — Momus m5.)* **Live-run note:**
the StateStore is engine-neutral and already unit/integration-tested (a MySQL SyncDef is structurally
identical to any other); the sync-def round-trip + advisory-lock exclusion for a MySQL sync **against a
live Postgres state store co-located with a live MySQL** is exercised under the **MM7 dual-harness**
(the only place both engines are genuinely present). MM2 delivers the **documentation** of the two
wrinkles (`docs/mysql.md`).
**Depends on:** MM0.5 (config shape). *(Parallel with MM3; **not** a dependency of MM3 — Sisyphus m2.)*

### MM3 — Trigger-based CDC capture (MySQL source machinery, idempotent, + versioning)
**Goal:** Correct least-privilege trigger capture into per-table delta tables, crash-safe to install.
**Deliverables:** `replicare` **source database** (MF4-versioned, **idempotent checkpointed steps** —
§0.5/M1) with **per-table delta tables** (`delta_id BIGINT AUTO_INCREMENT PK` = delete-by-id handle +
lag-only seq; op char; PK columns; `captured_at DATETIME(6)`) + **per-(target,table) track tables**
created lazily; **AFTER I/U/D triggers** (MySQL syntax, PK-only, **PK-update enqueues BOTH old+new PK**
§3.1; `DEFINER=CURRENT_USER` so no `SUPER` needed — Momus m2); **install/uninstall tolerant of partial
state**; set-difference consume + **delete-by-delta_id** (§3.3); **MF4 migration preserving in-flight
deltas**; documented+tested **least-privilege grants** (`TRIGGER`, `SELECT` on replicated tables across
the N databases, `CREATE`/owner on the `replicare` database, `INSERT`/`DELETE` on delta/track,
**`CREATE TEMPORARY TABLES` on the target** for staging — Momus m2; **no `SUPER`, no `REPLICATION
SLAVE`, no binlog**; §12 analog). **MySQL bloat note:** InnoDB has **no autovacuum** — no per-table
reloptions exist; bloat control is entirely MM5c (batched DELETE + optional `OPTIMIZE`/partition-DROP).
**Acceptance (5.7→8.4):** I/U/D produce correct delta rows; PK-change enqueues old+new; **kill
mid-install → re-run converges to complete capture** (M1); install/uninstall leaves the source clean;
works under a non-`SUPER` role with exactly the documented grants; a source-schema version bump
preserves queued deltas; emits F2 metrics/spans. *(Commit-order hazard test lives in MM5a.)*
**Depends on:** MM1a (introspection). *(Not MM2 — Sisyphus m2.)*

### MM4a — Initial copy mechanics (byte-faithful transport, acyclic) *(split per Sisyphus B1)*
**Goal:** Resumable, byte-faithful bulk copy of **acyclic** FK schemas — de-risks the entirely-novel
transport first.
**Deliverables:** **keyset PK-range chunking** (composite/text/`BINARY(16)` via row-value compare,
**`COLLATE utf8mb4_bin` for text keys** — §0.3/M7); **version-branched boundary discovery**
(`ROW_NUMBER()` 8.0+ / deterministic keyset-`LIMIT` walk 5.7 — §0.3/M8); **NO ctid fallback**
(documented); RANGE/LIST-partition-aware per-partition copy where present; **byte-faithful `LOAD DATA
LOCAL INFILE`** streamed source→target via `io.Pipe` with a **unique-per-load reader-handler name +
register/deregister bracketing** (§0.1; Sisyphus M2 / Momus m3 concurrency hazard) and the **full
byte-level escape set** (§0.1/B6); **`local_infile`-OFF text-literal batched-INSERT fallback** (§0.1);
empty-target **direct LOAD DATA + DELETE-range resume** (`DeleteRange` also `COLLATE utf8mb4_bin`);
exclude generated (virtual+stored) cols; **per-chunk progress → StateStore** (this is where the
"copy-progress persist/resume across restart" acceptance moved from MM2); backpressure (bounded queues
+ pool sizing tied to MF1 caps); **capture-first then copy** (§4).
**Write-side charset (Momus 2nd-pass B2):** load with the `CHARACTER SET` clause set to a
single-charset table's uniform column charset so the target **validates + halts loud** on an invalid
byte sequence; **mixed per-column-charset tables are pre-flight-flagged (MM1b)** and, if enabled,
loaded per-column-charset-group. This is the *write* half of byte-faithfulness and is tested here.
**Acceptance (5.7→8.4):** copies acyclic multi-table FK schemas (composite & `BINARY`/text PKs) 5.7→8.4
**byte-faithful** (fidelity corpus: **same-charset → byte-identical**, **cross-charset narrowing →
halt-loud** per §0.1/M1, zero-dates, **BLOB with embedded escape bytes + literal `\N`**, JSON,
generated-excluded); **an invalid-for-`utf8mb4` byte sequence on the write side halts loud** (write-side
validation, not silent store); **a mixed per-column-charset table copies byte-faithfully via the
per-charset-group load path** (or is refused if that path is disabled); **concurrent multi-chunk LOAD
DATA does not collide** (unique reader names); **text PK under a ci collation copies with no skipped/duplicated boundary rows**; kill mid-copy
→ **resume correct (progress persisted)**; a row changed during copy reconciles; `local_infile`-OFF path
falls back to text-literal INSERT and stays byte-faithful; backpressure holds under a throttled target;
emits F2 metrics/spans.
**Depends on:** MM3, MM1b (classification). *(States the MM1 dep — Sisyphus m3.)*

### MM4b — Cyclic-FK initial load + merge path
**Goal:** Correct initial load of cyclic/self-ref components without corrupting data or constraints.
**Deliverables:** **nullable cyclic → NULL-then-fill two-pass**; **`NOT NULL` cyclic → scoped
`FOREIGN_KEY_CHECKS=0` for the component load + mandatory PRE-COMMIT orphan-verification → loud halt +
rollback** (§0.2; the verification runs inside the load transaction before COMMIT, so a bad load is
never committed — initial copy is additionally recoverable by DELETE-range + re-copy, but the rule is
uniform with streaming); **TEMP staging + `INSERT … SELECT … ON DUPLICATE KEY UPDATE`** merge path for
non-empty targets (upsert on the replication key; secondary-unique tables already blocked at MM1b);
**the committed CLAUDE.md §4.1/§14/§15 edits land with this milestone** (change discipline — §0.2/B5).
**Acceptance (5.7→8.4):** nullable self-ref FK copies via NULL-then-fill; a `NOT NULL` mutual cycle
copies via scoped `FK_CHECKS=0` **and the orphan-verification passes**; a deliberately-injected orphan
under `FK_CHECKS=0` is **caught by verification and halts loud** (no silent dangling ref); merge path
into a non-empty target is idempotent; CLAUDE.md edits present; emits F2 metrics/spans.
**Depends on:** MM4a. **The FK_CHECKS decision (§0.2) is already made — no undecided fork enters this
milestone.**

### MM5a — Minimal streaming convergence (TRUE FIRST STREAMING END-TO-END)
**Goal:** Earliest honest streaming end-to-end: single-component MySQL streaming, crash-safe. *(Note:
the first **visible** end-to-end data movement is already MM4a's acyclic copy — Sisyphus m5.)*
**Deliverables:** consume `delta MINUS track`; **byte-faithful re-read → TEMP-staging `ON DUPLICATE KEY
UPDATE`** (§0.1, with `ON UPDATE CURRENT_TIMESTAMP` columns **explicitly set to the verbatim source
value** — §0.4/M2); single-component apply (no cross-table FK ordering yet); **crash-safe ordering**
apply→track→purge, at-least-once, no 2PC (§3.3); **cutover** copy→streaming (this is where the
"cursor persist/resume" acceptance moved from MM2). Streaming staging also honors the
`local_infile`-OFF fallback (§0.1/M5).
**Acceptance (5.7→8.4) — keystone correctness tests:**
- **Commit-order hazard:** uncommitted low-`delta_id` row not seen/purged by a pass; after commit, a
  second pass applies it. (InnoDB auto_increment assigns at insert, visible at commit — same skew as
  PG `nextval`.)
- **Delete-by-id read-and-clear race:** X=V1 processed while source updates X→V2 → only observed ids
  deleted → V2 survives and converges.
- **Crash between apply and track-write:** replays without duplication (idempotent upsert).
- **Coalescing:** N updates to one PK → one re-read, final-value convergence.
- **`ON UPDATE` timestamp fidelity (§0.4/M2):** a row with an `ON UPDATE CURRENT_TIMESTAMP` column
  applies the **source** value, not "now".
- End-to-end insert/update/delete/PK-change converge; **cursor persists + resumes across restart**;
  emits F2 metrics/spans.
*(Secondary-unique protection is the MM1b pre-flight block, not an MM5a runtime backstop — there is no
viable ODKU-compatible runtime check, per §0.4 / Momus 2nd-pass M3.)*
**Depends on:** MM4a (transport + copy). *(MM3 transitive. NOT MM4b — MM5a is single-component acyclic
streaming and does not need the cyclic initial-copy milestone; cyclic streaming is MM5b — Momus 2nd-pass
M4.)*

### MM5b — FK-ordered apply + retry fallback + cyclic streaming (no deferral)
**Goal:** Multi-table referential correctness within a component **without deferrable constraints**.
**Deliverables:** **per-component apply** — upserts parent→child, deletes child→parent (§3.3, §8.1) —
in one transaction with **FK checks ON** for **acyclic** components; **transient-FK mapping: errno 1452
/ SQLSTATE 23000 → `engine.TransientConstraintError`** so the neutral retry fallback handles cross-pass
deps — **acyclic path only** (a cyclic component is routed to `FK_CHECKS=0` and never raises 1452
mid-pass, so an intra-pass cyclic 1452 must NOT be wrapped as transient — Momus 2nd-pass m2);
**cyclic/self-ref components: `FOREIGN_KEY_CHECKS=0` for the component pass + PRE-COMMIT orphan-
verification (SET FK_CHECKS=1, orphan anti-join over the component's FK edges within the open txn,
loud error + rollback on any orphan, before COMMIT) → nothing broken ever visible** (§0.2/B3 + 2nd-pass
B1 — mandated, not opt-in, because retry provably can't close an intra-pass cycle; **post-commit
verification is rejected**); the **anti-join covers all component tables, not just the pass's dirty
rows** (a `DeleteAbsent` under `FK_CHECKS=0` can strand a non-dirty child — Momus 2nd-pass m1);
**implement the interface plumbing** to carry the **cyclic flag + component FK-edge list** into
`BeginApply`/`ApplyTx` (the apply layer receives only `comp.Order` today, and FK edges live on
`engine.Table.ForeignKeys` but are never passed to the sink — this is a **real, bounded interface
change that also touches the Postgres sink's `BeginApply` signature**, decided/implemented here BEFORE
the FK_CHECKS acceptance gate — Momus 2nd-pass M2); **retry termination policy:** bounded exponential
backoff, then **halt loud + observable** (§4.2), never infinite thrash; **genericize the
`component.go` PG-deferral doc-comment** (m1); **the CLAUDE.md exception (if not already in MM4b) is
confirmed present.**
**Acceptance (5.7→8.4):** acyclic multi-table component stays referentially consistent with checks ON;
a **cross-pass** dependency converges via retry; a **`NOT NULL` mutual cycle converges via
`FK_CHECKS=0` + pre-commit verification** (the case retry alone **cannot** close — asserted directly);
an injected orphan (missing parent AND wrong-parent) is **caught pre-commit, rolled back, halts loud**
(never committed/visible); a `DeleteAbsent` that would strand a **non-dirty** child is **caught by the
full-component anti-join**; a deliberately unsatisfiable non-cyclic dependency **exhausts bounded retry
and halts loud** (no thrash/skip); per-component transaction bounded by churn/pass; emits F2
metrics/spans.
**Depends on:** MM5a.

### MM5c — Bloat control: purge + bounded retention + forced reseed (MySQL)
**Goal:** Protect a source we may not own; survive a stalled **InnoDB history list** (xmin analog).
**Pre-work (oracle):** confirm the engine-neutral reseed state machine (`docs/reseed-state-machine.md`)
needs no MySQL change — or enumerate `TEMP`/`FK_CHECKS`/orphan-verify interactions during re-copy.
**Deliverables:** **batched consumption-gated purge** (plain `DELETE` — no autovacuum) with **optional
per-table `OPTIMIZE TABLE`** and/or **RANGE-partition + `DROP PARTITION`** (5.7+/8.0) as the
space-reclamation path (PG partition+DROP analog, opt-in); **bounded retention (age+size) + forced
reseed** of over-cap targets (reuse neutral `internal/reseed`). **MySQL bloat physics differ:** InnoDB
doesn't shrink the tablespace on DELETE, and a **long-running source transaction inflates the history
list length, blocking purge** — the direct xmin-horizon analog — **surfaced loudly** in telemetry.
**Acceptance (5.7→8.4):** convergence **after reseed under continuous source writes**;
**history-list-stall test:** a long-running source transaction inflates the history list → delta bloat
grows AND is surfaced loudly; bounded-retention/reseed still protects the source; over-cap target →
reseed bounds growth; emits F2 metrics/spans.
**Depends on:** MM5b.

### MM6 — Observability verification (REUSED F2)
**Goal:** Verify the reused F2 contract end-to-end for MySQL and the operator surface. *(Genuinely
mostly reuse — the engine instruments as it goes through MM1–MM5c against the neutral registry.)*
**Deliverables:** confirm the MySQL engine emits the declared metrics/spans/log events; confirm the
neutral status HTTP API + CLI `status` report MySQL syncs; the cross-channel target-unreachable signal
(§3.4/§10) fires for a downed MySQL target.
**Acceptance:** a scrape asserts the **named** series with expected labels for a live MySQL sync; a
drain pass against a downed MySQL target yields the span-error + escalating-log + gauge/backlog
trifecta; status API reports MySQL phase/lag/progress.
**Depends on:** MM5c.

### MM7 — Daemon, CLI & multi-sync + engine coexistence (REUSED core; real integration)
**Goal:** MySQL syncs run under the real daemon; two engines coexist. *(Not pure reuse — first time two
engines run in one daemon; dual-harness — Sisyphus M3/hidden effort.)*
**Deliverables:** verify the neutral daemon manages MySQL syncs (multiple named, worker pools, graceful
SIGTERM drain+checkpoint); CLI `run`/`validate`/`status`/`capture install|remove`/`reseed` on MySQL
configs; a **mixed multi-sync** — a MySQL sync **and** a Postgres sync in one daemon on the
**dual-harness** — runs concurrently (single-engine rule holds per-sync; stronger than Postgres M7).
**Acceptance:** ≥2 concurrent MySQL syncs on the pair; a MySQL + Postgres sync coexist on the
dual-harness; SIGTERM drains+checkpoints cleanly; ungraceful kill resumes; CLI exercises the MySQL
lifecycle.
**Depends on:** MM6, MM5c.

### MM8 — Packaging & docs (MySQL, feature-complete)
**Goal:** MySQL installable and documented.
**Deliverables:** the existing static pure-Go binary now links the MySQL driver (still CGO-free);
**MySQL grant SQL** (`deploy/grants-source-mysql.sql`, `deploy/grants-target-mysql.sql`, incl.
multi-DB SELECT/TRIGGER, replicare-db owner, target `CREATE TEMPORARY TABLES`, `local_infile` note);
example MySQL config; MySQL sections in getting-started/configuration/operations/troubleshooting (incl.
the two MM2 wrinkles, the `local_infile` requirement, and the FK-cycle/`FK_CHECKS` behavior); a
**MySQL demo** (compose: 5.7 source + 8.4 target + replicare); README status (Postgres + MySQL shipped).
**Acceptance:** binary runs a MySQL sync via the systemd sample; the MySQL compose demo replicates
end-to-end; MySQL quickstart works.
**Depends on:** MM7.

### MM9 — Hardening & E2E gate (MySQL shippable gate)
**Goal:** Confidence under faults, scale, version spread. **Passing MM9 = MySQL shippable.**
**Deliverables:** the full **5.7→8.4 integration run**; **fault-injection** (kill mid-install, crash,
target down, slow-but-reachable target, history-list stall); **throughput/perf benchmarks with recorded
thresholds + non-regression assertions**, including a **coarse `local_infile`-OFF fallback bound**
(§0.1/m4) — not a hand-wave; expanded byte-fidelity corpus.
**Acceptance:** pair green; perf numbers recorded with explicit thresholds (incl. the fallback bound);
fidelity corpus passes; retention/reseed proven to bound source growth under a stalled history list at
scale; slow-but-reachable target handled via backpressure without unbounded memory.
**Depends on:** MM7 (parallel with MM8).

---

## Critical path (resequenced per both reviews)
`MM0 → MM0.5 → MM1a → MM1b → {MM2′, MM3} → MM4a → MM4b → MM5a → MM5b → MM5c → MM6 → MM7 → {MM8, MM9}`.
**F2 is reused, not rebuilt** (MM6 verifies). The **FK_CHECKS decision (§0.2) and the byte-faithful
transport + strict-`sql_mode` decisions are made up front** (§0), so no milestone carries an undecided
fork into its acceptance. **First visible end-to-end: MM4a** (acyclic copy). **First streaming
end-to-end: MM5a.** Feature-complete: **MM8.** Shippable: **MM9 gate.**

## Reuse ledger (CONFIRMED by the code inventory, with the cyclic-apply caveat)
Reused **unchanged**: `internal/copy`, `internal/reseed`, `internal/pipeline`, the `internal/state`
interface, `internal/observability`, and the neutral half of `internal/config`; the daemon dispatches
purely through `engine.Get()` + `engine.*` — **no per-engine switch**. **`internal/apply` orchestration
is reused, but MySQL's `ApplyTx` implements `FOREIGN_KEY_CHECKS=0` + **pre-commit** orphan-verification
for cyclic components** (vs PG's `SET CONSTRAINTS DEFERRED`), which needs a **real (bounded) interface
change threading a cyclic flag + component FK edges into `BeginApply`/`ApplyTx`** — this also touches
the Postgres sink's signature, so it is **not purely additive** (decided/implemented in MM5b before the
FK_CHECKS gate). So the apply layer is **reused for the acyclic path, extended at the engine+interface
boundary for the cyclic path** (corrects the v1 "reused unchanged" and the v2 "minimal backward-
compatible" overclaims — Momus B3 + 2nd-pass M2). **No `internal/delta`/`internal/schema` package exists** — delta/track is MySQL-new engine
code; "schema" is the neutral `engine.Schema` type set. **`internal/state/postgres` is deliberately
always-Postgres** and orthogonal to the data engine (MM2). **The three additive seams:** `engine.
Register` (init), `config.RegisterEngine` (init), and the one hand-edit to `cmd/replicare/engines.go`.

## Resolved decisions (were v1 Open Questions)
1. **FK cycles** → scoped `FOREIGN_KEY_CHECKS=0` + **mandatory PRE-COMMIT orphan-verification → loud
   halt + rollback** (verify inside the open txn before COMMIT, so nothing broken is ever visible —
   Momus 2nd-pass B1), for both initial copy (MM4b) and streaming (MM5b); nullable cyclic →
   NULL-then-fill; acyclic → checks ON + retry fallback. Requires a **bounded interface change** to
   thread the cyclic flag + component FK edges into `ApplyTx` (touches the PG sink too — MM5b).
   **Overturns CLAUDE.md §4.1/§14/§15 → committed edits land in MM4b.** (Momus B3/B5/M9 + 2nd-pass
   B1/M2; retry-alone is provably insufficient for intra-pass cycles.)
2. **Charset fidelity** → **byte-faithful `character_set_results=binary`** transport + full byte-level
   LOAD DATA escaping (MM1a/MM4a). (Momus B4/B6.)
3. **Zero-dates** → target session keeps `STRICT_*`, drops only `NO_ZERO_DATE`/`NO_ZERO_IN_DATE`
   (MM1a/MM1b). (Momus B2.)
4. **MariaDB** → **out** for v1. **Floor** → **5.7 → 8.4**, one CI pair (MM0). (Sisyphus M4, Open Q7.)
5. **StateStore** → stays Postgres for MySQL syncs; MySQL syncs are strictly at-least-once; MySQL
   StateStore is a future item (MM2). (Momus m1.)
6. **`local_infile` OFF** → text-literal batched-INSERT fallback (byte-faithful), copy + streaming;
   coarse perf bound in MM9. (Momus M5, Sisyphus m4.)

## Remaining open items (bounded, decided within the noted milestone)
- The **`BeginApply` cyclic-signal shape** (MM5b) — *which* signal within the already-decided
  (non-additive) interface change: an explicit cyclic flag vs. engine self-detection from the passed
  component FK edges. A small choice inside a settled interface change (which does touch the Postgres
  sink signature — §0.2/MM5b/reuse ledger); **not** a backward-compatible or minimal one.
- Whether the **secondary-unique** pre-flight block is hard-block or loud-warn-with-opt-in-override
  (MM1b) — default hard-block; override documented.
- **`OPTIMIZE`/partition-DROP** as the MySQL space-reclamation default vs opt-in (MM5c) — default
  batched-DELETE + opt-in reclamation, mirroring the Postgres decision.

## How we execute (Sisyphus)
Each MMx is a task group = one branch + PR (the Postgres flow). MySQL-dialect SQL, the byte-faithful
escaper/charset mechanics, and the `FK_CHECKS`+verification path delegate to oracle; docs to
document-writer at MM8. A milestone closes only when its acceptance tests pass on the 5.7→8.4 pair.
**Plan/decision changes are mirrored into `CLAUDE.md` — specifically the §4.1/§14/§15 MySQL FK-cycle
exception lands in MM4b** (change-discipline rule, Momus B5).

## Review disposition
**Sisyphus (execution) — NEEDS-DECOMPOSITION, addressed:** B1 (MM4→MM4a/MM4b), B2 (MM2 shrunk;
progress/cursor acceptance moved to MM4a/MM5a), M1 (MM1→MM1a/MM1b), M2 (unique-per-load reader name +
concurrency test — MM4a), M3 (ports 3340/3341, `local_infile=ON`, dual-harness, `integration-mysql`
job — MM0), M4 (concrete 5.7→8.4 pair, "matrix"→"pair" throughout), m1 (genericize `component.go`
doc — MM5b), m2 (dropped false MM3→MM2 edge), m3 (MM4a states MM1 dep), m4 (fallback perf bound — MM9),
m5 (first visible E2E = MM4a, noted).
**Momus (design) — REVISE, addressed:** B1 (secondary-unique pre-flight block + halt-loud test —
MM1b/MM5a), B2 (strict-safe `sql_mode`, keep `STRICT_*` — §0.4/MM1), B3 (retry can't close intra-pass
cycle → `FK_CHECKS=0`+verify mandated, reuse-ledger corrected — §0.2/MM5b), B4 (byte-faithful
`character_set_results=binary` decided — §0.1/MM4a), B5 (CLAUDE.md §4.1/§14/§15 change-discipline
committed to MM4b + orphan-verification safeguard), B6 (full byte-level escape set + BLOB fuzz corpus —
§0.1/MM4a), M1 (implicit-COMMIT DDL → idempotent checkpointed install/migration — §0.5/MM3), M2
(`ON UPDATE CURRENT_TIMESTAMP` override — §0.4/MM5a), M3 (non-InnoDB block — §0.5/MM1b), M4
(pre-existing-trigger block — §0.5/MM1b), M5 (text-literal fallback, copy+streaming — §0.1), M6
(schema=database multi-DB semantics — §0.5), M7 (ci-collation → `COLLATE utf8mb4_bin` keyset — §0.3),
M8 (version-branched defined-order boundary scan — §0.3), M9 (FK_CHECKS decided up front, single-outcome
acceptance). Minors m1–m6 (at-least-once note, `CREATE TEMPORARY TABLES`+`DEFINER` grants, reader
concurrency/security, `utf8mb4→utf8mb3` block, MM2 assertable acceptance, full six-mode TLS mapping) all
folded.
**Momus pass 2 (design, on v2) — REVISE, addressed in v3:** 2B1 (verification was post-commit →
committed corruption; corrected to **pre-commit verify + rollback inside the txn** — §0.2/MM4b/MM5b),
2B2 (byte-faithful **write** side unspecified + `LOAD DATA` single-`CHARACTER SET` vs mixed-charset
tables; now specified: single-charset validate-and-halt-loud load + mixed-charset pre-flight flag +
per-column-group loads — §0.1/MM4a), 2M1 (impossible cross-charset "byte-identical" corpus split into
same-charset-identical vs cross-charset-halt-loud — §0.1/testing/MM4a), 2M2 (orphan-verify + cyclic
routing need FK edges + cyclic flag threaded into `ApplyTx` — a **real bounded interface change that
touches the PG sink**, not "minimal"; scoped in MM5b before the FK_CHECKS gate), 2M3 (secondary-unique
runtime backstop is infeasible with ODKU → dropped; **pre-flight block is the sole protection** —
§0.4/MM5a), 2M4 (MM5a→MM4b edge incoherent → MM5a depends on **MM4a only**), 2m1 (anti-join covers the
**whole component**, catching delete-stranded non-dirty children — MM5b), 2m2 (1452→transient mapping
is **acyclic-only** — MM5b), 2m3 (`character_set_results=binary` metadata-safety tested in MM1a).
**Momus pass 2 "verified good — do not touch":** the B3 retry-can't-close-an-intra-pass-cycle diagnosis,
the strict-safe `sql_mode` (version-correct for 5.7→8.4), the B6 escape set, read-side byte fidelity,
all Sisyphus decomposition, and the M3-M8/grants/TLS/DDL fixes.
**Momus pass 3 (confirmation, on v3) — APPROVED after two one-line fixes:** verified pass-2 B1's
pre-commit mechanism against `component.go`'s real control flow (Commit-returns-error → `committed`
stays false → deferred rollback fires → no orphan ever durable/visible; the orphan-verify error is
non-1452 so it halts immediately and never thrashes the retry loop), and confirmed B2/M1/M2/M3/M4 and
all minors properly closed. Two residual consistency nits, now fixed in v3: (3-Major) the "Remaining
open items" line still called the `BeginApply` change "minimal, backward-compatible," contradicting the
rest of v3 → rescoped to "which signal shape, inside the settled non-additive interface change";
(3-Minor) the mixed-per-column-charset flag was assigned to MM1b in prose but absent from MM1b's
deliverables/acceptance → added to MM1b (and a per-charset-group load case to MM4a). Pass 3's standing:
"fix those two lines and this is APPROVE-on-sight — do not open a fourth deep pass; the architecture and
sequencing are settled."
**Status: execution-ready. Next action = MM0** (engine skeleton + `go-sql-driver/mysql` + 5.7→8.4
harness/CI pair + MF3/MariaDB decision doc).
