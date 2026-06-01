# replicare

`replicare` is an open-source, high-performance data replication daemon. You install it
somewhere, point it at a **source** database and one or more **target** databases, and it:

1. performs an **initial copy** of the selected data, then
2. continuously replicates **deltas** — inserts, updates, and deletes — keeping the targets
   converged with the source.

It is written in **Go** and ships as a single static binary. Postgres is the first supported
engine; **MySQL and Redis are planned** and the architecture must accommodate them from day one.

> This document is the durable context for all future sessions. Read it fully before making
> architectural decisions. If you change a decision here, update this file in the same change.

---

## 1. Guiding principles (the non-negotiables)

These came directly from the project owner and shape every other decision:

1. **No privileged change stream — ever.** We assume we will **not** have access to the
   Postgres WAL, the MySQL binlog, or a durable Redis change feed. CDC must work with ordinary,
   grantable privileges. (This is the single most important constraint — see §3.)
2. **No full superuser required.** The daemon must run with the *least* privileges that still
   allow it to install its capture machinery and read data. Document the exact grants needed.
3. **Highly performant.** Parallel initial copy, batched delta apply, minimal overhead on the
   source. Performance is a feature, not an afterthought.
4. **Deeply observable.** Extensive structured logs **and** telemetry: initial-copy progress,
   replication lag, queue depth, throughput, errors, per-table state. An operator should always
   be able to answer "what is it doing right now and how far behind is it?"
5. **Engine-agnostic core.** Postgres is first, but MySQL and Redis are coming. The replication
   pipeline, state management, observability, and config must be engine-neutral; engine
   specifics live behind driver interfaces.
6. **Source databases may be very old.** A source Postgres might be many major versions behind
   the target. All source-side SQL (trigger functions, capture tables, introspection queries)
   must stay **conservative and version-tolerant**. The target is typically newer, so
   target-side apply may use modern features (e.g. `INSERT ... ON CONFLICT`).
7. **Faithful transport — never transform.** replicare moves each value **verbatim** and lets the
   target's own input function validate it. It **never** coerces, truncates, casts, or "repairs"
   data to fit — that is exactly how silent corruption happens. On a value/type error we fail
   **loudly**, never mangle. This is a hard product promise: **no value-transform capability
   exists, even as an opt-in.** (See §4.2.)

---

## 2. Project facts

| Thing | Value |
|---|---|
| Name | `replicare` |
| License | **MIT** |
| Language | **Go 1.23+** |
| Go module path | `github.com/rudimk/replicare` (placeholder; may be renamed/moved to an org later) |
| Postgres driver | **`jackc/pgx` v5** (fast, native `COPY` + batch support) |
| Target Postgres | 12+ recommended (apply path may assume `ON CONFLICT`, i.e. 9.5+) |
| Source Postgres | Best-effort back to old majors (9.x and older where feasible) — keep source SQL conservative |
| Distribution | Single static binary + sample `systemd` unit (Helm chart is a *future* goal; YAML config would be injected via chart values) |
| Config | **YAML config file** (primary), with env-var overrides. Secrets may be inline in YAML (with env override); per-connection TLS configurable |

---

## 3. CDC strategy — capture-and-reconcile (NOT change-stream)

Because we never get a privileged/durable change stream, **every engine** uses the same
high-level pattern:

> **Capture changes with grantable mechanisms → reconcile by re-reading current source state →
> apply idempotently to targets → checkpoint progress. Never trust a change feed we don't own.**

### 3.1 Postgres (and MySQL): trigger-based CDC (Bucardo-style)

We use **triggers**, modeled on how **Bucardo** works:

- In a dedicated schema on the **source** (e.g. `replicare`), create per-table **delta tables**
  and **AFTER INSERT/UPDATE/DELETE triggers**.
- The trigger records **only the changed row's primary key** (plus op type, a sequence/txid, and
  a timestamp) into the delta table. **It does NOT store the full row image** — this keeps
  trigger overhead and storage minimal. *(Decision: PK-only capture.)*
- **PK updates are a special case:** an `UPDATE` that changes the key (`OLD.pk <> NEW.pk`) must
  enqueue **both** keys — the **old** PK (so the target row under the old key is deleted) and the
  **new** PK (so the row is upserted under the new key). Effectively a PK change = delete(old) +
  upsert(new). This is also why keyset copy chunking can treat PKs as immutable (§4.1): any PK
  mutation is reconciled through the delta layer.
- At sync time, the daemon reads new delta rows, **re-reads the current values of those PKs from
  the live source table** (a JOIN back to the real table), and applies them to the target. For
  deletes, the PK in the delta is sufficient. This is **self-healing**: we always copy the
  latest value, so transient intermediate states don't matter and overlapping copy/stream
  windows reconcile naturally.
- A **per-target `track` table** records which deltas have been confirmed applied (needed for
  fan-out), so consumed deltas can be safely purged to control bloat. See **§3.3** for the full
  sequencing/consumption model and the commit-order hazard it avoids.

**Implications to keep in mind:**
- This **writes to the source** (delta tables + triggers). That is inherent to trigger CDC and
  must be documented as a requirement, not a surprise.
- Every replicated table **must have a primary key or usable unique key**. *(Decision: tables
  without one are **skipped with a loud warning** surfaced in logs and telemetry. A future option
  may let the user designate identity columns explicitly.)*
- Keep trigger functions and capture-table DDL in **broadly compatible PL/pgSQL / SQL** so they
  work against old source servers.

**MySQL** will use the same trigger-based approach (we assume **no binlog access**, just as we
assume no WAL access for Postgres). It is the MySQL implementation of the same `Source` interface.

### 3.2 Redis: SCAN-based snapshot + reconciliation scans

Redis has no privileged durable change feed we can rely on. Findings on hosted providers:

- **ElastiCache:** keyspace notifications are supported (enable `notify-keyspace-events` via a
  custom parameter group).
- **Redis Cloud / Enterprise:** generally enableable, but in **clustered** databases events are
  **node-local** (must subscribe per shard).
- **Universal problem:** keyspace notifications are **fire-and-forget Pub/Sub** — events are
  **lost on disconnect, with no persistence or replay**, and they carry only the key + event
  type, **not the value**.

Therefore, for Redis:
- **Source of truth = `SCAN`-based snapshot + periodic incremental reconciliation scans.** This
  works on every provider with ordinary privileges and is crash/restart-safe.
- **Keyspace notifications are an OPTIONAL low-latency accelerator** where available, used to
  reduce lag between reconciliation passes — but **never relied on for correctness**. A missed
  notification is always caught by the next reconciliation scan.

### 3.3 Delta sequencing & consumption (trigger CDC) — the dirty-key-set model

**The hazard we design around (commit-order ≠ assignment-order):** any value a trigger assigns
*inside* a transaction (a `nextval()` sequence, a `txid`, a timestamp) is fixed when the trigger
**fires**, but the delta row is only **visible** to the daemon when the transaction **commits** —
and transactions commit in a different order than they grab those values. So the naive design
(*"add `seq bigint`, read `WHERE seq > last_seq`, checkpoint `max(seq)`"*) is **broken**: a slow
transaction holding a lower seq that commits *after* the daemon's read is **skipped forever**.
This is why Postgres logical decoding uses commit-order LSN — which we don't have. Postgres'
true commit-order signal (`track_commit_timestamp`) needs a server GUC + restart, the same
privilege class we assume we lack. **Therefore a scalar high-water-mark cursor is never our
correctness mechanism** — only a lag/observability signal.

**The model we use (decisions):**

- **Delta table = a transactional *dirty-key set*, not an ordered log.** Because capture is
  PK-only and we re-read the current value at sync time, we only need to know a PK is **dirty**.
  Multiple changes to the same PK **coalesce** into one re-read (bounded work under churn). On
  re-read: row present → upsert; absent → delete (op type can be derived). Cross-PK ordering is
  irrelevant for convergence.
- **Consumption = set-difference against a per-target `track` table** (Bucardo model). Each pass
  processes `delta MINUS track` for a given target and writes track. An uncommitted delta isn't
  visible yet; a late-committing low-position row is simply "a delta not yet in track" → caught
  next pass, **never skipped**. This sidesteps the commit-order hazard entirely.
- **`track` (and `delta`) live on the *source* DB**, in the `replicare` schema. Consume +
  track-write + delta-purge happen in **one atomic source transaction**, keeping the delta
  lifecycle self-contained. (The external `StateStore` holds copy-progress, lag cursors, and the
  ownership lock — not per-row track state.)
- **Delete-by-`delta_id`, never by PK** — the read-and-clear race: while the daemon processes
  PK X (value V1), the source may UPDATE X→V2, inserting a *new* delta row. Each delta row has a
  unique `delta_id` (bigserial or `ctid`); the daemon deletes only the specific ids it observed,
  so V2's row survives and X is reprocessed → converges. Deleting "all deltas for PK X" would
  lose V2.
- **Crash safety / at-least-once without 2PC:** order is **read deltas → apply to target → only
  then write track / purge deltas on source.** A crash before the final step leaves deltas in
  place → reprocessed → idempotent re-read+upsert → harmless. (When the `StateStore` is the
  target DB, the track-write can even ride the apply transaction.)
- **Purge:** a delta row is deletable once present in **all** targets' track (fan-out safe).
- **`bigserial`/seq column:** kept for **lag metrics and ordering hints only**, never as the
  resume cursor.

**Foreign keys on the target — dependency-ordered apply (decision):** since we apply coalesced,
otherwise-unordered batches, we **topologically sort by the FK graph**: **upserts apply
parent→child, deletes apply child→parent (reverse)**. This ordering is always complete because
apply is scoped to an **FK connected component** (see §8.1) — no edge crosses the unit boundary.
**FK cycles and self-references cannot be topo-sorted**, so a **retry fallback** is required: a
row that hits a transient FK violation stays dirty (its delta isn't tracked/purged) and is
retried on a later pass once its dependency lands. This keeps us correct and self-healing without
needing deferrable constraints or elevated target privileges.

### 3.4 Delta purge, retention & source bloat

The delta table is a **high-churn queue table** (trigger inserts + purge deletes) — a classic
Postgres bloat antipattern — and **we are writing it to a source we may not own**, so bounding our
footprint is a first-class concern, not an afterthought.

**Per-table delta tables (decision).** Each replicated table has its **own** delta table (and its
trigger writes only there). This isolates bloat and write contention per table, enables per-table
autovacuum tuning and per-table truncate/partition, and avoids the single-shared-table write
hotspot. (Track tables are per (target, table).)

**Purge mechanism (decision: batched DELETE + aggressive autovacuum, universal; partition+DROP
opt-in on PG≥10).**
- **Baseline (any version):** a throttled, **consumption-gated batched `DELETE`** removes delta
  rows once consumed by **all** targets (present in every target's track / below the min confirmed
  position across targets). Batched + rate-limited to bound transaction churn.
- **Aggressive per-table autovacuum** is set on our delta/track tables at capture-install time via
  owner-level `ALTER TABLE … SET (autovacuum_vacuum_scale_factor=0, autovacuum_vacuum_threshold=…,
  autovacuum_vacuum_cost_delay=0, …)` — **no superuser, no global change**; keeps dead-tuple
  cleanup prompt without touching the source's global settings.
- **Optimization (PG≥10, opt-in):** **time-partitioned** delta tables purged by **`DROP`/`TRUNCATE`
  of fully-consumed partitions**. Reclaims space instantly, creates no dead tuples, and **dodges
  the `xmin`-horizon trap** (below). This is the real bloat-proof path where the source is modern.

**The `xmin`-horizon trap (documented risk).** Autovacuum cannot reclaim dead tuples older than the
source's oldest running transaction / held-back `xmin` (long analytics queries, idle-in-transaction
sessions, other slots — none of which we control). Under such conditions the batched-DELETE path
**can bloat regardless of our autovacuum settings.** The partition+`DROP` path avoids this; on old
sources we can only **monitor and surface it loudly** (see telemetry).

**Bounded retention + forced reseed (decision: configurable, bounded default).** A slow or down
target pins the queue (deltas can't purge until all targets consume) → unbounded source growth. To
protect the source: cap delta retention by **age and/or size**. A target exceeding the cap is
marked **"needs reseed"** — its track is reset and a full re-copy is scheduled on recovery — and
deltas beyond the cap are purged. Default **on** with configurable limits; trades a target re-copy
for a protected source. (Single-target is the same logic with one consumer.) Alternatives — pure
alert-only (risks filling the source disk) and hard-stop-the-sync — were rejected as defaults.

**Re-read load & the lag↔load tradeoff.** Consuming re-reads current values for dirty PKs; high
churn → more re-read SELECTs. **Per-PK coalescing** (one re-read per PK per drain pass) bounds it;
a **longer drain interval coalesces more (less source load) but raises lag.** Drain interval, batch
size, and re-read concurrency are configurable, defaulting to conservative source pressure.

**Telemetry (first-class).** Surface per (target, table): **delta backlog** (unconsumed rows/bytes),
**oldest-unconsumed delta age**, estimated **delta dead-tuple/bloat**, **purge rate**, and
**reseed events**. These are the headline signals for "is the source footprint healthy?"

**Target-unreachability must be loud across ALL three channels (decision).** When a target becomes
unreachable (or unhealthy) and its delta backlog starts growing, it must be reflected in **logs,
metrics, AND OTel traces** — not just one:
- **Logs (slog):** structured WARN→ERROR events on connection loss/recovery, with target id, error,
  and current backlog/age; escalating as the backlog grows and as the **retention cap** is
  approached (and a distinct event when a target is marked **needs-reseed**).
- **Metrics (Prometheus):** a target-reachability/up gauge, plus the growing **delta backlog** and
  **oldest-unconsumed-age** series per target — so alerts fire on both "target down" and "backlog
  climbing toward the cap."
- **Traces (OTel):** apply/drain spans for an unreachable target record the failure (span status =
  error, connection-error events/attributes) and carry backlog/age attributes, so the stall and its
  blast radius are visible in the trace timeline.

---

## 4. Initial copy & cutover

- **Triggers/capture are enabled FIRST**, so changes begin queuing into delta tables immediately.
- **Then** the daemon does a **chunked, parallel bulk copy** of each table (Postgres: `COPY`,
  chunked by PK ranges across a worker pool). **Within an FK component, tables are copied in topo
  order (parents first)** so target FK constraints hold during load; **distinct components copy in
  parallel** (see §8.1). Initial copy is chunked, **not** atomic.
- Because capture is already running and apply is idempotent (PK-keyed upsert, re-reading current
  values), **any change during the copy window reconciles automatically** when the delta queue is
  drained. **We do not require a frozen point-in-time snapshot.**
- This deliberately solves the **"the source is too large to snapshot"** problem: there is no
  single long-held snapshot/transaction; copy proceeds in chunks and deltas fill the gaps.
- After bulk copy completes, the daemon transitions the table to **streaming** (drain delta queue
  continuously).

### 4.1 Chunked-copy mechanics

**Chunking (decision: keyset PK-range default, ctid fallback).**
- **Keyset / PK-range** is the default: cut the PK/unique key space into balanced `[lo, hi)`
  ranges and copy each range as `COPY (SELECT … WHERE <range>) TO STDOUT`. Works for any
  comparable key including **composite** keys via row-value comparison
  (`WHERE (k1,k2) > ($1,$2) ORDER BY k1,k2 LIMIT n` — old-PG-safe, uses the multicolumn index)
  and UUID/text/timestamp keys. **Resumable** (the cursor *is* a key) and robust to concurrent
  row movement (PKs are effectively immutable; PK *changes* are reconciled by the delta layer —
  see §3.1).
- **Boundary discovery:** an **index-only scan of the key column collecting every Nth key**
  yields balanced ~N-row ranges regardless of key distribution (cheaper and far less skew-prone
  than equal-width min/max splitting). Ranges are handed to a worker pool.
- **ctid / block-range fallback** (`ctid >= '(p,0)' AND ctid < '(q,0)'`, sized from
  `pg_class.relpages`): for pathological key distributions or very wide composite keys. ctid is
  unstable under concurrent writes — acceptable, since moved/missed rows are reconciled by deltas.
- **Native declarative partitions** are copied per-partition as a free chunking win when present.

**Wire format (decision: text/CSV default, binary opt-in).** Stream `COPY … TO STDOUT` (source)
→ `COPY … FROM STDIN` (target) piped via `io.Pipe` so a chunk never fully buffers. **Text/CSV is
the default** for portability across very different PG versions and custom types (old→new).
**Binary** `COPY` is opt-in when source/target versions are close (faster, type-exact, but
brittle across major-version gaps).

**Target load (decision: empty-target direct COPY, delete-range resume).**
- We **never touch the user's target indexes/constraints** (not our schema; index-maintenance
  cost during load is accepted and documented).
- **Fast path:** direct `COPY FROM STDIN` per chunk; mark the chunk complete in the StateStore.
  **Resume without staging:** for any chunk not marked complete, `DELETE FROM target WHERE <pk
  range>` then re-`COPY`. Safe because during a table's initial-copy phase the target table is
  written *only* by the copier (delta apply for that table begins after copy completes), so
  range-delete + re-copy is exclusive and idempotent.
- **Merge path (auto when target non-empty / opt-in):** `COPY` into a `TEMP` staging table then
  `INSERT … ON CONFLICT DO UPDATE`. Privilege-light, idempotent, ~2× write amplification.

**FK components (decision: parents-before-children, direct).** Within a component, copy tables in
**topo order (parents first)** so live target constraints hold during load; **chunks within a
table run in parallel, and distinct components run fully in parallel.** Lower peak disk than
stage-all. **Cyclic / self-referential FKs** within a component are a documented edge case
(nullable self-refs are the common form; worst case lean on the §3.3 retry fallback or require
the user to flag the FK) — not over-engineered now.

**Documented defaults (not separately decided):**
- **Per-chunk progress in the Postgres StateStore:** a completed-range watermark plus a sparse set
  of out-of-order completed ranges, so restarts skip finished chunks.
- **Polite-client throttling:** configurable `max_source_connections` / `max_target_connections`,
  chunk size (rows), and concurrent-chunk caps — we may not control the source, so default to
  conservative source pressure.

### 4.2 Type fidelity & faithful transport

**Governing principle (§1.7): replicare is a faithful transporter, not a type translator.** It
moves values verbatim and lets Postgres validate; it never coerces, truncates, casts, repairs, or
otherwise transforms data. **No value-transform capability exists in the product — not even as an
opt-in.** This applies to *both* the initial-copy path and the delta-apply path.

**Verbatim text transport.** With text `COPY`, each value is rendered by the **source** server's
output function and parsed by the **target** server's input function — we are a pipe, never an
interpreter. For the vast majority of types (int, numeric, text, bool, uuid, json/jsonb, arrays,
ranges, inet, …) the text form is **stable across versions and lossless**. If the target can't
parse a value, it errors — the loud failure we want (see error policy below), never a corruption.

**Canonicalize the session, never the data.** A few types have text output that depends on
session GUCs. We pin these on **every source-read and target-write connection** — all are
session-level `SET`s requiring **no special privileges**, and they change *formatting only*:

| GUC | Value | Why |
|---|---|---|
| `DateStyle` | `ISO, YMD` | Unambiguous date text + input order (kills MDY/DMY ambiguity) |
| `TimeZone` | `UTC` | Deterministic `timestamptz` render/parse |
| `extra_float_digits` | `3` | **Critical for old sources (<12):** round-trippable `float4`/`float8`; else precision loss |
| `IntervalStyle` | `postgres` | Stable `interval` text |
| `bytea_output` | `hex` | Consistent `bytea` text (target input accepts both regardless) |
| `client_encoding` | `UTF8` | Well-defined transcoding across DB encodings; invalid bytes → loud error |
| `lc_monetary` | pinned | `money` output is locale-dependent — **warn against `money`**; pin if present |

**Format = text, not CSV** (CSV conflates empty-string vs `NULL`; text's `\N` is unambiguous).

**The delta-apply path must be as faithful as copy.** Re-read→upsert could lose fidelity if values
round-trip through Go's type system in the driver. To preserve the "Postgres parses, we don't
interpret" guarantee, **delta apply also moves values as text** — preferred mechanism: `COPY` the
re-read rows (text) into a `TEMP` staging table, then `INSERT … ON CONFLICT` from it (deletes by
key). This reuses the identical faithful transport; delta batches are small (churn/pass) so
staging overhead is modest. (Alternative: text/simple-protocol parameterized upserts.)

**Anti-breakage mechanics (these are NOT transforms — they're about *which columns* and *how to
address* them, never about altering values):**
- **Explicit, name-matched column lists** — never positional `COPY`. Match source↔target by column
  **name**; reconcile added/dropped/reordered columns explicitly.
- **Exclude `GENERATED … STORED` columns** from the insert list (target regenerates them).
- **Identity columns:** use `OVERRIDING SYSTEM VALUE` so source key values carry over.
- **Never install/alter target types, extensions, or enum labels** — missing ones fail loudly;
  the pre-existing target schema is the user's responsibility.
- **Collation note:** differing text collations affect only read-side chunk ordering, not
  correctness (we apply by exact key).

**Pre-flight compatibility check (decision: block on incompatible, warn on risky).** At sync start,
introspect and classify every source↔target column pair: **identical** / **safe-widening** (ok),
**risky/lossy** (e.g. `int8→int4`, `varchar(50)→varchar(30)`, `timestamp→timestamptz`) → **warn**,
**incompatible or missing type/extension** → **error, refuse to start.** Turns mid-stream failures
into an upfront, actionable report.

**Runtime error policy (decision: halt, loud, observable).** If the target rejects a value at
apply time (type/constraint error), **halt the affected sync/table/component** with error logs +
telemetry and require operator intervention. Nothing is skipped, guessed, or repaired. The
offending row stays **dirty** (its delta is not tracked/purged) and re-applies automatically once
the cause is fixed. No quarantine/dead-letter in v1.

---

## 5. Delivery semantics

- **At-least-once + idempotent apply.** *(Decision.)*
- Applies are **idempotent upserts** keyed on PK (target Postgres uses `INSERT ... ON CONFLICT
  ... DO UPDATE`) and **deletes by PK**.
- **Checkpointed progress** means a crash safely replays from the last confirmed cursor; replays
  are harmless because applies are idempotent.
- Deltas are applied in **batches** for throughput.

---

## 6. Topology — always a user choice

Replication topology is **configured by the user**, never hardcoded:

- **Default / common case: single source → single target.**
- **Supported: fan-out — one source → multiple targets** (per-target cursor tracking in the
  track/cursor mechanism).
- **Roadmap: multi-master / bidirectional** (Bucardo-like) with pluggable conflict resolution.
  Design interfaces so this is addable, but it is not a v1 requirement.

For one-way replication the source is authoritative (target rows are overwritten/deleted to
match). Multi-master conflict resolution (e.g. latest-timestamp-wins, source-priority,
custom) is **deferred** but the apply/cursor layer should not preclude it.

---

## 7. Schema management — data-only

- **The target schema is assumed to pre-exist.** *(Decision.)* The user creates/migrates target
  tables with their own tooling; `replicare` replicates **data only**.
- **No live DDL replication** in v1. Source schema drift is an operational concern handled by
  re-sync, not by automatic DDL propagation.
- We still **introspect** the source (columns, PK/unique keys, types) to build correct copy and
  apply statements and to validate that the target is compatible.

---

## 8. Process & concurrency model

- **Single long-running daemon** managing **multiple named "syncs"** (a sync = one source → one
  or more targets, with its table selection and tuning). *(Decision.)*
- Each sync runs **goroutine worker pools** for parallel per-table initial copy and parallel
  delta apply.
- One binary, one process, **central observability** across all syncs.
- (Bucardo-style separate controller/worker *processes* were considered and rejected as
  non-idiomatic for Go.)

### 8.1 FK components as scheduling & consistency units

A **sync** is the *user-configured* job (a table selection + source + targets + tuning, with its
own ownership lock and cursor namespace). The **table group** is an *engine-derived* unit: within
each sync, the engine auto-partitions the selected tables into **FK connected components**.

- **Discovery:** build the FK graph (edge = child→parent FK) from **source** catalogs
  (`pg_constraint` / `information_schema`; privilege-light, read-only, works on old servers),
  **over the selected tables only**, treat it as **undirected**, and compute connected
  components. *(Decision: automatic by default.)*
- **Why components are the right unit:** no FK edge ever crosses a component boundary, so
  (a) intra-component **dependency ordering is always complete** (no child stranded from its
  parent), and (b) **distinct components are referentially independent → safe to copy/stream
  fully in parallel.** Components are also the natural progress/lag reporting unit.
- **Ordering within a component:** **upserts apply in topo order (parent→child); deletes apply in
  reverse (child→parent).** Cycles/self-references can't be topo-sorted → handled by the **retry
  fallback** (see §3.3). Initial copy loads a component in topo order (parents first).
- **Streaming apply granularity (decision):** a component's coalesced changes for **one drain
  pass** are applied in a **single target transaction** → the target is always referentially
  consistent within the group. **Scope clarification:** the atomic unit is *the changes in one
  pass*, **not** all rows of the component — so transaction size scales with **churn per pass**,
  not table size. Under extreme churn a pass may exceed a batch-size cap; if so we either grow
  the transaction or split it (relaxing strict per-group atomicity for that oversized pass) — a
  documented tuning tension. **Initial bulk copy is NOT atomic** (it is chunked); per-component
  atomicity is a *streaming* guarantee only.
- **Giant-component problem (must handle):** real schemas often connect almost everything through
  hub tables (`users`, `tenant`, shared lookups), collapsing most of the schema into one giant
  component and erasing parallelism. *(Decision: auto-group by default, but emit a loud warning +
  telemetry when one component dominates the selection, and allow config override — force-split
  hints, or treat the whole selection as a single ordered group.)* Note the giant component also
  makes per-component transactions larger — another reason to surface it.
- **Selection interaction:** components are computed over **selected** tables only. **Warn on FK
  edges pointing to excluded/unreplicated tables** (dangling references — the target must already
  satisfy those parents or apply will fail). Ordering is derived from the **source** FK graph; if
  the target's constraints differ, the retry fallback (§3.3) covers the mismatch.

---

## 9. State storage

The daemon's own operational state — per-table initial-copy progress (which PK-range chunks are
done) and per-target cursors (last confirmed delta sequence per table per target) — is **small,
strongly-consistent, and checkpoint-heavy** (cursors update every applied batch and must be
crash-atomic). It is **not** bulk data.

**Decisions:**
- State lives behind a **pluggable `StateStore` interface**.
- **v1 ships exactly one backend: Postgres.** *(Decision.)* A dedicated schema on a user-pointed
  Postgres database — which may be the **target**, the **source**, or a **separate** Postgres.
  This was chosen over embedded BoltDB/SQLite because an embedded file pins the daemon to local
  disk, which is hostile to Kubernetes (forces a `StatefulSet` + PersistentVolume just to hold a
  few KB, and blocks clean reschedule/scale). Postgres is **zero new infrastructure** (we already
  speak it), works in K8s as just a connection string, gives **transactional checkpoints**, and
  has a Bucardo precedent (central Bucardo Postgres DB).
- **Embedded (BoltDB/SQLite), etcd, and cloud KV (e.g. DynamoDB) are deliberately deferred** —
  addable later via the same interface if a use case demands them.

**Concurrency / ownership:** v1 runs a **single active daemon per sync** — Kubernetes restarts it
on failure (e.g. `Deployment` replicas=1 or a `StatefulSet`); the external Postgres state store
lets a restarted/rescheduled pod resume cleanly from the last checkpoint. **HA (active/standby
with leader election) is deferred**, but the `StateStore`/ownership interface must be designed so
leader election can be added later **without a redesign** — the natural mechanism is
`pg_advisory_lock` (a replica acquires the per-sync advisory lock; others stand by), with
cursor-ownership fencing. Do not paint us into a corner that precludes this.

**Bonus property:** because state is in Postgres, when the state store *is* the target database a
cursor advance can ride in the **same transaction** as its apply batch — yielding effectively
exactly-once apply per batch on top of our at-least-once design.

**Do not conflate two different things on the source:** the **delta and track/cursor tables for
trigger CDC necessarily live on the *source* database** (that is where triggers write). The
`StateStore` above is the *daemon's own* operational state (which syncs exist, how far each has
progressed). They are separate concerns even if both happen to be Postgres.

---

## 10. Observability (first-class)

All four are in scope:

- **Prometheus metrics** — `/metrics` endpoint: throughput (rows/s), replication lag, delta
  queue depth, rows copied / total (per table), errors, current phase, per-target cursor age,
  **per-target reachability/up gauge**, **delta backlog & oldest-unconsumed age**.
- **OpenTelemetry** — traces + metrics via OTLP (works with Honeycomb, Tempo, etc.).
- **Structured logging** — Go stdlib **`log/slog`** (JSON/text, leveled, rich context fields).
- **Health/status HTTP API** — live status: phase (initial-copy vs streaming), per-table
  progress %, lag, last error — for dashboards and a CLI `status` command.

**Target unreachability + delta growth is a cross-channel signal (decision).** When a target goes
unreachable and its delta backlog grows, it must be visible in **logs, metrics, AND OTel traces**
simultaneously (see §3.4 for the per-channel breakdown): escalating slog WARN→ERROR with backlog
and proximity-to-retention-cap; a reachability gauge plus climbing backlog/age series; and drain
spans marked error with connection-error events and backlog/age attributes. A degrading target
must never be visible in only one channel.

Treat lag, queue depth, per-table progress, **target reachability, and delta backlog** as the
headline signals.

---

## 11. Configuration & deployment

- **YAML config** is primary: sources, targets, syncs, table selection, tuning knobs (worker
  counts, batch sizes, copy chunk size), observability endpoints, TLS.
- **Env-var overrides** for any field (container/secret-injection friendly).
- **Secrets:** may be inline in YAML, with env-var override; **per-connection TLS** is
  configurable (`sslmode`-style: disable → verify-full).
- **Table selection:** include/exclude lists with **schema globs** (e.g. `public.*`, exclude
  `*_audit`). *(Decision.)*
- **Distribution:** single static binary + sample `systemd` unit now; **Helm chart later**
  (YAML injected via chart values — no rush).

---

## 12. Minimum privileges (document & keep accurate)

**Source (Postgres):**
- `CREATE` on a schema (or a pre-created schema we own) to hold the `replicare` schema, delta
  tables, track/cursor tables, and trigger functions.
- `TRIGGER` on each replicated table (or table ownership).
- `SELECT` on each replicated table.
- **No** superuser, **no** `REPLICATION` attribute, **no** `wal_level` change.

**Target (Postgres):**
- `SELECT, INSERT, UPDATE, DELETE` on the (pre-existing) target tables.

**Redis:** ability to `SCAN`/read keys; optionally permission to enable/consume keyspace
notifications where supported.

---

## 13. Suggested layout (proposed — confirm as code lands)

```
cmd/replicare/          # main entrypoint (daemon + CLI: run, status, etc.)
internal/config/        # YAML config + env overrides + validation
internal/engine/        # engine-agnostic Source/Sink interfaces + registry
  postgres/             # pgx-based Source + Sink (trigger CDC, COPY, upsert apply)
  mysql/                # (future) trigger CDC
  redis/                # (future) SCAN snapshot + reconciliation
internal/pipeline/      # sync orchestration: initial copy, delta drain, worker pools
internal/state/         # pluggable StateStore interface; v1 backend = Postgres (advisory-lock ownership)
internal/observability/ # slog, Prometheus, OTel, status HTTP API
internal/copy/          # chunked bulk copy: keyset/ctid chunking, text COPY pipe, delete-range resume
internal/delta/         # per-table delta+track lifecycle: consume, delete-by-id, batched purge, retention/reseed, autovacuum tuning
internal/apply/         # idempotent batched apply; FK dependency-ordered + retry fallback; text-faithful (staging+upsert)
internal/schema/        # introspection + pre-flight type-compat check; session-GUC canonicalization
```

Core interfaces to define early: `Source` (introspect, install/remove capture, snapshot chunks,
read deltas, re-read by key, advance cursor) and `Sink` (introspect, bulk write, apply
upsert/delete batches). Keeping these clean is what makes MySQL/Redis additive rather than
invasive.

---

## 14. Decision log (quick reference)

| Topic | Decision |
|---|---|
| CDC mechanism | **Trigger-based (Bucardo-style)** for PG & MySQL; assume **no WAL / no binlog**. Redis: **SCAN + reconciliation**, notifications optional accelerator only. |
| Delta capture | **PK-only**, re-read current row at sync time. |
| Delta sequencing | **Dirty-key set**, NOT an ordered log. **No scalar high-water-mark cursor** (commit-order skew). Consume via **`delta MINUS track` set-difference**, **delete-by-`delta_id`**, per-PK coalescing. seq = lag metric only. See §3.3. |
| Track location | **On the source DB**, atomic with delta consume + purge. StateStore holds copy-progress/lag/ownership only. |
| Crash safety | read → apply to target → then track/purge on source. At-least-once, no 2PC. |
| FK on target | **Dependency-ordered apply** within an FK component: **upserts parent→child, deletes child→parent** **+ retry fallback** for cycles/self-refs/cross-pass deps. |
| Table grouping | Sync = user-configured selection; engine **auto-partitions into FK connected components** (over selected tables, from source catalogs). Components = units of ordering, **parallelism**, and consistency. **Auto by default; warn + override on giant component.** See §8.1. |
| Per-component apply | A component's changes for **one drain pass** apply in a **single target transaction** (intra-group referential consistency). Bounded by churn/pass, not table size. Initial copy is chunked (not atomic). |
| Copy chunking | **Keyset PK-range default** (balanced ranges via index-only boundary scan; composite/UUID/text keys OK), **ctid/block-range fallback**; native partitions copied per-partition. See §4.1. |
| Copy wire format | **Text/CSV `COPY` default** (cross-version safe), **binary opt-in** for close versions. Streamed source→target via `io.Pipe`. |
| Copy load path | **Empty-target direct COPY**, resume via **DELETE-range + re-COPY**; auto/opt-in **TEMP staging + upsert** when target non-empty. Never touch target indexes/constraints. |
| Component copy order | **Parents-before-children, direct** within a component; chunks parallel per table; components parallel. Cyclic FKs = documented edge case. |
| PK updates | `UPDATE` changing the key enqueues **both** old + new PK (= delete(old) + upsert(new)); lets copy treat PKs as immutable. |
| Type fidelity | **Faithful transport, never transform** (verbatim text, both copy + apply). **No transform capability exists, ever.** See §4.2. |
| Session GUCs | Pin `DateStyle=ISO,YMD`, `TimeZone=UTC`, `extra_float_digits=3`, `IntervalStyle=postgres`, `bytea_output=hex`, `client_encoding=UTF8` on all connections (formatting-only, no privilege). Warn against `money`. |
| Faithful apply | Delta apply moves values as **text** too (preferred: `COPY` to TEMP staging + `INSERT … ON CONFLICT`) to avoid Go-type fidelity loss. |
| Column mechanics | Name-matched explicit column lists; exclude `GENERATED STORED`; `OVERRIDING SYSTEM VALUE` for identity; never install/alter target types/extensions. |
| Pre-flight check | **Block on incompatible / missing type; warn on risky-lossy; ok on identical/widening.** |
| Runtime type error | **Halt the sync, loud + observable**; offending row stays dirty and re-applies after fix. No quarantine in v1. |
| Delta layout | **Per-table delta tables** (isolate bloat/contention; per-table autovacuum + truncate/partition). Track is per (target, table). |
| Delta purge | **Batched consumption-gated DELETE + aggressive per-table autovacuum reloptions** (universal); **time-partition + DROP** opt-in on PG≥10 (bloat-proof, dodges `xmin` horizon). See §3.4. |
| Source bloat protection | **Bounded retention (age/size) + forced reseed** of over-cap targets (configurable, default on). Protects a source we may not own. |
| Lag↔load | Per-PK coalescing; configurable drain interval / batch size / re-read concurrency; conservative source pressure by default. |
| Target-down visibility | Unreachable target + growing delta surfaced across **logs + metrics + OTel traces** simultaneously (§3.4, §10). |
| Tables without PK/unique | **Skip + loud warning** (telemetry-surfaced). |
| Initial copy vs deltas | **Enable capture first**, then chunked parallel copy; idempotent apply reconciles overlap → **no frozen snapshot needed** (handles huge DBs). |
| Delivery | **At-least-once + idempotent upserts**, checkpointed cursors. |
| Topology | **User choice**: default single→single; **fan-out supported**; **multi-master on roadmap**. |
| Schema | **Target pre-exists; data-only; no live DDL** (v1). |
| Process model | **Single daemon, many syncs, goroutine worker pools.** |
| State store | **Pluggable `StateStore`; v1 = Postgres only** (dedicated schema on target/source/separate PG). Embedded/etcd/cloud-KV deferred. (Delta/track tables always live on source — separate concern.) |
| HA / ownership | **Single active daemon per sync in v1** (K8s restarts handle failure). Leader election (`pg_advisory_lock`) deferred but must remain addable without redesign. |
| Observability | **Prometheus + OpenTelemetry + slog + status HTTP API.** |
| Config/deploy | **YAML (+ env overrides)**, single static binary + systemd; Helm later. Secrets inline+env; TLS per connection. Include/exclude + schema globs for selection. |
| Stack | Go 1.23+, `pgx` v5, MIT, module `github.com/rudimk/replicare`. |
| Compatibility | **Must read from very old source Postgres**; keep source SQL conservative; target may use modern features. |

---

## 15. Open questions / future work

- Additional `StateStore` backends beyond Postgres (embedded BoltDB/SQLite, etcd, cloud KV) —
  add behind the interface if/when a use case needs them.
- Initial copy of **cyclic / self-referential FK** subgroups under live target constraints —
  define the concrete strategy (deferrable detection, NULL-then-fill two-pass, or user flag).
- **Binary-COPY compatibility gate** — how to detect when source/target versions + types are
  close enough to safely enable binary format instead of text.
- HA / active-standby leader election (`pg_advisory_lock` + cursor fencing) — design when HA
  becomes a goal; keep the ownership interface ready for it.
- Multi-master conflict-resolution model (latest-wins / priority / custom) — design later.
- Concrete **retention-cap defaults** (age and/or size) and the **partition granularity/cadence**
  for the PG≥10 partition+DROP delta path (resolved in principle in §3.4; tune the numbers).
- **Reseed coordination:** mechanics of marking a target needs-reseed, resetting its track,
  re-running initial copy, and resuming streaming without gaps (ties to §4 cutover).
- Optional full-row capture mode (currently PK-only) if a use case demands exact history.
- Re-sync / reseed workflow for schema drift or divergence.
- Helm chart packaging.
