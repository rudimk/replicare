# replicare — Sequence / Identity-Counter Reseed Plan (v1)

> **Status: PLAN / NOT STARTED.** Design note + milestones for a `reseed-sequences`
> operator command that repairs the one gap in replicare's otherwise-faithful data
> transport: the **sequence / identity counter** (`last_value` / `AUTO_INCREMENT`) is not
> replicated, only the column *values* are. This plan scopes the fix for **passive
> (one-way / DR)** replication, documents **why the same fix is unsafe for active-active**,
> and records the **decided** active-active direction: **globally-unique keys (UUID v7 /
> ULID) are the blessed id strategy** (documented in the repo docs), with replicare's mesh
> obligation limited to **pre-flight policing** — not counter-syncing, not allocation (§6).
>
> Companion reading: `CLAUDE.md` §7 (data-only), §4.2 (identity columns / `OVERRIDING
> SYSTEM VALUE`), §12 (privileges); `docs/multi-master.md` (the mesh);
> `.sisyphus/multi-master-plan.md`.

---

## 1. The gap (what prompted this)

replicare replicates **row values faithfully**, including the integer values that a
sequence produced: a source row with `id = 42` lands on the target as `id = 42`
(Postgres emits `OVERRIDING SYSTEM VALUE`, `internal/engine/postgres/sink.go:274` &
`apply_mesh.go:182`; MySQL supplies the explicit `AUTO_INCREMENT` value in the column
list). The **data converges exactly** — no id is renumbered or lost.

What is **not** replicated is the sequence *object's* counter — Postgres
`pg_sequence.last_value`, MySQL's per-table `AUTO_INCREMENT` next value. This is a direct
consequence of the data-only design (`CLAUDE.md` §7: the target schema pre-exists;
replicare does no DDL / schema-object replication). There is no `setval` / `ALTER TABLE …
AUTO_INCREMENT` anywhere in the apply path today (the only sequences in the codebase are
replicare's own `replicare.delta_seq` ordering hint, `capture_schema.go:39`).

**Why it's inert for a passive replica:** the target never calls `nextval()` for a
replicated table — the applier always supplies explicit ids — so a stale counter just
sits there, unused. Data matches, `verify` passes.

**Where it bites:** the moment the target starts *generating* ids. For one-way that is
**promotion / failover (DR)**: you cut over, the app inserts, `nextval()` returns a value
at or near 1, and it collides with the replicated rows at id `1..N`. This is the gap an
operator hits (and currently works around by copying sequence values over by hand).

---

## 2. Scope of this plan

**In scope — passive / one-way / DR:** a `reseed-sequences` command that, for every
replicated table, advances the target's sequence / `AUTO_INCREMENT` to `max(id)+1`, so a
promoted node never re-issues an existing id. Engine-symmetric (Postgres + MySQL),
low-privilege, read-only until invoked.

**Explicitly out of scope — active-active:** the same operation is **silently
destructive** in a mesh (§5). The command therefore **must refuse to run against a
cluster-member node**. The active-active id-allocation problem is **decided, not built
here**: the blessed strategy is **globally-unique keys (UUID v7 / ULID)**, documented in
the repo docs (§6); replicare's only mesh-side code obligation is **pre-flight policing**
(refuse/warn on locally-allocated unique keys in a mesh). Transparent integer-PK
auto-config is an optional, lower-priority fallback for owners who can't change their
schema, deferred to a later plan.

**Non-goal:** replicare does **not** become an id allocator, and does **not** replicate
DDL. Reseed only advances an existing counter.

---

## 3. Design — the `reseed-sequences` command (one-way / DR)

### 3.1 What it does

For each replicated table with a locally-allocated integer PK:

- **Postgres:** `SELECT setval(pg_get_serial_sequence('schema.table','col'),
  GREATEST((SELECT COALESCE(max(col),0) FROM schema.table), 1))` — `pg_get_serial_sequence`
  resolves both `serial` defaults and `GENERATED … AS IDENTITY`. A `NULL` result (no owned
  sequence — e.g. a UUID or natural PK) means "nothing to reseed", skip.
- **MySQL:** `ALTER TABLE schema.table AUTO_INCREMENT = <max(id)+1>`. MySQL only moves the
  counter forward and clamps to `max+1` anyway, so this is inherently safe; a table with no
  `AUTO_INCREMENT` column is skipped.

Run against the **target** (the node that will be promoted). It reads each table's current
`max(id)` *on that node* and sets the counter past it.

### 3.2 Why `max(id)+1`, not "match the source's last_value"

The promoted node's data **is** the set of replicated rows, so `max(id)+1` cannot collide
with anything present, by construction. The source sequence may sit *ahead* of its max id
(cache, gaps, rolled-back txns, or ids allocated in the final seconds that never
replicated); those "ahead" ids belong to rows that don't exist on the target (lost in the
normal DR window), so re-issuing them is harmless. Chasing the source's exact `last_value`
buys nothing and would need an extra privileged read of the source sequence. **Decision:
`max(id)+1` on the target, no source-sequence read.**

### 3.3 CLI shape

```
replicare reseed-sequences <config> [--node <name>] [--dry-run]
```

- `<config>`: the usual config file; resolves connections + the replicated table set
  (reuses the sync's selection + introspection — same tables `verify` walks).
- `--node`: which target to reseed (defaults to the sole target in a single→single config).
- `--dry-run`: print the per-table current `max(id)` and the value it *would* set, write
  nothing. (Good for a failover runbook rehearsal.)
- Output: one line per table — `schema.table  max_id=N  ->  seq set to N+1` (or `skipped
  (no owned sequence)`), and a final summary. Read-only until the non-dry-run write.

### 3.4 Privileges (document in §12)

- **Postgres target:** `setval` needs `UPDATE` on the sequence (or ownership). This is a
  **new** grant beyond §12's target set (`SELECT,INSERT,UPDATE,DELETE` on tables). Document
  it as a DR-only extra; absence → the command fails loud with the exact missing grant.
- **MySQL target:** `ALTER` on the table. Also a DR-only extra beyond the DML grants.

Reseed needs **no** source privileges (it reads only the target's own `max(id)`).

### 3.5 The ordering rule (runbook, not code)

Reseed must run **before** the promoted node is opened to application writes: stop
replication into the node → `reseed-sequences` → open to writes. A write in the gap between
promotion and reseed could still grab a colliding id. Document this as the canonical
failover sequence.

### 3.6 The safety guard — refuse on a mesh (the most important part)

`reseed-sequences` **must detect that the target is a cluster member and refuse, loudly**,
naming why (point to §6). "Cluster member" = the config has a `clusters:` block the node
participates in, or any node it replicates with also replicates back to it. This guard is
as much the feature as the reseed itself — see §5 for the blast radius it prevents. A
`--i-know-this-is-a-mesh` style override is **deliberately not provided** in v1.

### 3.7 Optional polish — continuous `sync_sequences` (one-way only)

Not required for correctness (the promotion-time reseed is the guarantee), but useful:
an opt-in `sync_sequences: true` neutral-layer knob that advances the target's counters on
a lazy interval during streaming (same `max(id)+1` mechanism), so (a) the promotion step
has nothing surprising to do and (b) the gap is observable in `status`. Same mesh refusal
applies. **Decision: ship the command first; treat continuous sync as a fast-follow.**

---

## 4. Milestones

- **M1 — Command skeleton + discovery.** `reseed-sequences` subcommand; reuse config +
  selection + introspection to enumerate replicated tables and resolve each one's owned
  sequence (PG `pg_get_serial_sequence`) / `AUTO_INCREMENT` column (MySQL). `--dry-run`
  prints `max(id)` and the target value. No writes yet.
  *Acceptance:* dry-run against a seeded one-way PG and MySQL target lists every keyed
  table with the right sequence and a correct `->` value; UUID/natural-PK tables show
  `skipped`.
- **M2 — The write path.** Apply `setval` (PG) / `ALTER TABLE … AUTO_INCREMENT` (MySQL)
  per table; loud, exact error on a missing grant.
  *Acceptance (local gate):* after a one-way copy, promote the target by hand, run
  `reseed-sequences`, insert a new row with a DB-generated id → it lands at `max+1` with no
  collision. A `--dry-run` immediately before shows the same number it set.
- **M3 — The mesh refusal guard.** Detect cluster membership from config; refuse with a
  clear message + pointer to the active-active gap doc. Unit-testable (pure config → refuse
  / allow decision), CI-runnable.
  *Acceptance:* a `clusters:` config is refused with the explanatory error; a one-way
  config is allowed. Covered by a CI unit test (no DB needed).
- **M4 — Docs.** `docs/operations.md` failover/DR section: the gap, the command, the
  runbook ordering, the new privilege; `CLAUDE.md` §12 privilege note + a decision-log row;
  a loud "active-active: NOT this command — see below" cross-link.
- **M5 (fast-follow, optional) — continuous `sync_sequences`.** ✅ **SHIPPED.** An opt-in neutral
  `sync_sequences: true` knob on a one-way `sync` advances the target's owned counters to `max(id)+1`
  on a lazy interval (`seqSyncInterval`, 60s) during streaming, via the existing `engine.SequenceReseeder`
  capability. Off by default; a no-op in cluster mode, when the engine lacks the capability (Redis), or
  outside the interval; best-effort (a reseed error — e.g. the missing DR grant — is logged and retried,
  never aborting the pass). Same mesh refusal as the command, enforced at **config load** (a
  `sync_sequences: true` target that is also a cluster member is rejected by `validate`/`run`).
  `internal/config` (knob + `SyncsSequences()` + validation), `internal/pipeline/stream.go`
  (`syncSequences`), `internal/daemon/build.go` (threaded, always false on a cluster edge). Unit-tested
  (config parse + mesh refusal; pipeline gating/throttle/best-effort). Docs: `docs/configuration.md`,
  `docs/operations.md`, `CLAUDE.md` §12 + decision log.
- **M6 (active-active docs) — ✅ SHIPPED.** `docs/multi-master.md` §5.6 "Active-active id
  allocation — globally-unique keys": **UUID v7 / ULID (stored binary) is the blessed strategy**,
  with the §6.0 fine print (allocation-not-conflict-resolution, the migration cost,
  v7/ULID-over-v4 stored binary, and *all* unique keys not just the PK), a loud "do NOT sync
  integer counters across a mesh" (the §5 blast radius, cross-linked to the `reseed-sequences`
  refusal and `sync_sequences`), the §6.2 why-counter-syncing-can't-work summary, and a forward
  pointer to the M7 guardrail. Cross-linked from `docs/operations.md` → Sequences section; the
  planned policing is seeded as `docs/multi-master.md` §6.3 item 3. No code.
- **M7 (active-active guardrail — code, follows M6) —** mesh **pre-flight policing** (§6.1):
  refuse/warn when a `clusters:` mesh has a replicated table with a locally-allocated integer
  PK **or secondary unique key** and no declared collision-free scheme; flag `int4`.
  Introspection-only, low-privilege; the refusal logic is pure + CI-testable. The transparent
  integer-PK auto-config (§6.3) stays a **separate later plan**.

CI stays Postgres-only; the write path's end-to-end check is a **local gate** (PG + MySQL
harnesses), matching the repo's existing posture. M3's refusal logic is pure and CI-tested.

---

## 5. Why the same command is catastrophic in active-active (the blast radius M3 prevents)

In a converged mesh every node holds the **union** of all rows, so `max(id)` is essentially
the same global maximum on every node. Reseeding each node to `max(id)+1` sets **every
node's counter to the same value**. Then two nodes take concurrent writes, both `nextval →
global_max+1`, both insert *different* rows under the *same* id, they replicate, and
HLC-LWW keeps one and **silently discards the other** — the §1.7 cardinal sin (silent data
loss), delivered by a command that exits 0.

Worse on a *correctly partitioned* mesh (node A in window `[0,S)`, node B in `[S,2S)`):
`max(id)` lives in B's window, so reseeding both to `global_max+1` **yanks A out of its
window into B's space**, destroying the disjoint-allocation invariant that was keeping the
cluster collision-free. One run converts a healthy mesh into one that corrupts on the next
pair of concurrent inserts.

This is why M3's refusal is not optional hardening — it is a correctness requirement.

---

## 6. Active-active — the decided strategy: globally-unique keys

**Decision (owner-set): the blessed active-active id strategy is globally-unique keys —
UUID v7 / ULID — documented in the repo docs; replicare builds only the pre-flight
guardrail, not an allocator.** Rationale below. The transparent integer-PK auto-config
(§6.3) is an optional fallback for schemas that can't move to UUIDs, deferred to a later
plan.

### 6.0 Why UUID/ULID is the answer, and its fine print

UUID/ULID PKs dissolve the id-allocation problem in one move: every node mints its own
globally-unique keys with **zero coordination** — no sequence to sync, no windows, no
interleave, no engine asymmetry (§6.3), and **nothing for replicare to build** (faithful
transport already moves a UUID verbatim). Id allocation is upstream of replication, and
UUIDs put the fix exactly there, keeping replicare a pure replicator.

Fine print to carry into the docs so we don't oversell:
- **It solves *allocation*, not *conflict resolution*.** UUIDs guarantee two nodes never
  mint the same key for *different* rows; concurrent updates to the *same* row are still
  HLC-LWW's job. UUIDs retire one of the two mesh hazards, not both.
- **It's a schema migration** — "one go" conceptually, but on a live system it means every
  surrogate-int PK → UUID, every referencing FK, reindex, and app code that assumes integer
  ids. Greenfield: trivial. Production: a real project.
- **Use time-ordered UUID v7 / ULID, stored binary** (`uuid` on PG; `BINARY(16)` via
  `UUID_TO_BIN(…,1)` on MySQL) — random v4 as a PK scatters inserts (on MySQL InnoDB the PK
  *is* the clustered index → page splits / write amplification); v7/ULID restore insert
  locality and binary storage halves the width of the text form.
- **It must cover *every* locally-allocated unique value, not just the PK.** A secondary
  `UNIQUE` column fed by a sequence (e.g. a human-facing `order_number`) still collides
  across nodes even with a UUID PK — so the pre-flight guardrail checks **all unique keys**,
  and "we use UUIDs" has to mean every locally-allocated unique value.

### 6.1 replicare's mesh obligation: pre-flight policing (the only thing we build)

At mesh start, introspect every replicated table's **primary key and every unique key**.
Classify each locally-allocated integer key (serial / identity / `AUTO_INCREMENT`):
- With a declared/validated collision-free scheme in place → allow.
- Otherwise → **refuse to start, loudly**, naming the offending table/column and pointing at
  the UUID guidance (narrow `int4` keys flagged especially). This converts the silent
  data-loss setup (§5) into a loud startup refusal — the §4.2 "block-on-incompatible"
  philosophy. Pure introspection, low-privilege. **This is the whole active-active code
  deliverable for now.**

### 6.2 Background — why counter-syncing can't work, and what real DBs do

**Counter-syncing is fundamentally the wrong tool for a mesh.** Concurrent multi-writer + a
single shared contiguous integer sequence + no coordination is impossible — pick two. Every
production active-active database makes allocations **disjoint or globally-unique**; none
syncs counters. The three real families:

1. **Interleaved sequences (offset + increment = N).** MySQL Group Replication
   (`group_replication_auto_increment_increment`, default 7), Galera
   (`wsrep_auto_increment_control`), Oracle multimaster. Node *n* emits residue class *n*
   mod N. Engine-native on MySQL; manual per-sequence on Postgres.
2. **Coordinated block allocation ("global sequences").** Postgres BDR `galloc` (raft hands
   out chunks), CockroachDB / YugabyteDB per-node cached blocks. Behaves like one coherent
   sequence at the cost of a coordinator + occasional round-trip. Requires the *app* to
   allocate through the coordinator, not the local DB.
3. **Globally-unique by construction.** BDR `snowflakeid`/`timeshard`, CockroachDB
   `unique_rowid()`, UUID/ULID. Timestamp+node+counter packed into a `bigint`, or a random
   UUID. Zero coordination, concurrent-safe, schema/app-level.

**The architectural truth:** replicare cannot solve this in the replication layer, because
**id allocation happens upstream** — the source DB assigns the PK before capture ever sees
the row. replicare can replicate it, LWW-resolve a *value* conflict on it, and police the
chosen strategy; it cannot retroactively de-collide two independently-minted ids. That is
exactly why the blessed answer (§6.0) pushes allocation to the schema layer (UUIDs), where
it belongs.

### 6.3 Deferred fallback — transparent integer-PK auto-config (later plan, lower priority)

For owners who genuinely cannot move to UUIDs, a later plan may add opt-in window assignment
— with a stark engine asymmetry that makes it PG-only in practice:
- **Postgres — works.** A sequence is a separate object from the table; peer rows arrive via
  explicit-id inserts (`OVERRIDING SYSTEM VALUE`) that **do not advance the sequence**, so
  setting node *n*'s sequence to `RESTART WITH n·stride` holds and `nextval` stays
  node-disjoint, transparently to the app. Needs only per-sequence `ALTER` (grantable, no
  superuser). Range over residue because it's add-a-node-friendly.
- **MySQL — doesn't.** `AUTO_INCREMENT` is per-table and monotonic-only, and every node holds
  the union, so the counter **floats up to the global max** and can't be pinned to a low
  window; `ALTER TABLE … AUTO_INCREMENT = <low>` is clamped. The only scheme that survives is
  **residue** (`auto_increment_increment`/`offset`) — session/global server vars set on the
  *app's* connections or globally (`SYSTEM_VARIABLES_ADMIN`, server-wide), neither of which
  replicare controls. So MySQL is **police-only**: verify the vars are set disjointly for
  this node, else refuse/warn; the operator (or app) sets them.
- A mesh-aware "next free slot in *this node's* window" repair would be a **different** tool
  from the DR reseed (per-partition, never global `max(id)+1`).

**Open questions for that deferred plan:** window stride sizing vs `bigint` headroom vs node
count/throughput; verify-refuse vs warn; whether replicare may `ALTER` user sequences on PG
(it touches user-owned objects — explicit opt-in + documented grant); `int4`-PK handling;
and whether coordinated block allocation ("replicare as id authority," which would put it in
the app's write path) is ever in scope (tentatively: no).

---

## 7. Decision log (quick reference)

| Topic | Decision |
|---|---|
| The gap | Row **values** (incl. identity ids) replicate faithfully; the sequence **counter** does not (data-only, §7). Inert for a passive replica; bites on promotion. |
| Reseed target value | **`max(id)+1` on the target**, not the source's `last_value` (target data = replicated rows; `max+1` can't collide; no source-sequence read needed). |
| Engines | Postgres `setval(pg_get_serial_sequence(...), max+1)`; MySQL `ALTER TABLE … AUTO_INCREMENT = max+1`. UUID/natural PKs skipped. |
| Scope | **Passive / one-way / DR only.** |
| Privilege | New DR-only grant: PG `UPDATE` on the sequence; MySQL `ALTER` on the table. No source privilege. Missing → loud, exact error. |
| Runbook | Reseed **before** opening the promoted node to writes. |
| **Mesh guard** | `reseed-sequences` **refuses on a cluster-member node**, loudly (M3). No override in v1. The same `max(id)+1` is silent data loss in a mesh (§5). |
| Continuous sync | Optional one-way `sync_sequences: true` fast-follow; promotion-time reseed remains the guarantee. |
| Active-active | **Decided: globally-unique keys (UUID v7 / ULID, stored binary) are the blessed strategy**, documented in the repo docs (M6). replicare's only near-term code is **pre-flight policing** of all locally-allocated unique keys (M7); it never allocates or syncs counters. Counter-syncing is unsound (§6.2); transparent integer-PK auto-config is a PG-only, deferred fallback (§6.3 — MySQL `AUTO_INCREMENT` can't be pinned, so police-only there). |
| UUID fine print | Solves *allocation*, not conflict resolution (HLC-LWW still runs); it's a schema migration; prefer v7/ULID stored binary over random v4; must cover **every** locally-allocated unique value, not just the PK. |
| CI vs local | M3 refusal logic is pure + CI-tested; the write path is a **local gate** (PG+MySQL harnesses). CI stays Postgres-only. |
