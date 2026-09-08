# replicare — Multi-Master (Active-Active) Implementation Plan (v2)

> **Status: PLAN / NOT STARTED.** Companion to the design note
> [`docs/multi-master.md`](../docs/multi-master.md), which is the authoritative
> spec. This plan turns that spec into milestones with acceptance criteria, a
> critical path, and — first among equals — the invariants that keep the existing
> one-way path unchanged. Read the spec first; this plan does not restate its
> code-grounded findings, only builds on them.
>
> **v2** revises v1 after an independent, code-verifying plan review. The material
> corrections: (a) the conflict model — a metadata-free "source-priority" policy is
> **not** convergent for symmetric active-active, so **LWW over a totally-ordered
> version** is the true policy and priority is reframed as a non-symmetric fenced
> mode; (b) **v1 is full-mesh only**, so loop suppression is skip-only and the
> source-side `origin` column + ring forwarding are deferred (this also keeps one-way
> delta DDL literally unchanged); (c) mesh **bootstrap**, **tombstone GC**, and
> **reseed/retention** interactions are now named milestone steps, not open questions.

This plan covers **all three engines** (Postgres, MySQL, Redis). It is phased so
each milestone is independently shippable and low-risk-first, and so the project
can **stop after any milestone** with a coherent, released result.

---

## Guiding constraints (carried from CLAUDE.md + the design note)

Non-negotiable; every milestone is checked against them.

1. **BC-INVARIANT (the overriding constraint): one-way replication must never
   break.** A config with no `meshes:` block and no `node_id:` must parse, behave,
   and perform **identically** to today. This is spec §7 and it outranks every other
   goal here. If a milestone would require editing an existing one-way test to pass,
   that is a **BC violation and a hard stop** — the design changes instead. (See the
   [Backward-compatibility gate](#backward-compatibility-gate-bc).)
2. **No superuser, least privilege (CLAUDE.md §2, §12).** Loop suppression uses
   grantable primitives — a namespaced `replicare.apply` GUC (Postgres), a
   `@replicare_apply` user variable (MySQL) — never `session_replication_role` or any
   global server change. Every new grant is documented and re-verified on a managed
   offering.
3. **Faithful transport, never transform (CLAUDE.md §1.7).** Origin and version are
   *metadata about* a change; the value moves verbatim. Redis value keys are never
   wrapped — metadata lives in a parallel keyspace.
4. **Conservative, version-tolerant source SQL (CLAUDE.md §1.6).** Trigger/DDL
   changes stay broadly compatible with old source servers.
5. **Engine-agnostic core, engine-specific mechanism (CLAUDE.md §5).** The neutral
   layer gains node-identity, conflict-policy, and topology concepts; each engine
   implements the mechanism behind the existing `Source`/`Sink` seams.
6. **Schema evolution never drops in-flight state (CLAUDE.md §14).** State-store and
   any source-side changes go through the existing idempotent, in-place migration
   runner with a `schema_version` bump; deltas and cursors survive.
7. **Single-engine per mesh (CLAUDE.md §6).** A mesh is one engine, like a sync.
   Cross-engine multi-master is permanently out of scope.

---

## The crux: what one-way assumes that multi-master breaks

One-way replication is **source-authoritative and provenance-free**: one truth, the
target overwritten to match, exactly one direction so a change never returns.
Multi-master invalidates all three, and the code has no seam for any (grounded
per-engine in spec §2/§4, re-verified against the tree during review):

1. **No replication origin.** Nothing records where a change was first written, so a
   node cannot tell a foreign change from a local one → **loops**: replicare's own
   applied writes are re-captured (PG/MySQL triggers fire on them, verified — no
   `WHEN` guard; Redis re-`RESTORE`s them each SCAN pass) and ship forever.
2. **No conflict resolution, and no commit-order signal.** Apply is a blind overwrite
   (`ON CONFLICT DO UPDATE` with no `WHERE`; `ON DUPLICATE KEY UPDATE`; unconditional
   `RESTORE … REPLACE`) + delete-to-match. replicare **has no commit-order signal**
   (the trigger-CDC premise, CLAUDE.md §3.3), so true causal ordering is impossible;
   convergence must be built on a **totally-ordered per-row version**, or the topology
   must be fenced so writes only flow one way per key.
3. **No topology safety.** `Config.Validate()` does **no cycle detection** (verified,
   `internal/config/config.go:221-275`), so a bidirectional/ring wiring is accepted
   **silently** and corrupts data today.

**Two facts that shape the whole design (both surfaced by review):**
- **Symmetric conflict resolution needs a per-row substrate that travels with the
  row.** To resolve, on node C, a change from A against C's current row, C must
  compare something totally ordered that is present on *both* the incoming change and
  the stored row. The only such thing that respects §1.7 (no value mutation, no
  reading foreign metadata C doesn't have) is a **version column that is part of the
  row** (LWW). A policy claiming "no per-row data" cannot converge symmetrically — it
  is really single-writer-with-priority-fencing. This kills the v1 "source-priority as
  a symmetric active-active default" idea; see [Conflict model](#conflict-model).
- **A full mesh needs no origin *column*; loop suppression alone suffices.** If every
  node ships directly to every other node (no relaying), skip-suppression (don't
  re-capture apply writes) prevents all loops — a change reaches each node once,
  directly from its origin, and is never re-emitted. An origin *column* is only needed
  to **forward** a change across hops (ring/partial mesh). So **v1 is full-mesh only**,
  loop suppression is skip-only, and the source-side `origin` column + forwarding are a
  deferred post-v1 milestone. Bonus: one-way delta-table DDL then stays *literally*
  unchanged (no new column), tightening BC.

**Redis is the tall pole.** Capture-less and value-opaque: no capture step to guard,
no room in a `DUMP` payload for a version. LWW, loop suppression, and non-destructive
deletes all require a **new parallel metadata keyspace + tombstones** — a rewrite of
the Redis CDC model. Sub-key CRDT merge is out of scope (Redis Enterprise's domain).

---

## Conflict model (the design corrected in v2)

There is exactly **one symmetric active-active policy** in v1, plus one explicitly
non-symmetric fallback:

- **`lww` (last-write-wins over a totally-ordered version) — the real active-active
  policy.** Requires a user-designated **version column** that is monotonic per node.
  replicare forms the comparison key as **`(version, node_id)`** so it is **totally
  ordered with no ties** (the `node_id` component breaks equal-version ties
  deterministically — resolving the v1 "ties diverge" bug). Apply is a guarded upsert:
  Postgres `… ON CONFLICT (pk) DO UPDATE … WHERE (excluded.ver, excluded.node) >
  (target.ver, target.node)`; MySQL the equivalent guarded upsert. **The version
  travels in the row** (it is a real column), so it is present on both sides at apply
  time — **no target-side side table is needed** for LWW. A **pre-flight** validates
  the version column exists, is comparable, and is present on **every** member (named
  step in MM4). `lww` is refused for a table without a declared version column.
  Wall-clock timestamps are permitted only if the app guarantees monotonicity; the
  contract is documented, not assumed.
- **`priority-fenced` — convergent WITHOUT a version column, but NOT symmetric.**
  A static node ranking; a member **accepts a change for a key only from a
  strictly-higher-priority node** (and always emits its own). This converges with no
  per-row metadata, but it is **effectively single-writer-with-failover per key**, not
  symmetric active-active — a lower-priority node's concurrent write to the same key is
  overwritten and lost. It is offered for users who cannot supply a version column, and
  is documented plainly as non-symmetric. It is **not** the default and is **not**
  claimed to be active-active.
- **`custom` — deferred.** The policy interface must not preclude a pluggable
  resolver, but none ships in v1.

**Deletes** need a **tombstone** carrying `(version, node_id)` so a delete can be
compared against a concurrent update under the same total order (a delete is just a
change whose "value" is absent). Tombstones are bounded by a **coordinated GC** (MM4b)
so they cannot grow unboundedly on a source replicare may not own (CLAUDE.md §3.4).

---

## Goals & non-goals

**Goals**
- Active-active convergence of N same-engine nodes (target: 3 across clouds) for
  **Postgres and MySQL** under `lww`, writes on any node, **full mesh**.
- A **safe, opt-in config surface** (`node_id`, a `meshes:` block) that leaves the
  one-way path untouched, plus cycle-detection closing today's silent-corruption
  footgun.
- **Origin-based loop suppression** via grantable primitives (skip-only, full mesh).
- Hardened **fan-out** (one source → N read replicas) as an early low-risk win that
  independently satisfies "same data in three clouds" for single-writer users.
- A **Redis multi-master** on a metadata-keyspace + tombstone model — delivered last,
  gated, explicitly allowed to slip to a later release.

**Non-goals (v1)**
- Cross-engine meshes (permanent, §7).
- **Ring / partial-mesh forwarding** and the source-side `origin` column — deferred
  post-v1 (full mesh only in v1).
- Sub-key / field-level CRDT merge on any engine.
- A pluggable custom resolver (interface not precluded).
- Symmetric conflict resolution **without** a version column (`priority-fenced` is the
  only metadata-free option and is non-symmetric by construction).
- Wall-clock LWW as a safe default; synchronous/quorum writes; global ordering;
  distributed transactions; automatic schema propagation across members.

---

## Definition of Done (multi-master)

1. Three same-engine nodes, writes on all three, **converge** under continuous mixed
   churn for Postgres and MySQL under `lww`, with no replication loop and a documented,
   deterministic conflict outcome — proven by the **convergence oracle** (identical
   full-row-set checksum across all N nodes after quiescence) within a **numeric
   convergence SLA**, and a **loop metric** at zero after quiescence (see MM10).
2. **Every existing one-way integration test and both load harnesses
   (`test/loadgen`, `test/loadgen-redis`) pass unchanged** — the BC gate.
3. Config validation refuses an un-declared one-way cycle and validates meshes.
4. Combined mesh-member grants are documented and verified on a managed offering per
   engine (as MM3/MM5/MM6 acceptance, not deferred).
5. Docs and `CLAUDE.md` decision log updated.
6. Redis multi-master meets the same bar **or** is documented as a later-release
   non-goal with single-writer fan-out (MM2) as the supported alternative.

---

## Cross-cutting foundations

Designed once, in the neutral layer (MM0), reused by every engine:

- **Node identity.** A stable `node_id` per participating endpoint (config-declared,
  persisted with daemon state). Origin tag value and the `(version, node_id)`
  tiebreak component. Optional; ignored one-way.
- **Conflict policy.** A neutral `{policy, version_column?, priority?}` on a mesh; the
  apply layer consults it. `lww` compares `(version, node_id)`; `priority-fenced`
  compares node rank.
- **Loop suppression (v1: skip-only).** The apply path (and mesh bootstrap copy, per
  MM3) marks its writes as replicare-origin; capture **skips** marked writes. No origin
  column in v1 (full mesh). Recording origin for forwarding is the deferred
  ring-topology milestone.
- **Tombstones.** `(version, node_id)`-bearing delete markers, engine-specific storage
  (PG/MySQL: a tombstone table in the `replicare` schema; Redis: the metadata
  keyspace), **bounded by coordinated GC (MM4b)**.
- **Topology model.** v1 expands a `topology: mesh` to the full set of directed edges;
  each edge keeps its own cursor namespace and ownership lock.
- **State/schema migration.** All shape changes ride the existing in-place migration
  runner with a `schema_version` bump preserving in-flight rows; **the one-way delta
  table gains no column in v1** (tombstones are a separate table).
- **Observability.** New **distinct** metrics/log events/trace attributes (never new
  labels on existing series — the contract test asserts exact label sets): conflicts
  resolved (by policy + winner), loop-suppressed-write rate, tombstone backlog,
  per-edge lag. Added to the F2 contract with M6-style verification.

---

## Milestones (MM0–MM11)

Low-risk-first. Each lists **Acceptance** (its DoD) and **BC proof**. "Full one-way
suite green" = the entire existing integration + load-harness suite passes unedited.

### MM0 — Neutral foundations: node identity, conflict-policy types, config skeleton (inert)
Add neutral types (`NodeID`, `ConflictPolicy{policy, version_column, priority}`, a
`Mesh`/edge topology type) and config fields (`node_id` on `Endpoint`, a `meshes:`
block) — **parsed and validated but wired to no engine behaviour.** No delta/tombstone
schema yet.
- **Acceptance:** configs with `node_id`/`meshes:` parse and round-trip; configs
  without them are behaviourally identical; new types compile, unused by one-way; unit
  tests for parse/validate of the new fields.
- **BC proof:** full one-way suite green, zero edits; a golden test asserts a
  representative existing config resolves to an identical `Config`.

### MM1 — Cycle-detection guardrail (safety; ship first, standalone)
`Config.Validate()` refuses an **un-declared cycle** among one-way `syncs` not part of
a `meshes:` block (spec §4). Only *adds* a rejection for already-corrupt configs.
- **Acceptance:** `A→B`+`B→A` as plain syncs is rejected naming the cycle; a fan-out/
  fan-in DAG still validates; a mesh-declared cycle is not rejected.
- **BC proof:** every existing valid fixture still validates (none is a cycle); full
  one-way suite green.

### MM2 — Fan-out hardening (delivers single-writer "same data in 3 clouds")
Independent of mesh. Re-key initial-copy progress to include **target** (verified today
`(sync, schema_name, table_name)` with no target, `internal/state/postgres/progress.go:39-48`;
the streaming cursor already includes target, `:97-106`). **Migration recipe (named):**
add a nullable `target` column → backfill per each sync's configured target set → swap
the PK to `(sync, target, schema, table)`. **Edge case (named):** a multi-target sync
*already streaming* on the old schema has one shared progress row; on upgrade it can
seed only one target's progress, so the other target(s) re-copy once — a documented
one-time upgrade cost (multi-target streaming was never correct pre-MM2). Add a 2+-target
end-to-end convergence test.
- **Acceptance:** one source → two targets converge from cold copy and stay converged
  under churn; each target's progress is independent; crash mid-copy resumes each
  target correctly; the migration preserves single-target progress with no re-copy.
- **BC proof:** single-target behaviour unchanged; full one-way suite green.

### MM3 — Postgres loop suppression + mesh bootstrap (skip-only, full mesh)
Wire loop suppression for a full PG mesh. Add `WHEN (current_setting('replicare.apply',
true) IS NULL)` to the capture trigger; the apply transaction sets `SET LOCAL
replicare.apply = '1'`. **Crucially, the mesh bootstrap paths set the same marker:**
the initial copy (`internal/copy`, `COPY FROM STDIN`) and any reseed **into a mesh
member** run with `replicare.apply` set, so cold-copy writes are **not** re-captured as
local (prevents the review-identified echo storm). Origin-aware triggers are installed
**only for mesh tables**; one-way trigger DDL is byte-identical. **No origin column in
v1.** **Grants (named, not deferred):** document + verify the *combined* mesh-member
grant set (source `TRIGGER`+`SELECT`+schema `CREATE`/`USAGE` **and** target
`INSERT/UPDATE/DELETE`; note the §12 `RemoveCapture` ownership asymmetry applies on
*every* member).
- **Acceptance:** 2-node PG mesh — a write on A applied to B is **not** re-captured on
  B (B's delta has no replicare-origin row); a local B write is captured; **bootstrapping
  a fresh member B via initial copy produces no delta rows on B** (no echo); the
  `replicare.apply` GUC works on a least-privilege role (no superuser). No conflict
  policy yet; the test avoids concurrent same-key writes.
- **BC proof:** one-way trigger DDL and delta DDL byte-identical (guard + no column only
  for mesh tables); one-way apply/copy set no marker; full PG one-way suite green.
- **Floor:** mesh members need PG ≥ 9.6 (2-arg `current_setting`), matching the existing
  capture floor.

### MM4 — Postgres conflict resolution (`lww` + `priority-fenced`) + tombstones
- **MM4a — policies + tombstones.** `lww`: guarded upsert over `(version, node_id)` (no
  ties); **pre-flight (named)** extends `internal/schema` to validate the version column
  exists/comparable/monotonic-contract on **every member pair**, and refuses `lww`
  without it. `priority-fenced`: accept only from higher-priority node. **Tombstones**:
  a `(pk, version, node_id, deleted_at)` table in the `replicare` schema so a
  delete-vs-update resolves under the same order.
- **MM4b — tombstone GC (named prerequisite for shipping tombstones).** Per-mesh
  bounded retention coordinated across members so **no member resurrects a deleted key**
  before all members have observed the tombstone; bounded-growth acceptance metric
  (tombstone backlog age/size capped, analogous to delta retention §3.4). Tombstones do
  not ship without MM4b.
- **Acceptance:** 3-node PG mesh, concurrent same-key writes resolve **deterministically**
  per policy (test asserts the documented winner, including the `node_id` tiebreak at
  equal version); delete-vs-update resolves via tombstones; `lww` without a version
  column is a config error; tombstone backlog stays bounded under sustained delete
  churn. The **multi-master load harness** (extending `test/loadgen`) drives 3-node
  convergence and asserts the [convergence oracle](#mm10--multi-master-e2e-hardening-gate).
- **BC proof:** conflict/tombstone logic engages only on mesh edges; one-way apply stays
  blind source-wins + delete-to-match; the tombstone table is mesh-only; full PG one-way
  suite green.

### MM5 — MySQL mesh (mirror MM3+MM4)
Mirror loop suppression, bootstrap-marker, conflict policies, and tombstones for MySQL.
Loop suppression uses a **`@replicare_apply` user variable** set at `BeginApply` and an
`IF @replicare_apply IS NULL THEN … END IF` guard added to **all three** trigger bodies
(AFTER I/U/D); the marker resets on connection release alongside `FOREIGN_KEY_CHECKS`
(both session-level, non-transactional; the reset seam exists, `apply_tx.go:222-229`).
Delta trigger inserts already use explicit column lists, so no capture perturbation. The
bootstrap `LOAD DATA` path sets the marker too. Combined mesh-member grants + `deploy/`
presets are MM5 acceptance.
- **Acceptance:** 3-node MySQL mesh converges under `lww`; the user-variable marker is
  proven isolated to the pinned apply connection (`MaxOpenConns=1`) and does not leak to
  application writers; bootstrap produces no self-capture; install still blocks on a
  pre-existing non-replicare AFTER trigger; tombstone GC bounded.
- **BC proof:** one-way trigger bodies unchanged for non-mesh tables; one-way apply/copy
  set no variable; full MySQL one-way suite green.

### MM6 — Redis mesh (metadata keyspace + tombstone deletes) — downstream of MM4, gated
Consumes MM4's neutral tombstone/conflict-policy abstractions (so it is **downstream of
MM4**, not a parallel sibling). Introduce a **parallel metadata keyspace** — per data
key `K`, a sibling entry under a reserved prefix **hash-tagged into K's cluster slot** —
holding `{node_id, version, deleted_at?}`, written **atomically with** the value via
Lua/`MULTI`. Redesign delete reconciliation from today's stateless "missing at source ⇒
DEL" (verified, `internal/pipeline/delete.go:27-58`) to **tombstone-based**: delete a
target key only when a real origin tombstone exists and no newer local version is
recorded. Loop suppression + `lww` use the metadata `(version, node_id)`; value bytes
are never merged. Bootstrap into a mesh member seeds metadata without emitting spurious
tombstones. Combined ACLs (`+restore` on every node + metadata/tombstone keyspace RW) +
`deploy/acl-*` updates are MM6 acceptance.
- **Acceptance:** 3-node Redis mesh, writes on all nodes, converges with **no key
  destroyed by a peer's delete sweep** and **no `RESTORE` ping-pong** — falsified by a
  concrete oracle: after quiescence, per-key `RESTORE` rate → 0 and all nodes' keyspace
  checksums match; metadata/value stay atomic under crash injection (no orphan/mismatch);
  cluster: metadata co-locates with its value slot.
- **BC proof:** metadata keyspace, tombstones, redesigned sweep are **gated to mesh
  mode**; one-way Redis keeps the stateless SCAN-reconcile + target-vs-source sweep
  verbatim; full Redis one-way suite + `test/loadgen-redis` green.
- **Escape hatch:** if MM6 can't meet the bar in the window, it drops to a documented
  non-goal; Redis users take single-writer fan-out (MM2). Pre-approved, not a failure.

### MM7 — Mesh lifecycle: retention & reseed reconciliation
Reconcile meshes with the **default-on** retention/reseed machinery (CLAUDE.md §3.4;
`docs/reseed-state-machine.md`). A slow mesh member hitting the cap → needs-reseed →
re-copy must (a) run with the bootstrap marker (MM3) so it does not self-capture, and
(b) **not resurrect tombstoned keys** or lose version metadata.
- **Acceptance:** a mesh member forced to reseed re-converges without echoing its copy,
  and a key deleted (tombstoned) before the reseed **stays deleted** after it (named
  test); retention on a mesh member bounds source growth without breaking convergence.
- **BC proof:** one-way retention/reseed semantics unchanged; full one-way suite green.

### MM8 — HA / leader election (before "production-ready")
Generalize ownership from per-sync to **per-edge**; implement `pg_advisory_lock` leader
election + cursor fencing (today stubbed — `pg_try_advisory_lock` + skip-on-contention).
A mesh must not split-brain.
- **Acceptance:** two daemons contending for one mesh edge → exactly one drives it; a
  fenced cursor stops a deposed leader advancing; failover resumes from checkpoint with
  no lost/double-applied change (idempotency + fencing).
- **BC proof:** one-way per-sync ownership unchanged; full one-way suite green.

### MM9 — Observability, docs & CLAUDE.md
Add F2 contract entries as **new distinct series** (conflicts resolved by policy/winner,
loop-suppressed-write rate, tombstone backlog, per-edge lag — **not** new labels on
existing series, which the contract test forbids) with M6-style verification; update
`docs/multi-master.md` (plan→shipped where true), engine pages, configuration
reference, `CLAUDE.md` decision log, and the combined-grant docs.
- **Acceptance:** `/metrics` matches the extended contract; docs describe `meshes:`,
  the conflict policies + version-column contract, and per-engine mesh-member grants.
- **BC proof:** contract additions rename/remove/relabel **nothing** existing; full
  one-way suite green.

### MM10 — Multi-master E2E hardening gate
The shippable gate, with **concrete oracles** (fixing the review's "not just equal
counts" objection). A tri-node matrix per engine: kill a node mid-churn; partition;
concurrent same-key storms; delete/update races.
- **Convergence oracle:** after churn stops, **all N nodes have identical full row-set /
  keyspace content checksums** (reuse the loadgen checksum across every node pair), not
  merely equal row counts.
- **Convergence SLA:** convergence reached within a defined bound **T** seconds of
  quiescence (T set per engine from harness runs).
- **Loop metric:** replicare-origin re-capture rate → 0 after quiescence and total
  applies-per-quiescent-key bounded (no unbounded ping-pong).
- **Acceptance:** the matrix passes for PG and MySQL (and Redis if MM6 shipped) against
  those three oracles; the full pre-existing suite + both load harnesses green, unedited.

### MM11 — Release & packaging
Version bump, changelog, Helm/values surface for `meshes:`+`node_id`, sample
multi-master configs in `examples/`, and the multi-master load harness documented in
`docs/operations.md`.
- **Acceptance:** a released binary runs a 3-node mesh from a documented example; the
  demo converges (oracle); operations doc covers running and verifying a mesh.

### Deferred (post-v1): ring / partial-mesh forwarding
The source-side `origin` column, the *record-origin* trigger body (as opposed to v1's
*skip* body), and hop-to-hop forward-suppression (don't forward a change back toward its
origin). Only needed for non-full-mesh topologies; explicitly out of v1 so v1 adds no
column to the one-way delta table.

---

## Critical path

```
MM0 (foundations) ─┬─> MM1 (cycle guardrail)          [independent, ship anytime]
                   ├─> MM2 (fan-out hardening)         [independent, single-writer win]
                   └─> MM3 (PG loop-suppress+bootstrap) ─> MM4a (PG policies+tombstones)
                                                              └─> MM4b (tombstone GC) ─┐
                                                                                       ├─> MM7 (retention/reseed) ─> MM8 (HA) ─> MM10 (E2E gate) ─> MM11 (release)
                        MM5 (MySQL mesh, mirrors MM3+MM4) ───────────────────────────-┤
                        MM6 (Redis mesh; DOWNSTREAM of MM4; gated/deferrable) ─────────┘
   MM9 (observability/docs) runs alongside MM3–MM8 and lands before MM10.
```

- **MM3→MM4a→MM4b** is the spine: Postgres proves loop suppression, LWW over
  `(version,node_id)`, tombstones, and bounded tombstone GC end to end. MM5 mirrors it;
  **MM6 is downstream of MM4** (it reuses MM4's neutral abstractions) and is the
  high-risk tall pole that may slip.
- **MM1 and MM2 ship immediately** and deliver value (safety; single-writer multi-cloud)
  before any mesh mechanics exist.
- **MM4b gates tombstones** (no unbounded growth on an unowned source); **MM7 gates
  correctness under default retention**; **MM8 gates "production-ready."**

---

## Backward-compatibility gate (BC)

Spec §7 as a **testable gate at every milestone**. The one-way path is protected by
construction:

| Surface | One-way behaviour preserved because… | Verified by |
|---|---|---|
| **Config** | `meshes:`/`node_id` optional; strict parsing still rejects typos; the *only* new rejection is an un-declared one-way cycle (already-corrupt config) | MM0 golden-config test; MM1 validation tests |
| **Triggers (PG/MySQL)** | origin guard added **only for mesh tables**; a one-way source is never applied to (marker never set), a one-way target has no triggers; guard defaults to "capture" when unset | MM3/MM5 trigger-DDL golden tests + one-way capture tests |
| **Apply / copy** | conflict policy + marker engage only on mesh edges/bootstrap; one-way apply stays blind source-wins overwrite + delete-to-match; one-way copy sets no marker | MM3/MM4/MM5 apply+copy tests |
| **Delta table** | **v1 adds no column** to the delta table (full mesh needs no origin column; tombstones are a separate mesh-only table) — one-way delta DDL is literally unchanged | MM3/MM5 delta-DDL golden tests |
| **State schema** | new mesh-only tables (tombstones) + the MM2 progress re-key are in-place migrations preserving in-flight rows; consumption uses explicit column lists (verified `consume.go`), so nothing new perturbs it | MM2/MM4 migration tests (upgrade with live deltas) |
| **Redis** | metadata keyspace, tombstones, redesigned sweep gated to mesh mode | MM6 gating test; `test/loadgen-redis` unchanged |
| **Metrics** | additions are **new distinct series**, never new labels on or renames of existing ones (the contract test asserts exact label sets) | MM9 contract test |
| **Whole suite** | multi-master gets its **own** harness; no existing test is edited | MM10 gate: full pre-existing suite + both load harnesses green, unedited |

**A required edit to an existing one-way test is a hard stop** — the design changes
instead. Rollout is feature-flagged by the presence of a `meshes:` block.

---

## Decision log (quick reference)

| Topic | Decision |
|---|---|
| Overriding constraint | **One-way never breaks** (spec §7); per-milestone BC gate; a required edit to an existing one-way test is a hard stop. |
| Config model | **Additive, opt-in**: `node_id` + a `meshes:` block; existing `sources/targets/syncs` untouched. No mesh ⇒ identical behaviour. |
| Cycle safety | **Refuse un-declared one-way cycles** (MM1), independent safety fix; only new rejection, only for already-corrupt configs. |
| Topology scope (v1) | **Full mesh only.** Ring/partial-mesh forwarding + the source-side `origin` column are **deferred** — a full mesh needs no origin column, so one-way delta DDL stays literally unchanged. |
| Loop suppression (v1) | **Skip-only.** PG: `replicare.apply` custom GUC (no superuser) + trigger `WHEN` guard. MySQL: `@replicare_apply` user variable + `IF … IS NULL` guard in all three trigger bodies, reset on release. **Bootstrap copy/reseed into a mesh member also sets the marker** (no self-capture echo). |
| Conflict policy | **`lww` over `(version, node_id)`** is the symmetric active-active policy — totally ordered, no ties; the version column is app-supplied and **travels in the row** (no side table); refused without a declared, member-wide, comparable version column (pre-flight). **`priority-fenced`** is the only metadata-free option and is **explicitly non-symmetric** (single-writer-with-failover per key), not the default. `custom` deferred (interface not precluded). No wall-clock default. |
| Deletes in a mesh | **Tombstones** `(version, node_id)` in a mesh-only table (PG/MySQL) or the metadata keyspace (Redis), compared under the same order; **bounded by coordinated GC (MM4b)** so no unbounded growth and no premature resurrection. |
| Redis multi-master | **Metadata keyspace (hash-tag-co-located) + tombstone delete redesign**, atomic via Lua/`MULTI`; **downstream of MM4**; highest risk; **may be deferred** to a later release with single-writer fan-out (MM2) as the alternative. No sub-key CRDT merge. |
| Ordering / causality | **No commit-order signal exists**; convergence rests on the totally-ordered `(version, node_id)` (LWW) or on priority fencing — never wall-clock causality. |
| Convergence proof | **Content-checksum oracle across all N nodes** after quiescence (not equal counts), a numeric convergence SLA, and a loop metric → 0 (MM10). |
| Fan-out | **Harden first (MM2)** with a named progress re-key migration + a documented multi-target-upgrade edge case; delivers single-writer multi-cloud independent of mesh work. |
| Grants | Combined mesh-member grant/ACL set is an acceptance item in **MM3/MM5/MM6** (not deferred to docs), with `deploy/` preset updates. |
| HA | **Per-edge `pg_advisory_lock` leader election + cursor fencing (MM8)** before any mesh is production-ready; one-way per-sync ownership unchanged. |
| Retention/reseed × mesh | Named reconciliation (**MM7**): reseed runs with the bootstrap marker and must not resurrect a tombstoned key. |
| Engine scope | **Single-engine per mesh** (CLAUDE.md §6); cross-engine permanently out. |
| Schema evolution | In-place idempotent migrations, `schema_version` bump, in-flight deltas/cursors preserved; **no new column on the one-way delta table in v1**. |

---

## Open questions / risks

- **Version-column contract for `lww`.** The app must guarantee per-node monotonicity;
  replicare supplies the `node_id` tiebreak. Is a replicare-maintained hybrid-logical
  clock worth offering later so users need not maintain a column? (Deferred; `lww` with
  a declared column ships first.)
- **Redis metadata atomicity & cost.** A metadata entry per key ~doubles key count and
  needs Lua/`MULTI` atomicity; big-key and cluster-slot (hash-tag) interactions are the
  single biggest risk in the plan; crash-injection atomicity is an explicit MM6
  acceptance item.
- **Tombstone GC coordination.** MM4b must ensure every member has observed a tombstone
  before GC, without a global coordinator — likely a per-mesh min-observed-version
  watermark across members' cursors. Sub-design before MM4b ships.
- **Least-privilege re-verification.** `replicare.apply` GUC (PG) and the MySQL user
  variable re-checked on RDS/Cloud SQL as MM3/MM5 acceptance; `deploy/` grant presets
  updated.
- **Schema drift across members.** Pre-flight must check the version column (and general
  compatibility) on **every member pair**, not just source→target (named in MM4a).
- **N-way convergence property.** MM10's storm tests must actually establish
  order-independent convergence (the checksum oracle), and a short proof sketch that
  `(version, node_id)` LWW is associative/commutative (a join-semilattice max) should
  accompany MM4a so the property is argued, not just tested.
- **`priority-fenced` data loss is by design.** Because it silently drops a
  lower-priority node's concurrent write, it must be documented as non-symmetric and
  never presented as active-active; consider a config-load warning when it is selected.
