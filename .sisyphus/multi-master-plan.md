# replicare — Multi-Master (Active-Active) Implementation Plan (v3)

> **Status: PLAN / NOT STARTED.** Companion to the design note
> [`docs/multi-master.md`](../docs/multi-master.md). Read the spec first; this plan
> turns it into milestones with acceptance criteria, a critical path, and — first
> among equals — the invariants that keep the existing one-way path unchanged.
>
> **v3** locks the product decisions the owner set: **full active-active on all three
> engines (Redis included, not deferred); N-node full mesh (no 3-node limit);
> conflict resolution with ZERO user schema change and NO user-specified column —
> replicare maintains a hidden per-row version itself; no priority-fencing; tombstones
> (and the version records) live in the `replicare` schema and are GC'd regularly;
> must work with no superuser anywhere; config surface is `clusters:`.** These
> supersede v2's app-supplied-version / source-priority / Redis-deferral ideas.

Covers **Postgres, MySQL, and Redis**, phased so each milestone is independently
shippable, low-risk-first.

---

## Guiding constraints (carried from CLAUDE.md + the design note + owner direction)

Non-negotiable; every milestone is checked against them.

1. **BC-INVARIANT (overriding): one-way replication must never break.** A config with
   no `clusters:` block and no `node_id:` parses, behaves, and performs **identically**
   to today. Spec §7. A required edit to an existing one-way test is a **hard stop** —
   the design changes instead. See the [BC gate](#backward-compatibility-gate-bc).
2. **No superuser — anywhere, ever (owner-emphasized).** These databases run on
   arbitrary hosts/managed providers where superuser is often unavailable. Every
   mechanism — loop suppression, the version register, tombstones, capture — uses only
   grantable privileges (the same class §12 already documents). Superuser being present
   is fine; **requiring** it is forbidden. Verified on managed offerings as milestone
   acceptance.
3. **Zero user schema change for conflict resolution (owner-directed).** The user adds
   no column and specifies nothing. replicare stores the per-row version it needs in its
   **own** `replicare` schema (a side "version register" table on PG/MySQL; the metadata
   keyspace on Redis) — never in the user's tables. See [Conflict model](#conflict-model).
4. **Faithful transport, never transform (CLAUDE.md §1.7).** Version/origin are metadata
   *about* a change; the value moves verbatim. Redis value keys are never wrapped.
5. **Conservative, version-tolerant source SQL (CLAUDE.md §1.6).**
6. **Engine-agnostic core, engine-specific mechanism (CLAUDE.md §5).**
7. **Schema evolution never drops in-flight state (CLAUDE.md §14).** In-place idempotent
   migrations, `schema_version` bump, deltas/cursors preserved.
8. **Single-engine per cluster (CLAUDE.md §6).** A cluster is one engine, like a sync.
   Cross-engine multi-master is permanently out of scope.

---

## The crux: what one-way assumes that multi-master breaks

One-way is **source-authoritative and provenance-free**: one truth, the target
overwritten to match, one direction so a change never returns. Active-active
invalidates all three, and the code has no seam for any (grounded per-engine in spec
§2/§4, re-verified against the tree):

1. **No replication origin/version** → **loops** (replicare's own applied writes are
   re-captured — PG/MySQL triggers fire on them, Redis re-`RESTORE`s them) **and** no
   way to order two concurrent writes to the same key.
2. **No conflict resolution, and no commit-order signal** (the trigger-CDC premise,
   CLAUDE.md §3.3) → true causal ordering is impossible; convergence must rest on a
   **replicare-maintained total order** per row.
3. **No topology safety** — `Config.Validate()` does no cycle detection (verified,
   `internal/config/config.go:221-275`), so a bidirectional/ring wiring corrupts data
   **silently** today.

**Two facts that shape the design:**

- **Conflict resolution needs a per-row version, and the PK cannot be it.** The PK is
  the *conflict key* — two colliding writes share it — so it cannot pick a winner. Per
  owner direction we will **not** ask the user for a version column, so **replicare
  maintains a hidden version register itself** (see [Conflict model](#conflict-model)).
  This is the central design of v3 and it unifies all three engines.
- **A full mesh needs no origin *column*; skip-suppression + version comparison suffice.**
  With direct edges (every node → every other), not re-capturing apply writes prevents
  loops, and version comparison makes any stray re-delivery a no-op (self-limiting). So
  **v1 is N-node full mesh**, loop suppression is skip-only, and a source-side origin
  *column* + hop-to-hop forwarding (only needed for rings/partial meshes) is a deferred
  post-v1 topology. This also keeps the one-way delta table DDL *literally* unchanged.

**Redis is still the tallest pole** (capture-less, value-opaque → the version register
must be a parallel metadata keyspace with atomic writes and a redesigned delete path)
**but it is in scope for v1, co-equal with PG/MySQL — not deferred** (owner-directed).

---

## Conflict model (the v3 decision)

**One policy, zero-config: replicare-managed hybrid-logical-clock (HLC) last-write-wins.**

- **Version register.** For every replicated row/key, replicare keeps a hidden record
  **`PK → (hlc, origin_node)`** in its own schema — a side table in the `replicare`
  schema (PG/MySQL) or the metadata keyspace (Redis). No user column; nothing declared.
- **HLC (hybrid logical clock).** Each node maintains an HLC — `max(physical_now,
  last_seen_hlc) + logical_tick` — giving a monotonic counter that tracks wall-clock
  time and respects observed causality. Every change is stamped `(hlc, node_id)`; the
  **`node_id` component breaks equal-HLC ties**, so the order is **total, with no ties**.
- **Resolution.** On apply of an incoming change for `PK`, compare `(hlc_in, node_in)`
  against the register's stored `(hlc, node)`: apply (value + register update) iff
  strictly greater; otherwise **no-op**. The register update stores the *winner's*
  `(hlc, node)` (provenance travels with the value), and the local HLC is advanced past
  `hlc_in` (HLC merge). This is a **LWW-register CRDT**: a join-semilattice max →
  **converges for any N, regardless of message arrival order.**
- **Local writes.** Captured PK-only by the trigger; at drain, replicare re-reads the
  current value, stamps a fresh `(hlc, self)`, writes the register, and ships
  `(value, hlc, self)`. Coalesced multiple local writes ⇒ the latest value with the
  latest HLC (monotonic per node).
- **Deletes = tombstones** carrying `(hlc, node_id)` in the same register/keyspace, so a
  delete competes with a concurrent update under the same total order (delete wins iff
  its `(hlc,node)` is greater). Tombstones **and** version records for deleted keys are
  **GC'd** once every peer has observed a version ≥ them (a per-cluster min-observed
  watermark across peers' cursors).
- **No priority-fencing, no user version column, no `custom` resolver in v1.** (The
  policy interface stays open so a custom resolver could be added later, but v1 ships
  exactly the HLC-LWW default and nothing to configure.)

**Cost, stated honestly.** The register is ~one small row per replicated row (PK + ~16
bytes of `(hlc, node_id)`) — the size of an extra index. That is the price of touching
none of the user's schema; the rejected alternative was a version column on user tables.
Tombstones + deleted-key records are GC'd; live-row records persist as long as the row.

**Loop suppression is belt-and-suspenders.** Version comparison already makes a
re-received non-newer change a no-op (so loops are self-limiting and rings are safe
later), but the **skip-guard** (below) is the primary mechanism — it stops the wasteful
re-capture/re-ship entirely and keeps the delta tables from churning.

---

## Goals & non-goals

**Goals**
- **Active-active convergence of N same-engine nodes (full mesh, any N ≥ 2)** for
  **Postgres, MySQL, and Redis**, writes on any node, under HLC-LWW, with no loop and a
  deterministic, order-independent outcome.
- **Zero-config conflict resolution** — no user column, no schema change (the version
  register is replicare's own).
- **Loop suppression with no superuser** (PG `replicare.apply` GUC; MySQL
  `@replicare_apply` user variable) + version-comparison backstop.
- **Tombstones + version records in the `replicare` schema, GC'd** so the source
  footprint stays bounded (a source we may not own — CLAUDE.md §3.4).
- A **safe, opt-in `clusters:` config surface** that leaves the one-way path untouched,
  plus cycle-detection that closes today's silent-corruption footgun.

**Non-goals (v1)**
- Cross-engine clusters (permanent, §8).
- **Ring / partial-mesh forwarding** and the source-side origin *column* — deferred
  post-v1 (full mesh only; version comparison already makes rings *safe*, but efficient
  hop-suppression forwarding is later work).
- Sub-key / field-level CRDT merge on any engine (values are replaced wholesale;
  Redis values stay opaque).
- A user-supplied version column or any priority/fencing policy (explicitly rejected).
- Wall-clock-only LWW with no logical component (HLC includes the logical + node tie so
  clock skew cannot cause divergence, only a bounded ordering wobble — see risks).
- Synchronous/quorum writes, global ordering, distributed transactions, automatic
  schema propagation across members.

---

## Definition of Done (multi-master)

1. **N nodes (tested at N=3 and N≥5 to prove no 3-node limit)**, writes on all, per
   engine (PG, MySQL, Redis), **converge** under continuous mixed churn — proven by the
   **convergence oracle** (identical full-row-set / keyspace content checksum across all
   N nodes after quiescence) within a **numeric convergence SLA**, with the **loop
   metric** at zero after quiescence.
2. **Every existing one-way integration test + both load harnesses pass unchanged** —
   the BC gate.
3. Conflict resolution requires **no user schema change** and **no user configuration**
   beyond declaring the cluster membership.
4. Everything works with **no superuser**, verified on a managed offering per engine.
5. Tombstones + version records are GC'd; source footprint stays bounded under sustained
   delete churn.
6. Config validation refuses an un-declared one-way cycle and validates clusters.
7. Docs + `CLAUDE.md` decision log updated.

---

## Cross-cutting foundations (MM0, reused everywhere)

- **Node identity.** Stable `node_id` per endpoint (config-declared, persisted with
  daemon state). The origin tag and HLC tie component. Optional; ignored one-way.
- **HLC service.** A neutral hybrid-logical-clock (persisted per node so it never goes
  backwards across restarts) stamping `(hlc, node_id)` and merging on apply.
- **Version register.** Neutral `PK → (hlc, node_id, deleted?)` store; engine-specific
  backing (PG/MySQL side table in `replicare`; Redis metadata keyspace). Read on apply,
  written atomically with the value.
- **Loop suppression (skip-only, v1).** Apply + cluster-bootstrap copy mark their writes
  replicare-origin; capture skips marked writes. Version comparison is the correctness
  backstop.
- **Tombstones + GC.** `(hlc, node_id)` delete markers in the register/keyspace, bounded
  by a coordinated min-observed-version watermark across peers.
- **Topology model.** `topology: mesh` expands to the full set of directed edges for N
  members; each edge has its own cursor namespace + ownership lock.
- **State/schema migration.** In-place, `schema_version`-bumped; **the one-way delta
  table gains no column** (version register + tombstones are separate mesh-only objects).
- **Observability.** New **distinct** metrics/log/trace signals (never new labels on
  existing series — the contract test asserts exact label sets): conflicts resolved
  (winner origin), loop-suppressed-write rate, HLC skew, version-register + tombstone
  backlog, per-edge lag. Added to the F2 contract with M6-style verification.

---

## Milestones (MM0–MM11)

Low-risk-first. Each lists **Acceptance** and **BC proof** ("full one-way suite green" =
the entire existing integration + load-harness suite passes unedited).

### MM0 — Neutral foundations: node identity, HLC, version-register + policy types, `clusters:` config (inert)
Neutral types (`NodeID`, HLC, `VersionRegister`, the single HLC-LWW policy, a `Cluster`/
edge topology) + config (`node_id` on `Endpoint`, a `clusters:` block) — **parsed and
validated but wired to no engine behaviour.**
- **Acceptance:** configs with `node_id`/`clusters:` parse and round-trip; configs
  without them behave identically; HLC unit tests (monotonic across simulated restarts,
  correct merge); new types unused by one-way.
- **BC proof:** full one-way suite green, zero edits; golden test: a representative
  existing config resolves identically.

### MM1 — Cycle-detection guardrail (safety; ship first, standalone)
`Config.Validate()` refuses an **un-declared cycle** among one-way `syncs` not part of a
`clusters:` block (spec §4). Only *adds* a rejection for already-corrupt configs.
- **Acceptance:** `A→B`+`B→A` plain syncs rejected naming the cycle; a fan-out/fan-in DAG
  still validates; a cluster-declared mesh is not rejected.
- **BC proof:** every existing valid fixture still validates; full one-way suite green.

### MM2 — Fan-out hardening (now a mesh PREREQUISITE)
Each mesh node fans out to its N−1 peers, so correct multi-target fan-out is on the
critical path (not an alternative). Re-key initial-copy progress to include **target**
(today `(sync, schema, table)` with no target, verified `internal/state/postgres/progress.go:39-48`;
the streaming cursor already includes target, `:97-106`). **Migration recipe (named):**
add nullable `target` → backfill per each sync's targets → swap PK to
`(sync, target, schema, table)`. **Edge case (named):** a multi-target sync already
streaming shares one progress row; on upgrade extra targets re-copy once (documented
one-time cost; multi-target streaming was never correct pre-MM2). Add a 2+-target E2E
convergence test.
- **Acceptance:** one source → two targets converge from cold copy and stay converged;
  per-target progress independent; crash mid-copy resumes each target; migration
  preserves single-target progress with no re-copy.
- **BC proof:** single-target behaviour unchanged; full one-way suite green.

### MM3 — Postgres: loop suppression + version register + cluster bootstrap
- `WHEN (current_setting('replicare.apply', true) IS NULL)` on the capture trigger; the
  apply tx sets `SET LOCAL replicare.apply = '1'`. **Bootstrap paths set it too:** the
  initial copy (`internal/copy`, `COPY FROM STDIN`) and reseed **into a cluster member**
  run marked, so cold-copy writes are not self-captured (no echo storm) — and bootstrap
  **seeds the version register** from the source's register so copied rows carry their
  true `(hlc, node)`, not a fresh local stamp.
- **Version register** as a side table in the `replicare` schema; the HLC service; write
  the register on local-change drain. Origin-aware triggers + register only for cluster
  tables; **one-way trigger DDL and delta DDL are byte-identical**, and one-way adds no
  register.
- **Grants (named, not deferred):** the *combined* cluster-member grant set (source
  `TRIGGER`+`SELECT`+schema `CREATE`/`USAGE` **and** target `INSERT/UPDATE/DELETE`; §12
  `RemoveCapture` ownership asymmetry applies on every member), verified on a
  no-superuser managed role.
- **Acceptance:** 2-node PG mesh — a write on A applied to B is not re-captured on B; a
  local B write is captured; **bootstrapping fresh member B produces no delta rows on B
  and B's register matches A's**; all of it on a least-privilege, non-superuser role.
- **BC proof:** one-way trigger/delta DDL byte-identical (guard + register only for
  cluster tables); one-way apply/copy set no marker, write no register; full PG one-way
  suite green.
- **Floor:** cluster members need PG ≥ 9.6 (2-arg `current_setting`), matching the
  existing capture floor.

### MM4 — Postgres: HLC-LWW conflict resolution + tombstones + GC
- **MM4a — resolution + tombstones.** Apply becomes a **version-guarded upsert**: apply
  the value + register `(hlc,node)` iff `(excluded.hlc, excluded.node) > (register.hlc,
  register.node)`, else no-op; advance local HLC. **Tombstones** `(pk, hlc, node)` in the
  `replicare` schema so delete-vs-update resolves under the same order.
- **MM4b — GC (named; tombstones/register do not ship without it).** Coordinated
  min-observed-version watermark across peers so a tombstone/deleted-key record is
  removed only after **every** member has a version ≥ it (no member resurrects a deleted
  key); bounded-growth acceptance metric.
- **Acceptance:** 3-node PG mesh, concurrent same-key writes converge to the **same**
  value on all nodes **regardless of arrival order** (oracle), with the winner
  determined by `(hlc, node)` (incl. the `node` tie-break at equal HLC); delete-vs-update
  resolves via tombstones; tombstone/register backlog stays bounded under sustained
  delete churn. The **multi-master load harness** (extending `test/loadgen`) drives
  N-node convergence and asserts the [oracle](#mm10--multi-master-e2e-hardening-gate).
- **BC proof:** resolution/tombstones engage only on cluster edges; one-way apply stays
  blind source-wins overwrite + delete-to-match; tombstone table + register are
  mesh-only; full PG one-way suite green.

### MM5 — MySQL mesh (mirror MM3+MM4)
Mirror loop suppression, bootstrap-marker + register seeding, HLC-LWW, tombstones, GC.
Loop suppression: `@replicare_apply` user variable set at `BeginApply`, `IF
@replicare_apply IS NULL THEN … END IF` in **all three** trigger bodies, reset on
connection release alongside `FOREIGN_KEY_CHECKS` (verified reset seam,
`apply_tx.go:222-229`; `MaxOpenConns=1` isolates it). Delta trigger inserts use explicit
column lists (no capture perturbation). Bootstrap `LOAD DATA` runs marked + seeds the
register. Combined no-superuser grants + `deploy/` presets are MM5 acceptance.
- **Acceptance:** 3-node MySQL mesh converges under HLC-LWW; the marker is proven
  isolated to the pinned apply connection; bootstrap produces no self-capture and seeds
  the register; install still blocks a pre-existing non-replicare AFTER trigger; GC
  bounded; no superuser.
- **BC proof:** one-way trigger bodies unchanged for non-cluster tables; one-way
  apply/copy set no variable, write no register; full MySQL one-way suite green.

### MM6 — Redis mesh (metadata keyspace = version register + tombstones) — co-equal, NOT deferred
The tallest pole, but in scope. A **parallel metadata keyspace** — per data key `K`, a
sibling entry under a reserved prefix **hash-tagged into K's cluster slot** — holds the
register `{hlc, node_id, deleted_at?}`, written **atomically with** the value via
Lua/`MULTI`. Redesign delete reconciliation from today's stateless "missing at source ⇒
DEL" (verified `internal/pipeline/delete.go:27-58`) to **tombstone + version aware**:
delete a target key only when a tombstone with a greater `(hlc,node)` exists. HLC-LWW
uses the metadata `(hlc,node)`; values never merged. Bootstrap into a member seeds
metadata (no spurious tombstones). Combined ACLs (`+restore` on every node + metadata/
tombstone keyspace RW, no admin) + `deploy/acl-*` updates are MM6 acceptance.
- **Acceptance:** N-node Redis mesh, writes on all nodes, converges (oracle: after
  quiescence per-key `RESTORE` rate → 0 **and** all nodes' keyspace checksums match); **no
  key destroyed by a peer's delete sweep**; metadata/value atomic under crash injection;
  cluster: metadata co-locates with its value slot; GC bounded; no admin ACL.
- **BC proof:** metadata keyspace, tombstones, redesigned sweep **gated to cluster mode**;
  one-way Redis keeps the stateless SCAN-reconcile + target-vs-source sweep verbatim;
  full Redis one-way suite + `test/loadgen-redis` green.

### MM7 — Cluster lifecycle: retention & reseed reconciliation
Reconcile clusters with the default-on retention/reseed machinery (CLAUDE.md §3.4;
`docs/reseed-state-machine.md`). A slow member hitting the cap → needs-reseed → re-copy
must (a) run marked (MM3) so it doesn't self-capture, (b) **seed the register**, and
(c) **not resurrect tombstoned keys**.
- **Acceptance:** a member forced to reseed re-converges without echoing its copy, seeds
  its register, and a key tombstoned before the reseed **stays deleted** after (named
  test); retention bounds source growth without breaking convergence.
- **BC proof:** one-way retention/reseed unchanged; full one-way suite green.

### MM8 — HA / leader election (before "production-ready")
Generalize ownership from per-sync to **per-edge**; implement `pg_advisory_lock` leader
election + cursor fencing (today stubbed). A cluster must not split-brain.
- **Acceptance:** two daemons contending for one edge → exactly one drives it; a fenced
  cursor stops a deposed leader; failover resumes from checkpoint with no lost/double
  apply (idempotency + version register).
- **BC proof:** one-way per-sync ownership unchanged; full one-way suite green.

### MM9 — Observability, docs & CLAUDE.md
F2 contract entries as **new distinct series** (conflicts resolved by winner origin,
loop-suppressed-write rate, HLC skew, version-register/tombstone backlog, per-edge lag —
**not** new labels on existing series) with M6-style verification; update
`docs/multi-master.md` (plan→shipped where true), engine pages, configuration reference,
`CLAUDE.md`, and the combined no-superuser grant docs.
- **Acceptance:** `/metrics` matches the extended contract; docs describe `clusters:`,
  the zero-config HLC-LWW model, and per-engine no-superuser grants (and disambiguate a
  replicare `clusters:` entry from a Redis `mode: cluster` endpoint).
- **BC proof:** contract additions rename/remove/relabel nothing existing; full one-way
  suite green.

### MM10 — Multi-master E2E hardening gate
The shippable gate, with **concrete oracles**. A matrix per engine at **N=3 and N≥5**
(proving no 3-node limit): kill a node mid-churn; partition; concurrent same-key storms;
delete/update races; clock-skew injection (HLC robustness).
- **Convergence oracle:** after churn stops, **all N nodes have identical full row-set /
  keyspace content checksums** (reuse the loadgen checksum across every node pair) — not
  merely equal counts.
- **Convergence SLA:** reached within a defined bound **T** of quiescence.
- **Loop metric:** replicare-origin re-capture rate → 0 after quiescence; total
  applies-per-quiescent-key bounded.
- **Acceptance:** the matrix passes for PG, MySQL, and Redis against all three oracles;
  the full pre-existing suite + both load harnesses green, unedited.

### MM11 — Release & packaging
Version bump, changelog, Helm/values surface for `clusters:`+`node_id`, sample
multi-master configs in `examples/`, the multi-master load harness documented in
`docs/operations.md`.
- **Acceptance:** a released binary runs an N-node mesh from a documented example; the
  demo converges (oracle); operations doc covers running and verifying a cluster.

### Deferred (post-v1): ring / partial-mesh forwarding
The source-side origin *column*, a *record-origin* trigger body, and hop-to-hop
forward-suppression — an efficiency feature for non-full-mesh topologies. Version
comparison already makes such topologies *safe*; this milestone makes them *efficient*.
Out of v1 so v1 adds no column to the one-way delta table.

---

## Critical path

```
MM0 (foundations) ─┬─> MM1 (cycle guardrail)             [independent, ship anytime]
                   └─> MM2 (fan-out hardening; PREREQ) ─> MM3 (PG loop+register+bootstrap)
                                                            └─> MM4a (HLC-LWW+tombstones) ─> MM4b (GC) ─┐
                                                                                                        ├─> MM7 (retention/reseed) ─> MM8 (HA) ─> MM10 (E2E gate) ─> MM11 (release)
                        MM5 (MySQL mesh, mirrors MM3+MM4) ─────────────────────────────────────────────┤
                        MM6 (Redis mesh, DOWNSTREAM of MM4; CO-EQUAL, not deferred) ────────────────────┘
   MM9 (observability/docs) runs alongside MM3–MM8, lands before MM10.
```

- **MM2 → MM3 → MM4a → MM4b** is the spine: fan-out (each node ships to N−1 peers) then
  Postgres proves loop suppression, the HLC version register, HLC-LWW, tombstones, and
  bounded GC. **MM5 and MM6 are co-equal mirrors downstream of MM4** — Redis is **not**
  deferred.
- **MM1 ships immediately** (pure safety). **MM4b gates tombstones**, **MM7 gates
  correctness under default retention**, **MM8 gates production-readiness.**

---

## Backward-compatibility gate (BC)

Spec §7 as a per-milestone gate. The one-way path is protected by construction:

| Surface | Preserved because… | Verified by |
|---|---|---|
| **Config** | `clusters:`/`node_id` optional; strict parsing still rejects typos; the only new rejection is an un-declared one-way cycle (already-corrupt config) | MM0 golden-config test; MM1 tests |
| **Triggers (PG/MySQL)** | guard added **only for cluster tables**; a one-way source is never applied to (marker never set), a one-way target has no triggers; guard defaults to "capture" when unset | MM3/MM5 trigger-DDL golden tests |
| **Apply / copy** | version register, HLC-LWW, and marker engage only on cluster edges/bootstrap; one-way apply stays blind source-wins + delete-to-match; one-way copy sets no marker, writes no register | MM3/MM4/MM5 tests |
| **Delta table** | v1 adds **no column** (full mesh needs none; register + tombstones are separate mesh-only objects) — one-way delta DDL literally unchanged | MM3/MM5 delta-DDL golden tests |
| **State schema** | new mesh-only objects (version register, tombstones) + the MM2 progress re-key are in-place migrations preserving in-flight rows; consumption uses explicit column lists (verified `consume.go`) | MM2/MM4 migration tests |
| **Redis** | metadata keyspace, tombstones, redesigned sweep gated to cluster mode | MM6 gating test; `test/loadgen-redis` unchanged |
| **Metrics** | additions are new distinct series, never new labels/renames | MM9 contract test |
| **Whole suite** | multi-master gets its own harness; no existing test edited | MM10 gate |

**A required edit to an existing one-way test is a hard stop.** Rollout is feature-flagged
by the presence of a `clusters:` block.

---

## Decision log (quick reference)

| Topic | Decision |
|---|---|
| Overriding constraint | **One-way never breaks** (spec §7); per-milestone BC gate; editing an existing one-way test is a hard stop. |
| Scope | **Full active-active on all three engines — Redis included, not deferred** (owner-directed). |
| Topology | **N-node full mesh, any N** (no 3-node limit). HLC-LWW converges for any N. Ring/partial forwarding deferred (safe via version comparison, efficient later). |
| Conflict resolution | **replicare-managed HLC last-write-wins over `(hlc, node_id)`** — total order, no ties. **Zero user schema change, no user-specified column, PK is not the version** (PK is the conflict key, not a tiebreaker). Single zero-config policy. **No priority-fencing, no app version column, no custom resolver in v1.** |
| Version register | Hidden `PK → (hlc, node_id, deleted?)` in the **`replicare` schema** (PG/MySQL side table; Redis metadata keyspace) — ~1 row per replicated row, written atomically with the value. |
| Loop suppression (v1) | **Skip-only** (PG `replicare.apply` GUC + trigger `WHEN`; MySQL `@replicare_apply` var + body guard), **plus version-comparison backstop** (non-newer re-delivery is a no-op). **Bootstrap copy/reseed run marked AND seed the register.** |
| Deletes | **Tombstones `(hlc, node_id)`** in the register/keyspace; delete-vs-update under the same order. |
| GC | **Tombstones + deleted-key records GC'd** by a coordinated min-observed-version watermark across peers (MM4b) — bounded source footprint (owner: "cleaned out regularly"). |
| No superuser | **Required nowhere** (owner: "work regardless of superuser"). Grantable primitives only; verified on managed offerings per engine. |
| Ordering / causality | No commit-order signal; convergence rests on the replicare-maintained **HLC total order**, never wall-clock alone (HLC = physical + logical + node). |
| Config surface | **`clusters:` block** + optional `node_id`; existing `sources/targets/syncs` untouched. Disambiguated from a Redis `mode: cluster` endpoint in docs. |
| Fan-out | **A mesh prerequisite** (each node fans out to N−1 peers); hardened in MM2 with a named progress re-key migration. |
| Cycle safety | **Refuse un-declared one-way cycles** (MM1). |
| Convergence proof | **Content-checksum oracle across all N nodes** (N=3 and N≥5), a numeric SLA, a loop metric → 0. |
| HA | **Per-edge `pg_advisory_lock` leader election + cursor fencing (MM8)** before production-ready; one-way ownership unchanged. |
| Engine scope | **Single-engine per cluster** (CLAUDE.md §6); cross-engine permanently out. |
| Faithful transport | Preserved — version/origin are metadata, never a value mutation; Redis value keys never wrapped. |
| Schema evolution | In-place idempotent migrations, `schema_version` bump; **no new column on the one-way delta table**. |

---

## Open questions / risks

- **Version-register storage cost.** ~1 row per replicated row is accepted as the price
  of zero user-schema change. For very large tables this is index-sized but non-trivial;
  MM3 acceptance should measure it, and a future optimization could prune records for
  rows never involved in a conflict (hard to know a priori — likely not worth it).
- **HLC clock skew.** HLC bounds divergence to the skew between nodes for the *ordering*
  of truly-concurrent writes (never for convergence — the `node_id` tie guarantees a
  total order and all nodes still converge to the *same* winner). Document the assumed
  max skew and surface an HLC-skew metric (MM9); large skew makes "latest wins" less
  wall-clock-intuitive but never divergent.
- **Tombstone/register GC coordination** needs a per-cluster min-observed-version
  watermark without a global coordinator — likely derived from peers' cursors. Sub-design
  before MM4b ships.
- **Redis metadata atomicity & cost** — a metadata entry per key ~doubles key count and
  needs Lua/`MULTI` atomicity; big-key and cluster-slot (hash-tag) interactions are the
  single biggest risk; crash-injection atomicity is an explicit MM6 acceptance item.
- **Full-mesh edge scaling.** N·(N−1) directed edges; fine for small-to-moderate N. Beyond
  ~tens of nodes a hub/ring would be preferable — that is the deferred forwarding work,
  not a v1 concern, but note it so N isn't assumed unbounded in practice.
- **Bootstrap register seeding across versions.** Seeding a fresh member's register from a
  peer must carry `(hlc, node)` faithfully; verify it survives a mixed-version cluster
  (old source ↔ newer peer) — MM3/MM7 acceptance.
- **N-way convergence proof.** MM10's storm tests establish order-independent convergence
  empirically; a short proof sketch that HLC-LWW is a join-semilattice (max) should
  accompany MM4a so the property is argued, not only tested.
