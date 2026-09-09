# Multi-master replication — design note

> **Status: DESIGN / NOT IMPLEMENTED.** This document is a forward-looking plan.
> replicare today is a strictly **one-directional, source-authoritative**
> replicator for all three engines. Nothing described under "Proposed design"
> exists in the code yet. The **hard constraint** on any of this work is that the
> existing one-way path (source → target(s), changes never flow back) must keep
> behaving **exactly** as it does today — see [§7 Backward compatibility](#7-backward-compatibility-the-non-negotiable).

This note covers **Postgres, MySQL, and Redis**. It records what exists now, why
naively wiring a bidirectional topology breaks today, the mechanisms a real
multi-master mode would need per engine, the config-schema changes, and a phased
plan. It is written to sit alongside `CLAUDE.md` (multi-master is listed there as
roadmap, §6/§14) and the per-engine pages.

**Direction set by the project owner (v3):** full active-active is required on **all
three engines, Redis included (not deferred)**; **N-node full mesh** (no fixed node
count); conflict resolution must need **no user schema change and no user-specified
column** (replicare manages a hidden version itself — §5.3); **no priority-fencing**;
tombstones live in the `replicare` schema and are **GC'd**; it must work with **no
superuser anywhere**; the config surface is **`clusters:`**. The milestone-level plan is
[`.sisyphus/multi-master-plan.md`](../.sisyphus/multi-master-plan.md).

---

## 1. Goal

Keep **N database instances (across different clouds) converged**, where writes may land
on **any** node — i.e. **active-active / multi-master**, not just one writer fanned out to
read replicas. The target topology is a **full mesh of any N** (every node ↔ every node —
**no fixed node count**; 3 is only a common example). Ring / partial-mesh *forwarding* is
a deferred efficiency variant (§5.2); the version register (§5.3) already makes such
topologies *safe*.

This is distinct from what replicare does **today**: one authoritative source fanned out
to one or more **read-only** targets (§3) — which becomes the per-node building block of
the mesh rather than the end state.

---

## 2. Current behaviour (grounded in the code)

replicare is single-master by construction on every engine. The relevant facts,
with file references, because the design has to work *with* them:

**Topology / config** (`internal/config/config.go`, `internal/daemon/daemon.go`)
- A `Sync` is one `Source` → N `Targets` (`config.go:65-72`); at runtime the daemon
  spawns **one `Syncer` per (sync, target)** (`daemon.go:129-158`), and a `Syncer`
  holds a single source, single sink, single target (`internal/pipeline/syncer.go`)
  — deliberately one-directional.
- `Config.Validate()` (`config.go:221-275`) validates each sync in isolation. There
  is **no cross-sync graph analysis and no cycle detection**: two syncs `A→B` and
  `B→A`, or a ring, pass validation silently.
- Parsing is **strict** — unknown YAML keys are rejected (`config.go` `Load`), which
  is important for §6: new keys are additive and old configs are unaffected.
- Per-sync **single-engine** is enforced (`config.go:254-258`).
- Fan-out (one source → many targets) exists but is explicitly *"present, not
  hardened"* (`syncer.go` docstring) and has no end-to-end test; initial-copy
  progress is keyed `(sync, table)` **without** target (`internal/state/postgres/progress.go`),
  so two targets on one sync currently share copy progress.

**Postgres capture/apply** (`internal/engine/postgres/`)
- Capture triggers are `AFTER INSERT/UPDATE/DELETE ... FOR EACH ROW` with **no
  `WHEN` guard**; the trigger function unconditionally enqueues the changed PK
  (`capture_ddl.go`). No `current_setting`/session check.
- The apply connection sets only text-formatting GUCs (`conn.go` `sessionGUCs`) —
  **not** `session_replication_role='replica'` or anything that suppresses triggers.
- Apply is blind source-wins: `INSERT … ON CONFLICT (pk) DO UPDATE SET col =
  EXCLUDED.col` with **no `WHERE`** (`sink.go`), plus delete-to-match (`apply.go`).
- Delta rows are PK-only + `delta_id` + `rc_op` + seq/txid/`rc_at` — **no origin
  column**.

**MySQL capture/apply** (`internal/engine/mysql/`)
- Triggers are three separate `AFTER I/U/D` triggers (MySQL has no `WHEN` clause);
  the bodies unconditionally insert the PK (`capture_ddl.go`). The UPDATE body's
  `IF NOT (NEW.pk <=> OLD.pk)` is only a PK-change detector, **not** an origin guard.
- **MySQL has no `session_replication_role` and no per-session trigger-skip.** The
  apply path sets only `FOREIGN_KEY_CHECKS` (for cyclic components); no user
  variable, no origin marker.
- Apply is blind source-wins: `INSERT … ON DUPLICATE KEY UPDATE col = VALUES(col)`
  + delete-to-match (`apply.go`, `apply_tx.go`). `ON UPDATE CURRENT_TIMESTAMP`
  columns are carried **verbatim** from the source (faithfulness) — which removes
  the one timestamp signal a naive LWW might have used.
- Delta rows are PK-only + `delta_id` + `rc_op` + `k1..kn` + `rc_at` — **no origin,
  no txid**.
- Constraints that matter later: pre-8.0 allows only one trigger per (timing,event);
  install already **blocks** if a non-replicare AFTER trigger exists; InnoDB has **no
  deferrable FKs**; `FOREIGN_KEY_CHECKS` is session-level and non-transactional.

**Redis capture/apply** (`internal/engine/redis/`, `internal/pipeline/delete.go`)
- Capture-less. **Upserts** = rolling `SCAN` + `RESTORE … REPLACE` (idempotent,
  value-faithful). **Deletes** = a target-vs-source keyspace **diff**: `SCAN` the
  target, `EXISTS`-check the source master, `DEL` every target key missing at the
  source (`delete.go`, `stream.go` `MissingAtSource`).
- The transport record is `{key, ttl, flags, dump}` (`copy.go`, `framing.go`) —
  **no origin, no version**. The per-unit `changeID` is a local lag hint only; it
  never crosses nodes.
- No conflict resolution, no origin, no loop suppression anywhere.

**State / ownership** (`internal/state/`)
- Ownership is a **per-sync** Postgres advisory lock keyed by the sync *name*; cursors
  are namespaced `(sync, target, table)`. Running `A→B` and `B→A` means two separate
  sync names → two independent locks → **nothing coordinates the two directions**.
- HA / leader election is **stubbed** (a plain `pg_try_advisory_lock`; a second daemon
  is skipped with a warning), not implemented.

---

## 3. The fan-out building block (a prerequisite, not an alternative)

A single-writer **fan-out** — one authoritative primary → N **read-only** replicas (one
source → N targets) — keeps N instances converged *when all writes go to the primary*.
The owner requires **symmetric active-active** (writes on any node), so fan-out is **not**
the endpoint here — but it is the **building block the mesh is made of**: each cluster node
fans its local changes out to its N−1 peers. So hardening fan-out is the first substantive
milestone (§8 phase 2), on the critical path.

Caveats it must close (they matter for the mesh too):
- Fan-out is *present, not hardened* and **untested end-to-end**; initial-copy progress
  must be re-keyed to include the target before it is trustworthy for 2+ targets.
- Nothing enforces "read-only" on a plain fan-out target — but in a cluster every node is
  intentionally writable, and correctness there comes from the version register + HLC-LWW
  (§5.3), not from read-only discipline.

---

## 4. Why naive bidirectional wiring breaks today

Wiring `A→B` and `B→A` (or a ring) with today's code does not "sort of work with
lag" — it fails destructively:

- **Postgres / MySQL — infinite replication loop.** A node in a bidirectional
  topology is both a source (has triggers) and a target (receives applied writes).
  Because no trigger distinguishes replicare's own apply writes, an applied write is
  **re-captured** and shipped back, forever. (PG has `session_replication_role` as a
  *potential* fix but it needs superuser, which the project forbids; MySQL has no
  equivalent at all.)
- **Redis — data loss + ping-pong.** A key freshly written to `B` and not yet on `A`
  is, to the `A→B` delete sweep, "present on target, missing at source" → it is
  `DEL`'d on `B` **before** `B→A` can propagate it. New writes are indistinguishable
  from orphans. Upserts also ping-pong via repeated `RESTORE … REPLACE`.
- **All three — no conflict resolution.** Concurrent writes to the same key/row on
  two nodes clobber each other non-deterministically (whichever apply pass runs
  last wins).

Because `Config.Validate()` does no cycle detection, a user can enter this
configuration today and get silent corruption. **The first concrete change this
note recommends (independent of full multi-master) is to make the config layer
detect and refuse an un-declared cycle** — see §6 and §8.

---

## 5. Proposed design — the mechanisms multi-master needs

Three capabilities are missing everywhere and must be built. They are engine-neutral
in concept, engine-specific in mechanism.

### 5.1 Node identity & replication origin

Every participating instance gets a **stable `node_id`** (configured, persisted with
the daemon's state so it survives restarts). Every replicated change is associated
with the `node_id` where it **originated**. This is the foundation for both loop
suppression (5.2) and conflict attribution (5.3).

### 5.2 Loop suppression (origin filtering)

Two sub-problems:

1. **Don't re-capture replicare's own apply writes.** When the daemon applies a
   change to a node, that node's capture must not treat it as a fresh local write.
2. **Don't forward a change back toward where it came from.** In a ring/partial mesh
   a change is relayed hop-to-hop and must stop after one lap. In a **full mesh** with
   direct edges, (1) alone prevents loops (every node receives each change directly
   and never re-captures it); origin tagging is still needed for (3) and for rings.

Per-engine mechanism for (1):

- **Postgres.** Add a `WHEN (current_setting('replicare.apply', true) IS NULL)`
  clause to the capture trigger. The apply connection does `SET LOCAL
  replicare.apply = '<origin_node_id>'` inside the apply transaction. Application
  writes (no GUC set) still capture; replicare's apply writes are skipped. This is a
  **custom GUC** (a namespaced `replicare.*` setting) — it needs **no superuser** and
  no privilege beyond what the daemon already has, unlike `session_replication_role`.
  (When origin forwarding is needed, the trigger instead records the GUC's value as
  the origin rather than skipping unconditionally — see below.)
- **MySQL.** MySQL has no `WHEN` and no `session_replication_role`. Use a **user
  session variable**: the apply connection issues `SET @replicare_apply =
  '<origin_node_id>'` at `BeginApply`, and each of the three trigger bodies wraps its
  delta-insert in `IF @replicare_apply IS NULL THEN … END IF`. User variables are
  strictly connection-scoped; the apply pool is already `MaxOpenConns=1` with a pinned
  connection, so the marker is isolated — but it must be reset on connection release,
  exactly as `FOREIGN_KEY_CHECKS` already is.
- **Redis.** Capture-less, so there is nothing to suppress at "capture" time — the
  problem moves to the reconciliation passes (5.4). Loop suppression requires **per-key
  origin+version metadata** that a reconcile pass consults to decide "I already have
  the newest version; don't re-RESTORE, don't delete." Redis has no room in the value
  (`DUMP`/`RESTORE` is verbatim, §1.7 forbids wrapping it), so this metadata lives in a
  **parallel metadata keyspace** (see 5.4).

**v1 is full-mesh only, so sub-problem (2) does not arise** — every node ships directly
to every other, so skip-suppression alone prevents loops, and the **version register
(§5.3) is the correctness backstop**: any change that is re-delivered but not newer is a
no-op, so a stray loop is self-limiting (this is also what makes ring/partial topologies
*safe* to add later). The source-side **origin column** and hop-to-hop forward-suppression
(skip deltas whose origin is the destination; preserve origin across hops) are only needed
to make **ring/partial meshes efficient** and are **deferred post-v1** — which keeps the
one-way delta table DDL literally unchanged in v1.

**Bootstrapping a new member** uses the same marker: the initial copy / reseed *into* a
member runs with `replicare.apply` / `@replicare_apply` set (so its cold-copy writes are
not self-captured into an echo storm) **and seeds the version register** from the source's
register, so copied rows carry their true `(hlc, node)` rather than a fresh local stamp.

### 5.3 Conflict resolution — zero-config, replicare-managed

replicare deliberately has **no commit-order signal** (the entire trigger-CDC premise;
`CLAUDE.md` §3.3), so it cannot do true causal ordering. And a hard product requirement:
**conflict resolution must need no user schema change and no user-specified column.**
The **primary key cannot be the tiebreaker** — it is the *conflict key* (two colliding
writes share it), so it says *which* rows conflict, never *which value wins*. Resolution
needs something **ordered**, and we will not ask the user to add it.

**So replicare maintains the version itself, invisibly:** for every replicated row/key it
keeps a hidden record **`PK → (hlc, origin_node)`** in its **own `replicare` schema** — a
side "version register" table (Postgres/MySQL) or the metadata keyspace (Redis), never a
column on the user's tables. `hlc` is a **hybrid logical clock** (`max(physical_now,
last_seen_hlc) + logical_tick`): monotonic, wall-clock-tracking, causality-respecting.
Every change is stamped `(hlc, node_id)`; the **`node_id` breaks equal-HLC ties**, so the
order is **total, with no ties**.

- **The one policy is HLC last-write-wins, zero-config.** On apply of an incoming change
  for `PK`, compare `(hlc_in, node_in)` against the register's stored `(hlc, node)`:
  apply the value **and** update the register iff strictly greater, else **no-op**; then
  advance the local HLC past `hlc_in`. This is a **LWW-register CRDT** (a join-semilattice
  max) → it **converges for any number of nodes regardless of message order**.
- **No priority-fencing, no user version column, no `custom` resolver in v1.** The policy
  interface stays open for a future custom resolver, but v1 ships exactly this default and
  there is nothing to configure beyond declaring cluster membership.

**Cost, stated honestly:** the register is ~one small row per replicated row (PK + ~16
bytes) — the size of an extra index. That is the price of touching none of the user's
schema (the rejected alternative was a version column on user tables).

Deletes are **tombstones carrying `(hlc, node_id)`** in the same register/keyspace, so a
delete competes with a concurrent update under the same total order. Tombstones **and**
version records for deleted keys are **GC'd** once every peer has observed a version ≥
them (a per-cluster min-observed watermark across peers' cursors), keeping the source
footprint bounded (§3.4).

### 5.4 Redis specifics (the hardest engine)

Redis needs the most new machinery because it is capture-less and value-opaque:

- **Metadata keyspace.** For each replicated data key `K`, maintain a sibling metadata
  entry (e.g. a hash under a reserved prefix or a separate logical DB) holding
  `{origin_node, version, deleted_at?}`. This preserves the value-faithful `DUMP`/
  `RESTORE` promise (the value key is never wrapped) while giving reconcile passes
  something to reason about. It is written **atomically with** the value via a Lua
  script / `MULTI` so the pair cannot diverge.
- **Delete-diff redesign.** The current "present on target, missing at source ⇒ DEL"
  logic must become "present on target, missing at source **and** the target's
  metadata shows no newer local write, **and** a real tombstone exists at the origin".
  Without this, a peer's new write is still destroyed. This is effectively **tombstone-
  based deletion** replacing the stateless diff — a fundamental change to
  `internal/pipeline/delete.go` + `MissingAtSource`.
- **Conflict resolution on opaque bytes.** LWW uses the metadata `version`; the value
  itself is never merged (Redis values are opaque to us). Sub-key merges (e.g.
  hash-field-level) are out of scope — that is what Redis Enterprise Active-Active
  (CRDB) does with CRDTs, a different architecture we are not rebuilding.

Honest assessment: full active-active Redis is a **near-rewrite** of the Redis CDC model
and carries the most risk of the three engines. It is nonetheless **in scope and
co-equal — not deferred** (a hard requirement): Redis gets the same `(hlc, node_id)`
version register as Postgres/MySQL, here realized as the metadata keyspace, and the same
HLC-LWW resolution. It sequences *after* the Postgres milestones only because it reuses
their neutral version-register/tombstone abstractions, not because it can be dropped.

### 5.5 Ownership, cursors, topology expansion

- A mesh expands to a set of **directed edges**; each edge keeps its own cursor
  namespace (extend the existing `(sync, target, table)` keying with the mesh/edge and
  origin). The per-sync advisory lock generalizes to a **per-edge** (or per-mesh-member)
  lock.
- HA/leader-election is still deferred, but multi-master raises the stakes (a
  split-brain daemon in a mesh is worse than in one-way), so the ownership interface
  must be ready for `pg_advisory_lock`-based leader election before mesh is declared
  production-ready.

---

## 6. Config-schema changes

**Design principle: additive and opt-in.** Existing configs (no new keys) must be
byte-for-byte valid and behave identically. Multi-master is expressed by **new, optional**
constructs; the existing `sources` / `targets` / `syncs` schema is untouched, and a config
with no `clusters:` block is exactly today's one-way daemon.

> **Naming note.** A replicare **`clusters:`** entry is a set of active-active *peer
> nodes* — do not confuse it with a Redis endpoint's `mode: cluster` (one *sharded*
> Redis). Different scopes; the docs disambiguate.

### 6.1 New: `node_id` on an endpoint (optional)

```yaml
sources:
  us:
    engine: postgres
    node_id: us-east          # NEW, optional; required only for cluster members
    postgres: { host: ..., ... }
```

Stable origin identity — the value stamped into every change's `(hlc, node_id)`. Optional
and ignored on the one-way path.

### 6.2 New: a `clusters:` block (optional, top-level)

A cluster names its member nodes (each an endpoint that is simultaneously a source and a
target), the topology, and the selection/tuning — mirroring a `sync` but multi-directional.
**There is no conflict policy to configure:** resolution is the zero-config,
replicare-managed HLC last-write-wins of §5.3 (no version column, no priority). The daemon
expands the cluster into the full set of directed edges internally.

```yaml
# Existing one-way syncs keep working, unchanged, alongside clusters.
syncs:
  - name: analytics-fanout
    source: us
    targets: [warehouse]
    include: ["public.*"]

clusters:                        # NEW, entirely optional
  - name: global-app
    engine: postgres             # single-engine, like a sync
    members: [us, eu, ap]        # endpoint names; each is source AND target; any N >= 2
    topology: mesh               # v1: full mesh (ring/partial deferred)
    include: ["public.*"]
    exclude: ["*_audit"]
    tuning: { drain_interval: 1s }
    # No conflict block: HLC-LWW is automatic (§5.3). Nothing to declare.
```

Notes:
- `members` reference endpoint definitions; each must carry a `node_id` and be reachable
  as both read (capture+snapshot) and write (apply). **Any N ≥ 2** — no 3-node limit.
- `topology: mesh` is the only v1 value (full mesh, N·(N−1) edges); `ring`/`explicit`
  (partial) are deferred with the origin-column forwarding work (§5.2).
- The engine registry still owns per-engine connection parsing; `clusters` adds only
  neutral wiring, consistent with the `sync` model.

### 6.3 New validation (and a safety fix for the one-way path)

`Config.Validate()` gains:
1. **Cluster validation** — single-engine members; every member has a `node_id`;
   `topology: mesh` only (v1); no member endpoint reused in a conflicting plain sync.
   (No conflict-policy validation — there is no policy to configure.)
2. **Cycle detection for plain `syncs`** — refuse an *un-declared* cycle among one-way
   syncs (`A→B` + `B→A`, or a ring) not part of a `clusters:` block. Closes the
   silent-corruption footgun in §4, worth doing **independently**. It only *adds* a
   rejection for a config already broken today, so no valid one-way config is affected.

### 6.4 State-store & source-schema changes

- **The one-way delta table is unchanged** — v1 adds **no column** to it (a full mesh
  needs no origin column; §5.2). The **version register** (`PK → (hlc, node_id,
  deleted?)`) and **tombstones** are **new, mesh-only tables** in the `replicare` schema,
  created only for cluster tables. Applied via the existing **in-place, idempotent
  migration runner** with a `schema_version` bump that **preserves in-flight
  deltas/cursors** (`CLAUDE.md` §14) — never drop-recreate.
- **Cursors** extend their key with cluster/edge for cluster members; the existing
  `(sync, target, table)` shape is unchanged for one-way.
- **Redis** adds the metadata keyspace (the register + tombstones, §5.4); nothing changes
  for one-way Redis.

---

## 7. Backward compatibility — the non-negotiable

Every change above is designed so the **one-way path is provably unchanged**. The
invariants:

1. **Config.** No `clusters:` and no `node_id:` ⇒ identical parse and identical
   behaviour. New keys are optional; strict parsing still rejects genuine typos. The
   only new *rejection* is an un-declared cycle among one-way syncs — a config that is
   already corrupt today, never a working one.
2. **Triggers (PG/MySQL).** The origin guard is a **no-op for one-way**:
   - A one-way source is never applied to by replicare, so the apply marker
     (`replicare.apply` GUC / `@replicare_apply`) is never set on it → local writes are
     captured exactly as today.
   - A one-way target has **no capture triggers** (targets aren't sources), so the
     guard is irrelevant there.
   - The guard defaults to "capture" when the marker is unset, so any non-replicare
     write is still captured. Origin-aware triggers are only *installed* for tables
     that participate in a cluster; one-way tables keep today's trigger DDL.
3. **Apply path.** The apply marker + HLC-LWW version comparison are only engaged for
   cluster edges. One-way apply remains blind source-wins overwrite + delete-to-match,
   unchanged, and writes no version register.
4. **Delta schema.** **v1 adds no column to the delta table** — the version register and
   tombstones are separate, mesh-only tables. One-way delta DDL is literally unchanged;
   migrations preserve in-flight state.
5. **Redis.** The metadata keyspace, tombstones, and delete-diff redesign are gated to
   cluster mode. One-way Redis keeps the stateless SCAN-reconcile + target-vs-source
   delete sweep verbatim.
6. **Tests.** The entire existing one-way integration suite (PG/MySQL/Redis, plus the
   `test/loadgen` and `test/loadgen-redis` convergence harnesses) must pass
   **unchanged**. Multi-master gets its own harness (an N-node cluster convergence +
   conflict test) rather than modifying the one-way tests.

Rollout is feature-flagged by the presence of a `clusters:` block: a daemon with none
compiles and runs exactly as before.

---

## 8. Phased plan

Ordered so each phase is independently shippable and low-risk-first. The detailed,
milestone-by-milestone version (with acceptance criteria + a per-milestone BC proof) is
[`.sisyphus/multi-master-plan.md`](../.sisyphus/multi-master-plan.md); this is the summary:

1. **Cycle-detection guardrail (small, do first).** Make `Config.Validate()` refuse an
   un-declared one-way cycle. Pure safety, no behaviour change for valid configs.
2. **Harden fan-out (small–medium; a mesh *prerequisite*).** Re-key initial-copy progress
   by target; add a 2+-target test. Each cluster node fans out to its N−1 peers, so this
   is on the critical path (and independently delivers single-writer fan-out).
3. **Postgres loop suppression + version register + bootstrap (medium).** `node_id`; the
   HLC + version register in the `replicare` schema; `replicare.apply` GUC + trigger
   `WHEN` guard; apply *and cluster-bootstrap copy* set the marker and seed the register.
   Prove no-loop + no-echo on a 2-node PG mesh, no superuser.
4. **Postgres HLC-LWW + tombstones + GC (medium).** Version-guarded upsert over
   `(hlc, node_id)`; tombstones; coordinated GC. N-node PG convergence + conflict harness.
5. **MySQL mesh (medium).** Mirror 3–4 with the `@replicare_apply` user-variable guard in
   all three trigger bodies; reset-on-release alongside `FOREIGN_KEY_CHECKS`.
6. **Redis mesh (large / highest-risk, but co-equal — NOT deferred).** Metadata keyspace
   as the version register + tombstones; tombstone/version-aware delete reconciliation;
   HLC-LWW via the metadata `(hlc, node)`. Sequenced after 4 (reuses its abstractions).
7. **Cluster lifecycle (retention/reseed).** Reseed runs marked, seeds the register, and
   never resurrects a tombstoned key.
8. **HA / leader election (before "production-ready").** Per-edge `pg_advisory_lock`
   leader election + cursor fencing, so a cluster can't split-brain.

---

## 9. Open questions / risks

- **Version-register cost.** ~1 row per replicated row (PK + `(hlc, node_id)`) — the
  accepted price of zero user-schema change (the rejected alternative was a version
  column on user tables). Index-sized but non-trivial for very large tables; measured as
  milestone acceptance.
- **HLC clock skew.** The `node_id` tie in `(hlc, node_id)` guarantees a *total* order,
  so nodes always converge to the *same* winner regardless of skew; skew only affects
  which of two truly-concurrent writes is deemed "latest" (a bounded wobble, never
  divergence). Document the assumed max skew and surface an HLC-skew metric.
- **Tombstone/register GC coordination.** A per-cluster min-observed-version watermark
  across peers (no global coordinator) — sub-design before the GC milestone ships. Live
  keys keep their register row; only tombstones + deleted-key records are collected.
- **Redis metadata atomicity & cost.** A metadata entry per key ~doubles key count and
  needs Lua/`MULTI` atomicity; big-key and cluster-slot interactions need care (the
  metadata key must hash-tag to its value key's slot). The single biggest risk item.
- **Least-privilege, everywhere.** The PG `replicare.apply` GUC and MySQL user-variable
  guards are grantable (no superuser); re-verified against managed offerings (RDS/Cloud
  SQL/etc.) as milestone acceptance, and the grants docs updated. **No superuser is
  required anywhere.**
- **Schema drift across members.** One-way assumes the target schema pre-exists; a
  cluster assumes *all* members share a compatible schema. Pre-flight must check every
  member pair, not just source→target.
- **Full-mesh edge scaling.** N·(N−1) directed edges — fine for small-to-moderate N;
  beyond ~tens of nodes a hub/ring (the deferred forwarding work) would be preferable.
- **Faithful transport (§1.7) is preserved** — none of this transforms values; the HLC
  version/origin are *metadata about* a change, never a mutation of the replicated value.

---

## 10. Summary

| Capability | PG today | MySQL today | Redis today | Needed for multi-master |
|---|---|---|---|---|
| Loop suppression | ✗ (no trigger guard) | ✗ (no guard, no `session_replication_role`) | ✗ (re-RESTORE loop) | `replicare.apply` GUC guard / `@replicare_apply` var guard + version-comparison backstop |
| Conflict resolution | ✗ (blind overwrite) | ✗ (blind overwrite) | ✗ (blind RESTORE) | **replicare-managed HLC-LWW over `(hlc, node_id)`** — zero user schema change; tombstones for deletes |
| Version register / origin | ✗ | ✗ | ✗ | `node_id` + a hidden `PK → (hlc, node)` register in the `replicare` schema / Redis metadata keyspace |
| Topology / cycle safety | ✗ (silent cycles) | ✗ | ✗ | `clusters:` block (full mesh, any N) + cycle-detection validation |
| Delete handling in a cluster | delete-to-match | delete-to-match | destructive diff | tombstone-based, `(hlc, node)`-aware, GC'd |
| One-way path | ✓ | ✓ | ✓ | **must remain unchanged (§7)** |

Multi-master is a substantial, multi-phase project across **all three engines (Redis
included, not deferred)** — heaviest on Redis. Conflict resolution is **zero-config**
(replicare-managed HLC-LWW; no user column, no schema change, `clusters:` just declares
membership). The one-way, source-authoritative path stays the default and is protected by
the §7 invariants throughout. If a single writer happens to suffice, hardening fan-out
(§3, phase 2) delivers "same data in N clouds" even sooner — but it is a *prerequisite* of
the mesh, not an alternative to it.
