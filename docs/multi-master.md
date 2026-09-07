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

---

## 1. Goal

Keep **N database instances (typically 3, across different clouds) converged**,
where writes may land on **any** node — i.e. **active-active / multi-master**, not
just one writer fanned out to read replicas. A ring (`A→B→C→A`) and a full mesh
(every node ↔ every node) are both in scope; the mesh is the harder, more useful
target.

This is distinct from what replicare can do **today** (see §3): one authoritative
source fanned out to one or more **read-only** targets.

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

## 3. What replicare can already do that may be "good enough"

If the real requirement is *"the same data in three clouds"* and you can accept a
**single writer**, replicare already fits without any of this work: designate one
**authoritative primary** and fan out to two **read-only** replicas (one source → N
targets). All writes go to the primary; the replicas stay converged.

Caveats to close first even for this path:
- Fan-out is *present, not hardened* and **untested end-to-end**; initial-copy
  progress must be re-keyed to include the target before it is trustworthy for 2+
  targets.
- Nothing enforces "read-only" on the replicas — application discipline (or engine
  ACLs / `default_transaction_read_only`) must guarantee no writes land there, or
  you are back in multi-master territory.

**Recommendation:** if single-writer is acceptable, harden fan-out (a small,
well-scoped project) instead of building multi-master. The rest of this note is for
the case where writes genuinely must be accepted on multiple nodes.

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

For sub-problem (2) (forwarding), the delta row gains an **origin column** populated
from the marker (NULL/local = "written here", else the upstream origin). A forwarding
edge skips deltas whose origin is the destination node and preserves the original
origin across hops.

### 5.3 Conflict resolution

replicare deliberately has **no commit-order signal** (the entire trigger-CDC premise;
`CLAUDE.md` §3.3). So it cannot do true causal ordering. Realistic policies, chosen
per mesh (and defaulting conservatively):

- **`source-priority`** — a static total order of nodes; on a tie the higher-priority
  node wins. Deterministic, needs no per-row version, crude but safe. Good default.
- **`lww` (last-write-wins)** — requires a **trusted, monotonic per-row version**
  (a user-designated column: an app-maintained `updated_at`/version, or a sequence).
  Apply compares it: `… ON CONFLICT (pk) DO UPDATE … WHERE excluded.<ver> >
  target.<ver>` (Postgres) / an equivalent guarded upsert (MySQL). **Wall-clock LWW
  across clouds is unsafe** (skew), so the version column must be app-guaranteed
  monotonic or a hybrid-logical-clock the app maintains — replicare documents the
  requirement and refuses `lww` for a table without a declared version column.
- **`custom`** — a pluggable resolver hook. Deferred; the interface should not be
  precluded.

Deletes complicate every policy: a delete on one node vs. an update on another needs
**tombstones carrying origin+version** so the resolver can compare a deletion against
an update. This is the hardest part and is called out as an open question (§9).

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

Honest assessment: full active-active Redis is a **near-rewrite** of the Redis CDC
model and carries the most risk. A staged option is to support Postgres/MySQL
multi-master first and keep Redis single-writer-fan-out until the metadata/tombstone
model is proven.

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

**Design principle: additive and opt-in.** Because parsing is strict and validation
is per-sync, existing configs (no new keys) must be byte-for-byte valid and behave
identically. Multi-master is expressed by **new, optional** constructs; the existing
`sources` / `targets` / `syncs` schema is untouched, and a config with no mesh is
exactly today's one-way daemon.

### 6.1 New: `node_id` on an endpoint (optional)

```yaml
sources:
  us:
    engine: postgres
    node_id: us-east          # NEW, optional; required only for mesh members
    postgres: { host: ..., ... }
```

Stable origin identity. Optional and ignored on the one-way path.

### 6.2 New: a `meshes:` block (optional, top-level)

A mesh names its member nodes (each of which is an endpoint that is simultaneously a
source and a target), the topology, the conflict policy, and the selection/tuning —
mirroring a `sync` but bidirectional. The daemon expands it into origin-aware directed
edges internally.

```yaml
# Existing one-way syncs keep working, unchanged, alongside meshes.
syncs:
  - name: analytics-fanout
    source: us
    targets: [warehouse]
    include: ["public.*"]

meshes:                          # NEW, entirely optional
  - name: global-app
    engine: postgres             # single-engine, like a sync
    members: [us, eu, ap]        # endpoint names; each is source AND target
    topology: mesh               # mesh | ring | explicit
    conflict:
      policy: lww                # source-priority | lww | custom
      version_column: updated_at # required for lww (per-table override allowed)
      # priority: [us, eu, ap]   # required for source-priority
    include: ["public.*"]
    exclude: ["*_audit"]
    tuning: { drain_interval: 1s }
```

Notes:
- `members` reference endpoint definitions; for a mesh each member must carry a
  `node_id` and be reachable as both read (capture+snapshot) and write (apply).
- `topology: explicit` would take an `edges: [[us, eu], [eu, ap], ...]` list for
  rings/partial meshes; `mesh` and `ring` are conveniences that expand automatically.
- The engine registry still owns per-engine connection parsing; `meshes` adds only
  neutral wiring + conflict policy, consistent with the `sync` model.

### 6.3 New validation (and a safety fix for the one-way path)

`Config.Validate()` gains:
1. **Mesh validation** — single-engine members; every member has a `node_id`;
   `lww` requires a `version_column`; `source-priority` requires a `priority` list
   covering all members; no member endpoint reused in a conflicting plain sync.
2. **Cycle detection for plain `syncs`** — refuse (or loudly warn on) an *un-declared*
   cycle among one-way syncs (`A→B` + `B→A`, or a ring) that is **not** part of a
   `meshes` block. This closes the silent-corruption footgun in §4 and is worth doing
   **independently** of the rest of this note. It only *adds* a rejection for a
   configuration that is already broken today, so it does not affect any valid one-way
   config.

### 6.4 State-store & source-schema changes

- **Delta tables** gain an optional `origin` column (PG/MySQL); one-way consumption
  ignores it (default/NULL = local). Applied via the existing **in-place, idempotent
  migration runner** with a `schema_version` bump that **preserves in-flight
  deltas/cursors** (`CLAUDE.md` §14 "schema versioning") — never drop-recreate.
- **Cursors** extend their key with mesh/edge + origin for mesh syncs; the existing
  `(sync, target, table)` shape is unchanged for one-way.
- **Redis** adds the metadata keyspace (5.4); nothing changes for one-way Redis.

---

## 7. Backward compatibility — the non-negotiable

Every change above is designed so the **one-way path is provably unchanged**. The
invariants:

1. **Config.** No `meshes:` and no `node_id:` ⇒ identical parse and identical
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
     that participate in a mesh; one-way tables keep today's trigger DDL.
3. **Apply path.** The apply marker / conflict-policy comparison is only engaged for
   mesh edges. One-way apply remains blind source-wins overwrite + delete-to-match,
   unchanged.
4. **Delta schema.** The new `origin` column is nullable and ignored by one-way
   consumption; the migration preserves in-flight state.
5. **Redis.** The metadata keyspace, tombstones, and delete-diff redesign are gated to
   mesh mode. One-way Redis keeps the stateless SCAN-reconcile + target-vs-source
   delete sweep verbatim.
6. **Tests.** The entire existing one-way integration suite (PG/MySQL/Redis, plus the
   `test/loadgen` and `test/loadgen-redis` convergence harnesses) must pass
   **unchanged**. Multi-master gets its own harness (a 3-node mesh convergence +
   conflict test) rather than modifying the one-way tests.

Rollout is feature-flagged by the presence of a `meshes:` block: a daemon with none
compiles and runs exactly as before.

---

## 8. Phased plan

Ordered so each phase is independently shippable and low-risk-first:

1. **Cycle-detection guardrail (small, do first).** Make `Config.Validate()` refuse an
   un-declared one-way cycle. Pure safety, no behaviour change for valid configs.
   Closes the §4 footgun immediately.
2. **Harden fan-out (small–medium).** Re-key initial-copy progress by target; add a
   2+-target end-to-end test. Delivers the single-writer-across-clouds story (§3)
   without any multi-master risk.
3. **Origin plumbing — Postgres (medium).** `node_id`; delta `origin` column +
   migration; `replicare.apply` GUC + trigger `WHEN` guard; apply sets the marker.
   Prove no-loop on a 2-node PG mesh with `source-priority`.
4. **Conflict resolution — Postgres (medium).** `lww` with a declared version column;
   guarded upsert; tombstones for delete/update conflicts. 3-node PG mesh convergence
   + conflict harness.
5. **MySQL mesh (medium).** Mirror 3–4 with the `@replicare_apply` user-variable guard
   in all three trigger bodies; reset-on-release alongside `FOREIGN_KEY_CHECKS`.
6. **Redis mesh (large / highest-risk).** Metadata keyspace, tombstone-based delete
   reconciliation, LWW via metadata version. Keep Redis single-writer until this is
   proven.
7. **HA / leader election (cross-cutting, before "production-ready").** `pg_advisory_lock`
   leader election + cursor fencing, so a mesh can't split-brain.

---

## 9. Open questions / risks

- **Trusted version for LWW.** Wall-clock is unsafe across clouds; requiring an
  app-maintained monotonic column shifts burden to the user. Is `source-priority`
  enough as the only v1 policy, with `lww` gated behind a documented version-column
  contract?
- **Delete/update conflicts** need tombstones with origin+version on all engines;
  tombstone GC (when is it safe to forget a delete?) is its own sub-design.
- **Redis metadata atomicity & cost.** A metadata entry per key doubles key count and
  needs Lua/`MULTI` atomicity; big-key and cluster-slot interactions need care (the
  metadata key must hash to the same slot as its value key — a hash-tag scheme like
  the load-gen harness uses).
- **Least-privilege.** The PG `replicare.apply` GUC and MySQL user-variable guards are
  grantable (no superuser) — this must be re-verified against real managed offerings
  (RDS/Cloud SQL) as part of phase 3/5, and the grants docs updated.
- **Schema drift across mesh members.** One-way assumes the target schema pre-exists;
  a mesh assumes *all* members share a compatible schema. Pre-flight must check every
  member pair, not just source→target.
- **Faithful transport (§1.7) is preserved** — none of this transforms values; origin/
  version are *metadata about* a change, never a mutation of the replicated value.

---

## 10. Summary

| Capability | PG today | MySQL today | Redis today | Needed for multi-master |
|---|---|---|---|---|
| Loop suppression | ✗ (no trigger guard) | ✗ (no guard, no `session_replication_role`) | ✗ (re-RESTORE loop) | `replicare.apply` GUC guard / `@replicare_apply` var guard / metadata-gated reconcile |
| Conflict resolution | ✗ (blind overwrite) | ✗ (blind overwrite) | ✗ (blind RESTORE) | `source-priority` (default) or `lww` w/ version column; tombstones for deletes |
| Origin identity | ✗ | ✗ | ✗ | `node_id` + delta `origin` column / Redis metadata keyspace |
| Topology / cycle safety | ✗ (silent cycles) | ✗ | ✗ | `meshes:` block + cycle-detection validation |
| Delete handling in mesh | delete-to-match | delete-to-match | destructive diff | tombstone-based, origin/version-aware |
| One-way path | ✓ | ✓ | ✓ | **must remain unchanged (§7)** |

Multi-master is a substantial, multi-phase project, not a config toggle — heaviest on
Redis. The one-way, source-authoritative path stays the default and is protected by
the §7 invariants throughout. If a single writer is acceptable, hardening fan-out (§3,
phase 2) delivers "same data in three clouds" far sooner and at a fraction of the risk.
