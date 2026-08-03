# replicare — Redis Engine Implementation Plan (v2)

**Status:** **APPROVED to execute** after one Sisyphus (pre-planning) + **two** Momus (design) passes —
the 2nd Momus pass returned **APPROVED-WITH-NITS** with every prior BLOCKER/MAJOR verified resolved
against the actual neutral code, and its four remaining specification tightenings (delete-sweep in-memory
state machine; bounded cursor-per-batch not whole-pass buffering; the `ApplyTx.DeleteAbsent`-with-empty-
staging delete mechanism; the `streamOnce` invocation site) are folded in below. Third engine after
Postgres (shipped) and MySQL (shipped). Section refs like §3.2 point at `CLAUDE.md`. Single-engine rule ⇒
**Redis → Redis only**.

> **What v2 changed vs the v1 draft (the load-bearing corrections):**
> 1. **Delete reconciliation is now a durable target-vs-source keyspace diff (baseline), not an
>    in-memory seen-set.** The v1 seen-set-as-correctness **inverted CLAUDE.md §3.3** (the durable
>    set-difference must be load-bearing; cheap/ephemeral signals are hints). Notifications + an optional
>    seen-set are now *accelerators only*. This needs **one small additive OPTIONAL neutral interface**
>    (the sweep has no home on `Source`-alone or `Sink`-alone) — so the v1 "**zero neutral-pipeline
>    edits**" claim is **retracted**: Redis is *near*-zero-edit on copy/apply, plus one additive
>    delete-reconciliation seam that PG/MySQL don't implement.
> 2. **Command-reconstruction and big-key incremental transport are DROPPED** — they violate §1.7
>    ("no value-transform capability exists, not even opt-in") and are non-idempotent / torn-read.
>    v1 is **DUMP/RESTORE-only**; the version-gate/module-missing/oversize cases **fail loud**, never
>    reconstruct. Redis therefore needs **no §1.7 exception** (it stays fully faithful).
> 3. **"byte-faithful" → "value-faithful".** `RESTORE` re-encodes on the target, so cross-version
>    `DUMP` bytes differ; the fidelity oracle is **type-appropriate logical comparison**, DUMP-byte
>    equality only for same-version round-trips.
> 4. **TTL default = RELATIVE** (`PTTL`→`RESTORE ttl`), not ABSTTL (daemon-clock skew could DEL live
>    keys). ABSTTL is an opt-in for trusted-clock topologies, computed from the server `TIME`.
> 5. **Copy = one atomic chunk per shard**; `DeleteRange`/keyset `Watermark`/`Lo` are **no-ops**
>    (Redis has no ordered key ranges / durable SCAN cursor). Intra-shard parallelism lives *inside*
>    `CopyChunk`.
> 6. **RM1a split** (conn/topology vs introspect/pre-flight); **RM4 split** (upsert-convergence vs
>    delete-reconciliation). **Sentinel demoted** to "config+client wired, not hardened". **Fan-out is a
>    v1 non-goal.** **CLAUDE.md updates are a deliverable.** **CI is compile/unit/PG-only; Redis is a
>    local gate.**

---

## Guiding constraints carried over from CLAUDE.md (unchanged, engine-neutral)

1. **No privileged change stream — ever.** No replication link, RDB/AOF access, or `PSYNC`. CDC uses
   grantable commands (`SCAN`, `DUMP`, `PTTL`, `RESTORE`, `DEL`, `EXISTS`); keyspace notifications are an
   *optional accelerator* (§3.2).
2. **Least privilege.** Redis 6+ ACLs; no `@admin`/`@all`/`DEBUG`/`CONFIG`/`KEYS`/`FLUSH*` for the
   baseline. **`RESTORE` is `@dangerous` and must be granted explicitly** (the one foot-gun).
3. **Highly performant + polite on a source we may not own.** Non-blocking `SCAN`, tuned `COUNT`, rate
   limits; big keys handled by DUMP-with-warning, never torn incremental reads.
4. **Deeply observable.** Reuse the F2 contract; `table` labels carry **key-namespace / shard**; add a
   Redis-specific **delete-reconciliation-lag** signal (Momus M7).
5. **Engine-agnostic core.** Redis is the third `Source`/`Sink`; it plugs into the neutral pipeline with
   a semantic overload **plus one additive optional delete-reconciliation seam**.
6. **Source may be very old.** `SCAN` (2.8+), `DUMP`/`RESTORE` (2.6+), `RESTORE REPLACE` (3.0+), `PTTL`
   (2.6+) conservative; `SCAN TYPE` (6.0+), ACL (6.0+), `RESTORE ABSTTL` (5.0+) are modern-only niceties.
7. **Faithful transport — never transform.** `DUMP`→`RESTORE REPLACE` moves the value as RDB-serialized
   bytes; replicare never interprets, coerces, or reconstructs. Incompatible payload (version gate,
   missing module, oversize) ⇒ **fail loud**. **Value-faithful** (value-preserving), realized by RDB
   serialization. **No reconstruction path exists — §1.7 holds with no exception** (contrast MySQL's
   §4.1 FK_CHECKS carve-out; Redis needs none).

---

## Client library & floor/fork decisions (RM0)

- **Driver `github.com/redis/go-redis/v9`** — pure Go (CGO-free static binary preserved), binary-safe
  `[]byte` (mandatory for DUMP payloads and arbitrary-byte key names), native standalone/Sentinel/Cluster
  with `MOVED`/`ASK` handling + topology refresh.
- **Version floor (best-effort):** source Redis **3.0** (`RESTORE REPLACE`); target **5.0**; recommended
  **6.2+** both ends. CI-tested pair: **6.2 source → 7.4 target** (exercises the old→new RDB direction) +
  a **3-node 7.4 cluster**.
- **Fork stance (RQ-1 resolved):** detect via `INFO server`; **Valkey allowed** (shares Redis RDB format
  + protocol); **KeyDB best-effort**; **Dragonfly BLOCKED in v1** (DUMP/RESTORE RDB-compat unverified) —
  loud, documented, revisitable. Non-Redis-family servers blocked like MariaDB was for MySQL.
- **Big-key + reconstruction forks resolved HERE (Sisyphus M3):** **no incremental transport, no
  command-reconstruction in v1** (§1.7). Big key above a configurable hard cap → **DUMP-and-warn**
  (accept transient source blocking) up to an absolute refuse-cap → **block loud**. This removes the
  undecided forks from RM4/RM11.

---

## 0. The crux: what Redis lacks that the relational path assumes

Redis has **no SQL, schema, tables, PK/FK, triggers, cross-key transactions, ordered key ranges, or text
wire format.** The neutral layer survives because it treats `engine.TableRef`, `engine.KeyValues`,
`engine.DeltaID` as **opaque handles**. Redis overloads them:

| Neutral type | Relational meaning | Redis reinterpretation |
|---|---|---|
| `TableRef{Schema,Name}` | one table | one **replication unit** = `{Schema:"redis", Name:"<encoded unit>"}` (see §0.1 for the unit definition) |
| `KeyValues []any` | PK tuple | `[]any{[]byte(redisKey)}` — one opaque, **arbitrary-byte** Redis key |
| `DeltaID int64` | delta-row id | a **per-unit monotonic change-id** (RQ-2 resolved: per-unit monotonic, no cross-unit ordering needed) |
| text `COPY` (`io.Reader`) | PG/MySQL wire text | a **private length-prefixed binary framing** of `{key, pttl, dump-payload}` |
| `Component` (FK component) | FK component | a **single-member acyclic component** per replication unit |

### 0.1 The replication unit, the component model, and the (near-)zero-edit apply path
- **DECISION (replication unit).** A **unit = one `(cluster-master-shard × selection-scope)`** in
  cluster mode, or **one `(db-index × selection-scope)`** in standalone (selection-scope = the whole
  selected keyspace of that node/db; include/exclude globs are applied *within* a unit, not by splitting
  units). `TableRef.Name` encodes the unit (`shard-id` or `db-index`) so it round-trips through
  `transportColumns`→`Introspect(Include:["redis.<unit>"])` (Momus M10). One unit ⇒ one single-member
  acyclic `engine.Component`. **Fan-out is a v1 non-goal** (Momus m4): a single source SCAN cursor can't
  serve two targets at different rates without per-target scan state — v1 is **single source → single
  target**; multi-target is deferred (the apply/cursor layer already doesn't preclude it).
- **DECISION (pseudo-schema sets a usable key — Momus m8).** `Introspect` synthesizes one pseudo-`Table`
  per unit with a **single synthetic PK column** so `HasUsableKey()` is true (else the neutral layer
  skips it with a loud warning, §3.1). No FK edges, no real columns.
- **DECISION (thin `ApplyTx`, upserts + capture-driven deletes only).** Redis exposes single-member
  acyclic components, so `stream.go`→`DrainComponentRetrying`→`DrainComponent`→`BeginApply`/`ApplyTx`
  flows unchanged. Redis's `ApplyTx`: `StageUpsert(ref, cols, reread)` reads the framed re-read and
  **pipelines `RESTORE … REPLACE`**; `DeleteAbsent(ref, dirtyKeys)` **`DEL`s the dirty keys absent from
  the staged set** — this handles deletes that the engine *already knows about* (notification/seen-set
  accelerator emits them as `Op=D` dirty keys). `Commit` flushes the pipeline. **No `MULTI`/`EXEC` group
  atomicity** — apply is per-key idempotent (a departure from §8.1 per-component-atomicity worth
  recording in CLAUDE.md, Momus M11). `cyclic`/`componentTables` ignored; the retry-fallback never fires
  (Redis raises no transient FK errors — stated as an invariant, Momus m1).
- **Verified against the code:** the `DrainComponent` flow (`coalesce`→`distinct`/`ids`→`StageUpsert`→
  `DeleteAbsent`→`Commit`→`ConfirmConsumed`) is single-key-clean; `keySignature` uses one column so the
  `\x00`-separator collision can't occur; arbitrary-byte keys must survive `KeyValues`→`RereadCurrent`→
  `DeleteAbsent` verbatim with **no text coercion** (RM5 acceptance, Momus m10).

### 0.2 No text COPY / no output functions → DUMP/RESTORE value-faithful transport + two pre-flight gates
- **DECISION (transport = `DUMP` → `RESTORE … REPLACE`).** RDB-serialized bytes; type + logical content
  preserved; `REPLACE` mandatory (idempotent overwrite). Payload stays `[]byte` end-to-end (no text
  coercion) or CRC64 fails loud. **Value-faithful, not byte-faithful:** `RESTORE` re-selects the target's
  optimal encoding, so a target `DUMP` need not byte-match the source (Momus M7). **No transform, no
  reconstruction — §1.7 holds with no exception.**
- **DECISION (RDB-version directional gate — Preflight BLOCK).** `RESTORE` rejects a payload whose RDB
  version **exceeds** the target's max. So **older/equal source → newer/equal target works; newer →
  older FAILS LOUD.** `Preflight` receives `srcVersion, tgtVersion` **ints** already (`redis_version`
  → int in `ServerVersion`), so the version gate runs **inside `Preflight` cleanly** (no conn needed);
  **BLOCK when target < source**, actionable. Coarse-but-safe: version-based, may over-block a rare
  compatible pair (documented, m6).
- **DECISION (module gate — runs at INTROSPECT time, not in the pure `Preflight`).** `Preflight` is a
  **pure function (no ctx/conn; `Schema` has no module field)** (Momus M6), so the module check runs
  during **connected introspection**: `Introspect` calls `MODULE LIST` on both ends (the Source/Sink have
  connections), records the per-unit **module types present in the selection** into a new
  **`Schema.Capabilities` field** (a recorded, bounded **neutral edit**), and `Preflight` then BLOCKs when
  a source module type is absent on the target. (Module gate is coarse: presence-based, may miss a
  module *version* mismatch — documented, m6.) *(Alternative: run the module gate as a connected step in
  the daemon's `buildSyncer` before `Preflight`; the `Schema.Capabilities` edit is preferred for symmetry
  with PG/MySQL.)*
- **DECISION (no reconstruction fallback — §1.7).** When DUMP/RESTORE is unusable (version gate, missing
  module, or a managed tier restricting `DUMP`/`RESTORE`, RQ-4), replicare **fails loud and refuses the
  sync** — it never reconstructs via `GET/HGETALL/…` (that is the forbidden value-transform, and it is
  non-idempotent besides). RQ-4 (managed-tier `DUMP`/`RESTORE` availability) is verified in RM0/RM2; if a
  target tier truly forbids `RESTORE`, that engine/target is simply unsupported in v1.

### 0.3 No trigger capture, can't write to the source → SCAN reconciliation is the CDC baseline
- **DECISION (upserts = full-keyspace SCAN reconciliation; nothing written to source).** No delta/track
  table, no `replicare` source schema. "Capture" = repeated `SCAN` passes re-reading current values.
  `InstallCapture`/`RemoveCapture` = optional setup/teardown of the keyspace-notification subscription
  (no-op when off/unprivileged).
- **DECISION (deletes = durable target-vs-source keyspace diff, the §3.3-aligned baseline).** This is the
  v2 correction (Momus M4). A deleted key stops appearing in source SCAN; the **robust, durable**
  detector is the set-difference **target-keyspace MINUS source-keyspace** (both live and durable), run
  on a configurable interval: enumerate target keys in the unit, check source `EXISTS`, `DEL` orphans.
  **This is the correctness mechanism.** It needs Source+Sink together, which no single-role interface
  provides, so v2 adds **one additive OPTIONAL neutral interface** (§0.4).
- **DECISION (keyspace notifications + optional seen-set = accelerators only).** Fire-and-forget,
  value-less, lost-on-disconnect, node-local in cluster, privilege/provider-gated (§0.5). They **shorten
  delete/upsert latency** between sweeps by emitting dirty keys (present→re-read+RESTORE; `del`/`expired`→
  `Op=D` through the normal `DeleteAbsent` path). **Correctness never depends on them:** a missed event
  is caught by the next sweep/reconciliation. On subscription gap → force a reconciliation + a
  delete-sweep for that shard. The **in-memory seen-set is now an optional low-latency accelerator**, not
  a correctness mechanism, and can be dropped freely under memory pressure (so **no reseed-from-memory
  trigger, no new StateStore field** — Momus M9 resolved: the durable sweep needs nothing persisted,
  carrying only bounded in-memory cursor/phase state, §0.4).
- **N/A (huge simplification vs relational):** no source capture tables ⇒ **no source bloat we create,
  no autovacuum, no delta purge, no `xmin`/history-list trap, no retention-cap math.** `Purge` is a
  no-op (or trims a notification change-log if one is used). "Reseed" = force a full reconciliation +
  full delete-sweep.

### 0.4 The one additive neutral interface (delete reconciliation) + coarse checkpointing
- **DECISION (additive optional `DeleteReconciler`, a recorded neutral edit).** Deletes-by-diff need
  target-scan (Sink) + source-exists (Source) together; the pipeline (Syncer) has both. v2 adds a small
  **optional** capability the pipeline invokes on an interval **only for engines that implement it**:
  ```go
  // engine.KeyLister (Sink) + engine.KeyExister (Source): optional capabilities for engines whose
  // deletes are NOT captured by a source-side change log (Redis). Capture-driven engines (PG/MySQL)
  // do NOT implement them; their deletes flow through the delta path unchanged.
  type KeyLister interface { // implemented by the Redis Sink
      ScanTargetKeys(ctx, t TableRef, cursor uint64, count int) (keys []KeyValues, next uint64, err error)
  }
  type KeyExister interface { // implemented by the Redis Source
      MissingAtSource(ctx, t TableRef, keys []KeyValues) (missing []KeyValues, err error)
  }
  ```
  `pipeline` gains a **delete-reconciliation step** (guarded by a type-assert; a no-op when the engine
  doesn't implement it) that, per unit per interval: `ScanTargetKeys` a batch → `MissingAtSource` →
  `sink` `DeleteAbsent(missing)` (reusing the existing delete path). **PG/MySQL unaffected** (they don't
  implement the interfaces; the step is skipped). This is the honest, bounded price of Redis's
  capture-less deletes — the v1 "zero edits" claim is retracted to **"near-zero: reuse copy/apply/state/
  config/observability unchanged, plus one additive optional delete-reconciliation seam."**
- **DECISION (copy→stream cutover seeds the delete baseline — Momus M3).** A key deleted/expired *during
  the copy window* is orphaned by a seen-set (never seen present→absent), so the **first post-cutover
  delete-sweep runs a full target-vs-source diff** for the unit, closing the orphan window. Restart
  likewise runs a full sweep first (crash-safe, idempotent).
- **DECISION (coarse phase checkpointing — no durable SCAN cursor).** A `SCAN` cursor is an opaque
  reverse-binary index, **not durably resumable** across rehashes/restarts. The reused StateStore records
  **coarse phase** ("snapshot complete for unit X", streaming) only; **`CopyProgress.Watermark` stays
  nil** and the copy layer's `DeleteRange`/`Lo`/keyset-watermark are **no-ops for Redis** (§0.5, Momus
  M5). Crash mid-snapshot ⇒ restart re-`SCAN`s the unit from 0 (idempotent).
- **DECISION (the delete-sweep is DURABLY-stateless but carries a bounded IN-MEMORY pass-boundary state
  machine — Momus 2nd-pass A).** "Stateless" means **no StateStore field** (a full target-vs-source
  re-diff each cycle needs nothing durable; a crash just restarts the cycle from cursor 0 — Momus M9). It
  does **not** mean state-free at runtime: a full target `SCAN` of a large unit **spans many ticks**, so
  the `Syncer` holds, **per unit, in memory**: (a) the live target-`SCAN` cursor carried across ticks,
  (b) a **mid-sweep / new-cycle phase flag**, and (c) a **`delete_sweep_interval` cadence timer** so the
  sweep fires on its own interval, not every `DrainInterval` tick. This is a second pass-boundary state
  machine, structurally like RM5's — specified as such. It is **bounded**: one cursor + one bounded batch
  per tick, **never the whole keyset** (see the keyset-memory decision in RM5/decision-log).
- **DECISION (checkpoint/consume semantics).** `ConfirmConsumed`/`DeltaID` are largely vestigial:
  "consumed" is a lag/phase hint; crash-safety means "reprocessed on the next full pass," not the next
  tick (Momus m1) — stated explicitly because `DrainComponentRetrying`'s retry re-calls `ReadDirtyKeys`,
  which *advances* the SCAN cursor (harmless only because Redis raises no transient errors, so the retry
  loop never actually fires — invariant verified against `retry.go`, Momus 2nd-pass E).

### 0.5 Cluster, topology, selection, big-keys, TTL, ACL specifics Redis breaks
- **DECISION (Cluster = per-master-node everything).** `SCAN`, notification subscription, and the
  delete-sweep all run **per master node**; topology via `CLUSTER SHARDS` (7+) / `CLUSTER SLOTS` (older),
  refreshed on `MOVED`/`ASK`. Parallelism unit = **shard**. Per-key `DUMP`/`RESTORE`/`DEL`/`PTTL`/`EXISTS`
  sidestep cross-slot limits. **Read-from-replica** value reads are a politeness toggle, **but delete
  detection (`EXISTS`, `MissingAtSource`) MUST read the MASTER** — a lagging replica would report a
  just-written key absent → false `DEL` → churn (Momus M5 / Sisyphus M5). Enforced + tested.
- **DECISION (selection = key-pattern globs + DB index).** Standalone: DB 0–15. **Cluster: DB 0 only**
  (validated). Include/exclude globs applied **client-side** (`SCAN … MATCH` filters post-scan, so cost =
  total keyset either way; one full SCAN per pass + client-side match, exclude-wins); `SCAN … TYPE`
  (6.0+) prunes server-side when type-scoped. **Redis key-glob semantics** (`:` `.` `*` `{}` hash-tags)
  are unit-tested against the reused matcher (Sisyphus L1).
- **DECISION (big keys = DUMP-and-warn, NO incremental — §1.7).** Above a configurable **warn threshold**
  (`MEMORY USAGE`), replicare **still `DUMP`s** (value-faithful) and **logs a loud big-key warning**
  (transient source blocking accepted); above an absolute **refuse cap**, **block loud**. **No
  `*SCAN`/`GETRANGE` incremental transport** (torn reads over a mutating key are unfaithful — Momus B2).
  This applies to **both initial copy and streaming `RereadCurrent`** (Momus m3). Conservative source
  pressure by default (tuned `SCAN COUNT`, rate limits, no blocking commands).
- **DECISION (TTL = RELATIVE by default; ABSTTL opt-in — Momus M8).** `DUMP` omits TTL; capture with
  `PTTL`. **Default: relative `RESTORE key <pttl> …`** — the target evaluates expiry from *its own* now,
  immune to daemon/target clock skew. Map `PTTL -1`→ttl `0` (no expire); `PTTL -2`/nil-`DUMP`→
  **delete-reconcile**. **`ABSTTL` is opt-in** for trusted-clock topologies only, computed from the
  server `TIME`, not the daemon clock. **The past-ABSTTL→DEL guard is removed** in relative mode (it was
  the skew data-loss hazard); in ABSTTL mode it applies only against server-derived time.
- **DECISION (least-privilege ACLs, no admin).** Source reader: `+scan +dump +pttl +type +object
  +exists +memory|usage +cluster +info` on `~*` (or prefix-narrowed). Target writer: `+restore +del
  +unlink +pttl +pexpire +pexpireat +type +exists +scan +object +info +cluster`. **`RESTORE` is
  `@dangerous` — excluded from `+@all -@dangerous` / managed "read-write" presets — so `+restore` MUST be
  granted explicitly** (documented loudly, CLAUDE.md §12 update). Notification subscribe:
  `+subscribe +psubscribe +&__key*@*__:*`; enabling notifications needs `@admin` (operator, out-of-band).
  No `KEYS`/`FLUSH*`/`DEBUG`/`SHUTDOWN` baseline.

---

## Goals & non-goals

**Goals (v1 Redis):** Redis→Redis replication — value-faithful `DUMP`/`RESTORE` transport with the
RDB-version + module pre-flight gates; SCAN reconciliation (upserts) + durable target-vs-source diff
(deletes) + optional notification acceleration; **standalone and Cluster** topologies with per-master
fan-out; TTL fidelity (relative default); least-privilege ACLs; reuse of the neutral copy/apply/state/
config/observability/pipeline layers **plus one additive delete-reconciliation seam**; the F2 contract +
a delete-lag signal; the real daemon (tri-engine coexistence).

**Non-goals (v1):** cross-engine (never); **command-reconstruction / big-key incremental transport**
(refused, §1.7); **multi-target fan-out for Redis** (single→single in v1); **Sentinel-hardened
failover** (config+client wired, but not a hardened/tested v1 goal — RM1 note); point-in-time
consistency (eventual convergence); cross-key/group atomicity; active-active/multi-master; replicating
non-keyspace state (ACLs, configs, functions, pub/sub); RDB/AOF/`PSYNC` capture; module-value transform;
Dragonfly (blocked, RQ-1).

## Definition of Done (Redis engine)
A Redis sync **initial-copies** a selected keyspace (all six native types + TTL, standalone **and**
cluster) **value-faithfully**, then **streams to convergence** under continuous writes/deletes/expiries
via reconciliation + durable delete-diff + optional notifications; **pre-flight blocks** newer→older and
missing-module loudly; runs under the real daemon **alongside** Postgres and MySQL syncs; emits the F2
contract + delete-lag; is documented + demoable; passes a **local hardening gate** (CI is compile/unit/
PG-only — Momus M1); and the Redis decisions are recorded in **CLAUDE.md**.

## Cross-cutting foundations (Redis adaptations of F1–F4)
- **F1 (config):** new `redis:` block (RM0.5) via `config.RegisterEngine`.
- **F2 (observability):** reused; `table`→unit/shard; `delta_backlog`→reconciliation backlog;
  `rows_copied`→keys copied; **+ a new delete-reconciliation-lag signal** (Momus M7).
- **F3 (harness/CI):** Redis harness — standalone **6.2→7.4** + a **3-node cluster** (with the
  bootstrap/announce-ip/`cluster_state:ok` specifics of RM0); `test:integration:redis`;
  **local-only, gated `REPLICARE_INTEGRATION=1 REPLICARE_REDIS=1`** (Sisyphus M1). CI stays PG-only.
- **F4 (source-schema versioning):** **N/A** (nothing written to source). All durable state = reused
  Postgres StateStore (RM3).

## Testing strategy
Unit tests for pure logic (glob→match incl. Redis key semantics, RDB-version mapping, framing serde,
ACL preset shape, pass-boundary batch slicing). **Integration** against the old→new standalone pair
**and** the cluster harness, gated on `REPLICARE_INTEGRATION=1 REPLICARE_REDIS=1` (skips in PG-only CI —
verified locally each milestone, the MySQL precedent — **so "green CI" for Redis means compile/unit/PG-
regression only; correctness is the local gate**). Determinism (Sisyphus M6): drive convergence with
bounded `StreamOnce`/sweep passes at a quiescent point, assert target==source by **logical** compare;
notification "latency" asserts **ordering** (flagged key drained before the next scan boundary), not
wall-clock; resharding uses a **deterministic hook** (RM11).

---

## Milestones (RM0–RM11)

### RM0 — Foundations: skeleton, driver, harness (standalone + **real cluster**), floor/fork
**Goal:** Registered-but-stubbed engine; the concrete harness incl. a *working* cluster; floor/fork/big-key
decisions.
**Deliverables:** `internal/engine/redis/` package; `go-redis/v9` dep (CGO-free static verified);
`engine.Register` + `cmd/replicare/engines.go` import; **standalone harness** (6.2 src + 7.4 tgt) **and a
3-node cluster harness** with the Cluster-in-docker specifics (Sisyphus H6): fixed per-node **host ports**
+ `cluster-announce-ip=127.0.0.1`/`-port`, a **one-shot `redis-cli --cluster create` bootstrap service**,
and a **`CLUSTER INFO`→`cluster_state:ok` healthcheck** (not `PING`); `Taskfile` `harness:redis:*` +
`test:integration:redis`; `docs/redis-version-support.md` (floor + fork stance RQ-1); resolve RQ-4/RQ-5
(no reconstruction/incremental).
**Acceptance:** package builds; `CGO_ENABLED=0` static binary links go-redis; standalone + cluster come up
healthy; **go-redis connects from the host and a `MOVED` redirect resolves to a reachable node**;
`ServerVersion` + fork detection work on both.
**Depends on:** none.

### RM0.5 — Redis config block (F1)
**Goal:** Lock the Redis connection/selection/tuning surface (no dead knobs).
**Deliverables:** typed `redis:` block via `config.RegisterEngine`: **connection** (`mode:
standalone|cluster` [`sentinel` parsed but marked experimental], `nodes`, `db` (standalone; forced 0 in
cluster), TLS spectrum, ACL `user`/`password`, `read_from_replica` [value reads only]); **selection**
(`include`/`exclude` key globs — reuse neutral `Sync.Include/Exclude` — `db`, optional `types`);
**tuning** (`scan_count`, `reconcile_interval`, `delete_sweep_interval`, `big_key_warn_bytes`,
`big_key_refuse_bytes`, `notifications: on|off`, `ttl_mode: relative|absttl`, rate caps). **No big-key
incremental / reconstruction knobs** (dropped). Reconcile `scan_count` (COUNT hint) vs the neutral
`DrainBatch` (max-returned) (Momus m2).
**Acceptance:** golden parse+validate (standalone + cluster); env-override precedence; invalid combos
rejected (non-zero `db` in cluster; unknown keys); single-engine rule holds.
**Depends on:** RM0.

### RM1 — Connectivity & topology (standalone + cluster; Sentinel wired-not-hardened)
**Goal:** Secure sessions across topologies; live cluster topology. *(Split from the v1 RM1a per Sisyphus
H4; introspection/pre-flight moved to RM2.)*
**Deliverables:** go-redis lifecycle for **standalone** + **cluster** (+ **Sentinel parsed/constructed
but explicitly not-hardened**, Sisyphus H7 — no failover test in v1, listed as a non-goal); **cluster
shard-map discovery** (`CLUSTER SHARDS`/`SLOTS`) + `MOVED`/`ASK` refresh; `ServerVersion` via `INFO
server` + fork detection; TLS/ACL auth; **read-from-replica for value reads only** (delete detection
pinned to master, §0.5).
**Acceptance:** connects standalone + cluster; discovers all masters; a `MOVED` redirect resolves;
value read-from-replica works while `EXISTS`/delete-detection hits the master; Sentinel constructs but is
documented experimental.
**Depends on:** RM0.5.

### RM2 — Introspection, capability/module gate, pseudo-schema, selection, pre-flight
**Goal:** A Redis pair → validated report with the §1.7 blocks; near-vacuous otherwise.
**Deliverables:** **`Introspect(sel)`** synthesizing one pseudo-`Table` **per unit** (single synthetic
PK, Momus m8; unit = shard×scope / db×scope, Momus M10) with `MODULE LIST` results recorded into the new
**`Schema.Capabilities`** field (neutral edit, §0.2); selection glob→key matching
(`redis/selection.go`, Redis key-glob semantics, Sisyphus L1); **pre-flight** = **RDB-version directional
BLOCK** (in the pure `Preflight`, version ints) + **module-missing BLOCK** (from `Schema.Capabilities`) +
single-member components (no FK graph/giant-component/type-coercion).
**Acceptance:** `validate` blocks newer→older loudly; blocks a source module absent on target; passes
equal/older→newer native pair; reports units as single-member components; cluster validates per-shard;
arbitrary-byte key names survive introspection round-trip.
**Depends on:** RM1.

### RM3 — StateStore integration (REUSED — Postgres, no new code)
**Goal:** Confirm a Redis sync runs on the neutral Postgres StateStore — testing + docs only.
**Deliverables:** **no new StateStore code** (Momus M9 resolved: the delete-sweep persists nothing —
bounded in-memory cursor/phase only — and the seen-set is a droppable accelerator ⇒ no new field). Docs
of overloads: `TableRef`→unit;
**coarse phase checkpointing** (snapshot-complete-per-unit, `Watermark` nil); `DeltaID`→per-unit change-id
(RQ-2). Ownership advisory lock reused.
**Acceptance:** a Redis sync def round-trips; per-unit copy-progress + cursor rows persist/reload;
ownership lock excludes a second daemon; needs-reseed flag round-trips.
**Depends on:** RM0.5. *(Parallel with RM1/RM2.)*

### RM4 — Initial copy: SCAN snapshot → DUMP/PTTL → RESTORE REPLACE (value-faithful, per-shard)
**Goal:** Restart-idempotent, **value-faithful** initial copy, standalone + cluster.
**Deliverables:** **one atomic chunk per unit** (`PlanChunks` returns a single opaque chunk/unit;
`Watermark` nil; **`DeleteRange` a no-op**; Momus M5/Sisyphus H1); **private length-prefixed binary
framing** (`CopyChunk` emits `{key, pttl, dump}` to `io.Writer`; `BulkLoad` pipelines `RESTORE REPLACE`
with **relative TTL**); **intra-unit parallelism inside `CopyChunk`** (N SCAN goroutines per shard →
one writer — since the pool parallelizes across units, not within, Sisyphus H1); six native types + TTL;
big-key DUMP-and-warn / refuse-cap (§0.5); nil-DUMP/expired-mid-scan → skip; cluster **per-master
parallel**.
**Acceptance (6.2→7.4 + cluster):** all six types (binary values, TTLs, high-precision ZSET scores, a
STREAM **with groups/PEL** — a *proven* corpus test, Momus m9) copy **value-faithfully** verified by
**type-appropriate logical compare** (NOT DUMP-byte equality across the version gap — Momus M7); a
**same-version** round-trip additionally asserts DUMP-byte identity (RQ-6 binary-safety); kill mid-copy →
restart converges; already-expired key skipped.
**Depends on:** RM2 (gates), RM3 (progress).

### RM5 — Streaming upsert convergence (reconciliation SCAN → thin ApplyTx) — TRUE FIRST STREAMING
**Goal:** Continuous **write/overwrite** convergence via the single-member-component path (the honest,
deterministic half; deletes are RM6). *(Split from v1 RM4 per Sisyphus H5.)*
**Deliverables:** **`ReadDirtyKeys`** drives the rolling reconciliation SCAN (present keys as `Op=I/U`)
via a **bounded pass-boundary state machine (Momus 2nd-pass B): hold the live source-`SCAN` cursor across
ticks and return ONE bounded batch (≤ `max`) per `ReadDirtyKeys` call — NEVER buffer the whole pass**
(that would be gigabytes for a large keyspace and would contradict RM11's "backpressure without unbounded
memory"). Per-unit in-memory state = the SCAN cursor + a pass-phase flag; memory is O(batch), not
O(keyset). **`RereadCurrent`** DUMPs dirty keys into the framing (big-key handling here
too, Momus m3); the **thin `ApplyTx`** (RESTORE REPLACE); **`ConfirmConsumed`** advances the per-unit
change-id/phase; **`DeltaBacklog`** reports reconciliation backlog/age. Runs on `stream.go` unchanged.
**Acceptance (6.2→7.4 + cluster) — keystone:** post-copy `SET`/overwrite/`EXPIRE`/type-change on the
source **converge** (logical compare, driven by bounded `StreamOnce` — Sisyphus M6); a rapid rewrite
converges to the last value; convergence holds across a cluster; crash before `ConfirmConsumed` →
reprocessed next full pass (idempotent, Momus m1); arbitrary-byte keys round-trip verbatim (Momus m10).
**Depends on:** RM4.

### RM6 — Delete reconciliation: durable target-vs-source diff (baseline) + accelerators
**Goal:** Correct, durable delete propagation — the §3.3-aligned set-difference. *(v2 first-class
milestone; the riskiest delete machinery, separated from RM5 per Sisyphus H5 / Momus M4.)*
**Deliverables:** the **additive optional `KeyLister`/`KeyExister` interfaces** + the **neutral pipeline
delete-reconciliation step** (§0.4, guarded no-op for PG/MySQL). **Invocation site (Momus 2nd-pass D):**
in `streamOnce`, in the **healthy-pass section AFTER the drain** (you cannot `DEL` on a down target
anyway), **bounded per tick** and fired on the `delete_sweep_interval` cadence. **Delete mechanism
(Momus 2nd-pass C — `DeleteAbsent` is on `ApplyTx`, not `Sink`):** the step opens an `ApplyTx` and calls
`DeleteAbsent(missing)` with **empty staging** — nothing staged ⇒ every passed key is "absent" ⇒ all
`DEL`ed, exactly the intent — then `Commit`. Redis Sink `ScanTargetKeys` (carries the in-memory target
cursor across ticks, §0.4) + Source `MissingAtSource` (**master-pinned**, Momus M5); the full
target-vs-source diff **subsumes** the copy→stream cutover orphan window and restart case (every sweep is
a full diff — no special-casing, Momus 2nd-pass M3); **optional seen-set + notification `del`/`expired`
accelerators** feeding `Op=D` through the same `DeleteAbsent` path (droppable under memory pressure — no
reseed trigger); the **delete-reconciliation-lag** metric (Momus M7).
**Acceptance (6.2→7.4 + cluster):** a key deleted at source converges (target `DEL`) within one sweep
interval (driven, not timed — Sisyphus M6); a key **deleted during the copy window** converges via the
cutover sweep (Momus M3); a **replica-lag false-delete** is prevented (delete detection master-pinned,
Momus M5); dropping the seen-set accelerator still converges via the sweep; expiry-driven deletes
converge; cluster deletes reconciled per master.
**Depends on:** RM5.

### RM7 — Keyspace-notification accelerator (optional, lossy, never trusted)
**Goal:** Cut reconciliation/delete latency with notifications, never depending on them.
**Deliverables:** **per-master `PSUBSCRIBE`** to `__keyevent@<db>__:*` (node-local in cluster);
notification-flagged keys → priority dirty-set drained ahead of the rolling scan (upserts) and via the
delete path (`del`/`expired`); `InstallCapture` = subscribe/enable (privilege-gated no-op when off);
**on any subscription gap/reconnect → force a reconciliation + delete-sweep** for that shard;
ElastiCache-safe flag subset; value-less events → always re-read.
**Acceptance:** with notifications on, a flagged key is **drained before the next full-scan boundary**
(ordering assertion, not wall-clock — Sisyphus M6); a simulated subscriber disconnect dropping N events
still converges via the next sweep (no lost update/delete); notifications-off falls back cleanly; cluster
events subscribed per master.
**Depends on:** RM6.

### RM8 — Observability verification (REUSED F2 + delete-lag)
**Goal:** Verify the F2 contract + the new delete-lag signal for Redis.
**Deliverables:** confirm emitted metrics/spans/logs with `table`→unit/shard; `delta_backlog`→
reconciliation backlog, `rows_copied`→keys copied, `replication_lag`→reconciliation age; the **new
delete-reconciliation-lag** gauge; status API + CLI `status` report Redis syncs; the target-unreachable
trifecta fires for a downed Redis target.
**Acceptance:** `/metrics` scrape asserts the named series (incl. delete-lag) with expected labels for a
live Redis sync; downed-target trifecta (span-error + escalating-log + gauge/backlog); status API reports
Redis phase/lag/progress.
**Depends on:** RM6.

### RM9 — Daemon, CLI & multi-sync + **tri-engine** coexistence
**Goal:** Redis under the real daemon; three engines coexist.
**Deliverables:** neutral daemon manages Redis syncs (standalone + cluster, shard pools, graceful
SIGTERM); CLI `run`/`validate`/`status`/`reseed` on Redis configs; a **`test:integration:tri` task**
(concrete port map — PG 5440/1, MySQL 3340/1, **Redis 6390 + a standalone target**, Sisyphus M4: use
**standalone** Redis for coexistence, not cluster) bringing up a Redis + Postgres + MySQL mixed
multi-sync in one daemon.
**Acceptance:** ≥2 concurrent Redis syncs; Redis + Postgres + MySQL coexist on the tri-harness; SIGTERM
drains/checkpoints cleanly; ungraceful kill resumes; CLI exercises the Redis lifecycle.
**Depends on:** RM8, RM3.

### RM10 — Packaging, docs & **CLAUDE.md updates** (Redis, feature-complete)
**Goal:** Redis installable, documented, and the durable decisions recorded.
**Deliverables:** static binary links go-redis; **ACL presets** (`deploy/acl-source-redis.txt`,
`deploy/acl-target-redis.txt`, **explicit `+restore` foot-gun** front-and-center, cluster/notification
notes); example config (standalone + cluster); Redis sections in getting-started/configuration/
operations/troubleshooting (RDB-version gate, module gate, **delete-propagation latency**, notification
lossiness, cluster per-node fan-out, big-key DUMP-warn, **the "Redis→Redis still needs a Postgres state
store" wart**, Momus m7); a **Redis demo** (compose: 6.2 src + 7.4 tgt + replicare + a PG state sidecar;
cluster variant); README status (PG + MySQL + Redis shipped); **CLAUDE.md updates (Momus M11):** §3.2
(reconciliation/delete-diff/notification-accelerator specifics), §9 (Redis needs PG state store; coarse
checkpointing), §12 (Redis ACLs incl. `+restore`), and decision-log rows (vocabulary overload, value-
faithful DUMP/RESTORE, **no cross-key atomicity**, additive delete-reconciler, relative-TTL default,
fan-out/Sentinel/Dragonfly non-goals). §1.7 is **unchanged** (no exception needed).
**Acceptance:** binary runs a Redis sync via the systemd sample; the demo replicates end-to-end
(standalone + cluster); quickstart works; CLAUDE.md diffs land in the same change.
**Depends on:** RM9.

### RM11 — Hardening & E2E gate (Redis shippable gate — **local**, Momus M1)
**Goal:** Confidence under faults, scale, topology + version spread. **Passing RM11 = Redis shippable.**
**Deliverables:** full standalone + cluster run; **fault injection** — **deterministic resharding hook**
(test-only per-SCAN-batch delay + scripted single-slot migration of a known key + assert `MOVED`/`ASK`
observed and the key converges — Sisyphus H8, bounded retries, not a hard gate), notification-subscription
gap, big-key DUMP-warn/refuse, key-expiry races, **newer→older RDB-version loud-fail**, module-missing
loud-fail, target down, slow-but-reachable backpressure; **expanded value-fidelity corpus** (six types
incl. large collections + stream groups/PEL, deep binary, TTL edges, empty vs missing); coarse
throughput/politeness numbers (SCAN COUNT, source latency-impact); **delete-propagation latency
characterized** under the sweep interval.
**Acceptance:** standalone + cluster green (local); fidelity corpus value-identical; convergence under
churn + deterministic resharding; delete-diff bounds drift; notification-gap backstop proven; loud-fail
on version/module incompat; perf recorded; slow-target backpressure without unbounded memory.
**Depends on:** RM9 (parallel with RM10).

---

## Critical path
`RM0 → RM0.5 → RM1 → RM2 → RM4 → RM5 → RM6 → RM7 → RM8 → RM9 → {RM10, RM11}`
with **RM3 parallel to RM1/RM2** (StateStore reuse-only). De-risking novelties: **RM4** (value-faithful
binary transport + cluster), **RM5** (reconciliation upserts + pass-boundary machine), **RM6** (durable
delete-diff + the additive interface). The relational surface (FK components, cyclic, trigger CDC, delta
bloat/purge, type-coercion) is **absent**; the reinvested depth is the three above.

## Decision log (quick reference)
| Topic | Decision |
|---|---|
| Engine fit | Overload neutral vocabulary; **single-member acyclic components + thin `ApplyTx`**; **near-zero neutral edits** = reuse copy/apply/state/config/observability + **one additive optional `KeyLister`/`KeyExister` delete-reconciliation seam** + a `Schema.Capabilities` field. |
| Transport | **`DUMP`→`RESTORE REPLACE`**, **value-faithful** (not byte), `[]byte` end-to-end. **No reconstruction, no incremental — §1.7 holds, no exception.** |
| Fidelity oracle | **type-appropriate logical compare** across versions; DUMP-byte equality only same-version. |
| Pre-flight | **BLOCK newer→older (RDB-version, in pure `Preflight`)**; **BLOCK missing-module (via `Schema.Capabilities` at introspect)**; no type-coercion axis. |
| Upsert CDC | **SCAN full-keyspace reconciliation** (re-read every pass, idempotent RESTORE); nothing written to source. |
| Delete CDC | **Durable target-vs-source keyspace diff (baseline, §3.3-aligned)** via the additive interface; **notifications + optional seen-set = accelerators only** (droppable, no reseed trigger). Cutover + restart run a full sweep. |
| Delete detection reads | **Master-pinned** (replica lag would cause false deletes); value reads may use replicas. |
| Checkpointing | **Coarse phase** (per-unit snapshot-complete); SCAN cursor **not** durably resumable; `Watermark`/`DeleteRange`/`Lo` **no-ops**; sweep persists **nothing durable** but carries **bounded in-memory** cursor+phase+cadence state (a pass-boundary state machine, §0.4). |
| Keyset memory | **Bounded, O(batch) not O(keyset):** both the reconciliation scan (RM5) and the delete-sweep (RM6) hold a live SCAN cursor across ticks and emit ONE bounded batch per call — **never buffer a whole pass** (consistent with RM11 "backpressure without unbounded memory"). The seen-set accelerator is the only optional O(keyset) structure, and it is droppable. |
| Copy shape | **One atomic chunk per unit**; intra-unit parallelism **inside `CopyChunk`**; restart re-copies the unit idempotently. |
| Cluster | Per-master SCAN + subscription + delete-sweep; topology via `CLUSTER SHARDS`/`SLOTS`; parallelism unit = shard; per-key ops sidestep cross-slot. |
| Selection | key-pattern globs + DB index (0 only in cluster); one full SCAN + client-side match; `SCAN TYPE` (6.0+) when type-scoped. |
| TTL | **RELATIVE default** (skew-safe); `ABSTTL` opt-in from server `TIME`; `-1`→0, `-2`/nil→delete. |
| Big keys | **DUMP-and-warn** above a threshold; **block loud** above a refuse-cap; **no incremental** (torn reads unfaithful). Applies to copy AND streaming. |
| Privileges | Redis 6+ ACL, no admin; **`+restore` granted explicitly** (`@dangerous`); notification-enable/DEBUG are privilege-gated extras. |
| State store | **Reused Postgres, no new code**; nothing on source; F4 N/A. **Redis→Redis still requires a PG state store** (documented wart). |
| Atomicity | **Per-key idempotent apply, NO cross-key/group atomicity** (departs from §8.1 — recorded in CLAUDE.md). |
| Observability | F2 reused; `table`→unit/shard; **+ delete-reconciliation-lag** signal. |
| Consistency | Eventual convergence, self-healing; no point-in-time. |
| Non-goals | cross-engine; reconstruction/incremental; **Redis fan-out**; **Sentinel-hardened failover**; Dragonfly; multi-master. |
| CI vs local | **CI = compile/unit/PG-regression only; Redis correctness is a LOCAL gate** (`REPLICARE_INTEGRATION=1 REPLICARE_REDIS=1`). |

## Open questions / future work
- **RQ-2 (RESOLVED):** `DeltaID` = per-unit monotonic change-id; no cross-unit ordering needed (confirm
  against `coalesce`/`ConfirmConsumed` in RM3).
- **RQ-4:** verify no target tier forbids `DUMP`/`RESTORE` themselves (would make that target unsupported
  in v1 — no fallback exists) — check in RM0/RM2.
- **RQ-6:** confirm redis#3218 is binary-transport corruption (not a version-gate reversal) via a real
  6.2→7.4 binary-safe round-trip in RM4; and prove STREAM group/PEL fidelity across the gap (Momus m9).
- **RQ-7:** confirm Redis Cloud clustered notification per-shard subscription per tier in RM7.
- **RQ-8 (RESOLVED):** durable target-vs-source sweep is the delete **baseline**; seen-set/notifications
  are accelerators (§3.3-aligned, resolves Momus M4).
- **RQ-9 (deferred features, explicitly out of v1):** multi-target **fan-out** (needs per-target scan
  state); **Sentinel-hardened failover** (harness + failover test); **command-reconstruction** (only if a
  future §1.7 exception is ever justified — currently rejected); **big-key incremental** (only with a
  faithful, non-torn mechanism).
