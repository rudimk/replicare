# replicare — Redis Engine Implementation Plan (v1 draft)

**Status:** **DRAFT — for extensive review (Sisyphus pre-planning + Momus design passes).** This is the
third engine after Postgres (shipped) and MySQL (shipped). Section refs like §3.2 point at `CLAUDE.md`.
The single-engine rule (a sync never crosses engines) makes this **Redis → Redis only**.

> Read the two shipped engines first. Redis reuses the engine-neutral `copy`/`apply`/`state`/`config`/
> `observability`/`pipeline` packages **unchanged**, exactly as MySQL did — but Redis is **not relational**,
> so the FK/trigger/keyset/type-coercion machinery that MySQL still implemented mostly **vanishes** here,
> and two genuinely new axes appear: **binary DUMP/RESTORE transport with an RDB-version compatibility
> gate**, and **cluster per-node SCAN reconciliation with delete-by-keyset-diff**.

---

## Guiding constraints carried over from CLAUDE.md (unchanged, engine-neutral)

1. **No privileged change stream — ever.** No Redis replication link, no RDB/AOF file access, no
   `PSYNC`. CDC uses ordinary, grantable commands (`SCAN`, `DUMP`, `PTTL`, `RESTORE`, `DEL`), plus
   keyspace notifications as an *optional* accelerator (§3.2).
2. **Least privilege.** Redis 6+ ACLs; no `@admin`, no `@all`, no `DEBUG`/`CONFIG`/`KEYS`/`FLUSH*` for
   the correctness baseline. (The one foot-gun: `RESTORE` is `@dangerous` — must be granted explicitly.)
3. **Highly performant + polite on a source we may not own.** Non-blocking `SCAN`/`*SCAN`, tuned
   `COUNT`, rate limits, big-key handling that never monopolizes the single-threaded source.
4. **Deeply observable.** Reuse the F2 contract (metrics/OTel/slog/status), reinterpreting `table`
   labels as **key-pattern / shard**.
5. **Engine-agnostic core.** Redis is the third `Source`/`Sink` behind the same interfaces; it plugs
   into the neutral pipeline with a semantic overload, not a redesign.
6. **Source may be very old.** Keep source-side commands conservative: `SCAN` (2.8+), `DUMP`/`RESTORE`
   (2.6+), `RESTORE … REPLACE` (3.0+), `PTTL` (2.6+); treat `SCAN … TYPE` (6.0+), ACLs (6.0+), and
   `RESTORE … ABSTTL` (5.0+) as modern-only niceties with older fallbacks.
7. **Faithful transport — never transform.** `DUMP`→`RESTORE` moves the value as its **RDB-serialized
   bytes**; replicare never interprets, coerces, or reconstructs a value. On an incompatible payload
   (RDB-version gate, missing module) we **fail loud**, never mangle. This is the hard §1.7 promise,
   realized in Redis by RDB serialization rather than text output functions.

---

## Client library & floor decisions (RM0)

- **Driver: `github.com/redis/go-redis/v9`** — pure Go (keeps the binary CGO-free/static, matching
  `cmd/replicare/engines.go`), binary-safe `[]byte` values (mandatory for DUMP payloads), and native
  **standalone / Sentinel / Cluster** clients with automatic `MOVED`/`ASK` handling and topology
  refresh. (Alternative `rueidis` is faster/RESP3 but go-redis is the mature, lowest-risk fit.)
- **Version floor (best-effort):** **source Redis 3.0** (`RESTORE … REPLACE`), **target Redis 5.0**
  (`RESTORE … ABSTTL`); **recommended 6.2+ both ends** (ACLs, `SCAN TYPE`). CI-tested pair: an **older
  source → modern target** (e.g. **6.2 → 7.4**) to exercise the old→new RDB-version direction, plus a
  **3-node cluster** harness. MariaDB-style hard block: **no non-Redis forks silently** (detect via
  `INFO server`; a KeyDB/Dragonfly/Valkey compatibility stance is an open question, RQ-1).

---

## 0. The crux: what Redis lacks that the relational path assumes (and the §1.7 traps each opens)

Redis has **no SQL, schema, tables, primary keys, foreign keys, triggers, transactions-across-keys,
ordered key ranges, or text wire format.** The neutral layer survives because nothing in it *inspects*
the internals of `engine.TableRef`, `engine.KeyValues`, or `engine.DeltaID` — they are opaque handles
passed straight back to the engine. Redis therefore **overloads** them:

| Neutral type | Relational meaning | Redis reinterpretation |
|---|---|---|
| `engine.TableRef{Schema,Name}` | one table | one **key-namespace / logical shard** (`{Schema:"redis", Name:"<sel-unit>"}`) |
| `engine.KeyValues []any` | a PK/unique tuple | `[]any{redisKey}` — a single opaque Redis key (`[]byte`) |
| `engine.DeltaID int64` | delta-table row id | a monotonic **change-id** (scan-batch / notification seq) — see RQ-2 on encoding |
| text `COPY` stream (`io.Reader`) | PG/MySQL wire text | a **private binary framing** of `DUMP` payloads + PTTL (RM3) |
| `engine.Component` (FK component) | FK connected component | a **single-member acyclic component** per key-namespace |

### 0.1 No relational model → the FK/component/cyclic apply machinery is bypassed, not reimplemented
The neutral streaming loop (`pipeline/stream.go`) routes through `apply.DrainComponentRetrying →
DrainComponent → sink.BeginApply(cyclic, componentTables) + ApplyTx.StageUpsert/DeleteAbsent`. Redis has
no FKs, so:
- **DECISION (component model):** every key-namespace is exposed as a **single-member, acyclic
  `engine.Component`** (`Order=[ns]`, `Cyclic=nil`, `HasCycle()==false`). `Preflight` synthesizes these;
  there is no topo sort, no cycle case, no retry-fallback firing (Redis apply raises no transient FK
  errors). **Zero edits to the neutral pipeline** — Redis stays on the existing `stream.go` loop.
- **DECISION (thin `ApplyTx`):** Redis implements a trivial `ApplyTx`: `StageUpsert(ref, cols, reread)`
  reads the framed re-read stream and **pipelines `RESTORE … REPLACE`** (records the staged key set);
  `DeleteAbsent(ref, dirtyKeys)` **`DEL`s the dirty keys absent from the staged set** (deleted at
  source); `Commit` flushes the pipeline. **No `MULTI`/`EXEC` group atomicity** — apply is per-key
  idempotent, so cross-key/cross-slot atomicity is neither needed nor available in Cluster (§0.5). The
  `cyclic`/`componentTables` args are ignored.
- **Alternative considered + rejected for v1:** adding a single-unit `apply.Drain` branch in `stream.go`
  (bypassing `ApplyTx`). Cleaner conceptually but edits the neutral pipeline; the thin-`ApplyTx` path
  needs **zero neutral edits**, so it wins (revisit if the staging shape proves awkward — RQ-3).

### 0.2 No text COPY / no output functions → DUMP/RESTORE **binary** transport + two new pre-flight axes
- **DECISION (faithful transport = `DUMP` → `RESTORE … REPLACE`).** `DUMP key` returns the value's
  RDB-serialized bytes (type + logical content preserved; CRC64 + embedded RDB-version footer). `RESTORE
  key ttl payload REPLACE` recreates it, re-selecting the optimal on-target encoding. This is the §1.7
  faithful path — replicare never interprets the value. **`REPLACE` is mandatory** (idempotent
  overwrite = the apply model; without it `RESTORE` errors `BUSYKEY`). The payload is **raw binary** and
  MUST stay `[]byte` end-to-end (no text/UTF-8 coercion) or the CRC64 check fails loud.
- **DECISION (RDB-version directional gate — pre-flight BLOCK).** `RESTORE` rejects a payload whose
  embedded RDB version is **greater than** the target's max supported version. So **older/equal source →
  newer/equal target works; newer source → older target FAILS LOUD** (`DUMP payload version or checksum
  are wrong`). Pre-flight reads `INFO server`→`redis_version` on both ends, maps to max RDB version, and
  **blocks when target < source** (actionable: "upgrade the target, or opt into the command-reconstruction
  fallback"). This is the Redis analog of the relational "block on incompatible type."
- **DECISION (module compatibility gate — pre-flight BLOCK).** Module-defined types (`ReJSON-RL`,
  RedisBloom, RedisTimeSeries, …) produce **module-specific opaque DUMP payloads** that fail `RESTORE`
  on a target lacking the identical module. `TYPE` returns the module type name; pre-flight compares
  `MODULE LIST` on both ends and **blocks when a source module type is absent on the target** (never
  reconstructs an opaque module value — no faithful native reconstruction exists).
- **DECISION (command-reconstruction fallback — narrow, opt-in, lossy-refusing).** When DUMP/RESTORE is
  unusable (RDB-version gate fails, or a managed tier restricts DUMP/RESTORE — RQ-4), an **opt-in**
  per-type fallback (`GET/SET`, `HGETALL/HSET`, `SMEMBERS/SADD`, `LRANGE/RPUSH`) covers
  **string/hash/set/list only**, and **REFUSES `zset` (float-score precision loss), `stream`
  (consumer-group/PEL state unreconstructable), and module types** — loud, never lossy (§1.7).
- **N/A:** session-GUC canonicalization (DateStyle/TimeZone/extra_float_digits/…) — Redis values are
  opaque bytes; there is nothing to canonicalize.

### 0.3 No trigger capture, can't write to the source → SCAN reconciliation is the CDC baseline
- **DECISION (CDC = full-keyspace SCAN reconciliation; nothing written to the source).** There is no
  delta/track table, no trigger, no `replicare` source schema. The "capture" is **repeated `SCAN` passes
  that re-read current values**. `InstallCapture`/`RemoveCapture` become the **optional** setup/teardown
  of the keyspace-notification subscription (or a no-op when notifications are off).
- **DECISION (keyspace notifications = optional accelerator only, never trusted).** Fire-and-forget
  Pub/Sub, **value-less** (key + event type only — the exact PK-only-capture analog), **lost on
  disconnect**, **node-local in Cluster** (subscribe per master), and **privilege/provider-gated**
  (`CONFIG SET notify-keyspace-events` is `@admin`, usually withheld on managed sources; ElastiCache
  needs a custom parameter group with the restricted `EKg$lshzxeA` letter set). Used to shorten
  reconciliation lag by fast-pathing flagged keys; **on any subscription gap/reconnect, force a full
  reconciliation pass** for that shard (we can't know what we missed). Missed event → caught by the next
  scan. Correctness **never** depends on it.
- **N/A (huge simplification vs relational):** no delta tables → **no source bloat we create, no
  autovacuum, no delta purge, no `xmin`/history-list trap, no per-table retention math.** `Purge` becomes
  a near-no-op / change-log trim; the "reseed" concept survives but means "force a full reconciliation"
  (§0.4).

### 0.4 No cheap change token; deletes have no capture → re-read-every-pass + delete-by-diff
- **DECISION (dirty-key model = re-read current value every pass, idempotent apply).** Redis has **no
  trustworthy cheap per-key change token** (`OBJECT IDLETIME/FREQ` reset on reads / policy-dependent;
  `MEMORY USAGE` misses same-size edits; `DEBUG DIGEST-VALUE` is admin/managed-disabled). So — exactly
  like the relational "re-read current row at sync time" — every key in a reconciliation pass is treated
  as **potentially dirty** and re-`DUMP`ed → `RESTORE … REPLACE`d. Correctness is guaranteed; the cost is
  re-transporting unchanged values, bounded by (a) notifications narrowing *which* keys between full
  passes and (b) **optional, opportunistic fingerprinting** (`MEMORY USAGE` pre-filter / `DEBUG
  DIGEST-VALUE` where allowed) that may *skip* provably-unchanged values — always an optimization atop a
  full-re-read baseline, **never the correctness mechanism** (mirrors "scalar cursor = lag hint, never
  resume cursor").
- **DECISION (delete detection = source-side consecutive-pass seen-set → `Op=D`).** A deleted key simply
  stops appearing in `SCAN`. The Redis **Source** tracks the keyset seen in the previous full pass; a key
  present last pass but absent this pass is emitted as a **`DirtyKey{Op:OpDelete}`** through
  `ReadDirtyKeys`, so the neutral `ApplyPass`/`DeleteAbsent` path deletes it on the target — **zero
  pipeline edits.** (A `DUMP` returning nil mid-pass — key deleted/expired between SCAN and DUMP — is the
  same signal: emit delete.)
  - **Memory bound + reseed fallback (the headline cost).** The seen-set is O(keys-in-selection) daemon
    memory. It is **bounded by config**; a keyspace exceeding the bound is marked **needs-reseed**, and its
    delete-reconciliation degrades to a **target-anchored sweep** (`SCAN` the target, `EXISTS` on source,
    `DEL` the missing) run on a configurable interval — bounded memory, higher latency. This reuses the
    neutral **retention → forced-reseed** machinery conceptually: over-bound → reseed. On **daemon
    restart** the seen-set is empty, so the first pass runs a full target-vs-source delete sweep to
    re-establish it (crash-safe: idempotent). **Delete-propagation latency ≈ the reconciliation/diff
    interval unless a notification catches it sooner** — a documented product characteristic.
- **DECISION (no durable SCAN-cursor resume → coarse phase checkpointing).** A `SCAN` cursor is an
  opaque reverse-binary index, **not durably resumable** across rehashes/restarts. The StateStore records
  **coarse phase** ("initial snapshot complete for shard X", streaming) and the delete-sweep progress,
  **not** a mid-scan cursor. A crash mid-snapshot restarts that shard's `SCAN` from 0 — safe because apply
  is idempotent. (`CopyProgress.Watermark` may cache an in-flight cursor as a best-effort hint only.)

### 0.5 Cluster, topology, selection, big-keys, TTL, ACL specifics Redis breaks
- **DECISION (Cluster = per-master-node everything).** `SCAN` is **per-node**, not cluster-wide. Topology
  via `CLUSTER SHARDS` (7+) / `CLUSTER SLOTS` (older, conservative for old sources), refreshed on
  `MOVED`/`ASK`. Reconciliation **fans out one SCAN + one notification subscription per master**;
  parallelism unit = **shard** (the Redis analog of an FK component for scheduling). Per-key
  `DUMP`/`RESTORE`/`DEL`/`PTTL` sidestep the cross-slot multi-key restriction entirely. **Read-from-replica**
  is a politeness toggle (offload masters; tolerate slight staleness — the reconciliation model absorbs it).
- **DECISION (selection = key-pattern globs + DB index).** Standalone: DB 0–15 (`SELECT n`). **Cluster:
  DB 0 only** (validated). Include/exclude globs map to key patterns, but **`SCAN … MATCH` filters
  *after* walking the table** (cost = total keyset, not matched subset), so replicare does **one full
  `SCAN` per pass + client-side include/exclude matching** (single walk, exclude-wins, arbitrary globs);
  `SCAN … TYPE` (6.0+) prunes server-side when a selection is type-scoped. Selection lives in the
  engine-specific config block (§11), reusing neutral `engine.Selection`.
- **DECISION (big keys = fidelity/blocking tradeoff, configurable threshold).** `DUMP` of a multi-GB
  value **blocks the single-threaded source**. Above a configurable **big-key threshold** (detected via
  `MEMORY USAGE`), replicare switches that key to **non-blocking `HSCAN`/`SSCAN`/`ZSCAN` incremental
  transport** — which is the lower-fidelity command path, so it **refuses big `zset`/`stream`** (loud) and
  logs fidelity caveats; giant **strings** use `GETRANGE`/`SETRANGE` windowing. Default: DUMP/RESTORE for
  everything; conservative source pressure (tuned `SCAN COUNT`, rate limits, no blocking commands). *(v1
  scope: RQ-5 asks whether big-key incremental transport is v1 or deferred, defaulting to "DUMP-only + a
  loud size warning" if deferred.)*
- **DECISION (TTL faithful via `PTTL` → `RESTORE`, prefer `ABSTTL`).** `DUMP` omits TTL; capture with
  `PTTL` (ms). Map `PTTL -1`→`RESTORE ttl 0` (no expire); `PTTL -2`/nil-`DUMP`→**delete-reconcile** (key
  gone). Prefer **`RESTORE … ABSTTL`** (absolute ms expiry = `now+pttl`) on target ≥ 5.0 to avoid TTL
  drift from transport latency; relative-TTL fallback on older targets. Guard: an already-past `ABSTTL` →
  skip `RESTORE`, `DEL` instead (never write a doomed key).
- **DECISION (least-privilege ACLs, no admin).** Source reader: `+scan +dump +pttl +type +object
  +exists +memory|usage +cluster +info` on `~*` (or narrowed key patterns where prefix-shaped). Target
  writer: `+restore +del +unlink +pttl +pexpire +pexpireat +type +exists +scan +object +info +cluster`.
  **`RESTORE` is `@dangerous` — excluded from the common `+@all -@dangerous` / managed "read-write"
  preset — so `+restore` MUST be granted explicitly** (the #1 foot-gun, documented loudly). Notification
  subscribe needs `+subscribe +psubscribe +&__key*@*__:*`; enabling notifications needs `@admin` config
  (operator does it out-of-band). No `KEYS`/`FLUSH*`/`DEBUG`/`SHUTDOWN` in the baseline.

---

## Goals & non-goals

**Goals (v1 Redis):** Redis→Redis replication via SCAN reconciliation (baseline) + optional keyspace-
notification acceleration; **byte-faithful DUMP/RESTORE transport** with the RDB-version + module
pre-flight gates; **standalone, Sentinel, and Cluster** topologies with per-master fan-out; TTL fidelity;
delete-by-diff; least-privilege ACLs; full reuse of the neutral copy/apply/state/config/observability/
pipeline layers; the F2 observability contract; the real daemon (tri-engine coexistence).

**Non-goals (v1):** cross-engine (never — §6); command-reconstruction of `zset`/`stream`/module types
(refused, not lossy-reconstructed); point-in-time consistency (eventual convergence only); active-active/
multi-master; replicating non-keyspace state (ACLs, configs, functions, pub/sub channels, `CLIENT`
state); RDB/AOF-file or `PSYNC`-based capture; module-value transformation.

## Definition of Done (Redis engine)
A Redis sync **initial-copies** a selected keyspace (all six native types + TTL, standalone **and**
cluster) **byte-faithfully** (DUMP/RESTORE), then **streams to convergence** under continuous source
writes/deletes/expiries via reconciliation + optional notifications; **pre-flight blocks** newer-source→
older-target and missing-module cases loudly; runs under the real daemon **alongside** Postgres and
MySQL syncs; emits the F2 contract; is documented + demoable; and passes a hardening gate (topology
change, notification gap, big keys, expiry races, RDB-version mismatch).

## Cross-cutting foundations (Redis adaptations of F1–F4)
- **F1 (config):** new `redis:` block (RM0.5) via `config.RegisterEngine` — connection (mode/seeds/DB/
  TLS/ACL/read-replica), selection (patterns/db/types), tuning (COUNT, intervals, big-key threshold,
  seen-set bound, notifications on/off).
- **F2 (observability):** **reused unchanged**; `table` labels carry key-pattern/shard; `delta_backlog`
  ≈ reconciliation/notification backlog; `rows_copied` ≈ keys copied (RM6 verifies).
- **F3 (harness/CI):** new Redis harness — standalone **old→new pair** + a **3-node cluster**; a
  `test:integration:redis` task; local-only (like MySQL, CI stays PG-only).
- **F4 (source-schema versioning):** **N/A** — nothing is written to the source. **All durable state is
  the reused Postgres StateStore** (RM2); the only "migration" concern is the StateStore's own (already
  built).

## Testing strategy (every milestone)
Unit tests for pure logic (selection glob→match, RDB-version mapping, framing serde, ACL SQL shape,
seen-set diff); **integration** against the old→new standalone pair **and** the cluster harness, gated on
`REPLICARE_REDIS=1` (skips in the PG-only CI `integration` job — verified locally each milestone, the
MySQL precedent). Keystone correctness tests: fidelity corpus (six types + TTL + binary + big),
convergence-under-churn, delete-by-diff, notification-gap → reconciliation backstop, RDB-version-gate
loud-fail, cluster resharding mid-copy.

---

## Milestones (RM0–RM9)

### RM0 — Foundations: engine skeleton, driver, harness (standalone+cluster), floor, CI
**Goal:** Registered-but-stubbed Redis engine; the concrete harness pair + CI + floor/fork decisions.
**Deliverables:** `internal/engine/redis/` package; **`go-redis/v9`** dep (static binary preserved);
`engine.Register` + `cmd/replicare/engines.go` blank import; harness (`test/harness-redis/`: standalone
6.2 source + 7.4 target, and a 3-node 7.4 cluster) + `Taskfile` `harness:redis:*` / `test:integration:redis`;
`docs/redis-version-support.md` (floor rationale, fork stance RQ-1).
**Acceptance:** package builds; `CGO_ENABLED=0` static binary still links; harness comes up healthy;
`ServerVersion` probe works standalone + cluster; non-Redis fork detection stubbed.
**Depends on:** none.

### RM0.5 — Redis config block (F1)
**Goal:** Lock the Redis connection/selection/tuning surface.
**Deliverables:** typed `redis:` block parser+validator via `config.RegisterEngine`: **connection**
(`mode: standalone|sentinel|cluster`, `nodes: [host:port]`, `sentinel_master`, `db` (standalone-only;
forced 0 + validated in cluster), TLS spectrum, ACL `user`/`password`, `read_from_replica`);
**selection** (`include`/`exclude` key-pattern globs — reuse neutral `Sync.Include/Exclude` — plus
optional `types` and `db`); **tuning** (`scan_count`, `reconcile_interval`, `delete_sweep_interval`,
`big_key_bytes`, `seen_set_max_keys`, `notifications: on|off`, rate caps).
**Acceptance:** golden-file parse+validate of a full Redis config (standalone + cluster); env-override
precedence; invalid combos rejected (non-zero `db` in cluster; unknown keys via `StrictDecode`);
single-engine rule holds (Redis source ⇒ Redis targets).
**Depends on:** RM0.

### RM1a — Connectivity, topology discovery, version/module introspection
**Goal:** Secure Redis sessions; a topology-aware "introspected schema" of key-namespaces/shards.
**Deliverables:** go-redis client mgmt for all three modes; **cluster shard-map discovery** (`CLUSTER
SHARDS`/`SLOTS`, refresh on `MOVED`/`ASK`); `ServerVersion` via `INFO server` (+ fork detection);
`MODULE LIST`; **`Introspect(sel)`** synthesizing a `*Schema` of pseudo-tables (one per selection
unit / logical shard, single synthetic key column, no FKs); selection glob→key-pattern matching
(`internal/engine/redis/selection.go`); read-from-replica wiring.
**Acceptance:** connects standalone + Sentinel + cluster; discovers all masters; `Introspect` returns the
selected key-namespaces on old→new; version + module list correct; selection globs match the intended
keys (client-side + `SCAN TYPE` where scoped).
**Depends on:** RM0.5.

### RM1b — Pre-flight: RDB-version gate, module gate (the faithful-transport blocks)
**Goal:** A Redis pair → validated report with every §1.7 block; near-vacuous otherwise (no type-compat).
**Deliverables:** **RDB-version directional classification** (map `redis_version`→max RDB version; target
< source ⇒ **BLOCK**, actionable) ; **module presence classification** (source module type absent on
target ⇒ **BLOCK**); big-key/`zset`/`stream` fallback-refusal findings; a neutral `PreflightReport` with
**single-member components** (no FK graph, no giant-component logic) and no type-coercion findings.
**Acceptance:** `validate` blocks a newer→older pair loudly; blocks a source using a module the target
lacks; passes an equal/older→newer native-type pair; reports the key-namespaces as single-member
components; a cluster pair validates per-shard.
**Depends on:** RM1a.

### RM2 — StateStore integration (REUSED — Postgres) + the overload wrinkles
**Goal:** Confirm a Redis sync runs on the existing neutral Postgres StateStore — testing + docs only.
**Deliverables:** **no new StateStore code.** Wiring + docs of the **overloads**: `TableRef`→key-
namespace/shard; **coarse phase checkpointing** (snapshot-complete-per-shard, not a resumable cursor);
delete-sweep progress; the **`DeltaID` int64 encoding** for a change-id (RQ-2); ownership advisory lock
reused as-is.
**Acceptance (assertable):** a Redis **sync definition** round-trips through the Postgres StateStore;
copy-progress + cursor rows persist and reload keyed by the synthetic namespace; ownership lock excludes
a second daemon; needs-reseed flag round-trips.
**Depends on:** RM0.5. *(Parallel with RM1; not a dependency of RM3.)*

### RM3 — Initial copy: SCAN snapshot → DUMP/PTTL → RESTORE REPLACE (byte-faithful, cluster fan-out)
**Goal:** Resumable-by-restart, byte-faithful initial copy of a selected keyspace, standalone + cluster.
**Deliverables:** **SCAN-partition chunk planning** (`PlanChunks` returns per-shard SCAN units as opaque
`Chunk`s; reuse the neutral `copy` worker pool + progress checkpointing **unchanged**); **private binary
framing** (`CopyChunk` emits `{key, pttl, dump-payload}` length-prefixed binary to `io.Writer`;
`BulkLoad` reads it and pipelines `RESTORE … REPLACE` with `ABSTTL`); **six native types + TTL** faithful;
**big-key handling** per RM0.5 (or the deferred loud-warning per RQ-5); nil-DUMP/expired-mid-scan →
skip+note; cluster **per-master parallel** copy.
**Acceptance (6.2→7.4 + cluster):** a keyspace of all six types (incl. binary values, TTLs, a
`ZSET` with high-precision scores, a `STREAM` with groups) copies **byte-identically** (verify via
`DUMP` equality or type-appropriate compare) standalone **and** across a 3-node cluster; kill mid-copy →
restart converges (idempotent); an already-expired ABSTTL is DEL'd not written.
**Depends on:** RM1b (module/version gate), RM2 (progress). *(RM1a introspection transitive.)*

### RM4 — Streaming convergence: reconciliation + delete-by-diff (TRUE FIRST STREAMING)
**Goal:** Continuous convergence via SCAN reconciliation + the single-member-component apply path.
**Deliverables:** **`ReadDirtyKeys`** drives the rolling reconciliation SCAN (batch of present keys as
`Op=I/U`) **plus** the **consecutive-pass seen-set delete detection** (absent keys as `Op=D`), honoring
the seen-set memory bound → needs-reseed → target-anchored delete-sweep fallback (§0.4);
**`RereadCurrent`** DUMPs the dirty keys into the binary framing; the **thin `ApplyTx`** (`StageUpsert`
= pipelined `RESTORE REPLACE`; `DeleteAbsent` = `DEL` of absent dirty keys; `Commit` = flush);
**`ConfirmConsumed`** advances the change checkpoint; **`Purge`/`DeltaBacklog`** report reconciliation
backlog/lag (near-no-op purge — nothing on source to trim unless a notification change-log is used).
**Runs on the neutral `stream.go` loop unchanged.**
**Acceptance (6.2→7.4 + cluster) — keystone correctness:** capture-first (or reconcile-first), then
`SET`/overwrite/`DEL`/`EXPIRE`/PK-rename-equivalent on the source **converge** on the target; a rapid
rewrite converges to the last value (last-writer-at-read-time); a **delete propagates** via the seen-set
diff; convergence holds **under continuous churn**; a cluster **resharding mid-stream** still converges
(topology refresh). Crash before `ConfirmConsumed` → reprocess idempotently.
**Depends on:** RM3.

### RM5 — Keyspace-notification accelerator (optional, lossy, never trusted)
**Goal:** Reduce reconciliation lag with notifications, without ever depending on them for correctness.
**Deliverables:** **per-master `PSUBSCRIBE`** to `__keyevent@<db>__:*` (node-local in cluster);
notification-flagged keys enter a **priority dirty-set** drained ahead of the rolling scan; `InstallCapture`
= subscribe/enable (privilege-gated; no-op when off or unprivileged); **on any subscription gap/reconnect
→ force a full reconciliation pass** for that shard; ElastiCache-safe flag subset; value-less events →
always re-read (`DUMP`). Correctness backstop: **a deliberately-dropped event is still reconciled** by the
next scan.
**Acceptance:** with notifications on, a source write converges with **sub-interval latency**; a simulated
subscriber disconnect (drop N events) still converges via the next scan (no lost update, no lost delete);
notifications-off falls back cleanly to pure reconciliation; cluster events subscribed per master.
**Depends on:** RM4.

### RM6 — Observability verification (REUSED F2)
**Goal:** Verify the reused F2 contract end-to-end for Redis + the operator surface.
**Deliverables:** confirm the Redis engine emits the declared metrics/spans/log events with `table`
reinterpreted as key-pattern/shard (`delta_backlog`→reconciliation backlog, `rows_copied`→keys copied,
`replication_lag`→reconciliation age); the neutral status API + CLI `status` report Redis syncs; the
cross-channel **target-unreachable trifecta** fires for a downed Redis target.
**Acceptance:** a `/metrics` scrape asserts the **named** series with expected labels for a live Redis
sync; a drain against a downed Redis target yields span-error + escalating-log + gauge/backlog trifecta;
status API reports Redis phase/lag/progress.
**Depends on:** RM4.

### RM7 — Daemon, CLI & multi-sync + **tri-engine** coexistence
**Goal:** Redis syncs run under the real daemon; **three** engines coexist in one daemon.
**Deliverables:** verify the neutral daemon manages Redis syncs (standalone + cluster, worker/shard
pools, graceful SIGTERM); CLI `run`/`validate`/`status`/`reseed` on Redis configs; a **mixed multi-sync**
— a Redis sync **and** a Postgres sync **and** a MySQL sync in one daemon on the tri-harness — running
concurrently (single-engine-per-sync holds).
**Acceptance:** ≥2 concurrent Redis syncs (standalone + cluster); Redis + Postgres + MySQL coexist on the
tri-harness; SIGTERM drains/checkpoints cleanly; ungraceful kill resumes; CLI exercises the Redis
lifecycle.
**Depends on:** RM6, RM2.

### RM8 — Packaging & docs (Redis, feature-complete)
**Goal:** Redis installable and documented.
**Deliverables:** static binary links go-redis (CGO-free); **ACL SQL** (`deploy/acl-source-redis.txt`,
`deploy/acl-target-redis.txt`, with the **explicit `+restore` foot-gun** front-and-center, cluster/
notification notes); example Redis config (standalone + cluster); Redis sections in getting-started/
configuration/operations/troubleshooting (the RDB-version gate, module gate, delete-latency
characteristic, notification lossiness, cluster per-node fan-out, big-key tradeoff); a **Redis demo**
(compose: old source + new target + replicare; a cluster variant); README status (PG + MySQL + Redis
shipped).
**Acceptance:** binary runs a Redis sync via the systemd sample; the demo replicates end-to-end
(standalone **and** cluster); quickstart works.
**Depends on:** RM7.

### RM9 — Hardening & E2E gate (Redis shippable gate)
**Goal:** Confidence under faults, scale, topology + version spread. **Passing RM9 = Redis shippable.**
**Deliverables:** full old→new + cluster integration run; **fault injection** (cluster resharding
mid-copy/mid-stream, notification-subscription gap, big-key blocking avoidance, key expiry races,
newer→older RDB-version loud-fail, module-missing loud-fail, target down, slow-but-reachable target);
**expanded fidelity corpus** (all six types incl. large collections, deep binary, TTL edges, empty vs
missing); **coarse throughput/politeness benchmarks** with recorded numbers (SCAN COUNT tuning, source
latency-impact bound) — correctness-first; **delete-propagation-latency** characterized under the
seen-set bound + sweep fallback.
**Acceptance:** standalone + cluster green; fidelity corpus byte-identical; convergence under churn +
resharding; delete-diff bounds keyspace drift; notification-gap backstop proven; loud-fail on version/
module incompatibility; perf numbers recorded; slow-target backpressure without unbounded memory.
**Depends on:** RM7 (parallel with RM8).

---

## Critical path
`RM0 → RM0.5 → RM1a → RM1b → RM3 → RM4 → RM5 → RM6 → RM7 → {RM8, RM9}`
with **RM2 parallel to RM1** (StateStore is reuse-only) and **RM5 layered on RM4** (accelerator on a
correct baseline). The two **de-risking novelties** are **RM3** (binary DUMP/RESTORE transport + cluster
fan-out) and **RM4** (reconciliation + delete-by-diff) — everything relational (FK components, cyclic,
trigger CDC, delta bloat/purge, type-coercion) is **absent**, so the plan is *shorter in relational
surface* but *deeper in the two Redis-native mechanisms*.

## Decision log (quick reference)
| Topic | Decision |
|---|---|
| Engine fit | **Overload the neutral vocabulary** (`TableRef`→key-namespace, `KeyValues`→`[]any{key}`, `DeltaID`→change-id, text-COPY→binary framing); **single-member acyclic components + thin `ApplyTx`**; **zero neutral-pipeline edits**. |
| Transport | **`DUMP`→`RESTORE … REPLACE`**, byte-faithful RDB payload, kept `[]byte` end-to-end. Command-reconstruction fallback: opt-in, **string/hash/set/list only**, refuses zset/stream/module. |
| Pre-flight | **BLOCK newer-source→older-target (RDB-version gate)**; **BLOCK missing-module**; no type-coercion axis. |
| CDC | **SCAN full-keyspace reconciliation baseline** (re-read every pass, idempotent apply); **nothing written to source**; keyspace notifications = **optional, lossy, node-local, privilege-gated accelerator only**. |
| Deletes | **source-side consecutive-pass seen-set → `Op=D`** (neutral delete path); **memory-bounded → needs-reseed → target-anchored delete-sweep** fallback; restart does a full delete-diff. |
| Change token | **None reliable** — re-read every pass; `MEMORY USAGE`/`DEBUG DIGEST-VALUE` fingerprinting is an **optional optimization**, never the mechanism. |
| Checkpointing | **Coarse phase** (snapshot-complete-per-shard); SCAN cursor **not** durably resumable; idempotent restart. |
| Cluster | **Per-master SCAN + subscription**; topology via `CLUSTER SHARDS`/`SLOTS`; parallelism unit = **shard**; per-key ops sidestep cross-slot; read-from-replica toggle. |
| Selection | **key-pattern globs + DB index** (DB 0 only in cluster); **one full SCAN + client-side match** (MATCH is post-scan); `SCAN TYPE` (6.0+) when type-scoped. |
| TTL | **`PTTL`→`RESTORE … ABSTTL`** (drift-free), map `-1`→0 / `-2`/nil→delete; past-ABSTTL→`DEL`. |
| Big keys | **DUMP by default**; above a threshold, **non-blocking `*SCAN` incremental** (lower fidelity; refuse big zset/stream) — or deferred to a loud size-warning (RQ-5). |
| Privileges | Redis 6+ ACL, **no admin**; **`+restore` must be granted explicitly** (`@dangerous` foot-gun); notification-enable + DEBUG are privilege-gated extras. |
| State store | **Reused Postgres, no new code**; nothing on the source; F4 source-schema versioning **N/A**. |
| Observability | **F2 reused unchanged**; `table`→key-pattern/shard; `delta_*`→reconciliation backlog. |
| Bloat/purge/retention | **Largely N/A** (no source capture tables); "reseed" = force full reconciliation / target-anchored delete-sweep. |
| Consistency | **Eventual convergence**, self-healing dirty-key-set; no point-in-time, no cross-key atomicity (idempotent per-key apply). |

## Open questions / future work
- **RQ-1 (fork stance):** KeyDB/Dragonfly/Valkey are Redis-protocol-compatible — support, best-effort, or
  block like MariaDB? (Valkey shares the RDB format; Dragonfly's DUMP compatibility is uncertain.)
- **RQ-2 (`DeltaID` encoding):** a change-id in an `int64` — a monotonic per-pass counter is clean;
  confirm nothing in the neutral layer needs it globally ordered across shards.
- **RQ-3 (thin `ApplyTx` vs `stream.go` branch):** if the `StageUpsert(reread io.Reader)` staging shape
  proves awkward for Redis, add a single-unit `apply.Drain` branch in `stream.go` instead (small neutral
  edit) — decide during RM4.
- **RQ-4 (managed-tier command availability):** verify some serverless/proxy Redis tiers don't restrict
  `DUMP`/`RESTORE` themselves (would force the command-reconstruction fallback earlier).
- **RQ-5 (big-key transport in v1?):** ship non-blocking `*SCAN` incremental transport in v1, or defer it
  behind a loud big-key size warning + DUMP-only? (Fidelity/politeness tradeoff.)
- **RQ-6 (older→newer DUMP/RESTORE corruption):** validate redis#3218 is binary-transport corruption
  (not a version-gate reversal) with a real 6.2→7.4 binary-safe round-trip in RM3.
- **RQ-7 (Redis Cloud clustered notifications):** confirm per-shard subscription requirement per tier
  (proxy-fronted vs node-direct) in RM5.
- **RQ-8 (delete-sweep vs seen-set as the default):** the plan defaults to the source-side seen-set
  (pipeline-clean) with the target-sweep as the over-bound fallback; Momus should weigh whether the
  target-anchored sweep should instead be the *default* (bounded memory) with the seen-set as the
  low-latency optimization.
