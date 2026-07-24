# M5c — Delta purge, bounded retention & forced-reseed state machine

**Status:** design artifact for M5c (the Momus B3 pre-work). It specifies the
purge mechanics, the retention caps, and — the load-bearing part — the reseed
state machine, with a stated invariant for **why no source change is lost across
the handoff**. Section refs (§3.4, §4, §3.3, §8.1) point at `CLAUDE.md`.

This re-derives the two guarantees M5c has to hold together: **capture-first
initial copy** (§4) and **crash-safe at-least-once consumption** (§3.3).

---

## 1. What M5c protects against

The source-side delta table is a high-churn queue we write to a database we may
not own (§3.4). A slow or down target *pins* it: a delta row cannot be purged
until **every** target has consumed it, so one laggard makes the source grow
without bound. M5c bounds that growth in three layers:

1. **Consumption-gated purge** — the baseline: a delta consumed by *all* targets
   is deleted. Batched + rate-limited; paired with the aggressive per-table
   autovacuum already set at capture install (§3.4).
2. **Bounded retention (age + size)** — a cap on how far the queue may grow
   because of a laggard. When a target pushes the queue past the cap, that
   target is *sacrificed* rather than the source.
3. **Forced reseed** — the sacrificed target is marked `needs_reseed`, its
   pinning deltas are dropped, and it is brought back to convergence by a fresh
   full copy + streaming resume. This is the state machine specified in §4.

The `xmin` horizon (§3.4) is the residual risk the batched-DELETE path cannot
beat on old sources: autovacuum cannot reclaim dead tuples older than the
source's oldest running transaction, so a long idle-in-transaction session makes
the delta table bloat *regardless of purge*. M5c cannot fix that on an old
source — it **surfaces it loudly** (telemetry) and still bounds logical growth
via retention/reseed. On PG ≥ 10 the partition+DROP path avoids the trap; see §5.

---

## 2. Objects involved (grounding)

Per captured source table (keyed by a stable `rel_id`, in the source `replicare`
schema):

- `replicare.delta_<rel_id>` — the dirty-key set. Columns: `delta_id bigserial`
  PK, `rc_op`, key columns `k1..kn`, and `rc_txid` / `rc_seq` / `rc_at`
  (lag/ordering hints only, never a resume cursor — §3.3).
- `replicare.track_<rel_id>` — per `(target, delta_id)` consumption record. A row
  means that target has applied that delta. PK `(target, delta_id)`.

Daemon state (the `StateStore`, a *separate* Postgres schema — §9 "do not
conflate"):

- `cursors(sync, target, table)` carries `phase` (`initial_copy` / `streaming`),
  `last_delta` (lag only), and **`needs_reseed`** — the flag M5c drives.
- `copy_progress(sync, table)` — the resumable initial-copy watermark (§4.1).

**Separation of concerns (§9):** `Source.Purge` touches *only* source objects
(delta/track). It never writes the `StateStore`. It **returns** which targets it
sacrificed; the engine-neutral orchestration (`internal/reseed`) is what writes
`needs_reseed`, records events, and runs the re-copy. This keeps the delta
lifecycle self-contained on the source and the daemon state on the StateStore.

---

## 3. Purge mechanics

`Source.Purge(ctx, table, targets, retention) → (PurgeStats, error)`

`targets` is the **full configured target set** for the sync — required because
"consumed by all targets" is only decidable against the complete set (a target
that has consumed nothing has *no* track rows, yet still pins the queue).

### 3.1 Consumption-gated batched purge (baseline, all versions)

A delta is purgeable once every configured target has a track row for it. Batched
by `delta_id` to bound transaction churn, looped until a pass deletes < batch:

```sql
WITH purgeable AS (
    SELECT d.delta_id
    FROM   replicare.delta_<rel> d
    WHERE  (SELECT count(*) FROM replicare.track_<rel> tr
            WHERE tr.delta_id = d.delta_id AND tr.target = ANY($targets)) = $ntargets
    ORDER  BY d.delta_id
    LIMIT  $batch
)
DELETE FROM replicare.delta_<rel>
WHERE  delta_id IN (SELECT delta_id FROM purgeable);
```

`count(*)` is exact because `track` PK is `(target, delta_id)` — each target
tracks a `delta_id` at most once, so the count is the number of distinct targets
that consumed it. When it equals `$ntargets`, all have. Deletes are always
**by `delta_id`** (never by PK), preserving the §3.3 read-and-clear discipline.

### 3.2 Retention evaluation (per target)

For each target `T`, its **pinned backlog** is `delta MINUS track_T`. From it:

- **oldest-unconsumed age** = `now() - min(rc_at)` over the backlog. (`rc_at` is
  a hint, never a cursor — it is fine as a *retention* signal because we only
  ever compare it to a wall-clock cap, never resume from it.)
- **size signal** = `pg_total_relation_size('replicare.delta_<rel>')` — the
  actual on-disk footprint of the queue, which is what "protect the source"
  means. Attributed to the laggard: the target with the oldest unconsumed delta.

`T` is **over-cap** when `MaxAge > 0 && age > MaxAge`, or
`MaxBytes > 0 && size > MaxBytes && T pins the oldest delta`. Both caps are
opt-outable (`0` = unbounded); age defaults on (24h), size defaults off.

### 3.3 Retention-forced purge (sacrifice the laggard)

When a set `R` of targets is over-cap, Purge, in one pass:

1. Records `R` in `PurgeStats.TargetsReseeded` (the orchestration will mark them
   `needs_reseed`).
2. **Resets each `R`'s track:** `DELETE FROM track_<rel> WHERE target = ANY($R)`.
   A reseeded target is about to be re-derived from scratch, so its partial
   consumption record is void. (We reset to **empty**, never to a high-water
   mark — a fast-forward would re-introduce the §3.3 commit-order hazard.)
3. **Purges the now-unpinned deltas** — those consumed by all *remaining*
   (non-reseeding) targets, treating `R` as already satisfied:

```sql
WITH purgeable AS (
    SELECT d.delta_id
    FROM   replicare.delta_<rel> d
    WHERE  (SELECT count(*) FROM replicare.track_<rel> tr
            WHERE tr.delta_id = d.delta_id AND tr.target = ANY($remaining)) = $nremaining
    ORDER  BY d.delta_id
    LIMIT  $batch
)
DELETE FROM replicare.delta_<rel>
WHERE  delta_id IN (SELECT delta_id FROM purgeable);
```

For the **single-target** case (the v1 common path) `remaining` is empty and
`$nremaining = 0`, so `count(*) = 0` matches every row → the queue drains fully.
That is exactly the intended behaviour: the one laggard is being reseeded, so all
its deltas are discardable and the source is fully unpinned.

**Why dropping `R`'s unconsumed deltas is safe:** because `R` gets a *full*
re-copy of current source state (§4). A delta is a pointer to "PK X changed";
re-copy re-reads X's current value regardless. Discarding the pointer loses
nothing the re-copy does not re-derive. See the invariant in §4.3.

`PurgeStats{ DeltasPurged, TargetsReseeded }` is returned for telemetry
(`replicare_delta_purged_total`, `replicare_reseed_total`) and orchestration.

---

## 4. The reseed state machine

Driven by `internal/reseed` over the `Source` / `Sink` / `StateStore` interfaces,
reusing the M4 copy driver and the M5a/M5b drain. Per reseeding target `T`, over
the tables of its FK component:

```
        ┌─────────────┐
        │  STREAMING  │  (normal drain: delta MINUS track_T)
        └──────┬──────┘
               │ Purge reports T over-cap  (source unpinned, track_T reset)
               ▼
        ┌─────────────┐
        │  MARKED     │  StateStore: cursor.needs_reseed=true, phase=initial_copy
        │             │  event target.needs_reseed (ERROR); metric reseed_total++
        └──────┬──────┘
               │ reset copy_progress(T-tables) to fresh
               ▼
        ┌─────────────┐
        │  RECOPYING  │  per table: DeleteRange(nil,nil) → copy.Component (LoadDirect)
        │             │  checkpointed by copy_progress watermark (§4.1)
        └──────┬──────┘
               │ copy done for the component
               ▼
        ┌─────────────┐
        │  RESUMING   │  drain delta MINUS track_T with track_T EMPTY:
        │             │  re-consume every retained delta (idempotent re-read+upsert)
        └──────┬──────┘
               │ backlog drained; phase=streaming; needs_reseed=false
               ▼
        ┌─────────────┐
        │  STREAMING  │
        └─────────────┘
```

### 4.1 Why `DeleteRange(nil,nil)` before re-copy

The reseed target is **non-empty** (stale data). A fresh `copy_progress`
(`Watermark=nil`) makes the copy driver plan every chunk and `LoadDirect` (COPY
FROM) — which would collide with existing rows and, worse, leave *orphans* for
source rows deleted since the last copy. So reseed first clears the table with
`DeleteRange(ctx, ref, nil, nil)` (predicate `TRUE`, i.e. delete-all — within the
DML-only target grant, no `TRUNCATE` privilege needed), then copies into the now-
empty table. This re-derives `target = current source` exactly, deletes included.

### 4.2 Why track stays empty through RESUMING

After re-copy, `track_T` is empty, so the resume drain re-consumes **all**
currently-retained deltas. Each is an idempotent re-read of the key's *current*
value + upsert (§3.1 self-healing) — re-applying a delta the copy already covered
is a no-op-shaped upsert, never corruption. We deliberately do **not** fast-
forward the track: emptiness is both correct (idempotent) and hazard-free
(no commit-order skew). It re-consumes redundant work once; convergence is exact.

### 4.3 Invariant — no source change is lost across a reseed

> **Capture is never uninstalled during reseed, and initial copy is capture-first
> + idempotent; therefore for every key the target ends at the key's current
> source value, regardless of when changes landed relative to the handoff.**

Proof sketch. Triggers stay installed for the whole machine, so every source
change during reseed enqueues a fresh `delta_id`. Take any key `X` and any change
`C` to it during the reseed window. `C` reaches the target by at least one path:

- **(a) copy path** — if `C` committed before RECOPYING read `X`'s chunk, the
  re-copy loaded `X`'s current value directly.
- **(b) delta path** — `C` left a delta row (retained; not purged, since the
  §3.3 purge only removes rows consumed by remaining targets, and `track_T` is
  empty during resume). RESUMING re-reads `X`'s **current** value and upserts.

Because re-read always fetches the *current* value (self-healing), intermediate
states are irrelevant and the two paths cannot disagree on the final value. A
change after copy but before resume falls in (b). A delete of `X` at the source
is path (a) (absent from the re-copy → not loaded) or path (b) (re-read finds `X`
absent → delete-absent). Thus `X` converges to its current source state — which
is the one-way guarantee (§6: source authoritative, target overwritten to match).
The *only* thing discarded is `R`'s historical intermediate deltas, which reseed
intentionally supersedes with a current-state copy. ∎

### 4.4 Crash safety across the handoff (at-least-once, no 2PC)

Every transition is idempotent and independently checkpointed, so a crash at any
point resumes correctly — the same discipline as §3.3, applied to the machine:

| Crash point | On restart |
|---|---|
| after track reset, before MARKED persisted | `track_T` empty; Purge is idempotent; next purge pass re-detects over-cap → re-marks. No loss (deltas already unpinned). |
| after MARKED, before/into RECOPYING | `needs_reseed=true` + `phase=initial_copy` in StateStore → orchestration restarts the re-copy; `copy_progress` watermark resumes mid-table (§4.1). |
| mid RESUMING | `track_T` still (partially) empty; drain re-reads current values; idempotent upserts replay harmlessly. |
| after resume drained, before clearing `needs_reseed` | flag still set → one more (no-op) resume pass, then cleared. |

`needs_reseed` is cleared **only after** the resume backlog is drained and
`phase` set to `streaming`, so the flag conservatively over-approximates
"reseed in flight." At-least-once throughout; no distributed transaction.

### 4.5 Ordering guard (the trap we must not fall into)

The reseed must **reset `track_T` to empty**, never to `max(delta_id)` or any
high-water mark. A high-water reset would declare late-committing low-`delta_id`
rows "already consumed" and skip them forever — the exact commit-order hazard
§3.3 was built to defeat. Emptiness re-consumes them idempotently instead. This
is the single most important correctness rule of the machine.

---

## 5. Partition+DROP decision (Momus M-4)

**Decision:** the **batched-DELETE + aggressive-autovacuum** path is the
**universal v1 default**, on every supported source version. Time-partitioned
delta tables purged by `DROP`/`TRUNCATE` of fully-consumed partitions remain the
**documented, recommended optimization for high-churn deployments on PG ≥ 10**
(it is bloat-proof and dodges the `xmin` horizon), but it is **not auto-enabled
in v1** and is **not the default even on modern sources.**

Rationale: partition routing changes the delta-table DDL and the trigger's insert
target and requires a partition-maintenance loop (create-ahead, drop-behind) — a
larger surface that would delay a correct, universal M5c. Keeping one code path
(batched DELETE) correct on the 9.6 floor *and* PG 17 is worth more in v1 than
the bloat win, especially since retention/reseed already bounds *logical* growth
and telemetry makes the `xmin`-horizon physical-bloat case loud. Partition+DROP
is revisited as a tuning pass in M9 (partition granularity/cadence numbers are
open there per plan §15). Until then it is an opt-in extension point, not a
promise.

This resolves `CLAUDE.md` §15's "partition+DROP-as-default-on-PG≥10" question:
**no, not default in v1; opt-in optimization, tune in M9.**

---

## 6. Telemetry (F2 contract, emitted by M5c, verified by M6)

Purge/retention/reseed emit against the frozen F2 registry
(`internal/observability`) — no new names:

- `replicare_delta_backlog_rows` / `_bytes`, `replicare_delta_oldest_unconsumed_age_seconds`
  — per `(sync, target, table)`; the "is the source footprint healthy?" headline.
- `replicare_delta_purged_total` — per `(sync, table)`; purge progress.
- `replicare_reseed_total` — per `(sync, target)`; forced reseeds.
- span `replicare.reseed` — wraps a reseed; error status on failure, backlog/age
  attributes.
- events `retention.cap_approaching` (WARN, escalating with
  `retention_cap_proximity` toward 1.0) and `target.needs_reseed` (ERROR).

The §3.4/§10 cross-channel rule holds: a target pushing the queue toward the cap
is visible in **metrics** (climbing backlog/age + proximity), **logs**
(escalating WARN→ERROR), and **traces** (reseed span) simultaneously.

---

## 7. Scope / non-goals for M5c

- Fan-out purge is implemented correctly (consumed-by-all across the full target
  set) but only lightly exercised — v1 hardens single-target (plan goals).
- No partition+DROP implementation (see §5).
- Retention-cap *default numbers* are set conservatively here (age 24h on, size
  off) and **tuned in M9** against the at-scale fault matrix.
- The daemon loop that *schedules* purge/reseed passes is M7; M5c delivers the
  reusable mechanics + primitives and proves them under integration tests.
