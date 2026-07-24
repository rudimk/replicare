# replicare — Postgres Engine Implementation Plan (v3)

**Status:** REVISED through two Momus passes. Pass 1 (READY-WITH-REVISIONS): all 8 must-fix items +
2 folded majors addressed. Pass 2 (STILL-NEEDS-REWORK on B4): the cyclic-FK `NOT NULL` hole and
the plan↔CLAUDE.md divergence are now fixed (case-based strategy; pre-flight block for `NOT NULL`
non-deferrable; CLAUDE.md §4.1/§14/§15 reconciled) + 3 minor re-review folds (M5b retry bound, F4
shared runner, M9 consolidated-gate note).
**Scope:** The Postgres source→target engine only (MySQL/Redis out of scope, but core interfaces
must not preclude them). Decisions are specified in `CLAUDE.md`; this plan sequences them. Section
refs like §3.4 point at `CLAUDE.md`.

## Goals & non-goals
**Goals (v1):** trigger-based CDC (no WAL), PK-only capture, dirty-key-set consumption (§3.1–3.4);
resumable initial chunked copy + delta streaming to a **single target** (fan-out interface present,
not hardened), one-way; faithful transport, never transform (§1.7, §4.2); Postgres-backed state
store, single active daemon per sync (§9); full observability (§10); runs against very old source
Postgres, modern target (§1.6).
**Non-goals (v1):** multi-master, HA/leader-election, live DDL, MySQL/Redis, value transforms,
Helm. Interfaces left ready where noted.

## Definition of Done (whole engine)
Feature-complete at **end of M8**; **shippable only after the M9 matrix gate passes** (reconciles
the prior DoD/shippable mismatch, Momus m-7). The engine is "done" when: given a YAML config it can
validate a source/target pair (pre-flight), install least-privilege capture, perform a resumable
consistent initial copy of an FK-connected multi-table schema **including self-referential and
cyclic FKs**, then stream inserts/updates/deletes/PK-changes to convergence — crash-safe,
observable across all three channels, bounding source bloat even under a stalled xmin horizon —
**verified by the automated multi-version integration matrix (M9).**

---

## Cross-cutting foundations (locked early so later milestones don't churn)
These exist because Momus flagged late-binding rework (M-1, M-5) and load-bearing undecided floors (m-3).

- **F1 — Config schema (locked in M0.5, before M1).** Designed and frozen up front as a
  **thin engine-neutral envelope + a typed per-engine block, dispatched via the engine registry**
  (CLAUDE.md §11; mirrors the Source/Sink decoupling). Neutral layer: sync wiring, delivery,
  observability endpoints, state store, logging, and engine-neutral tuning (drain interval,
  retention caps age+size, pool caps `max_source_connections`/`max_target_connections`). Engine
  block (engine-owned/validated): connection shape, selection, and engine-specific CDC tuning
  (worker/batch/chunk). **Single-engine rule (CLAUDE.md §6): never cross-engine — validation
  enforces a sync's source and all targets share one engine.** **v1 builds the neutral envelope +
  registry dispatch + the full Postgres block** (connection, `schema.table` selection globs,
  chunk/batch/worker tuning, TLS disable→verify-full, secrets inline+env, env-override precedence —
  defined and tested); **MySQL/Redis are documented extension points** (Redis notably needs
  cluster/sentinel topology + key-pattern selection, which the envelope must not preclude).
- **F2 — Observability contract (locked in M0).** A single registry of every **metric** name+labels,
  **span** name+attributes, and **structured-log event key** — including the cross-channel
  target-unreachable signal (§3.4/§10). M2–M5* each emit against this registry (per-milestone
  acceptance: "emits its declared metrics/spans"); M6 *verifies* and adds the status API. No late
  retrofitting.
- **F3 — Old-source version floor (decided in M0).** Commit to a specific oldest source PG in CI
  and justify it against §4.2 float-fidelity (`extra_float_digits=3` matters <12) and PL/pgSQL
  portability (§3.1). If 9.x is unavailable in CI, **explicitly list which CLAUDE.md claims become
  unverified** and flag the risk — do not silently raise the floor.
- **F4 — Schema versioning (decided in M0, recorded in CLAUDE.md).** **One shared migration-runner
  abstraction**, instantiated twice: for the Postgres **state-store schema** (M2) and the source
  **`replicare` schema** (M3). Both carry a `schema_version`; migrations are idempotent and in-place
  and **preserve in-flight deltas/cursors** across upgrades (the source schema lives on a DB we may
  not own — migrate, never drop-recreate).

---

## Testing strategy (every milestone)
- **Multi-version Postgres docker harness** (M0): the F3 old source + a modern target (16/17).
  Integration tests run on the matrix, not just unit tests.
- **Type-fidelity corpus** (from M1): int/numeric/float/text/bool/uuid/json[b]/arrays/ranges/bytea/
  timestamp[tz]/interval/enum **+ `money`** (asserts the §4.2 warn + `lc_monetary` pin), with
  round-trip expectations across the version gap.
- **Named property/race tests** (from M5a) — see M5a/M5c acceptance.
- **Fault injection** (from M5*): kill mid-copy/mid-stream; target down; **target slow-but-reachable
  (backpressure)**; long-idle-in-transaction source (xmin horizon).
- A milestone is **not complete** until its acceptance tests pass on the matrix in CI.

---

## M0 — Foundations & scaffolding
**Goal:** Building, testable, lintable skeleton with engine interfaces, the F2 observability
contract, the F3 floor, and the F4 versioning decision.
**Deliverables:** `go.mod` (Go 1.23+, `github.com/rudimk/replicare`), layout per §13; core
interfaces `Source`/`Sink`/`StateStore`/registry (stubs); `cmd/replicare version`; slog bootstrap;
CI (build/vet/lint/test/-race) + **Taskfile (go-task)**; **docker-compose multi-version harness**; **F2 metric/
span/log registry**; **F3 floor decision doc**; **F4 schema-version + migration-runner skeleton**.
**Acceptance:** CI green; `replicare version` works; harness brings up F3-old + modern target;
F2 registry exists and a smoke test asserts emission wiring; F3 floor committed in writing; F4
migration runner applies v1 and is idempotent on re-run.
**Depends on:** none.

## M0.5 — Config schema & secrets/TLS design (F1)
**Goal:** Lock the full config surface before anything consumes it.
**Deliverables:** complete YAML schema (every knob in F1); loader + **strict validation**; env-var
override of any field with **defined precedence**; secret resolution (inline + env); per-connection
TLS config plumbing (modes only, exercised in M1).
**Acceptance:** golden-file tests for parse+validate of a full config; env override precedence test;
invalid configs rejected with actionable errors; secret-from-env resolves over inline.
**Depends on:** M0.

## M1 — Connectivity, introspection & pre-flight
**Goal:** Point at a DB pair → validated, classified replication report; secure connections.
**Deliverables:** pgx v5 conn mgmt; **session-GUC canonicalization** every connection (§4.2 table,
incl. `lc_monetary` pin + **`money` warning**); **TLS per connection** (test `verify-full` +
`disable`, incl. old-server TLS compatibility note); **secret resolution** wired; source
introspection (tables, columns incl. generated/identity, PK/unique, FK edges, native partitions),
old-PG-safe catalog queries behind a version probe; **table selection** (include/exclude + globs);
**FK connected-component** computation + **giant-component warning** + **dangling-FK-to-excluded
warning** (§8.1); **cyclic-FK classification** (detect cycles/self-refs and, per §4.1, flag
**`NOT NULL` non-deferrable cyclic FKs as a blocking pre-flight error** while passing nullable and
DEFERRABLE ones through to M4's load strategy); **pre-flight type-compat check**
(identical/widening/risky/incompatible → block-on-incompatible, warn-on-risky; §4.2); no-PK/unique
tables **skip+warn** (§3.1); CLI `validate`.
**Acceptance (matrix):** `validate` lists tables/keys/components correctly; a synthetic hub table
triggers the giant-component warning; an FK to an excluded table triggers the dangling warning; a
`NOT NULL` non-deferrable self-ref FK is **blocked** with an actionable error while nullable and
DEFERRABLE cyclic FKs pass; a hand-built mismatch corpus is classified correctly (incompatible
blocks, risky warns); a PK-less table is skipped+warned; `verify-full` and `disable` both connect; env-resolved secret connects;
emits its F2-declared metrics/spans.
**Depends on:** M0.5.

## M2 — Postgres state store (+ versioning)
**Goal:** Durable resumable state with single-active ownership and migrations.
**Deliverables:** `StateStore` Postgres impl: dedicated schema (F4-versioned); sync definitions;
per-table copy progress (completed-range watermark + sparse out-of-order set); per-(target,table)
cursors; events; **single-active ownership** via `pg_advisory_lock` per sync (interface ready for
later leader election, §9); **migration runner** for the state schema.
**Acceptance:** checkpoints persist and a restarted process resumes exactly; a second process
cannot acquire a held sync; schema self-creates idempotently; a simulated version bump migrates
in-place with **no state loss**; emits its F2-declared metrics/spans.
**Depends on:** M0 (M0.5 for sync/target config shape).

## M3 — Trigger-based CDC capture (source machinery, + versioning)
**Goal:** Correct least-privilege capture into per-table delta tables, version-upgradable in place.
**Deliverables:** `replicare` **source schema** (F4-versioned) with **per-table delta tables** +
**per-(target,table) track tables** (created lazily at stream time from M2 target identity —
clarifies Momus m-2); old-PG-conservative PL/pgSQL **trigger functions** (PK-only, op type,
delta_id, seq/ts); **PK-update enqueues BOTH old+new PK** (§3.1); capture **install/uninstall**;
**aggressive per-table autovacuum reloptions** at install (§3.4); set-difference consume primitives
+ **delete-by-delta_id** (§3.3); **migration path that preserves in-flight deltas**; documented &
test-verified **least-privilege grants** (CREATE on schema, TRIGGER, SELECT; §12).
**Acceptance (matrix):** INSERT/UPDATE/DELETE produce correct delta rows; PK-change enqueues
old+new; install/uninstall leaves source clean; works under a non-superuser role with exactly the
documented grants; a source-schema version bump preserves queued deltas; emits F2 metrics/spans.
*(Note: the definitive commit-order hazard test lives in M5a, where consumption exists — Momus B2.)*
**Depends on:** M1, M2.

## M4 — Initial copy (+ concrete cyclic-FK strategy, + backpressure)
**Goal:** Resumable, consistent, faithful bulk copy of an FK-connected schema **including cycles**.
**Deliverables:** **chunking** — keyset PK-range default (composite/UUID/text via row-value cmp),
index-only boundary discovery, **ctid/block-range fallback**, native-partition aware (§4.1); **text
COPY** source→target via `io.Pipe`; **empty-target direct + DELETE-range resume**; **TEMP staging +
upsert** merge path (§4.1); **FK-component parents-first** ordering, chunks parallel per table,
components parallel (§8.1); **cyclic/self-referential FK strategy, by case (§4.1, never touch
target constraints; Momus B4):** (a) **nullable** cyclic FK → **NULL-then-fill two-pass**;
(b) **DEFERRABLE** cyclic FK (nullable or not) → load the component in one txn with
`SET CONSTRAINTS … DEFERRED`; (c) **`NOT NULL` + non-deferrable** cyclic FK → **detected in M1
pre-flight and failed loud** (cannot insert NULL into NOT NULL; user must make the FK DEFERRABLE or
break the cycle); per-chunk progress → StateStore;
**backpressure**: bounded queues + pgx pool sizing tied to F1 connection caps + defined
blocking/throttle policy (Momus M-6); **capture-first then copy** (§4).
**Acceptance (matrix):** copies a multi-table FK schema (composite & UUID PKs) old→new with
byte-faithful values (fidelity corpus incl. `money`); cyclic-FK corpus copies to a **CORRECT
result** across all three cases — a **nullable** self-ref FK (NULL-then-fill), a **DEFERRABLE**
cyclic FK (`SET CONSTRAINTS DEFERRED`), and a **`NOT NULL` non-deferrable** self-ref FK which must
be **rejected at M1 pre-flight with a loud, actionable error** (never NULL-into-NOT-NULL, never a
silent partial load); kill mid-copy → resume correct; a row changed during copy reconciles;
bounded-queue backpressure holds under a throttled target; emits F2 metrics/spans.
**Depends on:** M2, M3.

## M5a — Minimal streaming convergence (TRUE FIRST END-TO-END)
**Goal:** The earliest honest end-to-end: single-component streaming with crash-safety. (Momus B1.)
**Deliverables:** consume `delta MINUS track`; **faithful text re-read → staging upsert** (§4.2);
single-component apply (no cross-table FK ordering yet); **crash-safe ordering** apply→track→purge,
at-least-once, no 2PC (§3.3); **cutover** copy→streaming for the covered scope.
**Acceptance (matrix) — the project's keystone correctness tests, defined concretely:**
- **Commit-order hazard (Momus B2):** open T1 that triggers a low-`delta_id` row but does NOT
  commit; run a drain pass → it must NOT see/purge T1's row; commit T1; run a second pass → T1's PK
  is now applied to target. Asserts the §3.3 hazard is defeated.
- **Delete-by-id read-and-clear race (Momus M-7a):** while daemon processes PK X=V1, source updates
  X→V2 inserting a new delta row; daemon deletes only observed ids → assert V2 survives and converges.
- **Crash between apply and track-write (M-7b):** replays without duplication.
- **Coalescing (M-7c):** N updates to one PK → one re-read, final-value convergence.
- End-to-end insert/update/delete/PK-change converge; emits F2 metrics/spans.
**Depends on:** M4.

## M5b — FK-ordered apply + retry fallback
**Goal:** Multi-table referential correctness within a component.
**Deliverables:** **per-component transactional apply** — upserts parent→child, deletes child→parent
(§3.3, §8.1); **retry fallback** for cycles/self-refs/cross-pass deps (dirty row retried later) with a **defined
termination policy:** bounded exponential-backoff retries; on exhaustion, **halt the component
loud + observable per the §4.2 error policy** (the row stays dirty, no skip/corruption), never
infinite thrash.
**Acceptance (matrix):** multi-table component with mixed inserts/deletes stays referentially
consistent at the target; an FK cycle converges via retry; a deliberately unsatisfiable dependency
**exhausts the bounded retry and halts the component with a loud error** (no thrash, no skip);
per-component transaction bounded by churn/pass; emits F2 metrics/spans.
**Depends on:** M5a.

## M5c — Bloat control: purge + bounded retention + forced reseed
**Goal:** Protect a source we may not own; survive a stalled xmin horizon.
**Pre-work (design artifact, delegate to oracle — Momus B3):** the **reseed state machine** —
exact sequence (mark needs-reseed → purge over-cap deltas → reset track for that target only →
re-run initial copy for that target only → resume streaming) with a stated invariant for **why no
delta is dropped across the handoff** (re-derives §4 capture-first + §3.3 crash-safety together).
**Deliverables:** **batched consumption-gated purge** + aggressive autovacuum (§3.4); **bounded
retention (age+size) + forced reseed** of over-cap targets; partition+DROP decision for PG≥10
(decide here whether it's the *default recommendation* on modern sources — Momus M-4).
**Acceptance (matrix):**
- Convergence **after reseed under continuous source writes** (not merely "bloat bounded").
- **xmin-horizon stall (Momus M-4):** a long idle-in-transaction session pins xmin → batched-DELETE
  bloat grows AND is surfaced loudly in telemetry; bounded-retention/reseed still protects the source.
- Over-cap target → reseed triggers and bounds source growth; emits F2 metrics/spans.
**Depends on:** M5b.

## M6 — Observability finalization
**Goal:** Verify the F2 contract end-to-end and add the operator surface.
**Deliverables:** confirm Prometheus `/metrics` (throughput, lag, queue depth, per-table progress,
**per-target reachability gauge, delta backlog & oldest-unconsumed age**, purge rate, reseed
events); OTel traces+metrics; slog; **health/status HTTP API** + CLI `status`; the
**target-unreachable+delta-growth-across-logs+metrics+traces** contract (§3.4/§10).
**Acceptance:** scrape asserts the **named** series exist with expected labels; a drain pass against
a downed target yields a **span with error status + backlog attributes**, **escalating WARN→ERROR
logs toward the retention cap**, AND the reachability gauge + backlog series — all three channels;
status API reports phase/lag/progress.
**Depends on:** M5c (instrumentation woven through M2–M5* via F2).

## M7 — Daemon, CLI & multi-sync orchestration
**Goal:** Real daemon, multiple syncs, clean lifecycle.
**Deliverables:** single daemon managing **multiple named syncs**, goroutine **worker pools**,
**graceful shutdown** (SIGTERM drain+checkpoint — distinct guarantee from M4/M5 *crash* safety;
both required — Momus m-4); CLI `run`, `validate`, `status`, `capture install|remove`, `reseed`.
**Acceptance:** ≥2 concurrent syncs on the matrix; SIGTERM drains+checkpoints cleanly; ungraceful
kill (separately) still resumes; CLI exercises the full lifecycle.
**Depends on:** M6 (for status surface), M5c.

## M8 — Packaging & docs (feature-complete)
**Goal:** Installable and documented (feature-complete, not yet "shippable" — gated by M9).
**Deliverables:** single **static pure-Go binary** (no CGO; pgx is pure Go), sample **systemd**
unit, **Docker image**, README + ops docs + **example configs** + **exact grant SQL** (§12).
**Acceptance:** binary runs via systemd sample on a clean box; docker image replicates in a compose
demo; quickstart works end-to-end.
**Depends on:** M7.

## M9 — Hardening & E2E matrix (shippable gate)
**Goal:** Confidence under faults, scale, version spread. **Passing M9 = shippable.** M9 is the
**consolidated gate** — it runs the full matrix + at-scale fault injection, not a re-derivation of
the per-milestone fault tests already in M4/M5* (those remain each milestone's own bar).
**Deliverables:** full **integration matrix** (F3-old→modern); **fault-injection** suite (crash,
target down, **slow-but-reachable target**, xmin-horizon); **throughput/perf benchmarks with
recorded thresholds + non-regression assertions** (not "sanity"); expanded fidelity corpus.
**Acceptance:** matrix green; perf numbers recorded with explicit pass/fail thresholds; fidelity
corpus passes; retention/reseed proven to bound source growth under a stalled xmin horizon at scale;
slow-but-reachable target handled via backpressure without unbounded memory.
**Depends on:** M7 (parallel with M8).

---

## Critical path & sequencing (revised)
M0 → M0.5 → M1 → {M2, M3} → M4 → **M5a (true first end-to-end)** → M5b → M5c → M6 → M7 → {M8, M9}.
**F2 observability contract is an M0 dependency for everyone**; M2–M5* emit against it; **M6 verifies**.
Earliest honest end-to-end demo: **end of M5a**. Feature-complete: **M8**. Shippable: **M9 gate**.

## Open questions — now resolved within milestones (was CLAUDE.md §15)
- Cyclic/self-ref FK initial copy → **RESOLVED** (CLAUDE.md §4.1/§14): nullable → NULL-then-fill;
  DEFERRABLE → `SET CONSTRAINTS DEFERRED`; `NOT NULL` non-deferrable → M1 pre-flight fails loud.
  M4 acceptance asserts correctness across all three cases.
- Reseed coordination → **design artifact precedes M5c**; acceptance asserts convergence-after-reseed under writes.
- Retention-cap defaults & partition cadence; partition+DROP-as-default-on-PG≥10 → **decided in M5c**, tuned in M9.
- Binary-COPY compatibility gate → optional; text is default; **deferred to M9** if pursued.

## How we execute (Sisyphus)
Each milestone is a task group; sub-tasks delegated to specialists (oracle for the reseed state
machine + concurrency/SQL-portability calls; document-writer for M8). A milestone closes only when
its acceptance tests pass on the matrix. Plan/decision changes are mirrored back into `CLAUDE.md`
(F4 schema-versioning is being recorded there now).

## Momus review disposition
**Pass 1 — READY-WITH-REVISIONS.** All 8 must-fix items addressed: B1 (M5→M5a/b/c), B2 (commit-order
test in M5a, concrete scenario), B3 (reseed state-machine pre-M5c), B4 (cyclic-FK strategy in M4),
M-1 (M0.5 config), M-2 (TLS/secrets in M0.5/M1), M-3 (F4 versioning in M0/M2/M3 + CLAUDE.md), M-5
(F2 contract in M0), M-7 (named race tests in M5a). Folded majors: M-4 (xmin test in M5c), M-6
(backpressure in M4/M5 + slow-target in M9). Minors m-1..m-7 all incorporated.
**Pass 2 — STILL-NEEDS-REWORK on B4, now fixed.** Momus found B4 papered over: NULL-then-fill can't
handle a `NOT NULL` cyclic FK (can't insert NULL; can't touch constraints), and the plan claimed
"resolved" while CLAUDE.md still listed it open. **Fix:** case-based strategy — nullable →
NULL-then-fill; DEFERRABLE → `SET CONSTRAINTS DEFERRED`; `NOT NULL` non-deferrable → **M1 pre-flight
blocks loud**; CLAUDE.md §4.1/§14 updated and §15 marked resolved (change-discipline rule honored);
M4 acceptance covers all three cases. Minor folds: M5b retry termination policy + bound; F4 one
shared migration-runner instantiated twice; M9 annotated as the consolidated gate.
