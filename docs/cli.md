# CLI reference

Most commands take the config file as an argument. Exit codes: `0` success,
`1` runtime error **or a pre-flight BLOCK** (see `validate`), `2` usage/config
error (bad flags, or a config that won't load — unknown engine, unknown field,
unset `${VAR}`, cross-engine sync).

```
replicare <command> [args]
```

## `help` / `version`

```sh
replicare help          # also: --help, -h — prints the command usage
replicare version       # also: --version, -v — version, commit, build date, Go version
```

## `validate <config>`

```sh
replicare validate config.yml
```

Loads the config and runs **pre-flight** for every (sync, target): connects
read-only, introspects both sides, and classifies every column pair —
identical/widening (ok), risky/lossy (warn), incompatible or missing type
(**block**). Also reports FK components, a giant-component warning, dangling FK
edges, and skipped no-key tables. Makes **no changes**. Fix all BLOCK findings
before running.

**Exit codes are meaningful for scripting:** `0` = clean (ok/warn only), `1` = at
least one **BLOCK** finding (a clean "incompatible" verdict, *not* a crash), `2` =
the config didn't load or a connection failed. So `replicare validate ... && replicare run ...`
gates a start on a block-free pre-flight.

## `run <config>`

```sh
replicare run config.yml
```

Runs the daemon: starts the observability endpoints, then brings up and streams
every sync until `SIGINT`/`SIGTERM`. On a signal it shuts down **gracefully** —
finishes the in-flight drain pass, checkpoints, and exits `0`.

Each sync runs under a single-active ownership lock; a second daemon pointed at
the same sync stands by. State is checkpointed, so a restarted process resumes
from where it left off.

## `status <config> [--json] [--sync <name>] [--no-live] [--watch <dur>]`

```sh
replicare status config.yml                    # human-readable table, LIVE by default
replicare status config.yml --json             # machine-readable
replicare status config.yml --sync pg-pipeline # one sync only
replicare status config.yml --no-live          # state store only (no source/target connections)
replicare status config.yml --watch 5s         # re-render every 5s until Ctrl-C
```

Reports, per sync/target/table: phase (initial-copy vs streaming), lag (cursor
age), needs-reseed flags, and recent events — read from the **state store**, so it
works whether or not a daemon is running.

**Live by default.** Unless you pass `--no-live`, `status` also connects to the
sync's source and targets to add signals that live only in the databases — the
visibility you'd otherwise get from Grafana:

- **`SRC_ROWS` / `TGT_ROWS`** — live source and target row counts (Redis: key
  counts). During initial copy `TGT_ROWS` climbs toward `SRC_ROWS`; while streaming
  they track each other.
- **`BACKLOG`** — the per-target unconsumed **delta backlog** as `rows (oldest-age)`
  (`0` when caught up), the headline "how far behind is streaming / is the source
  footprint healthy?" signal. Redis has no durable source-side queue, so it reports
  `-`.

Live collection is **best-effort and read-only** (it installs nothing): if the
source or a target is unreachable, the row still renders from the state store and a
`live: partial (…)` note explains what was missing. `--no-live` skips all
source/target connections (useful if you only have state-store access, or want the
cheapest possible check).

`--watch <dur>` re-renders on the given interval (e.g. `10s`, `1m`) until
`SIGINT`; combine with `--json` to stream snapshots.

## `verify <config> [--json] [--sync <name>] [--watch <dur>]`

```sh
replicare verify config.yml                    # all syncs
replicare verify config.yml --sync pg-pipeline # one sync
replicare verify config.yml --json             # machine-readable
replicare verify config.yml --watch 30s        # re-check every 30s until Ctrl-C
```

A **read-only** source↔target convergence spot-check. For every replicated unit it
counts and content-fingerprints the source and each target and compares. It writes
nothing and installs nothing, so it is safe to run against a source you may not own
and against live targets. Per table it reports one of:

- **`ok`** — row counts *and* content checksums match.
- **`drift-count`** — row counts differ (the target is missing/extra rows).
- **`drift-checksum`** — counts match but content differs (a value diverged).
- **`count-only-ok`** — counts match; the engine compares membership only (see below).
- **`error`** — a table could not be compared (e.g. missing on the target).

The content fingerprint is an **order-independent** hash over a name-matched column
projection, so it is robust to physical column-order differences and streams in
constant server memory. Relational engines (Postgres, MySQL) hash row content;
**Redis** compares the **key set** (count + membership), which catches missing/extra
keys — the dominant Redis divergence — since transport is value-faithful
`DUMP`→`RESTORE` (per-value Redis content diffing is a documented follow-up).

**Exit codes:** `0` = every selected sync converged; `1` = drift or an endpoint
could not be reached; `2` = usage/config. So `replicare verify config.yml` is
scriptable as a convergence gate. In `--watch` mode the exit code reflects the last
completed pass.

> Note: replication has lag — a `drift-count`/`drift-checksum` immediately after a
> burst of source writes usually just means the target hasn't caught up yet.
> Re-run (or use `--watch`); persistent drift is the real signal.

## `capture install|remove <config> [--sync <name>]`

```sh
replicare capture install config.yml            # all syncs
replicare capture remove config.yml --sync app-to-warehouse
```

Installs or removes the trigger-based CDC machinery on a sync's source, over its
selected tables. `run` installs capture automatically, so this is for
pre-provisioning or manual teardown.

**Note (§12):** `install` needs only `TRIGGER` (plus `SELECT`/`USAGE` and schema
creation). `remove` additionally needs **table ownership** to drop the triggers —
if the replicare role doesn't own the tables, removal of the trigger fails
(reported), though replicare's own delta/track tables are still dropped.

## `reseed <config> --sync <name> --target <name>`

```sh
replicare reseed config.yml --sync app-to-warehouse --target warehouse
```

Flags a target for a full re-copy. It signals the **running** daemon (via the
state store) to re-copy that target from current source state on its next pass,
then resume streaming — honoring single-active ownership rather than acting
directly. Use it after out-of-band divergence, or to recover a target that fell
behind.

If the target has **no cursors yet** (you ran `reseed` before the sync's first
run), there is nothing to flag: replicare reports that the target will full-copy on
its first run anyway and exits `0`. That is expected, not a failure.

## `reseed-sequences <config> --sync <name> [--target <name>] [--dry-run]`

```sh
replicare reseed-sequences config.yml --sync app-to-warehouse            # all targets of the sync
replicare reseed-sequences config.yml --sync app-to-warehouse --target warehouse
replicare reseed-sequences config.yml --sync app-to-warehouse --dry-run  # report, write nothing
```

Advances each replicated table's owned identity/serial/`AUTO_INCREMENT` counter on a
**promoted one-way (DR) target** to `max(id)+1`, so after a failover cutover the node
never re-issues an id already present in the replicated data. replicare replicates row
*values* faithfully but not the sequence *object's* counter (data-only, `CLAUDE.md`
§7); for a passive replica that is harmless, but a promoted node's `nextval()` would
collide with the copied rows. Postgres uses `setval`, MySQL `ALTER TABLE …
AUTO_INCREMENT`. Tables with no owned counter (UUID / natural / composite PK) are
skipped; `--dry-run` reports the target values and writes nothing.

- **Not the same as `reseed`.** `reseed` re-copies a target's *data*; `reseed-sequences`
  only bumps its sequence counters.
- **Run order:** stop replication into the node, run this, *then* open it to writes — a
  write in the gap could still grab a colliding id.
- **Passive / one-way / DR only.** It **refuses** a target that is an active-active
  cluster member: there the same `max(id)+1` collapses every node onto one counter and
  silently drops rows via last-write-wins. Active-active id allocation is a schema
  concern — use globally-unique keys (UUID v7 / ULID); see
  [multi-master](multi-master.md).
- **Privilege:** a DR-only grant beyond the DML set — Postgres `UPDATE` on each
  sequence, MySQL `ALTER` on each table (reads only the target's own `max(id)`; no
  source privilege). See [operations → Sequences & identity counters](operations.md#sequences--identity-counters-dr--promotion).
- **Exit codes:** `0` done; `1` runtime error (unreachable / missing grant / engine
  without the capability); `2` usage/config, or a refused mesh target.

## Signals

`run` responds to:

- `SIGTERM` / `SIGINT` — graceful shutdown (drain in-flight pass, checkpoint, exit 0).

An ungraceful kill is also safe: state is checkpointed and applies are
idempotent, so a fresh `run` resumes and converges without duplicates.
