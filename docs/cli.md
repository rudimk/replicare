# CLI Reference

This page is the reference for the `replicare` command line, which runs the daemon and is also
how an operator checks on it, validates a config, compares a source with its targets, and
handles one-off jobs like a reseed. It goes through each command in turn, with its flags, what
it does, and what its exit codes mean.

Most commands take the config file as an argument:

```
replicare <command> [args]
```

The exit codes are the same across commands. `0` means success, and `1` means a runtime error
*or* a pre-flight BLOCK(see `validate`). `2` means a usage / config error - bad flags, or a
config that won't load, such as an unknown engine, an unknown field, an unset `${VAR}`, or a
cross-engine sync.

## `help` / `version`

```sh
replicare help          # also: --help, -h — prints the command usage
replicare version       # also: --version, -v — version, commit, build date, Go version
```

## `validate <config>`

```sh
replicare validate config.yml
```

This loads the config and runs pre-flight for every(sync, target) pair. It connects read-only,
introspects both sides, and classifies every column pair: identical / widening is ok,
risky / lossy is a warning, and an incompatible or missing type is a BLOCK. It also reports FK
components, a giant-component warning, dangling FK edges, and tables skipped for having no
key. It makes *no* changes, and every BLOCK finding needs fixing before running.

The exit codes are meaningful for scripting. `0` means clean(ok / warn only), `1` means at
least one BLOCK finding, which is a clean "incompatible" verdict and *not* a crash, and `2`
means the config didn't load or a connection failed. That means
`replicare validate ... && replicare run ...` only starts the daemon after a block-free
pre-flight.

## `run <config>`

```sh
replicare run config.yml
```

This runs the daemon. It starts the observability endpoints, then brings up and streams every
sync until it gets `SIGINT`/`SIGTERM`. On a signal it shuts down gracefully; it finishes the
in-flight drain pass, checkpoints, and exits `0`.

Each sync runs under a single-active ownership lock, so a second daemon pointed at the same
sync stands by rather than competing with the first. State is checkpointed, so a restarted
process resumes from where it left off.

## `status <config> [--json] [--sync <name>] [--no-live] [--watch <dur>]`

```sh
replicare status config.yml                    # human-readable table, LIVE by default
replicare status config.yml --json             # machine-readable
replicare status config.yml --sync pg-pipeline # one sync only
replicare status config.yml --no-live          # state store only (no source/target connections)
replicare status config.yml --watch 5s         # re-render every 5s until Ctrl-C
```

Each sync prints a one-line health headline, followed by a grid with a row per(table, target),
read from the state store - so it works whether or not a daemon is running:

```
sync: app-to-warehouse
  streaming · last pass 2s ago · last applied 40s ago (1240 rows total) · backlog 0 (caught up)
  TABLE           COPY  ...  SEEN  ...  LAST_SYNC   ROWS   RESEED
  public.orders   done  ...  2s    ...  40s ago     1240   -
```

The grid shows two *different* time signals, and telling them apart is what makes a caught-up
sync legible.

`SEEN` is the time since the last healthy streaming pass, which makes it a liveness heartbeat.
The daemon refreshes it on every pass whether or not any data moved, so a small `SEEN` means
the sync is running and current, while a large `SEEN` means the daemon is stopped or stalled.
(This is what the old `LAG` column was, renamed; it was never a measure of how far behind a
target is.)

`LAST_SYNC` / `ROWS` are the time since the last pass that actually applied rows to this(target,
table) pair, and how many rows it applied. A healthy, idle sync shows a recent `SEEN` but a
growing `LAST_SYNC`, which is normal - there's simply nothing to apply - and isn't lag. Both
show `-` until the first pass that moves data. They come from the state store, so they show
with `--no-live` too.

The headline rolls these up across the sync: its phase, the most recent `SEEN`, the most recent
data movement along with the total rows applied, and(when live) the backlog rollup.

`status` is live by default. Unless `--no-live` is passed, it also connects to the sync's
source and targets to add signals that only live in the databases, which is the visibility
an operator would otherwise get from Grafana:

- `SRC_ROWS` / `TGT_ROWS` are the live source and target row counts(key counts on Redis).
  During the initial copy, `TGT_ROWS` climbs towards `SRC_ROWS`; while streaming, the two track
  each other.
- `BACKLOG` is the per-target unconsumed delta backlog, shown as `rows (oldest-age)`. It
  distinguishes three states. `0` means caught up, which is healthy and the common steady
  state. `N (age)` means that many deltas are waiting. `-` means *unknown*, not zero - the
  signal couldn't be read(a table that isn't captured, Redis's capture-less engine, or a
  transient error), and a `live: partial (…)` note says why.

Live collection is best-effort and read-only, and installs nothing. If the source or a target
is unreachable, the row still renders from the state store, and a `live: partial (…)` note
explains what was missing. `--no-live` skips every source / target connection, which is
useful with only state-store access, or for the cheapest possible check.

`--watch <dur>` re-renders on the given interval(for example `10s` or `1m`) until `SIGINT`;
combine it with `--json` to stream snapshots.

## `verify <config> [--json] [--sync <name>] [--watch <dur>]`

```sh
replicare verify config.yml                    # all syncs
replicare verify config.yml --sync pg-pipeline # one sync
replicare verify config.yml --json             # machine-readable
replicare verify config.yml --watch 30s        # re-check every 30s until Ctrl-C
```

This is a read-only convergence spot-check between a source and its targets. For every
replicated unit, it counts and content-fingerprints the source and each target, then compares
them. It writes nothing and installs nothing, so it's safe to run against a source we may not
own, and against live targets. For each table it reports one of:

- `ok` - the row counts *and* the content checksums match.
- `drift-count` - the row counts differ(the target is missing rows / has extra rows).
- `drift-checksum` - the counts match but the content differs(a value has diverged).
- `count-only-ok` - the counts match, but there was no content hash to compare. Every engine
  `replicare` ships today hashes content, so this shouldn't normally appear.
- `error` - a table couldn't be compared(for example, it's missing on the target).

The content fingerprint is an order-independent hash over a name-matched column projection, so
it isn't thrown by differences in physical column order, and it streams in constant server
memory. The relational engines(Postgres and MySQL) hash row content. Redis hashes each key's
name, type and value, so it catches a changed value as well as missing / extra keys. Values are
read in their logical form rather than as raw `DUMP` bytes; transport is a value-faithful `DUMP`
then `RESTORE`, but the bytes themselves can differ between Redis versions for the same value, so
hashing them would report drift that isn't there. Key expiry times aren't part of the hash, since
they're replicated as time remaining and differ slightly by design. Stream entries are compared,
but stream consumer groups aren't.

The exit codes make `replicare verify config.yml` scriptable as a convergence gate. `0` means
every selected sync converged, `1` means drift or an endpoint that couldn't be reached, and `2`
means a usage / config error. In `--watch` mode, the exit code reflects the last completed pass.

> ⚠️ Replication has lag, so a `drift-count`/`drift-checksum` straight after a burst of source
> writes usually just means the target hasn't caught up yet. Re-run it(or use `--watch`);
> drift that persists is the real signal.

## `capture install|remove <config> [--sync <name>]`

```sh
replicare capture install config.yml            # all syncs
replicare capture remove config.yml --sync app-to-warehouse
```

This installs or removes the trigger-based capture machinery on a sync's source, over its
selected tables. `run` installs capture automatically, so this command is for pre-provisioning
or for manual teardown.

> ⚠️ `install` only needs `TRIGGER`(plus `SELECT`/`USAGE` and schema creation), but `remove`
> also needs table ownership to drop the triggers. If the `replicare` role doesn't own the
> tables, removing the trigger fails(and is reported), though `replicare`'s own delta / track
> tables are still dropped.

## `reseed <config> --sync <name> --target <name>`

```sh
replicare reseed config.yml --sync app-to-warehouse --target warehouse
```

This flags a target for a full re-copy. Rather than acting directly, it signals the *running*
daemon through the state store to re-copy that target from the current source state on its
next pass, then resume streaming; this way it respects single-active ownership. Use it after
the target has diverged out-of-band, or to recover a target that's fallen behind.

If the target has no cursors yet(because `reseed` was run before the sync's first run),
there's nothing to flag. `replicare` reports that the target will do a full copy on its first
run anyway, and exits `0`. That's expected, and isn't a failure.

## `reseed-sequences <config> --sync <name> [--target <name>] [--dry-run]`

```sh
replicare reseed-sequences config.yml --sync app-to-warehouse            # all targets of the sync
replicare reseed-sequences config.yml --sync app-to-warehouse --target warehouse
replicare reseed-sequences config.yml --sync app-to-warehouse --dry-run  # report, write nothing
```

`replicare` replicates row *values* faithfully, but not the sequence *object's* counter, since
it only copies data. For a passive replica that's harmless, but on a promoted node `nextval()`
would collide with the copied rows. This command advances each replicated table's owned
identity / serial / `AUTO_INCREMENT` counter on a promoted one-way(DR) target to `max(id)+1`,
so after a failover cutover the node never re-issues an id that's already in the replicated
data. Postgres uses `setval`, and MySQL uses `ALTER TABLE … AUTO_INCREMENT`. Tables with no
owned counter(a UUID, natural or composite PK) are skipped, and `--dry-run` reports the target
values without writing anything.

It's not the same thing as `reseed`. `reseed` re-copies a target's *data*, while
`reseed-sequences` only bumps its sequence counters.

The order matters: stop replication into the node, run this, and *then* open the node up to
writes, since a write in the gap could still grab a colliding id.

It needs a DR-only grant beyond the usual DML set - Postgres `UPDATE` on each sequence, MySQL
`ALTER` on each table. It only reads the target's own `max(id)`, and needs no privilege on the
source. See [sequences & identity counters in operations](operations.md#sequences--identity-counters-dr--promotion).

The exit codes are `0` when it's done, `1` for a runtime error(an unreachable target, a missing
grant, or an engine without the capability), and `2` for a usage / config error, or a refused
mesh target.

> ⚠️ This command is for passive / one-way / DR setups only, and it refuses a target that's a
> member of an active-active cluster. There, the same `max(id)+1` collapses every node onto one
> counter and silently drops rows through last-write-wins. In active-active, allocating ids is a
> schema concern, and the answer is globally-unique keys(UUID v7 / ULID); see
> [active-active, Rule 1](active-active.md#rule-1-use-globally-unique-ids).

## Signals

`run` responds to `SIGTERM` / `SIGINT` with a graceful shutdown; it drains the in-flight pass,
checkpoints, and exits 0.

An ungraceful kill is safe too. State is checkpointed and applies are idempotent, so a fresh
`run` resumes and converges without duplicates.
