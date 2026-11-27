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

## `verify <config> [options]`

```sh
replicare verify config.yml                                  # every row of every sync
replicare verify config.yml --sync pg-pipeline               # one sync
replicare verify config.yml --sync pg-pipeline --table public.orders --table public.customers
replicare verify config.yml --sample 50                      # quick check: 50 random ranges per table
replicare verify config.yml --json                           # machine-readable
replicare verify config.yml --watch 30m                      # re-check every 30 minutes until Ctrl-C
```

This is a read-only check that a source and its targets hold the same data. It writes nothing
and installs nothing, so it's safe to run against a source we may not own, and against live
targets.

| Option | Default | Meaning |
|---|---|---|
| `--sync <name>` | every sync | Check one sync only |
| `--table <schema.table>` | every table | Check only this table; repeat it for more. Needs `--sync` when the config has more than one sync |
| `--sample <n>` | off | Check `n` random key ranges per table instead of every row |
| `--chunk-rows <n>` | `10000` | Rows in each key range |
| `--pause <dur>` | `100ms` | Wait between key ranges, to keep the load on the source down. `0` means no wait |
| `--recheck-after <dur>` | `30s` | Wait before looking up rows that differed a second time. `0` means look them up straight away |
| `--json` | off | Print the report as JSON |
| `--watch <dur>` | off | Run the check again on this interval until `SIGINT` |

### How a Postgres or MySQL table is checked

On Postgres and MySQL, `verify` compares a table one key range at a time, rather than in one pass
over the whole table. The source picks where each range ends(the key `--chunk-rows` rows further on,
read from the table's key index), then the source and every target hash the rows in that same
range with the same query and compare the results. Each query reads only the rows in its range,
so no query holds a long scan or a long-running snapshot on the source, and a large table is
checked bit by bit with a short pause in between.

When a range differs, `verify` fetches that range's rows on both sides, with a hash per row, to
find exactly which keys differ. It then waits `--recheck-after` and looks those keys up again by
key on both sides. A row that was just on its way to the target matches by then and isn't
reported, so a busy, streaming sync can be checked without false alarms. What still differs is
reported by key, as one of:

- `missing` - on the source, not on the target.
- `extra` - on the target, not on the source.
- `different` - on both, with different values.

A full check reads every row, so it takes a while on a large table; it prints a progress line
every 30 seconds while a table is being checked. `--sample <n>` is the quick version: it picks
`n` random starting keys and compares the `--chunk-rows` rows after each one. On Postgres the
starting keys come from `TABLESAMPLE`, so finding them reads only a small share of the table. On
MySQL each one is a single lookup on the key index: a random value between the smallest and
largest key(for number, text and binary keys), then the first key at or after it. For a MySQL
table whose key starts with another type(a date, a decimal), a random position in key order is
used instead, which reads through the key index up to that position and is slower on a large
table. A sampled check can only find
differences in the rows it looked at, so an `ok` means "nothing wrong in the sample", not
"nothing wrong anywhere". In sampled mode the row counts in the report are the rows that were
checked, not the size of the table.

Tables are walked in the source's key order. If the target sorts text keys differently(a
different collation), rows can land in different ranges on the two sides; the key-by-key
re-check sorts that out, so it isn't reported as drift, but it does make the check slower for
such tables.

Redis keys are checked whole: one count and one content hash for the whole unit on each side,
with no key list.

### Reading the report

For each table, `verify` prints how it was checked(`MODE`: `chunked`, `sampled` or `whole`), the
row counts, and one of:

- `ok` - the rows match.
- `drift-count` - rows are missing from the target, or the target has extra rows.
- `drift-checksum` - the same rows are on both sides, but some values differ.
- `count-only-ok` - the counts match, but there was no content hash to compare. Every engine
  `replicare` ships today hashes content, so this shouldn't normally appear.
- `error` - a table couldn't be compared(for example, it's missing on the target).

A Postgres table that differs also lists how many rows are missing, extra and different, followed
by up to 20 of their keys(a composite key is shown with its parts comma-separated). `--json`
carries the same keys, along with `mode`, `chunks`, `missing_on_target`, `extra_on_target` and
`different`. When a table differs in more rows than `verify` keeps track of(10,000 per table and
target), `truncated` is set, and the counts cover only the rows it tracked.

The content hash is order-independent and works on a name-matched list of columns, so it isn't
thrown by differences in physical column order. Postgres and MySQL hash each row's content. Redis
hashes each key's name, type and value, so it catches a changed value as well as missing / extra
keys. Values are read in their logical form rather than as raw `DUMP` bytes; transport is a
value-faithful `DUMP` then `RESTORE`, but the bytes themselves can differ between Redis versions
for the same value, so hashing them would report drift that isn't there. Key expiry times aren't
part of the hash, since they're replicated as time remaining and differ slightly by design.
Stream entries are compared, but stream consumer groups aren't.

The exit codes make `replicare verify config.yml` scriptable as a convergence gate. `0` means
every checked sync matched, `1` means drift or an endpoint that couldn't be reached, and `2`
means a usage / config error. In `--watch` mode, the exit code reflects the last completed pass.

### Load on the source

A full check reads every replicated row on the source once, through short range queries on the
key index. With the defaults, that's 10,000 rows per query and a 100 ms pause between queries.
A larger `--pause` or a smaller `--chunk-rows` spreads the reads out further; `--sample` reads a
small fraction of each table. On a very large database, a good habit is a `--sample` run first,
then a full check of the tables that matter most, with `--table`, at a quiet time.

> ⚠️ Redis is hashed in one pass, which reads every selected key; on a large keyspace it's best
> run at a quiet time.

> ⚠️ Rows still waiting in the delta queue differ until they're applied. The re-check handles
> rows that arrive within `--recheck-after`; a sync that's far behind will still show drift, and
> the fix is to let it catch up(see `replicare status`) and check again.

## `capture install|remove <config> [--sync <name>]`

```sh
replicare capture install config.yml            # all syncs
replicare capture remove config.yml --sync app-to-warehouse
```

This installs or removes the trigger-based capture machinery on a sync's source, over its
selected tables. `run` installs capture automatically, so this command is for pre-provisioning
or for manual teardown. It works on a paused sync too. Installing capture ahead of time is what
lets a sync start later without a copy: see
[`skip_initial_copy`](configuration.md#starting-without-a-copy-skip_initial_copy) and
[cutting over](cutover.md#on-the-day).

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

## `indexes restore <config> [--sync <name>] [--target <name>]`

```sh
replicare indexes restore config.yml                          # every sync in the config
replicare indexes restore config.yml --sync app-to-warehouse --target warehouse
```

This builds again, straight away, every target index `replicare` took down for an initial copy
and hasn't built yet(see
[target indexes during the first copy](operations.md#target-indexes-during-the-first-copy)). The
daemon does this by itself once a sync is streaming, so the command is for a sync that won't run
again: one abandoned partway through its copy, or one whose state store is about to be dropped,
which is where the saved definitions live. It builds one index at a time without blocking writes,
prints a line as each one finishes, and uses the sync's `defer_indexes` build settings.

It refuses a sync that a running daemon owns, since the daemon builds these itself and two builds
of the same index would collide; stop the daemon first(on Kubernetes, scale it to 0 replicas).
Ctrl-C stops it partway, safely: the index being built is left unusable, and the next run drops it
and builds it again.

The exit codes are `0` when every index is built(or there was nothing to build), `1` when some
couldn't be built or a daemon owns the sync, and `2` for a usage or config error.

## `triggers restore <config> [--sync <name>] [--target <name>]`

```sh
replicare triggers restore config.yml                          # every sync in the config
replicare triggers restore config.yml --sync app-to-warehouse --target warehouse
```

This switches back on, straight away, every target trigger `replicare` switched off so that it
wouldn't fire on `replicare`'s writes, each in the mode it had before(see
[the target's own triggers](operations.md#the-targets-own-triggers)). It prints a line per
trigger. A trigger dropped since then is reported and forgotten.

The daemon does the same by itself when it starts with the sync paused(`enabled: false`), so the
command is for doing it at a moment of our choosing, or for a sync that won't run again. It
refuses a sync that a running daemon owns, since that daemon needs the triggers off while it
writes; pause the sync, or stop the daemon(on Kubernetes, scale it to 0 replicas), first.

The exit codes are `0` when every trigger is back on(or there was nothing to do), `1` when some
couldn't be switched back on or a daemon owns the sync, and `2` for a usage or config error.

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
> [active-active, Rule 1](failover.md#rule-1-use-globally-unique-ids).

## Signals

`run` responds to `SIGTERM` / `SIGINT` with a graceful shutdown; it drains the in-flight pass,
checkpoints, and exits 0.

An ungraceful kill is safe too. State is checkpointed and applies are idempotent, so a fresh
`run` resumes and converges without duplicates.
