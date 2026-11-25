# Failover and High Availability

This page covers keeping an application's database available when a database fails: holding
copies elsewhere, moving the application to one of them, and bringing the failed database back.
We describe three layouts, from simplest to most demanding:

1. [One active, one passive](#one-active-one-passive): one database takes writes, and a single
   copy stands by.
2. [One active, several passive](#one-active-several-passive): one database takes writes, and
   several copies stand by, fed directly or in a chain.
3. [Several active](#several-active): every database takes writes at the same time, and
   `replicare` keeps them all carrying the same data.

*Active* means the application writes to it; *passive* means it only receives changes from
`replicare`. In the first two layouts only one database is written to at any moment, so they work
with any schema `replicare` can replicate, including tables that count up their own integer ids.
The third has strict rules for the schema and the application, covered in
[several active](#several-active).

## Choosing a layout

| Need | Layout |
|---|---|
| Somewhere to move the application if its database fails | [One active, one passive](#one-active-one-passive) |
| Standbys in several places, or a standby for the standby | [One active, several passive](#one-active-several-passive) |
| Writes accepted in several places at once, with a schema built for it | [Several active](#several-active) |

If the aim is only to survive a database failing, a passive layout is much simpler than several
active ones, and it has none of the schema rules: we'd recommend it unless the application really
has to write in more than one place at the same time.

> ⚠️ `replicare` copies a change a moment after it's committed, not as part of the commit. An
> unplanned failover in a passive layout therefore loses whatever the active database had
> committed but not yet sent: normally the last few seconds, and at most its delta backlog at
> that moment. Those changes aren't destroyed; they wait on the failed database, and
> [when it comes back](#when-the-old-active-database-comes-back) we show how to find them. If no
> committed write may ever go missing, even for a while, `replicare` isn't enough on its own; the
> database's built-in synchronous replication confirms each write on a copy before acknowledging
> it. A *planned* switchover loses nothing.

## Before any layout

- **Run `replicare` and its state store away from the databases it protects.** The daemon has to
  keep running when a database's site is down, and reach every other database. The
  `state_store` shouldn't live on any database in the layout; a small separate Postgres that can
  fail over on its own is the right home for it. If the state store were on the active database,
  losing that database would stop every sync. See
  [when `replicare` itself fails](#when-replicare-itself-fails).
- **Grant every database both sets of permissions.** A passive database is a target now and may
  be a source later, so its `replicare` user needs the source grants and the target grants
  (see [least-privilege grants](operations.md#least-privilege-grants)).
- **Keep id counters ahead on passive copies.** Set
  [`sync_sequences: true`](configuration.md#keeping-dr-counters-warm-sync_sequences) on every sync
  whose target might be promoted, so a promoted database's own counters(Postgres sequences, MySQL
  `AUTO_INCREMENT`) are already past the copied ids. That needs `UPDATE` on each sequence(Postgres)
  or `ALTER` on each table(MySQL) on the targets. It doesn't apply to several active, which uses
  globally-unique ids instead.
- **Leave out tables the application can do without.** Large append-only tables(logs, webhook
  payloads, audit trails) are often most of a database's size. Excluding them cuts the trigger work
  on the active database, the copy time and the disk on every copy, provided the application can
  run without their history after a failover. Framework bookkeeping tables(Rails'
  `schema_migrations` and `ar_internal_metadata`, PostGIS's `spatial_ref_sys`) should always be
  excluded, since each database runs its own migrations.
- **Order schema changes.** A database that's missing a column its source sends stalls that sync
  until the column is added. In a passive layout, every migration goes to the passive copies before
  the active database; in several active, to every node first. The application code that uses the
  change follows.
- **Keep each database in one config.** `replicare` keeps a queued change until every target
  reading it in the same config has it. A database read by two separate configs isn't covered:
  one config's syncs could clear changes the other's still need.

## One active, one passive

```
active ──► passive
```

### Setting it up

One sync, from the active database to the passive one:

```yaml
syncs:
  - name: active-to-passive
    source: active
    targets: [passive]
    include: ["public.*"]
    exclude: ["public.schema_migrations", "public.ar_internal_metadata", "public.spatial_ref_sys"]
    sync_sequences: true
```

It copies the passive database from the active one, then streams every change.

### Watching it

The passive copy's delta backlog(`replicare_delta_backlog_rows`,
`replicare_delta_oldest_unconsumed_age_seconds`) is how much an unplanned failover would lose
right now. Alert on it climbing, and on `replicare_target_up` dropping to `0`. `replicare status`
shows the same from the command line, and the
[Grafana dashboard](grafana-dashboard.md) has both.

### Planned switchover

For maintenance on the active database, or to move the active role on purpose, with nothing lost:

1. Stop writes to the active database, and wait until the sync's backlog is 0 and stays there.
2. Follow [cutting over, with a way back](operations.md#cutting-over-with-a-way-back). It moves
   the active role and sets up the sync running the other way without copying anything, so the
   old active database becomes the new passive copy straight away.

### Unplanned failover

The active database is down:

1. **Fence it.** Make sure nothing can write to it: cut the application's access by network,
   credentials or DNS. If it came back while some application servers still used it, both
   databases would take writes, and those can't be reconciled.
2. **Pause `active-to-passive`**(`enabled: false`, then restart the daemon) before the old active
   database can come back. A sync still switched on would reconnect, re-read the changes stuck in
   its queue from the old database, and write them over newer data on the new one.
3. **Note what was missed:** the backlog for `active-to-passive` just before the failure, from the
   metrics above.
4. **Check the counters.** With `sync_sequences` on, they're already ahead; to be sure, run
   [`replicare reseed-sequences <config> --sync active-to-passive`](cli.md#reseed-sequences-config---sync-name---target-name---dry-run),
   which only reads and changes the passive database.
5. **Point the application at the passive database.** It's the active one now.

### When the passive copy fails

Nothing changes for the application. Changes for the passive copy wait on the active database
while it's down, and the sync catches up on its own when it returns. If it stays down past the
retention limit(about a day by default), the active database stops holding changes for it and
marks it to be copied again from scratch when it's back; that protects the active database's
disk. To replace a copy that's gone for good, point the sync at a new, empty database.

### When the old active database comes back

After an unplanned failover the old active database is behind(everything written since went to
the new one) and slightly ahead(the changes it took but never sent). It can't simply be resumed.

1. **Keep it fenced from the application.**
2. **List the changes that never got out.**

   On Postgres, this prints, per table, how many changes were still queued, their time range, and
   up to 20 of them(`I`/`U`/`D` for insert, update or delete, then the first key column):

   ```sql
   DO $$
   DECLARE r record; n bigint; first_at timestamptz; last_at timestamptz; keys text;
   BEGIN
     FOR r IN SELECT rel_id, schema_name, table_name FROM replicare.captured ORDER BY 2, 3 LOOP
       EXECUTE format(
         'SELECT count(*), min(rc_at), max(rc_at) FROM replicare.%I d
            WHERE NOT EXISTS (SELECT 1 FROM replicare.%I t WHERE t.delta_id = d.delta_id)',
         'delta_' || r.rel_id, 'track_' || r.rel_id) INTO n, first_at, last_at;
       IF n > 0 THEN
         EXECUTE format(
           'SELECT string_agg(rc_op || '':'' || k1::text, '', '') FROM (
              SELECT rc_op, k1 FROM replicare.%I d
              WHERE NOT EXISTS (SELECT 1 FROM replicare.%I t WHERE t.delta_id = d.delta_id)
              ORDER BY delta_id LIMIT 20) s',
           'delta_' || r.rel_id, 'track_' || r.rel_id) INTO keys;
         RAISE NOTICE '%.%: % changes not sent (% to %): %',
           r.schema_name, r.table_name, n, first_at, last_at, keys;
       END IF;
     END LOOP;
   END $$;
   ```

   On MySQL, this lists the count and time range per table; each table's queued keys are in
   `replicare.delta_<rel_id>`, with `rel_id` from `replicare.captured`:

   ```sql
   SET SESSION group_concat_max_len = 1000000;
   SELECT GROUP_CONCAT(CONCAT(
            'SELECT ', QUOTE(CONCAT(schema_name, '.', table_name)), ' AS table_name, ',
            'COUNT(*) AS not_sent, MIN(rc_at) AS first_at, MAX(rc_at) AS last_at ',
            'FROM replicare.delta_', rel_id, ' d WHERE NOT EXISTS (',
            'SELECT 1 FROM replicare.track_', rel_id, ' t WHERE t.delta_id = d.delta_id)')
          SEPARATOR ' UNION ALL ')
     INTO @q FROM replicare.captured;
   PREPARE not_sent FROM @q;
   EXECUTE not_sent;
   DEALLOCATE PREPARE not_sent;
   ```

   Both are exact when the database had a single target, as in this layout. Redis keeps no queue:
   a passive Redis copy follows by re-reading the keyspace, so writes made after the last pass
   reached it are only on the old database, and there's no list of them.

   Compare each listed row with the new active database, and re-enter by hand whatever matters.
   Save the output first: the next step deletes these queues.
3. **Remove its capture:**
   [`replicare capture remove <config> --sync active-to-passive`](cli.md#capture-installremove-config---sync-name).
4. **Rebuild it as the passive copy.** Empty it(drop and recreate the database, then load the
   schema with the application's migration tooling) and add a sync from the new active database
   to it, with `sync_sequences: true`. Copying into an empty database takes the fast path.
   Copying into one that still holds rows would only insert and update, never delete, so rows
   deleted since the failover, and the unsent rows above, would stay behind.

After a planned switchover there's nothing to recover: the old active database is already the
passive copy.

### Failing back

To make the original database active again once it's a caught-up passive copy, follow the
[planned switchover](#planned-switchover) in the other direction.

## One active, several passive

### Fan-out or chain

Several passive copies can be fed in two ways:

```
fan-out                                chain

         ┌──► passive-1                active ──► passive-1 ──► passive-2 ──► passive-3
active ──┼──► passive-2
         └──► passive-3
```

| | Fan-out | Chain |
|---|---|---|
| How far behind each copy is | Every copy one hop behind the active database | One hop more for each step down the chain |
| Work on the active database | Feeds every copy | Feeds one copy |
| Initial copies read from | The active database, once per copy | The copy before each one |
| Failing over | The copy promoted, then the other copies re-pointed at it, and some may need copying again | `passive-1` is promoted; the others already follow it |
| A copy fails | Nothing else is affected | The last one affects nothing; one in the middle stops the copies after it until re-pointed |

In a chain each copy passes on what it receives: `replicare`'s writes into a database are
captured by that database's own triggers, like any application write, and flow on to the next
sync. In a fan-out, a change queued on the active database stays queued until every copy has it,
so a copy that's slow, down or paused misses nothing.

We'd recommend the **chain** when the copies exist for failover: promoting `passive-1` needs no
re-pointing, and `passive-1` is always the furthest ahead, so it's always the one to promote. The
**fan-out** suits copies that serve reads first(several regions reading local data), where being
one hop behind matters more than a simple failover. Redis syncs take a single target, so on Redis
several copies are always a chain.

### The chain

```yaml
syncs:
  - name: active-to-p1
    source: active
    targets: [passive-1]
    include: ["public.*"]
    sync_sequences: true
  - name: p1-to-p2
    source: passive-1          # passive-1, read as a source
    targets: [passive-2]
    include: ["public.*"]
    sync_sequences: true
  - name: p2-to-p3
    source: passive-2
    targets: [passive-3]
    include: ["public.*"]
    sync_sequences: true
```

`passive-1` appears under both `sources:` and `targets:` in the config, with the same connection
details.

**Setting it up.** Build it one hop at a time: start `active-to-p1`, and add `p1-to-p2` only once
the first is streaming. Starting a sync installs capture on its source, and capture on
`passive-1` while `passive-1` is still receiving its own initial copy would queue every copied row
for `passive-2`. Apply schema changes from the end of the chain backwards: `passive-3`, then
`passive-2`, then `passive-1`, then the active database.

**Watching it.** Every hop has its own backlog. Only `active-to-p1`'s is what a failover would
lose; the later hops show how far behind the further copies are.

**Planned switchover.** Stop writes to the active database, wait until `active-to-p1`'s backlog
is 0, then follow [cutting over, with a way back](operations.md#cutting-over-with-a-way-back)
for that one hop. `p1-to-p2` and `p2-to-p3` carry on untouched, and the old active database becomes
a copy feeding nothing.

**Unplanned failover.** The steps under [unplanned failover](#unplanned-failover), for
`active-to-p1`. `p1-to-p2` and `p2-to-p3` carry on untouched: `passive-1`'s triggers now capture
the application's writes, as they captured `replicare`'s before.

**A copy fails.** The last one affects nothing upstream; rebuild it later as the target of the
hop before it. One in the middle leaves the copies after it without changes. For example, if
`passive-1` fails:

1. Pause `p1-to-p2`.
2. Add a sync from the active database to `passive-2`, with
   [`skip_initial_copy: { enabled: true }`](configuration.md#starting-without-a-copy-skip_initial_copy),
   so it streams from the active database's queue straight away.
3. Wait for its check against the active database. Expect it to report differences: `passive-2`
   is missing whatever `passive-1` received but hadn't passed on, and the active database has
   already cleared those changes. Copy `passive-2` again with
   [`replicare reseed`](operations.md#forcing-a-reseed) if any table differs.

**The old active database comes back.** The steps under
[when the old active database comes back](#when-the-old-active-database-comes-back), but add it
back at the *end* of the chain, as the target of a sync from `passive-3`.

**Adding a copy.** Add it at the end of the chain, as the target of a sync from the last copy.

**Failing back.** To make the original database active again once it's at the end of the chain
and caught up, move it to the front:

1. Stop writes to the active database(`passive-1` at this point), and wait until every hop's
   backlog is 0 and stays there.
2. Pause the sync into the original database.
3. Install capture on the original database for a new sync from it to `passive-1`, with
   [`replicare capture install <config> --sync <name>`](cli.md#capture-installremove-config---sync-name),
   before any application write reaches it. Start that sync with
   [`skip_initial_copy: { enabled: true }`](configuration.md#starting-without-a-copy-skip_initial_copy).
   The paused sync can stay in the config meanwhile: a paused sync doesn't count as a loop.
4. Point the application at the original database.
5. Remove the paused sync's capture from the copy it read
   ([`replicare capture remove`](cli.md#capture-installremove-config---sync-name)), then delete
   that sync from the config.

### The fan-out

```yaml
syncs:
  - name: active-to-passives
    source: active
    targets: [passive-1, passive-2, passive-3]
    include: ["public.*"]
    sync_sequences: true
```

**Setting it up.** Each copy is copied from the active database in turn and then streams on its
own; one slow copy doesn't hold the others back. Apply schema changes to every copy before the
active database.

**Watching it.** Each copy has its own backlog; the largest is what a failover to the *least*
caught-up copy would lose.

**Planned switchover.** Stop writes to the active database, wait until every copy's backlog is 0,
then move the active role to one copy as in [unplanned failover](#unplanned-failover) below,
re-pointing the others; with every copy caught up, nothing needs copying again.

**Unplanned failover** to, say, `passive-1`:

1. The steps under [unplanned failover](#unplanned-failover), for `active-to-passives`. Promote
   the copy that was furthest ahead: the smallest backlog just before the failure.
2. Re-point the other copies: add a sync from `passive-1` to `passive-2` and `passive-3`, with
   [`skip_initial_copy: { enabled: true }`](configuration.md#starting-without-a-copy-skip_initial_copy).
   Install its capture on `passive-1` before the application's first write, with
   [`replicare capture install <config> --sync <name>`](cli.md#capture-installremove-config---sync-name),
   so nothing written there is missed.
3. Wait for its check. The copies each stopped at a slightly different point, so expect some
   tables to differ; copy those targets again with [`replicare reseed`](operations.md#forcing-a-reseed).

**A copy fails.** Nothing else is affected, as for a [single passive copy](#when-the-passive-copy-fails).

**The old active database comes back.** As for one passive copy, then add it as another target of
the new active database's sync.

**Adding a copy.** Add it to the sync's `targets`; it's copied from the active database, then
streams.

## Several active

Every database(we call these nodes) accepts writes, and `replicare` carries each change to every
other node, so they all end up holding the same data. It's supported for Postgres, MySQL and
Redis, with each node replicating to nodes of the same engine.

### Why would anyone need this?

Most `replicare` setups are one-way: a single source, and one or more targets that only ever
receive changes. That works well for migrations, read replicas, analytics copies and the passive
layouts above, but not once an application needs to *write* in more than one place, say an app
running in two regions with a database in each. Several active covers that case.

It's more demanding than one-way replication, and there are two things that'll break it:

1. IDs that collide across nodes. If two nodes each hand out id `100` to two *different* rows,
   one of those rows is lost the moment they sync.
2. Schema changes that reach one node before the others. `replicare` only copies data, never
   structure, so a column that exists on one node and not on another stalls replication in that
   direction until they match.

Both are decisions that live in the database and in the deployment process, before `replicare`
ever sees a row. `replicare` is built to stop loudly rather than corrupt anything, but it can't
fix either of these on its own; [rule 1](#rule-1-use-globally-unique-ids) and
[rule 2](#rule-2-push-schema-changes-to-every-node-first) below are how to avoid them. Beyond
those, the application has to cope with two nodes changing related data at once; see
[what can't be settled](#what-cant-be-settled).

### Setting up a cluster

Each node is listed under `nodes:`, and the nodes that belong together are grouped in a
`clusters:` entry. The table structure has to exist on every node already(`replicare` copies
data, not structure), and the daemon needs a Postgres `state_store` for its own progress, which,
as for every layout, is best kept on a separate database:

```yaml
state_store:
  engine: postgres
  postgres: { host: pg-state, port: 5432, database: replicare_state, user: replicare, password: ${PW}, sslmode: require }

nodes:
  us:
    engine: postgres
    postgres: { host: pg-us, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require }
  eu:
    engine: postgres
    postgres: { host: pg-eu, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require }

clusters:
  - name: global-app
    engine: postgres
    members: [us, eu]        # any number of nodes, 2 or more
    include: ["public.*"]
    tuning: { drain_interval: 1s }
```

A cluster can have any number of nodes, from two upwards, and every node sends its changes
directly to every other node. Like a sync, a cluster sticks to a single engine. There's nothing
to configure for conflicts - no conflict setting, and no column to add to any table(see
[when two nodes change the same row](#when-two-nodes-change-the-same-row)). One-way syncs keep
working alongside clusters in the same config, and a config without `clusters:` behaves exactly
as it did before. Every field is listed in the
[configuration reference](configuration.md#active-active-clusters-nodes-and-clusters).

#### Permissions each node needs

Every node is both read from and written to, so its `replicare` user needs the source grants
*and* the target grants together:

- Postgres: `TRIGGER` and `SELECT` on the replicated tables, `CREATE`(or a pre-created
  `replicare` schema owned by the `replicare` user), plus `INSERT`, `UPDATE` and `DELETE` on the
  replicated tables. See
  [`grants-source.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source.sql) and
  [`grants-target.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target.sql).
- MySQL: the same combination, from
  [`grants-source-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source-mysql.sql) and
  [`grants-target-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target-mysql.sql).
- Redis: both ACL presets, plus the extra active-active section in each([`acl-source-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-source-redis.txt),
  [`acl-target-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-target-redis.txt)).

None of the engines need a superuser, a `REPLICATION` role, binlog access or a `wal_level`
change.

### How `replicare` runs a cluster

For every pair of nodes, `replicare` runs one job in each direction(`us` to `eu`, and `eu` to
`us`). Each job works much like a one-way sync: it captures changes on one node, re-reads their
current values, and applies them on the other. A change waiting on a node stays queued until
every other node has it.

When `replicare` writes a change into a node, that write is marked as `replicare`'s own, so the
node doesn't capture it again and send it back. This is what stops changes from echoing around
the cluster; a change travels once, from the node where it was made to every other node, and
stops there.

The initial copy also runs in every direction, and it's safe to repeat - a row that's already
present on the receiving node is simply overwritten with the same values. Plain target indexes
stay in place during a cluster's initial copy, since every node is serving the application.

### When two nodes change the same row

If the same row is changed on two nodes at about the same time, the later change wins, on every
node. `replicare` decides which change is "later" using a timestamp it keeps for each row, so all
the nodes end up with the same value regardless of the order the changes arrive in.

Nothing is added to the application's own tables for this. The timestamps live in `replicare`'s
own `replicare` schema on Postgres and MySQL, and in separate hidden keys on Redis; the cost is
roughly one small extra row(or key) per replicated row. If two changes carry exactly the same
timestamp, the tie is broken by node name(its `node_id`, which defaults to its key under
`nodes:`), so two nodes can never disagree about the winner. Differences between the servers'
clocks only affect which of two near-simultaneous writes counts as the later one - the nodes
still all agree on the same winner.

Deletes take part in this too. A delete is recorded with its own timestamp, so a delete and a
concurrent update are settled exactly the same way. These delete records are cleaned up once
every node has seen them; on Redis, they're cleaned up after a fixed age instead, 24 hours by
default, set with the `rc_tombstone_retention` connection parameter.

The winning change replaces the whole row. Values are never merged column by column(or, on
Redis, field by field).

> ⚠️ There's one gap to be aware of. A row placed on a node only by the initial copy carries no
> timestamp until it's next written. If another node changes that same row while the initial
> copy is still running, the other node's change wins, even if the copied value was newer. Once
> the initial copy has finished, every conflict is settled normally.

### What can't be settled

Last-write-wins settles two nodes changing *the same row*. Some things it can't settle, and the
application has to avoid them:

- **Lost updates within a row.** Since the winner replaces the whole row, two nodes changing
  *different* columns of one row at the same moment keep only one of the two changes. A counter or
  balance updated on two nodes at once(`count + 1`, `balance - 10`) loses one of the updates. No
  error is raised. Keep such values on one node, or record each change as its own row and add
  them up.
- **Decisions made on a node's own copy.** Each node decides on data that may be a moment behind
  the others, so two nodes can each approve the last unit of stock, or the last of a spending
  limit.
- **Clashes between different rows.** Two nodes creating *different* rows that break another rule
  together: the same value in a unique column(an email, a "one row per customer" key, an
  idempotency key), or overlapping ranges under an exclusion constraint. Each row is fine on its own
  node; once they meet, the receiving node rejects one, and replication in that direction stops,
  loudly, holding the change until the clash is fixed by hand(change or remove one of the rows on
  every node). The same happens when one node deletes a row while another adds a row that refers to
  it.
- **Work done once per row.** A job queue, an outbox or a lock table replicated to every node is
  picked up by workers on every node, so the work runs more than once. Keep such tables out of
  the cluster(`exclude`), on one node only.

### Engine-specific notes

On Postgres, `replicare` marks its own writes with a transaction-level setting, which needs no
special privilege.

On MySQL, `replicare` marks its own writes with a session variable, and keeps its timestamps in a
plain table updated by ordinary SQL rather than a stored function; a stored function that writes
data would need extra privileges on managed MySQL services, which we don't want to require.

Redis has no triggers, so `replicare` spots local changes by comparing each key against a
fingerprint it stored on the previous pass. Each data key gets a hidden companion key holding its
timestamp and fingerprint; the data key itself is never wrapped or changed, and the companion sits
in the same Redis Cluster slot, so both are written together in one step. This roughly doubles the
key count, and every pass reads each key's value to compare it. The one-way delete sweep is turned
off in a cluster, since a key that's missing on one node may simply be a new key written on
another; deletes travel as delete records instead. When two nodes that already hold different data
are joined into a cluster, each key settles on one value, but a key deleted *before* a node joined
isn't remembered as deleted. Every Redis node in a cluster must run the same Redis version.

### Where things stand

At the moment, clusters are full mesh only - every node talks directly to every other node. Ring
or partial layouts aren't supported, and with N nodes there are N × (N - 1) jobs, so the number of
jobs grows quickly for large clusters.

There's also no automatic standby for `replicare` itself yet. Each job is run by one daemon at a
time; if that daemon stops, it's restarted(by Kubernetes, for example) and carries on from its
last checkpoint, but a hot standby that takes over on its own isn't available.

Finally - the ID check described in rule 1 runs when the daemon starts, and not as part of
`replicare validate`.

### Rule 1: use globally-unique IDs

Many tables give each row an id that the database counts up automatically - `1, 2, 3, 4…`(Postgres
`serial` / `IDENTITY`, MySQL `AUTO_INCREMENT`). With several active nodes this breaks, because each
node counts on its own. Two nodes can, at the same moment, create two *different* rows that both
get id `100`. When those rows sync to each other, there are two rows claiming the same id; one is
kept, and the other is silently lost.

The same trap applies to any *other* column that's forced to be unique and filled in by the
database, for example an auto-generated `order_number`. It isn't only about the primary key.

So every id, and every other auto-filled unique value, in a table replicated by a cluster has to be
globally unique by design, so that any node can create one with no coordination and they'll never
clash. In practice, that means UUID v7 or ULID - large, time-based ids that are effectively
impossible to collide. Plain auto-counting integers aren't allowed in a cluster.

A few clarifications on this:

- Natural keys are fine. A key that's already unique in the real world(an email address, an ISO
  country code, an id assigned by the application itself) has no collision problem; the rule is
  specifically about letting the *database* hand out sequential numbers. Two nodes can still
  create the *same* natural key at once, though; see [what can't be settled](#what-cant-be-settled).
- It's a schema decision, not a setting. `replicare` doesn't and won't invent ids or "sync the
  counters" between nodes, since that approach can't be made safe. The fix lives in the table
  design.
- The passive layouts aren't affected. Only one database writes at a time there, so auto-counting
  integer ids are fine; keeping a promoted copy's counter ahead is what
  [`sync_sequences`](configuration.md#keeping-dr-counters-warm-sync_sequences) is for.

Before it'll run a cluster, `replicare` inspects every table it's about to replicate. If it finds
an auto-counting integer used as a primary key or a unique key, it refuses to start, names the
offending table and column, and points at this rule, which stops a colliding setup from ever going
live.

> ⚠️ At the moment, that check only runs when the daemon starts. A schema change made while the
> cluster is already running - a new table, or a new auto-counting column - isn't caught until the
> next restart. Until a running check exists, it's on the operator not to add auto-counting integer
> keys to replicated tables in a running cluster.

### Rule 2: push schema changes to every node first

`replicare` never replicates schema changes. It copies the data(the rows) between nodes, but not
changes to the structure of a table - adding, dropping or renaming a column, changing a column's
type, or adding a new table. These structure changes are often called DDL. This is deliberate and
permanent; `replicare` is a data replicator, not a schema manager. So a structure change made on one
node doesn't reach the other nodes on its own, and has to be applied to each node separately.

Here's what happens when the nodes disagree. Say nodes A and B both have a `users` table, and a
`phone` column is added to A only. A new user is then written on A, with a phone number.
`replicare` tries to copy that row to B, B has no `phone` column, and the copy fails. `replicare`
doesn't guess, doesn't drop the phone number, and doesn't corrupt anything - it stops copying in
that direction, says so loudly in its logs and status, and holds on to the row. Once the `phone`
column is added to B as well, `replicare` resumes on its own and delivers everything it was
holding.

That stall is safe, but it isn't free. While a node is behind, the changes meant for it are piling
up rather than flowing, and `replicare` only waits so long(about a day by default) before giving up
and re-copying that node from scratch. So every node needs the change promptly; nodes shouldn't
stay mismatched for long.

The way to avoid the stall entirely is to order every schema change like this:

1. Apply the change to every node first, while nothing is using it yet. Adding an empty new
   column(or a new table) to all the nodes breaks nothing on its own.
2. Then deploy the app code that reads or writes the new structure.

Since step 1 finishes everywhere before step 2 begins, no node is ever asked to store something its
structure can't hold, and nothing stalls.

A few changes need a different order:

- Dropping a column: reverse it. Stop using the column in the app everywhere first, then drop it
  from every node last.
- Renaming a column: never rename in place in a running cluster. Add the new column, copy the
  values across, switch the app over to the new column, and then drop the old one. A bare rename
  looks like "drop one column, add another", and it's the easiest way to lose data.
- Changing a column's type: this is only safe if the old values still fit the new type, for
  example widening a number. An incompatible change stalls that node loudly, so do it the
  add-a-new-column-and-migrate way instead.
- Adding a new table: besides creating it on every node(with globally-unique ids, per rule 1),
  `replicare` has to be told to start replicating it, which is a config change plus a restart. It's
  a coordinated step, not a one-line migration.

> ⚠️ Online schema change tools like `gh-ost` and `pt-online-schema-change` rebuild a table behind
> the scenes and swap it in, which doesn't play well with how `replicare` captures changes. We'd
> recommend the database's native, in-place `ALTER` for simple additive changes, and testing
> carefully before using an online schema change tool against a replicated cluster.

### When a node fails

There's no promotion step and no counter to fix: every other node already holds everything that
reached it.

1. **Move the application** to any other node.
2. **Leave the cluster running.** Changes to and from the failed node wait in queues, and the
   other nodes keep replicating among themselves.
3. **When it comes back,** its jobs resume from their checkpoints and it catches up in both
   directions: the changes it missed arrive, and any it took but hadn't sent go out and are settled
   by [last-write-wins](#when-two-nodes-change-the-same-row) like any other change. Nothing has to
   be recovered by hand.
4. **If it stays down past the retention limit**(about a day by default), the other nodes stop
   holding changes for it and mark it to be copied again from scratch when it returns, so their
   disks don't fill. Any changes it took but never sent are then lost; that's the cost of a long
   outage.

If the node wasn't down but cut off(the application kept writing to it), nothing special happens:
it's just a node that's behind, and it settles the same way when it reconnects.

### Adding and removing nodes

**Adding a node:** create the table structure on it, add it under `nodes:` and to the cluster's
`members`, and restart the daemon. It receives an initial copy from the others, and the gap above
applies while that runs. Plan it for a quiet time on a large database.

**Removing a node** for good: take it out of `members` and restart the daemon, so the other nodes
stop holding changes for it, then remove its capture with
[`replicare capture remove`](cli.md#capture-installremove-config---sync-name).

## When `replicare` itself fails

`replicare` sits beside the databases, not in front of them, so the application keeps working when
it stops; only replication pauses.

- **The daemon stops:** it's restarted(by Kubernetes or systemd) and carries on from its last
  checkpoint. Changes wait on their source databases meanwhile, so nothing is missed. A
  `/healthz` check restarts a daemon that's still running but has stopped making progress.
- **The state store is unreachable:** the daemon can't record progress, so replication stops
  until it's back; changes wait on the sources. Keep the state store on a database that fails over
  on its own, separate from the ones it protects.
- **Either is down for long:** past the retention limit, the source databases stop holding changes
  for targets that haven't taken them, and mark those targets to be copied again. This protects the
  sources' disks; it means a long outage of `replicare` ends in re-copies.

## Rehearse it

Whichever layout it is, rehearse it on copies first: a planned switchover, an unplanned failover,
a copy or node failing, the old database coming back, and a failback, timing each, before relying
on it.
