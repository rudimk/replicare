# Failover and High Availability

This page covers keeping a database available when it fails: holding copies elsewhere, moving
the application to one of them, and bringing the failed database back. We describe three
layouts, from simplest to most demanding:

1. [One active, one passive](#one-active-one-passive): one database takes writes and a single
   copy stands by.
2. [One active, several passive](#one-active-several-passive): one database takes writes and
   several copies stand by.
3. [Several active](#several-active): every database takes writes at the same time.

*Active* means the application writes to it; *passive* means it only receives changes from
`replicare`. In the first two layouts only one database is ever written to at a time, so they
work with any schema `replicare` can replicate, including tables that count up their own integer
ids. The third has strict schema rules, covered on the [active-active](active-active.md) page.

> ⚠️ `replicare` copies changes after they're committed, a moment later, so an unplanned failover
> loses whatever the active database had committed but not yet sent: normally the last few
> seconds, and at most its delta backlog at that moment. If no committed write may ever be lost,
> `replicare` isn't enough on its own; the database's built-in synchronous replication confirms
> each write on a copy before acknowledging it. A *planned* switchover, as described below,
> loses nothing.

## Before any of them

- **Run `replicare` and its state store away from the active database.** The daemon has to
  keep running when the active database's site is down, and reach every other database. The
  `state_store` shouldn't live on any database in the layout; a small separate Postgres that
  can fail over on its own is the right home for it. If the state store were on the active
  database, losing that database would stop every sync.
- **Grant every database both sets of permissions.** A passive database is a target now and
  may be a source later, so its `replicare` user needs the source grants and the target grants
  (see [least-privilege grants](operations.md#least-privilege-grants)).
- **Keep counters ahead.** Set [`sync_sequences: true`](configuration.md#keeping-dr-counters-warm-sync_sequences)
  on every sync whose target might be promoted, so a promoted database's own id counters are
  already past the copied ids. That needs `UPDATE` on each sequence(Postgres) or `ALTER` on each
  table(MySQL) on the targets.
- **Leave out tables the application can do without.** Large append-only tables(logs, webhook
  payloads, audit trails) are often most of a database's size. Excluding them cuts the trigger
  work on the active database, the copy time and the disk on every copy, if the application can
  run without their history after a failover. Framework bookkeeping tables(Rails'
  `schema_migrations` and `ar_internal_metadata`, PostGIS's `spatial_ref_sys`) should always be
  excluded, since each database runs its own migrations.
- **Apply schema changes to the passive databases first.** A target that's missing a column its
  source sends stalls that sync until the column is added, so every migration goes to the
  passive databases before the active one, then the application code follows.

## One active, one passive

```
active ──► passive
```

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

The passive database's delta backlog(`replicare_delta_backlog_rows`,
`replicare_delta_oldest_unconsumed_age_seconds`) is how much an unplanned failover would lose
right now; alert on it, and on `replicare_target_up`.

### Failing over

1. **Fence the active database.** Make sure nothing can write to it(cut the application's
   access by network, credentials or DNS). If it came back while some application servers still
   used it, both databases would take writes, which can't be reconciled.
2. **Pause `active-to-passive`**(`enabled: false`, then restart the daemon) before the old active
   database can come back. A sync still switched on would reconnect, re-read the changes stuck in
   its queue from the old database, and write them over newer data on the new one.
3. **Note what was lost:** the backlog for `active-to-passive` just before the failure, from the
   metrics above.
4. **Check the counters.** With `sync_sequences` on they're already ahead; to be sure, run
   [`replicare reseed-sequences <config> --sync active-to-passive`](cli.md#reseed-sequences-config---sync-name---target-name---dry-run),
   which only reads and changes the passive database.
5. **Point the application at the passive database.** It's the active one now.

For a **planned switchover**, first stop writes to the active database and wait until the sync's
backlog is 0 and stays there; then follow the same steps. Nothing is lost, and nothing is stuck
on the old database.

### When the old active database comes back

It's behind(everything written since the failover went elsewhere) and, after an unplanned
failover, slightly ahead(the changes it took but never sent).

1. **Keep it fenced from the application.**
2. **List the changes that never got out.** On the old database, this prints, per table, how
   many changes were still queued, their time range, and up to 20 of them(`I`/`U`/`D` for
   insert, update or delete, then the first key column):

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

   It's exact when the database had a single target, as in this layout. Compare each listed row
   with the new active database, and re-enter by hand what matters. Save the output: the next
   step deletes these queues.
3. **Remove its capture:**
   [`replicare capture remove <config> --sync active-to-passive`](cli.md#capture-installremove-config---sync-name).
4. **Rebuild it as the passive database.** Recreate its tables empty(drop and recreate the
   database, then load the schema with the application's migration tooling), and add a sync from
   the new active database to it, with `sync_sequences: true`. Copying into an empty database takes
   the fast path. Copying into one that still holds rows would only insert and update, never
   delete, so rows deleted since the failover, and the unsent rows above, would stay behind.

### Failing back

To make the original database active again once it's a caught-up passive copy, follow
[cutting over, with a way back](operations.md#cutting-over-with-a-way-back): it moves the active
role with no copy at all, using [`skip_initial_copy`](configuration.md#starting-without-a-copy-skip_initial_copy).

## One active, several passive

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
| Failing over | The new active database is promoted, and the other copies are re-pointed at it | `passive-1` is promoted; the others already follow it |
| A copy in the middle fails | Nothing else is affected | The copies after it stop until re-pointed |

Each copy passes on what it receives in a chain: `replicare`'s writes into a database are
captured by that database's own triggers like any application write, so they flow on to the next
sync. A queued change on a database feeding several targets stays queued until every target has
it, so a copy that's slow or down at the time misses nothing.

**We'd recommend the chain when the copies exist for failover:** promoting `passive-1` needs
no re-pointing, and `passive-1` is always the furthest ahead, so it's always the one to promote.
The fan-out suits copies that are read replicas first(several regions reading local data), where
being one hop behind matters more than a simple failover.

### The chain

```yaml
syncs:
  - name: active-to-p1
    source: active
    targets: [passive-1]
    include: ["public.*"]
    sync_sequences: true
  - name: p1-to-p2
    source: passive-1          # passive-1 as a source
    targets: [passive-2]
    include: ["public.*"]
    sync_sequences: true
  - name: p2-to-p3
    source: passive-2
    targets: [passive-3]
    include: ["public.*"]
    sync_sequences: true
```

Build it one hop at a time: start `active-to-p1`, and add `p1-to-p2` only once the first is
streaming. Starting a sync installs capture on its source, and capture on `passive-1` while
`passive-1` is still receiving its own initial copy would queue every copied row for
`passive-2`. Apply schema changes from the end of the chain backwards: `passive-3`, then
`passive-2`, then `passive-1`, then the active database.

**Failing over:** the steps under [one active, one passive](#failing-over), for
`active-to-p1`. `p1-to-p2` and `p2-to-p3` carry on untouched: `passive-1`'s triggers now capture
the application's writes, as they captured `replicare`'s before.

**A copy fails:** the last one affects nothing upstream; rebuild it later. One in the middle
leaves the copies after it without changes. For example, if `passive-1` fails:

1. Pause `p1-to-p2`.
2. Add a sync from the active database to `passive-2`, with
   [`skip_initial_copy: { enabled: true }`](configuration.md#starting-without-a-copy-skip_initial_copy),
   so it streams from the active database's queue straight away.
3. Wait for its check against the active database. Expect it to report differences: `passive-2`
   is missing whatever `passive-1` received but hadn't passed on, and the active database has
   already cleared those changes. Copy `passive-2` again with
   [`replicare reseed`](operations.md#forcing-a-reseed) if any table differs.

**The old active database comes back:** the steps under
[when the old active database comes back](#when-the-old-active-database-comes-back), but add it
back at the *end* of the chain, as the target of a sync from `passive-3`.

### The fan-out

```yaml
syncs:
  - name: active-to-passives
    source: active
    targets: [passive-1, passive-2, passive-3]
    include: ["public.*"]
    sync_sequences: true
```

**Failing over** to, say, `passive-1`: the steps under [one active, one passive](#failing-over),
then re-point the other copies at it:

1. Add a sync from `passive-1` to `passive-2` and `passive-3`, with
   [`skip_initial_copy: { enabled: true }`](configuration.md#starting-without-a-copy-skip_initial_copy).
   Install its capture on `passive-1` before the application's first write, with
   [`replicare capture install <config> --sync <name>`](cli.md#capture-installremove-config---sync-name),
   so nothing written there is missed.
2. Wait for its check. The copies each stopped at a slightly different point, so expect some
   tables to differ; copy those targets again with [`replicare reseed`](operations.md#forcing-a-reseed).
   Promoting the copy that was furthest ahead(the smallest backlog before the failure) keeps this
   to a minimum.

**The old active database comes back:** as for one passive, then add it as another target of the
new active database's sync.

## Several active

Every database takes writes, and `replicare` sends each change to every other database. When
one fails, the application simply moves to another, which already holds everything that reached
it; there's no promotion step and no counter to fix. It's the most available layout, and the
most demanding: the schema must follow strict rules, and the application has to cope with two
databases changing the same row at once. Setting it up, how conflicts are settled, and the rules
are on the [active-active](active-active.md) page; read it before choosing this layout. In
short:

- **Ids must be globally unique**(UUID v7 or ULID). Database-counted integer keys are refused
  when the cluster starts([rule 1](active-active.md#rule-1-use-globally-unique-ids)).
- **Schema changes reach every database before the application uses them**
  ([rule 2](active-active.md#rule-2-push-schema-changes-to-every-node-first)).
- **Two databases changing the same row:** the later change wins and replaces the whole row. Two
  changes to *different* columns of a row at the same moment keep only one of them, and a
  counter or balance updated on two databases at once loses one update, with no error.
- **Two databases creating different rows that clash on another rule**(a unique email, a
  "one row per customer" key, an exclusion constraint) can't be settled: replication in that
  direction stops until the clash is fixed by hand. The same happens when one database deletes a
  row while another adds a row that refers to it.

### When a database fails

1. **Move the application** to any other database in the cluster.
2. Leave the cluster running. Changes to and from the failed database wait in queues; the other
   databases keep replicating among themselves.
3. **When it comes back**, its jobs resume from their checkpoints and it catches up in both
   directions: the changes it missed arrive, and any it took but hadn't sent go out. Nothing has
   to be recovered by hand, since those changes are settled like any other conflict.
4. If it stays down past the retention limit(about a day by default), the other databases stop
   holding changes for it and mark it to be copied again, from scratch, when it returns. Taking
   it out of the cluster(remove it from `members`, and its capture with
   [`replicare capture remove`](cli.md#capture-installremove-config---sync-name)) avoids that
   for a database that won't return.

## Which one to choose

| Need | Layout |
|---|---|
| A standby to move to if the database fails | [One active, one passive](#one-active-one-passive) |
| Standbys in several places, or a standby for the standby | [One active, several passive](#one-active-several-passive) |
| Writes accepted in several places at once, and a schema built for it | [Several active](#several-active) |

Whichever it is, rehearse it on copies first: a failover, a copy failing, the old database coming
back, and a failback, timing each, before relying on it.
