# Active-Active Replication

This page describes active-active replication in replicare, where several databases accept
writes at the same time and replicare keeps all of them carrying the same data. It covers why
it exists, how to set a cluster up, how replicare settles two nodes changing the same row, where
things currently stand, as well as the two rules a cluster has to follow to stay safe.

## Why Would Anyone Need This?

Most replicare setups are one-way - a single source, and one or more targets that only ever
receive changes. That works well for migrations, read replicas and analytics copies, but it
falls apart once an application needs to write in more than one place, say an app running in
two regions with a database in each.

Active-active covers that case. Every database in the cluster(we call these nodes) accepts
writes, and replicare carries each change to every other node, so they all end up holding the
same data. It's supported for Postgres, MySQL and Redis, with each node replicating to nodes of
the same engine.

It's more demanding than one-way replication, and there are two things that'll break it:

1. IDs that collide across nodes. If two nodes each hand out id `100` to two *different* rows,
   one of those rows is lost the moment they sync.
2. Schema changes that reach one node before the others. replicare only copies data, never
   structure, so a column that exists on one node and not on another stalls replication in
   that direction until they match.

Both are decisions that live in the database and in the deployment process, before replicare
ever sees a row. replicare is built to stop loudly rather than corrupt anything, but it can't
fix either of these on its own; the two rules at the end of this page are how to avoid them.

## Setting Up a Cluster

Each node is listed under `nodes:`, and the nodes that belong together are grouped in a
`clusters:` entry. The table structure has to exist on every node already(replicare copies
data, not structure), and the daemon needs a Postgres `state_store` for its own progress,
which can live on any of the nodes or on a separate database:

```yaml
state_store:
  engine: postgres
  postgres: { host: pg-us, port: 5432, database: replicare_state, user: replicare, password: ${PW}, sslmode: require }

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
[When Two Nodes Change the Same Row](#when-two-nodes-change-the-same-row)). One-way syncs keep
working alongside clusters in the same config, and a config without `clusters:` behaves exactly
as it did before. Every field is listed in the
[configuration reference](configuration.md#active-active-clusters-nodes-and-clusters).

### Permissions Each Node Needs

Every node is both read from and written to, so its replicare user needs the source grants
*and* the target grants together:

- Postgres: `TRIGGER` and `SELECT` on the replicated tables, `CREATE`(or a pre-created
  `replicare` schema owned by the replicare user), plus `INSERT`, `UPDATE` and `DELETE` on the
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

## How replicare Runs a Cluster

For every pair of nodes, replicare runs one job in each direction(`us` to `eu`, and `eu` to
`us`). Each job works much like a one-way sync: it captures changes on one node, re-reads their
current values, and applies them on the other.

When replicare writes a change into a node, that write is marked as replicare's own, so the
node doesn't capture it again and send it back. This is what stops changes from echoing around
the cluster; a change travels once, from the node where it was made to every other node, and
stops there.

The initial copy also runs in every direction, and it's safe to repeat - a row that's already
present on the receiving node is simply overwritten with the same values.

## When Two Nodes Change the Same Row

If the same row is changed on two nodes at about the same time, the later change wins, on
every node. replicare decides which change is "later" using a timestamp it keeps for each row,
so all the nodes end up with the same value regardless of the order the changes arrive in.

Nothing is added to the application's own tables for this. The timestamps live in replicare's
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

## Engine-Specific Notes

On Postgres, replicare marks its own writes with a transaction-level setting, which needs no
special privilege.

On MySQL, replicare marks its own writes with a session variable, and keeps its timestamps in a
plain table updated by ordinary SQL rather than a stored function; a stored function that
writes data would need extra privileges on managed MySQL services, which we don't want to
require.

Redis has no triggers, so replicare spots local changes by comparing each key against a
fingerprint it stored on the previous pass. Each data key gets a hidden companion key holding
its timestamp and fingerprint; the data key itself is never wrapped or changed, and the
companion sits in the same Redis Cluster slot, so both are written together in one step. This
roughly doubles the key count, and every pass reads each key's value to compare it. The one-way
delete sweep is turned off in a cluster, since a key that's missing on one node may simply be a
new key written on another; deletes travel as delete records instead. When two nodes that
already hold different data are joined into a cluster, each key settles on one value, but a key
deleted *before* a node joined isn't remembered as deleted.

## Where Things Stand

At the moment, clusters are full mesh only - every node talks directly to every other node.
Ring or partial layouts aren't supported, and with N nodes there are N × (N - 1) jobs, so the
number of jobs grows quickly for large clusters.

There's also no automatic standby yet. Each job is run by one daemon at a time; if that daemon
stops, it's restarted(by Kubernetes, for example) and carries on from its last checkpoint, but
a hot standby that takes over on its own isn't available.

Finally - the ID check described in Rule 1 runs when the daemon starts, and not as part of
`replicare validate`.

## Rule 1: Use Globally-Unique IDs

Many tables give each row an id that the database counts up automatically - `1, 2, 3, 4…`(Postgres `serial` /
`IDENTITY`, MySQL `AUTO_INCREMENT`). In active-active this breaks, because each node counts on
its own. Two nodes can, at the same moment, create two *different* rows
that both get id `100`. When those rows sync to each other, there are two rows claiming the same
id; one is kept, and the other is silently lost.

The same trap applies to any *other* column that's forced to be unique and filled in by the
database, for example an auto-generated `order_number`. It isn't only about the primary key.

So every id, and every other auto-filled unique value, in a table replicated by an active-active
cluster has to be globally unique by design, so that any node can create one with no coordination and they'll
never clash. In practice, that means UUID v7 or ULID - large, time-based ids that are
effectively impossible to collide. Plain auto-counting integers aren't allowed in an
active-active cluster.

A few clarifications on this:

- Natural keys are fine. A key that's already unique in the real world(an email address, an ISO
  country code, an id assigned by the application itself) has no collision problem; the rule is
  specifically about letting the *database* hand out sequential numbers.
- It's a schema decision, not a setting. replicare doesn't and won't invent ids or "sync the
  counters" between nodes, since that approach can't be made safe. The fix lives in the table
  design.
- One-way replication isn't affected. This rule only applies to active-active, and a normal
  one-way copy can keep using auto-counting integer ids. Keeping a failover copy's counter in
  step is a separate, one-way concern, covered in
  [operations](operations.md#sequences--identity-counters-dr--promotion).

Before it'll run an active-active cluster, replicare inspects every table it's about to
replicate. If it finds an auto-counting integer used as a primary key or a unique key, it
refuses to start, names the offending table and column, and points at this rule, which stops a
colliding setup from ever going live.

> ⚠️ At the moment, that check only runs when the daemon starts. A schema change made while the
> cluster is already running - a new table, or a new auto-counting column - isn't caught until
> the next restart. Until a running check exists, it's on the operator not to add auto-counting
> integer keys to replicated tables in a running cluster.

## Rule 2: Push Schema Changes to Every Node First

replicare never replicates schema changes. It copies the data(the rows) between nodes, but not
changes to the structure of a table - adding, dropping or renaming a column, changing a column's
type, or adding a new table. These structure changes are often called DDL. This is deliberate
and permanent; replicare is a data replicator, not a schema manager. So a structure change made
on one node doesn't reach the other nodes on its own, and has to be applied to each node
separately.

Here's what happens when the nodes disagree. Say nodes A and B both have a `users` table, and a
`phone` column is added to A only. A new user is then written on A, with a phone number.
replicare tries to copy that row to B, B has no `phone` column, and the copy fails. replicare
doesn't guess, doesn't drop the phone number, and doesn't corrupt anything - it stops copying in
that direction, says so loudly in its logs and status, and holds on to the row. Once the `phone`
column is added to B as well, replicare resumes on its own and delivers everything it was
holding.

That stall is safe, but it isn't free. While a node is behind, the changes meant for it are
piling up rather than flowing, and replicare only waits so long(about a day by default) before
giving up and re-copying that node from scratch. So every node needs the change promptly; nodes
shouldn't stay mismatched for long.

The way to avoid the stall entirely is to order every schema change like this:

1. Apply the change to every node first, while nothing is using it yet. Adding an empty new
   column(or a new table) to all the nodes breaks nothing on its own.
2. Then deploy the app code that reads or writes the new structure.

Since step 1 finishes everywhere before step 2 begins, no node is ever asked to store something
its structure can't hold, and nothing stalls.

A few changes need a different order:

- Dropping a column: reverse it. Stop using the column in the app everywhere first, then drop
  it from every node last.
- Renaming a column: never rename in place in a running cluster. Add the new column, copy the
  values across, switch the app over to the new column, and then drop the old one. A bare rename
  looks like "drop one column, add another", and it's the easiest way to lose data.
- Changing a column's type: this is only safe if the old values still fit the new type, for
  example widening a number. An incompatible change stalls that node loudly, so do it the
  add-a-new-column-and-migrate way instead.
- Adding a new table: besides creating it on every node(with globally-unique ids, per Rule 1),
  replicare has to be told to start replicating it, which is a config change plus a restart on
  each node. It's a coordinated step, not a one-line migration.

> ⚠️ Online schema change tools like `gh-ost` and `pt-online-schema-change` rebuild a table
> behind the scenes and swap it in, which doesn't play well with how replicare captures changes.
> We'd recommend the database's native, in-place `ALTER` for simple additive changes, and testing
> carefully before using an online schema change tool against a replicated cluster.
