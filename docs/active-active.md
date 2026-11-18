# Running active-active replication — the rules

Active-active means several databases (called **nodes**) that an application can **write to
on any of them**, with replicare keeping them all carrying the same data. This is more
demanding than one-way replication (one source, one or more read-only copies), and two
things decide whether it works:

1. **IDs must never collide across nodes.**
2. **Schema changes must reach every node before anything uses them.**

Following the two rules below keeps active-active safe. Ignoring them risks silently losing
data. replicare is built to **stop loudly rather than corrupt**, but it cannot fix either
problem on its own — both are decisions that live in the database and in the deployment
process, before replicare ever sees a row.

> For how any of this works under the hood, see the design note,
> [`multi-master.md`](multi-master.md). This page is just the operating rules.

---

## Rule 1 — Use globally-unique IDs (UUID or ULID), always

**The problem.** Many tables give each row an id that the database counts up
automatically — `1, 2, 3, 4…` (Postgres `serial` / `IDENTITY`, MySQL `AUTO_INCREMENT`). In
active-active this breaks, because **each node counts on its own**. Two nodes can, at the
same moment, create two *different* rows that both get id `100`. When those rows sync to
each other, the system sees two rows claiming the same id, keeps one, and **the other is
silently lost**.

The same trap applies to any *other* column that is forced to be unique and filled in by
the database — for example an auto-generated `order_number`. It is not only about the
primary key.

**The rule.** Every id (and every other auto-filled unique value) in an active-active
replicated table must be **globally unique by design**, so any node can create one with no
coordination and they will never clash. In practice that means **UUID v7 or ULID** — large
time-based ids that are effectively impossible to collide. Plain auto-counting integers are
not allowed in an active-active cluster.

A few clarifications:

- **Natural keys are fine.** A key that is already unique in the real world (an email
  address, an ISO country code, an id assigned by the application itself) has no collision
  problem — the rule is specifically about letting the *database* hand out sequential
  numbers.
- **It is a schema decision, not a setting.** replicare does not and will not invent ids or
  "sync the counters" between nodes — that approach cannot be made safe. The fix lives in
  the table design.
- **One-way replication is unaffected.** This rule is only for active-active. A normal
  one-way copy can keep using auto-counting integer ids. (See
  [operations → Sequences & identity counters](operations.md#sequences--identity-counters-dr--promotion)
  for the separate, one-way concern of keeping a failover copy's counter in step.)

**How replicare helps.**

- **At startup (built today):** before it will run an active-active cluster, replicare
  inspects every table it is about to replicate. If it finds an auto-counting integer used
  as a primary key or a unique key, it **refuses to start**, names the offending table and
  column, and points at this rule. This stops the collision setup from ever going live.
- **Continuously (planned, not built yet):** the startup check only runs at startup. A
  schema change made *while the cluster is already running* — a new table, or a new
  auto-counting column — could introduce the problem with no restart to catch it. The plan
  is to run the same check on a schedule (and on demand) so a risky change is flagged the
  day it ships, not the next time something restarts. **Until that exists, it remains the
  operator's responsibility: do not add auto-counting integer keys to replicated tables in
  a running cluster.**

---

## Rule 2 — Push schema changes to every node first, then deploy the app

**replicare never replicates schema changes.** It copies *data* (the rows) between nodes.
It does **not** copy changes to the *structure* of a table — adding a column, dropping one,
renaming one, changing a column's type, or adding a new table. (These structure changes are
often called **DDL**.) This is deliberate and permanent: replicare is a data replicator,
not a schema manager.

So a structure change made on one node does **not** reach the other nodes automatically. It
must be applied to each node separately.

**What happens if the nodes disagree.** Suppose nodes A and B both have a `users` table,
and a `phone` column is added to A only. Then a new user is written on A with a phone
number. replicare tries to copy that row to B, B has no `phone` column, and the copy
**fails**. replicare does not guess, does not drop the phone, and does not corrupt
anything — it **stops copying in that direction and says so loudly** in its logs and
status, holding the row. Once the `phone` column is added to B as well, replicare
**resumes on its own** and delivers everything it was holding. It heals once the structures
match.

Two cautions about that stall:

- While a node is behind, data for it is **piling up, not flowing**. replicare waits only
  so long (about a day by default) before giving up and **re-copying that node from
  scratch**. So every node should get the change promptly — nodes must not stay mismatched
  for long.
- A stall is **safe but not invisible** — it is a real interruption to that node staying
  current. The rule below avoids it entirely.

**The rule.** For every schema change:

1. **Apply the change to every node first**, while nothing is using it yet. Adding an empty
   new column (or a new table) to all nodes breaks nothing on its own.
2. **Then deploy the app code** that reads or writes the new structure.

Because step 1 finishes everywhere before step 2 begins, no node is ever asked to store
something its structure cannot hold. Nothing stalls.

The mirror image and the special cases:

- **Dropping a column:** reverse the order. Stop using it in the app everywhere **first**,
  then drop it from every node **last**.
- **Renaming a column:** never rename in place in a running cluster. Instead: add the new
  column, copy the values across, switch the app to the new column, then drop the old one.
  (A bare rename looks like "drop one column, add another" and is the easiest way to lose
  data.)
- **Changing a column's type:** only safe if the old values still fit the new type (for
  example widening a number). An incompatible change stalls that node loudly — do it the
  add-new-column-and-migrate way instead.
- **Adding a new table:** besides creating it on every node (with globally-unique ids, per
  Rule 1), replicare must be told to start replicating it — a config change plus a restart
  on each node. It is a coordinated step, not a one-line migration.

> **A note on online-schema-change tools** (e.g. `gh-ost`,
> `pt-online-schema-change`): these rebuild a table behind the scenes and swap it in, which
> does not play well with how replicare captures changes. Prefer the database's native,
> in-place `ALTER` for simple additive changes, and test carefully before using an
> online-schema-change tool against a replicated cluster.

---

## The short version

- **IDs:** UUID or ULID on every replicated table. No auto-counting integer keys in a
  cluster. replicare refuses to start if it finds one.
- **Schema changes:** replicare will not carry them. Apply every change to **all nodes
  first**, then deploy the app that uses it. Drops last; never rename in place.
- **If structures ever disagree,** replicare stalls loudly and heals once the structures
  match — but that is a safety net, not a plan.
