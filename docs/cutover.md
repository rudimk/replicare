# Cutting Over

A migration with `replicare` ends with a cutover: the moment the applications stop writing to the
old database and start writing to the new one. Until then the new database is a copy, written only
by `replicare`. This page explains how a cutover works, what to check before the day, the steps on
the day, how to keep a way back, and the things that most often catch a cutover out.

Below, *old* is the database being moved off and *new* the one being moved to; `forward` is the
sync from old to new, and `back` an optional sync from new to old that keeps *old* usable as a
fallback.

## How a cutover works

While `forward` runs, every change made on *old* is queued there and carried to *new*, usually a
few seconds behind. A cutover closes that gap on purpose:

1. **Writes stop on *old*.** Nothing new joins the queue.
2. **The queue drains.** `forward` carries the last changes to *new*. Once its backlog is 0, the two
   hold the same data.
3. **`forward` is paused.** From here on `replicare` doesn't write to *new*, and it switches back on
   any of *new*'s own triggers it had switched off(see
   [the target's own triggers](operations.md#the-targets-own-triggers)).
4. **The applications move to *new*.**
5. **Optionally, `back` starts**, carrying every change made on *new* to *old*, so going back stays
   possible. Since the two held the same data at step 2, it doesn't copy anything first; it only
   carries the changes made since(see
   [`skip_initial_copy`](configuration.md#starting-without-a-copy-skip_initial_copy)).

The only downtime is between steps 1 and 4: the time the last changes take to arrive, plus the time
the applications take to move. With a small backlog, that's usually a minute or two.

## Before the day

Each of these is something `replicare` reports, so each can be checked well ahead:

1. **Every table is streaming.** `replicare status` shows `streaming` for each one, and nothing
   still copying.
2. **The target's indexes are all back.** On Postgres, `replicare` takes the target's plain indexes
   down for the first copy and builds them again afterwards(see
   [target indexes during the first copy](operations.md#target-indexes-during-the-first-copy)).
   `replicare status` warns while any are missing. Moving the applications before they're back
   makes their queries slow, sometimes very slow.
3. **No checks are waiting.** `replicare status` warns about tables still waiting to be compared
   with the source, for example after [tables were copied while the target's triggers
   fired](operations.md#tables-copied-while-the-triggers-fired). Each is repaired by itself; wait
   for them to finish.
4. **`replicare verify <config> --sync forward` reports no differences.** It's read-only and safe
   on a busy source; see [the CLI reference](cli.md#verify-config-options).
5. **Every table the applications need is on *new*.** `replicare` copies only tables with a primary
   key or a unique key; at start-up it logs the ones it skips(`sync.tables_selected`, at WARN, with
   their names). Those, and any table outside the sync's `include` and `exclude`, aren't on *new*.
   Give each one a key and let the sync pick it up, or copy it by other means during the write stop.
6. **Everything that isn't table data is on *new*.** `replicare` replicates data only: the schema,
   views, functions, extensions, roles and grants on *new* are created by the same migrations and
   tools as on *old*. Materialised views aren't replicated either; refresh each one on *new* after
   the last changes arrive.
7. **Counters are ready.** The values in identity and serial columns are copied, but each
   sequence's counter isn't. Turn on [`sync_sequences`](configuration.md#keeping-dr-counters-warm-sync_sequences)
   so the counters keep up, or run
   [`replicare reseed-sequences`](cli.md#reseed-sequences-config---sync-name---target-name---dry-run)
   on the day, before the applications write. Without it, the first insert on *new* can fail with a
   duplicate key.
8. **The way back is set up**, if one is wanted. Add `back` to the config: source *new*, target
   *old*, the same `include` and `exclude` as `forward`, `enabled: false` for now, and
   `skip_initial_copy` turned on. One config may hold both directions as long as only one runs at a
   time(see [cycle safety](configuration.md#cycle-safety-for-one-way-syncs)). Then run
   `replicare validate <config>`, which checks *new*'s tables against *old*'s for `back`; that
   matters most when *old* runs an older version.
9. **Everything that writes is known.** Applications, background workers, scheduled jobs, admin
   tools, other services: all of them have to stop writing to *old* at step 1, and all of them have
   to be pointed at *new* at step 4.
10. **It's been rehearsed**, on copies of the databases, with the real applications, so the timings
   are known.

## On the day

1. **Stop writes to *old*,** from every writer listed above. Ideally, make *old* refuse writes as
   well, by revoking the applications' write access or making their role read-only, so a writer
   that was missed fails loudly instead of writing changes that never reach *new*. A transaction
   already open on *old* still counts: let it finish.
2. **Wait until `forward`'s backlog is 0 and stays there**, in `replicare status` or the
   `replicare_delta_backlog_rows` metric. Optionally, run
   `replicare verify <config> --sync forward` once more.
3. **Pause `forward`**(`enabled: false`) and restart the daemon(with Helm, change the values and
   upgrade). This comes before step 4: running both directions at once would send every change round
   in a loop, which is why the config refuses it. The restart also switches *new*'s own triggers
   back on, if `replicare` had switched any off. Check that `replicare status` no longer warns about
   them.
4. **If there's a way back, install `back`'s capture on *new*,** before any application writes to
   it:

   ```sh
   replicare capture install <config> --sync back
   ```

   Every change on *new* is queued for *old* from here on, even before `back` runs.
5. **Bring the counters up**, unless `sync_sequences` is on:
   `replicare reseed-sequences <config> --sync forward`.
6. **Refresh any materialised views on *new*.**
7. **Move the applications to *new*,** every writer listed earlier.
8. **If there's a way back:**
   1. Remove `forward`'s capture from *old*:

      ```sh
      replicare capture remove <config> --sync forward
      ```

      This works with `forward` paused. Without it, *old*'s own capture would record every write
      `back` makes, in queue tables nothing reads; and if `forward` ran again later, it would send
      all of them to *new* for nothing.
   2. Turn `back` on(`enabled: true`) and restart the daemon. It streams the changes queued on
      *new* straight away, and compares every table with *new* once it has caught up, repairing any
      row that differs. `replicare status` shows `skipped` against each table until its check passes,
      and the logs show `copy.skip_check_ok` for each.

Anything written to *new* before step 4 never reaches *old*, deletes included. The check after
step 8 finds those rows and repairs them, but the order still matters: install the capture first.

## Falling back

The same steps the other way round: stop writes to *new*, wait for `back`'s backlog to reach 0,
pause `back`(which switches *old*'s own triggers back on, if `replicare` had switched any off), and
move the applications to *old*. If `forward` will run again, install its capture on *old* first(step
4 the other way round), and the same order applies.

## What catches a cutover out

| What happens | Why | What prevents it |
|---|---|---|
| Some writes never reach *new* | A writer still pointed at *old* after step 1: a worker, a cron job, an admin tool | List every writer beforehand; make *old* refuse writes at step 1 |
| The first inserts on *new* fail with a duplicate key | The sequences' counters weren't brought up | `sync_sequences`, or `reseed-sequences` at step 5 |
| The applications are slow on *new* | The target's indexes were still being built again | Wait until `replicare status` stops warning about indexes |
| *new* doesn't record versions, or lets protected rows change | Triggers `replicare` switched off weren't switched back on | Pause `forward` and restart at step 3; check `replicare status`; `replicare triggers restore` does the same by hand |
| A table is empty or missing on *new* | It has no primary key or unique key, or isn't in the sync's selection | Check the `sync.tables_selected` warning before the day |
| Reports or dashboards show old figures | Materialised views aren't replicated | Refresh them at step 6 |
| The changes made just before the stop are missing | `forward` was paused before its backlog reached 0 | Wait for 0 at step 2 |
| `back` sends nothing, or misses early changes | Its capture was installed after the applications started writing to *new* | Install it at step 4, before step 7 |
| The daemon refuses the config | `forward` and `back` both enabled | Pause one before turning the other on |
| A pause or unpause seems to do nothing | `enabled` takes effect when the daemon starts | Restart the daemon(or upgrade the Helm release) after each change |

For a planned move of the active role in a high-availability layout, and for an unplanned
failover, see [failover and high availability](failover.md), which builds on these steps.
