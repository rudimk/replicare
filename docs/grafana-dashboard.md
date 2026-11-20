# Grafana Dashboard

This page describes `replicare-dashboard.json`, a ready-to-import Grafana dashboard for
`replicare`'s Prometheus metrics, along with how to import it, what each section shows, and how
to read it. The dashboard has a configurable data source; the Prometheus data source it queries
is picked at import time(and can be switched later from the dashboard's Data source dropdown),
so it isn't tied to any one Prometheus.

It assumes `replicare`'s `/metrics` endpoint is already being scraped into Prometheus. See
`deploy/helm/replicare-controller`(the `serviceMonitor` option, or a raw scrape job) for how to
wire that up.

## Importing the dashboard

### Through the Grafana UI

1. Open Dashboards, then New, then Import.
2. Upload `replicare-dashboard.json`(or paste its contents).
3. When prompted, select the Prometheus data source for the `Data source` variable, then click
   Import.

### Through provisioning

For file-based provisioning, drop the JSON into a provisioned dashboards folder and point a
provider at it, for example:

```yaml
# /etc/grafana/provisioning/dashboards/replicare.yaml
apiVersion: 1
providers:
  - name: replicare
    type: file
    options:
      path: /var/lib/grafana/dashboards/replicare
```

The dashboard's data source is a template variable(`${datasource}`), so provisioning doesn't
hard-code one; Grafana resolves it from the variable's current value, which defaults to the
default Prometheus. To pin it, set the `datasource` variable's current value, or pass
`DS_PROMETHEUS` via the import API.

## Variables

| Variable | Meaning |
|---|---|
| Data source(`datasource`) | Which Prometheus data source to query. Chosen at import; can be changed any time. |
| Sync(`sync`) | Filters to one or more syncs(`label_values(replicare_table_phase_info, sync)` - a metric that's present from the moment a sync starts, so the filter works during the initial copy too). Defaults to *All*. |
| Target(`target`) | Filters to one or more targets within the selected syncs. Defaults to *All*. |
| Table(`table`) | Filters the per-table panels to one or more tables(from `replicare_table_phase_info`, so every replicated table is listed from the start). Defaults to *All*. |
| Component(`component`) | Filters the per-component panels to one or more FK components. Defaults to *All*. |

## Panels

The panels are organised into sections(Grafana rows).

### Initial copy

This section sits at the top, since the initial copy is the first thing a new sync does:

| Panel | Query | Reading |
|---|---|---|
| Initial copy (overall) | `replicare_initial_copy_progress_ratio` | How much of the copy is done, weighted by table size and counting the link-filling pass. It stays under 100% until every table has switched to streaming |
| Initial copy ETA | `replicare_initial_copy_eta_seconds` | How long the copy has left, as a duration. `replicare` works it out from how much is still to copy and how fast it went over the last 10 minutes, weighing each table by its size on the source, so a table of large rows isn't made to look quick by a run of small ones. It reads "Not moving" when nothing was copied in that time, "Done" at the end, and "Working it out…" for the first minute. The pass that fills in the links for tables linked in a loop counts as extra work for those tables. A guide, not a promise: the row counts are the source's estimates |
| Tables still copying / Tables streaming | `count(replicare_table_phase_info{phase=…})` | How many tables are in each phase; copying reaches 0 once the whole sync is streaming |
| Copy rate (rows/s) | `rate(replicare_rows_copied_total[2m])` | Current copy speed |
| Initial copy progress % (per table) / Rows copied vs target (per table) | `replicare_rows_copied_total` / `replicare_initial_copy_rows_target`, `by (table)` | Which tables have their rows in, and which are still moving |
| Link-filling pass (per table) | `replicare_link_fill_rows_total` / `replicare_link_fill_rows_target`, `by (table)` | For tables linked in a loop: how far the second stage, filling in the links between rows, has got. A table is only finished once its bar is full |

Rows copied only counts rows that have been committed on the target, so a retried chunk is
never counted twice. A table that loads in one statement(a self-referencing table whose link
column can't be emptied) jumps from 0 to done in one step. `replicare_rows_copied_total` restarts
from 0 when the daemon restarts, and a restart re-copies whatever wasn't finished, so the
percentage restarts with it.

### Index rebuild

On a Postgres sync, `replicare` takes the target's plain indexes down for the initial copy and
builds them again once every table is streaming(see
[Target indexes during the first copy](operations.md#target-indexes-during-the-first-copy)).
This row follows that rebuild:

| Panel | Query | Reading |
|---|---|---|
| Indexes built again | `replicare_deferred_indexes_built` / `replicare_deferred_indexes` | The share of the indexes taken down that are back. Empty when none were taken down |
| Indexes still to build | `replicare_deferred_indexes - replicare_deferred_indexes_built` | How many are still missing; it should reach 0 not long after the copy |
| Building now | `replicare_index_build_info` | The index being built, its table, and the stage its build is at. Blank when nothing is building |
| Current stage progress | `replicare_index_build_phase_progress_ratio` | How far through that stage the build is. Sorting and waiting stages read 0% |

Each build reads the table, sorts, writes the index, then reads the table a second time to pick
up rows written meanwhile, so on a big table each index takes a while, and "Current stage
progress" goes round once per stage.

### Source & target

The controller checks both endpoints, and emits their reachability along with two size
figures - one for the whole database, and one for just the replicated tables:

| Panel | Query(source metric) | Reading |
|---|---|---|
| Source up / Targets up | `min(replicare_source_up)` / `min(replicare_target_up)` | `UP`(green) / `DOWN`(red), from the controller's health check of that endpoint |
| Source / Target DB size | `replicare_source_db_bytes` / `replicare_target_db_bytes` | Whole-database on-disk size(Postgres / MySQL). The source usually runs larger, since it carries `replicare`'s capture schema + delta bloat and any unreplicated tables |
| Source / Target replicated size | `replicare_source_replicated_bytes` / `replicare_target_replicated_bytes` | Size of just the replicated tables - the like-for-like comparison; on a caught-up sync the two converge, even when the whole-DB sizes differ |

There are two size figures because the whole-DB gauge counts everything in the database,
including `replicare`'s own `replicare` capture schema on the source and any tables that weren't
selected, so a source that's bigger than its target is normal, and not drift. The
replicated-size gauge only sums the selected tables, with the same function on both sides, so
it's the series to watch for convergence. See
[Whole-DB vs replicated size](operations.md#whole-db-vs-replicated-size).

### Replication lag

This is the headline lag section:

| Panel | Query | Reading |
|---|---|---|
| Max replication lag (SLO) | `max(replicare_replication_lag_seconds)` | Worst lag; green below 30s, amber below 5m, red beyond |
| Catch-up ETA | `replicare_catchup_eta_seconds` | How long until the target has applied every waiting change, as a duration, from the backlog and how fast it shrank over the last 10 minutes. "Caught up" means nothing has been waiting longer than a minute; "Not catching up" means the backlog is flat or growing, or the target is unreachable |
| Inflow vs drain (rows/s) | `sum(replicare_throughput_rows_per_second)` vs `sum(replicare_throughput_rows_per_second) + sum(rate(replicare_delta_backlog_rows[5m]))` | Inflow above drain means the backlog is growing |
| Replication lag / Total backlog | `replicare_replication_lag_seconds`, `sum(replicare_delta_backlog_rows)` | Per-table lag; total rows still to apply |

### Per FK component

These panels are rolled up by the `component` label:

| Panel | Query | Reading |
|---|---|---|
| Replication lag (per component) | `max by (component) (replicare_replication_lag_seconds)` | Which FK component is behind |
| Delta backlog (per component) | `sum by (component) (replicare_delta_backlog_rows)` | Queue depth per component |

### Backlog, throughput & errors

The remaining sections are "Backlog detail (per table)", "Throughput & apply latency" and
"Errors & maintenance". They cover per-table backlog rows / bytes / oldest age; throughput and apply p95;
and the error rate, purge rate and reseeds, using the metrics as they're named.

The Source / Target health and size panels fill in as soon as a sync starts. The lag, backlog
and throughput panels only fill in once tables reach streaming, so during an initial copy
they're expected to read *No data*.

## Things to know

Metrics refresh once per drain pass(the sync's `drain_interval`, 1s by default), so the series
update at that cadence; the dashboard itself auto-refreshes every 30s by default.

The backlog and lag series are the catch-up view. After a burst of churn they rise, then fall
back to baseline as streaming drains the queue. A backlog that keeps climbing while
`Targets up` reads DOWN is the signal we intend for "this target is unreachable and falling
behind".

The dashboard's `uid` is `replicare-overview`, so re-importing the same file updates the
existing dashboard in place, rather than creating a copy.
