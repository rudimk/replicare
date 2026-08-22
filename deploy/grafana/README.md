# Grafana dashboard for replicare

`replicare-dashboard.json` is a ready-to-import Grafana dashboard for replicare's
Prometheus metrics (CLAUDE.md §10). It has a **configurable data source** — you
pick which Prometheus data source it queries at import time (and can switch it
later from the dashboard's **Data source** dropdown), so it is not tied to any one
Prometheus.

It assumes replicare's `/metrics` endpoint is being scraped into Prometheus. See
`deploy/helm/replicare-controller` (the `serviceMonitor` option, or a raw scrape
job) for how to wire that up.

## Import

**Grafana UI**

1. **Dashboards → New → Import**.
2. Upload `replicare-dashboard.json` (or paste its contents).
3. When prompted, select your **Prometheus data source** for the `Data source`
   variable, then **Import**.

**Provisioning (file-based)**

Drop the JSON into a provisioned dashboards folder and point a provider at it, e.g.:

```yaml
# /etc/grafana/provisioning/dashboards/replicare.yaml
apiVersion: 1
providers:
  - name: replicare
    type: file
    options:
      path: /var/lib/grafana/dashboards/replicare
```

The dashboard's data source is a template variable (`${datasource}`), so
provisioning does not hard-code one; Grafana resolves it from the current value
(default: your default Prometheus). To pin it, set the `datasource` variable's
current value or pass `DS_PROMETHEUS` via the import API.

## Variables

| Variable | Meaning |
|---|---|
| **Data source** (`datasource`) | Which Prometheus data source to query. Choose at import; change any time. |
| **Sync** (`sync`) | Filter to one or more syncs (`label_values(replicare_target_up, sync)`). Defaults to *All*. |
| **Target** (`target`) | Filter to one or more targets within the selected syncs. Defaults to *All*. |
| **Table** (`table`) | Filter the per-table panels to one or more tables. Defaults to *All*. |
| **Component** (`component`) | Filter the per-component panels to one or more FK components. Defaults to *All*. |

## Panels

Organized into sections (Grafana rows):

**Source** / **Target** — the controller checks both endpoints and emits their reachability and two size figures — the whole database and just the replicated tables:

| Panel | Query (source metric) | Reading |
|---|---|---|
| Source up / Targets up | `min(replicare_source_up)` / `min(replicare_target_up)` | `UP` (green) / `DOWN` (red) — the controller's health-check of that endpoint |
| Source / Target DB size | `replicare_source_db_bytes` / `replicare_target_db_bytes` | Whole-database on-disk size (Postgres/MySQL). The source usually runs larger — it carries replicare's capture schema + delta bloat and any unreplicated tables |
| Source / Target replicated size | `replicare_source_replicated_bytes` / `replicare_target_replicated_bytes` | Size of **just the replicated tables** — the apples-to-apples comparison; on a caught-up sync the two converge even when the whole-DB sizes differ |

> Why two size figures? The whole-DB gauge counts everything in the database — including replicare's own `replicare` capture schema on the source and any tables you didn't select — so source > target is normal, not drift. The replicated-size gauge sums only the selected tables with the same function on both sides, so it's the series to watch for convergence. See [Whole-DB vs replicated size](../../docs/operations.md#whole-db-vs-replicated-size).

**Replication lag** — the headline lag section:

| Panel | Query | Reading |
|---|---|---|
| Max replication lag (SLO) | `max(replicare_replication_lag_seconds)` | Worst lag; green < 30s, amber < 5m, red beyond |
| Catch-up ETA | `sum(delta_backlog_rows) / clamp_min(sum(throughput),1)` | Estimated seconds to converge at the current apply rate |
| Inflow vs drain | `throughput` vs `throughput + rate(sum(delta_backlog_rows)[5m])` | Inflow above drain ⇒ the backlog is growing |
| Replication lag / Total backlog | `replicare_replication_lag_seconds`, `sum(replicare_delta_backlog_rows)` | Per-table lag; total rows still to apply |

**Per FK component** — rolled up by the `component` label:

| Panel | Query | Reading |
|---|---|---|
| Replication lag (per component) | `max by (component) (replicare_replication_lag_seconds)` | Which FK component is behind |
| Delta backlog (per component) | `sum by (component) (replicare_delta_backlog_rows)` | Queue depth per component |

**Backlog detail (per table)**, **Initial copy**, **Throughput & apply latency**, **Errors & maintenance** — per-table backlog rows/bytes/oldest-age; copy progress % and rows-copied-vs-target; throughput and apply p95; error rate, purge rate, and reseeds. (Metrics as named.)

## Notes

- Metrics refresh **once per drain pass** (your `drain_interval`, default 1s), so
  series update at that cadence — the dashboard auto-refreshes every 30s by default.
- The **backlog and lag series are the catch-up view**: after a churn burst they
  rise, then fall back to baseline as streaming drains the queue. A backlog that
  climbs while `Targets up` reads DOWN is the intended "target is unreachable and
  falling behind" signal.
- The dashboard `uid` is `replicare-overview`; re-importing the same file updates
  the existing dashboard in place rather than creating a copy.
