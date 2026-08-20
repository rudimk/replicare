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

## Panels

| Panel | Query (source metric) | Reading |
|---|---|---|
| Targets up | `min(replicare_target_up)` | `UP` (green) / `DOWN` (red) — DOWN if any selected target is unreachable |
| Max replication lag | `max(replicare_replication_lag_seconds)` | Worst per-table lag; green < 30s, red ≥ 300s |
| Total delta backlog | `sum(replicare_delta_backlog_rows)` | Rows still to apply across the selection |
| Throughput | `sum(replicare_throughput_rows_per_second)` | Current apply/copy rows/s |
| Delta backlog rows (per table) | `replicare_delta_backlog_rows` | Per-table queue depth; spikes on churn, drains toward 0 — the catch-up view |
| Oldest unconsumed delta age (per table) | `replicare_delta_oldest_unconsumed_age_seconds` | How stale each table is |
| Replication lag (per table) | `replicare_replication_lag_seconds` | Lag broken down per table |
| Delta backlog bytes (per table) | `replicare_delta_backlog_bytes` | Source-footprint signal per table |
| Initial copy progress % (per table) | `replicare_rows_copied_total / replicare_initial_copy_rows_target` | 0→100% during initial copy |
| Rows copied vs target (per table) | `replicare_rows_copied_total`, `replicare_initial_copy_rows_target` | Absolute copy progress |
| Throughput (rows/s) | `replicare_throughput_rows_per_second` | Per-sync throughput over time |
| Apply batch latency (p95) | `histogram_quantile(0.95, rate(replicare_apply_batch_seconds_bucket[5m]))` | Apply-batch tail latency |
| Error rate (per category) | `rate(replicare_errors_total[5m])` | Should be flat at 0 |
| Delta purge rate (per table) | `rate(replicare_delta_purged_total[5m])` | Consumed deltas being reclaimed (bloat control) |
| Reseeds (selected range) | `increase(replicare_reseed_total[$__range])` | Non-zero = a laggard target was reseeded to protect the source |

## Notes

- Metrics refresh **once per drain pass** (your `drain_interval`, default 1s), so
  series update at that cadence — the dashboard auto-refreshes every 30s by default.
- The **backlog and lag series are the catch-up view**: after a churn burst they
  rise, then fall back to baseline as streaming drains the queue. A backlog that
  climbs while `Targets up` reads DOWN is the intended "target is unreachable and
  falling behind" signal.
- The dashboard `uid` is `replicare-overview`; re-importing the same file updates
  the existing dashboard in place rather than creating a copy.
