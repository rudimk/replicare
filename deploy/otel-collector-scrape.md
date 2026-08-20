# Scraping replicare metrics (OpenTelemetry Collector / Prometheus)

replicare exposes Prometheus metrics on an HTTP endpoint:

| | Default | Set by |
|---|---|---|
| Path | `/metrics` | fixed |
| Port | `9090`, named **`metrics`** | chart `service.ports.metrics` / config `observability.metrics_addr` |
| Metric names | `replicare_*` | the observability contract (see [`docs/operations.md`](../docs/operations.md#metrics-reference)) |

The chart's Service/Pod also exposes `/status` and `/healthz` on the **`status`** port
(`8080`), so a scrape must target the **`metrics`** port specifically — not just "the
Service".

There are two ways to get these metrics into your monitoring stack.

## Option A — Prometheus Operator (simplest, if you run it)

The Helm chart ships a `ServiceMonitor`. Just enable it:

```sh
helm upgrade --install replicare oci://ghcr.io/rudimk/replicare-controller \
  -f your-values.yaml \
  --set serviceMonitor.enabled=true
```

That wires the operator to scrape the `metrics` port for you; nothing below is needed.

## Option B — Prometheus scrape job (OTel Collector `prometheus` receiver, or plain Prometheus)

If you scrape with an **OpenTelemetry Collector** (its `prometheus` receiver embeds a
Prometheus scrape config) or a **plain Prometheus**, add the scrape job below. It uses
Kubernetes `endpointslice` service discovery and keeps only replicare's `metrics`
endpoint, selected by the chart's stable label `app.kubernetes.io/name=replicare-controller`
(release-independent — works regardless of your Helm release name).

```yaml
- job_name: replicare
  honor_labels: true
  scrape_interval: 30s
  scrape_timeout: 10s
  kubernetes_sd_configs:
    - role: endpointslice
      # Optional: scope discovery to replicare's namespace to cut SD churn.
      # namespaces:
      #   names: [replicare]
  relabel_configs:
    # Keep only endpoints of the replicare Service (stable chart label, any release name).
    - action: keep
      regex: replicare-controller
      source_labels:
        - __meta_kubernetes_endpointslice_label_app_kubernetes_io_name
    # Keep only the metrics port (the Service also exposes status:8080).
    - action: keep
      regex: metrics
      source_labels:
        - __meta_kubernetes_endpointslice_port_name
    # Nice-to-have target labels.
    - action: replace
      source_labels: [__meta_kubernetes_namespace]
      target_label: namespace
    - action: replace
      source_labels: [__meta_kubernetes_pod_name]
      target_label: pod
  metric_relabel_configs:
    # Optional: drop anything that isn't a replicare metric (e.g. go_/process_ noise).
    - action: keep
      regex: replicare_.*
      source_labels:
        - __name__
```

### Where it goes in an OpenTelemetry Collector

The job slots into the `prometheus` receiver's embedded config, then feeds your normal
pipeline:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: replicare
          # ... the job from above ...

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [prometheusremotewrite]   # or otlp, etc. — whatever you already use
```

For a **plain Prometheus**, the same block goes under `scrape_configs:` in `prometheus.yml`
unchanged.

## Notes

- **Port name over number.** The job keys on the port *name* (`metrics`), not `9090`, so it
  keeps working if you change `service.ports.metrics`. If you match on the number instead,
  use `__meta_kubernetes_endpointslice_port` with your configured value.
- **No scrape annotations.** The chart sets no `prometheus.io/scrape` annotations by default,
  so annotation-based discovery won't find replicare — use the label-based relabeling above
  (or add your own annotations via `podAnnotations` if your collector discovers by annotation).
- **RBAC.** The collector's ServiceAccount needs `list`/`watch` on `endpointslices` (and
  `pods` if you set the `pod` target label) in the target namespace(s). Most Kubernetes
  Collector deployments already grant this.
- **Scrape interval.** replicare refreshes its gauges once per drain pass
  (`tuning.drain_interval`, default `1s`), so a 15–30s scrape interval captures the movement
  without over-sampling.
- **What to build on top.** Once these metrics land in Prometheus, import the Grafana
  dashboard in [`deploy/grafana/`](grafana/README.md) — it has a configurable data source and
  covers per-table backlog/lag, catch-up, initial-copy progress, throughput, and errors.
