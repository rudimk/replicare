# Scraping `replicare` Metrics

This page describes how to get `replicare`'s Prometheus metrics into an existing monitoring
stack, either through the Prometheus Operator, or with a scrape job for an OpenTelemetry
Collector or a plain Prometheus. `replicare` exposes its metrics on an HTTP endpoint:

| | Default | Set by |
|---|---|---|
| Path | `/metrics` | fixed |
| Port | `9090`, named `metrics` | chart `service.ports.metrics` / config `observability.metrics_addr` |
| Metric names | `replicare_*` | the observability contract(see [`docs/operations.md`](operations.md#metrics-reference)) |

The chart's Service / Pod also exposes `/status` and `/healthz` on the `status` port(`8080`), so
a scrape has to target the `metrics` port specifically, and not just "the Service".

There are two ways to get these metrics into a monitoring stack.

## Option A: the Prometheus Operator

This is the simplest option, for anyone already running the operator. The Helm chart ships a
`ServiceMonitor`, so all it takes is enabling it:

```sh
helm upgrade --install replicare oci://ghcr.io/rudimk/replicare-controller \
  -f your-values.yaml \
  --set serviceMonitor.enabled=true
```

That wires the operator up to scrape the `metrics` port, and nothing below is needed.

## Option B: a Prometheus scrape job

This option covers an OpenTelemetry Collector(its `prometheus` receiver embeds a Prometheus
scrape config) as well as a plain Prometheus. Add the scrape job below; it uses Kubernetes
`endpointslice` service discovery, and keeps only `replicare`'s `metrics` endpoint, selected by
the chart's stable label `app.kubernetes.io/name=replicare-controller`. That label doesn't depend
on the release, so it works whatever the Helm release is called.

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

The job slots into the `prometheus` receiver's embedded config, and then feeds the normal
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

For a plain Prometheus, the same block goes under `scrape_configs:` in `prometheus.yml`,
unchanged.

## Things to know

The job keys on the port's *name*(`metrics`), and not on `9090`, so it keeps working if
`service.ports.metrics` is changed. To match on the number instead, use
`__meta_kubernetes_endpointslice_port` with the configured value.

The chart sets no `prometheus.io/scrape` annotations by default, so discovery based on
annotations won't find `replicare`. Use the label-based relabelling above instead(or add
annotations via `podAnnotations`, for a collector that discovers by annotation).

The collector's ServiceAccount needs `list`/`watch` on `endpointslices`(and on `pods`, if the
`pod` target label is set) in the target namespace(s). Most Kubernetes Collector deployments
already grant this.

`replicare` refreshes its gauges once per drain pass(`tuning.drain_interval`, `1s` by default),
so a scrape interval of 15 to 30s captures the movement without over-sampling.

Once these metrics land in Prometheus, the natural next step is to import the
[Grafana dashboard](grafana-dashboard.md). It has a configurable data source, and covers
per-table backlog / lag, catch-up, initial-copy progress, throughput and errors.
