# The `replicare-controller` Helm Chart

This page describes the `replicare-controller` Helm chart, which deploys the `replicare`
daemon(`replicare run`) on Kubernetes. It runs a single replica that brings up and streams
every sync in the config, and exposes the `/status`, `/healthz` and `/metrics` endpoints. It
covers how to install the chart, how the daemon's config is passed in(including secrets and
pausing a pipeline), the values most deployments touch, as well as a few things to watch out
for.

The chart is published as an OCI artifact at `ghcr.io/rudimk/replicare-controller`, which is a
separate package from the daemon image(`ghcr.io/rudimk/replicare`). Both are stamped with the
same version on each `vX.Y.Z` release.

## Installing the chart

We'd recommend installing from the published OCI chart with the version pinned, although a
checkout of the repo works just as well:

```sh
# From the published OCI chart (recommended) — pin the version:
helm install my-replicare oci://ghcr.io/rudimk/replicare-controller \
  --version 0.1.1 \
  -f my-values.yaml

# ...or from a checkout of the repo:
helm install my-replicare deploy/helm/replicare-controller \
  -f my-values.yaml
```

The chart's `appVersion` is the default for `image.tag`, and points at the matching daemon
image(`ghcr.io/rudimk/replicare:<version>`), so a versioned OCI install always pulls a chart and
an image that belong together. To decouple the two, override `image.tag`.

## How it's configured

`replicare` is driven by a single config file. The chart renders the `config` value into a
ConfigMap mounted at `/etc/replicare/config.yml`, which means the chart can express everything
`replicare` itself can - any engines, and any number of sources / targets / syncs. Every field
is covered in the [configuration reference](https://replicare.rudimk.com/configuration.html).

By default, `config` is a structured YAML object, so Helm merges and overrides it the normal
way; layering `-f` files or passing `--set config.logging.level=debug` changes only that one
field:

```yaml
config:
  logging: { level: info, format: json }
  observability: { status_addr: ":8080", metrics_addr: ":9090" }
  state_store:
    engine: postgres           # always OUTSIDE this chart — point at your own Postgres
    postgres: { host: state.rds, port: 5432, database: replicare_state, user: replicare, password: ${STATE_PW}, sslmode: require }
  sources:
    cache: { engine: redis, redis: { host: my-redis, port: 6379, password: ${SRC_PW:-} } }
  targets:
    ec: { engine: redis, redis: { host: my-elasticache, port: 6379, tls: require, user: ${EC_USER}, password: ${EC_PW} } }
  syncs:
    - { name: cache-to-ec, source: cache, targets: [ec], include: ["*"], tuning: { drain_interval: 1s } }
```

> ⚠️ There's one small catch with the structured form. Any all-digit scalar written inline(a
> numeric-only password, for example) has to be quoted, so that it's decoded as a string and
> not an int. `${VAR}` placeholders don't need quoting, since they render in block style.

To hand the daemon a byte-exact config instead, set `config` to a block-scalar string; it's
then emitted verbatim.

### Pausing a pipeline

Each sync(and cluster) takes an optional `enabled: true|false`. Setting it to `false` pauses
just that pipeline, and the others keep running. Since it's a config change, it's applied by
the pod rollout that `helm upgrade` triggers; it takes effect at start-up, not live. We'd
recommend editing the sync's block by name in the values file, so that the right one gets
paused:

```yaml
config:
  syncs:
    - name: pg-pipeline
      enabled: false      # pauses THIS pipeline only
    - name: redis-pipeline
      # ...               # keeps running
```

`--set` works too, but it addresses syncs by array index rather than by name. `config.syncs[0]`
is the *first* sync in the list, which may not be the one intended(`--set-string 'config.syncs[1].enabled=false'`
for the second, and so on). After the rollout, confirm which sync was paused in the pod
logs(`sync disabled (paused); skipping sync=<name>`, as opposed to `sync streaming sync=<name>`)
or with `replicare status`, which marks it `[PAUSED]`.

The paused sync's source capture stays installed, so re-enabling it drains the queued backlog
with no data loss. A long pause does grow the source-side delta tables, though. The full
behaviour is described under
[Pausing a sync](https://replicare.rudimk.com/configuration.html#pausing-a-sync-enabled) in the
configuration reference.

### Keeping secrets out of the config

Secrets stay out of `config` through `${VAR}` placeholders(`replicare` expands env vars in
*any* field), and the values come from a Secret:

```yaml
secret:
  # Option A — let the chart create a Secret (fine for dev):
  env: { EC_USER: replicare, EC_PW: the-token }
  # Option B — reference a Secret you manage (preferred for prod; `env` is ignored):
  existingSecret: my-replicare-secrets
```

The pod loads the Secret through `envFrom`, so every key in it becomes an env var the config can
reference.

## Key values

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/rudimk/replicare` / chart appVersion | the daemon image(a separate GHCR package from this chart) |
| `replicaCount` | `1` | keep at 1; there's a single active daemon per sync, and any extra replicas just stand by |
| `config` | a Redis to Redis sample | the full `replicare` config, as a structured map(mergeable) or as a string for a verbatim config |
| `secret.existingSecret` | `""` | references an existing Secret for `${VAR}` values |
| `secret.env` | `{}` | a chart-managed Secret(for dev) |
| `service.ports.status` / `.metrics` | `8080` / `9090` | has to match `observability.status_addr`/`metrics_addr` in `config` |
| `serviceMonitor.enabled` | `false` | scrapes `/metrics` through the Prometheus Operator |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | none | the standard scheduling knobs |
| `extraVolumes` / `extraVolumeMounts` | `[]` | for example, a CA bundle for `tls: verify-full` |

## Notes & gotchas

The chart runs a single active daemon. Its rollout strategy is `Recreate`, so the old daemon
exits(releasing its ownership lock) before the new one starts, and there are never two writers.
Scaling above 1 replica isn't supported.

The chart's Service / probe ports have to match the addresses the `config` binds
to(`observability.status_addr`, `metrics_addr`). The defaults already line up, at
`:8080`/`:9090`.

To scrape metrics with the Prometheus Operator, set `serviceMonitor.enabled=true`. Without
it(with an OpenTelemetry Collector or plain Prometheus, for example), use the scrape job in
[the metrics-scraping guide](https://replicare.rudimk.com/otel-collector.html). Once `/metrics`
is being scraped(through `serviceMonitor.enabled` or a separate scrape job), import
[`deploy/grafana/replicare-dashboard.json`](https://replicare.rudimk.com/grafana-dashboard.html);
it has a configurable Prometheus data source, and covers per-table backlog / lag, catch-up,
initial-copy progress, throughput and errors.

Every sync needs a Postgres state store(the only backend in v1), and that includes Redis to
Redis and MySQL to MySQL syncs. Point `state_store` at a reachable Postgres; it always lives
outside this chart.

Config changes roll the pod automatically(through a checksum annotation), so a `helm upgrade`
after editing `config` restarts the daemon cleanly.

Finally - the container runs as non-root(uid 65534), with a read-only root filesystem and all
capabilities dropped. Grant the daemon's database roles using the `grants-*.sql` and
`acl-*-redis.txt` files in [`deploy/`](https://github.com/rudimk/replicare).

## Validating before applying

To check a values file before it reaches the cluster, lint the chart, then render it and run it
through a client-side dry run:

```sh
helm lint deploy/helm/replicare-controller
helm template my-replicare deploy/helm/replicare-controller -f my-values.yaml | kubectl apply --dry-run=client -f -
```
