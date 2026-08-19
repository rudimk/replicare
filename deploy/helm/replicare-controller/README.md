# replicare-controller Helm chart

Deploys the replicare daemon (`replicare run`) on Kubernetes: one replica that
brings up and streams every sync in your config, exposing the `/status`,
`/healthz`, and `/metrics` endpoints.

The chart is published as an **OCI artifact** at
`ghcr.io/rudimk/replicare-controller` — a package distinct from the daemon image
(`ghcr.io/rudimk/replicare`). Both are stamped with the same version on each
`vX.Y.Z` release.

## Install

```sh
# From the published OCI chart (recommended) — pin the version:
helm install my-replicare oci://ghcr.io/rudimk/replicare-controller \
  --version 0.1.1 \
  -f my-values.yaml

# ...or from a checkout of the repo:
helm install my-replicare deploy/helm/replicare-controller \
  -f my-values.yaml
```

The chart's `appVersion` defaults `image.tag` to the matching daemon image
(`ghcr.io/rudimk/replicare:<version>`), so a versioned OCI install pulls a matched
chart + image pair. Override `image.tag` to decouple them.

## How it's configured

replicare is driven by **one config file**. This chart renders the `config` value
into a ConfigMap mounted at `/etc/replicare/config.yml` — you get replicare's full
expressiveness (any engines, any number of sources/targets/syncs). See the
[configuration reference](../../../docs/configuration.md).

`config` is a **structured YAML object** by default, so Helm merges and overrides
it the normal way — layer `-f` files or `--set config.logging.level=debug` and only
that field changes:

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

Two small caveats for the structured form: **quote any all-digit scalar** you inline
(e.g. a numeric-only password) so it decodes as a string, not an int; `${VAR}`
placeholders need no quoting (they render in block style). If you'd rather hand the
daemon a **byte-exact** config, set `config` to a block-scalar **string** instead and
it is emitted verbatim.

Secrets stay out of `config` using `${VAR}` placeholders (replicare expands env vars
in **any** field), supplied via a Secret:

```yaml
secret:
  # Option A — let the chart create a Secret (fine for dev):
  env: { EC_USER: replicare, EC_PW: the-token }
  # Option B — reference a Secret you manage (preferred for prod; `env` is ignored):
  existingSecret: my-replicare-secrets
```

The pod loads the Secret via `envFrom`, so every key becomes an env var the config
can reference.

## Key values

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/rudimk/replicare` / chart appVersion | the daemon image (a separate GHCR package from this chart) |
| `replicaCount` | `1` | **keep at 1** — single-active per sync; more just stand by |
| `config` | a Redis→Redis sample | your full replicare config as a structured map (mergeable), or a string for a verbatim config |
| `secret.existingSecret` | `""` | reference an existing Secret for `${VAR}` values |
| `secret.env` | `{}` | chart-managed Secret (dev) |
| `service.ports.status` / `.metrics` | `8080` / `9090` | **must match** `observability.status_addr`/`metrics_addr` in `config` |
| `serviceMonitor.enabled` | `false` | scrape `/metrics` via the Prometheus Operator |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | — | standard scheduling knobs |
| `extraVolumes` / `extraVolumeMounts` | `[]` | e.g. a CA bundle for `tls: verify-full` |

## Notes & gotchas

- **Single active.** The rollout is `Recreate` so the old daemon exits (releasing
  its ownership lock) before the new one starts — never two writers. Scaling above
  1 replica is unsupported.
- **Port alignment.** The chart's Service/probe ports must match the addresses your
  `config` binds (`observability.status_addr`, `metrics_addr`). Defaults line up at
  `:8080`/`:9090`.
- **State store.** Every sync needs a Postgres state store (v1's only backend),
  including Redis→Redis and MySQL→MySQL. Point `state_store` at a reachable Postgres.
- **Config changes roll the pod** automatically (a checksum annotation), so
  `helm upgrade` after editing `config` restarts the daemon cleanly.
- **Least privilege.** The container runs non-root (uid 65534), read-only root FS,
  all capabilities dropped. Grant the daemon roles per
  [`deploy/`](../../) (`grants-*.sql`, `acl-*-redis.txt`).

## Validate before applying

```sh
helm lint deploy/helm/replicare-controller
helm template my-replicare deploy/helm/replicare-controller -f my-values.yaml | kubectl apply --dry-run=client -f -
```
