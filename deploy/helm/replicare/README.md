# replicare Helm chart

Deploys the replicare daemon (`replicare run`) on Kubernetes: one replica that
brings up and streams every sync in your config, exposing the `/status`,
`/healthz`, and `/metrics` endpoints.

## Install

```sh
# from the repo:
helm install my-replicare deploy/helm/replicare \
  -f my-values.yaml

# or point at a packaged chart / OCI registry once published.
```

## How it's configured

replicare is driven by **one config file**. This chart renders the `config` value
**verbatim** into a ConfigMap mounted at `/etc/replicare/config.yml` — so you write
normal replicare config and get its full expressiveness (any engines, any number
of sources/targets/syncs). See the [configuration reference](../../../docs/configuration.md).

Secrets stay out of `config` using `${VAR}` placeholders (replicare expands env
vars in **any** field), supplied via a Secret:

```yaml
config: |
  ...
  targets:
    replica:
      engine: redis
      redis: { host: my-elasticache, port: 6379, tls: require, user: "${EC_USER}", password: "${EC_PW}" }
  ...

secret:
  # Option A — let the chart create a Secret (fine for dev):
  env:
    EC_USER: replicare
    EC_PW:   the-token
  # Option B — reference a Secret you manage (preferred for prod; `env` is ignored):
  existingSecret: my-replicare-secrets
```

The pod loads the Secret via `envFrom`, so every key becomes an env var the config
can reference.

## Key values

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `replicare` / chart appVersion | the daemon image |
| `replicaCount` | `1` | **keep at 1** — single-active per sync; more just stand by |
| `config` | a Redis→Redis sample | your full replicare config, rendered verbatim |
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
helm lint deploy/helm/replicare
helm template my-replicare deploy/helm/replicare -f my-values.yaml | kubectl apply --dry-run=client -f -
```
