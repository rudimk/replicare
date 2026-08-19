# Deploying on Kubernetes (Helm)

replicare ships a Helm chart — `replicare-controller` — that runs the daemon
(`replicare run`) on Kubernetes. This page is the deployment guide; the chart's own
[README](../deploy/helm/replicare-controller/README.md) is the exhaustive values
reference.

## Two GHCR packages

Each `vX.Y.Z` release publishes two independent OCI artifacts to GHCR, stamped with
the same version:

| Artifact | Package | Pull |
|---|---|---|
| Daemon image | `ghcr.io/rudimk/replicare` | `docker pull ghcr.io/rudimk/replicare:0.1.1` |
| Helm chart | `ghcr.io/rudimk/replicare-controller` | `helm pull oci://ghcr.io/rudimk/replicare-controller --version 0.1.1` |

The chart's `appVersion` defaults `image.tag` to the matching daemon image, so a
versioned chart install pulls a matched chart + image pair unless you override
`image.tag`.

## Install

```sh
# From the published OCI chart (recommended) — pin the version:
helm install my-replicare oci://ghcr.io/rudimk/replicare-controller \
  --version 0.1.1 -f my-values.yaml

# ...or from a checkout of the repo:
helm install my-replicare deploy/helm/replicare-controller -f my-values.yaml
```

`my-values.yaml` is where you put your config and point at your image and secrets.
There is no separate config file to manage — the chart renders your `config` value
into a ConfigMap at `/etc/replicare/config.yml` and runs the daemon against it.

## Configuring the daemon

`config` is a **structured YAML object** (the ergonomic default): Helm merges and
overrides it across `-f` files and `--set`, so `--set config.logging.level=debug`
changes just that field and leaves the rest intact. It is the exact
[replicare config](configuration.md) — any engines, any number of sources, targets,
and syncs.

```yaml
image:
  repository: <your-registry>/replicare
  tag: <version>

config:
  logging: { level: info, format: json }
  observability: { status_addr: ":8080", metrics_addr: ":9090" }

  # replicare's own state — Postgres, always OUTSIDE this chart (see below).
  state_store:
    engine: postgres
    postgres: { host: state.rds.amazonaws.com, port: 5432, database: replicare_state,
                user: replicare, password: ${STATE_PW}, sslmode: require }

  sources:
    cache:
      engine: redis
      redis: { host: my-redis.default.svc.cluster.local, port: 6379, password: ${SRC_PW:-} }
  targets:
    replica:
      engine: redis
      redis: { host: master.my-ec.cache.amazonaws.com, port: 6379, tls: require,
               user: ${EC_USER}, password: ${EC_PW} }
  syncs:
    - { name: cache-to-replica, source: cache, targets: [replica], include: ["*"],
        tuning: { drain_interval: 1s } }

secret:
  existingSecret: my-replicare-secrets   # provides STATE_PW / SRC_PW / EC_USER / EC_PW
```

Two caveats for the structured form:

- **Quote any all-digit scalar** you inline (e.g. a numeric-only password) so it
  decodes as a string, not an int. `${VAR}` placeholders need **no** quoting here —
  they render in block style.
- Prefer a **verbatim string** for byte-exact control: set `config` to a
  block-scalar string and the chart emits it unchanged (Helm can't merge a string,
  but nothing is re-serialized either).

## Secrets

Keep passwords and tokens out of `config` with `${VAR}` placeholders — replicare
expands env vars in **any** field. Supply them via a Secret, injected into the pod
with `envFrom`:

- **`secret.existingSecret`** — reference a Secret you manage (preferred for
  production; the chart creates none, so secrets never live in Helm state).
- **`secret.env`** — a map the chart renders into a Secret (fine for dev).

## Endpoints & monitoring

The pod serves the operator HTTP surface (see [operations](operations.md)):

- `/healthz` — liveness/readiness (both probes use it), on the status port.
- `/status` — per-sync phase, progress, lag (JSON); what `replicare status` renders.
- `/metrics` — Prometheus scrape.

The Service exposes the **status** and **metrics** ports. Set
`serviceMonitor.enabled: true` to scrape `/metrics` via the Prometheus Operator.

> **Port alignment:** `service.ports.status` / `.metrics` **must match** the
> addresses your `config` binds (`observability.status_addr`, `metrics_addr`). The
> defaults line up at `:8080` / `:9090`; if you change one, change both.

## Single-active, rollouts & state

- **Keep `replicaCount: 1`.** replicare is single-active per sync — a second daemon
  just stands by on the state store's ownership lock, so extra replicas are wasted
  and never run two writers. The chart uses a **`Recreate`** rollout so the old
  daemon fully exits (releasing its lock) before the new one starts.
- **Config changes roll the pod** automatically: a checksum annotation over the
  rendered ConfigMap means `helm upgrade` after editing `config` restarts the daemon
  cleanly.
- **The state store lives outside the chart, always.** Every sync needs a Postgres
  state store (v1's only backend) — including Redis→Redis and MySQL→MySQL. Point
  `state_store` at your own managed Postgres (e.g. RDS). The chart never bundles it;
  it is control-plane bookkeeping, separate from your data.

## Security

The container runs **non-root** (uid 65534), with a **read-only root filesystem**
and **all capabilities dropped** (a writable `emptyDir` is mounted at `/tmp`). Grant
the daemon's database/Redis roles the least privilege documented in
[`deploy/`](../deploy/) — [`grants-*.sql`](../deploy/) for Postgres/MySQL and
[`acl-*-redis.txt`](../deploy/) for Redis (note the explicit `+restore` on Redis
targets). Mount a CA bundle via `extraVolumes`/`extraVolumeMounts` if you use
`tls: verify-full`.

## Worked example: in-cluster Redis → ElastiCache

A common shape is replicating a self-managed Redis running in EKS to ElastiCache.
Run replicare **as a pod in the same cluster** — with the AWS VPC CNI the pod gets a
VPC IP, so it can reach both the in-cluster Redis (via its Service DNS) and
ElastiCache (only reachable inside the VPC; open its security group to the node/pod
SG). Watch-outs specific to that move:

- **Version direction:** `RESTORE` rejects a newer-than-target payload, so the
  ElastiCache engine version must be **≥** the source version, or pre-flight blocks.
- **`RESTORE` must be permitted** on the target (it is `@dangerous`; grant `+restore`
  via RBAC). Run `replicare validate` against the real endpoint first — some managed
  tiers restrict commands.
- **TLS + AUTH:** set `tls: require` (or `verify-full`) and the RBAC `user`/`password`
  on the target block. Redis `tls` defaults to `disable`, so set it explicitly.
- **Cluster-mode ElastiCache:** use `mode: cluster` seeded with the configuration
  endpoint (DB 0 only); cluster-mode-disabled connects as `standalone` to the primary
  endpoint.
- **Deletes lag** the sweep interval, and **big keys** briefly block the source on
  `DUMP` — see the [Redis engine page](redis.md) and
  [troubleshooting](troubleshooting.md).

## Validate before applying

```sh
helm lint deploy/helm/replicare-controller
helm template my-replicare deploy/helm/replicare-controller -f my-values.yaml \
  | kubectl apply --dry-run=client -f -
```
