# Deploying on Kubernetes with Helm

`replicare` ships a Helm chart, `replicare-controller`, that runs the daemon(`replicare run`) on
Kubernetes. This page is the deployment guide - where the chart and image come from, how to
install it, how the daemon's config and secrets get into the pod, how it's monitored and kept
single-active, and a worked example. The chart's own [README](helm-chart.md) is the exhaustive
values reference.

## Two GHCR packages

Each `vX.Y.Z` release publishes two independent OCI artifacts to GHCR, stamped with the same
version:

| Artifact | Package | Pull |
|---|---|---|
| Daemon image | `ghcr.io/rudimk/replicare` | `docker pull ghcr.io/rudimk/replicare:0.1.1` |
| Helm chart | `ghcr.io/rudimk/replicare-controller` | `helm pull oci://ghcr.io/rudimk/replicare-controller --version 0.1.1` |

The chart's `appVersion` is the default for `image.tag`, pointing at the matching daemon image, so
a versioned chart install pulls a matched chart and image pair unless `image.tag` is overridden.

## Install

Install from the published OCI chart(which we'd recommend, with the version pinned), or from a
checkout of the repo:

```sh
# From the published OCI chart (recommended) — pin the version:
helm install my-replicare oci://ghcr.io/rudimk/replicare-controller \
  --version 0.1.1 -f my-values.yaml

# ...or from a checkout of the repo:
helm install my-replicare deploy/helm/replicare-controller -f my-values.yaml
```

`my-values.yaml` holds the config, and points at the image and the secrets. There's no separate
config file to manage; the chart renders the `config` value into a ConfigMap at
`/etc/replicare/config.yml`, and runs the daemon against it.

## Configuring the daemon

`config` is a structured YAML object, which is the more ergonomic default. Helm merges and
overrides it across `-f` files and `--set`, so `--set config.logging.level=debug` changes just
that one field and leaves the rest intact. It's exactly the same as the
[`replicare` config](configuration.md), with any engines and any number of sources, targets and
syncs:

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
        tuning: { drain_interval: 1s, drain_batch: 1000 } }

secret:
  existingSecret: my-replicare-secrets   # provides STATE_PW / SRC_PW / EC_USER / EC_PW
```

There are two caveats with the structured form:

1. Quote any all-digit scalar that's inlined(a numeric-only password, for example), so that it
   decodes as a string and not an int. `${VAR}` placeholders need *no* quoting here, since they
   render in block style.
2. For byte-exact control, a verbatim string is the better choice. Set `config` to a block-scalar
   string, and the chart emits it unchanged; Helm can't merge a string, but nothing is
   re-serialised either.

## Secrets

Passwords and tokens are kept out of `config` with `${VAR}` placeholders, since `replicare`
expands env vars in *any* field. They're supplied through a Secret, injected into the pod with
`envFrom`, in one of two ways.

`secret.existingSecret` references a Secret that's managed outside the chart. This is what we'd
recommend for production; the chart creates none, so the secrets never live in Helm state. On the
other hand, `secret.env` is a map the chart renders into a Secret itself, which is fine for dev.

## Endpoints & monitoring

The pod serves the operator HTTP surface(described in [operations](operations.md)). `/healthz`
is used for both liveness and readiness probes, on the status port. `/status` returns per-sync
phase, progress and lag as JSON, and it's what `replicare status` renders. Finally - `/metrics` is
the Prometheus scrape endpoint.

The Service exposes the status and metrics ports. Set `serviceMonitor.enabled: true` to scrape
`/metrics` through the Prometheus Operator.

> ⚠️ `service.ports.status` / `.metrics` have to match the addresses the `config`
> binds(`observability.status_addr`, `metrics_addr`). The defaults line up at `:8080` / `:9090`;
> if one is changed, change both.

### Checking progress without Prometheus / Grafana

Sometimes there's no way to run a metrics stack next to the pod - for example, when the daemon
runs inside the source cluster for network reasons. In that case, `exec` the CLI directly against
the mounted config. There's no need for a shell here; `kubectl exec` runs the binary directly,
without needing `/bin/sh`:

```sh
kubectl exec deploy/replicare -- /usr/local/bin/replicare status /etc/replicare/config.yml   # live counts + delta backlog
kubectl exec deploy/replicare -- /usr/local/bin/replicare verify /etc/replicare/config.yml   # source<->target convergence (exit 0 = converged)
```

`status` is live by default, showing source / target row counts and the per-target delta backlog,
and `verify` is a read-only convergence spot-check. Both accept `--json`, `--sync <name>` and
`--watch <dur>`. There's more in the [CLI reference](cli.md) and in
[Checking progress from the CLI](operations.md#checking-progress-from-the-cli-no-grafana-required).

The runtime image is Chainguard `wolfi-base`, which also ships `/bin/sh` for an interactive
session when debugging something ad hoc(`kubectl exec -it deploy/replicare -- /bin/sh`). The
direct-exec form above doesn't need a shell, though.

## Single-active, rollouts & state

There are five things to know about how the chart runs the daemon over time:

- Keep `replicaCount: 1`. `replicare` is single-active per sync; a second daemon just stands by
  on the state store's ownership lock, so extra replicas are wasted, and two writers never run at
  once. The chart uses a `Recreate` rollout, so that the old daemon fully exits(releasing its
  lock) before the new one starts.
- Config changes roll the pod automatically. A checksum annotation over the rendered ConfigMap
  means a `helm upgrade` after editing `config` restarts the daemon cleanly.
- One pipeline can be paused without touching the others, by setting `enabled: false` on that
  sync. It's a per-pipeline switch, and omitting it or setting it to `true` keeps the sync
  running. Since it's a config change, the upgrade's pod rollout applies it - it's start-time,
  not a live command. We'd recommend editing the sync's block in the values file by name, so that
  the pipeline that's paused is the one that's meant to be:
  ```yaml
  config:
    syncs:
      - name: pg-pipeline
        # ...
        enabled: false        # pauses THIS pipeline only
      - name: redis-pipeline
        # ...                  # (no enabled / true) keeps running
  ```
  Then run `helm upgrade -f values.yaml`. `--set` works too, but it addresses syncs by array
  index rather than by name; `config.syncs[0]` is the *first* sync in the list, which may not be
  the one that should be paused:
  ```
  # WRONG target if redis-pipeline is the 2nd sync — this pauses syncs[0] (e.g. pg-pipeline):
  helm upgrade ... --set-string 'config.syncs[0].enabled=false'
  # Pause the second sync instead:
  helm upgrade ... --set-string 'config.syncs[1].enabled=false'
  ```
  Check which sync actually paused in the pod logs. The paused one logs
  `sync disabled (paused); skipping sync=<name>`, and each one still running logs
  `sync streaming sync=<name>`.
  `replicare status` also shows `[PAUSED]` for each paused sync. The paused sync's source capture
  stays installed, so re-enabling it drains the queued backlog with no data loss. The full
  semantics, and the caveat about long pauses(source-side delta growth), are in
  [Pausing a sync](configuration.md#pausing-a-sync-enabled) in the configuration reference.
- On a Postgres sync, `replicare` takes the target's plain indexes down for the initial copy and
  builds them again once every table is streaming, which makes a large first copy much faster.
  It's on by default, and set per sync in the same `config.syncs` entries:
  ```yaml
  config:
    syncs:
      - name: pg-pipeline
        # ...
        defer_indexes:
          enabled: false            # copy with the target's indexes in place
          # maintenance_work_mem: 2GB # or, left on: memory per index build
          # parallel_workers: 4       #              extra workers per build
  ```
  With `--set`, that's `--set 'config.syncs[0].defer_indexes.enabled=false'`, with the same
  array-index caveat as above. Like any config change, it applies at the next pod start. Turning
  it off partway through a copy only stops more indexes being taken down; any already taken down
  are still built again. The full behaviour is in
  [Target indexes during the first copy](operations.md#target-indexes-during-the-first-copy).
- The state store *always* lives outside the chart. Every sync needs a Postgres state
  store(v1's only backend), including Redis to Redis and MySQL to MySQL. Point `state_store` at
  a managed Postgres(RDS, for example). The chart never bundles it, since it's control-plane
  bookkeeping, separate from the replicated data.

## Security

The container runs as non-root(uid 65534), with a read-only root filesystem and all capabilities
dropped; a writable `emptyDir` is mounted at `/tmp`. Grant the daemon's database / Redis roles the
least privilege documented in
[`deploy/`](https://github.com/rudimk/replicare/tree/main/deploy) - [`grants-*.sql`](https://github.com/rudimk/replicare/tree/main/deploy) for Postgres / MySQL, and
[`acl-*-redis.txt`](https://github.com/rudimk/replicare/tree/main/deploy) for Redis(note the
explicit `+restore` on Redis targets). Mount a CA bundle through `extraVolumes` /
`extraVolumeMounts` when using `tls: verify-full`.

## Worked example: in-cluster Redis to ElastiCache

A common shape is replicating a self-managed Redis running in EKS to ElastiCache. For this, we'd
run `replicare` as a pod in the same cluster; with the AWS VPC CNI the pod gets a VPC IP, so it
can reach both the in-cluster Redis(through its Service DNS) and ElastiCache, which is only
reachable inside the VPC(open its security group to the node / pod SG). There are a few things to
watch out for that are specific to this move.

The first is version direction. `RESTORE` rejects a payload from a newer version than the
target's, so the ElastiCache engine version has to be the same as or newer than the source
version, otherwise pre-flight blocks.

`RESTORE` also has to be permitted on the target. It's `@dangerous`, so grant `+restore` through
RBAC, and run `replicare validate` against the real endpoint first, since some managed tiers
restrict commands.

For TLS and AUTH, set `tls: require`(or `verify-full`) and the RBAC `user` / `password` on the
target block. Redis `tls` defaults to `disable`, so it has to be set explicitly.

Cluster-mode ElastiCache uses `mode: cluster`, seeded with the configuration endpoint(DB 0 only);
with cluster mode disabled, it connects as `standalone` to the primary endpoint.

Finally - deletes lag behind by the sweep interval, and big keys briefly block the source on
`DUMP`. The [Redis engine page](redis.md) and [troubleshooting](troubleshooting.md) cover both.

## Validate before applying

Lint the chart and render it against the cluster with a client-side dry run before applying it
for real:

```sh
helm lint deploy/helm/replicare-controller
helm template my-replicare deploy/helm/replicare-controller -f my-values.yaml \
  | kubectl apply --dry-run=client -f -
```
