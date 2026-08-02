# Configuration reference

replicare is configured by a single YAML file (passed to every command). The
schema is an engine-neutral envelope plus a typed per-engine connection block.

Any string value may reference the environment as `${VAR}` (required — errors if
unset) or `${VAR:-default}` (falls back to `default`). This is how secrets stay
out of the file.

Validate a config without connecting-and-running via `replicare validate <config>`.

## Top-level structure

```yaml
logging:        { ... }   # log level/format
observability:  { ... }   # metrics/status/OTLP endpoints
state_store:    { ... }   # replicare's own progress store (an endpoint)
sources:        { ... }   # named source endpoints
targets:        { ... }   # named target endpoints
syncs:          [ ... ]   # what replicates where
```

## `logging`

| Field | Type | Default | Notes |
|---|---|---|---|
| `level` | `debug`\|`info`\|`warn`\|`error` | `info` | |
| `format` | `json`\|`text` | `json` | `text` is friendlier for local runs |

## `observability`

| Field | Type | Default | Notes |
|---|---|---|---|
| `metrics_addr` | listen addr, e.g. `:9090` | off | serves Prometheus `/metrics` |
| `status_addr` | listen addr, e.g. `:8080` | off | serves `/status`, `/healthz`, and `/metrics` |
| `otlp_endpoint` | host:port, e.g. `otel:4317` | off | exports OTel traces to an OTLP/gRPC collector |

Leaving an address empty disables that endpoint. When `metrics_addr` and
`status_addr` differ, `/metrics` is served on both.

## Endpoints: `state_store`, `sources`, `targets`

An endpoint is an engine name plus a typed connection block. `sources` and
`targets` are maps of name → endpoint; `state_store` is a single endpoint.

```yaml
sources:
  app:                       # the name referenced by a sync
    engine: postgres
    postgres:                # the engine-specific block
      host: app-db
      port: 5432
      database: app
      user: replicare
      password: "${APP_PW}"
      sslmode: verify-full
      params:                # optional extra connection params
        application_name: replicare
```

### Postgres connection block

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | usually 5432 |
| `database` | string | |
| `user` | string | the least-privilege replicare role |
| `password` | string | use `${VAR}` |
| `sslmode` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode (libpq semantics) |
| `params` | map | extra key=value connection parameters |

### MySQL connection block

Used when an endpoint's `engine` is `mysql`. See [the MySQL engine page](mysql.md)
for the two operational wrinkles (a MySQL sync still keeps its state store on
Postgres; MySQL syncs are strictly at-least-once).

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | default 3306 |
| `database` | string | the DSN default database only — selection may reference other databases as `db.table` (a MySQL schema *is* a database) |
| `user` | string | the least-privilege replicare role |
| `password` | string | use `${VAR}` |
| `tls` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode (same spectrum as Postgres `sslmode`); default `prefer` |
| `local_infile` | bool | hint that the target permits `LOAD DATA LOCAL INFILE`, the copy/apply transport (**required** in v1). replicare probes it at connect and halts loud if off — no INSERT fallback yet. A server system variable, not a grant |
| `params` | map | extra key=value DSN parameters |

**`state_store`** is where replicare keeps its *own* operational state (sync
progress, cursors, the ownership lock) — a dedicated `replicare_state` schema it
creates and owns. It may point at the target DB, the source DB, or a separate
Postgres. (This is distinct from the source-side delta/track tables, which always
live on the source.)

## `syncs`

A sync is one replication job: a source, one or more targets, and a table
selection. All targets of a sync must use the same engine as the source
(never cross-engine).

```yaml
syncs:
  - name: app-to-warehouse    # unique; also the ownership-lock key
    source: app               # a key in `sources`
    targets: [warehouse]      # keys in `targets` (one or more; fan-out)
    include: ["public.*"]     # selection globs (schema.table)
    exclude: ["*_audit"]      # excluded from the include set
    tuning: { ... }           # optional (see below)
```

### Selection

`include`/`exclude` are `schema.table` globs (`*` matches within a name segment).
For MySQL, a schema *is* a database, so these are `db.table` globs and a single
sync may span several databases. Tables without a primary key or usable unique key
are **skipped with a warning** (they can't be captured). An FK pointing from a
selected table to an *excluded* one triggers a dangling-FK warning — the target
must already satisfy that parent.

### `tuning` (per sync)

| Field | Type | Default | Meaning |
|---|---|---|---|
| `drain_interval` | duration (`1s`, `500ms`) | `1s` | time between streaming drain passes; longer = more coalescing (less source load) but higher lag |
| `retention.max_age` | duration (`24h`, `0` = off) | `24h` | oldest unconsumed delta before a laggard target is reseeded |
| `retention.max_bytes` | size (`512MB`, `0` = off) | off | delta-table on-disk size before reseed |
| `pool.max_source_connections` | int | 4 | source connection cap (copy worker pool is sized from it) |
| `pool.max_target_connections` | int | 4 | target connection cap |

Durations use Go syntax (`ms`, `s`, `m`, `h`); sizes accept `KB`/`MB`/`GB`/`TB`
(×1000) or `KiB`/`MiB`/`GiB` (×1024), or a bare byte count. Defaults favor low
source pressure — see [operations.md](operations.md) for tuning guidance.

## Full example

See [`../examples/replicare.yml`](../examples/replicare.yml) for a fully annotated
config.
