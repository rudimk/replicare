# CLI reference

All commands take the config file as an argument. Exit codes: `0` success,
`1` runtime error, `2` usage/config error.

```
replicare <command> [args]
```

## `version`

```sh
replicare version
```

Prints the version, commit, build date, and Go version.

## `validate <config>`

```sh
replicare validate config.yml
```

Loads the config and runs **pre-flight** for every (sync, target): connects
read-only, introspects both sides, and classifies every column pair —
identical/widening (ok), risky/lossy (warn), incompatible or missing type
(**block**). Also reports FK components, a giant-component warning, dangling FK
edges, and skipped no-key tables. Makes **no changes**. Fix all BLOCK findings
before running.

## `run <config>`

```sh
replicare run config.yml
```

Runs the daemon: starts the observability endpoints, then brings up and streams
every sync until `SIGINT`/`SIGTERM`. On a signal it shuts down **gracefully** —
finishes the in-flight drain pass, checkpoints, and exits `0`.

Each sync runs under a single-active ownership lock; a second daemon pointed at
the same sync stands by. State is checkpointed, so a restarted process resumes
from where it left off.

## `status <config> [--json] [--sync <name>]`

```sh
replicare status config.yml            # human-readable table
replicare status config.yml --json     # machine-readable
replicare status config.yml --sync app-to-warehouse
```

Reads the state store and reports, per sync/target/table: phase (initial-copy vs
streaming), lag (cursor age), initial-copy progress, needs-reseed flags, and
recent events. Works whether or not a daemon is currently running (it reads
persisted checkpoints).

## `capture install|remove <config> [--sync <name>]`

```sh
replicare capture install config.yml            # all syncs
replicare capture remove config.yml --sync app-to-warehouse
```

Installs or removes the trigger-based CDC machinery on a sync's source, over its
selected tables. `run` installs capture automatically, so this is for
pre-provisioning or manual teardown.

**Note (§12):** `install` needs only `TRIGGER` (plus `SELECT`/`USAGE` and schema
creation). `remove` additionally needs **table ownership** to drop the triggers —
if the replicare role doesn't own the tables, removal of the trigger fails
(reported), though replicare's own delta/track tables are still dropped.

## `reseed <config> --sync <name> --target <name>`

```sh
replicare reseed config.yml --sync app-to-warehouse --target warehouse
```

Flags a target for a full re-copy. It signals the **running** daemon (via the
state store) to re-copy that target from current source state on its next pass,
then resume streaming — honoring single-active ownership rather than acting
directly. Use it after out-of-band divergence, or to recover a target that fell
behind.

## Signals

`run` responds to:

- `SIGTERM` / `SIGINT` — graceful shutdown (drain in-flight pass, checkpoint, exit 0).

An ungraceful kill is also safe: state is checkpointed and applies are
idempotent, so a fresh `run` resumes and converges without duplicates.
