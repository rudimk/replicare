// Package redis is the Redis implementation of the engine interfaces
// (engine.Engine / Source / Sink), the third engine after Postgres and MySQL.
// Redis is NOT relational: it has no SQL, schema, tables, primary/foreign keys,
// triggers, or text wire format. It fits the neutral Source/Sink abstraction by
// overloading the vocabulary — TableRef -> key-namespace/shard, KeyValues -> a
// single opaque []byte key, DeltaID -> a per-unit change-id — and by framing
// values over DUMP/RESTORE (value-faithful, never transformed; CLAUDE.md §1.7).
// CDC is SCAN full-keyspace reconciliation (no privileged change stream, §3.2)
// with keyspace notifications as an optional accelerator. See
// .sisyphus/redis-plan.md for the milestone sequence — this file is the RM0
// skeleton (registration + version/fork probe); the Source/Sink methods are
// filled in across RM1–RM11.
package redis

import (
	"github.com/rudimk/replicare/internal/engine"
)

// EngineName is the identifier used in config and both registries.
const EngineName = "redis"

// redisEngine is the Redis implementation of engine.Engine: the factory for the
// Redis Source and Sink. Registered in init(); the config-block parser is
// registered separately in config.go (RM0.5).
type redisEngine struct{}

// Name is the engine identifier used in config and both registries.
func (redisEngine) Name() string { return EngineName }

// NewSource constructs (but does not connect) a Redis Source for an endpoint.
func (redisEngine) NewSource(cfg engine.ConnConfig) (engine.Source, error) {
	return &Source{cfg: cfg}, nil
}

// NewSink constructs (but does not connect) a Redis Sink for an endpoint.
func (redisEngine) NewSink(cfg engine.ConnConfig) (engine.Sink, error) {
	return &Sink{cfg: cfg}, nil
}

// Preflight classifies an introspected source against an introspected target
// (RM2): the RDB-version directional gate + module-presence gate, no type-coercion
// axis; every unit is a single-member acyclic component.
func (redisEngine) Preflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) *engine.PreflightReport {
	return buildPreflight(syncName, srcVersion, tgtVersion, source, target)
}

// Compile-time assertion that redisEngine satisfies the interface.
var _ engine.Engine = redisEngine{}

func init() {
	engine.Register(redisEngine{})
}
