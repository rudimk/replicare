package postgres

import (
	"github.com/rudimk/replicare/internal/engine"
)

// pgEngine is the Postgres implementation of engine.Engine: the factory for the
// Postgres Source and Sink (CLAUDE.md §5, §13). Registered in init() alongside
// the config-block parser.
type pgEngine struct{}

// Name is the engine identifier used in config and both registries.
func (pgEngine) Name() string { return EngineName }

// NewSource constructs (but does not connect) a Postgres Source for an endpoint.
func (pgEngine) NewSource(cfg engine.ConnConfig) (engine.Source, error) {
	return &Source{cfg: cfg}, nil
}

// NewSink constructs (but does not connect) a Postgres Sink for an endpoint.
func (pgEngine) NewSink(cfg engine.ConnConfig) (engine.Sink, error) {
	return &Sink{cfg: cfg}, nil
}

// Preflight classifies an introspected source against an introspected target
// (M1). The rich internal analysis (FK components, cyclic-FK strategies,
// per-column compatibility) is retained for later milestones and projected to
// the engine-neutral report here.
func (pgEngine) Preflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) *engine.PreflightReport {
	return buildPreflight(syncName, srcVersion, tgtVersion, source, target).toReport()
}

// Compile-time assertion that pgEngine satisfies the interface.
var _ engine.Engine = pgEngine{}

func init() {
	engine.Register(pgEngine{})
}
