// Package mysql is the MySQL implementation of the engine interfaces
// (engine.Engine / Source / Sink), the second engine after Postgres. It follows
// the same trigger-based, no-binlog CDC model (CLAUDE.md §3.1) and reuses the
// engine-neutral copy/apply/reseed/pipeline layers unchanged; only the MySQL
// dialect specifics live here. See .sisyphus/mysql-plan.md for the milestone
// sequence — this file is the MM0 skeleton (registration + version probe); the
// Source/Sink methods are filled in across MM1–MM5.
package mysql

import (
	"errors"

	"github.com/rudimk/replicare/internal/engine"
)

// EngineName is the identifier used in config and both registries.
const EngineName = "mysql"

// errNotImplemented marks a Source/Sink method that is a stub until its
// milestone lands. It is deliberately loud: the MM0 engine registers and probes
// versions but cannot yet run a sync, and any attempt to use an unfinished path
// should fail obviously rather than silently no-op.
var errNotImplemented = errors.New("mysql: not implemented (skeleton engine; see .sisyphus/mysql-plan.md)")

// mysqlEngine is the MySQL implementation of engine.Engine: the factory for the
// MySQL Source and Sink. Registered in init(); the config-block parser is
// registered separately in config.go (MM0.5).
type mysqlEngine struct{}

// Name is the engine identifier used in config and both registries.
func (mysqlEngine) Name() string { return EngineName }

// NewSource constructs (but does not connect) a MySQL Source for an endpoint.
func (mysqlEngine) NewSource(cfg engine.ConnConfig) (engine.Source, error) {
	return &Source{cfg: cfg}, nil
}

// NewSink constructs (but does not connect) a MySQL Sink for an endpoint.
func (mysqlEngine) NewSink(cfg engine.ConnConfig) (engine.Sink, error) {
	return &Sink{cfg: cfg}, nil
}

// Preflight classifies an introspected source against an introspected target
// (MM1b). The MM0 skeleton returns an empty report; real classification (type
// compatibility, FK components, MySQL-specific blocks) lands in MM1b.
func (mysqlEngine) Preflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) *engine.PreflightReport {
	return &engine.PreflightReport{Sync: syncName, SourceVersion: srcVersion, TargetVersion: tgtVersion}
}

// Compile-time assertion that mysqlEngine satisfies the interface.
var _ engine.Engine = mysqlEngine{}

func init() {
	engine.Register(mysqlEngine{})
}
