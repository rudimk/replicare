package redis

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
)

// pseudoKeyColumn is the single synthetic identity column of a Redis unit's
// pseudo-table. Redis keys ARE their identity, so one PK column named "key" makes
// engine.Table.HasUsableKey() true (else the neutral layer skips the unit with a
// loud warning, §3.1) without inventing real columns.
const pseudoKeyColumn = "key"

// unitRef is the pseudo-table identity for a Redis replication unit. v1 models one
// unit per sync — the connection's logical DB (cluster is DB 0) — and fans SCAN
// out across shard masters INSIDE CopyChunk/ReadDirtyKeys (RM4+), rather than
// exposing one pseudo-table per shard. This keeps a single, stable TableRef that
// round-trips through the neutral copy/apply layers (Momus M10) and matches the
// "intra-unit parallelism lives inside CopyChunk" decision (Sisyphus H1).
func unitRef(cfg engine.ConnConfig) engine.TableRef {
	db := cfg.Database
	if db == "" {
		db = "0"
	}
	return engine.TableRef{Schema: "redis", Name: "db" + db}
}

// pseudoTable builds the one synthetic table describing a unit's keyspace.
func pseudoTable(cfg engine.ConnConfig) engine.Table {
	ref := unitRef(cfg)
	return engine.Table{
		Ref:        ref,
		Columns:    []engine.Column{{Name: pseudoKeyColumn, DataType: "redis-key"}},
		PrimaryKey: &engine.Key{Name: "pk", Columns: []string{pseudoKeyColumn}, IsPrimary: true},
	}
}

// introspect synthesizes the pseudo-schema for a Redis endpoint: the unit's
// pseudo-table plus the loaded module list (into Schema.Capabilities) so the
// pre-flight module gate (RM2) can compare source vs target. sel is accepted for
// interface parity; key-glob filtering happens at SCAN time (RM4+).
func introspect(ctx context.Context, db doer, cfg engine.ConnConfig, _ engine.Selection) (*engine.Schema, error) {
	modules, err := moduleList(ctx, db)
	if err != nil {
		return nil, err
	}
	return &engine.Schema{
		Tables:       []engine.Table{pseudoTable(cfg)},
		Capabilities: engine.Capabilities{Modules: modules},
	}, nil
}

// moduleList returns the names of loaded server modules (MODULE LIST). Empty when
// no modules are loaded (the common case). Parses both the RESP2 (array-of-arrays)
// and RESP3 (array-of-maps) shapes defensively.
func moduleList(ctx context.Context, db doer) ([]string, error) {
	res, err := db.Do(ctx, "MODULE", "LIST").Result()
	if err != nil {
		return nil, fmt.Errorf("redis: MODULE LIST: %w", err)
	}
	entries, ok := res.([]any)
	if !ok {
		return nil, nil
	}
	var names []string
	for _, e := range entries {
		if name := moduleName(e); name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

// moduleName extracts the "name" field from one MODULE LIST entry (a map in RESP3,
// or a flat [k, v, k, v, ...] array in RESP2).
func moduleName(entry any) string {
	switch m := entry.(type) {
	case map[any]any:
		if v, ok := m["name"]; ok {
			return fmt.Sprint(v)
		}
	case map[string]any:
		if v, ok := m["name"]; ok {
			return fmt.Sprint(v)
		}
	case []any:
		for i := 0; i+1 < len(m); i += 2 {
			if fmt.Sprint(m[i]) == "name" {
				return fmt.Sprint(m[i+1])
			}
		}
	}
	return ""
}
