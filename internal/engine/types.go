// Package engine defines the engine-agnostic contracts that the replication
// pipeline is built on. Postgres is the first implementation; MySQL and Redis
// are intended to slot in behind these same interfaces (see CLAUDE.md §5, §13).
//
// The vocabulary here is deliberately database-neutral. Anything Postgres- (or
// engine-) specific lives inside the concrete Source/Sink implementations, not
// in these types.
package engine

import "fmt"

// TableRef identifies a table by schema-qualified name.
type TableRef struct {
	Schema string
	Name   string
}

// String returns the schema-qualified name, e.g. "public.orders".
func (t TableRef) String() string {
	if t.Schema == "" {
		return t.Name
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

// Column describes one column of a source or target table.
//
// Generated and Identity matter for faithful apply (CLAUDE.md §4.2): generated
// columns are excluded from inserts (the target regenerates them) and identity
// columns require OVERRIDING SYSTEM VALUE so source key values carry over.
type Column struct {
	Name      string
	DataType  string // canonical type name as reported by the source catalog
	Nullable  bool
	Generated bool // GENERATED ... STORED
	Identity  bool // GENERATED ... AS IDENTITY
}

// Key is a primary or unique key (an ordered set of columns).
type Key struct {
	Name      string
	Columns   []string
	IsPrimary bool
}

// ForeignKey is a single FK edge: Child(cols) -> Parent(cols).
//
// Deferrable governs the cyclic-FK initial-copy strategy (CLAUDE.md §4.1): a
// DEFERRABLE cyclic FK can be loaded inside one transaction with SET CONSTRAINTS
// DEFERRED; a NOT NULL non-deferrable cyclic FK is rejected at pre-flight.
type ForeignKey struct {
	Name       string
	Child      TableRef
	ChildCols  []string
	Parent     TableRef
	ParentCols []string
	Deferrable bool
}

// Table is the introspected description of one table.
type Table struct {
	Ref         TableRef
	Columns     []Column
	PrimaryKey  *Key  // nil if the table has no primary key
	UniqueKeys  []Key // usable unique keys (fallback identity)
	ForeignKeys []ForeignKey
	Partitioned bool // native declarative partition parent
}

// HasUsableKey reports whether the table has a PK or any unique key usable as a
// replication identity. Tables without one are skipped+warned (CLAUDE.md §3.1).
func (t Table) HasUsableKey() bool {
	return t.PrimaryKey != nil || len(t.UniqueKeys) > 0
}

// Schema is the set of introspected tables for a source or target.
type Schema struct {
	Tables []Table
}

// Selection describes which tables a sync replicates: include/exclude lists with
// schema globs (CLAUDE.md §11). Evaluated against the source catalog.
type Selection struct {
	Include []string // glob patterns, e.g. "public.*"
	Exclude []string // glob patterns, e.g. "*_audit"
}

// TargetID identifies one target within a sync (fan-out is supported; CLAUDE.md
// §6). Track/cursor state is keyed per (target, table).
type TargetID string
