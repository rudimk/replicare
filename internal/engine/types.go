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
	Generated bool // GENERATED ... STORED (excluded from inserts; MySQL: VIRTUAL or STORED)
	Identity  bool // GENERATED ... AS IDENTITY (Postgres) / AUTO_INCREMENT (MySQL)
	// AutoUpdate marks a column whose value the engine rewrites on every UPDATE
	// (MySQL `ON UPDATE CURRENT_TIMESTAMP`). Faithful apply must set it to the
	// verbatim source value so it is not silently mutated to "now" (CLAUDE.md
	// §1.7; mysql-plan §0.4). Zero (false) for engines without this behavior.
	AutoUpdate bool
	// Charset is the character-set name for character columns when the engine
	// stores it per column (MySQL: `utf8mb4`, `latin1`, ...); empty otherwise.
	// Pre-flight uses it to classify charset-narrowing (`utf8mb4`->`utf8mb3`,
	// lossy) and to flag mixed-per-column-charset tables (mysql-plan §0.1).
	Charset string
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
	// StorageEngine is the physical storage engine when the database distinguishes
	// one (MySQL: "InnoDB", "MyISAM", ...). Non-transactional engines break the
	// per-component atomic apply and source consume/track atomicity, so pre-flight
	// blocks them (mysql-plan §0.5). Empty for engines without this concept.
	StorageEngine string
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
