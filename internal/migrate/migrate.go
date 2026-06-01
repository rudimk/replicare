// Package migrate is the F4 shared schema-migration runner (one abstraction,
// instantiated twice): for the Postgres state-store schema (M2) and the source
// `replicare` schema (M3). Migrations are versioned, idempotent, applied in
// order, each atomically with its version bump, and must preserve in-flight
// data across upgrades (CLAUDE.md "Schema versioning" decision; §9, §3.4).
//
// The runner is split into pure logic (Set ordering/validation, fully testable
// without a database) and a small DB port (implemented over pgx in M2/M3).
package migrate

import (
	"context"
	"fmt"
	"sort"
)

// Migration is a single versioned schema change. Statements run in order, all
// within one transaction together with the version bump.
type Migration struct {
	Version    int
	Name       string
	Statements []string
}

// Set is an ordered collection of migrations for one schema, plus the name of
// the table that records the applied version.
type Set struct {
	// Name is a human label, e.g. "statestore" or "source".
	Name string
	// VersionTable is the schema-qualified table tracking applied versions,
	// e.g. "replicare_state.schema_version".
	VersionTable string
	Migrations   []Migration
}

// Validate checks the migration set is well-formed: versions start at 1, strictly
// increase by 1 with no gaps or duplicates, and each has statements.
func (s Set) Validate() error {
	if s.VersionTable == "" {
		return fmt.Errorf("migrate %q: VersionTable is required", s.Name)
	}
	ms := s.sorted()
	for i, m := range ms {
		want := i + 1
		if m.Version != want {
			return fmt.Errorf("migrate %q: expected version %d at position %d, got %d (%q) — versions must start at 1 with no gaps/dups",
				s.Name, want, i, m.Version, m.Name)
		}
		if m.Name == "" {
			return fmt.Errorf("migrate %q: version %d has no name", s.Name, m.Version)
		}
		if len(m.Statements) == 0 {
			return fmt.Errorf("migrate %q: version %d (%q) has no statements", s.Name, m.Version, m.Name)
		}
	}
	return nil
}

// Target returns the highest version in the set (0 if empty).
func (s Set) Target() int {
	t := 0
	for _, m := range s.Migrations {
		if m.Version > t {
			t = m.Version
		}
	}
	return t
}

// Pending returns migrations with Version > current, in ascending order.
func (s Set) Pending(current int) []Migration {
	var out []Migration
	for _, m := range s.sorted() {
		if m.Version > current {
			out = append(out, m)
		}
	}
	return out
}

func (s Set) sorted() []Migration {
	ms := make([]Migration, len(s.Migrations))
	copy(ms, s.Migrations)
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	return ms
}

// Tx is the transactional surface a single migration runs against. RecordVersion
// writes the version bump inside the same transaction as the statements, so a
// migration and its version are committed atomically.
type Tx interface {
	Exec(ctx context.Context, sql string) error
	RecordVersion(ctx context.Context, versionTable string, version int, name string) error
}

// DB is the minimal database port the runner needs. The Postgres implementation
// lands in M2/M3; it must make EnsureVersionTable idempotent and InTx atomic.
type DB interface {
	EnsureVersionTable(ctx context.Context, versionTable string) error
	CurrentVersion(ctx context.Context, versionTable string) (int, error)
	InTx(ctx context.Context, fn func(Tx) error) error
}

// Apply brings db up to the set's target version, applying only pending
// migrations in order. It is idempotent: re-running after a full apply is a
// no-op. Returns the versions actually applied this call.
func Apply(ctx context.Context, db DB, s Set) ([]int, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := db.EnsureVersionTable(ctx, s.VersionTable); err != nil {
		return nil, fmt.Errorf("migrate %q: ensure version table: %w", s.Name, err)
	}
	current, err := db.CurrentVersion(ctx, s.VersionTable)
	if err != nil {
		return nil, fmt.Errorf("migrate %q: read current version: %w", s.Name, err)
	}

	var applied []int
	for _, m := range s.Pending(current) {
		m := m
		err := db.InTx(ctx, func(tx Tx) error {
			for _, stmt := range m.Statements {
				if err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}
			return tx.RecordVersion(ctx, s.VersionTable, m.Version, m.Name)
		})
		if err != nil {
			return applied, fmt.Errorf("migrate %q: applying version %d (%q): %w", s.Name, m.Version, m.Name, err)
		}
		applied = append(applied, m.Version)
	}
	return applied, nil
}
