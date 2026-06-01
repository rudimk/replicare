package migrate

import (
	"context"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	good := Set{
		Name:         "ok",
		VersionTable: "s.schema_version",
		Migrations: []Migration{
			{Version: 1, Name: "init", Statements: []string{"CREATE SCHEMA s"}},
			{Version: 2, Name: "more", Statements: []string{"CREATE TABLE s.t()"}},
		},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid set returned error: %v", err)
	}

	bad := []struct {
		name string
		set  Set
	}{
		{"no version table", Set{Name: "x", Migrations: good.Migrations}},
		{"gap", Set{Name: "x", VersionTable: "v", Migrations: []Migration{
			{Version: 1, Name: "a", Statements: []string{"x"}},
			{Version: 3, Name: "c", Statements: []string{"x"}},
		}}},
		{"dup", Set{Name: "x", VersionTable: "v", Migrations: []Migration{
			{Version: 1, Name: "a", Statements: []string{"x"}},
			{Version: 1, Name: "b", Statements: []string{"x"}},
		}}},
		{"no statements", Set{Name: "x", VersionTable: "v", Migrations: []Migration{
			{Version: 1, Name: "a"},
		}}},
		{"doesn't start at 1", Set{Name: "x", VersionTable: "v", Migrations: []Migration{
			{Version: 2, Name: "a", Statements: []string{"x"}},
		}}},
	}
	for _, tc := range bad {
		if err := tc.set.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", tc.name)
		}
	}
}

func TestPendingAndTarget(t *testing.T) {
	s := Set{Name: "x", VersionTable: "v", Migrations: []Migration{
		{Version: 2, Name: "b", Statements: []string{"x"}},
		{Version: 1, Name: "a", Statements: []string{"x"}},
		{Version: 3, Name: "c", Statements: []string{"x"}},
	}}
	if s.Target() != 3 {
		t.Fatalf("Target() = %d, want 3", s.Target())
	}
	p := s.Pending(1)
	if len(p) != 2 || p[0].Version != 2 || p[1].Version != 3 {
		t.Fatalf("Pending(1) = %+v, want versions [2,3] in order", p)
	}
	if len(s.Pending(3)) != 0 {
		t.Fatalf("Pending(3) should be empty")
	}
}

// fakeDB records executed statements and the current version, simulating an
// atomic, persistent store.
type fakeDB struct {
	version  int
	execs    []string
	tableSet bool
}

type fakeTx struct{ db *fakeDB }

func (tx *fakeTx) Exec(_ context.Context, sql string) error {
	tx.db.execs = append(tx.db.execs, sql)
	return nil
}
func (tx *fakeTx) RecordVersion(_ context.Context, _ string, v int, _ string) error {
	tx.db.version = v
	return nil
}

func (db *fakeDB) EnsureVersionTable(context.Context, string) error { db.tableSet = true; return nil }
func (db *fakeDB) CurrentVersion(context.Context, string) (int, error) {
	return db.version, nil
}
func (db *fakeDB) InTx(ctx context.Context, fn func(Tx) error) error {
	// Snapshot to emulate rollback-on-error atomicity.
	snapVer, snapExecs := db.version, len(db.execs)
	if err := fn(&fakeTx{db: db}); err != nil {
		db.version = snapVer
		db.execs = db.execs[:snapExecs]
		return err
	}
	return nil
}

func TestApplyIdempotent(t *testing.T) {
	s := Set{Name: "x", VersionTable: "v", Migrations: []Migration{
		{Version: 1, Name: "a", Statements: []string{"CREATE A"}},
		{Version: 2, Name: "b", Statements: []string{"CREATE B1", "CREATE B2"}},
	}}
	db := &fakeDB{}

	applied, err := Apply(context.Background(), db, s)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(applied) != 2 || applied[0] != 1 || applied[1] != 2 {
		t.Fatalf("applied = %v, want [1 2]", applied)
	}
	if !db.tableSet {
		t.Error("EnsureVersionTable was not called")
	}
	if db.version != 2 {
		t.Errorf("version = %d, want 2", db.version)
	}
	if len(db.execs) != 3 {
		t.Errorf("execs = %v, want 3 statements", db.execs)
	}

	// Re-apply: should be a no-op.
	applied2, err := Apply(context.Background(), db, s)
	if err != nil {
		t.Fatalf("re-Apply: %v", err)
	}
	if len(applied2) != 0 {
		t.Fatalf("re-Apply applied = %v, want none (idempotent)", applied2)
	}
	if len(db.execs) != 3 {
		t.Errorf("re-Apply changed execs to %v, want unchanged", db.execs)
	}
}

func TestApplyValidatesFirst(t *testing.T) {
	bad := Set{Name: "x", VersionTable: "v", Migrations: []Migration{
		{Version: 2, Name: "a", Statements: []string{"x"}}, // doesn't start at 1
	}}
	if _, err := Apply(context.Background(), &fakeDB{}, bad); err == nil ||
		!strings.Contains(err.Error(), "version") {
		t.Fatalf("expected validation error mentioning version, got %v", err)
	}
}
