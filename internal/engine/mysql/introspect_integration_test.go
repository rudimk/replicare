package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func mustExec(t *testing.T, ctx context.Context, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// connectSource opens a connected Source against the harness source, resetting
// the rc_it database so each test starts clean.
func connectSource(t *testing.T, ctx context.Context) *Source {
	t.Helper()
	s := &Source{cfg: srcCfg()}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	mustExec(t, ctx, s.db,
		"DROP DATABASE IF EXISTS rc_it",
		"CREATE DATABASE rc_it",
	)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS rc_it") })
	return s
}

func findTable(s *engine.Schema, name string) *engine.Table {
	for i := range s.Tables {
		if s.Tables[i].Ref.Name == name {
			return &s.Tables[i]
		}
	}
	return nil
}

// TestIntrospectRichSchema exercises the MM1a introspection surface: columns
// (generated/auto_increment/on-update-timestamp), PK + secondary unique, FK edge
// (incl. the child->parent direction), storage engine, and selection globs.
func TestIntrospectRichSchema(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := connectSource(t, ctx)

	mustExec(t, ctx, s.db,
		`CREATE TABLE rc_it.customers (
			id INT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(120) NOT NULL UNIQUE,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			full_name VARCHAR(160) GENERATED ALWAYS AS (UPPER(email)) STORED
		) ENGINE=InnoDB`,
		`CREATE TABLE rc_it.orders (
			id INT PRIMARY KEY,
			customer_id INT NOT NULL,
			note VARCHAR(200),
			CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES rc_it.customers(id)
		) ENGINE=InnoDB`,
		`CREATE TABLE rc_it.legacy (id INT PRIMARY KEY) ENGINE=MyISAM`,
	)

	sc, err := s.Introspect(ctx, engine.Selection{Include: []string{"rc_it.*"}})
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if len(sc.Tables) != 3 {
		t.Fatalf("got %d tables, want 3", len(sc.Tables))
	}

	cust := findTable(sc, "customers")
	if cust == nil {
		t.Fatal("customers missing")
	}
	if cust.StorageEngine != "InnoDB" {
		t.Errorf("customers engine = %q, want InnoDB", cust.StorageEngine)
	}
	if cust.PrimaryKey == nil || len(cust.PrimaryKey.Columns) != 1 || cust.PrimaryKey.Columns[0] != "id" {
		t.Errorf("customers PK = %+v, want [id]", cust.PrimaryKey)
	}
	if len(cust.UniqueKeys) != 1 || cust.UniqueKeys[0].Columns[0] != "email" {
		t.Errorf("customers unique keys = %+v, want [email]", cust.UniqueKeys)
	}
	col := func(name string) engine.Column {
		for _, c := range cust.Columns {
			if c.Name == name {
				return c
			}
		}
		t.Fatalf("column %q missing", name)
		return engine.Column{}
	}
	if c := col("id"); !c.Identity {
		t.Error("id should be Identity (auto_increment)")
	}
	if c := col("updated_at"); !c.AutoUpdate {
		t.Error("updated_at should be AutoUpdate (ON UPDATE CURRENT_TIMESTAMP)")
	}
	if c := col("full_name"); !c.Generated {
		t.Error("full_name should be Generated (STORED)")
	}

	ord := findTable(sc, "orders")
	if ord == nil || len(ord.ForeignKeys) != 1 {
		t.Fatalf("orders FK = %+v, want 1 edge", ord)
	}
	fk := ord.ForeignKeys[0]
	if fk.Child.Name != "orders" || fk.ChildCols[0] != "customer_id" ||
		fk.Parent.Name != "customers" || fk.ParentCols[0] != "id" || fk.Deferrable {
		t.Errorf("orders FK = %+v, want orders.customer_id -> customers.id, not deferrable", fk)
	}

	legacy := findTable(sc, "legacy")
	if legacy == nil || legacy.StorageEngine != "MyISAM" {
		t.Errorf("legacy engine = %v, want MyISAM (for the MM1b non-InnoDB block)", legacy)
	}

	// Selection excludes work over db.table.
	sc2, err := s.Introspect(ctx, engine.Selection{Include: []string{"rc_it.*"}, Exclude: []string{"*.legacy"}})
	if err != nil {
		t.Fatalf("introspect (exclude): %v", err)
	}
	if findTable(sc2, "legacy") != nil {
		t.Error("legacy should be excluded")
	}
}

// TestSessionCanonFaithfulness proves the pinned session (§0.4): a zero-date
// lands verbatim while an oversize value on another column halts loud under the
// SAME strict-safe sql_mode, and character_set_results=binary returns raw bytes.
func TestSessionCanonFaithfulness(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := connectSource(t, ctx)

	mustExec(t, ctx, s.db,
		`CREATE TABLE rc_it.dt (id INT PRIMARY KEY, d DATE NOT NULL, s VARCHAR(3) NOT NULL) ENGINE=InnoDB`,
	)

	// Zero-date lands verbatim (NO_ZERO_DATE/NO_ZERO_IN_DATE absent, STRICT kept).
	if _, err := s.db.ExecContext(ctx, "INSERT INTO rc_it.dt VALUES (1, '0000-00-00', 'ok')"); err != nil {
		t.Fatalf("zero-date insert should succeed under strict-safe sql_mode: %v", err)
	}
	var got []byte
	if err := s.db.QueryRowContext(ctx, "SELECT d FROM rc_it.dt WHERE id=1").Scan(&got); err != nil {
		t.Fatalf("read zero-date: %v", err)
	}
	if string(got) != "0000-00-00" {
		t.Errorf("zero-date round-trip = %q, want 0000-00-00", got)
	}

	// Oversize value on another column STILL halts loud (STRICT preserved).
	if _, err := s.db.ExecContext(ctx, "INSERT INTO rc_it.dt VALUES (2, '2020-01-01', 'toolong')"); err == nil {
		t.Error("oversize varchar should halt loud under STRICT, but succeeded (STRICT was dropped?)")
	}
}
