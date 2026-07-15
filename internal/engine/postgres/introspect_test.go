package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// These are integration tests (gated on REPLICARE_INTEGRATION=1) that exercise
// the real catalog queries against the multi-version harness. They create a
// dedicated schema, introspect it, and tear it down.

const itSchema = "rc_it"

// connectRole opens a live connection to a harness endpoint for integration use.
func connectRole(t *testing.T, ctx context.Context, role string) *pgx.Conn {
	t.Helper()
	conn, err := connect(ctx, harnessConn(t, role)) // harnessConn skips if integration is off
	if err != nil {
		t.Fatalf("connect %s: %v", role, err)
	}
	return conn
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec failed: %v\nSQL: %s", err, sql)
	}
}

func dropITSchema(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	mustExec(t, ctx, conn, "DROP SCHEMA IF EXISTS "+itSchema+" CASCADE")
}

func schemaByRef(s *engine.Schema) map[string]engine.Table {
	m := map[string]engine.Table{}
	for _, t := range s.Tables {
		m[t.Ref.String()] = t
	}
	return m
}

// TestIntrospectModernTarget covers the full feature matrix against the modern
// target: PK, composite PK, unique key, FK, self-reference, identity column,
// generated column, no-PK table, and native partitioning.
func TestIntrospectModernTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := connectRole(t, ctx, "target")
	defer conn.Close(ctx)

	dropITSchema(t, ctx, conn)
	defer dropITSchema(t, ctx, conn)

	ddl := []string{
		"CREATE SCHEMA " + itSchema,
		`CREATE TABLE rc_it.customers (
			id int PRIMARY KEY,
			email text UNIQUE,
			created timestamptz
		)`,
		`CREATE TABLE rc_it.orders (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			customer_id int NOT NULL REFERENCES rc_it.customers(id),
			total numeric(10,2),
			note text
		)`,
		`CREATE TABLE rc_it.order_items (
			order_id bigint,
			sku text,
			qty int,
			PRIMARY KEY (order_id, sku),
			FOREIGN KEY (order_id) REFERENCES rc_it.orders(id)
		)`,
		`CREATE TABLE rc_it.employees (
			id int PRIMARY KEY,
			manager_id int REFERENCES rc_it.employees(id)
		)`,
		`CREATE TABLE rc_it.gen (
			id int PRIMARY KEY,
			w int, h int,
			area int GENERATED ALWAYS AS (w * h) STORED
		)`,
		`CREATE TABLE rc_it.nopk (a int, b int)`,
		`CREATE TABLE rc_it.events (
			id bigint,
			ts date,
			PRIMARY KEY (id, ts)
		) PARTITION BY RANGE (ts)`,
		`CREATE TABLE rc_it.events_2026 PARTITION OF rc_it.events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')`,
	}
	for _, s := range ddl {
		mustExec(t, ctx, conn, s)
	}

	version, err := serverVersion(ctx, conn)
	if err != nil {
		t.Fatalf("serverVersion: %v", err)
	}
	schema, err := introspectConn(ctx, conn, version, engine.Selection{Include: []string{itSchema + ".*"}})
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	tbls := schemaByRef(schema)

	// Partition child must be excluded; parent present and flagged.
	if _, ok := tbls["rc_it.events_2026"]; ok {
		t.Error("partition child rc_it.events_2026 should be excluded")
	}
	if ev, ok := tbls["rc_it.events"]; !ok || !ev.Partitioned {
		t.Errorf("rc_it.events should be present and partitioned: %+v", ev)
	}

	// customers: PK {id}, unique {email}.
	cust := tbls["rc_it.customers"]
	if cust.PrimaryKey == nil || !eq(cust.PrimaryKey.Columns, []string{"id"}) {
		t.Errorf("customers PK = %+v", cust.PrimaryKey)
	}
	if len(cust.UniqueKeys) != 1 || !eq(cust.UniqueKeys[0].Columns, []string{"email"}) {
		t.Errorf("customers unique = %+v", cust.UniqueKeys)
	}

	// orders: identity id, FK to customers (non-deferrable), numeric type text.
	orders := tbls["rc_it.orders"]
	if !columnBy(orders, "id").Identity {
		t.Error("orders.id should be an identity column")
	}
	if got := columnBy(orders, "total").DataType; got != "numeric(10,2)" {
		t.Errorf("orders.total type = %q, want numeric(10,2)", got)
	}
	if len(orders.ForeignKeys) != 1 || orders.ForeignKeys[0].Parent.String() != "rc_it.customers" {
		t.Errorf("orders FKs = %+v", orders.ForeignKeys)
	}
	if orders.ForeignKeys[0].Deferrable {
		t.Error("orders FK should be non-deferrable")
	}

	// order_items: composite PK.
	oi := tbls["rc_it.order_items"]
	if oi.PrimaryKey == nil || !eq(oi.PrimaryKey.Columns, []string{"order_id", "sku"}) {
		t.Errorf("order_items PK = %+v", oi.PrimaryKey)
	}

	// employees: self-referential FK.
	emp := tbls["rc_it.employees"]
	if len(emp.ForeignKeys) != 1 || emp.ForeignKeys[0].Parent.String() != "rc_it.employees" {
		t.Errorf("employees self-ref FK = %+v", emp.ForeignKeys)
	}

	// gen: area is a generated column.
	gen := tbls["rc_it.gen"]
	if !columnBy(gen, "area").Generated {
		t.Error("gen.area should be a generated column")
	}

	// nopk: no usable key.
	if tbls["rc_it.nopk"].HasUsableKey() {
		t.Error("rc_it.nopk should have no usable key")
	}
}

// TestIntrospectOldSource verifies the version-gated queries run cleanly against
// the old source and that identity/generated flags default to false there.
func TestIntrospectOldSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := connectRole(t, ctx, "source")
	defer conn.Close(ctx)

	dropITSchema(t, ctx, conn)
	defer dropITSchema(t, ctx, conn)

	ddl := []string{
		"CREATE SCHEMA " + itSchema,
		`CREATE TABLE rc_it.customers (id int PRIMARY KEY, email text UNIQUE)`,
		`CREATE TABLE rc_it.orders (
			id int PRIMARY KEY,
			customer_id int NOT NULL REFERENCES rc_it.customers(id),
			total numeric(10,2)
		)`,
		`CREATE TABLE rc_it.nopk (a int)`,
	}
	for _, s := range ddl {
		mustExec(t, ctx, conn, s)
	}

	version, err := serverVersion(ctx, conn)
	if err != nil {
		t.Fatalf("serverVersion: %v", err)
	}
	schema, err := introspectConn(ctx, conn, version, engine.Selection{Include: []string{itSchema + ".*"}})
	if err != nil {
		t.Fatalf("introspect old source: %v", err)
	}
	tbls := schemaByRef(schema)

	cust := tbls["rc_it.customers"]
	if cust.PrimaryKey == nil || !eq(cust.PrimaryKey.Columns, []string{"id"}) {
		t.Errorf("customers PK = %+v", cust.PrimaryKey)
	}
	orders := tbls["rc_it.orders"]
	if len(orders.ForeignKeys) != 1 {
		t.Errorf("orders should have 1 FK, got %+v", orders.ForeignKeys)
	}
	// On an old server nothing is identity/generated.
	for _, c := range orders.Columns {
		if c.Identity || c.Generated {
			t.Errorf("old server column %s should not be identity/generated", c.Name)
		}
	}
	if tbls["rc_it.nopk"].HasUsableKey() {
		t.Error("rc_it.nopk should have no usable key")
	}
}

func columnBy(t engine.Table, name string) engine.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return engine.Column{}
}
