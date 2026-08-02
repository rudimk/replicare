package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestPreflightEndToEnd runs the full introspect->Preflight path against the live
// pair: an identical customers<-orders schema on both sides passes clean with one
// FK component, and a MyISAM source table blocks.
func TestPreflightEndToEnd(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	src := connectSource(t, ctx)
	tgt := &Sink{cfg: tgtCfg()}
	if err := tgt.Connect(ctx); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	t.Cleanup(func() { _ = tgt.Close(context.Background()) })

	ddl := []string{
		"DROP DATABASE IF EXISTS rc_it",
		"CREATE DATABASE rc_it",
		"CREATE TABLE rc_it.customers (id INT PRIMARY KEY, name VARCHAR(80)) ENGINE=InnoDB",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, customer_id INT NOT NULL, note VARCHAR(200), " +
			"CONSTRAINT fk_c FOREIGN KEY (customer_id) REFERENCES rc_it.customers(id)) ENGINE=InnoDB",
	}
	mustExec(t, ctx, src.db, ddl...)
	mustExec(t, ctx, tgt.db, ddl...)
	t.Cleanup(func() { _, _ = tgt.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	sel := engine.Selection{Include: []string{"rc_it.*"}}
	ss, err := src.Introspect(ctx, sel)
	if err != nil {
		t.Fatalf("introspect source: %v", err)
	}
	ts, err := tgt.Introspect(ctx, sel)
	if err != nil {
		t.Fatalf("introspect target: %v", err)
	}
	sv, _ := src.ServerVersion(ctx)
	tv, _ := tgt.ServerVersion(ctx)

	r := mysqlEngine{}.Preflight("s1", sv, tv, ss, ts)
	if r.Blocked() {
		t.Fatalf("identical schema should not block: %+v", r.Findings)
	}
	if len(r.Components) != 1 || len(r.Replicable) != 2 {
		t.Errorf("want 1 component / 2 replicable, got %d / %d", len(r.Components), len(r.Replicable))
	}
	// Parent before child in the component order.
	comp := r.Components[0]
	if comp.Order[0].Name != "customers" || comp.Order[1].Name != "orders" {
		t.Errorf("component order = %v, want [customers orders]", comp.Order)
	}

	// A MyISAM source table blocks.
	mustExec(t, ctx, src.db, "CREATE TABLE rc_it.legacy (id INT PRIMARY KEY) ENGINE=MyISAM")
	ss2, _ := src.Introspect(ctx, sel)
	r2 := mysqlEngine{}.Preflight("s1", sv, tv, ss2, ts)
	if !r2.Blocked() {
		t.Error("a MyISAM source table should block pre-flight")
	}
}
