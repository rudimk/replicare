package mysql

import (
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func mref(name string) engine.TableRef { return engine.TableRef{Schema: "app", Name: name} }

func innoTable(name string, cols ...engine.Column) engine.Table {
	return engine.Table{
		Ref:           mref(name),
		Columns:       cols,
		PrimaryKey:    &engine.Key{Name: "PRIMARY", Columns: []string{"id"}, IsPrimary: true},
		StorageEngine: "InnoDB",
	}
}

func idCol() engine.Column { return engine.Column{Name: "id", DataType: "int"} }
func col(name, typ string) engine.Column {
	return engine.Column{Name: name, DataType: typ}
}

// findingsByCategory indexes a report's findings.
func hasFinding(r *engine.PreflightReport, sev engine.Severity, category string) bool {
	for _, f := range r.Findings {
		if f.Severity == sev && f.Category == category {
			return true
		}
	}
	return false
}

func TestPreflightNoKeySkip(t *testing.T) {
	src := &engine.Schema{Tables: []engine.Table{{Ref: mref("nokey"), Columns: []engine.Column{col("x", "int")}, StorageEngine: "InnoDB"}}}
	r := buildPreflight("s", 50744, 80400, src, &engine.Schema{})
	if len(r.Skipped) != 1 || !hasFinding(r, engine.SevWarn, "no-key") {
		t.Fatalf("expected a no-key skip+warn, got %+v", r)
	}
	if r.Blocked() {
		t.Error("a skipped no-key table should not block")
	}
}

func TestPreflightSecondaryUniqueBlocks(t *testing.T) {
	tgt := innoTable("orders", idCol(), col("email", "varchar(120)"))
	tgt.UniqueKeys = []engine.Key{{Name: "uq_email", Columns: []string{"email"}}}
	src := &engine.Schema{Tables: []engine.Table{innoTable("orders", idCol(), col("email", "varchar(120)"))}}
	r := buildPreflight("s", 50744, 80400, src, &engine.Schema{Tables: []engine.Table{tgt}})
	if !hasFinding(r, engine.SevBlock, "secondary-unique") || !r.Blocked() {
		t.Fatalf("expected a secondary-unique BLOCK, got %+v", r.Findings)
	}
}

func TestPreflightNonInnoDBBlocks(t *testing.T) {
	src := innoTable("legacy", idCol())
	src.StorageEngine = "MyISAM"
	r := buildPreflight("s", 50744, 80400,
		&engine.Schema{Tables: []engine.Table{src}},
		&engine.Schema{Tables: []engine.Table{innoTable("legacy", idCol())}})
	if !hasFinding(r, engine.SevBlock, "non-innodb") || !r.Blocked() {
		t.Fatalf("expected a non-innodb BLOCK, got %+v", r.Findings)
	}
}

func TestPreflightTypeIncompatibleBlocks(t *testing.T) {
	src := &engine.Schema{Tables: []engine.Table{innoTable("t", idCol(), col("data", "json"))}}
	tgt := &engine.Schema{Tables: []engine.Table{innoTable("t", idCol(), col("data", "int"))}}
	r := buildPreflight("s", 50744, 80400, src, tgt)
	if !hasFinding(r, engine.SevBlock, "type-incompatible") || !r.Blocked() {
		t.Fatalf("expected a type-incompatible BLOCK (json->int), got %+v", r.Findings)
	}
}

func TestPreflightRiskyTypeWarns(t *testing.T) {
	src := &engine.Schema{Tables: []engine.Table{innoTable("t", idCol(), col("n", "bigint"))}}
	tgt := &engine.Schema{Tables: []engine.Table{innoTable("t", idCol(), col("n", "int"))}}
	r := buildPreflight("s", 50744, 80400, src, tgt)
	if !hasFinding(r, engine.SevWarn, "type-risky") {
		t.Fatalf("expected a type-risky WARN (bigint->int), got %+v", r.Findings)
	}
	if r.Blocked() {
		t.Error("a risky narrowing should warn, not block")
	}
}

// TestPreflightMixedCharsetOK: a table mixing per-column charsets replicates
// without warning — the byte-faithful CHARACTER SET binary load (MM9) copies each
// column's raw bytes and validates them against that column's own charset, so no
// per-charset-group split is needed and the old mixed-charset warning is gone.
func TestPreflightMixedCharsetOK(t *testing.T) {
	tbl := innoTable("t", idCol(),
		engine.Column{Name: "a", DataType: "varchar(10)", Charset: "latin1"},
		engine.Column{Name: "b", DataType: "varchar(10)", Charset: "utf8mb4"})
	src := &engine.Schema{Tables: []engine.Table{tbl}}
	r := buildPreflight("s", 50744, 80400, src, src)
	if hasFinding(r, engine.SevWarn, "mixed-charset") {
		t.Fatalf("mixed-charset should no longer warn (binary load handles it): %+v", r.Findings)
	}
	if len(r.Replicable) != 1 {
		t.Fatalf("mixed-charset table should be replicable, got %+v", r.Replicable)
	}
}

func TestPreflightCyclicStrategyNoted(t *testing.T) {
	// Self-referential NOT NULL FK -> FK_CHECKS strategy note (info), no block.
	tbl := innoTable("emp", idCol(), engine.Column{Name: "mgr", DataType: "int", Nullable: false})
	tbl.ForeignKeys = []engine.ForeignKey{{Name: "fk_mgr", Child: mref("emp"), ChildCols: []string{"mgr"}, Parent: mref("emp"), ParentCols: []string{"id"}}}
	src := &engine.Schema{Tables: []engine.Table{tbl}}
	r := buildPreflight("s", 50744, 80400, src, src)
	if !hasFinding(r, engine.SevInfo, "cyclic-fk") {
		t.Fatalf("expected a cyclic-fk INFO note, got %+v", r.Findings)
	}
	if r.Blocked() {
		t.Error("MySQL NOT NULL cyclic FK is handled (FK_CHECKS+verify), not blocked")
	}
	// The note should mention the FK_CHECKS strategy for a NOT NULL cycle.
	var msg string
	for _, f := range r.Findings {
		if f.Category == "cyclic-fk" {
			msg = f.Message
		}
	}
	if !strings.Contains(msg, "FOREIGN_KEY_CHECKS=0") {
		t.Errorf("NOT NULL cyclic note = %q, want FOREIGN_KEY_CHECKS strategy", msg)
	}
}

func TestPreflightCleanSchemaOK(t *testing.T) {
	src := &engine.Schema{Tables: []engine.Table{
		innoTable("customers", idCol(), col("name", "varchar(80)")),
		func() engine.Table {
			o := innoTable("orders", idCol(), col("customer_id", "int"))
			o.ForeignKeys = []engine.ForeignKey{{Name: "fk_c", Child: mref("orders"), ChildCols: []string{"customer_id"}, Parent: mref("customers"), ParentCols: []string{"id"}}}
			return o
		}(),
	}}
	r := buildPreflight("s", 50744, 80400, src, src)
	if r.Blocked() {
		t.Fatalf("clean schema should not block: %+v", r.Findings)
	}
	if len(r.Components) != 1 || len(r.Replicable) != 2 {
		t.Errorf("expected 1 component over 2 replicable tables, got %d comps / %d replicable", len(r.Components), len(r.Replicable))
	}
}
