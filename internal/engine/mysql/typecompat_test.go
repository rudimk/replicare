package mysql

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestClassifyTypes(t *testing.T) {
	cases := []struct {
		src, tgt string
		want     compatLevel
	}{
		{"int", "int", compatIdentical},
		{"int", "bigint", compatWiden},       // widening
		{"bigint", "int", compatRisky},       // narrowing
		{"int", "int unsigned", compatRisky}, // signedness
		{"varchar(50)", "varchar(100)", compatWiden},
		{"varchar(100)", "varchar(50)", compatRisky},
		{"decimal(10,2)", "decimal(12,4)", compatWiden},
		{"decimal(12,4)", "decimal(10,2)", compatRisky},
		{"float", "double", compatWiden},
		{"datetime", "datetime(6)", compatWiden},
		{"varchar(50)", "text", compatWiden},          // char->text same conceptual
		{"json", "int", compatIncompatible},           // structured -> numeric blocks
		{"int", "json", compatIncompatible},           // numeric -> structured blocks
		{"int", "datetime", compatIncompatible},       // numeric -> temporal blocks
		{"enum('a','b')", "varchar(10)", compatRisky}, // enum -> string risky
	}
	for _, c := range cases {
		got, _ := classifyTypes(c.src, c.tgt)
		if got != c.want {
			t.Errorf("classifyTypes(%q,%q) = %d, want %d", c.src, c.tgt, got, c.want)
		}
	}
}

func TestClassifyCharset(t *testing.T) {
	col := func(cs string) engine.Column { return engine.Column{DataType: "varchar(50)", Charset: cs} }
	// utf8mb4 -> utf8mb3 is risky (4-byte truncation).
	if lvl, _ := classifyColumn(col("utf8mb4"), col("utf8mb3")); lvl != compatRisky {
		t.Errorf("utf8mb4->utf8mb3 = %d, want risky", lvl)
	}
	// utf8 is an alias for utf8mb3: utf8mb4 -> utf8 also risky.
	if lvl, _ := classifyColumn(col("utf8mb4"), col("utf8")); lvl != compatRisky {
		t.Errorf("utf8mb4->utf8 = %d, want risky", lvl)
	}
	// Widening utf8mb3 -> utf8mb4 is fine (identical-or-widen).
	if lvl, _ := classifyColumn(col("utf8mb3"), col("utf8mb4")); lvl == compatRisky || lvl == compatIncompatible {
		t.Errorf("utf8mb3->utf8mb4 = %d, want ok/widen", lvl)
	}
	// Same charset, same type: identical.
	if lvl, _ := classifyColumn(col("utf8mb4"), col("utf8mb4")); lvl != compatIdentical {
		t.Errorf("same = %d, want identical", lvl)
	}
}
