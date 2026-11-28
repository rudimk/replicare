package mysql

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestStagingKeyIndex(t *testing.T) {
	tbl := engine.Table{
		Columns: []engine.Column{
			{Name: "u", DataType: "varchar(10)"}, {Name: "d", DataType: "date"},
			{Name: "c", DataType: "varchar(10)"}, {Name: "v", DataType: "text"},
		},
		PrimaryKey: &engine.Key{Columns: []string{"u", "d", "c"}},
	}
	if got := stagingKeyIndex(tbl, []string{"u", "d", "c", "v"}); got != ", INDEX `rc_key` (`u`, `d`, `c`)" {
		t.Errorf("composite key: %q", got)
	}
	if got := stagingKeyIndex(tbl, []string{"u", "v"}); got != "" {
		t.Errorf("key column missing from the staging: %q, want none", got)
	}
	tbl.PrimaryKey = &engine.Key{Columns: []string{"v"}}
	if got := stagingKeyIndex(tbl, []string{"u", "d", "c", "v"}); got != "" {
		t.Errorf("TEXT key column: %q, want none", got)
	}
	tbl.PrimaryKey = nil
	if got := stagingKeyIndex(tbl, []string{"u"}); got != "" {
		t.Errorf("no key: %q, want none", got)
	}
}
