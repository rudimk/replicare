package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TablesWithChanges tells queued from empty change queues, in several statements when
// there are more tables than one statement checks, and answers true for a table it does
// not capture; Purge of a queue it calls empty deletes nothing (skipping it is safe).
func TestTablesWithChanges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
	var refs []engine.TableRef
	for i := 0; i < 5; i++ {
		mustExec(t, ctx, s.db, fmt.Sprintf("CREATE TABLE rc_it.t%d (id INT PRIMARY KEY, v INT) ENGINE=InnoDB", i))
		refs = append(refs, engine.TableRef{Schema: "rc_it", Name: fmt.Sprintf("t%d", i)})
	}
	if err := s.InstallCapture(ctx, refs); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, s.db, "INSERT INTO rc_it.t1 VALUES (1, 1)", "INSERT INTO rc_it.t4 VALUES (1, 1), (2, 2)")
	notCaptured := engine.TableRef{Schema: "rc_it", Name: "nope"}

	defer func(n int) { presencePerStatement = n }(presencePerStatement)
	presencePerStatement = 2 // 5 tables: three statements
	got, err := s.TablesWithChanges(ctx, append(append([]engine.TableRef{}, refs...), notCaptured))
	if err != nil {
		t.Fatal(err)
	}
	want := map[engine.TableRef]bool{refs[0]: false, refs[1]: true, refs[2]: false, refs[3]: false, refs[4]: true, notCaptured: true}
	for ref, w := range want {
		if got[ref] != w {
			t.Errorf("%v: has changes = %v, want %v (all: %v)", ref, got[ref], w, got)
		}
	}

	st, err := s.Purge(ctx, refs[0], []engine.TargetID{"dst"}, engine.RetentionPolicy{MaxAgeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if st.DeltasPurged != 0 || len(st.TargetsReseeded) != 0 {
		t.Fatalf("purge of an empty queue = %+v, want nothing", st)
	}
	bl, err := s.DeltaBacklog(ctx, refs[0], "dst")
	if err != nil {
		t.Fatal(err)
	}
	if bl.Rows != 0 || bl.HasBacklog || bl.OldestAge != 0 {
		t.Fatalf("backlog of an empty queue = %+v, want zero (what the monitor uses when it skips it)", bl)
	}
}
