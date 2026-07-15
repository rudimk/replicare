package statepg

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

func TestAdvisoryKeyDeterministicAndDistinct(t *testing.T) {
	a1, a2 := advisoryKey("s1"), advisoryKey("s1")
	if a1 != a2 {
		t.Error("advisoryKey should be deterministic")
	}
	if advisoryKey("s1") == advisoryKey("s2") {
		t.Error("distinct sync names should (almost surely) yield distinct keys")
	}
}

func TestAcquireExclusiveAcrossProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// s and s2 are independent Stores (separate pools) — two "processes" against
	// the same state DB.
	s := openTestStore(t, ctx)
	s2 := reopen(t, ctx)

	held, release, err := s.Acquire(ctx, "sync-a")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !held {
		t.Fatal("first Acquire should hold the lock")
	}

	// A second process must not be able to acquire the same sync.
	held2, release2, err := s2.Acquire(ctx, "sync-a")
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if held2 {
		if release2 != nil {
			_ = release2()
		}
		t.Fatal("second process must not acquire a held sync")
	}

	// After the owner releases, the second process can acquire it.
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	held3, release3, err := s2.Acquire(ctx, "sync-a")
	if err != nil {
		t.Fatalf("re-Acquire after release: %v", err)
	}
	if !held3 {
		t.Fatal("lock should be acquirable after the owner releases")
	}
	if err := release3(); err != nil {
		t.Fatalf("release3: %v", err)
	}
}

func TestAcquireDifferentSyncsIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	heldA, releaseA, err := s.Acquire(ctx, "a")
	if err != nil || !heldA {
		t.Fatalf("Acquire a: held=%v err=%v", heldA, err)
	}
	defer releaseA()
	heldB, releaseB, err := s.Acquire(ctx, "b")
	if err != nil || !heldB {
		t.Fatalf("Acquire b: held=%v err=%v", heldB, err)
	}
	defer releaseB()
}

func TestRecordEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	ev := state.Event{
		Sync:    "s1",
		Target:  "dst",
		Table:   engine.TableRef{Schema: "public", Name: "orders"},
		Level:   "ERROR",
		Event:   "target.needs_reseed",
		Message: "retention cap exceeded",
		Attrs:   map[string]any{"delta_backlog": 1234, "age_seconds": 90},
	}
	if err := s.RecordEvent(ctx, ev); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}

	var (
		sync, target, schema, table, level, event, message string
		backlog                                            int
	)
	err := s.pool.QueryRow(ctx, `
		SELECT sync, target, schema_name, table_name, level, event, message,
		       (attrs->>'delta_backlog')::int
		FROM replicare_state.events ORDER BY id DESC LIMIT 1`).
		Scan(&sync, &target, &schema, &table, &level, &event, &message, &backlog)
	if err != nil {
		t.Fatalf("read back event: %v", err)
	}
	if sync != "s1" || target != "dst" || schema != "public" || table != "orders" {
		t.Errorf("event identity wrong: %s/%s/%s.%s", sync, target, schema, table)
	}
	if level != "ERROR" || event != "target.needs_reseed" || message != "retention cap exceeded" {
		t.Errorf("event fields wrong: %s %s %q", level, event, message)
	}
	if backlog != 1234 {
		t.Errorf("attrs.delta_backlog = %d, want 1234", backlog)
	}
}

func TestRecordEventMinimal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	// No sync/target/table/attrs — optional columns should be NULL, not "".
	if err := s.RecordEvent(ctx, state.Event{Level: "INFO", Event: "capture.installed"}); err != nil {
		t.Fatalf("RecordEvent minimal: %v", err)
	}
	var syncNull, attrsNull bool
	err := s.pool.QueryRow(ctx, `
		SELECT sync IS NULL, attrs IS NULL
		FROM replicare_state.events ORDER BY id DESC LIMIT 1`).Scan(&syncNull, &attrsNull)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !syncNull || !attrsNull {
		t.Errorf("optional columns should be NULL: syncNull=%v attrsNull=%v", syncNull, attrsNull)
	}
}
