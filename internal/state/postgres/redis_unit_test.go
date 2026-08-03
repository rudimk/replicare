package statepg

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// RM3 — the Redis engine ships NO new StateStore code; it reuses this neutral
// Postgres backend by overloading the neutral vocabulary. These tests prove the
// store round-trips Redis-shaped state exactly:
//
//   - TableRef -> a replication *unit* (the connection's logical DB, `redis.dbN`),
//     not a schema.table. One unit per sync (per-shard SCAN fan-out is internal
//     to the Source, so the persisted identity stays a single stable ref).
//   - CopyProgress is COARSE: a unit's SCAN snapshot is atomic, so there is no
//     keyset watermark or sparse completed-range set — Watermark/Completed stay
//     nil and Done flips false->true when the unit finishes (redis-plan RM3/RM4).
//   - Cursor.LastDelta (engine.DeltaID) is a per-unit *change-id*, not a delta
//     table row id; NeedsReseed is the same retention/reseed flag.
//   - Ownership + reseed are engine-neutral and reused verbatim.
//
// The store never interprets these fields, so a Redis unit persists and reloads
// through the identical code path as a Postgres table — that is the property
// RM3 asserts. Gated on the Postgres harness (REPLICARE_INTEGRATION=1).

// redisUnit mirrors the redis engine's unitRef output ({Schema:"redis",
// Name:"db"+db}) without importing the engine package into the state tests.
func redisUnit(db string) engine.TableRef {
	if db == "" {
		db = "0"
	}
	return engine.TableRef{Schema: "redis", Name: "db" + db}
}

func TestRedisSyncDefRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	// A Redis sync: engine-neutral SyncDef, but Selection carries KEY globs (not
	// schema.table globs) and the source is a Redis endpoint reference.
	def := state.SyncDef{
		Name:    "redis-sync",
		Source:  "redis:primary",
		Targets: []engine.TargetID{"redis-replica"},
		Selection: engine.Selection{
			Include: []string{"user:*", "session:*"},
			Exclude: []string{"*:tmp"},
		},
	}
	if err := s.PutSync(ctx, def); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	got, err := reopen(t, ctx).GetSync(ctx, "redis-sync")
	if err != nil {
		t.Fatalf("GetSync: %v", err)
	}
	if !reflect.DeepEqual(got, def) {
		t.Errorf("Redis sync def mismatch:\n got  %+v\n want %+v", got, def)
	}
}

func TestRedisUnitCopyProgressCoarse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "r", Source: "redis:src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	unit := redisUnit("0")

	// Absent -> fresh, not done (the unit's snapshot hasn't run yet).
	fresh, err := s.LoadCopyProgress(ctx, "r", unit)
	if err != nil {
		t.Fatalf("LoadCopyProgress(absent): %v", err)
	}
	if fresh.Done || fresh.Watermark != nil || len(fresh.Completed) != 0 {
		t.Errorf("absent unit progress should be fresh, got %+v", fresh)
	}

	// Coarse checkpoint: the SCAN snapshot is atomic, so completion is a single
	// Done flag with NO watermark and NO completed-range set.
	done := state.CopyProgress{Table: unit, Done: true}
	if err := s.SaveCopyProgress(ctx, "r", done); err != nil {
		t.Fatalf("SaveCopyProgress: %v", err)
	}
	got, err := reopen(t, ctx).LoadCopyProgress(ctx, "r", unit)
	if err != nil {
		t.Fatalf("LoadCopyProgress: %v", err)
	}
	if !reflect.DeepEqual(got, done) {
		t.Errorf("coarse unit progress mismatch:\n got  %+v\n want %+v", got, done)
	}
	if got.Watermark != nil || len(got.Completed) != 0 {
		t.Errorf("Redis unit must checkpoint coarsely (nil watermark/ranges), got %+v", got)
	}
}

func TestRedisUnitCursorChangeIDAndReseed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "r", Source: "redis:src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	unit := redisUnit("2")

	// LastDelta is a per-unit change-id (a reconciliation-pass counter for lag),
	// and NeedsReseed is the retention/reseed flag — both round-trip verbatim.
	saved := state.Cursor{
		Target:      "dst",
		Table:       unit,
		Phase:       state.PhaseStreaming,
		LastDelta:   7,
		NeedsReseed: true,
	}
	if err := s.SaveCursor(ctx, "r", saved); err != nil {
		t.Fatalf("SaveCursor: %v", err)
	}
	got, err := reopen(t, ctx).LoadCursor(ctx, "r", "dst", unit)
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if !reflect.DeepEqual(got, saved) {
		t.Errorf("Redis unit cursor mismatch:\n got  %+v\n want %+v", got, saved)
	}

	// Clearing the reseed flag (post-reseed) also round-trips.
	saved.NeedsReseed = false
	if err := s.SaveCursor(ctx, "r", saved); err != nil {
		t.Fatalf("SaveCursor(clear reseed): %v", err)
	}
	got2, err := reopen(t, ctx).LoadCursor(ctx, "r", "dst", unit)
	if err != nil {
		t.Fatalf("LoadCursor after clear: %v", err)
	}
	if got2.NeedsReseed {
		t.Errorf("reseed flag should clear, got %+v", got2)
	}
}

func TestRedisSyncOwnershipExcludesSecondDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two independent daemons (separate pools) against one state DB — the Redis
	// sync must be single-active exactly like a relational sync.
	s := openTestStore(t, ctx)
	s2 := reopen(t, ctx)

	held, release, err := s.Acquire(ctx, "redis-sync")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !held {
		t.Fatal("first daemon should hold the Redis sync lock")
	}

	held2, release2, err := s2.Acquire(ctx, "redis-sync")
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if held2 {
		if release2 != nil {
			_ = release2()
		}
		t.Fatal("a second daemon must not acquire a held Redis sync")
	}

	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	held3, release3, err := s2.Acquire(ctx, "redis-sync")
	if err != nil {
		t.Fatalf("re-Acquire after release: %v", err)
	}
	if !held3 {
		t.Fatal("Redis sync lock should be acquirable after release")
	}
	if release3 != nil {
		_ = release3()
	}
}
