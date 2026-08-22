package health

import (
	"testing"
	"time"
)

func TestBeatStaleness(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	b := New(30 * time.Second)
	b.now = func() time.Time { return now }

	// Nothing registered → healthy.
	if k, stale := b.Stale(); stale {
		t.Fatalf("empty beat reported stale: %q", k)
	}

	b.Register("s1/dst") // seeded at now
	if _, stale := b.Stale(); stale {
		t.Fatal("freshly registered sync should be fresh")
	}

	// Advance past the threshold without a mark → stale.
	now = now.Add(31 * time.Second)
	if k, stale := b.Stale(); !stale || k != "s1/dst" {
		t.Fatalf("expected s1/dst stale, got %q stale=%v", k, stale)
	}

	// A mark refreshes it.
	b.Mark("s1/dst")
	if _, stale := b.Stale(); stale {
		t.Fatal("marked sync should be fresh again")
	}

	// A second sync that keeps marking must not mask a stalled one.
	b.Register("s2/dst")
	now = now.Add(31 * time.Second)
	b.Mark("s2/dst") // s2 fresh, s1 now stale
	k, stale := b.Stale()
	if !stale || k != "s1/dst" {
		t.Fatalf("expected the stalled s1/dst, got %q stale=%v", k, stale)
	}
}

func TestBeatDisabled(t *testing.T) {
	b := New(0) // disabled
	b.Register("s1/dst")
	b.now = func() time.Time { return time.Unix(2_000_000, 0) } // far future
	if k, stale := b.Stale(); stale {
		t.Fatalf("threshold<=0 must disable staleness, got %q", k)
	}
}
