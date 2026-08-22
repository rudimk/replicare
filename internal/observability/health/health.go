// Package health tracks per-(sync,target) streaming liveness so the operator
// health probe can report a WEDGED daemon — one that is alive but no longer
// iterating its drain loop (a hung query on a dropped socket, a deadlock) — and
// let Kubernetes restart it.
//
// This is deliberately distinct from target reachability (the
// replicare_target_up metric): liveness means "the streaming loop is still
// cycling", NOT "replication is succeeding". A daemon that is correctly retrying
// against a down target — completing each pass with a handled error — is HEALTHY
// and must not be restarted. Only a loop that stops iterating is unhealthy.
package health

import (
	"sort"
	"sync"
	"time"
)

// Beat records, per registered key, the last time a streaming iteration
// completed. A key is stale if it has not iterated within the threshold.
type Beat struct {
	threshold time.Duration
	now       func() time.Time
	mu        sync.Mutex
	last      map[string]time.Time
}

// New returns a Beat with the given staleness threshold. A threshold <= 0
// disables staleness detection (Stale always reports healthy).
func New(threshold time.Duration) *Beat {
	return &Beat{threshold: threshold, now: time.Now, last: map[string]time.Time{}}
}

// Register starts tracking a key, seeding its last-seen to now so a slow first
// pass does not read as immediately stale. Call it when streaming begins (after
// the initial copy), so a long bring-up is never mistaken for a wedge.
func (b *Beat) Register(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last[key] = b.now()
}

// Mark records that a key just completed a streaming iteration.
func (b *Beat) Mark(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last[key] = b.now()
}

// Stale returns the first key (deterministically ordered) that has not iterated
// within the threshold and true, or "" and false when every registered key is
// fresh — or when staleness detection is disabled (threshold <= 0) or nothing is
// registered yet.
func (b *Beat) Stale() (string, bool) {
	if b.threshold <= 0 {
		return "", false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := b.now().Add(-b.threshold)
	keys := make([]string, 0, len(b.last))
	for k := range b.last {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if b.last[k].Before(cutoff) {
			return k, true
		}
	}
	return "", false
}
