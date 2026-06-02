package config

import "time"

// Engine-neutral tuning defaults, applied when a field is omitted. Conservative
// by default so we stay a polite client on a source we may not own (§4.1), and
// bounded by age so deltas can't grow forever (§3.4).
const (
	defaultDrainInterval   = 1 * time.Second
	defaultRetentionMaxAge = 24 * time.Hour
	defaultMaxConns        = 4
)
