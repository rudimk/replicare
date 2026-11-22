package engine

import "context"

// TrackSweeper is an optional Source capability for trigger-CDC engines: removing
// track rows that versions before 0.7.0 left behind when they purged a delta (they
// never deleted a delta's track rows). Purge now deletes a delta's track rows with it,
// so leftovers only exist on sources that ran an earlier version, and once removed
// they never come back.
//
// It is deliberately NOT part of Purge: Purge runs inside every streaming pass, and
// clearing millions of leftovers there made each pass wait on large deletes on a busy
// source. The pipeline calls SweepLeftoverTrack from a background goroutine on a
// connection of its own instead, so the sweep never delays applying changes.
type TrackSweeper interface {
	// SweepLeftoverTrack removes one bounded window of leftover track rows for each
	// target of table t, and returns how many rows it removed (0 once none are left).
	SweepLeftoverTrack(ctx context.Context, t TableRef, targets []TargetID) (removed int64, err error)
}
