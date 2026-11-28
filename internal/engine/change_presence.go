package engine

import "context"

// ChangePresence is an optional Source capability for trigger-CDC engines: telling, for
// many tables at once, which have any queued change at all.
//
// The streaming pass purges every table's change queue and the monitor reads every
// table's backlog, one table at a time, a few round trips each. On a large schema most
// queues are empty, so most of that is wasted: on 820 tables with about 50 queues in use,
// the purge alone took minutes per pass, mostly network round trips (decision, 0.8.5, from
// a production catch-up). With this, callers skip the tables with nothing queued. For such
// a table skipping changes nothing: there is nothing to purge, its backlog is zero, and it
// cannot be over a retention cap.
type ChangePresence interface {
	// TablesWithChanges reports, for each of the given tables, whether its change queue
	// holds any row. A table it cannot answer for (not captured, say) is reported true, so
	// the caller treats it as before. A change queued after the call is seen next time.
	TablesWithChanges(ctx context.Context, tables []TableRef) (map[TableRef]bool, error)
}
