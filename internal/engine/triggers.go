package engine

import "context"

// TargetTrigger is one trigger on a replicated target table (CLAUDE.md §4.2 "the
// target's own triggers"). A target's own triggers must not fire on replicare's writes:
// they would write a second copy of rows the source's own triggers already wrote (a
// version or audit row), change values on the way in (an updated_at stamp), or refuse
// the write outright (an append-only guard), so the target would no longer match the
// source.
type TargetTrigger struct {
	Table TableRef
	Name  string
	// Mode is when the trigger fires, as Postgres records it: "O" in ordinary sessions
	// (the default), "A" always, "R" only in replica-mode sessions, "D" never (switched
	// off).
	Mode string
	// Owned: this role may switch the trigger off and on (it is a member of the table
	// owner's role).
	Owned bool
	// Replicare marks one of replicare's own triggers (the change capture of a sync that
	// reads this database). Those are never switched off.
	Replicare bool
}

// Fires reports whether the trigger fires in a session of the given kind.
func (t TargetTrigger) Fires(replicaSession bool) bool {
	switch t.Mode {
	case "A":
		return true
	case "R":
		return replicaSession
	case "D":
		return false
	default:
		return !replicaSession
	}
}

// TriggerGuard is an optional Sink capability: keep the target's own triggers from
// firing on replicare's writes. Postgres implements it. Two ways, used together:
//
//   - replica mode on replicare's own connections, where the target lets this role use
//     it: triggers set to fire in ordinary sessions stay on for everyone else and skip
//     replicare's writes, with nothing to undo later;
//   - switching a trigger off (and back on in its earlier mode later), for those replica
//     mode does not cover.
type TriggerGuard interface {
	// TargetTriggers lists the triggers on these tables, replicare's own included.
	TargetTriggers(ctx context.Context, tables []TableRef) ([]TargetTrigger, error)
	// ReplicaModeAllowed reports whether this role may run its sessions in replica mode.
	ReplicaModeAllowed(ctx context.Context) (bool, error)
	// UseReplicaMode switches this Sink's connection, now and after every reconnect,
	// to replica mode (on) or back to ordinary (off).
	UseReplicaMode(ctx context.Context, on bool) error
	// DisableTrigger switches the trigger off, never queueing the application behind a
	// waiting lock: a short lock wait, backoff and retries, then an error.
	DisableTrigger(ctx context.Context, t TargetTrigger) error
	// RestoreTrigger switches the trigger back on in t.Mode, with the same lock
	// handling. found is false when the trigger no longer exists.
	RestoreTrigger(ctx context.Context, t TargetTrigger) (found bool, err error)
	// RestoreStatement is the statement that switches the trigger back on in t.Mode,
	// for the logs, so it can always be done by hand.
	RestoreStatement(t TargetTrigger) string
}
