package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/pipeline"
	"github.com/rudimk/replicare/internal/state"
)

// restorePausedTriggers switches back on the target triggers that replicare switched off
// for a sync that is now paused (CLAUDE.md §4.2 "the target's own triggers"): pausing a
// sync is the cutover step, after which the application writes to the target and its
// triggers must fire again. A target that a running sync in this config also writes to
// keeps them off (that sync needs them off), with a note in the log. Best effort: a
// failure is logged and tried again on the next start.
func (d *Daemon) restorePausedTriggers(ctx context.Context) {
	ts, ok := d.store.(state.TriggerStore)
	if !ok {
		return
	}
	writers := map[string]string{} // target database identity -> a running sync writing to it
	for _, s := range d.cfg.Syncs {
		if !s.IsEnabled() {
			continue
		}
		for _, t := range s.Targets {
			if id := config.ConnIdentity(d.cfg.Targets[t]); id != "" {
				writers[id] = s.Name
			}
		}
	}
	for _, s := range d.cfg.Syncs {
		if s.IsEnabled() {
			continue
		}
		saved, err := ts.ListTriggers(ctx, s.Name)
		if err != nil {
			d.log.Warn("paused sync: could not list its switched-off target triggers", slog.String("sync", s.Name), slog.Any("error", err))
			continue
		}
		seen := map[engine.TargetID]bool{}
		for _, st := range saved {
			if seen[st.Target] {
				continue
			}
			seen[st.Target] = true
			ep := d.cfg.Targets[string(st.Target)]
			if ep == nil {
				d.log.Warn("paused sync: target triggers stay switched off, the target is no longer in the config; "+
					"switch them on by hand (the statements are in the trigger.switched_off events)",
					slog.String("sync", s.Name), slog.String("target", string(st.Target)))
				continue
			}
			if other, busy := writers[config.ConnIdentity(ep)]; busy {
				d.log.Info("paused sync: target triggers stay switched off, another running sync writes to that database",
					slog.String("sync", s.Name), slog.String("target", string(st.Target)), slog.String("running_sync", other))
				continue
			}
			n, err := d.restoreTriggersOn(ctx, s.Name, st.Target, ep, ts)
			if err != nil {
				d.log.Error("paused sync: could not switch its target triggers back on; tried again on the next start, "+
					"or run `replicare triggers restore`", slog.String("sync", s.Name), slog.String("target", string(st.Target)), slog.Any("error", err))
				continue
			}
			d.log.Info(fmt.Sprintf("paused sync: switched %d target triggers back on", n),
				slog.String("sync", s.Name), slog.String("target", string(st.Target)))
		}
	}
}

func (d *Daemon) restoreTriggersOn(ctx context.Context, sync string, target engine.TargetID, ep *config.Endpoint, ts state.TriggerStore) (int, error) {
	eng, err := engine.Get(ep.Engine)
	if err != nil {
		return 0, err
	}
	sk, err := eng.NewSink(ep.Conn.ConnConfig())
	if err != nil {
		return 0, err
	}
	if err := sk.Connect(ctx); err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = sk.Close(context.Background()) }()
	g, ok := sk.(engine.TriggerGuard)
	if !ok {
		return 0, fmt.Errorf("the %s engine does not switch triggers", ep.Engine)
	}
	n, err := pipeline.RestoreTargetTriggers(ctx, ts, g, sync, target, func(ctx context.Context, e state.Event) {
		d.tel.LogEvent(ctx, e)
		_ = d.store.RecordEvent(ctx, e)
	})
	if err == nil {
		d.tel.SetTargetTriggers(sync, target, 0, 0)
	}
	return n, err
}
