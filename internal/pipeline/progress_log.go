package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// Progress lines in the logs, so an operator can tell what each sync is doing from the
// logs alone: capture being installed (which takes minutes on a large schema), the initial
// copy, and streaming. They go to the log only, not the state store's events table: they
// repeat every minute and would only grow it.

// progressLogInterval is the gap between two progress lines for one sync target.
var progressLogInterval = time.Minute

// captureLogInterval is the gap between two lines while capture is being installed.
var captureLogInterval = 15 * time.Second

// streamProgress accumulates what streaming did since the last progress line.
type streamProgress struct {
	mu       sync.Mutex
	since    time.Time
	passes   int
	failed   int
	lastErr  string
	applied  int64
	backlog  map[engine.TableRef]engine.DeltaBacklog
	lastSeen time.Time // last healthy pass (backlog read)
}

// logOnly writes an event to the log, not to the state store.
func (s *Syncer) logOnly(ctx context.Context, level, event, msg string) {
	if s.Tel == nil {
		return
	}
	s.Tel.LogEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: level, Event: event, Message: msg})
}

// withCaptureProgress returns a context whose capture install logs how far it has got,
// at most every captureLogInterval, and logs the start right away.
func (s *Syncer) withCaptureProgress(ctx context.Context, total int) context.Context {
	s.logOnly(ctx, "INFO", observability.EventCaptureInstalling,
		fmt.Sprintf("installing capture on %s (a trigger and a change queue on each, one table at a time)", tablesWord(total)))
	var mu sync.Mutex
	last := time.Now()
	return engine.WithCaptureProgress(ctx, func(done, n int) {
		mu.Lock()
		defer mu.Unlock()
		if done < n && time.Since(last) < captureLogInterval {
			return
		}
		last = time.Now()
		s.logOnly(ctx, "INFO", observability.EventCaptureInstalling,
			fmt.Sprintf("installing capture: %d of %d tables done", done, n))
	})
}

// logCopyProgress logs the initial copy's progress, at most every progressLogInterval.
func (s *Syncer) logCopyProgress(ctx context.Context, last *time.Time) {
	if s.Tel == nil || time.Since(*last) < progressLogInterval {
		return
	}
	st, ok := s.Tel.CopyStatus(s.Name)
	if !ok {
		return
	}
	*last = time.Now()
	parts := []string{fmt.Sprintf("initial copy: %d of %d tables done, %s of about %s rows copied",
		st.TablesDone, st.Tables, groupDigits(st.RowsCopied), groupDigits(st.RowsEstimate))}
	if st.HasProgress {
		parts = append(parts, fmt.Sprintf("%.1f%% of the work", st.Progress*100))
	}
	switch {
	case !st.HasETA:
		parts = append(parts, "no time estimate yet")
	case st.ETASeconds < 0:
		parts = append(parts, "no progress in the last 10 minutes")
	case st.ETASeconds > 0:
		parts = append(parts, "about "+roughDuration(time.Duration(st.ETASeconds)*time.Second)+" to go")
	}
	level := "INFO"
	if st.HasETA && st.ETASeconds < 0 {
		level = "WARN"
	}
	s.logOnly(ctx, level, observability.EventCopyProgress, strings.Join(parts, "; "))
}

// notePass records one streaming pass for the next progress line.
func (s *Syncer) notePass(consumed int, backlogs map[engine.TableRef]engine.DeltaBacklog, err error) {
	p := &s.progress
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.since.IsZero() {
		p.since = time.Now()
	}
	p.passes++
	p.applied += int64(consumed)
	if backlogs != nil {
		p.backlog = backlogs
		p.lastSeen = time.Now()
	}
	if err != nil {
		p.failed++
		p.lastErr = err.Error()
	}
}

// noteBacklogs records the latest backlogs for the progress line without counting a pass.
func (s *Syncer) noteBacklogs(backlogs map[engine.TableRef]engine.DeltaBacklog) {
	p := &s.progress
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.since.IsZero() {
		p.since = time.Now()
	}
	p.backlog = backlogs
	p.lastSeen = time.Now()
}

// logStreamProgress logs what streaming did since the last line, at most every
// progressLogInterval: rows applied, passes (and failures), and the backlog.
func (s *Syncer) logStreamProgress(ctx context.Context) {
	p := &s.progress
	p.mu.Lock()
	if p.since.IsZero() || time.Since(p.since) < progressLogInterval {
		p.mu.Unlock()
		return
	}
	since, passes, failed, lastErr, applied := time.Since(p.since), p.passes, p.failed, p.lastErr, p.applied
	backlog, lastSeen := p.backlog, p.lastSeen
	p.since, p.passes, p.failed, p.lastErr, p.applied = time.Now(), 0, 0, "", 0
	p.mu.Unlock()

	msg := fmt.Sprintf("streaming: applied %s rows in the last %s (%d passes)",
		groupDigits(applied), roughDuration(since), passes)
	var rows int64
	var oldest time.Duration
	waiting := 0
	for _, bl := range backlog {
		rows += bl.Rows
		if bl.Rows > 0 {
			waiting++
		}
		oldest = max(oldest, bl.OldestAge)
	}
	switch {
	case lastSeen.IsZero():
		msg += "; backlog not read yet"
	case rows == 0:
		msg += "; caught up, nothing waiting"
	default:
		msg += fmt.Sprintf("; backlog %s rows in %s, oldest waiting %s",
			groupDigits(rows), tablesWord(waiting), roughDuration(oldest))
	}
	level := "INFO"
	if failed > 0 {
		level = "WARN"
		msg += fmt.Sprintf("; %d of the passes failed (last: %s)", failed, lastErr)
	}
	s.logOnly(ctx, level, observability.EventStreamProgress, msg)
}

// groupDigits formats n with thousands separators: 1234567 -> "1,234,567".
func groupDigits(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// roughDuration rounds a duration for people: whole seconds under a minute, whole
// minutes under a day, then days and hours.
func roughDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return d.Round(time.Second).String()
	case d < 24*time.Hour:
		return d.Round(time.Minute).String()
	default:
		days := int(d / (24 * time.Hour))
		hours := int((d % (24 * time.Hour)).Round(time.Hour) / time.Hour)
		return fmt.Sprintf("%dd%dh", days, hours)
	}
}
