package redis

import (
	"context"
	"log/slog"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// RM7 — the OPTIONAL keyspace-notification accelerator. Notifications are
// fire-and-forget Pub/Sub: value-less, lost on disconnect, node-local in cluster
// (CLAUDE.md §3.2). We therefore NEVER trust them for correctness — the rolling
// reconciliation SCAN (RM5) and delete sweep (RM6) remain the source of truth. When
// enabled they merely SHORTEN latency: a flagged key is drained ahead of the next
// full-scan boundary. A value-less event only marks a key dirty; the current value
// is always re-read (DUMP), so a `set` and a `del` flow through the identical
// re-read → RESTORE-or-DEL apply path (the RM6 staged-tracking ApplyTx), Op-agnostic.
//
// InstallCapture subscribes (a privilege-gated no-op when off); RemoveCapture and
// Close tear the subscription down. On a subscription gap the accelerator forces a
// full reconciliation pass — but even a missed gap is caught by the next scan/sweep.

// psuber is the minimal PSUBSCRIBE surface, satisfied by *goredis.Client and by the
// UniversalClient (standalone/sentinel routing client).
type psuber interface {
	PSubscribe(ctx context.Context, channels ...string) *goredis.PubSub
}

// notifier holds the per-unit priority dirty-set fed by keyspace-notification
// subscribers (one goroutine per master). All fields are mutex-guarded; the
// subscriber goroutines are cancelled and waited on teardown.
type notifier struct {
	mu       sync.Mutex
	priority map[string]struct{}
	gapped   bool // a subscription closed/reconnected → force a full reconciliation

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	subs    []*goredis.PubSub
	clients []*goredis.Client // per-master subscriber clients we own (cluster)
}

func (n *notifier) flag(key string) {
	n.mu.Lock()
	n.priority[key] = struct{}{}
	n.mu.Unlock()
}

func (n *notifier) markGap() {
	n.mu.Lock()
	n.gapped = true
	n.mu.Unlock()
}

// drain removes and returns up to max flagged keys plus whether a subscription gap
// has occurred since the last drain (both cleared).
func (n *notifier) drain(max int) (keys []string, gapped bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	gapped = n.gapped
	n.gapped = false
	for k := range n.priority {
		keys = append(keys, k)
		delete(n.priority, k)
		if len(keys) >= max {
			break
		}
	}
	return keys, gapped
}

// consume feeds one master's keyspace events into the priority set until ctx is
// cancelled or the subscription channel closes (a gap → forces a full pass).
func (n *notifier) consume(ctx context.Context, ps *goredis.PubSub) {
	defer n.wg.Done()
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				n.markGap()
				return
			}
			// For __keyevent@<db>__:<event> the payload IS the affected key.
			if msg.Payload != "" {
				n.flag(msg.Payload)
			}
		}
	}
}

// subscribers returns one PSUBSCRIBE surface per master (node-local in cluster, per
// redis-plan §0.5) plus the clients we own and must close.
func (c *conn) subscribers(ctx context.Context, cc engine.ConnConfig) (subs []psuber, owned []*goredis.Client, err error) {
	if c.cluster == nil {
		return []psuber{c.uc}, nil, nil
	}
	addrs, err := c.masters(ctx)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := tlsConfig(cc.TLS, cc.Host)
	for _, addr := range addrs {
		cl := goredis.NewClient(&goredis.Options{
			Addr:      addr,
			Username:  cc.User,
			Password:  cc.Password,
			TLSConfig: tlsCfg,
		})
		subs = append(subs, cl)
		owned = append(owned, cl)
	}
	return subs, owned, nil
}

// InstallCapture enables the notification accelerator: best-effort CONFIG SET to
// turn on keyspace events, then one PSUBSCRIBE per master to __keyevent@<db>__:*.
// It is a NO-OP when notifications are disabled in config (the default), so the
// engine falls back cleanly to pure reconciliation. Idempotent.
func (s *Source) InstallCapture(ctx context.Context, _ []engine.TableRef) error {
	if s.db == nil {
		return errNotConnected
	}
	if s.cfg.Params[paramNotifications] != "1" {
		return nil // accelerator off — pure reconciliation (RM5/RM6)
	}
	if s.notify != nil {
		return nil
	}
	db := s.cfg.Database
	if db == "" {
		db = "0"
	}
	pattern := "__keyevent@" + db + "__:*"

	// Best-effort enable on EVERY master (events are node-local in cluster). Managed
	// tiers (ElastiCache) set this via a parameter group and reject CONFIG SET — that
	// is fine; we rely on the external config then.
	enable := func(ctx context.Context, rc goredis.Cmdable) error {
		if err := rc.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
			slog.Warn("redis: could not enable keyspace notifications via CONFIG SET; relying on external config",
				slog.String("event", "redis.notify_config"), slog.Any("error", err))
		}
		return nil
	}
	_ = s.db.forEachShard(ctx, enable)

	subs, owned, err := s.db.subscribers(ctx, s.cfg)
	if err != nil {
		return err
	}
	n := &notifier{priority: map[string]struct{}{}, clients: owned}
	nctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	for _, sub := range subs {
		ps := sub.PSubscribe(nctx, pattern)
		n.subs = append(n.subs, ps)
		n.wg.Add(1)
		go n.consume(nctx, ps)
	}
	s.notify = n
	return nil
}

// RemoveCapture stops the notification subscribers and closes the clients we own.
// A no-op when the accelerator was never installed.
func (s *Source) RemoveCapture(context.Context, []engine.TableRef) error {
	s.stopNotifier()
	return nil
}

// stopNotifier cancels the subscriber goroutines, closes the subscriptions and any
// owned clients, and waits for the goroutines to exit. Safe to call repeatedly.
func (s *Source) stopNotifier() {
	if s.notify == nil {
		return
	}
	n := s.notify
	s.notify = nil
	if n.cancel != nil {
		n.cancel()
	}
	for _, ps := range n.subs {
		_ = ps.Close()
	}
	n.wg.Wait()
	for _, cl := range n.clients {
		_ = cl.Close()
	}
}
