package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// errNotImplemented marks a Source/Sink method that is a stub until its milestone
// lands (redis-plan RM2–RM11).
var errNotImplemented = fmt.Errorf("redis: not implemented (skeleton engine; see .sisyphus/redis-plan.md)")

// Internal ConnConfig.Params keys (rc_-prefixed) that carry the topology + CDC
// tuning from the config block (RM0.5) to the Source/Sink (RM1+). rc_ params are
// engine-internal and never forwarded to the driver.
const (
	paramMode                = "rc_mode"
	paramNodes               = "rc_nodes"
	paramDB                  = "rc_db"
	paramSentinelMaster      = "rc_sentinel_master"
	paramReadReplica         = "rc_read_replica"
	paramNotifications       = "rc_notifications"
	paramScanCount           = "rc_scan_count"
	paramReconcileInterval   = "rc_reconcile_interval"
	paramDeleteSweepInterval = "rc_delete_sweep_interval"
	paramBigKeyWarn          = "rc_big_key_warn"
	paramBigKeyRefuse        = "rc_big_key_refuse"
	paramTTLMode             = "rc_ttl_mode"
	paramTypes               = "rc_types"
)

// doer is the minimal command surface used by version/INFO probes, satisfied by
// *conn (and by go-redis clients directly, for testing).
type doer interface {
	Do(ctx context.Context, args ...any) *goredis.Cmd
}

// conn wraps a mode-appropriate go-redis client (standalone / cluster / sentinel)
// and exposes cluster topology for the per-shard operations RM4+ need. go-redis
// handles MOVED/ASK redirection and topology refresh internally for the cluster
// client. Delete detection is master-pinned (redis-plan §0.5), so read-from-replica
// routing is deferred to the read paths (RM4/RM5); RM1 stores the flag and routes
// everything to masters for correctness.
type conn struct {
	mode        string
	uc          goredis.UniversalClient
	cluster     *goredis.ClusterClient // non-nil in cluster mode (ForEachMaster)
	primaryAddr string
	readReplica bool
}

// Do delegates to the routing client.
func (c *conn) Do(ctx context.Context, args ...any) *goredis.Cmd { return c.uc.Do(ctx, args...) }

// pipeline returns a routing-aware pipeliner: in cluster mode go-redis groups the
// queued commands by owning node, so per-key RESTORE dispatches correctly.
func (c *conn) pipeline() goredis.Pipeliner { return c.uc.Pipeline() }

// Close releases the client.
func (c *conn) Close() error { return c.uc.Close() }

// masters returns the addresses of every master node: all shard masters in
// cluster mode, or the single primary otherwise. Used for the per-shard SCAN
// reconciliation fan-out (RM4+); RM1 uses it to prove topology discovery.
//
// It reads CLUSTER SHARDS from a live node rather than go-redis's cached cluster
// state — the cache can be stale (loaded mid-gossip right after cluster creation),
// so querying the server directly is authoritative and race-free.
func (c *conn) masters(ctx context.Context) ([]string, error) {
	if c.cluster == nil {
		return []string{c.primaryAddr}, nil
	}
	shards, err := c.cluster.ClusterShards(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: discover cluster masters: %w", err)
	}
	var addrs []string
	for _, sh := range shards {
		for _, n := range sh.Nodes {
			if strings.EqualFold(n.Role, "master") {
				host := n.Endpoint
				if host == "" {
					host = n.IP
				}
				addrs = append(addrs, fmt.Sprintf("%s:%d", host, n.Port))
			}
		}
	}
	return addrs, nil
}

// forEachShard runs fn against every master's command surface: each shard master
// in cluster mode (concurrently, via ForEachMaster — the per-master parallelism
// unit, redis-plan §0.5), or the single primary otherwise. The per-node client
// keeps SCAN and the following DUMP/PTTL local to one keyspace, sidestepping
// cluster cross-slot limits (redis-plan §0.5). fn must be safe for concurrent use.
func (c *conn) forEachShard(ctx context.Context, fn func(ctx context.Context, rc goredis.Cmdable) error) error {
	if c.cluster != nil {
		return c.cluster.ForEachMaster(ctx, func(ctx context.Context, cl *goredis.Client) error {
			return fn(ctx, cl)
		})
	}
	return fn(ctx, c.uc)
}

// open builds a mode-appropriate client from a resolved ConnConfig (topology +
// tuning in Params, RM0.5) and pings it.
func open(ctx context.Context, cc engine.ConnConfig) (*conn, error) {
	mode := cc.Params[paramMode]
	if mode == "" {
		mode = modeStandalone
	}
	nodes := seedAddrs(cc)
	db, err := dbIndex(cc.Database)
	if err != nil {
		return nil, err
	}
	tlsCfg := tlsConfig(cc.TLS, cc.Host)
	readReplica := cc.Params[paramReadReplica] == "1"

	c := &conn{mode: mode, primaryAddr: nodes[0], readReplica: readReplica}
	switch mode {
	case modeCluster:
		cl := goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:     nodes,
			Username:  cc.User,
			Password:  cc.Password,
			TLSConfig: tlsCfg,
		})
		c.uc, c.cluster = cl, cl
	case modeSentinel:
		c.uc = goredis.NewFailoverClient(&goredis.FailoverOptions{
			MasterName:    cc.Params[paramSentinelMaster],
			SentinelAddrs: nodes,
			Username:      cc.User,
			Password:      cc.Password,
			DB:            db,
			TLSConfig:     tlsCfg,
		})
	default: // standalone
		c.uc = goredis.NewClient(&goredis.Options{
			Addr:      nodes[0],
			Username:  cc.User,
			Password:  cc.Password,
			DB:        db,
			TLSConfig: tlsCfg,
		})
	}

	if err := c.uc.Ping(ctx).Err(); err != nil {
		_ = c.uc.Close()
		return nil, fmt.Errorf("redis: connect (%s) %s: %w", mode, nodes[0], err)
	}
	return c, nil
}

// seedAddrs returns the seed node addresses: the rc_nodes list if present, else a
// single host:port from the resolved ConnConfig.
func seedAddrs(cc engine.ConnConfig) []string {
	if raw := cc.Params[paramNodes]; raw != "" {
		return strings.Split(raw, ",")
	}
	return []string{fmt.Sprintf("%s:%d", cc.Host, cc.Port)}
}

// dbIndex parses the ConnConfig.Database string as a Redis logical DB index
// (default 0). Cluster mode enforces 0 in the config block (RM0.5).
func dbIndex(database string) (int, error) {
	if database == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(database)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("redis: invalid db index %q (want a non-negative integer)", database)
	}
	return n, nil
}

// serverVersion runs INFO server, rejects an unsupported fork (Dragonfly), and
// returns the comparable version number (§1.6). Valkey/KeyDB are allowed.
func serverVersion(ctx context.Context, c doer) (int, error) {
	info, err := infoSection(ctx, c, "server")
	if err != nil {
		return 0, err
	}
	if ok, reason := detectFork(info).supported(); !ok {
		return 0, fmt.Errorf("redis: %s", reason)
	}
	ver := infoField(info, "redis_version")
	if ver == "" {
		return 0, fmt.Errorf("redis: INFO server did not report redis_version")
	}
	return serverVersionNum(ver)
}

// infoSection runs `INFO <section>` and returns the raw text.
func infoSection(ctx context.Context, c doer, section string) (string, error) {
	res, err := c.Do(ctx, "INFO", section).Text()
	if err != nil {
		return "", fmt.Errorf("redis: INFO %s: %w", section, err)
	}
	return res, nil
}

// infoField extracts a `field:value` line from an INFO blob (CRLF-delimited).
func infoField(info, field string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimRight(line, "\r")
		if k, v, ok := strings.Cut(line, ":"); ok && k == field {
			return v
		}
	}
	return ""
}
