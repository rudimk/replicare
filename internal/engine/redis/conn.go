package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// errNotImplemented marks a Source/Sink method that is a stub until its milestone
// lands (redis-plan RM1–RM11). RM0 is the skeleton: connection lifecycle + the
// version/fork probe only.
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

// client is the minimal command surface RM0 needs from go-redis, satisfied by
// *goredis.Client (standalone). RM1 generalizes connection to standalone /
// Sentinel / Cluster behind the same Source/Sink methods.
type client interface {
	Do(ctx context.Context, args ...any) *goredis.Cmd
	Close() error
}

// open builds a standalone go-redis client from a resolved ConnConfig and pings
// it. Cluster/Sentinel construction is RM1 (mode is carried in the redis config
// block, RM0.5); RM0 connects standalone so the version/fork probe works against
// the harness pair.
func open(ctx context.Context, cc engine.ConnConfig) (client, error) {
	db, err := dbIndex(cc.Database)
	if err != nil {
		return nil, err
	}
	opts := &goredis.Options{
		Addr:     fmt.Sprintf("%s:%d", cc.Host, cc.Port),
		Username: cc.User,
		Password: cc.Password,
		DB:       db,
	}
	// Minimal TLS: RM1 adds the full disable->verify-full spectrum (redis-plan
	// §0.5). RM0 only distinguishes off vs on; the harness uses plaintext.
	if cc.TLS != "" && cc.TLS != engine.TLSDisable {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c := goredis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis: connect %s:%d: %w", cc.Host, cc.Port, err)
	}
	return c, nil
}

// dbIndex parses the ConnConfig.Database string as a Redis logical DB index
// (default 0). In cluster mode only DB 0 is valid (enforced in the config block,
// RM0.5).
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
func serverVersion(ctx context.Context, c client) (int, error) {
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
func infoSection(ctx context.Context, c client, section string) (string, error) {
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
