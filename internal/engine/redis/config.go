package redis

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

const defaultPort = 6379

// Conn is the Redis connection+CDC block (the `redis:` block under an endpoint).
// Redis is topology-shaped (standalone / cluster / sentinel) rather than a single
// host, and it carries engine-specific selection (key `types`) and CDC tuning
// (SCAN/reconcile/delete-sweep/big-key/TTL/notifications) that the relational
// blocks don't need. Everything beyond the neutral Host/Port/User/Password/TLS is
// threaded to the Source/Sink through ConnConfig.Params (rc_-prefixed, consumed by
// the engine, never sent to the driver) — the same pattern as MySQL's local_infile
// hint. RM0.5 locks and validates the surface; RM1+ consume it.
type Conn struct {
	// Topology.
	Mode           string   `yaml:"mode"`            // standalone | cluster | sentinel (default standalone)
	Nodes          []string `yaml:"nodes"`           // host:port seeds (cluster/sentinel; optional standalone)
	Host           string   `yaml:"host"`            // standalone convenience (alt to a single node)
	Port           int      `yaml:"port"`            // standalone (default 6379)
	DB             int      `yaml:"db"`              // logical db index (standalone only; must be 0 in cluster)
	User           string   `yaml:"user"`            // ACL username (Redis 6+)
	Password       string   `yaml:"password"`        // use ${VAR}
	TLS            string   `yaml:"tls"`             // disable|allow|prefer|require|verify-ca|verify-full
	SentinelMaster string   `yaml:"sentinel_master"` // required when mode=sentinel
	// ReadFromReplica offloads VALUE reads to replicas; delete detection still
	// reads the master (a lagging replica would cause false deletes, redis-plan §0.5).
	ReadFromReplica bool `yaml:"read_from_replica"`

	// Engine-specific selection.
	Types []string `yaml:"types"` // optional TYPE filter (string/list/set/zset/hash/stream)

	// CDC tuning (engine-specific; consumed RM4–RM7).
	ScanCount           int             `yaml:"scan_count"`            // SCAN COUNT hint (per-call, ≠ the neutral DrainBatch max)
	ReconcileInterval   config.Duration `yaml:"reconcile_interval"`    // between rolling reconciliation passes
	DeleteSweepInterval config.Duration `yaml:"delete_sweep_interval"` // between target-vs-source delete sweeps
	BigKeyWarnBytes     config.ByteSize `yaml:"big_key_warn_bytes"`    // DUMP-and-warn above this
	BigKeyRefuseBytes   config.ByteSize `yaml:"big_key_refuse_bytes"`  // block loud above this
	Notifications       bool            `yaml:"notifications"`         // enable the keyspace-notification accelerator
	TTLMode             string          `yaml:"ttl_mode"`              // relative (default) | absttl

	Params map[string]string `yaml:"params"`
}

const (
	modeStandalone = "standalone"
	modeCluster    = "cluster"
	modeSentinel   = "sentinel"
)

// validTLSModes mirrors the libpq/MySQL spectrum for a consistent config surface.
var validTLSModes = map[string]engine.TLSMode{
	string(engine.TLSDisable):    engine.TLSDisable,
	string(engine.TLSAllow):      engine.TLSAllow,
	string(engine.TLSPrefer):     engine.TLSPrefer,
	string(engine.TLSRequire):    engine.TLSRequire,
	string(engine.TLSVerifyCA):   engine.TLSVerifyCA,
	string(engine.TLSVerifyFull): engine.TLSVerifyFull,
}

var validTTLModes = map[string]bool{"": true, "relative": true, "absttl": true}

// EngineName satisfies config.EngineConn.
func (c *Conn) EngineName() string { return EngineName }

// Validate checks the Redis connection block.
func (c *Conn) Validate(role, name string) error {
	mode := c.mode()
	switch mode {
	case modeStandalone, modeCluster, modeSentinel:
	default:
		return fmt.Errorf("%s %q: invalid redis.mode %q (valid: standalone, cluster, sentinel)", role, name, c.Mode)
	}

	// A reachable endpoint: either host/port or at least one node.
	if c.Host == "" && len(c.Nodes) == 0 {
		return fmt.Errorf("%s %q: redis needs either host/port or nodes", role, name)
	}
	if mode == modeCluster && len(c.Nodes) == 0 && c.Host == "" {
		return fmt.Errorf("%s %q: redis.mode cluster needs seed nodes", role, name)
	}
	for _, n := range c.Nodes {
		if !strings.Contains(n, ":") {
			return fmt.Errorf("%s %q: redis node %q must be host:port", role, name, n)
		}
	}

	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("%s %q: redis.port %d is out of range", role, name, c.Port)
	}
	if c.DB < 0 {
		return fmt.Errorf("%s %q: redis.db %d must be non-negative", role, name, c.DB)
	}
	// Cluster has only DB 0.
	if mode == modeCluster && c.DB != 0 {
		return fmt.Errorf("%s %q: redis.db must be 0 in cluster mode (Redis Cluster has a single keyspace)", role, name)
	}
	if mode == modeSentinel && c.SentinelMaster == "" {
		return fmt.Errorf("%s %q: redis.mode sentinel requires sentinel_master", role, name)
	}
	if c.TLS != "" {
		if _, ok := validTLSModes[c.TLS]; !ok {
			return fmt.Errorf("%s %q: invalid redis.tls %q (valid: disable, allow, prefer, require, verify-ca, verify-full)", role, name, c.TLS)
		}
	}
	if !validTTLModes[c.TTLMode] {
		return fmt.Errorf("%s %q: invalid redis.ttl_mode %q (valid: relative, absttl)", role, name, c.TTLMode)
	}
	if c.ScanCount < 0 {
		return fmt.Errorf("%s %q: redis.scan_count %d must be non-negative", role, name, c.ScanCount)
	}
	if c.BigKeyRefuseBytes > 0 && c.BigKeyWarnBytes > 0 && c.BigKeyRefuseBytes < c.BigKeyWarnBytes {
		return fmt.Errorf("%s %q: redis.big_key_refuse_bytes (%d) must be >= big_key_warn_bytes (%d)",
			role, name, c.BigKeyRefuseBytes, c.BigKeyWarnBytes)
	}
	for _, tp := range c.Types {
		switch tp {
		case "string", "list", "set", "zset", "hash", "stream":
		default:
			return fmt.Errorf("%s %q: invalid redis selection type %q (valid: string, list, set, zset, hash, stream)", role, name, tp)
		}
	}
	return nil
}

func (c *Conn) mode() string {
	if c.Mode == "" {
		return modeStandalone
	}
	return c.Mode
}

// ConnConfig produces the resolved low-level descriptor. Host/Port is the primary
// seed (first node, else host/port); the full topology + CDC tuning ride in Params
// (rc_-prefixed, engine-internal — never forwarded to the driver as options).
func (c *Conn) ConnConfig() engine.ConnConfig {
	host, port := c.Host, c.Port
	if host == "" && len(c.Nodes) > 0 {
		host, port = splitHostPort(c.Nodes[0])
	}
	if port == 0 {
		port = defaultPort
	}
	mode := c.mode()
	tls := c.TLS
	if tls == "" {
		tls = string(engine.TLSDisable)
	}
	ttl := c.TTLMode
	if ttl == "" {
		ttl = "relative"
	}

	params := map[string]string{}
	for k, v := range c.Params {
		params[k] = v
	}
	set := func(k, v string) {
		if v != "" {
			params[k] = v
		}
	}
	set(paramMode, mode)
	set(paramNodes, strings.Join(c.Nodes, ","))
	params[paramDB] = strconv.Itoa(c.DB)
	set(paramSentinelMaster, c.SentinelMaster)
	set(paramTTLMode, ttl)
	if c.ReadFromReplica {
		params[paramReadReplica] = "1"
	}
	if c.Notifications {
		params[paramNotifications] = "1"
	}
	if c.ScanCount > 0 {
		params[paramScanCount] = strconv.Itoa(c.ScanCount)
	}
	if c.ReconcileInterval > 0 {
		set(paramReconcileInterval, c.ReconcileInterval.String())
	}
	if c.DeleteSweepInterval > 0 {
		set(paramDeleteSweepInterval, c.DeleteSweepInterval.String())
	}
	if c.BigKeyWarnBytes > 0 {
		params[paramBigKeyWarn] = strconv.FormatInt(c.BigKeyWarnBytes.Bytes(), 10)
	}
	if c.BigKeyRefuseBytes > 0 {
		params[paramBigKeyRefuse] = strconv.FormatInt(c.BigKeyRefuseBytes.Bytes(), 10)
	}
	if len(c.Types) > 0 {
		set(paramTypes, strings.Join(c.Types, ","))
	}

	return engine.ConnConfig{
		Host:     host,
		Port:     port,
		Database: strconv.Itoa(c.DB),
		User:     c.User,
		Password: c.Password,
		TLS:      validTLSModes[tls],
		Params:   params,
	}
}

// splitHostPort splits "host:port" (port defaulted on parse failure).
func splitHostPort(s string) (string, int) {
	h, p, ok := strings.Cut(s, ":")
	if !ok {
		return s, defaultPort
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return h, defaultPort
	}
	return h, port
}

// parse decodes a Redis connection block, rejecting unknown keys.
func parse(block *yaml.Node) (config.EngineConn, error) {
	var c Conn
	if err := config.StrictDecode(block, &c); err != nil {
		return nil, fmt.Errorf("redis block: %w", err)
	}
	return &c, nil
}

func init() {
	config.RegisterEngine(EngineName, parse)
}
