package redis

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rudimk/replicare/internal/config"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register postgres (state_store)
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

const standaloneConfig = `
state_store:
  engine: postgres
  postgres: {host: localhost, database: replicare_state, user: replicare, password: pw, sslmode: disable}
sources:
  cache:
    engine: redis
    redis:
      host: src
      port: 6390
      db: 3
      user: repl
      password: pw
      tls: require
      read_from_replica: true
      scan_count: 500
      reconcile_interval: 5s
      delete_sweep_interval: 1m
      big_key_warn_bytes: 8MB
      big_key_refuse_bytes: 512MB
      notifications: true
      ttl_mode: absttl
      types: [string, hash]
targets:
  warehouse:
    engine: redis
    redis: {host: wh, port: 6391, tls: disable}
syncs:
  - name: cache-to-wh
    source: cache
    targets: [warehouse]
    include: ["user:*"]
    exclude: ["user:*:tmp"]
`

// TestRedisConfigLoadsEndToEnd proves a full standalone Redis config dispatches
// through the neutral loader to the redis block parser and threads topology + CDC
// tuning into ConnConfig.Params (RM0.5).
func TestRedisConfigLoadsEndToEnd(t *testing.T) {
	c, err := config.Load(writeTemp(t, standaloneConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	src, ok := c.Sources["cache"]
	if !ok || src.Engine != "redis" {
		t.Fatalf("source cache missing or wrong engine: %+v", src)
	}
	cc := src.Conn.ConnConfig()
	if cc.Host != "src" || cc.Port != 6390 || cc.TLS != "require" {
		t.Errorf("resolved conn = %+v, want host=src port=6390 tls=require", cc)
	}
	want := map[string]string{
		paramMode:                "standalone",
		paramDB:                  "3",
		paramReadReplica:         "1",
		paramNotifications:       "1",
		paramScanCount:           "500",
		paramReconcileInterval:   "5s",
		paramDeleteSweepInterval: "1m0s",
		paramBigKeyWarn:          "8000000",
		paramBigKeyRefuse:        "512000000",
		paramTTLMode:             "absttl",
		paramTypes:               "string,hash",
	}
	for k, v := range want {
		if cc.Params[k] != v {
			t.Errorf("param %s = %q, want %q (all: %v)", k, cc.Params[k], v, cc.Params)
		}
	}
}

const clusterConfig = `
state_store:
  engine: postgres
  postgres: {host: localhost, database: replicare_state, user: replicare, password: pw, sslmode: disable}
sources:
  cache:
    engine: redis
    redis:
      mode: cluster
      nodes: ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"]
targets:
  dst:
    engine: redis
    redis: {mode: cluster, nodes: ["127.0.0.1:7100"]}
syncs:
  - {name: c, source: cache, targets: [dst], include: ["*"]}
`

func TestRedisClusterConfigLoads(t *testing.T) {
	c, err := config.Load(writeTemp(t, clusterConfig))
	if err != nil {
		t.Fatalf("Load cluster: %v", err)
	}
	cc := c.Sources["cache"].Conn.ConnConfig()
	if cc.Host != "127.0.0.1" || cc.Port != 7000 {
		t.Errorf("cluster primary seed = %s:%d, want 127.0.0.1:7000", cc.Host, cc.Port)
	}
	if cc.Params[paramMode] != "cluster" || cc.Params[paramNodes] != "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002" {
		t.Errorf("cluster topology not threaded: %v", cc.Params)
	}
}

func TestRedisConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		conn Conn
		ok   bool
	}{
		{"standalone-host", Conn{Host: "h", Port: 6379}, true},
		{"cluster-nodes", Conn{Mode: "cluster", Nodes: []string{"h:7000"}}, true},
		{"cluster-nonzero-db", Conn{Mode: "cluster", Nodes: []string{"h:7000"}, DB: 2}, false},
		{"cluster-no-nodes", Conn{Mode: "cluster"}, false},
		{"sentinel-no-master", Conn{Mode: "sentinel", Nodes: []string{"h:26379"}}, false},
		{"sentinel-ok", Conn{Mode: "sentinel", Nodes: []string{"h:26379"}, SentinelMaster: "mymaster"}, true},
		{"bad-mode", Conn{Mode: "bogus", Host: "h"}, false},
		{"bad-tls", Conn{Host: "h", TLS: "sslv3"}, false},
		{"bad-ttl", Conn{Host: "h", TTLMode: "eternal"}, false},
		{"bad-node", Conn{Mode: "cluster", Nodes: []string{"noport"}}, false},
		{"bad-type", Conn{Host: "h", Types: []string{"geo"}}, false},
		{"no-endpoint", Conn{}, false},
		{"refuse-lt-warn", Conn{Host: "h", BigKeyWarnBytes: 100, BigKeyRefuseBytes: 50}, false},
	}
	for _, c := range cases {
		err := c.conn.Validate("source", "x")
		if c.ok != (err == nil) {
			t.Errorf("%s: Validate err=%v, wantOk=%v", c.name, err, c.ok)
		}
	}
}

func TestRedisConfigRejectsUnknownKeys(t *testing.T) {
	body := `
state_store:
  engine: postgres
  postgres: {host: localhost, database: s, user: u, password: pw, sslmode: disable}
sources:
  cache:
    engine: redis
    redis: {host: h, bogus_key: 1}
targets:
  dst:
    engine: redis
    redis: {host: h2}
syncs:
  - {name: c, source: cache, targets: [dst], include: ["*"]}
`
	if _, err := config.Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected an unknown-key rejection from StrictDecode")
	}
}
