package redis

import (
	"fmt"
	"strconv"
	"strings"
)

// serverVersionNum converts a Redis version string (e.g. "6.2.14", "7.4.0") into
// a comparable integer using the same major*10000 + minor*100 + patch scheme the
// Postgres and MySQL engines use (§1.6): 6.2.14 -> 60214, 7.4.0 -> 70400. This is
// the coarse signal the RDB-version directional pre-flight gate branches on (RM2):
// RESTORE rejects a payload whose RDB version exceeds the target's, so a
// newer-source -> older-target pair is blocked (redis-plan §0.2).
func serverVersionNum(v string) (int, error) {
	core := v
	if i := strings.IndexAny(core, "- "); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 {
		return 0, fmt.Errorf("redis: unparseable server version %q", v)
	}
	num := 0
	weights := []int{10000, 100, 1}
	for i := 0; i < 3; i++ {
		n := 0
		if i < len(parts) {
			p, err := strconv.Atoi(parts[i])
			if err != nil {
				return 0, fmt.Errorf("redis: unparseable server version %q: %w", v, err)
			}
			n = p
		}
		num += n * weights[i]
	}
	return num, nil
}

// redisFork is a detected server family. v1 supports the Redis RDB/RESP lineage;
// forks whose DUMP/RESTORE RDB compatibility differs are handled per redis-plan
// RQ-1: Valkey shares the format and is ALLOWED; KeyDB is best-effort; Dragonfly's
// RDB compatibility is unverified and is BLOCKED.
type redisFork int

const (
	forkRedis     redisFork = iota // Redis proper (or an unlabeled compatible)
	forkValkey                     // Valkey — shares Redis RDB format; allowed
	forkKeyDB                      // KeyDB — best-effort
	forkDragonfly                  // Dragonfly — RDB compat unverified; blocked in v1
)

// detectFork classifies the server family from INFO fields. Redis proper reports
// no fork marker; Valkey/KeyDB/Dragonfly advertise themselves (in `redis_version`
// build suffixes or dedicated INFO fields like `valkey_version`/`dragonfly_...`),
// so we sniff the lower-cased blob conservatively.
func detectFork(infoServer string) redisFork {
	s := strings.ToLower(infoServer)
	switch {
	case strings.Contains(s, "dragonfly"):
		return forkDragonfly
	case strings.Contains(s, "valkey"):
		return forkValkey
	case strings.Contains(s, "keydb"):
		return forkKeyDB
	default:
		return forkRedis
	}
}

// supported reports whether a detected fork is usable in v1, with an actionable
// reason when it is not.
func (f redisFork) supported() (bool, string) {
	switch f {
	case forkDragonfly:
		return false, "Dragonfly is not supported in v1: its DUMP/RESTORE RDB-format " +
			"compatibility with Redis is unverified, and replicare's transport is DUMP/RESTORE " +
			"(redis-plan RQ-1). Use a Redis or Valkey server."
	default:
		// Redis, Valkey (shares the RDB format), and KeyDB (best-effort) are allowed.
		return true, ""
	}
}

func (f redisFork) String() string {
	switch f {
	case forkValkey:
		return "valkey"
	case forkKeyDB:
		return "keydb"
	case forkDragonfly:
		return "dragonfly"
	default:
		return "redis"
	}
}
