package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

// serverVersionNum converts a MySQL version string (e.g. "5.7.44", "8.4.0",
// "8.0.36-0ubuntu0.22.04.1") into a comparable integer using the same
// major*10000 + minor*100 + patch scheme the Postgres engine uses (§1.6): 5.7.44
// -> 50744, 8.4.0 -> 80400. Version-tolerant code paths branch on this number.
func serverVersionNum(v string) (int, error) {
	// Strip any suffix after the first '-' (distro build metadata) or ' '.
	core := v
	if i := strings.IndexAny(core, "- "); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 {
		return 0, fmt.Errorf("mysql: unparseable server version %q", v)
	}
	num := 0
	// major*10000 + minor*100 + patch, each clamped to two digits of weight.
	weights := []int{10000, 100, 1}
	for i := 0; i < 3; i++ {
		n := 0
		if i < len(parts) {
			p, err := strconv.Atoi(parts[i])
			if err != nil {
				return 0, fmt.Errorf("mysql: unparseable server version %q: %w", v, err)
			}
			n = p
		}
		num += n * weights[i]
	}
	return num, nil
}

// isMariaDB reports whether a version string denotes MariaDB rather than MySQL.
// MariaDB is out of scope for v1 (.sisyphus/mysql-plan.md, Open Q4): its dialect
// has forked from MySQL's, so we detect and refuse it rather than silently
// mis-driving it. MariaDB advertises itself in the version string (e.g.
// "10.11.6-MariaDB").
func isMariaDB(version string) bool {
	return strings.Contains(strings.ToLower(version), "mariadb")
}
