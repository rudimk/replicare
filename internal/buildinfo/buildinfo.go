// Package buildinfo exposes version/build metadata. Version/Commit/Date are
// overridable at build time via -ldflags; otherwise we fall back to the Go module
// build info (VCS stamp) recorded by the toolchain.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// These are set via -ldflags "-X github.com/rudimk/replicare/internal/buildinfo.Version=..."
var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// String returns a human-readable one-line version summary.
func String() string {
	commit := Commit
	if commit == "" {
		commit = vcsRevision()
	}
	if commit == "" {
		commit = "unknown"
	}
	date := Date
	if date == "" {
		date = "unknown"
	}
	return fmt.Sprintf("replicare %s (commit %s, built %s, %s)", Version, commit, date, runtime.Version())
}

// vcsRevision pulls the git revision from the embedded build info, if present.
func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			if len(s.Value) > 12 {
				return s.Value[:12]
			}
			return s.Value
		}
	}
	return ""
}
