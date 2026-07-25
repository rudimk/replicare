// Command replicare is the replication daemon and CLI entrypoint.
//
// M0 provides only `version`; subcommands (run, validate, status, capture,
// reseed) land in later milestones (see .sisyphus plan M1/M7).
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/rudimk/replicare/internal/buildinfo"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns a process exit code. It is separated
// from main (and takes explicit writers) so it is unit-testable.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Fprintln(stdout, buildinfo.String())
		return 0
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "run":
		return runDaemon(args[1:], stdout, stderr)
	case "capture":
		return runCapture(args[1:], stdout, stderr)
	case "reseed":
		return runReseed(args[1:], stdout, stderr)
	case "help", "--help", "-h":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "replicare: unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `replicare - open-source database replicator

Usage:
  replicare <command> [flags]

Commands:
  version               Print version information
  validate <config>     Introspect and pre-flight a config without starting sync
  run <config>          Run the daemon: all syncs until SIGINT/SIGTERM
  status <config>       Report sync phase, lag, progress, and recent events
  capture ... <config>  Install or remove source-side CDC capture
  reseed <config>       Flag a target for re-copy (--sync, --target)
  help                  Show this help
`)
}
