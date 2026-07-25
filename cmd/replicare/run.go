package main

import (
	"context"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/daemon"
	"github.com/rudimk/replicare/internal/logging"
)

// runDaemon implements `replicare run <config>`: it loads the config, starts the
// operator HTTP surface (/metrics, /status, /healthz), and runs every sync until
// SIGINT/SIGTERM, which triggers a graceful drain+checkpoint shutdown (CLAUDE.md
// §8). Exit codes: 0 clean shutdown, 1 runtime error, 2 usage/config.
func runDaemon(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: replicare run <config.yml>")
		return 2
	}
	cfg, err := config.Load(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return 2
	}
	log := logging.Setup(cfg.Logging.Level, logging.Format(cfg.Logging.Format))

	d, err := daemon.New(cfg, log)
	if err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return 2
	}

	// SIGINT/SIGTERM cancels the run context → graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	servers, err := d.StartHTTP()
	if err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = servers.Shutdown(shutdownCtx)
	}()
	log.Info("replicare running",
		"metrics_addr", servers.MetricsAddr, "status_addr", servers.StatusAddr)

	if err := d.Run(ctx); err != nil {
		log.Error("daemon exited with error", "error", err.Error())
		return 1
	}
	log.Info("shutdown complete")
	return 0
}
