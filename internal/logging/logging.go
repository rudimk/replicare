// Package logging bootstraps structured logging on top of log/slog (CLAUDE.md
// §10). Output is JSON by default (machine-friendly) with a text option for
// local development.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Format selects the slog handler output format.
type Format string

const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

// Setup builds a *slog.Logger for the given level and format, installs it as the
// default logger, and returns it. Unrecognized levels default to info;
// unrecognized formats default to JSON.
func Setup(level string, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	switch format {
	case FormatText:
		h = slog.NewTextHandler(os.Stderr, opts)
	default:
		h = slog.NewJSONHandler(os.Stderr, opts)
	}

	l := slog.New(h)
	slog.SetDefault(l)
	return l
}

// parseLevel maps a level string to slog.Level, defaulting to Info.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
