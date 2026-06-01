package logging

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"":        slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSetupReturnsLoggerAndSetsDefault(t *testing.T) {
	l := Setup("info", FormatJSON)
	if l == nil {
		t.Fatal("Setup returned nil logger")
	}
	if slog.Default() == nil {
		t.Fatal("Setup did not install a default logger")
	}
	// text format path
	if l2 := Setup("debug", FormatText); l2 == nil {
		t.Fatal("Setup(text) returned nil logger")
	}
}
