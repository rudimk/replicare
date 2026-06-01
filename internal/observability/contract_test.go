package observability

import (
	"strings"
	"testing"
)

func TestMetricNamesUniqueAndPrefixed(t *testing.T) {
	seen := map[string]bool{}
	for _, m := range Metrics {
		if m.Name == "" {
			t.Errorf("metric with empty name: %+v", m)
		}
		if !strings.HasPrefix(m.Name, "replicare_") {
			t.Errorf("metric %q missing replicare_ prefix", m.Name)
		}
		if m.Kind == "" {
			t.Errorf("metric %q missing kind", m.Name)
		}
		if m.Help == "" {
			t.Errorf("metric %q missing help text", m.Name)
		}
		if seen[m.Name] {
			t.Errorf("duplicate metric name %q", m.Name)
		}
		seen[m.Name] = true
	}
}

func TestSpanNamesUniqueAndPrefixed(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Spans {
		if !strings.HasPrefix(s, "replicare.") {
			t.Errorf("span %q missing replicare. prefix", s)
		}
		if seen[s] {
			t.Errorf("duplicate span name %q", s)
		}
		seen[s] = true
	}
}

// TestTargetDownContractPresent guards the CLAUDE.md §3.4 cross-channel rule: a
// degrading target must be representable in metrics, traces, and logs.
func TestTargetDownContractPresent(t *testing.T) {
	var hasMetric bool
	for _, m := range Metrics {
		if m.Name == MetricTargetUp {
			hasMetric = true
		}
	}
	if !hasMetric {
		t.Error("missing target-reachability metric")
	}
	var hasSpan bool
	for _, s := range Spans {
		if s == SpanDeltaDrain {
			hasSpan = true
		}
	}
	if !hasSpan {
		t.Error("missing delta-drain span (carries target-down trace signal)")
	}
	if EventTargetUnreachable == "" {
		t.Error("missing target-unreachable log event")
	}
}
