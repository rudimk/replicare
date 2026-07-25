package prom

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/rudimk/replicare/internal/observability"
)

// emitOnePerMetric writes one sample for every contract metric with all its
// declared labels set, so Gather/scrape expose the full series set.
func emitOnePerMetric(r *Registry) {
	for _, m := range observability.Metrics {
		lbls := prometheus.Labels{}
		for _, l := range m.Labels {
			lbls[l] = "x"
		}
		switch m.Kind {
		case observability.Counter:
			r.Counter(m.Name).With(lbls).Inc()
		case observability.Gauge:
			r.Gauge(m.Name).With(lbls).Set(1)
		case observability.Histogram:
			r.Histogram(m.Name).With(lbls).Observe(0.1)
		}
	}
}

// TestRegistryMatchesContract is the M6 acceptance for metrics: the live registry
// exposes exactly the F2 contract's names, kinds, and label sets — no drift.
func TestRegistryMatchesContract(t *testing.T) {
	r := New()
	emitOnePerMetric(r)

	fams, err := r.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		byName[f.GetName()] = f
	}

	wantType := map[observability.MetricKind]dto.MetricType{
		observability.Counter:   dto.MetricType_COUNTER,
		observability.Gauge:     dto.MetricType_GAUGE,
		observability.Histogram: dto.MetricType_HISTOGRAM,
	}

	for _, m := range observability.Metrics {
		f, ok := byName[m.Name]
		if !ok {
			t.Errorf("metric %q missing from live registry", m.Name)
			continue
		}
		if f.GetType() != wantType[m.Kind] {
			t.Errorf("metric %q type = %s, want kind %s", m.Name, f.GetType(), m.Kind)
		}
		if len(f.Metric) == 0 {
			t.Errorf("metric %q has no series", m.Name)
			continue
		}
		got := labelNames(f.Metric[0])
		want := append([]string(nil), m.Labels...)
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("metric %q labels = %v, want %v", m.Name, got, want)
		}
		if f.GetHelp() != m.Help {
			t.Errorf("metric %q help = %q, want %q", m.Name, f.GetHelp(), m.Help)
		}
	}
}

// TestHandlerScrapeExposesNames asserts a real HTTP scrape of the handler carries
// every contract metric's TYPE line — the operator-facing /metrics surface.
func TestHandlerScrapeExposesNames(t *testing.T) {
	r := New()
	emitOnePerMetric(r)

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)
	for _, m := range observability.Metrics {
		if !strings.Contains(text, "# TYPE "+m.Name+" ") {
			t.Errorf("scrape missing TYPE line for %q", m.Name)
		}
	}
}

func labelNames(m *dto.Metric) []string {
	out := make([]string, 0, len(m.Label))
	for _, l := range m.Label {
		out = append(out, l.GetName())
	}
	return out
}
