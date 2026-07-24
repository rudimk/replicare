// Package prom is the live Prometheus registry for replicare, built directly
// from the F2 observability contract (internal/observability). Every metric in
// observability.Metrics is instantiated here with its declared name, help, kind,
// and labels, so the running /metrics surface can never drift from the contract
// (M6 verifies this — see prom_test.go — instead of retrofitting; plan F2/M6).
package prom

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	"github.com/rudimk/replicare/internal/observability"
)

// Registry holds the concrete collectors for the F2 contract, keyed by metric
// name, plus the underlying Prometheus registry that backs /metrics.
type Registry struct {
	reg        *prometheus.Registry
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
}

// New builds the registry from observability.Metrics. It panics on a malformed
// contract (duplicate name or unknown kind) — that is a programming error in the
// contract, caught at startup, never at scrape time.
func New() *Registry {
	r := &Registry{
		reg:        prometheus.NewRegistry(),
		counters:   map[string]*prometheus.CounterVec{},
		gauges:     map[string]*prometheus.GaugeVec{},
		histograms: map[string]*prometheus.HistogramVec{},
	}
	for _, m := range observability.Metrics {
		if r.known(m.Name) {
			panic(fmt.Sprintf("observability: duplicate metric %q in contract", m.Name))
		}
		switch m.Kind {
		case observability.Counter:
			c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: m.Name, Help: m.Help}, m.Labels)
			r.counters[m.Name] = c
			r.reg.MustRegister(c)
		case observability.Gauge:
			g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: m.Name, Help: m.Help}, m.Labels)
			r.gauges[m.Name] = g
			r.reg.MustRegister(g)
		case observability.Histogram:
			h := prometheus.NewHistogramVec(
				prometheus.HistogramOpts{Name: m.Name, Help: m.Help, Buckets: prometheus.DefBuckets}, m.Labels)
			r.histograms[m.Name] = h
			r.reg.MustRegister(h)
		default:
			panic(fmt.Sprintf("observability: metric %q has unknown kind %q", m.Name, m.Kind))
		}
	}
	return r
}

func (r *Registry) known(name string) bool {
	_, c := r.counters[name]
	_, g := r.gauges[name]
	_, h := r.histograms[name]
	return c || g || h
}

// Handler serves the Prometheus text exposition for this registry (the daemon's
// /metrics endpoint, wired in M6/M7).
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// Gather exposes the collected metric families (used by verification tests).
func (r *Registry) Gather() ([]*dto.MetricFamily, error) { return r.reg.Gather() }

// Counter returns the counter vector for a contract metric name. It panics on an
// unknown name because names are compile-time constants from the contract; a miss
// is a programming error, not a runtime condition.
func (r *Registry) Counter(name string) *prometheus.CounterVec {
	c := r.counters[name]
	if c == nil {
		panic(fmt.Sprintf("observability: %q is not a registered counter", name))
	}
	return c
}

// Gauge returns the gauge vector for a contract metric name (panics if unknown).
func (r *Registry) Gauge(name string) *prometheus.GaugeVec {
	g := r.gauges[name]
	if g == nil {
		panic(fmt.Sprintf("observability: %q is not a registered gauge", name))
	}
	return g
}

// Histogram returns the histogram vector for a contract metric name (panics if
// unknown).
func (r *Registry) Histogram(name string) *prometheus.HistogramVec {
	h := r.histograms[name]
	if h == nil {
		panic(fmt.Sprintf("observability: %q is not a registered histogram", name))
	}
	return h
}
