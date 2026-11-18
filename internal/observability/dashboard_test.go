package observability

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The shipped Grafana dashboard must only query metrics replicare actually emits, and
// its sync/table filters must come from a metric that exists during an initial copy:
// filters fed by streaming-only metrics left every panel at "No data" until streaming.
func TestGrafanaDashboardMatchesContract(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/grafana/replicare-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, m := range Metrics {
		known[m.Name] = true
	}
	for _, name := range regexp.MustCompile(`replicare_[a-z_]+`).FindAllString(string(raw), -1) {
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
		if !known[base] && !known[name] {
			t.Errorf("dashboard queries %q, which replicare does not emit", name)
		}
	}

	var d struct {
		Templating struct {
			List []struct {
				Name       string `json:"name"`
				Definition string `json:"definition"`
			} `json:"list"`
		} `json:"templating"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	for _, v := range d.Templating.List {
		if (v.Name == "sync" || v.Name == "table") && !strings.Contains(v.Definition, MetricPhaseInfo) {
			t.Errorf("variable %q is filled from %q; it must use %s, which exists from the start of the initial copy",
				v.Name, v.Definition, MetricPhaseInfo)
		}
	}
}
