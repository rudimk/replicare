package redis

import (
	"context"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestEngineRegistered(t *testing.T) {
	eng, err := engine.Get("redis")
	if err != nil {
		t.Fatalf("engine.Get(redis): %v", err)
	}
	if eng.Name() != "redis" {
		t.Fatalf("Name() = %q, want redis", eng.Name())
	}
	// Factories construct without connecting.
	if _, err := eng.NewSource(engine.ConnConfig{}); err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if _, err := eng.NewSink(engine.ConnConfig{}); err != nil {
		t.Fatalf("NewSink: %v", err)
	}
}

func TestServerVersionNum(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"6.2.14", 60214, true},
		{"7.4.0", 70400, true},
		{"7.2.5", 70205, true},
		{"3.0.0", 30000, true},
		{"7.4", 70400, true},                   // patch defaults to 0
		{"255.255.255-build.7", 2575755, true}, // suffix stripped
		{"garbage", 0, false},
		{"7", 0, false}, // too few components
	}
	for _, c := range cases {
		got, err := serverVersionNum(c.in)
		if c.ok && err != nil {
			t.Errorf("serverVersionNum(%q) error: %v", c.in, err)
			continue
		}
		if !c.ok && err == nil {
			t.Errorf("serverVersionNum(%q) = %d, want error", c.in, got)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("serverVersionNum(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestDetectForkAndSupport(t *testing.T) {
	cases := []struct {
		info      string
		fork      redisFork
		supported bool
	}{
		{"redis_version:7.4.0\r\nredis_mode:standalone", forkRedis, true},
		{"redis_version:7.2.4\r\nvalkey_version:8.0.0", forkValkey, true},
		{"redis_version:6.3.4\r\nkeydb_version:6.3.4", forkKeyDB, true},
		{"dragonfly_version:df-v1.0\r\nredis_version:7.4.0", forkDragonfly, false},
	}
	for _, c := range cases {
		f := detectFork(c.info)
		if f != c.fork {
			t.Errorf("detectFork(%q) = %v, want %v", c.info, f, c.fork)
		}
		ok, reason := f.supported()
		if ok != c.supported {
			t.Errorf("%v.supported() = %v, want %v", f, ok, c.supported)
		}
		if !ok && reason == "" {
			t.Errorf("%v unsupported but gave no reason", f)
		}
	}
}

func TestInfoField(t *testing.T) {
	info := "# Server\r\nredis_version:7.4.0\r\nredis_mode:cluster\r\nos:Linux\r\n"
	if v := infoField(info, "redis_version"); v != "7.4.0" {
		t.Errorf("redis_version = %q, want 7.4.0", v)
	}
	if v := infoField(info, "redis_mode"); v != "cluster" {
		t.Errorf("redis_mode = %q, want cluster", v)
	}
	if v := infoField(info, "absent"); v != "" {
		t.Errorf("absent field = %q, want empty", v)
	}
}

func TestDBIndex(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"", 0, true},
		{"0", 0, true},
		{"5", 5, true},
		{"-1", 0, false},
		{"x", 0, false},
	} {
		got, err := dbIndex(c.in)
		if c.ok != (err == nil) {
			t.Errorf("dbIndex(%q) err=%v, wantOk=%v", c.in, err, c.ok)
		}
		if c.ok && got != c.want {
			t.Errorf("dbIndex(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// Stubs return errNotImplemented until their milestones — asserts the skeleton is
// wired without a real connection.
func TestStubsNotImplemented(t *testing.T) {
	ctx := context.Background()
	s := &Source{}
	if _, err := s.PlanChunks(ctx, engine.TableRef{}, engine.ChunkOptions{}); err != errNotImplemented {
		t.Errorf("Source.PlanChunks stub = %v, want errNotImplemented", err)
	}
	sink := &Sink{}
	if _, err := sink.BeginApply(ctx, false, nil); err != errNotImplemented {
		t.Errorf("Sink.BeginApply stub = %v, want errNotImplemented", err)
	}
}
