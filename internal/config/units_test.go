package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationUnmarshal(t *testing.T) {
	var d Duration
	if err := yaml.Unmarshal([]byte(`"5s"`), &d); err != nil {
		t.Fatalf("unmarshal 5s: %v", err)
	}
	if d.Duration() != 5*time.Second {
		t.Errorf("got %v, want 5s", d.Duration())
	}
	if err := yaml.Unmarshal([]byte(`"nope"`), &d); err == nil {
		t.Error("expected error on invalid duration")
	}
}

func TestByteSizeUnmarshal(t *testing.T) {
	cases := map[string]int64{
		`1024`:    1024,
		`"1024"`:  1024,
		`"1KB"`:   1000,
		`"1KiB"`:  1024,
		`"10GB"`:  10 * 1000 * 1000 * 1000,
		`"1MiB"`:  1 << 20,
		`"2 GiB"`: 2 << 30,
	}
	for in, want := range cases {
		var b ByteSize
		if err := yaml.Unmarshal([]byte(in), &b); err != nil {
			t.Errorf("unmarshal %s: %v", in, err)
			continue
		}
		if b.Bytes() != want {
			t.Errorf("unmarshal %s = %d, want %d", in, b.Bytes(), want)
		}
	}

	var b ByteSize
	if err := yaml.Unmarshal([]byte(`"-5GB"`), &b); err == nil {
		t.Error("expected error on negative size")
	}
	if err := yaml.Unmarshal([]byte(`"banana"`), &b); err == nil {
		t.Error("expected error on garbage size")
	}
}
