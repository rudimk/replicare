package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a Go duration string
// ("5s", "24h", "500ms"). A bare number is rejected to avoid unit ambiguity.
type Duration time.Duration

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"5s\" or \"24h\": %w", err)
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// ByteSize is a size in bytes that unmarshals from a human string
// ("10GB", "512MB", "1024", "2KiB"). Both decimal (KB/MB/GB/TB, powers of 1000)
// and binary (KiB/MiB/GiB/TiB, powers of 1024) suffixes are accepted; a bare
// number is bytes.
type ByteSize int64

var byteUnits = []struct {
	suffix string
	mult   int64
}{
	// Order matters: check longer/binary suffixes before shorter ones.
	{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
	{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000}, {"TB", 1000 * 1000 * 1000 * 1000},
	{"B", 1},
}

// UnmarshalYAML parses a byte-size string or integer.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	// Allow a plain integer (bytes).
	var n int64
	if err := node.Decode(&n); err == nil {
		if n < 0 {
			return fmt.Errorf("byte size must not be negative: %d", n)
		}
		*b = ByteSize(n)
		return nil
	}

	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("byte size must be a number or string like \"10GB\": %w", err)
	}
	parsed, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(parsed)
	return nil
}

func parseByteSize(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	if t == "" {
		return 0, fmt.Errorf("empty byte size")
	}
	for _, u := range byteUnits {
		if strings.HasSuffix(t, u.suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(t, u.suffix))
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
			}
			if num < 0 {
				return 0, fmt.Errorf("byte size must not be negative: %q", s)
			}
			return int64(num * float64(u.mult)), nil
		}
	}
	// No recognized suffix: must be a plain number.
	num, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q (use a number or a unit like 10GB)", s)
	}
	return num, nil
}

// Bytes returns the size as an int64.
func (b ByteSize) Bytes() int64 { return int64(b) }
