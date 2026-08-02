package mysql

import (
	"bytes"
	"testing"
)

func serialize(row [][]byte) string {
	var buf bytes.Buffer
	bw := newRowWriter(&buf)
	writeRow(bw, row)
	_ = bw.Flush()
	return buf.String()
}

func TestEscapeSpecialBytes(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte("plain"), "plain"},
		{[]byte{0x00}, `\0`},
		{[]byte{0x08}, `\b`},
		{[]byte("\t"), `\t`},
		{[]byte("\n"), `\n`},
		{[]byte("\r"), `\r`},
		{[]byte{0x1A}, `\Z`},
		{[]byte(`\`), `\\`},
		{[]byte(`\N`), `\\N`}, // literal backslash-N must NOT become the NULL sentinel
		{[]byte("N"), "N"},    // a bare N is verbatim
	}
	for _, c := range cases {
		got := serialize([][]byte{c.in})
		want := c.want + "\n"
		if got != want {
			t.Errorf("serialize(%q) = %q, want %q", c.in, got, want)
		}
	}
}

func TestNullSentinelDistinctFromField(t *testing.T) {
	// A nil value is the NULL sentinel \N; the two-byte data "\N" is \\N.
	if got := serialize([][]byte{nil}); got != "\\N\n" {
		t.Errorf("NULL = %q, want %q", got, "\\N\n")
	}
	if got := serialize([][]byte{[]byte(`\N`)}); got != "\\\\N\n" {
		t.Errorf(`data "\N" = %q, want %q`, got, "\\\\N\n")
	}
}

func TestRowFieldSeparators(t *testing.T) {
	got := serialize([][]byte{[]byte("a"), nil, []byte("c")})
	if got != "a\t\\N\tc\n" {
		t.Errorf("row = %q, want %q", got, "a\t\\N\tc\n")
	}
}
