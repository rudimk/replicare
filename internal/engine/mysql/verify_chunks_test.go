package mysql

import (
	"math/big"
	"testing"
	"unicode/utf8"
)

func TestRandomPointsInteger(t *testing.T) {
	pts, ok := randomPoints("bigint(20) unsigned", "10", "20", 50)
	if !ok || len(pts) != 50 {
		t.Fatalf("got %d points, ok=%v", len(pts), ok)
	}
	for _, p := range pts {
		v, _ := new(big.Int).SetString(p.(string), 10)
		if v == nil || v.Int64() < 10 || v.Int64() > 20 {
			t.Fatalf("point %v outside [10, 20]", p)
		}
	}
}

func TestRandomPointsText(t *testing.T) {
	lo, hi := "c0a1b2c3", "cz99zzzz"
	pts, ok := randomPoints("varchar(36)", lo, hi, 200)
	if !ok {
		t.Fatal("text key not sampled by value")
	}
	for _, p := range pts {
		s := p.(string)
		if !utf8.ValidString(s) {
			t.Fatalf("point %q is not valid text", s)
		}
		for _, c := range []byte(s) {
			if c < 0x20 || c > 0x7e {
				t.Fatalf("point %q has a non-printable byte", s)
			}
		}
	}
}

func TestRandomPointsBinaryAndOther(t *testing.T) {
	if _, ok := randomPoints("varbinary(16)", "\x00\x01", "\xff\xfe", 5); !ok {
		t.Fatal("binary key not sampled by value")
	}
	for _, typ := range []string{"date", "datetime(6)", "decimal(12,2)", "text"} {
		if _, ok := randomPoints(typ, "a", "b", 5); ok {
			t.Fatalf("%s sampled by value, want the by-position fallback", typ)
		}
	}
	if _, ok := randomPoints("int", "20", "10", 5); ok {
		t.Fatal("an inverted range was accepted")
	}
}
