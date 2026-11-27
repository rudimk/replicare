package engine

import "testing"

// TestRetentionMaySacrifice: with no list, any target may be given up on; with a list,
// only the targets on it.
func TestRetentionMaySacrifice(t *testing.T) {
	if !(RetentionPolicy{}).MaySacrifice("a") {
		t.Error("nil Sacrificable: want any target allowed")
	}
	r := RetentionPolicy{Sacrificable: []TargetID{"mine"}}
	if !r.MaySacrifice("mine") || r.MaySacrifice("other") {
		t.Errorf("Sacrificable [mine]: mine=%v other=%v, want true/false", r.MaySacrifice("mine"), r.MaySacrifice("other"))
	}
	if (RetentionPolicy{Sacrificable: []TargetID{}}).MaySacrifice("a") {
		t.Error("empty Sacrificable: want nothing allowed")
	}
}
