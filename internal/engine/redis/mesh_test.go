package redis

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// TestSlotTag reproduces Redis's cluster hash-tag rule exactly: content between the
// first '{' and the first '}' after it, when non-empty; otherwise the whole key.
func TestSlotTag(t *testing.T) {
	cases := []struct{ key, want string }{
		{"foo", "foo"},           // no braces → whole key
		{"user:{42}:name", "42"}, // standard tag
		{"{user1000}.following", "user1000"},
		{"{}.x", "{}.x"},       // empty tag → whole key
		{"}{abc", "}{abc"},     // no '}' to the RIGHT of the first '{' → whole key
		{"a}b", "a}b"},         // no '{' → whole key (even with a '}')
		{"{a{b}c", "a{b"},      // '{' inside tag content is literal
		{"", ""},               // empty key
		{"{tag}", "tag"},       // tag is the whole remainder
		{"pre{t}post{u}", "t"}, // only the FIRST pair counts
	}
	for _, c := range cases {
		if got := slotTag(c.key); got != c.want {
			t.Errorf("slotTag(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}

// TestMetaKeyCoLocates is the load-bearing invariant: a metadata key's OWN slot tag
// must equal its data key's slot tag, so value and metadata share a cluster slot and
// a single Lua may touch both atomically. We assert slotTag(metaKey(k)) == slotTag(k)
// for keys that stress the brace rules.
func TestMetaKeyCoLocates(t *testing.T) {
	keys := []string{
		"foo",
		"user:{42}:name",
		"user:{42}:email", // shares a tag with the previous → distinct meta keys, same slot
		"a}b",
		"{a{b}c",
		"{}.x",
		"",
		"weird\x00key",
		"pre{t}post{u}",
	}
	seen := map[string]string{}
	for _, k := range keys {
		mk := metaKey(k)
		// Co-location is about the cluster SLOT (CRC16), not the tag string.
		if got, want := slotOf(mk), slotOf(k); got != want {
			t.Errorf("metaKey(%q)=%q: slot %d != data slot %d", k, mk, got, want)
		}
		if !isMetaKey(mk) {
			t.Errorf("metaKey(%q)=%q not recognized by isMetaKey", k, mk)
		}
		if prev, dup := seen[mk]; dup {
			t.Errorf("metaKey collision: %q and %q both map to %q", prev, k, mk)
		}
		seen[mk] = k
	}
	// A user key never looks like a metadata key (control-char prefix).
	for _, k := range keys {
		if isMetaKey(k) && k != "" && k[0] != 0x00 {
			t.Errorf("isMetaKey(%q) true for a plain user key", k)
		}
	}
}

// TestDataKeyFromMeta is the inverse of metaKey: every data key round-trips through its
// metadata key, and a non-metadata string is rejected.
func TestDataKeyFromMeta(t *testing.T) {
	keys := []string{"foo", "user:{42}:name", "a}b", "{a{b}c", "{}.x", "", "x:y:z", "pre{t}post{u}"}
	for _, k := range keys {
		mk := metaKey(k)
		got, ok := dataKeyFromMeta(mk)
		if !ok {
			t.Errorf("dataKeyFromMeta(%q) not recognized as a meta key", mk)
			continue
		}
		if got != k {
			t.Errorf("round-trip: dataKeyFromMeta(metaKey(%q)) = %q", k, got)
		}
	}
	if _, ok := dataKeyFromMeta("plain-user-key"); ok {
		t.Error("dataKeyFromMeta accepted a non-meta key")
	}
}

// TestSelectionExcludesMeta: the reserved metadata namespace is never selected, so the
// version register is invisible to copy/verify/reconcile even under a match-all glob.
func TestSelectionExcludesMeta(t *testing.T) {
	sel := compileSelection(engine.Selection{}) // match-all
	if sel.match(metaKey("anything")) {
		t.Error("selection matched a metadata key under match-all")
	}
	if !sel.match("anything") {
		t.Error("selection should match an ordinary key under match-all")
	}
	star := compileSelection(engine.Selection{Include: []string{"*"}})
	if star.match(metaKey("k")) {
		t.Error(`selection "*" matched a metadata key`)
	}
}

// TestCRC16KeySlot checks our CRC16 against known Redis CLUSTER KEYSLOT values, and
// that a hash tag routes a key to its tag's slot (co-location precondition).
func TestCRC16KeySlot(t *testing.T) {
	// Known values from Redis (CLUSTER KEYSLOT): "123456789" CRC16 = 0x31C3 = 12739;
	// "foo" → 12182; "bar" → 5061.
	if got := crc16("123456789"); got != 0x31C3 {
		t.Errorf("crc16(123456789) = %#x, want 0x31C3", got)
	}
	if got := slotOf("foo"); got != 12182 {
		t.Errorf("slotOf(foo) = %d, want 12182", got)
	}
	if got := slotOf("bar"); got != 5061 {
		t.Errorf("slotOf(bar) = %d, want 5061", got)
	}
	// A tagged key routes to its tag's slot.
	if slotOf("user:{foo}:name") != slotOf("foo") {
		t.Errorf("tagged key not routed to tag slot")
	}
	// slotToTag is a genuine inverse: its tag hashes back to the slot, for every slot.
	for s := uint16(0); s < hashSlots; s++ {
		if crc16(slotToTag(s))%hashSlots != s {
			t.Fatalf("slotToTag(%d) hashes to %d", s, crc16(slotToTag(s))%hashSlots)
		}
	}
}

// TestVersionBeats checks the total LWW order (phys, log, node), that a never-seen
// register always loses, and that node_id breaks equal-HLC ties (no ties remain).
func TestVersionBeats(t *testing.T) {
	v := func(p int64, l int32, n string) version { return version{phys: p, log: l, node: n} }
	cases := []struct {
		name     string
		a, b     version
		bPresent bool
		want     bool
	}{
		{"absent loses", v(1, 0, "a"), version{}, false, true},
		{"higher phys wins", v(2, 0, "a"), v(1, 9, "z"), true, true},
		{"lower phys loses", v(1, 9, "z"), v(2, 0, "a"), true, false},
		{"higher log wins at equal phys", v(5, 3, "a"), v(5, 2, "z"), true, true},
		{"node breaks tie (greater wins)", v(5, 3, "b"), v(5, 3, "a"), true, true},
		{"node breaks tie (lesser loses)", v(5, 3, "a"), v(5, 3, "b"), true, false},
		{"identical does not beat itself", v(5, 3, "a"), v(5, 3, "a"), true, false},
	}
	for _, c := range cases {
		if got := c.a.beats(c.b, c.bPresent); got != c.want {
			t.Errorf("%s: beats = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestHLCTick: a local tick is strictly monotonic under (phys, log) and tracks
// wall-clock when it advances past the stored physical time.
func TestHLCTick(t *testing.T) {
	// wall-clock advanced past stored → jump phys, reset log.
	if p, l := hlcTick(100, 50, 7); p != 100 || l != 0 {
		t.Errorf("advance = (%d,%d), want (100,0)", p, l)
	}
	// wall-clock behind stored (skew) → hold phys, bump log (still strictly greater).
	if p, l := hlcTick(40, 50, 7); p != 50 || l != 8 {
		t.Errorf("skew = (%d,%d), want (50,8)", p, l)
	}
	// wall-clock equal to stored → hold phys, bump log.
	if p, l := hlcTick(50, 50, 7); p != 50 || l != 8 {
		t.Errorf("equal = (%d,%d), want (50,8)", p, l)
	}
}

// TestHLCObserve: after observing an incoming version, a subsequent local tick
// strictly exceeds both the prior local clock and the observed one (skew tolerance).
func TestHLCObserve(t *testing.T) {
	// incoming ahead of both wall-clock and stored → adopt incoming phys, log+1.
	if p, l := hlcObserve(10, 20, 2, 100, 5); p != 100 || l != 6 {
		t.Errorf("incoming-ahead = (%d,%d), want (100,6)", p, l)
	}
	// stored ahead → hold stored phys, log+1.
	if p, l := hlcObserve(10, 200, 4, 100, 5); p != 200 || l != 5 {
		t.Errorf("stored-ahead = (%d,%d), want (200,5)", p, l)
	}
	// stored and incoming at same phys → max logical + 1.
	if p, l := hlcObserve(10, 100, 4, 100, 9); p != 100 || l != 10 {
		t.Errorf("equal-phys = (%d,%d), want (100,10)", p, l)
	}
	// wall-clock ahead of both → jump to wall, reset logical.
	if p, l := hlcObserve(500, 100, 4, 200, 9); p != 500 || l != 0 {
		t.Errorf("wall-ahead = (%d,%d), want (500,0)", p, l)
	}

	// A ticked-then-observed clock never regresses: after observe, a tick beats the
	// observed version under the total order.
	p, l := hlcObserve(0, 0, 0, 100, 5)
	np, nl := hlcTick(0, p, l)
	got := version{phys: np, log: nl, node: "n"}
	seen := version{phys: 100, log: 5, node: "n"}
	if !got.beats(seen, true) {
		t.Errorf("post-observe tick %+v does not beat observed %+v", got, seen)
	}
}
