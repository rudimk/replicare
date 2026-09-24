package redis

import (
	"context"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// stubDoer implements the minimal `doer` surface for moduleList: it returns a
// pre-canned *goredis.Cmd carrying either an error or a value.
type stubDoer struct {
	err error
	val any
}

func (s stubDoer) Do(ctx context.Context, _ ...any) *goredis.Cmd {
	c := goredis.NewCmd(ctx)
	if s.err != nil {
		c.SetErr(s.err)
	} else {
		c.SetVal(s.val)
	}
	return c
}

// TestModuleListToleratesDisabledCommand proves a managed-Redis MODULE block
// (ElastiCache "unknown command", or an ACL NOPERM) degrades to "no modules"
// instead of failing introspection — while a genuine error still propagates.
func TestModuleListToleratesDisabledCommand(t *testing.T) {
	ctx := context.Background()

	tolerated := []string{
		"ERR unknown command 'MODULE', with args beginning with: 'LIST'",
		"ERR unknown subcommand or wrong number of arguments for 'LIST'",
		"NOPERM this user has no permissions to run the 'module|list' command",
		"ERR This command is disabled",
		"ERR command not allowed",
	}
	for _, msg := range tolerated {
		mods, err := moduleList(ctx, stubDoer{err: errors.New(msg)})
		if err != nil {
			t.Errorf("moduleList(%q) returned error %v, want nil (degrade to no modules)", msg, err)
		}
		if mods != nil {
			t.Errorf("moduleList(%q) = %v, want nil modules", msg, mods)
		}
	}

	// A genuine error must still propagate (not be swallowed).
	if _, err := moduleList(ctx, stubDoer{err: errors.New("connection reset by peer")}); err == nil {
		t.Error("moduleList on a real error returned nil; want it to propagate")
	}

	// A normal empty MODULE LIST (no modules loaded) → no modules, no error.
	if mods, err := moduleList(ctx, stubDoer{val: []any{}}); err != nil || mods != nil {
		t.Errorf("moduleList(empty) = (%v, %v), want (nil, nil)", mods, err)
	}

	// A populated MODULE LIST still parses (RESP2 flat array shape).
	entry := []any{"name", "ReJSON", "ver", int64(20803)}
	mods, err := moduleList(ctx, stubDoer{val: []any{entry}})
	if err != nil {
		t.Fatalf("moduleList(populated): %v", err)
	}
	if len(mods) != 1 || mods[0] != "ReJSON" {
		t.Errorf("moduleList(populated) = %v, want [ReJSON]", mods)
	}
}
