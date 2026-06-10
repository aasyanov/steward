package steward_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

func TestInstance_RunningFailed(t *testing.T) {
	inst := steward.NewInstance(cfg{Name: "x"}, stubCfgBuild(), stubCfgEqual())
	inst.Start(context.Background())
	waitUntil(t, time.Second, func() bool { return inst.Running() })
	if inst.Failed() {
		t.Fatal("unexpected failed")
	}
	inst.Stop(context.Background())
}

func TestSet_StartAfterStop(t *testing.T) {
	set := steward.NewSet[string, cfg](stubCfgBuild(), stubCfgEqual())
	set.Start(context.Background())
	set.Stop(context.Background())
	if err := set.Start(context.Background()); !errors.Is(err, steward.ErrStopped) {
		t.Fatalf("got %v", err)
	}
}
