package steward_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

type upgradeCfg struct {
	Version int
}

type handoffUnit struct {
	id      string
	version int
	value   atomic.Int32
}

func (u *handoffUnit) ID() string { return u.id }
func (u *handoffUnit) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
func (u *handoffUnit) Stop(context.Context) error { return nil }

func copyHandoff(old steward.Unit, new steward.Unit) error {
	o, ok := old.(*handoffUnit)
	if !ok {
		return errors.New("unexpected old type")
	}
	n, ok := new.(*handoffUnit)
	if !ok {
		return errors.New("unexpected new type")
	}
	n.value.Store(o.value.Load())
	return nil
}

func TestHandoffPreservesState(t *testing.T) {
	set := steward.NewSet[string, upgradeCfg](
		func(_ context.Context, id string, c upgradeCfg) (steward.Unit, error) {
			return &handoffUnit{id: id, version: c.Version}, nil
		},
		func(a, b upgradeCfg) bool { return a.Version == b.Version },
		steward.WithHandoff(copyHandoff),
	)
	set.Start(context.Background())

	set.Reconcile(map[string]upgradeCfg{"u": {Version: 1}})
	time.Sleep(30 * time.Millisecond)

	old := &handoffUnit{id: "u", version: 1}
	old.value.Store(42)
	newU := &handoffUnit{id: "u", version: 2}
	if err := copyHandoff(old, newU); err != nil {
		t.Fatal(err)
	}
	if newU.value.Load() != 42 {
		t.Fatalf("handoff value = %d, want 42", newU.value.Load())
	}

	set.Reconcile(map[string]upgradeCfg{"u": {Version: 2}})
	time.Sleep(50 * time.Millisecond)

	if len(set.Snapshot()) != 1 {
		t.Fatalf("units = %d, want 1", len(set.Snapshot()))
	}

	set.Stop(context.Background())
}

func failHandoff(steward.Unit, steward.Unit) error {
	return errors.New("handoff failed")
}

func TestHandoffErrorKeepsOld(t *testing.T) {
	set := steward.NewSet[string, upgradeCfg](
		func(_ context.Context, id string, c upgradeCfg) (steward.Unit, error) {
			return &handoffUnit{id: id, version: c.Version}, nil
		},
		func(a, b upgradeCfg) bool { return a.Version == b.Version },
		steward.WithHandoff(failHandoff),
	)
	set.Start(context.Background())
	set.Reconcile(map[string]upgradeCfg{"u": {Version: 1}})
	time.Sleep(30 * time.Millisecond)

	err := set.Reconcile(map[string]upgradeCfg{"u": {Version: 2}})
	if err == nil {
		t.Fatal("expected handoff error")
	}

	units := set.Snapshot()
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	if units[0].State != steward.StateRunning && units[0].State != steward.StateStarting {
		t.Fatalf("old unit state = %v, want running", units[0].State)
	}

	set.Stop(context.Background())
}
