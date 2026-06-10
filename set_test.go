package steward_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

type cfg struct {
	Name string
}

func TestSetReconcileLifecycle(t *testing.T) {
	var started atomic.Int32

	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, c cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				started.Add(1)
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },)
	ctx := context.Background()

	if err := set.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := set.Reconcile(map[string]cfg{
		"a": {Name: "one"},
		"b": {Name: "two"},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	if started.Load() != 2 {
		t.Fatalf("started = %d, want 2", started.Load())
	}

	if err := set.Reconcile(map[string]cfg{
		"a": {Name: "one"},
		"b": {Name: "changed"},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	snap := set.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("units = %d, want 2", len(snap))
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := set.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestSetStopWaitsForRun(t *testing.T) {
	running := make(chan struct{})
	done := make(chan struct{})

	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				close(running)
				<-ctx.Done()
				time.Sleep(20 * time.Millisecond)
				close(done)
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },)
	ctx := context.Background()
	set.Start(ctx)
	set.Reconcile(map[string]cfg{"x": {Name: "x"}})

	<-running
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	set.Stop(stopCtx)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not finish before Stop returned")
	}
}

func TestInstanceReload(t *testing.T) {
	var count atomic.Int32

	inst := steward.NewInstance(cfg{Name: "v1"},
		func(_ context.Context,
		id string, c cfg) (steward.Unit, error) {
			name := c.Name
			return steward.RunUnit(id, func(ctx context.Context) error {
				count.Add(1)
				_ = name
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },)
	inst.Start(context.Background())
	time.Sleep(30 * time.Millisecond)

	inst.Reload(cfg{Name: "v2"})
	time.Sleep(50 * time.Millisecond)

	if count.Load() < 2 {
		t.Fatalf("runs = %d, want >= 2", count.Load())
	}

	inst.Stop(context.Background())
}

func TestPolicyRestart(t *testing.T) {
	attempts := make(chan struct{}, 8)

	policy := steward.DefaultPolicy{}
	set := steward.NewSet[string, cfg](
		func(_ context.Context, id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				select {
				case attempts <- struct{}{}:
				default:
				}
				return steward.ClassifyError(steward.FailureTransient, errors.New("transient"))
			}), nil
		},
		func(a, b cfg) bool { return a == b },
		steward.WithPolicy(policy),
	)
	set.Start(context.Background())
	set.Reconcile(map[string]cfg{"u": {Name: "u"}})

	select {
	case <-attempts:
	case <-time.After(time.Second):
		t.Fatal("unit did not start")
	}

	select {
	case <-attempts:
	case <-time.After(3 * time.Second):
		t.Fatal("policy did not restart unit")
	}

	set.Stop(context.Background())
}
