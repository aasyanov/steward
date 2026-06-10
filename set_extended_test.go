package steward_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

func stubCfgBuild() steward.BuildFunc[string, cfg] {
	return func(_ context.Context, id string, _ cfg) (steward.Unit, error) {
		return steward.RunUnit(id, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}), nil
	}
}

func stubCfgEqual() steward.EqualFunc[cfg] {
	return func(a, b cfg) bool { return a == b }
}

func TestSet_StartFailureClosesLoop(t *testing.T) {
	set := steward.NewSet[string, cfg](stubCfgBuild(), stubCfgEqual())
	set.Start(context.Background())
	set.Stop(context.Background())
	if err := set.Start(context.Background()); !errors.Is(err, steward.ErrStopped) {
		t.Fatalf("got %v", err)
	}
}

func TestSet_ReplaceWhenStopped(t *testing.T) {
	set := steward.NewSet[string, cfg](stubCfgBuild(), stubCfgEqual())
	set.Start(context.Background())
	set.Stop(context.Background())
	if err := set.Replace(stubCfgBuild(), stubCfgEqual(), map[string]cfg{"a": {}}); !errors.Is(err, steward.ErrStopped) {
		t.Fatalf("got %v", err)
	}
}

func TestInstance_DoubleStart(t *testing.T) {
	inst := steward.NewInstance(cfg{Name: "x"}, stubCfgBuild(), stubCfgEqual())
	if err := inst.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(context.Background()); !errors.Is(err, steward.ErrAlreadyStarted) {
		t.Fatalf("got %v", err)
	}
	inst.Stop(context.Background())
}

func TestReconcilePartialFailureOnUpdate(t *testing.T) {
	set := steward.NewSet[string, int](
		func(_ context.Context, id string, v int) (steward.Unit, error) {
			if id == "bad" {
				return nil, errors.New("build")
			}
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },
	)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]int{"good": 1})
	waitUntil(t, time.Second, func() bool { return set.Running("good") })

	err := set.Reconcile(map[string]int{"good": 2, "bad": 2})
	if err == nil {
		t.Fatal("expected error")
	}
	waitUntil(t, time.Second, func() bool { return set.Running("good") })
}

func TestReconcileRemoveAndUpdate(t *testing.T) {
	set := steward.NewSet[string, int](
		func(_ context.Context, id string, v int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },
	)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]int{"a": 1, "b": 2})
	waitUntil(t, time.Second, func() bool { return set.Running("a") && set.Running("b") })

	set.Reconcile(map[string]int{"a": 2})
	waitUntil(t, time.Second, func() bool { return set.Running("a") && !set.Running("b") })
}

func TestSet_DroppedAuditEvents(t *testing.T) {
	set := steward.NewSet[string, cfg](
		stubCfgBuild(),
		stubCfgEqual(),
		steward.WithAuditBuffer(1),
	)
	set.Start(context.Background())
	set.Reconcile(map[string]cfg{"a": {}, "b": {}, "c": {}})
	_ = set.DroppedAuditEvents()
	set.Stop(context.Background())
}

func TestSet_ReconcileBeforeStart(t *testing.T) {
	set := steward.NewSet[string, cfg](stubCfgBuild(), stubCfgEqual())
	if err := set.Reconcile(map[string]cfg{"a": {}}); !errors.Is(err, steward.ErrNotStarted) {
		t.Fatalf("got %v", err)
	}
	if err := set.Replace(stubCfgBuild(), stubCfgEqual(), map[string]cfg{"a": {}}); !errors.Is(err, steward.ErrNotStarted) {
		t.Fatalf("got %v", err)
	}
}
