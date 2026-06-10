package steward_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

func TestCoverage_FactoryErrorPartialReconcile(t *testing.T) {
	set := steward.NewSet[string, int](
		func(_ context.Context,
		id string, v int) (steward.Unit, error) {
			if v == 2 {
				return nil, errors.New("build fail")
			}
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]int{"a": 1})
	waitUntil(t, time.Second, func() bool { return set.Running("a") })

	err := set.Reconcile(map[string]int{"a": 1, "b": 2})
	if err == nil {
		t.Fatal("expected error")
	}
	if !set.Running("a") {
		t.Fatal("existing unit should keep running")
	}
	if set.Running("b") {
		t.Fatal("failed build should not run")
	}
}

func TestCoverage_DrainerUnit(t *testing.T) {
	drained := make(chan struct{}, 1)
	u := &drainProbe{id: "u", drained: drained}
	set := steward.NewSet[string, struct{}](
		func(_ context.Context,
		_ string, _ struct{}) (steward.Unit, error) { return u, nil },
		func(a, b struct{}) bool { return true },)
	set.Start(context.Background())
	set.Reconcile(map[string]struct{}{"k": {}})
	waitUntil(t, time.Second, func() bool { return set.Running("k") })
	set.Stop(context.Background())
	select {
	case <-drained:
	default:
		t.Fatal("drain not invoked")
	}
}

type drainProbe struct {
	id      string
	drained chan struct{}
}

func (d *drainProbe) ID() string { return d.id }
func (d *drainProbe) Start(ctx context.Context) error {
	go func() { <-ctx.Done() }()
	return nil
}
func (d *drainProbe) Drain(context.Context) error {
	select {
	case d.drained <- struct{}{}:
	default:
	}
	return nil
}
func (d *drainProbe) Stop(context.Context) error { return nil }

func TestCoverage_InstanceEvents(t *testing.T) {
	inst := steward.NewInstance(cfg{Name: "v1"}, stubCfgBuild(), stubCfgEqual())
	inst.Start(context.Background())
	ch := inst.Events()
	inst.Reload(cfg{Name: "v2"})
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("events closed early")
			}
			if ev.Type == steward.EventReloaded {
				inst.Stop(context.Background())
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for reload event")
		}
	}
}

func TestCoverage_ClassifyError(t *testing.T) {
	err := steward.ClassifyError(steward.FailureTransient, errors.New("dep"))
	if (steward.DefaultPolicy{}).Classify(err) != steward.FailureTransient {
		t.Fatal("policy did not unwrap classified error")
	}
}

func TestCoverage_StopBeforeStart(t *testing.T) {
	set := steward.NewSet[string, cfg](stubCfgBuild(), stubCfgEqual())
	if err := set.Stop(context.Background()); !errors.Is(err, steward.ErrNotStarted) {
		t.Fatalf("got %v", err)
	}
}

func TestInstance_ReplaceSnapshotConfig(t *testing.T) {
	makeHooks := func(g int) (steward.BuildFunc[string, cfg], steward.EqualFunc[cfg]) {
		return func(_ context.Context, id string, c cfg) (steward.Unit, error) {
				_ = g
				return steward.RunUnit(id, func(ctx context.Context) error {
					_ = c
					<-ctx.Done()
					return nil
				}), nil
			},
			func(a, b cfg) bool { return a == b }
	}

	build, equal := makeHooks(1)
	inst := steward.NewInstance(cfg{Name: "v1"}, build, equal)
	inst.Start(context.Background())
	build, equal = makeHooks(2)
	inst.Replace(build, equal, cfg{Name: "v2"})

	if inst.Config().Name != "v2" {
		t.Fatalf("config = %+v", inst.Config())
	}
	if len(inst.Snapshot()) != 1 {
		t.Fatalf("snapshot units = %d", len(inst.Snapshot()))
	}
	inst.Stop(context.Background())
}

type classifyAllTransient struct{ steward.DefaultPolicy }

func (classifyAllTransient) Classify(error) steward.FailureClass {
	return steward.FailureTransient
}

func TestPolicy_CustomClassify(t *testing.T) {
	set := steward.NewSet[string, int](
		func(_ context.Context, id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },
		steward.WithPolicy(classifyAllTransient{}),
	)
	set.Start(context.Background())
	set.Stop(context.Background())
}

func TestPolicy_AllFailureClasses(t *testing.T) {
	p := steward.DefaultPolicy{}
	if p.ShouldRestart(steward.StateRunning, steward.Failure{Class: steward.FailureConfigError}) {
		t.Fatal("config error should not restart")
	}
	if !p.ShouldRestart(steward.StateRunning, steward.Failure{Class: steward.FailureTransient}) {
		t.Fatal("transient should restart")
	}
}

func TestClassifiedErrorString(t *testing.T) {
	err := steward.ClassifyError(steward.FailureFatal, context.Canceled)
	if err.Error() == "" {
		t.Fatal("expected error string")
	}
	if (steward.DefaultPolicy{}).Classify(context.Canceled) != steward.FailureTransient {
		t.Fatal("context canceled should be transient")
	}
}
