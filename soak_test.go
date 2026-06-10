package steward_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

func TestSoak_RapidReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak in -short mode")
	}

	var active atomic.Int32
	set := steward.NewSet[int, int](
		func(_ context.Context,
		id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				active.Add(1)
				defer active.Add(-1)
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },)
	set.Start(context.Background())

	deadline := time.Now().Add(3 * time.Second)
	i := 0
	for time.Now().Before(deadline) {
		set.Reconcile(map[int]int{
			i % 13:       i,
			(i + 1) % 13: i + 1,
			(i + 5) % 13: i + 5,
		})
		i++
	}

	set.Stop(context.Background())
	gcAndWait()
	if active.Load() != 0 {
		t.Fatalf("leaked workers: %d", active.Load())
	}
}

func TestSoak_InstanceReload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak in -short mode")
	}

	inst := steward.NewInstance(0,
		func(_ context.Context,
		id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },)
	inst.Start(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for i := 0; i < 200 && time.Now().Before(deadline); i++ {
		_ = inst.Reload(i)
	}
	inst.Stop(context.Background())
}

func TestSoak_ParallelReplace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak in -short mode")
	}

	gen := 0
	makeHooks := func() (steward.BuildFunc[string, int], steward.EqualFunc[int]) {
		gen++
		g := gen
		return func(_ context.Context, id string, v int) (steward.Unit, error) {
				_ = g
				return steward.RunUnit(id, func(ctx context.Context) error {
					_ = v
					<-ctx.Done()
					return nil
				}), nil
			},
			func(a, b int) bool { return a == b }
	}

	build, equal := makeHooks()
	set := steward.NewSet[string, int](build, equal)
	set.Start(context.Background())
	set.Reconcile(map[string]int{"a": 1, "b": 2})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, e := makeHooks()
		_ = set.Replace(b, e, map[string]int{"a": 1, "b": 2, "c": 3})
		_ = set.Reconcile(map[string]int{"a": 1})
	}
	set.Stop(context.Background())
}

type fastRestartPolicy struct {
	steward.DefaultPolicy
}

func (fastRestartPolicy) Backoff(_ string, _ int) time.Duration {
	return 50 * time.Millisecond
}

func TestSoak_PolicyRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak in -short mode")
	}

	attempts := make(chan struct{}, 64)
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(context.Context) error {
				select {
				case attempts <- struct{}{}:
				default:
				}
				return steward.ClassifyError(steward.FailureTransient, fmt.Errorf("transient"))
			}), nil
		},
		func(a, b cfg) bool { return a == b }, steward.WithPolicy(fastRestartPolicy{}))
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]cfg{"u": {Name: "u"}})

	var count int
	deadline := time.After(2 * time.Second)
	for count < 2 {
		select {
		case <-attempts:
			count++
		case <-deadline:
			t.Fatalf("only %d restart attempts, want >= 2", count)
		}
	}
}
