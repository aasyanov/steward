package steward_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

type hotCfg struct {
	Name    string
	Version int
}

func TestRace_ParallelReconcileSnapshot(t *testing.T) {
	set := steward.NewSet[string, hotCfg](
		func(_ context.Context,
		id string, _ hotCfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b hotCfg) bool { return a.Name == b.Name && a.Version == b.Version },)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]hotCfg{"a": {Name: "a"}, "b": {Name: "b"}})
	waitUntil(t, time.Second, func() bool {
		return set.Running("a") && set.Running("b")
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					d := map[string]hotCfg{
						"a": {Name: "a"},
						"b": {Name: "b", Version: n % 3},
					}
					_ = set.Reconcile(d)
					_ = set.Snapshot()
					_ = set.Running("a")
					_ = set.Failed("b")
				}
			}
		}(i)
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestRace_ParallelReplace(t *testing.T) {
	gen := atomic.Int32{}
	makeHooks := func(g int32) (steward.BuildFunc[string, hotCfg], steward.EqualFunc[hotCfg]) {
		return func(_ context.Context, id string, c hotCfg) (steward.Unit, error) {
				_ = g
				_ = c
				return steward.RunUnit(id, func(ctx context.Context) error {
					<-ctx.Done()
					return nil
				}), nil
			},
			func(a, b hotCfg) bool { return a == b }
	}

	build, equal := makeHooks(0)
	set := steward.NewSet[string, hotCfg](build, equal)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					g := gen.Add(1)
					b, e := makeHooks(g)
					_ = set.Replace(b, e, map[string]hotCfg{
						"x": {Name: "x"},
						"y": {Name: "y"},
					})
				}
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestSet_DoubleStart(t *testing.T) {
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },)
	if err := set.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer set.Stop(context.Background())
	if err := set.Start(context.Background()); !errors.Is(err, steward.ErrAlreadyStarted) {
		t.Fatalf("got %v", err)
	}
}

func TestSet_ReconcileAfterStop(t *testing.T) {
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },)
	set.Start(context.Background())
	set.Stop(context.Background())
	if err := set.Reconcile(map[string]cfg{"a": {}}); !errors.Is(err, steward.ErrStopped) {
		t.Fatalf("got %v", err)
	}
}

func TestSet_GoroutineLeakOnStop(t *testing.T) {
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },
	)

	before := runtimeNumGoroutine()
	set.Start(context.Background())
	set.Reconcile(map[string]cfg{
		"a": {Name: "a"}, "b": {Name: "b"}, "c": {Name: "c"},
	})
	set.Stop(context.Background())
	gcAndWait()
	after := runtimeNumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}
