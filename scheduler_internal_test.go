package steward

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func stubBuild() BuildFunc[string, int] {
	return func(_ context.Context, id string, _ int) (Unit, error) {
		return RunUnit(id, func(ctx context.Context) error { <-ctx.Done(); return nil }), nil
	}
}

func stubEqual() EqualFunc[int] {
	return func(a, b int) bool { return a == b }
}

func TestNewSchedulerDefaults(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		nil, nil, 8, 0,
	)
	if s.policy == nil {
		t.Fatal("expected default policy")
	}
}

func TestHandleStartErrors(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	if err := s.handleStart(context.Background()); err != ErrAlreadyStarted {
		t.Fatalf("got %v", err)
	}
	s.state = schedStopped
	if err := s.handleStart(context.Background()); err != ErrStopped {
		t.Fatalf("got %v", err)
	}
}

func TestHandleReconcileNotRunning(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	if err := s.handleReconcile(map[string]int{"a": 1}, false, nil, nil); err != ErrNotStarted {
		t.Fatalf("got %v", err)
	}
}

func TestRemoveUnitAbsent(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	s.parentCtx = context.Background()
	if err := s.removeUnit("missing"); err != nil {
		t.Fatal(err)
	}
}

func TestReplaceUnitCreatesWhenMissing(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	s.parentCtx = context.Background()
	if err := s.replaceUnit("new", 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.engines["new"]; !ok {
		t.Fatal("expected engine")
	}
	_ = s.handleStop(context.Background())
}

func TestForceReconcileBuildError(t *testing.T) {
	s := newScheduler[string, int](
		func(_ context.Context, _ string, v int) (Unit, error) {
			if v == 2 {
				return nil, errors.New("fail")
			}
			return RunUnit("x", func(ctx context.Context) error { <-ctx.Done(); return nil }), nil
		},
		func(a, b int) bool { return a == b },
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	s.parentCtx = context.Background()
	s.engines["a"] = newUnitSlot("a", 1, RunUnit("a", func(ctx context.Context) error { <-ctx.Done(); return nil }))
	if err := s.forceReconcile(map[string]int{"a": 1, "b": 2}); err == nil {
		t.Fatal("expected error")
	}
}

func TestHandleUnitFailedNoRestart(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	s.engines["k"] = newUnitSlot("k", 0, RunUnit("k", func(context.Context) error { return nil }))
	s.handleUnitFailed("k", Failure{Class: FailureConfigError, Err: errors.New("cfg")})
	if len(s.pendingRest) != 0 {
		t.Fatal("config error should not schedule restart")
	}
}

func TestHandleUnitFailedNotRunningState(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedStopped
	s.handleUnitFailed("k", Failure{Class: FailureTransient, Err: errors.New("x")})
}

func TestHandlePendingRestartsStartError(t *testing.T) {
	s := newScheduler[string, int](
		func(_ context.Context, _ string, _ int) (Unit, error) {
			return nil, errors.New("build")
		},
		func(a, b int) bool { return a == b },
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedRunning
	s.parentCtx = context.Background()
	s.configs["k"] = 1
	s.pendingRest["k"] = time.Now().Add(-time.Second)
	s.handlePendingRestarts()
}

func TestNotifyWhenChannelFull(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 1, 0,
	)
	s.cmdCh = make(chan any, 1)
	s.cmdCh <- tickCmd{}
	s.notify(tickCmd{})
}

func TestHandleStopNotStarted(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	if err := s.handleStop(context.Background()); err != ErrNotStarted {
		t.Fatalf("got %v", err)
	}
}

func TestHandleStopAlreadyStopped(t *testing.T) {
	s := newScheduler[string, int](
		stubBuild(), stubEqual(),
		DefaultPolicy{}, nil, 8, 0,
	)
	s.state = schedStopped
	if err := s.handleStop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUnitSlotStopTimeout(t *testing.T) {
	block := make(chan struct{})
	u := RunUnit("x", func(ctx context.Context) error {
		<-block
		return nil
	})
	slot := newUnitSlot("x", 0, u)
	notify := func(any) {}
	cancel, done := launchSupervisor(context.Background(), u, DefaultPolicy{}, notify)
	slot.cancel = cancel
	slot.done = done
	time.Sleep(20 * time.Millisecond)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelCtx()
	if err := slot.stop(ctx); err == nil {
		t.Fatal("expected timeout")
	}
	close(block)
}

func TestRunSupervisorStartError(t *testing.T) {
	var failed Failure
	u := &failStartUnit{}
	runSupervisor(context.Background(), u, DefaultPolicy{}, func(cmd any) {
		if f, ok := cmd.(supervisorFailed); ok {
			failed = f.failure
		}
	})
	if failed.Err == nil {
		t.Fatal("expected failure")
	}
}

func TestRunSupervisorWaitReadyError(t *testing.T) {
	var failed Failure
	u := &failReadyUnit{ready: make(chan struct{})}
	runSupervisor(context.Background(), u, DefaultPolicy{Start: 50 * time.Millisecond}, func(cmd any) {
		if f, ok := cmd.(supervisorFailed); ok {
			failed = f.failure
		}
	})
	if failed.Err == nil {
		t.Fatal("expected ready failure")
	}
}

func TestRunSupervisorPlainUnitWaitsCtx(t *testing.T) {
	ready := make(chan struct{}, 1)
	u := &plainUnit{id: "p"}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSupervisor(ctx, u, DefaultPolicy{}, func(cmd any) {
			if _, ok := cmd.(supervisorReady); ok {
				ready <- struct{}{}
			}
		})
	}()
	<-ready
	cancel()
	wg.Wait()
}

func TestReportFailureIgnoresCancel(t *testing.T) {
	called := false
	reportFailure(DefaultPolicy{}, context.Canceled, func(any) { called = true })
	if called {
		t.Fatal("should ignore canceled")
	}
}

func TestNewSetPanicsOnNilEqual(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil equal")
		}
	}()
	NewSet[string, int](
		func(_ context.Context, _ string, _ int) (Unit, error) { return nil, nil },
		nil,
	)
}

func TestFormatKeyStringNoAlloc(t *testing.T) {
	if got := formatKey("cam-1"); got != "cam-1" {
		t.Fatalf("got %q", got)
	}
	if got := formatKey(42); got != "42" {
		t.Fatalf("got %q", got)
	}
}

func TestNewSetPanicsOnNilBuild(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil build")
		}
	}()
	NewSet[string, int](nil, func(a, b int) bool { return a == b })
}

type failStartUnit struct{}

func (failStartUnit) ID() string { return "f" }
func (failStartUnit) Start(context.Context) error {
	return errors.New("start failed")
}
func (failStartUnit) Stop(context.Context) error { return nil }

type failReadyUnit struct {
	ready chan struct{}
}

func (u *failReadyUnit) ID() string { return "r" }
func (u *failReadyUnit) Start(ctx context.Context) error {
	go func() { <-ctx.Done() }()
	return nil
}
func (u *failReadyUnit) WaitReady(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (u *failReadyUnit) Stop(context.Context) error { return nil }

type plainUnit struct{ id string }

func (u *plainUnit) ID() string { return u.id }
func (u *plainUnit) Start(ctx context.Context) error {
	go func() { <-ctx.Done() }()
	return nil
}
func (u *plainUnit) Stop(context.Context) error { return nil }
