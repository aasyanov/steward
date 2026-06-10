package steward

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockingUnit struct {
	id      string
	started chan struct{}
	release chan struct{}
}

func (u *blockingUnit) ID() string { return u.id }
func (u *blockingUnit) Start(ctx context.Context) error {
	go func() {
		close(u.started)
		select {
		case <-ctx.Done():
		case <-u.release:
		}
	}()
	return nil
}
func (u *blockingUnit) Stop(context.Context) error { return nil }

func TestSupervisor_ReadyCallback(t *testing.T) {
	ready := make(chan struct{}, 1)
	unit := RunUnit("u", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	notify := func(cmd any) {
		if _, ok := cmd.(supervisorReady); ok {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	}

	cancel, done := launchSupervisor(context.Background(), unit, DefaultPolicy{}, notify)
	defer cancel()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("ready not signaled")
	}
	cancel()
	<-done
}

func TestSupervisor_FailureCallback(t *testing.T) {
	fail := make(chan Failure, 1)
	unit := RunUnit("u", func(context.Context) error {
		return ClassifyError(FailureFatal, errors.New("boom"))
	})

	notify := func(cmd any) {
		if f, ok := cmd.(supervisorFailed); ok {
			fail <- f.failure
		}
	}

	_, done := launchSupervisor(context.Background(), unit, DefaultPolicy{}, notify)
	<-done

	select {
	case f := <-fail:
		if f.Class != FailureFatal {
			t.Fatalf("class = %v", f.Class)
		}
	default:
		t.Fatal("failure not reported")
	}
}

func TestUnitSlot_StopMarksStopped(t *testing.T) {
	u := &blockingUnit{
		id:      "x",
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	slot := newUnitSlot("x", 0, u)

	notify := func(any) {}
	cancel, done := launchSupervisor(context.Background(), u, DefaultPolicy{}, notify)
	slot.cancel = cancel
	slot.done = done

	<-u.started
	if err := slot.stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if slot.state != StateStopped {
		t.Fatalf("state = %v, want stopped", slot.state)
	}
}
