package steward

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEventBusEmitAuditDrop(t *testing.T) {
	b := newEventBus(4, 1)
	b.emit(Event{UnitID: "a"})
	b.emit(Event{UnitID: "b"})
	if b.auditDroppedCount() == 0 {
		t.Fatal("expected audit drops")
	}
}

func TestEventBusCloseWithAudit(t *testing.T) {
	b := newEventBus(4, 4)
	b.close()
	ch := b.auditChannel()
	if _, ok := <-ch; ok {
		t.Fatal("expected closed audit channel")
	}
}

func TestFailureClassAllStrings(t *testing.T) {
	if FailureConfigError.String() != "config_error" {
		t.Fatal()
	}
	if FailureTransient.String() != "transient" {
		t.Fatal()
	}
}

func TestPolicyFatalNoRestart(t *testing.T) {
	p := DefaultPolicy{}
	if p.ShouldRestart(StateRunning, Failure{Class: FailureFatal}) {
		t.Fatal()
	}
}

func TestPolicyCustomDrainAndBackoff(t *testing.T) {
	p := DefaultPolicy{
		Drain:      5 * time.Second,
		MaxBackoff: 2 * time.Second,
	}
	if p.DrainTimeout("u") != 5*time.Second {
		t.Fatal()
	}
	if p.Backoff("u", 5) != 2*time.Second {
		t.Fatal()
	}
}

func TestRunUnitDoubleStart(t *testing.T) {
	u := RunUnit("id", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	u.Start(context.Background())
	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunUnitStopTimeout(t *testing.T) {
	block := make(chan struct{})
	u := RunUnit("id", func(context.Context) error {
		<-block
		return nil
	})
	u.Start(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := u.Stop(ctx); err == nil {
		t.Fatal("expected stop timeout")
	}
	close(block)
}

func TestRunUnitFailChDefaultBranch(t *testing.T) {
	u := RunUnit("id", func(context.Context) error {
		return errors.New("boom")
	}).(*runUnit)
	u.failCh = make(chan error) // unbuffered blocks send -> default branch
	u.Start(context.Background())
	time.Sleep(50 * time.Millisecond)
}

func TestRunUnitWaitRunNilFailCh(t *testing.T) {
	u := &runUnit{id: "x"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := u.waitRun(ctx); err != nil {
		t.Fatal(err)
	}
}
