package steward

import (
	"context"
	"errors"
	"time"
)

// unitSlot is scheduler-owned unit state. Only the scheduler goroutine mutates it.
type unitSlot[C any] struct {
	id   string
	cfg  C
	unit Unit

	state          State
	lastTransition time.Time
	startedAt      time.Time
	lastError      error
	failureClass   FailureClass

	cancel context.CancelFunc
	done   <-chan struct{}
}

func newUnitSlot[C any](id string, cfg C, unit Unit) *unitSlot[C] {
	now := time.Now()
	return &unitSlot[C]{
		id:             id,
		cfg:            cfg,
		unit:           unit,
		state:          StateStarting,
		lastTransition: now,
	}
}

func (s *unitSlot[C]) view(restarts int) UnitView {
	uptime := time.Duration(0)
	if s.state == StateRunning && !s.startedAt.IsZero() {
		uptime = time.Since(s.startedAt)
	}
	return UnitView{
		ID:             s.id,
		State:          s.state,
		LastTransition: s.lastTransition,
		Uptime:         uptime,
		RestartCount:   restarts,
		LastError:      s.lastError,
		FailureClass:   s.failureClass,
	}
}

func (s *unitSlot[C]) mark(state State) {
	s.state = state
	s.lastTransition = time.Now()
}

func (s *unitSlot[C]) markRunning() {
	s.state = StateRunning
	s.startedAt = time.Now()
	s.lastTransition = s.startedAt
}

func (s *unitSlot[C]) markFailed(class FailureClass, err error) {
	s.state = StateFailed
	s.lastError = err
	s.failureClass = class
	s.lastTransition = time.Now()
}

func (s *unitSlot[C]) stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.done == nil {
		return nil
	}
	select {
	case <-s.done:
		s.mark(StateStopped)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// launchSupervisor runs unit lifecycle in a goroutine owned by the slot.
// Callbacks enqueue scheduler commands; they must not touch slot state directly.
func launchSupervisor(
	parent context.Context,
	unit Unit,
	policy Policy,
	notify func(any),
) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})

	go func() {
		defer close(done)
		runSupervisor(ctx, unit, policy, notify)
	}()

	return cancel, done
}

func runSupervisor(
	ctx context.Context,
	unit Unit,
	policy Policy,
	notify func(any),
) {
	if err := unit.Start(ctx); err != nil {
		reportFailure(policy, err, notify)
		return
	}

	if r, ok := unit.(Ready); ok {
		readyCtx, cancel := context.WithTimeout(ctx, policy.StartTimeout(unit.ID()))
		err := r.WaitReady(readyCtx)
		cancel()
		if err != nil {
			_ = shutdownUnit(unit, policy)
			reportFailure(policy, err, notify)
			return
		}
	}

	notify(supervisorReady{})

	if err := waitForUnit(ctx, unit); err != nil {
		_ = shutdownUnit(unit, policy)
		reportFailure(policy, err, notify)
		return
	}

	_ = shutdownUnit(unit, policy)
}

// runner is implemented by units that have a blocking run function (e.g. RunUnit).
// The supervisor waits for the runner to complete or for ctx cancellation.
type runner interface {
	waitRun(context.Context) error
}

func waitForUnit(ctx context.Context, unit Unit) error {
	if w, ok := unit.(runner); ok {
		return w.waitRun(ctx)
	}
	<-ctx.Done()
	return nil
}

func shutdownUnit(unit Unit, policy Policy) error {
	if d, ok := unit.(Drainer); ok {
		drainCtx, cancel := context.WithTimeout(context.Background(), policy.DrainTimeout(unit.ID()))
		_ = d.Drain(drainCtx)
		cancel()
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), policy.DrainTimeout(unit.ID()))
	defer cancel()
	return unit.Stop(stopCtx)
}

func reportFailure(policy Policy, err error, notify func(any)) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	notify(supervisorFailed{failure: Failure{Class: policy.Classify(err), Err: err}})
}

type supervisorReady struct{}

type supervisorFailed struct {
	failure Failure
}
