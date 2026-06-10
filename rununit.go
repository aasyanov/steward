package steward

import (
	"context"
	"errors"
	"sync"
)

// RunFunc is a blocking lifecycle function compatible with the Unit model.
type RunFunc func(ctx context.Context) error

// RunUnit wraps a RunFunc as a Unit. Start launches fn in a goroutine and returns
// immediately; Stop cancels the context and waits for fn to exit.
func RunUnit(id string, fn RunFunc) Unit {
	return &runUnit{id: id, fn: fn}
}

type runUnit struct {
	id    string
	fn    RunFunc
	mu    sync.Mutex
	state State

	cancel context.CancelFunc
	done   chan struct{}
	failCh chan error
}

// ID implements Unit.
func (u *runUnit) ID() string { return u.id }

// Start implements Unit.
func (u *runUnit) Start(ctx context.Context) error {
	u.mu.Lock()
	if u.cancel != nil {
		u.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	u.cancel = cancel
	u.done = make(chan struct{})
	u.failCh = make(chan error, 1)
	u.state = StateRunning
	u.mu.Unlock()

	go func() {
		defer close(u.done)
		err := u.fn(runCtx)
		u.mu.Lock()
		defer u.mu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			u.state = StateFailed
			select {
			case u.failCh <- err:
			default:
			}
		} else {
			u.state = StateStopped
			close(u.failCh)
		}
	}()
	return nil
}

// waitRun blocks until ctx is cancelled or the run function exits with error.
func (u *runUnit) waitRun(ctx context.Context) error {
	if u.failCh == nil {
		<-ctx.Done()
		return nil
	}
	select {
	case err, ok := <-u.failCh:
		if ok && err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return nil
	}
}

// Stop implements Unit.
func (u *runUnit) Stop(ctx context.Context) error {
	u.mu.Lock()
	cancel := u.cancel
	done := u.done
	u.mu.Unlock()

	if cancel == nil {
		return nil
	}
	u.setState(StateStopping)
	cancel()
	select {
	case <-done:
		u.setState(StateStopped)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// State returns internal lifecycle tracking (not part of Unit).
func (u *runUnit) State() State {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state
}

func (u *runUnit) setState(s State) {
	u.mu.Lock()
	u.state = s
	u.mu.Unlock()
}
