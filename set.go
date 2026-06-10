package steward

import (
	"context"
	"sync"
)

// Set is the keyed lifecycle Set: desired state → actual units.
//
// All public methods are safe for concurrent use.
// Reconcile with unchanged desired is a no-op (idempotent).
// Stop is idempotent: repeated calls return nil.
type Set[K comparable, C any] struct {
	sched *scheduler[K, C]
	loop  chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool
}

// NewSet creates a Set with the given controller and options.
func NewSet[K comparable, C any](build BuildFunc[K, C], equal EqualFunc[C], opts ...Option) *Set[K, C] {
	validateSetFuncs(build, equal)
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Set[K, C]{
		sched: newScheduler[K, C](build, equal, cfg.policy, cfg.handoff, cfg.eventBuffer, cfg.auditBuffer),
	}
}

// Start begins the scheduler loop. Must be called exactly once before Reconcile.
// Returns ErrAlreadyStarted on repeated call, ErrStopped after Stop.
func (s *Set[K, C]) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		if s.stopped {
			return ErrStopped
		}
		return ErrAlreadyStarted
	}

	s.loop = make(chan struct{})
	go func() {
		s.sched.loop()
		close(s.loop)
	}()

	if err := s.sched.callStart(ctx); err != nil {
		close(s.sched.cmdCh)
		<-s.loop
		return err
	}
	s.started = true
	return nil
}

// Reconcile converges actual state to desired using diff + current controller.
// Idempotent: calling with the same desired map is a no-op.
// Returns ErrStopped after Stop, ErrNotStarted before Start.
func (s *Set[K, C]) Reconcile(desired map[K]C) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	if !s.started {
		s.mu.Unlock()
		return ErrNotStarted
	}
	s.mu.Unlock()
	return s.sched.callReconcile(desired, false, nil, nil)
}

// Replace atomically swaps build/equal and recreates all units.
func (s *Set[K, C]) Replace(build BuildFunc[K, C], equal EqualFunc[C], desired map[K]C) error {
	validateSetFuncs(build, equal)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	if !s.started {
		s.mu.Unlock()
		return ErrNotStarted
	}
	s.mu.Unlock()
	return s.sched.callReconcile(desired, true, build, equal)
}

// Stop shuts down all units and the scheduler.
// Idempotent: repeated calls after the first successful Stop return nil.
func (s *Set[K, C]) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return ErrNotStarted
	}
	if s.stopped {
		return nil
	}

	err := s.sched.callStop(ctx)
	close(s.sched.cmdCh)
	if s.loop != nil {
		<-s.loop
	}
	s.stopped = true
	return err
}

// Snapshot returns a shallow read-only view of all units.
func (s *Set[K, C]) Snapshot() []UnitView {
	return s.sched.callSnapshot()
}

// Running reports whether key's unit is in the Running state.
func (s *Set[K, C]) Running(key K) bool {
	state, ok := s.sched.callUnitState(key)
	return ok && state == StateRunning
}

// Failed reports whether key's unit is in the Failed state.
func (s *Set[K, C]) Failed(key K) bool {
	state, ok := s.sched.callUnitState(key)
	return ok && state == StateFailed
}

// Events returns the lifecycle event channel (bounded, DropOldest on overflow).
func (s *Set[K, C]) Events() <-chan Event {
	return s.sched.events.channel()
}

// DroppedEvents returns the count of events dropped due to buffer overflow.
func (s *Set[K, C]) DroppedEvents() uint64 {
	return s.sched.events.droppedCount()
}

// AuditEvents returns the isolated audit event channel when WithAuditBuffer is set.
func (s *Set[K, C]) AuditEvents() <-chan Event {
	return s.sched.events.auditChannel()
}

// DroppedAuditEvents returns audit pipeline drops (never affects reconcile).
func (s *Set[K, C]) DroppedAuditEvents() uint64 {
	return s.sched.events.auditDroppedCount()
}
