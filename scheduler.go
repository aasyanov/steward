package steward

import (
	"context"
	"fmt"
	"time"
)

const cmdBuffer = 256

type schedState int

const (
	schedNew schedState = iota
	schedRunning
	schedStopped
)

// scheduler is the single goroutine owner of all lifecycle state.
type scheduler[K comparable, C any] struct {
	cmdCh chan any

	build   BuildFunc[K, C]
	equal   EqualFunc[C]
	policy  Policy
	handoff Handoff
	events       *eventBus
	parentCtx    context.Context
	state        schedState
	engines      map[K]*unitSlot[C]
	configs      map[K]C
	pendingRest  map[K]time.Time
	restarts     map[K]int
	restartTimer *time.Timer
}

func newScheduler[K comparable, C any](
	build BuildFunc[K, C],
	equal EqualFunc[C],
	policy Policy,
	handoff Handoff,
	eventBuf int,
	auditBuf int,
) *scheduler[K, C] {
	if policy == nil {
		policy = DefaultPolicy{}
	}
	return &scheduler[K, C]{
		cmdCh:   make(chan any, cmdBuffer),
		build:   build,
		equal:   equal,
		policy:  policy,
		handoff: handoff,
		events:      newEventBus(eventBuf, auditBuf),
		engines:     make(map[K]*unitSlot[C]),
		configs:     make(map[K]C),
		pendingRest: make(map[K]time.Time),
		restarts:    make(map[K]int),
	}
}

func (s *scheduler[K, C]) loop() {
	for cmd := range s.cmdCh {
		s.dispatch(cmd)
	}
}

func (s *scheduler[K, C]) dispatch(cmd any) {
	switch c := cmd.(type) {
	case startCmd:
		c.resp <- s.handleStart(c.ctx)
	case reconcileCmd[K, C]:
		c.resp <- s.handleReconcile(c.desired, c.force, c.build, c.equal)
	case stopCmd:
		c.resp <- s.handleStop(c.ctx)
	case snapshotCmd:
		c.resp <- s.snapshot()
	case tickCmd:
		s.handlePendingRestarts()
	case unitReadyCmd[K]:
		s.handleUnitReady(c.key)
	case unitFailedCmd[K]:
		s.handleUnitFailed(c.key, c.failure)
	case unitStateQueryCmd[K]:
		c.resp <- s.queryUnitState(c.key)
	}
}

type unitStateResp struct {
	state State
	ok    bool
}

type unitStateQueryCmd[K comparable] struct {
	key  K
	resp chan unitStateResp
}

type startCmd struct {
	ctx  context.Context
	resp chan error
}

type reconcileCmd[K comparable, C any] struct {
	desired map[K]C
	force   bool
	build BuildFunc[K, C]
	equal EqualFunc[C]
	resp  chan error
}

type stopCmd struct {
	ctx  context.Context
	resp chan error
}

type snapshotCmd struct {
	resp chan []UnitView
}

type tickCmd struct{}

type unitReadyCmd[K comparable] struct {
	key K
}

type unitFailedCmd[K comparable] struct {
	key     K
	failure Failure
}

func (s *scheduler[K, C]) handleStart(ctx context.Context) error {
	if s.state == schedRunning {
		return ErrAlreadyStarted
	}
	if s.state == schedStopped {
		return ErrStopped
	}
	s.parentCtx = ctx
	s.state = schedRunning
	return nil
}

func (s *scheduler[K, C]) handleReconcile(
	desired map[K]C,
	force bool,
	build BuildFunc[K, C],
	equal EqualFunc[C],
) error {
	if s.state != schedRunning {
		return ErrNotStarted
	}
	if build != nil {
		s.build = build
	}
	if equal != nil {
		s.equal = equal
	}

	if force {
		return s.forceReconcile(desired)
	}

	plan := Diff(s.configs, desired, s.equal)

	for _, key := range plan.Removes {
		if err := s.removeUnit(key); err != nil {
			return err
		}
	}
	for _, entry := range plan.Updates {
		if err := s.replaceUnit(entry.Key, entry.Config); err != nil {
			return err
		}
		s.events.emit(Event{
			UnitID: formatKey(entry.Key),
			Type:   EventReloaded,
			From:   StateRunning,
			To:     StateRunning,
		})
	}
	for _, entry := range plan.Creates {
		if err := s.startUnit(entry.Key, entry.Config); err != nil {
			return err
		}
		s.events.emit(Event{
			UnitID: formatKey(entry.Key),
			Type:   EventStarted,
			From:   StateCreated,
			To:     StateRunning,
		})
	}

	for k, v := range desired {
		s.configs[k] = v
	}
	for k := range s.configs {
		if _, ok := desired[k]; !ok {
			delete(s.configs, k)
		}
	}
	return nil
}

func (s *scheduler[K, C]) forceReconcile(desired map[K]C) error {
	for key, slot := range s.engines {
		_ = slot.stop(s.parentCtx)
		delete(s.engines, key)
		s.events.emit(Event{
			UnitID: formatKey(key),
			Type:   EventStopped,
			From:   slot.state,
			To:     StateStopped,
		})
	}
	for key, cfg := range desired {
		if err := s.startUnit(key, cfg); err != nil {
			return err
		}
		s.events.emit(Event{
			UnitID: formatKey(key),
			Type:   EventReloaded,
			From:   StateCreated,
			To:     StateRunning,
		})
	}
	s.configs = make(map[K]C, len(desired))
	for k, v := range desired {
		s.configs[k] = v
	}
	return nil
}

func (s *scheduler[K, C]) startUnit(key K, cfg C) error {
	id := formatKey(key)
	unit, err := s.build(s.parentCtx, id, cfg)
	if err != nil {
		s.events.emit(Event{UnitID: id, Type: EventFailed, To: StateFailed, Err: err})
		return err
	}
	s.attachUnit(key, cfg, unit)
	return nil
}

func (s *scheduler[K, C]) attachUnit(key K, cfg C, unit Unit) {
	id := formatKey(key)
	slot := newUnitSlot(id, cfg, unit)
	s.engines[key] = slot
	s.configs[key] = cfg

	notify := func(cmd any) {
		switch c := cmd.(type) {
		case supervisorReady:
			s.notify(unitReadyCmd[K]{key: key})
		case supervisorFailed:
			s.notify(unitFailedCmd[K]{key: key, failure: c.failure})
		}
	}

	cancel, done := launchSupervisor(s.parentCtx, unit, s.policy, notify)
	slot.cancel = cancel
	slot.done = done
}

func (s *scheduler[K, C]) replaceUnit(key K, cfg C) error {
	slot, ok := s.engines[key]
	if !ok {
		return s.startUnit(key, cfg)
	}

	id := formatKey(key)
	newUnit, err := s.build(s.parentCtx, id, cfg)
	if err != nil {
		s.events.emit(Event{UnitID: id, Type: EventFailed, To: StateFailed, Err: err})
		return err
	}

	if s.handoff != nil && slot.unit != nil {
		if err := s.handoff(slot.unit, newUnit); err != nil {
			return err
		}
	}

	if err := slot.stop(s.parentCtx); err != nil {
		return err
	}
	delete(s.engines, key)

	s.attachUnit(key, cfg, newUnit)
	return nil
}

func (s *scheduler[K, C]) removeUnit(key K) error {
	slot, ok := s.engines[key]
	if !ok {
		delete(s.configs, key)
		return nil
	}
	from := slot.state
	if err := slot.stop(s.parentCtx); err != nil {
		return err
	}
	delete(s.engines, key)
	delete(s.configs, key)
	s.events.emit(Event{
		UnitID: formatKey(key),
		Type:   EventStopped,
		From:   from,
		To:     StateStopped,
	})
	return nil
}

func (s *scheduler[K, C]) handleStop(ctx context.Context) error {
	if s.state == schedNew {
		return ErrNotStarted
	}
	if s.state == schedStopped {
		return nil
	}
	s.stopRestartTimer()
	for key, slot := range s.engines {
		from := slot.state
		_ = slot.stop(ctx)
		s.events.emit(Event{
			UnitID: formatKey(key),
			Type:   EventStopped,
			From:   from,
			To:     StateStopped,
		})
	}
	s.engines = make(map[K]*unitSlot[C])
	s.configs = make(map[K]C)
	s.pendingRest = make(map[K]time.Time)
	s.restarts = make(map[K]int)
	s.state = schedStopped
	s.events.close()
	return nil
}

func (s *scheduler[K, C]) snapshot() []UnitView {
	views := make([]UnitView, 0, len(s.engines))
	for key, slot := range s.engines {
		views = append(views, slot.view(s.restarts[key]))
	}
	return copyUnitViews(views)
}

func (s *scheduler[K, C]) handleUnitReady(key K) {
	if s.state != schedRunning {
		return
	}
	slot, ok := s.engines[key]
	if !ok {
		return
	}
	slot.markRunning()
}

func (s *scheduler[K, C]) handleUnitFailed(key K, failure Failure) {
	if s.state != schedRunning {
		return
	}
	slot, ok := s.engines[key]
	if !ok {
		return
	}
	from := slot.state
	slot.markFailed(failure.Class, failure.Err)
	s.events.emit(Event{
		UnitID: formatKey(key),
		Type:   EventFailed,
		From:   from,
		To:     StateFailed,
		Err:    failure.Err,
	})

	if !s.policy.ShouldRestart(slot.state, failure) {
		return
	}

	attempt := s.restarts[key] + 1
	delay := s.policy.Backoff(formatKey(key), attempt)
	s.pendingRest[key] = time.Now().Add(delay)
	s.armRestartTimer()
}

func (s *scheduler[K, C]) handlePendingRestarts() {
	if s.state != schedRunning {
		return
	}
	now := time.Now()
	for key, at := range s.pendingRest {
		if now.Before(at) {
			continue
		}
		delete(s.pendingRest, key)
		cfg, ok := s.configs[key]
		if !ok {
			continue
		}
		if slot, exists := s.engines[key]; exists {
			_ = slot.stop(s.parentCtx)
			delete(s.engines, key)
		}
		s.restarts[key]++
		if err := s.startUnit(key, cfg); err == nil {
			s.events.emit(Event{
				UnitID: formatKey(key),
				Type:   EventStarted,
				From:   StateFailed,
				To:     StateRunning,
			})
		}
	}
	if len(s.pendingRest) > 0 {
		s.armRestartTimer()
	} else {
		s.stopRestartTimer()
	}
}

func (s *scheduler[K, C]) armRestartTimer() {
	s.stopRestartTimer()
	var earliest time.Time
	for _, at := range s.pendingRest {
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
	}
	if earliest.IsZero() {
		return
	}
	d := time.Until(earliest)
	if d < 0 {
		d = 0
	}
	s.restartTimer = time.AfterFunc(d, func() {
		s.notify(tickCmd{})
	})
}

func (s *scheduler[K, C]) stopRestartTimer() {
	if s.restartTimer != nil {
		s.restartTimer.Stop()
		s.restartTimer = nil
	}
}

// notify enqueues an internal command without blocking the scheduler loop.
func (s *scheduler[K, C]) notify(cmd any) {
	select {
	case s.cmdCh <- cmd:
	default:
		go func() { s.cmdCh <- cmd }()
	}
}

func (s *scheduler[K, C]) callStart(ctx context.Context) error {
	resp := borrowErrResp()
	defer returnErrResp(resp)
	s.cmdCh <- startCmd{ctx: ctx, resp: resp}
	return <-resp
}

func (s *scheduler[K, C]) callReconcile(desired map[K]C, force bool, build BuildFunc[K, C], equal EqualFunc[C]) error {
	resp := borrowErrResp()
	defer returnErrResp(resp)
	s.cmdCh <- reconcileCmd[K, C]{desired: desired, force: force, build: build, equal: equal, resp: resp}
	return <-resp
}

func (s *scheduler[K, C]) callStop(ctx context.Context) error {
	resp := borrowErrResp()
	defer returnErrResp(resp)
	s.cmdCh <- stopCmd{ctx: ctx, resp: resp}
	return <-resp
}

func (s *scheduler[K, C]) callSnapshot() []UnitView {
	resp := make(chan []UnitView, 1)
	s.cmdCh <- snapshotCmd{resp: resp}
	return <-resp
}

func (s *scheduler[K, C]) queryUnitState(key K) unitStateResp {
	slot, ok := s.engines[key]
	if !ok {
		return unitStateResp{state: StateStopped, ok: false}
	}
	return unitStateResp{state: slot.state, ok: true}
}

func (s *scheduler[K, C]) callUnitState(key K) (State, bool) {
	resp := make(chan unitStateResp, 1)
	s.cmdCh <- unitStateQueryCmd[K]{key: key, resp: resp}
	r := <-resp
	return r.state, r.ok
}

func formatKey[K comparable](key K) string {
	if s, ok := any(key).(string); ok {
		return s
	}
	return fmt.Sprintf("%v", key)
}
