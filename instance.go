package steward

import (
	"context"
	"sync"
)

const singletonKey = "default"

// Instance is a singleton Set: one config type, fixed key "default".
type Instance[C any] struct {
	inner *Set[string, C]
	mu    sync.Mutex
	cfg   C
}

// NewInstance creates a singleton Set with initial config.
func NewInstance[C any](cfg C, build BuildFunc[string, C], equal EqualFunc[C], opts ...Option) *Instance[C] {
	return &Instance[C]{
		inner: NewSet[string, C](build, equal, opts...),
		cfg:   cfg,
	}
}

// Start begins the scheduler and reconciles the initial config.
func (i *Instance[C]) Start(ctx context.Context) error {
	if err := i.inner.Start(ctx); err != nil {
		return err
	}
	return i.inner.Reconcile(map[string]C{singletonKey: i.cfg})
}

// Reload replaces config and restarts the unit.
func (i *Instance[C]) Reload(cfg C) error {
	i.mu.Lock()
	i.cfg = cfg
	i.mu.Unlock()
	return i.inner.Reconcile(map[string]C{singletonKey: cfg})
}

// Replace atomically swaps build/equal and config, recreating the unit.
func (i *Instance[C]) Replace(build BuildFunc[string, C], equal EqualFunc[C], cfg C) error {
	i.mu.Lock()
	i.cfg = cfg
	i.mu.Unlock()
	return i.inner.Replace(build, equal, map[string]C{singletonKey: cfg})
}

// Stop shuts down the unit and scheduler.
func (i *Instance[C]) Stop(ctx context.Context) error {
	return i.inner.Stop(ctx)
}

// Snapshot returns a shallow copy of unit views.
func (i *Instance[C]) Snapshot() []UnitView {
	return i.inner.Snapshot()
}

// Events returns the lifecycle event channel.
func (i *Instance[C]) Events() <-chan Event {
	return i.inner.Events()
}

// DroppedEvents returns primary event drops.
func (i *Instance[C]) DroppedEvents() uint64 {
	return i.inner.DroppedEvents()
}

// AuditEvents returns the audit channel when WithAuditBuffer is set.
func (i *Instance[C]) AuditEvents() <-chan Event {
	return i.inner.AuditEvents()
}

// DroppedAuditEvents returns audit pipeline drops.
func (i *Instance[C]) DroppedAuditEvents() uint64 {
	return i.inner.DroppedAuditEvents()
}

// Config returns the last applied configuration value.
func (i *Instance[C]) Config() C {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.cfg
}

// Running reports whether the singleton unit is running.
func (i *Instance[C]) Running() bool {
	return i.inner.Running(singletonKey)
}

// Failed reports whether the singleton unit failed.
func (i *Instance[C]) Failed() bool {
	return i.inner.Failed(singletonKey)
}
