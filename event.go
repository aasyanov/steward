package steward

import "time"

// EventType identifies a lifecycle transition.
type EventType string

// Lifecycle event type constants.
const (
	EventStarted  EventType = "started"
	EventStopped  EventType = "stopped"
	EventReloaded EventType = "reloaded"
	EventFailed   EventType = "failed"
)

// Event describes a lifecycle transition.
type Event struct {
	UnitID string
	Type   EventType
	From   State
	To     State
	Err    error
	Time   time.Time
}

const defaultEventBuffer = 64

// eventBus delivers lifecycle events without blocking the scheduler.
// Primary channel uses DropOldest on overflow with accounting.
// Optional audit channel is isolated: drops never affect reconcile.
type eventBus struct {
	primary      chan Event
	dropped      uint64
	audit        chan Event
	auditDropped uint64
}

func newEventBus(primaryBuf, auditBuf int) *eventBus {
	if primaryBuf <= 0 {
		primaryBuf = defaultEventBuffer
	}
	b := &eventBus{
		primary: make(chan Event, primaryBuf),
	}
	if auditBuf > 0 {
		b.audit = make(chan Event, auditBuf)
	}
	return b
}

func (b *eventBus) emit(e Event) {
	if b == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	b.emitPrimary(e)
	b.emitAudit(e)
}

func (b *eventBus) emitPrimary(e Event) {
	select {
	case b.primary <- e:
		return
	default:
	}

	select {
	case <-b.primary:
		b.dropped++
	default:
		b.dropped++
	}

	select {
	case b.primary <- e:
	default:
		b.dropped++
	}
}

func (b *eventBus) emitAudit(e Event) {
	if b.audit == nil {
		return
	}
	select {
	case b.audit <- e:
	default:
		b.auditDropped++
	}
}

func (b *eventBus) channel() <-chan Event {
	if b == nil {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	return b.primary
}

func (b *eventBus) auditChannel() <-chan Event {
	if b == nil || b.audit == nil {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	return b.audit
}

func (b *eventBus) droppedCount() uint64 {
	if b == nil {
		return 0
	}
	return b.dropped
}

func (b *eventBus) auditDroppedCount() uint64 {
	if b == nil {
		return 0
	}
	return b.auditDropped
}

func (b *eventBus) close() {
	if b == nil {
		return
	}
	close(b.primary)
	if b.audit != nil {
		close(b.audit)
	}
}
