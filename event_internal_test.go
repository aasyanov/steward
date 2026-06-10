package steward

import (
	"testing"
	"time"
)

func TestEventBusNilSafe(t *testing.T) {
	var b *eventBus
	b.emit(Event{UnitID: "x"})
	if b.droppedCount() != 0 {
		t.Fatal()
	}
	ch := b.channel()
	if ch == nil {
		t.Fatal("expected closed channel")
	}
	_, ok := <-ch
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestEventBusAuditNil(t *testing.T) {
	b := newEventBus(4, 0)
	ch := b.auditChannel()
	_, ok := <-ch
	if ok {
		t.Fatal("expected closed audit channel when audit disabled")
	}
}

func TestDefaultPolicyBackoff(t *testing.T) {
	p := DefaultPolicy{MaxBackoff: time.Second}
	if d := p.Backoff("u", 0); d != time.Second {
		t.Fatalf("backoff(0) = %v", d)
	}
	if d := p.DrainTimeout("u"); d != 15*time.Second {
		t.Fatalf("drain = %v", d)
	}
}
