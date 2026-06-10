package steward_test

import (
	"context"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

func TestAuditEventsIsolated(t *testing.T) {
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b },
		steward.WithEventBuffer(1),
		steward.WithAuditBuffer(64),
	)
	set.Start(context.Background())

	// Do not drain primary — force DropOldest on primary path.
	set.Reconcile(map[string]cfg{
		"a": {Name: "a"},
		"b": {Name: "b"},
		"c": {Name: "c"},
	})

	var auditCount int
	deadline := time.After(500 * time.Millisecond)
auditLoop:
	for {
		select {
		case _, ok := <-set.AuditEvents():
			if !ok {
				break auditLoop
			}
			auditCount++
		case <-deadline:
			break auditLoop
		}
	}

	if auditCount < 3 {
		t.Fatalf("audit events = %d, want >= 3", auditCount)
	}
	if set.DroppedAuditEvents() != 0 {
		t.Fatalf("audit dropped = %d, want 0 with large audit buffer", set.DroppedAuditEvents())
	}

	set.Stop(context.Background())
}

func TestDroppedEventsAccounting(t *testing.T) {
	set := steward.NewSet[string, cfg](
		func(_ context.Context,
		id string, _ cfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b cfg) bool { return a == b }, steward.WithEventBuffer(1))
	set.Start(context.Background())

	// Flood without consuming — primary must account drops.
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		_ = set.Reconcile(map[string]cfg{key: {Name: key}})
	}

	if set.DroppedEvents() == 0 {
		t.Fatal("expected DroppedEvents > 0 with buffer=1 and no consumer")
	}

	set.Stop(context.Background())
}
