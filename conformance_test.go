package steward_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

// Conformance tests verify behaviors required by the L2 specification in README.md.

func TestConformance_DrainBeforeStop(t *testing.T) {
	var order []string
	u := &orderUnit{id: "u", order: &order}

	set := steward.NewSet[string, struct{}](
		func(_ context.Context, id string, _ struct{}) (steward.Unit, error) { return u, nil },
		func(a, b struct{}) bool { return true },
	)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]struct{}{"k": {}})
	waitUntil(t, time.Second, func() bool { return set.Running("k") })

	set.Reconcile(map[string]struct{}{})
	waitUntil(t, time.Second, func() bool { return !set.Running("k") })

	if len(order) < 2 || order[0] != "drain" || order[len(order)-1] != "stop" {
		t.Fatalf("order = %v, want drain before stop", order)
	}
}

type orderUnit struct {
	id    string
	order *[]string
}

func (u *orderUnit) ID() string { return u.id }
func (u *orderUnit) Start(ctx context.Context) error {
	go func() { <-ctx.Done() }()
	return nil
}
func (u *orderUnit) Drain(context.Context) error {
	*u.order = append(*u.order, "drain")
	return nil
}
func (u *orderUnit) Stop(context.Context) error {
	*u.order = append(*u.order, "stop")
	return nil
}

func TestConformance_WaitReadyRequired(t *testing.T) {
	u := &readyGateUnit{id: "u", ready: make(chan struct{})}
	set := steward.NewSet[string, struct{}](
		func(_ context.Context, id string, _ struct{}) (steward.Unit, error) { return u, nil },
		func(a, b struct{}) bool { return true },
		steward.WithPolicy(steward.DefaultPolicy{
			Start: 200 * time.Millisecond,
		}),
	)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]struct{}{"k": {}})
	time.Sleep(50 * time.Millisecond)
	if set.Running("k") {
		t.Fatal("unit should not be running before ready")
	}
	close(u.ready)
	waitUntil(t, time.Second, func() bool { return set.Running("k") })
}

type readyGateUnit struct {
	id    string
	ready chan struct{}
}

func (u *readyGateUnit) ID() string { return u.id }
func (u *readyGateUnit) Start(ctx context.Context) error {
	go func() { <-ctx.Done() }()
	return nil
}
func (u *readyGateUnit) WaitReady(ctx context.Context) error {
	select {
	case <-u.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (u *readyGateUnit) Stop(context.Context) error { return nil }

func TestConformance_ConfigErrorNoRestart(t *testing.T) {
	var attempts atomic.Int32
	set := steward.NewSet[string, struct{}](
		func(_ context.Context,
		id string, _ struct{}) (steward.Unit, error) {
			return steward.RunUnit(id, func(context.Context) error {
				attempts.Add(1)
				return steward.ClassifyError(steward.FailureConfigError, errors.New("bad config"))
			}), nil
		},
		func(a, b struct{}) bool { return true }, steward.WithPolicy(steward.DefaultPolicy{}))
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]struct{}{"k": {}})
	time.Sleep(1500 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (no restart for config error)", attempts.Load())
	}
	if !set.Failed("k") {
		t.Fatal("expected failed state")
	}
}
