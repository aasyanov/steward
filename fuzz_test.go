package steward

import (
	"context"
	"testing"
)

func FuzzDiff(f *testing.F) {
	f.Add("a", "b", 1, 2)
	f.Fuzz(func(t *testing.T, k1, k2 string, v1, v2 int) {
		if k1 == k2 {
			k2 += "_x"
		}
		old := map[string]int{k1: v1}
		new := map[string]int{k2: v2, k1: v1}

		plan := Diff(old, new, func(a, b int) bool { return a == b })

		for _, k := range plan.Removes {
			if _, keep := new[k]; keep {
				t.Fatalf("remove key %q still in new", k)
			}
		}
		for _, e := range plan.Creates {
			if _, ok := old[e.Key]; ok {
				t.Fatalf("create key %q already in old", e.Key)
			}
		}
		for _, e := range plan.Updates {
			if !containsKey(old, e.Key) || !containsKey(new, e.Key) {
				t.Fatalf("update key %q missing from old/new", e.Key)
			}
		}
	})
}

func containsKey(m map[string]int, k string) bool {
	_, ok := m[k]
	return ok
}

func FuzzReconcileSet(f *testing.F) {
	f.Add("a", "b", "c", true, false)
	f.Fuzz(func(t *testing.T, k1, k2, k3 string, include2, include3 bool) {
		build := func(_ context.Context, id string, _ int) (Unit, error) {
			return RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		}
		equal := func(a, b int) bool { return a == b }
		set := &Set[string, int]{
			sched: newScheduler[string, int](build, equal, DefaultPolicy{}, nil, 4, 0),
		}
		set.loop = make(chan struct{})
		go func() {
			set.sched.loop()
			close(set.loop)
		}()
		_ = set.sched.callStart(context.Background())
		set.started = true

		desired := map[string]int{k1: 1}
		if include2 {
			desired[k2] = 2
		}
		if include3 {
			desired[k3] = 3
		}
		_ = set.sched.callReconcile(desired, false, nil, nil)

		desired2 := map[string]int{k1: 10}
		_ = set.sched.callReconcile(desired2, false, nil, nil)

		_ = set.sched.callStop(context.Background())
		close(set.sched.cmdCh)
		<-set.loop
	})
}
