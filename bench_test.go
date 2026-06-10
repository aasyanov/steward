package steward_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aasyanov/steward"
)

func BenchmarkSet_ReconcileUnchanged(b *testing.B) {
	set := steward.NewSet[int, int](
		func(_ context.Context,
		id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },)
	set.Start(context.Background())
	desired := map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
	set.Reconcile(desired)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = set.Reconcile(desired)
	}
	b.StopTimer()
	set.Stop(context.Background())
}

func BenchmarkSet_ReconcileChurn(b *testing.B) {
	set := steward.NewSet[int, int](
		func(_ context.Context,
		id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },)
	set.Start(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = set.Reconcile(map[int]int{
			i % 10:       i,
			(i + 1) % 10: i + 1,
			(i + 3) % 10: i + 3,
		})
	}
	b.StopTimer()
	set.Stop(context.Background())
}

func BenchmarkInstance_Reload(b *testing.B) {
	build := func(_ context.Context, id string, _ int) (steward.Unit, error) {
		return steward.RunUnit(id, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}), nil
	}
	equal := func(a, b int) bool { return a == b }
	inst := steward.NewInstance(0, build, equal)
	inst.Start(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = inst.Reload(i)
	}
	b.StopTimer()
	inst.Stop(context.Background())
}

func BenchmarkSet_Reconcile1k(b *testing.B) {
	benchmarkReconcileN(b, 1000)
}

func BenchmarkSet_Reconcile10k(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10k in -short")
	}
	benchmarkReconcileN(b, 10_000)
}

func benchmarkReconcileN(b *testing.B, n int) {
	set := steward.NewSet[string, int](
		func(_ context.Context, id string, _ int) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		},
		func(a, b int) bool { return a == b },
	)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	desired := make(map[string]int, n)
	for i := 0; i < n; i++ {
		desired[fmt.Sprintf("k-%d", i)] = i
	}
	set.Reconcile(desired)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = set.Reconcile(desired)
	}
}
