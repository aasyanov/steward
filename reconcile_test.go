package steward_test

import (
	"testing"

	"github.com/aasyanov/steward"
)

func TestDiff(t *testing.T) {
	old := map[string]int{"a": 1, "b": 2}
	new := map[string]int{"a": 1, "b": 3, "c": 4}

	plan := steward.Diff(old, new, func(a, b int) bool { return a == b })

	if plan.Noops != 1 {
		t.Fatalf("noop = %d, want 1", plan.Noops)
	}
	if len(plan.Updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(plan.Updates))
	}
	if len(plan.Creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(plan.Creates))
	}
	if len(plan.Removes) != 0 {
		t.Fatalf("removes = %d, want 0", len(plan.Removes))
	}
}

func TestDiffRemoveFirst(t *testing.T) {
	old := map[string]int{"a": 1, "b": 2, "c": 3}
	new := map[string]int{"a": 1}

	plan := steward.Diff(old, new, func(a, b int) bool { return a == b })
	if len(plan.Removes) != 2 {
		t.Fatalf("removes = %d, want 2", len(plan.Removes))
	}
	if plan.Noops != 1 {
		t.Fatalf("noops = %d, want 1", plan.Noops)
	}
	if len(plan.Creates) != 0 {
		t.Fatalf("creates = %d, want 0", len(plan.Creates))
	}
}

func TestDiffRemove(t *testing.T) {
	old := map[string]int{"a": 1, "b": 2}
	new := map[string]int{"a": 1}

	plan := steward.Diff(old, new, func(a, b int) bool { return a == b })
	if len(plan.Removes) != 1 {
		t.Fatalf("remove = %d, want 1", len(plan.Removes))
	}
}
