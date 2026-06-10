package steward

import "context"

// BuildFunc constructs a Unit from configuration.
// Must not start goroutines outside Unit.Start.
type BuildFunc[K comparable, C any] func(ctx context.Context, id string, cfg C) (Unit, error)

// EqualFunc compares two configs for reconcile diff.
// Must be deterministic, side-effect free, and symmetric.
type EqualFunc[C any] func(a, b C) bool

func validateSetFuncs[K comparable, C any](build BuildFunc[K, C], equal EqualFunc[C]) {
	if build == nil {
		panic("steward: NewSet requires non-nil build function")
	}
	if equal == nil {
		panic("steward: NewSet requires non-nil equal function")
	}
}
