package steward

// DiffAction describes a reconcile change for one key.
type DiffAction int

// Diff plan action constants.
const (
	DiffNoop DiffAction = iota
	DiffCreate
	DiffUpdate
	DiffRemove
)

// DiffEntry is one keyed reconcile operation.
type DiffEntry[K comparable, C any] struct {
	Key    K
	Action DiffAction
	Config C
}

// DiffPlan is an ordered reconcile plan: removes, then updates/creates.
type DiffPlan[K comparable, C any] struct {
	Removes []K
	Updates []DiffEntry[K, C]
	Creates []DiffEntry[K, C]
	Noops   int
}

// Diff computes an ordered reconcile plan from old and new desired maps.
// Order: removes → updates → creates. Pure logic, O(n).
func Diff[K comparable, C any](
	old map[K]C,
	new map[K]C,
	equal func(a, b C) bool,
) DiffPlan[K, C] {
	var plan DiffPlan[K, C]

	for k := range old {
		if _, keep := new[k]; !keep {
			plan.Removes = append(plan.Removes, k)
		}
	}

	for k, cfg := range new {
		oldCfg, ok := old[k]
		if !ok {
			plan.Creates = append(plan.Creates, DiffEntry[K, C]{
				Key: k, Action: DiffCreate, Config: cfg,
			})
			continue
		}
		if equal != nil && equal(oldCfg, cfg) {
			plan.Noops++
			continue
		}
		plan.Updates = append(plan.Updates, DiffEntry[K, C]{
			Key: k, Action: DiffUpdate, Config: cfg,
		})
	}

	return plan
}
