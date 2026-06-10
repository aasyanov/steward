package steward

import "time"

// UnitView is a read-only snapshot of one unit.
type UnitView struct {
	ID             string
	State          State
	LastTransition time.Time
	Uptime         time.Duration
	RestartCount   int
	LastError      error
	FailureClass   FailureClass
}

func copyUnitViews(units []UnitView) []UnitView {
	out := make([]UnitView, len(units))
	copy(out, units)
	return out
}
