package steward

// Handoff optionally migrates state between old and new units on config update.
//
// Runs in the scheduler goroutine between Build(new) and stopping old.
// Must not block. On error, the old unit keeps running.
type Handoff func(old, new Unit) error
