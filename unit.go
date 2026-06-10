package steward

import "context"

// Unit is a long-lived runtime component governed by steward.
//
// Start MUST return quickly without blocking on I/O. Use Ready.WaitReady
// for blocking initialization. Build MUST NOT start goroutines.
type Unit interface {
	ID() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Drainer optionally quiesces a unit before termination.
// When implemented, the runtime calls Drain before Stop.
type Drainer interface {
	Drain(ctx context.Context) error
}

// Ready optionally blocks until a unit is ready to serve work.
// Called once after Start returns successfully.
type Ready interface {
	WaitReady(ctx context.Context) error
}
