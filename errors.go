package steward

import "errors"

var (
	// ErrNotStarted is returned when an operation requires Start first.
	ErrNotStarted = errors.New("steward: not started")
	// ErrAlreadyStarted is returned on a second Start call.
	ErrAlreadyStarted = errors.New("steward: already started")
	// ErrStopped is returned after the Set or Instance has shut down.
	ErrStopped = errors.New("steward: stopped")
)
