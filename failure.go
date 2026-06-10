package steward

import (
	"context"
	"errors"
)

// FailureClass categorizes unit failures for policy decisions.
type FailureClass int

const (
	// FailureTransient is retried with backoff (network blips, canceled ctx, dependency outages).
	FailureTransient FailureClass = iota
	// FailureConfigError is permanent misconfiguration — no restart.
	FailureConfigError
	// FailureFatal is permanent runtime failure — no restart.
	FailureFatal
)

func (c FailureClass) String() string {
	switch c {
	case FailureTransient:
		return "transient"
	case FailureConfigError:
		return "config_error"
	case FailureFatal:
		fallthrough
	default:
		return "fatal"
	}
}

// Failure is a classified unit failure.
type Failure struct {
	Class FailureClass
	Err   error
}

type classifiedError struct {
	class FailureClass
	err   error
}

func (e classifiedError) Error() string { return e.err.Error() }
func (e classifiedError) Unwrap() error   { return e.err }

// ClassifyError wraps err with an explicit failure class for Policy.Classify.
func ClassifyError(class FailureClass, err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{class: class, err: err}
}

func classifyError(err error) FailureClass {
	if err == nil {
		return FailureFatal
	}
	var f classifiedError
	if errors.As(err, &f) {
		return f.class
	}
	if errors.Is(err, context.Canceled) {
		return FailureTransient
	}
	return FailureFatal
}
