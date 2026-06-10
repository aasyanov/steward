package steward

import (
	"math"
	"time"
)

// Policy controls error classification, timeouts, backoff, and restart decisions.
type Policy interface {
	Classify(err error) FailureClass
	ShouldRestart(state State, failure Failure) bool
	Backoff(unitID string, attempt int) time.Duration
	StartTimeout(unitID string) time.Duration
	DrainTimeout(unitID string) time.Duration
}

// DefaultPolicy implements production restart semantics.
type DefaultPolicy struct {
	Start      time.Duration
	Drain      time.Duration
	MaxBackoff time.Duration
}

// Classify maps errors to failure classes using classifyError.
func (p DefaultPolicy) Classify(err error) FailureClass {
	return classifyError(err)
}

// ShouldRestart returns true for transient failures only.
func (p DefaultPolicy) ShouldRestart(_ State, failure Failure) bool {
	switch failure.Class {
	case FailureTransient:
		return true
	case FailureConfigError, FailureFatal:
		return false
	default:
		return false
	}
}

// Backoff returns exponential backoff capped by MaxBackoff.
func (p DefaultPolicy) Backoff(_ string, attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	sec := math.Min(float64(int(1)<<min(attempt, 10)), float64(p.maxBackoff().Seconds()))
	return time.Duration(sec) * time.Second
}

// StartTimeout returns WaitReady timeout for a unit.
func (p DefaultPolicy) StartTimeout(_ string) time.Duration {
	if p.Start > 0 {
		return p.Start
	}
	return 30 * time.Second
}

// DrainTimeout returns Drain timeout before Stop.
func (p DefaultPolicy) DrainTimeout(_ string) time.Duration {
	if p.Drain > 0 {
		return p.Drain
	}
	return 15 * time.Second
}

func (p DefaultPolicy) maxBackoff() time.Duration {
	if p.MaxBackoff > 0 {
		return p.MaxBackoff
	}
	return 5 * time.Minute
}
