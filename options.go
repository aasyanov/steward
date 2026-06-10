package steward

type config struct {
	policy      Policy
	eventBuffer int
	auditBuffer int
	handoff     Handoff
}

func defaultConfig() config {
	return config{
		policy:      DefaultPolicy{},
		eventBuffer: defaultEventBuffer,
	}
}

// Option configures a Set or Instance.
type Option func(*config)

// WithPolicy sets classification, restart, backoff, and timeouts.
func WithPolicy(p Policy) Option {
	return func(c *config) {
		c.policy = p
	}
}

// WithEventBuffer sets the lifecycle event channel buffer size.
func WithEventBuffer(n int) Option {
	return func(c *config) {
		c.eventBuffer = n
	}
}

// WithAuditBuffer enables an isolated audit event pipeline with the given buffer.
// Audit delivery is best-effort and never blocks reconcile.
func WithAuditBuffer(n int) Option {
	return func(c *config) {
		c.auditBuffer = n
	}
}

// WithHandoff sets optional hot-reload state migration between old and new units.
func WithHandoff(h Handoff) Option {
	return func(c *config) {
		c.handoff = h
	}
}
