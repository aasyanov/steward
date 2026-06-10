// Package steward implements a deterministic in-process lifecycle
// control plane for long-lived Go components.
//
// steward is an L2 primitive: it manages Unit lifecycle and keyed reconcile
// for homogeneous sets (Set[K,C]) and singletons (Instance[C]). Pass a
// BuildFunc and EqualFunc to NewSet; dependency graphs and composition belong
// in the application (L1). steward provides Start, Reconcile, Stop, Policy,
// Ready, and Drain semantics only.
//
// Specification and architectural positioning: see README.md.
package steward
