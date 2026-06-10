# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] — 2026-06-10

### Added

- **`LICENSE`** (MIT) — enables pkg.go.dev to display package documentation.

### Fixed

- **README badges** — CI, Go Reference, and License use shield images (consistent with slabix/uewal).

## [0.1.0] — 2026-06-10

Initial public release — deterministic in-process lifecycle control plane for long-lived Go components.

### Added

- **`Set[K, C]`** — keyed reconcile: desired `map[K]C` converges to actual units via ordered diff (remove → update → create).
- **`Instance[C]`** — singleton wrapper over `Set[string, C]` with fixed key `"default"`.
- **`Unit` contract** — `Start` / `Stop` with optional `Ready` (`WaitReady`) and `Drainer` (`Drain`) interfaces.
- **`BuildFunc` / `EqualFunc`** — explicit build and config equality; `NewSet` panics on nil funcs.
- **`Replace(build, equal, desired)`** — atomically swaps build/equal and recreates all units (shared dependency hot-swap).
- **`Policy`** — `Classify`, `ShouldRestart`, `Backoff`, `StartTimeout`, `DrainTimeout`; `DefaultPolicy` with exponential backoff.
- **Three `FailureClass` values** — `FailureTransient`, `FailureConfigError`, `FailureFatal`; `ClassifyError` for explicit classification.
- **`Handoff`** — optional state migration on config update (`WithHandoff`); runs in scheduler between `Build(new)` and stopping old.
- **Serialized scheduler** — single goroutine owns all lifecycle state; API methods enqueue commands on `cmdCh`.
- **Supervisor per unit** — isolated goroutine for `Start` / `WaitReady` / `Drain` / `Stop`; policy-driven restart with backoff.
- **Events** — bounded primary channel (`WithEventBuffer`), optional isolated audit pipeline (`WithAuditBuffer`), drop accounting.
- **`Snapshot() []UnitView`** — shallow per-unit observability (state, uptime, restart count, last error, failure class).
- **`RunUnit`** — adapter for simple blocking `func(context.Context) error` loops.
- **Pure diff** — `Diff` / `DiffPlan` for O(n) reconcile planning without side effects.
- **Sentinel errors** — `ErrNotStarted`, `ErrAlreadyStarted`, `ErrStopped`.
- **`a-report-pprof.ps1`** — automated benchmark profiling script producing per-benchmark CPU/memory profiles and a consolidated markdown report for AI-assisted optimization analysis. See `a-report-howto.md`.
- **`a-report-howto.md`** — documentation for the profiling pipeline: calibration, adaptive benchtime, output format.
- Comprehensive test suite: 80 tests, 2 fuzz targets, 5 benchmarks, 4 soak tests, scenario and conformance coverage.
- GoDoc for all exported symbols; architectural specification in `README.md`.
- Zero external dependencies (stdlib only).

### Performance

- **`formatKey` for `string` keys** — returns the key directly instead of `fmt.Sprintf`, eliminating per-event allocations on string-keyed sets (camera pools, NVR fleets).
- **`errRespPool`** — `sync.Pool` of buffered `chan error` for `Start` / `Reconcile` / `Stop` API calls; no-op reconcile drops from 3 to **1** alloc/op (48 B/op vs 176 B/op).

### CI

- GitHub Actions pipeline on `main` — **verified green** before release: lint, test, coverage, fuzz, benchmarks, profile.
- **Lint** — `golangci-lint` v2.12+ via `.golangci.yml` (errcheck, govet, staticcheck, revive, gocritic, goconst, …).
- **Test** — matrix Go 1.24–1.26 × Linux/Windows, `-race`, 90% coverage gate.
- **Fuzz** — `FuzzDiff`, `FuzzReconcileSet`.
- **Benchmarks** — artifacts on each run.
- **Profile** — `a-report-pprof.ps1` on Windows (push to `main`); uploads `a-report-*.md` and `profiles/` (90-day retention).

### Dependencies

- Go 1.24+

[Unreleased]: https://github.com/aasyanov/steward/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/aasyanov/steward/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aasyanov/steward/releases/tag/v0.1.0
