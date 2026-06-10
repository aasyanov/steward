# steward — L2 Lifecycle Control Plane for Industrial Stack

[CI](https://github.com/aasyanov/steward/actions/workflows/ci.yml)
[Go Reference](https://pkg.go.dev/github.com/aasyanov/steward)
[License: MIT](https://opensource.org/licenses/MIT)

Deterministic in-process lifecycle control plane for long-lived Go components. Go 1.24+. Zero external dependencies.

```
go get github.com/aasyanov/steward
```

> [!IMPORTANT]
> **Core primitive:** `desired map[key]config → Reconcile → actual units` — a lifecycle control plane for **homogeneous sets** (cameras, PLCs, consumers) and **singleton services** (DB, HTTP) via `Instance`. This is **not** a DI container, **not** a dependency graph engine, **not** a job scheduler, and **not** a distributed orchestrator. `Stop()` **blocks** until every `Unit` has finished — the primary signal that work has actually ended.

> **Naming:** `steward` is the Go module (L2 lifecycle + reconcile). A full process runtime (L1 composition of Logger, DB, HTTP, …) is assembled **outside** this package via DI and multiple `Instance` / `Set` / `Unit` values.

## The Problem

Production services in an industrial stack (SCADA, NVR, IoT hubs, multi-tenant SaaS) contain **dozens of long-lived components** that:

- start at process boot and run until shutdown
- change on config hot-reload (cameras, PLCs, subscriptions)
- fail and must recover according to policy
- require readiness probes before accepting work
- must shut down correctly (drain → stop)

Without a control plane, every team rewrites fragile code:

```go
mu sync.Mutex
workers map[string]*Worker
// start, stop, reload, policy restart, readiness, drain...
// don't leak goroutines, don't deadlock on reconcile/stop
```

`steward` addresses **seven production requirements**:

1. **Declarative reconcile** — `map[key]config` → actual units (ordered diff: remove → update → create)
2. **Hot-reload** — add/remove/change without process restart
3. **Policy-driven recovery** — auto-restart with backoff by `FailureClass`
4. **Explicit readiness** — `WaitReady(ctx)`, not polling `Ready() bool`
5. **Graceful shutdown** — `Drain → Stop` when `Unit` implements `Drainer`
6. **Serialized scheduler** — one goroutine owns all state (no mutex on the unit map)
7. **Zero deps** — flat Go module, stdlib only

## Architectural Position: What `steward` Actually Does

Before using the package, understand the **managed object** and **abstraction level**. Without this, it is easy to expect from `steward` what it deliberately does not do.

### Managed Object

`steward` manages the **lifecycle** of components that implement `Unit`. The reconcile object is a **homogeneous set** or a **single singleton**:

```text
✅ steward.Set[string, CameraConfig]     → Camera, Camera, Camera, ...  (one type C)
✅ steward.Instance[DBConfig]            → one DB pool
✅ CameraManager as Unit                 → Set of 1000 cameras inside

❌ Logger + DB + Cache + HTTP            → heterogeneous graph in one Set
❌ Requires() / TopologicalSort          → dependency DAG
❌ Replace("logger") → cascade DB        → automatic graph rebuild
```

`**Set[K,C]**` requires one config type `C` for all keys. This is a reconcile engine for **homogeneous sets** — controller-runtime inside a single process.

`**Instance[C]`** is the same kernel for **one** instance (Logger, DB, HTTP server).

A heterogeneous process graph (Logger → DB → HTTP) is **not built with one `Set`**. The **composition layer** (your `main`, fx, wire) assembles it from several `Instance[T]` values and custom `Unit` implementations.

### Three-Layer Model of the Industrial Stack

```text
┌─────────────────────────────────────────────────────────────┐
│  L1  Composition + DI                                       │
│      Graph wiring, startup order, Ref/handles,              │
│      decision "changed DB — recreate Cache?"                │
│      (fx / wire / dig / manual main)                        │
└────────────────────────────┬────────────────────────────────┘
                             │ already wired Units
┌────────────────────────────▼────────────────────────────────┐
│  L2  steward — lifecycle control plane                      │
│      Set[K,C]   — homogeneous sets                          │
│      Instance[C] — singleton services                       │
│      Start / Reconcile / Stop / Policy / Events             │
└────────────────────────────┬────────────────────────────────┘
                             │ Build → Unit
┌────────────────────────────▼────────────────────────────────┐
│  L3  Business components                                    │
│      Camera, Poller, Consumer, Recorder, ModbusClient...    │
└─────────────────────────────────────────────────────────────┘
```

**L1** knows *who depends on whom* and *when to recreate*.  
**L2 (`steward`)** knows only *how to start, stop, restart, and reconcile* a `Unit` you pass in.  
**L3** is your domain logic.

Unix analogy: the kernel does not know that nginx depends on postgres. The kernel knows `fork`, `exec`, `wait`, `kill`. Dependencies are resolved by processes themselves — via locks, retry, `WaitReady`.

### Two Usage Modes


| Mode                | API                   | When                                    | Example                                                         |
| ------------------- | --------------------- | --------------------------------------- | --------------------------------------------------------------- |
| **Homogeneous set** | `Set[K,C]`            | N same-type entities, hot-reload by key | `map[cameraID]CameraConfig`, `map[tenantID]WorkerConfig`        |
| **Singleton**       | `Instance[C]`         | One long-lived service                  | DB pool, HTTP server, metrics exporter                          |
| **Set inside Unit** | custom `Unit` + `Set` | Set manager as part of the process      | `CameraManager` implements `Unit`, embeds `Set` for 10k cameras |


Typical industrial service:

```text
Process
├── Instance[LoggerConfig]        ← steward
├── Instance[DBConfig]            ← steward
├── Instance[HTTPConfig]          ← steward
└── CameraManager (Unit)          ← steward.Instance or custom Unit
        └── Set[string, CameraConfig]
                └── cam-1 … cam-N
```

`steward` **does not disappear** at the top level — it remains a building block for both singletons and sets. Without `Set`, you cannot efficiently hold 10,000 cameras in one process.

### What `steward` Does at Runtime (Precisely)

For each registered `Unit`, the scheduler runs a **deterministic pipeline**:

```text
Build(cfg)           ← BuildFunc, no goroutines
    ↓
Start(ctx)           ← async launch, return quickly
    ↓
WaitReady(ctx)?      ← if Unit implements Ready
    ↓
Running              ← supervisor reports ready
    ↓
[work until cancel or failure]
    ↓
Drain(ctx)?          ← if Unit implements Drainer
    ↓
Stop(ctx)
    ↓
Stopped / Failed
```

**Reconcile** compares `desired map[key]C` to actual via `Equal` and applies an ordered diff:

```text
1. removes   — cancel supervisor → Drain? → Stop → delete slot
2. updates   — Build(new) → Handoff? → stop old → start new
3. creates   — Build → attach supervisor
```

**Policy** handles runtime failures **without changing desired**: transient error → backoff → recreate unit with the same config.

**Scheduler** — one goroutine; all mutations to `map[key]*unitSlot` are serialized. API goroutines send commands on `cmdCh`; they never touch state directly.

> **Serialized command queue:** while `Reconcile` runs, commands `Stop`, a new `Reconcile`, `Snapshot`, `Running` **wait in the queue** on `cmdCh`. `Stop` **does not interrupt** the current reconcile — it runs after the active command completes. Heavy work in `Equal`, `Build`, or `Handoff` blocks the **entire** scheduler; keep them O(1) or move I/O out of the hot path.

> **Two goroutine layers:** scheduler (one per Set/Instance) and supervisor (one per Unit). `Start` / `WaitReady` / `Stop` run in the supervisor; `Reconcile` / `Build` / `Equal` / `Handoff` run in the scheduler.

### Dependencies: DI Builds, `steward` Runs

`steward` **does not** decide startup order Logger → DB → HTTP. The composition layer does:

```go
logger := NewLogger(cfg)
db     := NewDB(logger)      // DI: db holds logger
cache  := NewCache(db)
http   := NewHTTP(cache)

// steward manages lifecycle of already wired Units:
loggerInst := steward.NewInstance(loggerCfg, loggerCtrl)
dbInst     := steward.NewInstance(dbCfg, dbCtrl)
// ...
loggerInst.Start(ctx)
dbInst.Start(ctx)   // Start order is the application's responsibility
```

**Readiness synchronization** — via `WaitReady`, not via a DAG:

```go
// DB waits in a ping loop inside WaitReady
func (d *DB) WaitReady(ctx context.Context) error {
    return pingUntil(ctx, d.conn)
}

// HTTP waits until DB is reachable — logic inside HTTP, not steward
func (h *HTTP) WaitReady(ctx context.Context) error {
    return h.db.WaitUntilConnected(ctx)
}
```

Dependencies live **inside the component** or **in DI at build time**. The runtime sees only `Start → WaitReady → Running`.

### Stable vs Stateful: Changing Dependencies Without Mini-Kubernetes

A dangerous trap: "I changed the logger — what happens to DB that holds it?"

`steward` **does not** compute cascade restart. Three sensible approaches live **outside core**:


| Approach                        | Idea                                                                     | When                        |
| ------------------------------- | ------------------------------------------------------------------------ | --------------------------- |
| **Immutable Unit**              | Unit does not change after creation; change = `Reload` / `Reconcile`     | Simple cases, stateful deps |
| **Stable interface + hot impl** | DB holds a `Logger` interface; impl is thread-safe or atomically swapped | Logger, metrics, tracer     |
| **Ref/handle (composition)**    | DB gets `Ref[Logger]`; composition changes target without restarting DB  | Hot-swap stateless deps     |


**Stable services** (change without restarting consumers):

```text
Logger, Metrics, Tracer, ConfigProvider, SecretsProvider, FeatureFlags
```

**Stateful services** (change implies Unit recreation and possibly consumers — an L1 decision):

```text
DB connection pool, Cache, Kafka/NATS client, Redis, Filesystem mount
```

Ref pattern example (implemented **in the application**, not in `steward`):

```go
type Ref[T any] interface { Get() T }

type DB struct {
    loggerRef Ref[Logger]
}

func (d *DB) Query(...) {
    d.loggerRef.Get().Info("query")  // after logger Replace — new impl
}
```

`steward.Reconcile` / `Instance.Reload` recreates **that specific Unit** when its config or controller changes. Whether consumers must be recreated is an **explicit composition-layer decision**, not a hidden DAG in the runtime.

### Package Boundaries: What Is Complete, What Is Not


| Area                                     | Status in steward                                |
| ---------------------------------------- | ------------------------------------------------ |
| Unit lifecycle (Start/Stop)              | ✅                                                |
| Reconcile of homogeneous sets (`Set`)    | ✅                                                |
| Singleton lifecycle (`Instance`)         | ✅                                                |
| Policy restart, backoff, failure classes | ✅                                                |
| WaitReady, Drain, Handoff                | ✅                                                |
| Events, Snapshot, audit pipeline         | ✅                                                |
| Serialized scheduler, race-free          | ✅                                                |
| Dependency graph / `Requires()`          | ❌ deliberately out of scope                      |
| Cascade restart on dep change            | ❌ out of scope                                   |
| `Ref[T]` service registry                | ❌ out of scope (L1/extension)                    |
| Heterogeneous `Runtime.Add(Unit)`        | ❌ out of scope (multiple Instance + composition) |


**steward is a complete L2 primitive.** Reference implementation for an industrial reconcile/lifecycle kernel. It is a building block for a full process runtime (L1), not the entire runtime.

### What Not to Expect (and Why That Is Correct)

```text
❌ runtime.Replace("logger") automatically restarts DB and HTTP
   → that is mini-Kubernetes; a year later: Requires, TopologicalSort, CycleDetection

❌ steward knows HTTP depends on DB
   → a DAG in core kills simplicity and predictability

❌ steward.Reconcile(map[string]any{...}) for Logger+DB+HTTP
   → different types = different Instance values or a custom composition Unit

✅ steward.Reconcile(map[string]CameraConfig{...})
   → exactly what Set was designed for

✅ DI built the graph → steward holds lifecycle → WaitReady synchronizes
   → Unix model: processes resolve dependencies themselves
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Application                              │
│  config watcher / API / operator / DI composition            │
│       │                                                      │
│       ▼                                                      │
│  desired map[key]config ──▶ Reconcile() ──▶ actual units    │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     Set[K, C]  (public API)                  │
│  Start / Reconcile / Replace / Stop / Snapshot           │
└──────────────────────────────┬───────────────────────────────┘
                               │  commands (serialized)
┌──────────────────────────────▼───────────────────────────────┐
│                   scheduler goroutine                        │
│  handleReconcile │ handleStop │ handleUnitFailed │ restarts  │
└──────────────────────────────┬───────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌─────────────────────────┐       ┌─────────────────────────┐
│  unitSlot per key       │       │  Instance[C]            │
│  launchSupervisor       │       │  (singleton Set)        │
│  cancel → wait → stop   │       │                         │
└─────────────────────────┘       └─────────────────────────┘
```

**Layer:** infrastructure control plane (steward reference implementation).

**Dependencies:** zero external. Stdlib only.

**Consumers:** SCADA gateways, NVR/recorder pools, IoT hubs, streaming pipelines, multi-tenant workers — **10+ services** in the industrial stack.

**File graph (flat module):**

```
unit.go, state.go, failure.go, policy.go   — steward contracts
controller.go, reconcile.go                — build + pure diff
engine.go                                  — supervisor lifecycle
scheduler.go                               — command loop (ownership)
set.go, instance.go                        — public API
event.go, snapshot.go, options.go          — observability + config
rununit.go, handoff.go                     — adapters + hot-reload handoff
```

No cyclic dependencies. Normative invariants: [Normative Contracts](#normative-contracts) below.

## How It Works

### Unit Contract

```go
type Unit interface {
    ID() string
    Start(ctx context.Context) error  // MUST return quickly; see rules below
    Stop(ctx context.Context) error
}
```

- `Build` **does not** start goroutines — only `Start` does
- `Start` **MUST NOT block** on I/O, `net.Listen`, connect, or sleep. Blocking init belongs in `WaitReady` (if `Ready` is implemented) or in a goroutine spawned from `Start`
- A blocking `Start` **does not stop the scheduler** (other keys in the `Set` keep running), but **freezes that Unit's supervisor**: `WaitReady` is never called, the Unit never becomes `Running`
- `Stop` tears down the unit (via supervisor: Drain → Stop)

### Readiness (`steward` — No Polling)

```go
type Ready interface {
    WaitReady(ctx context.Context) error
}
```

Called **once** after `Start`. Timeout via `Policy.StartTimeout`. Error → `StateFailed`.

### Graceful Shutdown

```go
type Drainer interface {
    Drain(ctx context.Context) error
}
```

Order: **Drain → Stop**. Timeout via `Policy.DrainTimeout`.

### Scheduler — Serialized Command Loop

All `Reconcile`, `Stop`, `Snapshot`, `Running`, and failure notifications go through **one** `cmdCh`:

```
API goroutine ──▶ cmdCh ──▶ scheduler loop ──▶ unitSlot mutations
                              │
                              ├── Reconcile  (may take seconds)
                              ├── Stop       (waits in queue)
                              └── Snapshot   (waits in queue)
```

No mutex on `map[key]*unitSlot`. No concurrent reconcile — a second `Reconcile` waits for the first to finish.

**Callbacks that run in the scheduler goroutine (must not block):**


| Callback                  | When                       | Risk if blocked            |
| ------------------------- | -------------------------- | -------------------------- |
| `EqualFunc`               | diff at start of reconcile | entire Set/Instance frozen |
| `BuildFunc`               | create/update              | same                       |
| `Handoff`                 | update with state transfer | same                       |


The supervisor goroutine (per Unit) calls `Unit.Start`, `WaitReady`, `Drain`, `Stop` — blocking there **does not** block the scheduler, only that Unit.

### Reconcile — Ordered Diff

```
1. removes   — Drain → Stop → delete
2. updates   — Build(new) → [Handoff] → Stop(old) → Start(new)
3. creates   — Build → Start
```

Pure diff: `Diff(old, new, equal) → DiffPlan` — O(n), no side effects. The diff itself is pure; **apply** (Build, stop, Handoff) runs synchronously in the scheduler.

### Handoff

```go
type Handoff func(old, new Unit) error
```

`Handoff` is called **in the scheduler goroutine** during update (between `Build(new)` and stopping old). **MUST NOT block** — otherwise the entire Set blocks. On error, the old unit keeps running and reconcile returns an error.

Configure via `WithHandoff(h)`.

### Policy — Classification and Failure Recovery

```go
type Policy interface {
    Classify(err error) FailureClass
    ShouldRestart(state State, failure Failure) bool
    Backoff(unitID string, attempt int) time.Duration
    StartTimeout(unitID string) time.Duration
    DrainTimeout(unitID string) time.Duration
}
```

Three failure classes:


| FailureClass | Restart                  |
| ------------ | ------------------------ |
| Transient    | yes, exponential backoff |
| ConfigError  | no                       |
| Fatal        | no                       |


**Error classification** — `Policy.Classify` runs in the supervisor when a Unit errors. `DefaultPolicy.Classify` unwraps `ClassifyError`, maps `context.Canceled` → `Transient`, everything else unclassified → `Fatal`.

```go
// Explicit class for policy (e.g. dependency outage → transient retry):
return steward.ClassifyError(steward.FailureTransient, fmt.Errorf("postgres: %w", err))

// Config mistakes → no restart:
return steward.ClassifyError(steward.FailureConfigError, fmt.Errorf("invalid bitrate %d", cfg.Bitrate))
```

Custom policies can override `Classify` for domain-specific routing before `ShouldRestart`.

### Events — Primary + Audit

- **Primary:** bounded channel, `DropOldest`, `DroppedEvents()` counter
- **Audit** (optional): isolated pipeline via `WithAuditBuffer(n)`, `AuditEvents()`
- Runtime **never** blocks on a slow consumer

### Snapshot — Shallow UnitView

`Snapshot()` → `[]UnitView` (state, uptime, restart count, last error, failure class). No deep copy of configs.

---

## Normative Contracts

Conformance tests in `conformance_test.go` enforce this contract. Narrative explanations are in [How It Works](#how-it-works) and [Architectural Position](#architectural-position-what-steward-actually-does) — this section collects **invariants only** (no duplicate API listings).

### Reconcile mental model

| You declare | `steward` does |
|-------------|----------------|
| `map[key]config` — desired state | Creates missing units |
| | Stops removed ones (Drain → Stop) |
| | Restarts changed ones |
| | Leaves unchanged ones alone |
| | Restarts failed ones per Policy |

### Idempotency

- `Reconcile(desired)` with unchanged desired map is a no-op (`Equal` determines identity).
- `Stop` is idempotent: repeated calls after shutdown return nil.
- `Start` is exactly-once: second call returns `ErrAlreadyStarted`.
- `Reconcile` / `Replace` after `Stop` returns `ErrStopped`.
- Policy restart is bounded: same key is never restarted concurrently.
- `Reconcile` MUST NOT overlap itself — commands serialize on `cmdCh`.

### Performance

- Reconcile diff: O(n); Stop/Start per unit: O(1); Snapshot: O(n) shallow; event emission: O(1).
- No reflection in hot path; no blocking I/O in reconcile loop.
- `Equal`, `Build`, `Handoff` run in scheduler goroutine — MUST NOT block.

### Panic policy

Panic in any user callback crashes the process. No recovery. Applies to: `Unit.Start` / `Stop` / `Drain` / `WaitReady`, `Build` / `Equal`, all `Policy` methods, `Handoff`.

### Failure classification

Every failure MUST be classified. Unclassified errors → `Fatal`. Use `ClassifyError` for explicit `FailureClass`.

### Events

- Events MUST NOT block scheduler execution.
- Primary channel drops MUST be accounted (`DroppedEvents()`).
- Optional audit pipeline is isolated — audit failure MUST NOT impact reconcile.

### Dependency model

No runtime DAG in core. Changing Unit A MUST NOT automatically restart Unit B unless L1 explicitly triggers it.

### Scope

| In scope (complete) | Out of scope (by design) |
|---------------------|--------------------------|
| Unit lifecycle; `Set` / `Instance` reconcile; Policy; Ready / Drain; Handoff; Events; Snapshot | Dependency graph; cascade restart; `Ref[T]` registry; heterogeneous single-Set |

`steward` is a **complete L2 primitive** — building block for process runtime, not the entire L1 composition layer.

---

## Quick Start

```go
package main

import (
    "context"
    "github.com/aasyanov/steward"
)

type PLCConfig struct {
    IP       string
    Protocol string
}

func main() {
    set := steward.NewSet[string, PLCConfig](
        func(_ context.Context,
        id string, cfg PLCConfig) (steward.Unit, error) {
            return steward.RunUnit(id, func(ctx context.Context) error {
                // poll until ctx.Done()
                <-ctx.Done()
                return nil
            }), nil
        },
        func(a, b PLCConfig) bool { return a == b },
        steward.WithPolicy(steward.DefaultPolicy{}),
    )
    set.Start(context.Background())

    set.Reconcile(map[string]PLCConfig{
        "plc-a": {IP: "192.168.10.10", Protocol: "modbus"},
        "plc-b": {IP: "192.168.10.11", Protocol: "snmp"},
    })

    set.Stop(context.Background()) // blocks until all units exit
}
```

## Usage Scenarios

### Config File Watcher (Hot-Reload)

Watcher reads JSON/YAML, pushes desired state; `Set.Reconcile` converges actual:

```go
for cfg := range configCh {
    if err := set.Reconcile(cfg.Controllers); err != nil {
        log.Printf("reconcile: %v", err)
    }
}
```

### Camera Recorder Pool (NVR)

```go
set.Reconcile(map[string]CameraConfig{
    "cam-1": {URL: "rtsp://10.0.0.1/stream1"},
    "cam-2": {URL: "rtsp://10.0.0.2/stream1"},
})
// cam-3 added, cam-2 URL changed → cam-2 restarted, cam-1 untouched
```

### Changing Global Dependencies (DB URL)

```go
newCtrl := buildController(globalCfg) // closure captures new DB pool
set.Replace(newBuild, newEqual, currentDesired)
```

### Handoff (State Migration)

```go
set := steward.NewSet(build, equal, steward.WithHandoff(func(old, new steward.Unit) error {
    return copyBuffers(old, new)
}))
// on config change: Build(new) → Handoff(old,new) → Stop → Start
// Handoff error → old unit keeps running
```

### Singleton (`Instance`)

`Instance[C]` — thin wrapper: internally `Set[string, C]` with fixed key `"default"`. All methods (`Start`, `Reload`, `Stop`, `Snapshot`, …) delegate to `Set`.

```go
inst := steward.NewInstance(cfg, build, equal)
inst.Start(ctx)              // Set.Start + Reconcile({"default": cfg})
inst.Reload(newCfg)          // Reconcile with updated config
inst.Replace(newBuild, newEqual, newCfg)
inst.Stop(ctx)
```

### Set as Unit (Set Manager)

When a process has both singleton services and a pool of homogeneous entities — **wrap `Set` in a `Unit`**:

```go
type CameraManager struct {
    set *steward.Set[string, CameraConfig]
}

func (m *CameraManager) ID() string { return "camera-manager" }

func (m *CameraManager) Start(ctx context.Context) error {
    if err := m.set.Start(ctx); err != nil {
        return err
    }
    return m.set.Reconcile(initialCameras)
}

func (m *CameraManager) Stop(ctx context.Context) error {
    return m.set.Stop(ctx)  // blocks until all cameras in Set have stopped
}

// Hot-reload cameras — Reconcile inside the manager:
func (m *CameraManager) ApplyConfig(desired map[string]CameraConfig) error {
    return m.set.Reconcile(desired)
}
```

The composition layer registers `CameraManager` as one `Unit` alongside `Instance[DB]`, `Instance[HTTP]`.  
Inside — a full `Set` for thousands of cameras with policy, events, and drain.

> **Embedded Set:** `Unit.Stop` **must** call `set.Stop(ctx)` and wait for it. Otherwise the parent Unit returns `Stopped` while cameras are still running. `set.Stop` blocks until all supervisors exit — matching the `Unit.Stop` contract.

## DI Integration (fx, wire, dig)

`steward` does not impose DI — but pairs well with it. Pattern: DI wires `build`/`equal` closures and configures `Set`; runtime lifecycle is managed via `fx.Lifecycle` or equivalent.

### uber-go/fx

```go
func NewRecorderSet(lc fx.Lifecycle, cfg Config, db *sql.DB) *steward.Set[string, CameraConfig] {
    set := steward.NewSet[string, CameraConfig](
        func(_ context.Context, id string, c CameraConfig) (steward.Unit, error) {
            return NewRecorder(id, c, db), nil
        },
        func(a, b CameraConfig) bool { return a == b },
        steward.WithPolicy(steward.DefaultPolicy{
            Start: 10 * time.Second,
            Drain: 5 * time.Second,
        }),
    )

    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            return set.Start(ctx)
        },
        OnStop: func(ctx context.Context) error {
            return set.Stop(ctx)
        },
    })
    return set
}
```

### google/wire

```go
func InitializeApp() (*App, func(), error) {
    wire.Build(provideDB, provideBuild, provideEqual, provideSet, provideApp)
    return nil, nil, nil
}
```

### Key Pattern

DI owns **composition** (who depends on whom, Ref handles, rebuild decisions).  
`steward` owns **runtime lifecycle** (start, stop, restart, reconcile).

```text
Boot time:   DI/wire/fx → build graph → NewSet(build, equal) / Instance
Runtime:     config watcher → Reconcile(desired) → steward converges actual
Shutdown:    signal → Stop(ctx) → steward waits for all supervisors
```

**Order of `Start` across different `Instance` values** — L1 responsibility.  
**Readiness between Units** — `WaitReady` inside each component.  
**Hot-swap logger without restarting DB** — `Ref[Logger]` in L1, not in `steward`.

## Signal Handling (Graceful Shutdown)

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    set := steward.NewSet(build, equal, steward.WithPolicy(steward.DefaultPolicy{}))
    set.Start(ctx)
    set.Reconcile(desired)

    <-ctx.Done()

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := set.Stop(shutdownCtx); err != nil {
        log.Fatalf("shutdown: %v", err)
    }
}
```

`Stop` is queued on the scheduler **after** the current `Reconcile` if one is still running. Signal `ctx.Done()` **does not cancel** an active reconcile — for shutdown use a separate `shutdownCtx` in `Stop`, as in the example.

## Health Checks (HTTP /healthz)

`Snapshot` reflects **lifecycle state**, not business health. `StateFailed` — unit crashed; `StateStarting` — has not passed `WaitReady` yet.

```go
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    units := set.Snapshot()
    for _, u := range units {
        switch u.State {
        case steward.StateFailed:
            w.WriteHeader(http.StatusServiceUnavailable)
            fmt.Fprintf(w, "unit %s failed: %v\n", u.ID, u.LastError)
            return
        case steward.StateStarting:
            w.WriteHeader(http.StatusServiceUnavailable)
            fmt.Fprintf(w, "unit %s not ready\n", u.ID)
            return
        }
    }
    w.WriteHeader(http.StatusOK)
    fmt.Fprintf(w, "ok: %d units\n", len(units))
})

// Liveness/readiness inside Unit — via WaitReady at start.
// // Business-degraded — separate probe or custom metrics.
```

## Verifying That Work Has Actually Finished


| Signal                         | Meaning                  | Reliability    |
| ------------------------------ | ------------------------ | -------------- |
| `Stop()` returned              | All supervisors finished | **Maximum**    |
| `Running(key) == false`        | Unit not in Running      | High           |
| `Failed(key) == true`          | Unit in Failed           | Diagnostic     |
| `EventStopped` / `EventFailed` | Async telemetry          | Medium (lossy) |
| `Snapshot()[].State`           | Point-in-time view       | High           |


## Lifecycle

Set/Instance lifecycle from the caller's perspective:

```
not started ──▶ Start(ctx) ──▶ running ──▶ Stop(ctx) ──▶ stopped
                    │              │
                    │              └── Reconcile(desired) anytime (serialized)
                    │
                    └── second Start() → ErrAlreadyStarted
```

Per-unit state machine (supervisor):

```
Created → Starting → Running → Stopping → Stopped / Failed
              │          │                      │
              │          └── policy failure ──▶ Failed (maybe restart)
              └── WaitReady timeout/error ──▶ Failed
```

- `**Start(ctx)**` — starts the scheduler goroutine exactly once. Stores `parentCtx` for supervisor cancellation. Idempotent: second call returns `ErrAlreadyStarted`.
- `**Reconcile(desired)**` — enqueues an ordered diff on `cmdCh`. Serialized: never overlaps another command. Returns when converge completes or errors mid-reconcile.
- `**Replace(build, equal, desired)**` — atomically replaces build/equal and recreates **all** units. Use when shared dependencies in the `Build` closure change (e.g. new DB pool).
- `**Stop(ctx)`** — enqueues stop after any in-flight command. Cancels all supervisors, waits for Drain → Stop per unit, closes event channels. **Blocks** until every unit has exited. Idempotent after shutdown.
- `**Snapshot()`** — enqueues a shallow copy of all `UnitView` values. Safe to read after return.
- After `Stop`, `Reconcile` / `Replace` return `ErrStopped`. `Start` after `Stop` returns `ErrStopped`.

Policy restart runs **outside** reconcile: same desired config, new supervisor after backoff. The scheduler never restarts the same key concurrently.

## Extensibility

### Build and Equal

```go
type BuildFunc[K comparable, C any] func(ctx context.Context, id string, cfg C) (Unit, error)
type EqualFunc[C any] func(a, b C) bool
```

`NewSet(build, equal)` panics if either function is nil — no implicit `reflect.DeepEqual`. You must define equality semantics for your config type explicitly.

`**Build` contract:**

- Return a `Unit` value; do not start goroutines
- Capture dependencies via closure (DB handle, logger, metrics) — wired at DI time
- Return error to abort reconcile for that key; partial state may remain

`**Equal` contract:**

- Deterministic, side-effect free, symmetric
- Runs in scheduler — O(1) or cheap comparison only
- Drives no-op detection: unchanged desired → no stop/start

### Policy

```go
type Policy interface {
    Classify(err error) FailureClass
    ShouldRestart(state State, failure Failure) bool
    Backoff(unitID string, attempt int) time.Duration
    StartTimeout(unitID string) time.Duration
    DrainTimeout(unitID string) time.Duration
}
```

`DefaultPolicy` provides production defaults: `Classify` via `classifyError`, exponential backoff capped at 5 minutes, 30s start timeout, 15s drain timeout. Override per deployment:

```go
steward.WithPolicy(steward.DefaultPolicy{
    Start:      10 * time.Second,
    Drain:      5 * time.Second,
    MaxBackoff: 2 * time.Minute,
})
```

Custom policies can implement circuit-breaker semantics (stop restarting after N attempts), per-unit backoff jitter, or different timeouts by unit ID.

Use `ClassifyError` in Unit code to attach explicit classes before `Policy.Classify`:

```go
return steward.ClassifyError(steward.FailureConfigError, fmt.Errorf("invalid bitrate %d", cfg.Bitrate))
```

### Handoff

```go
type Handoff func(old, new Unit) error
```

Optional state migration on config change: copy buffers, transfer connections, migrate in-memory state. Runs synchronously in scheduler between `Build(new)` and stopping `old`. On error, `old` keeps running. Configure with `WithHandoff`.

Limitations by design: no traffic routing, no guarantee both units run simultaneously, no blue/green in core.

### RunUnit Adapter

For simple blocking loops without a custom struct:

```go
steward.RunUnit("poller", func(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            if err := poll(); err != nil {
                return err  // → Failure → Policy
            }
        }
    }
})
```

`Start` launches the function in a goroutine and returns immediately. `Stop` cancels context and waits for the function to return.

## API

### Core Types


| Type                       | Description                          |
| -------------------------- | ------------------------------------ |
| `Unit`                     | Lifecycle contract: Start/Stop     |
| `Drainer`                  | Optional: Drain before Stop          |
| `Ready`                    | Optional: WaitReady after Start      |
| `BuildFunc` / `EqualFunc`  | Build + Equal funcs for `NewSet`     |
| `Policy`                   | Classify, restart, backoff, timeouts |
| `Failure` / `FailureClass` | Classified failures (3 classes)      |
| `Handoff`                  | Optional hot-reload state migration  |
| `Event`                    | Lifecycle telemetry                  |
| `UnitView`                 | Shallow per-unit snapshot            |


### `Set[K, C]`


| Method                               | Description                           |
| ------------------------------------ | ------------------------------------- |
| `NewSet(build, equal, opts...)`      | Create set                            |
| `Start(ctx)`                         | Start scheduler (exactly once)        |
| `Reconcile(desired)`                 | Ordered diff + converge               |
| `Replace(build, equal, desired)` | Atomic build/equal swap + recreate all |
| `Stop(ctx)`                          | Stop all, wait, close events          |
| `Snapshot()`                         | Shallow `[]UnitView`                  |
| `Running(key)` / `Failed(key)`       | Query unit state                      |
| `Events()`                           | Primary event channel                 |
| `DroppedEvents()`                    | Overflow counter                      |
| `AuditEvents()`                      | Isolated audit channel (if enabled)   |
| `DroppedAuditEvents()`               | Audit overflow counter                |


### `Instance[C]`

Thin wrapper over `Set[string, C]` with fixed key `"default"`.


| Method                                  | Description                |
| --------------------------------------- | -------------------------- |
| `NewInstance(cfg, build, equal, opts...)` | Singleton set            |
| `Start(ctx)`                            | Start + initial reconcile  |
| `Reload(cfg)`                           | Config change → restart    |
| `Replace(build, equal, cfg)`       | Build/equal + config change |
| `Stop(ctx)`                             | Shutdown                   |
| `Running()` / `Failed()`                | Status                     |
| `Config()`                              | Last applied config        |
| `Snapshot()` / `Events()`               | Observability              |


### Options


| Option                   | Default             | Description              |
| ------------------------ | ------------------- | ------------------------ |
| `WithPolicy(p)`          | `DefaultPolicy{}`   | Classify/restart/backoff/timeouts |
| `WithEventBuffer(n)`     | `64`                | Primary events buffer    |
| `WithAuditBuffer(n)`     | `0` (off)           | Isolated audit pipeline  |
| `WithHandoff(h)`         | nil                 | Hot-reload state migration |


## Configuration

### Equal Contract

`Equal(a, b)` must be deterministic, side-effect free, and symmetric. **Required** — `NewSet` panics if `equal == nil`. This forces explicit equality semantics for your config type.

### Context Contract

```
Set.Start(parentCtx) — stores parentCtx in scheduler

command queue (serialized):
    Reconcile / Stop / Snapshot — one command at a time, others wait

per unit (supervisor goroutine):
    supervisor ctx = WithCancel(parentCtx)
    Start(supervisorCtx) → [WaitReady] → running

on reconcile-replace / remove / Stop:
    cancel → wait supervisor → [Drain → Stop]

on policy restart:
    AfterFunc(backoff) → tick → recreate unit
```

## Observability

### Events

```go
type Event struct {
    UnitID string
    Type   EventType   // started, stopped, reloaded, failed
    From   State
    To     State
    Err    error
    Time   time.Time
}
```

Primary channel (`Events()`):

- Bounded buffer (default 64, configurable via `WithEventBuffer`)
- On overflow: drop oldest event, increment `DroppedEvents()`
- Scheduler never blocks on send — slow consumers lose events

Audit channel (`AuditEvents()`, optional):

- Enabled via `WithAuditBuffer(n)` where `n > 0`
- Isolated from primary — audit drops do not affect reconcile
- `DroppedAuditEvents()` tracks audit overflow separately
- Best-effort delivery for compliance logging

**Goroutine model:** one consumer goroutine per channel is typical. Multiple consumers on the same channel compete for events.

### Snapshot

```go
for _, u := range set.Snapshot() {
    // u.ID, u.State, u.Uptime, u.RestartCount, u.LastError, u.FailureClass
}
```

- Shallow copy — no deep copy of configs
- Point-in-time view; units may transition immediately after return
- Safe to call from any goroutine; serialized through scheduler
- Use for `/healthz`, dashboards, debug endpoints

### Dropped Event Accounting

Always monitor `DroppedEvents()` in production if you rely on events for alerting:

```go
if set.DroppedEvents() > 0 {
    metrics.Inc("steward_events_dropped", set.DroppedEvents())
}
```

For lossless audit trails, enable `WithAuditBuffer` and a dedicated fast consumer.

## Pitfalls

> [!WARNING]
> **Policy restart, not reconcile.** If a unit fails with a transient error — `Policy` restarts it. If config did not change and policy says "no restart" — the unit stays Failed.

> [!WARNING]
> **Panic is not recovered.** Panic in any user callback crashes the process (by design). Affects: `Unit.Start`/`Stop`/`Drain`/`WaitReady`, `BuildFunc`, `EqualFunc`, `Policy.*`, `Handoff`. No recovery in runtime.

> [!WARNING]
> `**Start` MUST NOT block.** Network, listen, connect — in `WaitReady` or a goroutine from `Start`. Blocking `Start` leaves the Unit in limbo (supervisor stuck, not Running).

> [!WARNING]
> **Scheduler callbacks MUST NOT block.** `Equal`, `Build`, `Handoff` run in the scheduler — long reconcile blocks `Stop`, `Snapshot`, and the next `Reconcile`.

> [!WARNING]
> **Build error aborts reconcile.** Mid-reconcile error → partial state. Handle the error and retry with correct desired.

> [!WARNING]
> **Primary events are lossy.** Slow consumer drops events. Use `DroppedEvents()` + `Failed()` polling. For audit — `WithAuditBuffer`.

> [!WARNING]
> **`Handoff` error** — old unit keeps running; reconcile returns error.

> [!WARNING]
> `**Replace` recreates ALL units** — even if config unchanged. Use for shared dependency changes.

> [!WARNING]
> **`NewSet` panics on nil build or equal.** No fallback to `reflect.DeepEqual` — you must define equality explicitly.

## vs Alternatives


| Solution                   | Focus                                              | Missing                                                      |
| -------------------------- | -------------------------------------------------- | ------------------------------------------------------------ |
| `errgroup`                 | One-shot parallel tasks                            | Hot-reload, keyed sets, policy                               |
| `uber-go/fx`               | DI wiring at startup                               | Runtime reconcile, restart                                   |
| `go-supervisor` / `suture` | Panic restart trees                                | Desired-state diff, ready/drain                              |
| `oklog/run`                | Actor group lifecycle                              | Keyed reconcile, config diff                                 |
| `**steward`**              | **L2 lifecycle + reconcile for Unit/Set/Instance** | DAG, cascade restart, Ref registry, heterogeneous single-Set |


**Rule:** homogeneous set with hot-reload and policy → `Set`. Singleton long-lived service → `Instance`. Heterogeneous process → multiple `Instance` + composition in L1.

## Errors

Four sentinel errors, all comparable with `==` and `errors.Is`:


| Error               | When                                                 |
| ------------------- | ---------------------------------------------------- |
| `ErrNotStarted`     | `Reconcile` / `Stop` before `Start`                  |
| `ErrAlreadyStarted` | Second `Start`                                       |
| `ErrStopped`        | `Start` / `Reconcile` / `Replace` after `Stop` |
| Build error         | Propagated from `Reconcile` / `Reload`               |
| Unit error          | `EventFailed`, `Failed(key)==true`, policy restart   |


## Safety and Concurrency

- **Thread safety:** entire public API safe from multiple goroutines
- **Serialization:** all lifecycle mutations through scheduler goroutine
- **Ownership:** `unitSlot` state — scheduler only; supervisor reports via commands
- **No leaks:** `Stop` waits for supervisor `done`
- **Race-free:** `-race` on all tests
- **Idempotency:** `Stop` idempotent; `Reconcile` with same desired = no-op
- **No IPC/network:** in-process only
- **No goroutine introspection:** runtime never inspects or kills goroutines directly — cancellation only
- **No lock while waiting:** scheduler never holds mutex across unit Drain/Stop waits
- **Panic = crash:** by design; fail-fast for programming errors in callbacks

## Benchmarks

Pure in-process CPU and memory — no I/O, no network, no filesystem. `B/op` and `allocs/op` are deterministic; ns/op varies by CPU.

### Environment (reference run)


|            | Value                                              |
| ---------- | -------------------------------------------------- |
| CPU        | Intel Core i7-10510U @ 1.80GHz (4C/8T, 15W mobile) |
| OS         | Windows 10 (amd64)                                 |
| Go         | 1.24                                               |
| GOMAXPROCS | 8                                                  |
| Runs       | 1 (`-count=1`)                                     |


### Results


| Benchmark                | What it measures                               | ns/op     | B/op  | allocs/op |
| ------------------------ | ---------------------------------------------- | --------- | ----- | --------- |
| `Set_ReconcileUnchanged` | 5 keys, same desired, no-op diff               | 1,928     | 48    | 1         |
| `Set_ReconcileChurn`     | 3 rotating keys, constant create/update/remove | 21,487    | 3,676 | 49        |
| `Instance_Reload`        | Singleton config change → stop + start         | 15,910    | 1,666 | 23        |
| `Set_Reconcile1k`        | 1,000 keys, unchanged desired                  | 166,521   | 105   | 1         |
| `Set_Reconcile10k`       | 10,000 keys, unchanged desired                 | 2,467,206 | 8,204 | 102       |


### Analysis

**No-op reconcile is sub-microsecond per key at small scale.** Five keys unchanged: ~1.9 µs total, **1 alloc/op** (pooled response channel). At 1,000 keys: ~167 µs ≈ 167 ns/key — linear O(n) scan with near-constant per-key cost.

**Churn dominates.** Rotating 3 keys with stop/start each iteration: ~21 µs — ~10× no-op cost. Allocations (~49 allocs) come from supervisor teardown and context creation. Real hot-reload with few changes per tick should stay closer to the unchanged path.

**Singleton reload: ~16 µs.** `Instance.Reload` stops and restarts one supervisor — fixed cost regardless of how many other Sets exist in the process.

**10k keys: ~2.5 ms unchanged reconcile.** ~247 ns/key at 10,000 scale. Suitable for camera/PLC fleets in one process. Actual stop/start on large fleets is bounded by per-unit Drain/Stop time, not diff cost.

**Allocations are low on steady state.** Unchanged reconcile at 1k keys: **1 alloc/op**; string keys skip `fmt.Sprintf` in `formatKey` during events and supervisor setup.

**What the numbers mean:** benchmarks measure scheduler + diff overhead, not Unit work. A camera recorder doing RTSP decode will dominate wall time. Profile your `Build`/`Equal` and Unit `Start`/`WaitReady` separately.

```bash
go test -bench=Benchmark -benchmem -run=^Benchmark .
```

### Profiling (AI-assisted optimization)

```powershell
.\a-report-pprof.ps1
```

Produces `a-report-{timestamp}.md` with per-benchmark CPU/memory pprof tops and raw profiles in `profiles/`. See `a-report-howto.md`. CI runs this on push to `main` (Windows) and uploads artifacts.

## Quality


| Metric         | Value                                                                                 |
| -------------- | ------------------------------------------------------------------------------------- |
| Test functions | 80                                                                                    |
| Fuzz targets   | 2 (`FuzzDiff`, `FuzzReconcileSet`)                                                    |
| Benchmarks     | 5                                                                                     |
| Coverage       | **94.9%** statements                                                                  |
| Race detector  | `-race` on all tests                                                                  |
| Soak tests     | 4 (rapid reconcile 3s, instance reload 2s, parallel Replace 2s, policy restart) |
| Scenario tests | camera pool, Modbus fleet, message consumers                                          |
| Conformance    | Drain before Stop, WaitReady gating, config-error no-restart                          |
| External deps  | 0 (stdlib only)                                                                       |
| Go version     | 1.24+                                                                                 |


```bash
# All tests + race
go test -race -timeout 120s ./...

# CI (no soak)
go test -race -short ./...

# Coverage
go test -coverprofile=cov.out -covermode=atomic -short .
go tool cover -func=cov.out

# Soak (no -short)
go test -race -run Soak -timeout 120s .

# Benchmarks
go test -bench=Benchmark -benchmem -run=^Benchmark .

# Fuzz
go test -fuzz=FuzzDiff -fuzztime=10s .
```

## File Structure

```
steward/
├── doc.go                  Package godoc
├── unit.go                 Unit, Drainer, Ready
├── state.go                State machine
├── failure.go              FailureClass, ClassifyError
├── policy.go               Policy, DefaultPolicy
├── controller.go           BuildFunc, EqualFunc
├── reconcile.go            Diff, DiffPlan
├── engine.go               unitSlot, launchSupervisor
├── scheduler.go            Command loop
├── respchan.go             Pooled scheduler response channels
├── set.go                  Set[K,C]
├── instance.go             Instance[C]
├── event.go                eventBus (primary + audit)
├── snapshot.go             UnitView
├── rununit.go              RunFunc → Unit adapter
├── handoff.go              Handoff
├── options.go              WithPolicy, WithHandoff, WithAuditBuffer, ...
├── a-report-pprof.ps1      Benchmark profiling for optimization
├── a-report-howto.md       Profiling pipeline docs
├── errors.go
├── go.mod
├── CHANGELOG.md
├── README.md
└── *_test.go               unit, engine, race, scenario, conformance,
                            soak, bench, fuzz, coverage tests
```

## License

MIT