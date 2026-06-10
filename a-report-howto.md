# a-report-pprof.ps1 — How It Works

Automated profiling script for the `steward` Go module.
Discovers all benchmarks, profiles each one, and produces a single
markdown report optimized for AI-assisted performance analysis.

## Pipeline

```
Phase 0: Clean profiles/
Phase 1: go test -list '^Benchmark' .  →  discover all bench functions
Phase 2: go test -c -o __bench_test.exe .  →  compile once
Phase 3: calibration (-benchtime=1x)  →  measure ns/op per benchmark
Phase 4: profiling  →  run each bench with adaptive benchtime + cpuprofile + memprofile
Phase 5: pprof top + parse metrics  →  write a-report-{timestamp}.md
Phase 6: remove __bench_test.exe
```

## Why calibration?

CPU profiler samples at 100 Hz. A benchmark running for 1 second collects
~100 samples — enough for top-5 hotspots. For 200 samples (the default
`-TargetSamples`) we need ~2 seconds.

steward benchmarks span a wide range: no-op reconcile at ~1–2 µs for 5 keys
to `BenchmarkSet_Reconcile10k` at ~3 ms for 10,000 keys. `BenchmarkSet_ReconcileChurn`
and `BenchmarkInstance_Reload` include supervisor stop/start — slower per iteration.
Adaptive benchtime ensures each benchmark gets enough profiler samples without
running `Reconcile10k` longer than necessary.

**Calibration runs each benchmark once** (`-benchtime=1x`) and uses the measured
`ns/op` to compute the ideal benchtime: enough wall-clock seconds for
`TargetSamples` profiler samples, clamped to `[MinBenchSec, MaxBenchSec]`.

## Benchmarks profiled

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkSet_ReconcileUnchanged` | 5 keys, same desired, no-op diff |
| `BenchmarkSet_ReconcileChurn` | Rotating 3 keys, create/update/remove |
| `BenchmarkInstance_Reload` | Singleton config change → stop + start |
| `BenchmarkSet_Reconcile1k` | 1,000 keys, unchanged desired |
| `BenchmarkSet_Reconcile10k` | 10,000 keys, unchanged desired |

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-BenchFilter` | `.` | Regex filter on benchmark names |
| `-MinBenchSec` | `1` | Floor for computed benchtime |
| `-MaxBenchSec` | `5` | Ceiling for computed benchtime |
| `-TargetSamples` | `200` | Target CPU profiler samples (100 Hz) |
| `-PprofTop` | `15` | Number of top functions per profile |
| `-SkipCalibration` | off | Use fixed benchtime instead |
| `-FixedBenchTime` | `2s` | Benchtime when calibration is skipped |
| `-NoClean` | off | Keep existing profiles/ contents |

## Outputs

- `a-report-{timestamp}.md` — consolidated report with summary table
  and per-benchmark CPU/memory pprof tops
- `profiles/cpu_{BenchmarkName}.prof` — raw CPU profiles (viewable in `go tool pprof`)
- `profiles/mem_{BenchmarkName}.prof` — raw memory profiles

## Usage Examples

```powershell
# All benchmarks, default settings
.\a-report-pprof.ps1

# Only reconcile benchmarks (skip Reload)
.\a-report-pprof.ps1 -BenchFilter 'Reconcile'

# Quick pass with fixed 1s benchtime, no calibration
.\a-report-pprof.ps1 -SkipCalibration -FixedBenchTime '1s'

# CI-style: shorter max benchtime for large fleet benchmarks
.\a-report-pprof.ps1 -MinBenchSec 1 -MaxBenchSec 3 -TargetSamples 150
```

## Report Structure

The generated markdown report contains:

1. **Header** — timestamp, Go version, OS, benchmark count, filter
2. **Summary table** — one row per benchmark: ns/op, B/op, allocs/op, MB/s, iterations, benchtime used
3. **Detailed Profiles** — per-benchmark section with:
   - Metrics table
   - CPU profile top (cumulative, `go tool pprof -top -cum`)
   - Memory profile top (alloc_objects cumulative)
4. **Errors** — benchmarks that failed to produce results
5. **Timing** — total/compilation/profiling durations

## Design Decisions

**Pre-compilation** (`go test -c`): compiling once saves time vs
separate `go test` invocations that each recompile.

**Root package only**: all 5 benchmarks live in `bench_test.go` (external test package).
Profiling targets scheduler diff, command queue, and supervisor lifecycle overhead.

**No `-show` filter on pprof**: full cumulative output is more useful for AI
analysis — it reveals channel/command-loop dominance vs per-unit supervisor cost.

**Adaptive benchtime**: `Reconcile10k` is capped at 2s when ns/op > 50ms to avoid
multi-minute profiling runs on CI while still collecting meaningful samples.
