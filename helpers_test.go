package steward_test

import (
	"runtime"
	"testing"
	"time"
)

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met before timeout")
		case <-tick.C:
		}
	}
}

func gcAndWait() {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
}

func runtimeNumGoroutine() int {
	gcAndWait()
	return runtime.NumGoroutine()
}
