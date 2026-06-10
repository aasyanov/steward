package steward

import "testing"

func TestStateString(t *testing.T) {
	cases := []struct {
		state State
		want  string
	}{
		{StateCreated, "created"},
		{StateStarting, "starting"},
		{StateRunning, "running"},
		{StateStopping, "stopping"},
		{StateStopped, "stopped"},
		{StateFailed, "failed"},
		{State(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("%v.String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestFailureClassString(t *testing.T) {
	if FailureTransient.String() != "transient" {
		t.Fatal()
	}
	if FailureClass(99).String() != "fatal" {
		t.Fatal()
	}
}

func TestClassifyErrorNil(t *testing.T) {
	if ClassifyError(FailureFatal, nil) != nil {
		t.Fatal("expected nil")
	}
}
