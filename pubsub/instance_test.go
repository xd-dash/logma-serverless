package pubsub

import (
	"os"
	"testing"
)

func TestComputeInstanceIDUsesHostnameWhenRunningOnCloudRun(t *testing.T) {
	t.Setenv("K_SERVICE", "stonks")

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		t.Skip("no hostname available in this environment")
	}

	if got := computeInstanceID(); got != hostname {
		t.Fatalf("computeInstanceID() = %q, want hostname %q", got, hostname)
	}
}

func TestComputeInstanceIDGeneratesDevValueOutsideCloudRun(t *testing.T) {
	os.Unsetenv("K_SERVICE")

	got := computeInstanceID()
	if len(got) < len("dev-") || got[:4] != "dev-" {
		t.Fatalf("expected a dev-prefixed id outside Cloud Run, got %q", got)
	}

	if got2 := computeInstanceID(); got2 == got {
		// Not a hard requirement (randomHex could theoretically repeat),
		// but with 8 random bytes a collision here would be a sign
		// something is broken rather than bad luck.
		t.Logf("computeInstanceID() returned the same value twice: %q -- acceptable but notable", got)
	}
}

func TestInstanceIDIsStableAcrossCalls(t *testing.T) {
	first := InstanceID()
	second := InstanceID()
	if first != second {
		t.Fatalf("expected InstanceID() to be stable for the process lifetime, got %q then %q", first, second)
	}
}
