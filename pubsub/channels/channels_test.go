package channels

import "testing"

func TestDefaultsShutdownChannelWithNamespace(t *testing.T) {
	d := ForNamespace("stonks")
	if got := d.ShutdownChannel(); got != "stonks:control:shutdown" {
		t.Fatalf("ShutdownChannel() = %q, want %q", got, "stonks:control:shutdown")
	}
}

func TestDefaultsAddChannelWithoutNamespace(t *testing.T) {
	d := ForNamespace("")
	if got := d.AddChannel(); got != "control:add" {
		t.Fatalf("AddChannel() = %q, want %q", got, "control:add")
	}
}

func TestDiscoverUsesKServiceWhenSet(t *testing.T) {
	t.Setenv("K_SERVICE", "stonks")

	d := Discover()
	if d.Namespace != "stonks" {
		t.Fatalf("Discover().Namespace = %q, want %q", d.Namespace, "stonks")
	}
}

func TestDiscoverFallsBackToModulePath(t *testing.T) {
	t.Setenv("K_SERVICE", "")

	d := Discover()
	if d.Namespace != "logma-serverless" {
		t.Fatalf("Discover().Namespace = %q, want %q (this module's own name)", d.Namespace, "logma-serverless")
	}
}
