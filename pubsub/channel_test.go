package pubsub

import "testing"

func TestDefaultChannelWithNamespace(t *testing.T) {
	if got := DefaultChannel("stonks", "shutdown"); got != "stonks:control:shutdown" {
		t.Fatalf("DefaultChannel(stonks, shutdown) = %q, want %q", got, "stonks:control:shutdown")
	}
}

func TestDefaultChannelWithoutNamespace(t *testing.T) {
	if got := DefaultChannel("", "add"); got != "control:add" {
		t.Fatalf("DefaultChannel(\"\", add) = %q, want %q", got, "control:add")
	}
}
