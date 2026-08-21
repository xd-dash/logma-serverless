package pubsub

import "testing"

func TestControlPlaneInstanceChannel(t *testing.T) {
	cp := ControlPlane{InstanceID: "host-1"}
	got := cp.InstanceChannel("stonks:control:shutdown")
	want := "stonks:control:shutdown:host-1"
	if got != want {
		t.Fatalf("InstanceChannel() = %q, want %q", got, want)
	}
}

func TestControlPlaneGlobalChannel(t *testing.T) {
	cp := ControlPlane{InstanceID: "host-1"}
	got := cp.GlobalChannel("stonks:control:shutdown")
	want := "stonks:control:shutdown:global"
	if got != want {
		t.Fatalf("GlobalChannel() = %q, want %q", got, want)
	}
}

func TestControlPlaneChannelsDifferPerInstance(t *testing.T) {
	a := ControlPlane{InstanceID: "host-a"}
	b := ControlPlane{InstanceID: "host-b"}

	if a.InstanceChannel("control:add") == b.InstanceChannel("control:add") {
		t.Fatal("expected different instances to derive different instance channels")
	}
	if a.GlobalChannel("control:add") != b.GlobalChannel("control:add") {
		t.Fatal("expected the global channel to be the same regardless of instance")
	}
}
