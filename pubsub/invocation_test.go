package pubsub

import (
	"net/http/httptest"
	"os"
	"testing"
)

func TestInvocationKeyUsesServiceInstanceRequest(t *testing.T) {
	info := InvocationInfo{Service: "stonks", InstanceID: "host-1", RequestID: "req-1"}
	got := InvocationKey(info)
	want := "instance:stonks:host-1:req-1"
	if got != want {
		t.Fatalf("InvocationKey() = %q, want %q", got, want)
	}
}

func TestInvocationKeyFallsBackToUnknownForEmptySegments(t *testing.T) {
	got := InvocationKey(InvocationInfo{})
	want := "instance:unknown:unknown:unknown"
	if got != want {
		t.Fatalf("InvocationKey() = %q, want %q", got, want)
	}
}

func TestInvocationInfoFromRequestReadsEnvAndRequest(t *testing.T) {
	t.Setenv("K_SERVICE", "stonks")
	t.Setenv("K_REVISION", "stonks-00001-abc")
	t.Setenv("K_CONFIGURATION", "stonks")

	r := httptest.NewRequest("POST", "/stream", nil)
	r.RemoteAddr = "203.0.113.5:1234"

	info := InvocationInfoFromRequest(r, "req-42")

	if info.Service != "stonks" || info.Revision != "stonks-00001-abc" || info.Configuration != "stonks" {
		t.Fatalf("unexpected env-derived fields: %+v", info)
	}
	if info.RequestID != "req-42" {
		t.Fatalf("expected request ID req-42, got %q", info.RequestID)
	}
	if info.Method != "POST" || info.Path != "/stream" || info.RemoteAddr != "203.0.113.5:1234" {
		t.Fatalf("unexpected request-derived fields: %+v", info)
	}
	if info.StartedAt.IsZero() {
		t.Fatal("expected StartedAt to be set")
	}
	if info.InstanceID == "" {
		t.Fatal("expected InstanceID to be populated from os.Hostname")
	}
}

func TestInvocationInfoFromRequestDefaultsMissingEnv(t *testing.T) {
	os.Unsetenv("K_SERVICE")
	os.Unsetenv("K_REVISION")
	os.Unsetenv("K_CONFIGURATION")

	r := httptest.NewRequest("GET", "/", nil)
	info := InvocationInfoFromRequest(r, "")

	if info.Service != "" || info.Revision != "" || info.Configuration != "" {
		t.Fatalf("expected empty env-derived fields when unset, got %+v", info)
	}
}
