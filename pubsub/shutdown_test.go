package pubsub

import "testing"

func TestParseShutdownRequestEmptyPayload(t *testing.T) {
	got := ParseShutdownRequest("")
	if got.Reason != "" {
		t.Fatalf("expected empty reason for empty payload, got %q", got.Reason)
	}
}

func TestParseShutdownRequestValidPayload(t *testing.T) {
	got := ParseShutdownRequest(`{"reason":"maintenance"}`)
	if got.Reason != "maintenance" {
		t.Fatalf("expected reason %q, got %q", "maintenance", got.Reason)
	}
}

func TestParseShutdownRequestInvalidJSONReturnsZeroValue(t *testing.T) {
	got := ParseShutdownRequest("not json")
	if got.Reason != "" {
		t.Fatalf("expected empty reason for invalid JSON, got %q", got.Reason)
	}
}
