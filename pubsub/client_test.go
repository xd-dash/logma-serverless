package pubsub

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientFromRequestPrefersEnvAuth(t *testing.T) {
	t.Setenv("REDISCLI_AUTH", "env-secret")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRedisAuth, "header-secret")

	client := NewClientFromRequest(req)
	if got := client.Options().Password; got != "env-secret" {
		t.Fatalf("expected env auth to take precedence, got %q", got)
	}
}

func TestNewClientFromRequestFallsBackToHeaderWhenEnvUnset(t *testing.T) {
	t.Setenv("REDISCLI_AUTH", "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRedisAuth, "header-secret")

	client := NewClientFromRequest(req)
	if got := client.Options().Password; got != "header-secret" {
		t.Fatalf("expected header auth fallback, got %q", got)
	}
}

func TestNewClientFromRequestEmptyWhenNeitherIsSet(t *testing.T) {
	t.Setenv("REDISCLI_AUTH", "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	client := NewClientFromRequest(req)
	if got := client.Options().Password; got != "" {
		t.Fatalf("expected empty password, got %q", got)
	}
}
