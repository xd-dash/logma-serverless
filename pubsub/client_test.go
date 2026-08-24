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

func TestNewClientFromRequestPrefersEnvURI(t *testing.T) {
	t.Setenv("REDIS_URI", "env-host:6379")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRedisURI, "header-host:6379")

	client := NewClientFromRequest(req)
	if got := client.Options().Addr; got != "env-host:6379" {
		t.Fatalf("expected env URI to take precedence, got %q", got)
	}
}

func TestNewClientFromRequestFallsBackToHeaderURIWhenEnvUnset(t *testing.T) {
	t.Setenv("REDIS_URI", "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRedisURI, "header-host:6379")

	client := NewClientFromRequest(req)
	if got := client.Options().Addr; got != "header-host:6379" {
		t.Fatalf("expected header URI fallback, got %q", got)
	}
}

func TestNewClientFromRequestURIAndAuthFallBackIndependently(t *testing.T) {
	t.Setenv("REDIS_URI", "env-host:6379")
	t.Setenv("REDISCLI_AUTH", "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRedisAuth, "header-secret")
	req.Header.Set(HeaderRedisURI, "header-host:6379")

	client := NewClientFromRequest(req)
	opts := client.Options()
	if opts.Addr != "env-host:6379" {
		t.Fatalf("expected env URI to still win even though auth fell back, got %q", opts.Addr)
	}
	if opts.Password != "header-secret" {
		t.Fatalf("expected auth to fall back to the header, got %q", opts.Password)
	}
}
