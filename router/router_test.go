package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestAuthenticateAPIKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := authenticateAPIKey(next)

	t.Run("missing API_KEY env var", func(t *testing.T) {
		os.Unsetenv("API_KEY")

		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rec.Code)
		}
	})

	t.Run("missing header", func(t *testing.T) {
		os.Setenv("API_KEY", "secret")
		defer os.Unsetenv("API_KEY")

		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("wrong header", func(t *testing.T) {
		os.Setenv("API_KEY", "secret")
		defer os.Unsetenv("API_KEY")

		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		req.Header.Set(apiKeyHeader, "wrong")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("correct header", func(t *testing.T) {
		os.Setenv("API_KEY", "secret")
		defer os.Unsetenv("API_KEY")

		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		req.Header.Set(apiKeyHeader, "secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})
}

func TestNewRouterHasNoRootRoute(t *testing.T) {
	os.Setenv("API_KEY", "secret")
	defer os.Unsetenv("API_KEY")

	r := NewRouter()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(apiKeyHeader, "secret")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /, got %d", rec.Code)
	}
}

func TestRuntimeHolderReusesAfterSessionEnds(t *testing.T) {
	holder := &runtimeHolder{}

	first := holder.claim()
	if first == nil {
		t.Fatal("expected first claim to succeed")
	}
	go first.Start(context.Background())

	if second := holder.claim(); second != nil {
		t.Fatal("expected second claim to fail while first session is active")
	}

	// Simulate the first session's request ending.
	first.Cancel()
	<-first.Done()

	third := holder.claim()
	if third == nil {
		t.Fatal("expected claim to succeed once the prior session finished")
	}
	if third == first {
		t.Fatal("expected a fresh Runtime for the new session")
	}
	go third.Start(context.Background())
	third.Cancel()
	<-third.Done()
}
