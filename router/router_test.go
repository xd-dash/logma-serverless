package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewRouterHasNoRootRoute(t *testing.T) {
	r := NewRouter()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
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
