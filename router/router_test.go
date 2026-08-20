package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/xd-dash/logma-serverless/pubsub"
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

func TestBuildMountsProvidedRoutes(t *testing.T) {
	h := Build(func(r chi.Router) {
		r.Get("/ping", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a route mounted by register, got %d", rec.Code)
	}
}

func TestBuildLeavesUnregisteredPathsNotFound(t *testing.T) {
	h := Build(func(r chi.Router) {})

	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unregistered path, got %d", rec.Code)
	}
}

func TestBuildAttachesMiddlewareStack(t *testing.T) {
	var gotReqID string
	h := Build(func(r chi.Router) {
		r.Get("/ping", func(w http.ResponseWriter, req *http.Request) {
			gotReqID = middleware.GetReqID(req.Context())
			w.WriteHeader(http.StatusOK)
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotReqID == "" {
		t.Fatal("expected middleware.RequestID to inject a request ID into the context")
	}
}

func TestBuildRecoversFromPanic(t *testing.T) {
	h := Build(func(r chi.Router) {
		r.Get("/boom", func(w http.ResponseWriter, req *http.Request) {
			panic("kaboom")
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected middleware.Recoverer to turn a panic into 500, got %d", rec.Code)
	}
}

func TestRuntimeHolderReusesAfterSessionEnds(t *testing.T) {
	holder := pubsub.NewHolder(NewRuntime)

	first, ok := holder.Claim()
	if !ok {
		t.Fatal("expected first claim to succeed")
	}
	go first.Start(context.Background())

	if _, ok := holder.Claim(); ok {
		t.Fatal("expected second claim to fail while first session is active")
	}

	// Simulate the first session's request ending.
	first.Cancel()
	<-first.Done()

	third, ok := holder.Claim()
	if !ok {
		t.Fatal("expected claim to succeed once the prior session finished")
	}
	if third == first {
		t.Fatal("expected a fresh Runtime for the new session")
	}
	go third.Start(context.Background())
	third.Cancel()
	<-third.Done()
}
