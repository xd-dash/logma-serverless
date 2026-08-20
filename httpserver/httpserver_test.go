package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
)

func TestNewAttachesMiddlewareStack(t *testing.T) {
	r := New()
	var gotReqID string
	r.Get("/ping", func(w http.ResponseWriter, req *http.Request) {
		gotReqID = middleware.GetReqID(req.Context())
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotReqID == "" {
		t.Fatal("expected middleware.RequestID to inject a request ID into the context")
	}
}

func TestNewRecoversFromPanic(t *testing.T) {
	r := New()
	r.Get("/boom", func(w http.ResponseWriter, req *http.Request) {
		panic("kaboom")
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected middleware.Recoverer to turn a panic into 500, got %d", rec.Code)
	}
}

func TestNewHasNoRoutesRegistered(t *testing.T) {
	r := New()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 with no routes registered, got %d", rec.Code)
	}
}
