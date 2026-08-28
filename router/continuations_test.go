package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestContinuationHandlerPublishesWrappedEvent(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`{"version":"v1","id":"run-1","channel":"chatgpt:continuation:bootstrap-abc","status":"success","issued_at":%q,"chat_url":"https://chatgpt.com/c/abc","prompt":"continue"}`, now)

	var channel string
	var payload []byte
	handler := continuationHandler(func(_ context.Context, _ *http.Request, gotChannel string, gotPayload []byte) error {
		channel, payload = gotChannel, gotPayload
		return nil
	})
	req := httptest.NewRequest(http.MethodPost, "/continuations", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if channel != "chatgpt:continuation:bootstrap-abc" {
		t.Fatalf("unexpected channel %q", channel)
	}
	if !strings.Contains(string(payload), `"channel":"chatgpt:continuation:bootstrap-abc"`) || !strings.Contains(string(payload), `"id":"run-1"`) {
		t.Fatalf("event was not wrapped as PublishRequest: %s", payload)
	}
}

func TestContinuationHandlerRejectsInvalidEventBeforePublish(t *testing.T) {
	called := false
	handler := continuationHandler(func(context.Context, *http.Request, string, []byte) error {
		called = true
		return nil
	})
	req := httptest.NewRequest(http.MethodPost, "/continuations", strings.NewReader(`{"version":"v1"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if called {
		t.Fatal("invalid event reached publisher")
	}
}
