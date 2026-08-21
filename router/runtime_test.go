package router

import (
	"testing"
	"time"
)

func TestHandlePublish(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

	t.Run("empty payload is ignored", func(t *testing.T) {
		rt.handlePublish(runtimeMessage{channel: "chan", payload: ""})
		select {
		case ev := <-rt.events:
			t.Fatalf("expected no event, got %+v", ev)
		default:
		}
	})

	t.Run("empty object payload is ignored", func(t *testing.T) {
		rt.handlePublish(runtimeMessage{channel: "chan", payload: "{}"})
		select {
		case ev := <-rt.events:
			t.Fatalf("expected no event, got %+v", ev)
		default:
		}
	})

	t.Run("invalid JSON is dropped", func(t *testing.T) {
		rt.handlePublish(runtimeMessage{channel: "chan", payload: "not json"})
		select {
		case ev := <-rt.events:
			t.Fatalf("expected no event, got %+v", ev)
		default:
		}
	})

	t.Run("channel defaults to the Redis channel it arrived on", func(t *testing.T) {
		rt.handlePublish(runtimeMessage{channel: "dev:global:logs:1", payload: `{"data":{"x":1}}`})
		select {
		case ev := <-rt.events:
			if ev.Channel != "dev:global:logs:1" {
				t.Fatalf("expected channel to default to dev:global:logs:1, got %q", ev.Channel)
			}
		case <-time.After(time.Second):
			t.Fatal("expected an event")
		}
	})

	t.Run("explicit channel in payload wins", func(t *testing.T) {
		rt.handlePublish(runtimeMessage{channel: "dev:global:logs:1", payload: `{"channel":"override"}`})
		select {
		case ev := <-rt.events:
			if ev.Channel != "override" {
				t.Fatalf("expected channel to be override, got %q", ev.Channel)
			}
		case <-time.After(time.Second):
			t.Fatal("expected an event")
		}
	})
}

func TestClaim(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

	if !rt.Claim() {
		t.Fatal("first Claim should succeed")
	}
	if rt.Claim() {
		t.Fatal("second Claim should fail while runtime is running")
	}
}
