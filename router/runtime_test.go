package router

import (
	"os"
	"testing"
	"time"
)

func TestHandlePublish(t *testing.T) {
	rt := NewRuntime()
	defer rt.cancel()

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

func TestHandleAdd(t *testing.T) {
	rt := NewRuntime()
	defer rt.cancel()

	t.Run("empty channel is rejected", func(t *testing.T) {
		called := false
		rt.handleAdd(`{"channel":""}`, func(string) error {
			called = true
			return nil
		})
		if called {
			t.Fatal("add should not be called for an empty channel")
		}
	})

	t.Run("invalid JSON is rejected", func(t *testing.T) {
		called := false
		rt.handleAdd("not json", func(string) error {
			called = true
			return nil
		})
		if called {
			t.Fatal("add should not be called for invalid JSON")
		}
	})

	t.Run("valid channel is added", func(t *testing.T) {
		var got string
		rt.handleAdd(`{"channel":"dev:global:logs:2"}`, func(channel string) error {
			got = channel
			return nil
		})
		if got != "dev:global:logs:2" {
			t.Fatalf("expected dev:global:logs:2, got %q", got)
		}
	})
}

func TestBootstrap(t *testing.T) {
	rt := NewRuntime()
	defer rt.cancel()

	t.Run("unset env var is a no-op", func(t *testing.T) {
		os.Unsetenv("REDIS_DEFAULT_SUBSCRIPTIONS")
		if err := rt.bootstrap(func(string) error { return nil }); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("adds every listed channel", func(t *testing.T) {
		os.Setenv("REDIS_DEFAULT_SUBSCRIPTIONS", `["a","b","c"]`)
		defer os.Unsetenv("REDIS_DEFAULT_SUBSCRIPTIONS")

		var added []string
		err := rt.bootstrap(func(channel string) error {
			added = append(added, channel)
			return nil
		})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(added) != 3 || added[0] != "a" || added[1] != "b" || added[2] != "c" {
			t.Fatalf("unexpected channels added: %v", added)
		}
	})

	t.Run("invalid JSON errors", func(t *testing.T) {
		os.Setenv("REDIS_DEFAULT_SUBSCRIPTIONS", "not json")
		defer os.Unsetenv("REDIS_DEFAULT_SUBSCRIPTIONS")

		if err := rt.bootstrap(func(string) error { return nil }); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestClaim(t *testing.T) {
	rt := NewRuntime()
	defer rt.cancel()

	if !rt.Claim() {
		t.Fatal("first Claim should succeed")
	}
	if rt.Claim() {
		t.Fatal("second Claim should fail while runtime is running")
	}
}
