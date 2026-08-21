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

func TestHandleAdd(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

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

func TestHandleShutdown(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

	rt.handleShutdown(`{"reason":"maintenance"}`)

	select {
	case <-rt.Context().Done():
	default:
		t.Fatal("expected handleShutdown to cancel the runtime")
	}
}

func TestDefaultSubscriptionsIncludeAddAndShutdown(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

	if len(Subscriptions) < 2 {
		t.Fatalf("expected at least 2 default subscriptions, got %d", len(Subscriptions))
	}

	addSub, shutdownSub := Subscriptions[0], Subscriptions[1]

	if got, want := addSub.Channel(rt), rt.AddChannel(); got != want {
		t.Fatalf("expected the first default subscription's channel to be AddChannel (%q), got %q", want, got)
	}
	if got, want := shutdownSub.Channel(rt), rt.ShutdownChannel(); got != want {
		t.Fatalf("expected the second default subscription's channel to be ShutdownChannel (%q), got %q", want, got)
	}

	var addedChannel string
	addSub.Handle(rt, `{"channel":"dev:global:logs:3"}`, func(channel string) error {
		addedChannel = channel
		return nil
	})
	if addedChannel != "dev:global:logs:3" {
		t.Fatalf("expected the add subscription's Handle to delegate to handleAdd, got %q", addedChannel)
	}

	shutdownSub.Handle(rt, `{"reason":"test"}`, nil)
	select {
	case <-rt.Context().Done():
	default:
		t.Fatal("expected the shutdown subscription's Handle to delegate to handleShutdown")
	}
}

func TestRegisterAppendsSubscription(t *testing.T) {
	orig := Subscriptions
	defer func() { Subscriptions = orig }()
	before := len(Subscriptions)

	var called bool
	Register(
		func(rt *Runtime) string { return "test:channel" },
		func(rt *Runtime, payload string, add func(string) error) { called = true },
	)

	if len(Subscriptions) != before+1 {
		t.Fatalf("expected Register to append one subscription, got %d (was %d)", len(Subscriptions), before)
	}

	added := Subscriptions[len(Subscriptions)-1]
	if got := added.Channel(&Runtime{}); got != "test:channel" {
		t.Fatalf("expected the registered channel resolver to return %q, got %q", "test:channel", got)
	}
	added.Handle(&Runtime{}, "payload", func(string) error { return nil })
	if !called {
		t.Fatal("expected the registered Handle to be called")
	}
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
