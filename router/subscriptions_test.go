package router

import (
	"testing"

	"github.com/xd-dash/logma-serverless/internal/addsub"
	"github.com/xd-dash/logma-serverless/internal/shutdown"
	"github.com/xd-dash/logma-serverless/pubsub"
)

func TestSubscriptionsIncludeAddAndShutdown(t *testing.T) {
	addHandle, ok := Subscriptions[addsub.Channel]
	if !ok {
		t.Fatalf("expected Subscriptions to include %q", addsub.Channel)
	}
	shutdownHandle, ok := Subscriptions[shutdown.Channel]
	if !ok {
		t.Fatalf("expected Subscriptions to include %q", shutdown.Channel)
	}

	var addedChannel string
	addHandle(nil, `{"channel":"dev:global:logs:3"}`, func(channel string) error {
		addedChannel = channel
		return nil
	})
	if addedChannel != "dev:global:logs:3" {
		t.Fatalf("expected the control:add Handle to delegate to addsub.Handle, got %q", addedChannel)
	}

	session := pubsub.NewSession()
	shutdownHandle(&session, `{"reason":"test"}`, nil)
	select {
	case <-session.Context().Done():
	default:
		t.Fatal("expected the control:shutdown Handle to delegate to shutdown.Handle")
	}
}

func TestRegisterChannelAddsSubscription(t *testing.T) {
	const testChannel = "test:channel"
	defer delete(Subscriptions, testChannel)

	var called bool
	RegisterChannel(testChannel, func(session *pubsub.Session, payload string, add func(string) error) {
		called = true
	})

	handle, ok := Subscriptions[testChannel]
	if !ok {
		t.Fatalf("expected RegisterChannel to add %q to Subscriptions", testChannel)
	}
	handle(nil, "payload", func(string) error { return nil })
	if !called {
		t.Fatal("expected the registered Handle to be called")
	}
}
