package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xd-dash/logma-serverless/pubsub"
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

func TestDefaultSubscriptionsFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want []string
	}{
		{name: "unset", env: "", want: nil},
		{name: "valid JSON array", env: `["stonks:control:add:global","stonks:control:shutdown:global"]`, want: []string{"stonks:control:add:global", "stonks:control:shutdown:global"}},
		{name: "empty JSON array", env: `[]`, want: nil},
		{name: "invalid JSON falls back to nil", env: `not json`, want: nil},
		{name: "JSON object (not an array) falls back to nil", env: `{"a":"b"}`, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REDIS_DEFAULT_SUBSCRIPTIONS", tt.env)

			got := defaultSubscriptionsFromEnv()
			if len(got) != len(tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("expected %v, got %v", tt.want, got)
				}
			}
		})
	}
}

func TestNewRuntimePicksUpDefaultChannelsFromEnv(t *testing.T) {
	t.Setenv("REDIS_DEFAULT_SUBSCRIPTIONS", `["stonks:control:add:global"]`)

	rt := NewRuntime()
	defer rt.Cancel()

	if len(rt.defaultChannels) != 1 || rt.defaultChannels[0] != "stonks:control:add:global" {
		t.Fatalf("expected defaultChannels to be [stonks:control:add:global], got %v", rt.defaultChannels)
	}
}

func TestRuntimeSubscribeSetsChannels(t *testing.T) {
	rt := NewRuntime()
	defer rt.Cancel()

	rt.Subscribe([]string{"a", "b"})

	if len(rt.channels) != 2 || rt.channels[0] != "a" || rt.channels[1] != "b" {
		t.Fatalf("expected Subscribe to set channels to [a b], got %v", rt.channels)
	}
}

func TestRuntimeRecordInvocationFallsBackToHeaderWhenRedisAuthUnset(t *testing.T) {
	t.Setenv("REDIS_URI", "env-host:6379")
	t.Setenv("REDISCLI_AUTH", "")

	rt := NewRuntime()
	defer rt.Cancel()

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set(pubsub.HeaderRedisAuth, "header-secret")

	rt.RecordInvocation(req, "req-1")

	if got := rt.Client.Options().Password; got != "header-secret" {
		t.Fatalf("expected RecordInvocation to pick up the header auth, got %q", got)
	}
}

func TestRuntimeRecordInvocationPrefersEnvRedisAuthOverHeader(t *testing.T) {
	t.Setenv("REDIS_URI", "env-host:6379")
	t.Setenv("REDISCLI_AUTH", "env-secret")

	rt := NewRuntime()
	defer rt.Cancel()

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set(pubsub.HeaderRedisAuth, "header-secret")

	rt.RecordInvocation(req, "req-1")

	if got := rt.Client.Options().Password; got != "env-secret" {
		t.Fatalf("expected env auth to take precedence, got %q", got)
	}
}

func TestRuntimeRecordInvocationFallsBackToHeaderWhenRedisURIUnset(t *testing.T) {
	t.Setenv("REDIS_URI", "")
	t.Setenv("REDISCLI_AUTH", "env-secret")

	rt := NewRuntime()
	defer rt.Cancel()

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set(pubsub.HeaderRedisURI, "header-host:6379")

	rt.RecordInvocation(req, "req-1")

	if got := rt.Client.Options().Addr; got != "header-host:6379" {
		t.Fatalf("expected RecordInvocation to pick up the header URI, got %q", got)
	}
}

func TestRuntimeRecordInvocationPrefersEnvRedisURIOverHeader(t *testing.T) {
	t.Setenv("REDIS_URI", "env-host:6379")
	t.Setenv("REDISCLI_AUTH", "env-secret")

	rt := NewRuntime()
	defer rt.Cancel()

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set(pubsub.HeaderRedisURI, "header-host:6379")

	rt.RecordInvocation(req, "req-1")

	if got := rt.Client.Options().Addr; got != "env-host:6379" {
		t.Fatalf("expected env URI to take precedence, got %q", got)
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
