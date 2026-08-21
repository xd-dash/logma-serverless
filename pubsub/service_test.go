package pubsub

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// unreachableClient builds a *redis.Client pointed at an address that
// refuses connections immediately, so Subscribe's workers fail fast and
// retry, without this test needing a live Redis server.
func unreachableClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
}

func TestRunReturnsWorksErrorPromptly(t *testing.T) {
	cp := NewControlPlane(unreachableClient())
	wantErr := errors.New("boom")

	done := make(chan error, 1)
	go func() {
		done <- cp.Run(context.Background(), ServiceSpec{
			Channels: ChannelHandlers{"pubsub-test:run": func(string) {}},
			Work:     func(ctx context.Context) error { return wantErr },
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected Run to return Work's error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		// This is exactly the deadlock a wrong cancel/teardown defer
		// ordering produces: teardown() blocks forever waiting for
		// subscribers that only stop once the context Work ran under is
		// cancelled.
		t.Fatal("Run did not return -- possible cancel/teardown ordering deadlock")
	}
}

func TestRuntimeSatisfiesLifecycle(t *testing.T) {
	var _ Lifecycle = (*Runtime)(nil)
}

func TestRuntimeStartRunsConfiguredWork(t *testing.T) {
	sr := NewRuntime(unreachableClient())
	if !sr.Claim() {
		t.Fatal("expected first claim to succeed")
	}

	called := make(chan struct{})
	sr.Configure(ServiceSpec{
		Work: func(ctx context.Context) error {
			close(called)
			<-ctx.Done()
			return nil
		},
	})

	go sr.Start(context.Background())

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Start to run the configured Work")
	}

	sr.Cancel()
	select {
	case <-sr.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expected Cancel to unblock Work and close Done")
	}
}

func TestRuntimeRecordInvocationFillsSpecAtStart(t *testing.T) {
	sr := NewRuntime(unreachableClient())
	if !sr.Claim() {
		t.Fatal("expected first claim to succeed")
	}

	req := httptest.NewRequest("POST", "/stream", nil)
	sr.RecordInvocation(req, "req-1")

	var gotRequestID string
	sr.Configure(ServiceSpec{
		Work: func(ctx context.Context) error {
			gotRequestID = sr.invocation.RequestID
			return nil
		},
	})

	sr.Start(context.Background())

	if gotRequestID != "req-1" {
		t.Fatalf("expected Start to have recorded invocation before Work ran, got RequestID %q", gotRequestID)
	}
}

func TestRuntimeDefaultShutdownHandlerCancelsSession(t *testing.T) {
	sr := NewRuntime(unreachableClient())

	handler := sr.DefaultShutdownHandler()
	handler(`{"reason":"maintenance"}`)

	select {
	case <-sr.Context().Done():
	default:
		t.Fatal("expected DefaultShutdownHandler to cancel the Runtime's context")
	}
}

func TestRuntimeDefaultShutdownHandlerAcceptsEmptyPayload(t *testing.T) {
	sr := NewRuntime(unreachableClient())

	handler := sr.DefaultShutdownHandler()
	handler("")

	select {
	case <-sr.Context().Done():
	default:
		t.Fatal("expected DefaultShutdownHandler to cancel the Runtime's context even with no reason given")
	}
}

func TestRunStopsChannelsWhenExternalContextIsCancelled(t *testing.T) {
	cp := NewControlPlane(unreachableClient())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- cp.Run(ctx, ServiceSpec{
			Channels: ChannelHandlers{"pubsub-test:run-external": func(string) {}},
			Work: func(workCtx context.Context) error {
				<-workCtx.Done()
				return workCtx.Err()
			},
		})
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Run to return once the external context was cancelled")
	}
}
