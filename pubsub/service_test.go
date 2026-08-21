package pubsub

import (
	"context"
	"errors"
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
