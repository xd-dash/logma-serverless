package pubsub

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	reconnectMinDelay     = 500 * time.Millisecond
	reconnectMaxDelay     = 30 * time.Second
	redisOperationTimeout = 10 * time.Second
)

// Subscriber runs a reconnecting Redis Pub/Sub subscription for a single
// channel until its context is cancelled, invoking onMessage for every
// message received. Redis connection failures are retried with
// exponential backoff (reconnectMinDelay..reconnectMaxDelay); the
// subscription itself is not retried once ctx is done.
//
// onMessage is called synchronously from the subscriber's goroutine, so
// a slow or blocking onMessage stalls delivery of further messages on
// this channel. Callers that need to keep the subscriber responsive to
// ctx cancellation while forwarding into their own (possibly full)
// channel should make onMessage select on ctx.Done() alongside the send.
type Subscriber struct {
	stopped chan struct{}
}

// Subscribe starts the subscription worker in its own goroutine and
// returns immediately.
func Subscribe(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) *Subscriber {
	s := &Subscriber{stopped: make(chan struct{})}
	go s.run(ctx, client, channel, onMessage)
	return s
}

// Stopped returns a channel that's closed once the subscription worker
// has returned (ctx cancelled).
func (s *Subscriber) Stopped() <-chan struct{} {
	return s.stopped
}

func (s *Subscriber) run(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) {
	defer close(s.stopped)

	delay := reconnectMinDelay
	for {
		if ctx.Err() != nil {
			return
		}

		ps := client.Subscribe(ctx, channel)

		receiveCtx, cancel := context.WithTimeout(ctx, redisOperationTimeout)
		_, err := ps.Receive(receiveCtx)
		cancel()

		if err != nil {
			_ = ps.Close()
			if !sleepContext(ctx, delay) {
				return
			}
			delay *= 2
			if delay > reconnectMaxDelay {
				delay = reconnectMaxDelay
			}
			continue
		}
		delay = reconnectMinDelay

	receive:
		for {
			select {
			case <-ctx.Done():
				_ = ps.Close()
				return
			case message, ok := <-ps.Channel():
				if !ok {
					_ = ps.Close()
					break receive
				}
				onMessage(message.Payload)
			}
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
