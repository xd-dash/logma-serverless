// Package router implements a single-request, instance-local,
// bounded-lifetime Redis Pub/Sub runtime, exposed over SSE and hosted
// inside a Cloud Functions Gen 2 HTTP function pinned to
// maxInstanceRequestConcurrency=1. The HTTP request that starts the
// runtime owns its entire lifetime: it establishes control-plane
// subscriptions (control:add, control:shutdown), lets Redis hot-load
// additional subscriptions into the running container, fans every
// subscribed channel's messages out as one event stream, and shuts the
// runtime down (ending the request) on a control:shutdown publish or
// client disconnect.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	controlAddChannel      = "control:add"
	controlShutdownChannel = "control:shutdown"

	inputBufferSize = 64
	eventBufferSize = 64

	reconnectMinDelay     = 500 * time.Millisecond
	reconnectMaxDelay     = 30 * time.Second
	redisOperationTimeout = 10 * time.Second
)

const (
	runtimeStateIdle int32 = iota
	runtimeStateRunning
	runtimeStateDone
)

type runtimeMessage struct {
	channel string
	payload string
}

type subscriptionStopped struct {
	channel string
}

type subscription struct {
	channel string
	cancel  context.CancelFunc
}

// AddSubscription is the payload published to control:add to hot-load a
// new subscription into a running container.
type AddSubscription struct {
	Channel string `json:"channel"`
}

// ShutdownRequest is the payload published to control:shutdown to drain
// and terminate a running container.
type ShutdownRequest struct {
	Reason string `json:"reason"`
}

// PublishRequest is the payload delivered to a subscribed channel. If the
// message itself doesn't carry a channel, the Redis channel it arrived on
// is used instead.
type PublishRequest struct {
	Channel string          `json:"channel,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Runtime is a container-global, single-owner Redis Pub/Sub actor. Claim()
// is a defensive guard against a second request driving the same runtime
// concurrently; the actual guarantee comes from the Cloud Function's
// maxInstanceRequestConcurrency=1 configuration.
type Runtime struct {
	client *redis.Client

	ctx    context.Context
	cancel context.CancelFunc

	input  chan runtimeMessage
	events chan PublishRequest
	status chan subscriptionStopped
	done   chan struct{}

	state     atomic.Int32
	startOnce sync.Once
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect or start any subscriptions until Start is called.
func NewRuntime() *Runtime {
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URI"),
		Password: os.Getenv("REDISCLI_AUTH"),
		DB:       0,
	})

	ctx, cancel := context.WithCancel(context.Background())

	return &Runtime{
		client: client,
		ctx:    ctx,
		cancel: cancel,
		input:  make(chan runtimeMessage, inputBufferSize),
		events: make(chan PublishRequest, eventBufferSize),
		status: make(chan subscriptionStopped, inputBufferSize),
		done:   make(chan struct{}),
	}
}

// Claim attempts to take exclusive ownership of the runtime for the
// calling request. It returns false if the runtime has already been
// claimed (by a concurrent request, or a previous one that hasn't
// finished) or has already run to completion.
func (rt *Runtime) Claim() bool {
	return rt.state.CompareAndSwap(runtimeStateIdle, runtimeStateRunning)
}

// Events returns the channel of fanned-out Pub/Sub messages.
func (rt *Runtime) Events() <-chan PublishRequest {
	return rt.events
}

// Done returns a channel that's closed once Start has returned.
func (rt *Runtime) Done() <-chan struct{} {
	return rt.done
}

// Cancel ends the runtime's lifetime, causing Start to return. It's safe
// to call multiple times and from any goroutine.
func (rt *Runtime) Cancel() {
	rt.cancel()
}

// Start runs the runtime's actor loop until ctx is cancelled or a
// control:shutdown message is received. It blocks until the runtime
// stops, and must only be called once (guarded by startOnce -- a second
// call is a no-op that returns immediately).
func (rt *Runtime) Start(ctx context.Context) {
	rt.startOnce.Do(func() {
		defer rt.state.Store(runtimeStateDone)
		defer close(rt.done)

		go func() {
			select {
			case <-ctx.Done():
				rt.cancel()
			case <-rt.ctx.Done():
			}
		}()

		rt.run()
	})
}

func (rt *Runtime) run() {
	subscriptions := make(map[string]*subscription)

	startSubscription := func(channel string) error {
		return rt.startSubscription(channel, subscriptions)
	}

	stopSubscription := func(channel string) {
		sub, ok := subscriptions[channel]
		if !ok {
			return
		}
		delete(subscriptions, channel)
		sub.cancel()
	}

	stopAll := func() {
		for channel, sub := range subscriptions {
			delete(subscriptions, channel)
			sub.cancel()
		}
	}
	defer stopAll()

	// The control plane is mandatory and is established before the
	// runtime starts accepting normal subscription traffic.
	if err := startSubscription(controlAddChannel); err != nil {
		log.Printf("failed to initialize %q: %v", controlAddChannel, err)
		return
	}
	if err := startSubscription(controlShutdownChannel); err != nil {
		log.Printf("failed to initialize %q: %v", controlShutdownChannel, err)
		return
	}

	if err := rt.bootstrap(startSubscription); err != nil {
		log.Printf("subscription bootstrap failed: %v", err)
		return
	}

	log.Printf("Redis runtime started")

	for {
		select {
		case <-rt.ctx.Done():
			return

		case message := <-rt.input:
			switch message.channel {
			case controlAddChannel:
				rt.handleAdd(message.payload, startSubscription)
			case controlShutdownChannel:
				rt.handleShutdown(message.payload)
				return
			default:
				rt.handlePublish(message)
			}

		case stopped := <-rt.status:
			stopSubscription(stopped.channel)
			if rt.ctx.Err() != nil {
				return
			}

			// A non-control subscription that disappears is restarted.
			// The subscription worker itself handles Redis connection
			// recovery while it remains alive, but a terminal worker
			// failure is re-established here.
			if stopped.channel != controlAddChannel &&
				stopped.channel != controlShutdownChannel {
				if err := startSubscription(stopped.channel); err != nil {
					log.Printf("failed to restart subscription %q: %v", stopped.channel, err)
				}
			}
		}
	}
}

// bootstrap adds the channels listed in REDIS_DEFAULT_SUBSCRIPTIONS (a
// JSON array of channel names). An unset/empty value is a no-op --
// control:add and control:shutdown are the only structurally required
// subscriptions.
func (rt *Runtime) bootstrap(add func(string) error) error {
	raw := os.Getenv("REDIS_DEFAULT_SUBSCRIPTIONS")
	if raw == "" {
		return nil
	}

	var channels []string
	if err := json.Unmarshal([]byte(raw), &channels); err != nil {
		return fmt.Errorf("parse REDIS_DEFAULT_SUBSCRIPTIONS: %w", err)
	}

	for _, channel := range channels {
		if err := add(channel); err != nil {
			return err
		}
	}
	return nil
}

func (rt *Runtime) handleAdd(payload string, add func(string) error) {
	var request AddSubscription
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("invalid add subscription message: %v", err)
		return
	}

	if request.Channel == "" {
		log.Printf("add subscription contained empty channel")
		return
	}

	if err := add(request.Channel); err != nil {
		log.Printf("failed to add subscription %q: %v", request.Channel, err)
	}
}

func (rt *Runtime) handleShutdown(payload string) {
	var request ShutdownRequest
	if payload != "" {
		if err := json.Unmarshal([]byte(payload), &request); err != nil {
			log.Printf("invalid shutdown message: %v", err)
		}
	}
	log.Printf("shutting down runtime: reason=%q", request.Reason)
}

// handlePublish is the entire fanout primitive: every subscribed Redis
// channel's messages become an event on rt.events.
func (rt *Runtime) handlePublish(message runtimeMessage) {
	if message.payload == "" || message.payload == "{}" {
		return
	}

	var publish PublishRequest
	if err := json.Unmarshal([]byte(message.payload), &publish); err != nil {
		log.Printf("invalid Redis message on %q: %v", message.channel, err)
		return
	}

	if publish.Channel == "" {
		publish.Channel = message.channel
	}

	select {
	case rt.events <- publish:
	case <-rt.ctx.Done():
	}
}

func (rt *Runtime) startSubscription(channel string, subscriptions map[string]*subscription) error {
	if channel == "" {
		return errors.New("subscription channel is empty")
	}
	if _, exists := subscriptions[channel]; exists {
		return nil
	}

	ctx, cancel := context.WithCancel(rt.ctx)
	subscriptions[channel] = &subscription{
		channel: channel,
		cancel:  cancel,
	}

	go rt.subscriptionWorker(ctx, channel)
	return nil
}

func (rt *Runtime) subscriptionWorker(ctx context.Context, channel string) {
	defer func() {
		select {
		case rt.status <- subscriptionStopped{channel: channel}:
		case <-rt.ctx.Done():
		}
	}()

	delay := reconnectMinDelay
	for {
		if ctx.Err() != nil {
			return
		}

		pubsub := rt.client.Subscribe(ctx, channel)

		receiveCtx, cancel := context.WithTimeout(ctx, redisOperationTimeout)
		_, err := pubsub.Receive(receiveCtx)
		cancel()

		if err != nil {
			_ = pubsub.Close()
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
				_ = pubsub.Close()
				return
			case message, ok := <-pubsub.Channel():
				if !ok {
					_ = pubsub.Close()
					break receive
				}
				select {
				case rt.input <- runtimeMessage{
					channel: channel,
					payload: message.Payload,
				}:
				case <-ctx.Done():
					_ = pubsub.Close()
					return
				}
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
