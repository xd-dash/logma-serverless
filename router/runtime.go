// Package router implements a single-request, instance-local,
// bounded-lifetime Redis Pub/Sub runtime, exposed over SSE and hosted
// inside a Cloud Functions Gen 2 HTTP function pinned to
// maxInstanceRequestConcurrency=1. The HTTP request that starts the
// runtime owns its entire lifetime: it establishes control-plane
// subscriptions (control:add, control:shutdown) on both this
// container's own instance channel (via pubsub.ControlPlane, embedded
// below) and the shared global channel that reaches every container, so
// a control message can target one specific container or all of them,
// lets Redis hot-load additional subscriptions into the running
// container, fans every subscribed channel's messages out as one event
// stream, and shuts the runtime down (ending the request) on a
// control:shutdown publish or client disconnect.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/xd-dash/logma-serverless/pubsub"
)

const (
	controlAddChannel      = "control:add"
	controlShutdownChannel = "control:shutdown"

	inputBufferSize = 64
	eventBufferSize = 64
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
	pubsub.ControlPlane
	pubsub.Session

	input  chan runtimeMessage
	events chan PublishRequest
	status chan subscriptionStopped

	invocation pubsub.InvocationInfo
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect or start any subscriptions until Start is called.
func NewRuntime() *Runtime {
	return &Runtime{
		ControlPlane: pubsub.NewControlPlane(pubsub.NewClientFromEnv()),
		Session:      pubsub.NewSession(),
		input:        make(chan runtimeMessage, inputBufferSize),
		events:       make(chan PublishRequest, eventBufferSize),
		status:       make(chan subscriptionStopped, inputBufferSize),
	}
}

// RecordInvocation captures which Cloud Function instance and HTTP
// request are driving this Runtime. It must be called after a
// successful Claim and before Start -- Start records it in Redis as the
// first thing it does, strictly before the client's first Subscribe.
func (rt *Runtime) RecordInvocation(r *http.Request, requestID string) {
	rt.invocation = pubsub.InvocationInfoFromRequest(r, requestID)
}

// Events returns the channel of fanned-out Pub/Sub messages.
func (rt *Runtime) Events() <-chan PublishRequest {
	return rt.events
}

// Start runs the runtime's actor loop until ctx is cancelled or a
// control:shutdown message is received. It blocks until the runtime
// stops, and must only be called once -- a second call is a no-op that
// returns immediately (guarded by the embedded Session.Begin, which
// also provides Claim/Done/Cancel).
func (rt *Runtime) Start(ctx context.Context) {
	rt.Begin(ctx, rt.run)
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

	// The instance-scoped names of the control channels: publishing to
	// these reaches only this container. Each also has a global
	// (broadcast) variant -- see the relays below -- for reaching every
	// listening container with a single publish.
	instanceAddChannel := rt.InstanceChannel(controlAddChannel)
	instanceShutdownChannel := rt.InstanceChannel(controlShutdownChannel)

	relayCtx, cancelRelays := context.WithCancel(rt.Context())
	addRelay := rt.Relay(relayCtx, controlAddChannel)
	shutdownRelay := rt.Relay(relayCtx, controlShutdownChannel)
	defer func() {
		cancelRelays()
		<-addRelay.Stopped()
		<-shutdownRelay.Stopped()
	}()

	// Recorded via plain Redis commands before the client issues its
	// first Subscribe below -- once it does, this client is never used
	// for anything but Publish/Subscribe again.
	if err := pubsub.RegisterInvocation(rt.Context(), rt.Client, rt.invocation); err != nil {
		log.Printf("failed to record invocation info: %v", err)
	}

	// The control plane is mandatory and is established before the
	// runtime starts accepting normal subscription traffic.
	if err := startSubscription(instanceAddChannel); err != nil {
		log.Printf("failed to initialize %q: %v", instanceAddChannel, err)
		return
	}
	if err := startSubscription(instanceShutdownChannel); err != nil {
		log.Printf("failed to initialize %q: %v", instanceShutdownChannel, err)
		return
	}

	if err := rt.bootstrap(startSubscription); err != nil {
		log.Printf("subscription bootstrap failed: %v", err)
		return
	}

	log.Printf("Redis runtime started (instance=%s)", rt.InstanceID)

	for {
		select {
		case <-rt.Context().Done():
			return

		case message := <-rt.input:
			switch message.channel {
			case instanceAddChannel:
				rt.handleAdd(message.payload, startSubscription)
			case instanceShutdownChannel:
				rt.handleShutdown(message.payload)
				return
			default:
				rt.handlePublish(message)
			}

		case stopped := <-rt.status:
			stopSubscription(stopped.channel)
			if rt.Context().Err() != nil {
				return
			}

			// A non-control subscription that disappears is restarted.
			// The subscription worker itself handles Redis connection
			// recovery while it remains alive, but a terminal worker
			// failure is re-established here.
			if stopped.channel != instanceAddChannel &&
				stopped.channel != instanceShutdownChannel {
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
	request := pubsub.ParseShutdownRequest(payload)
	log.Printf("shutting down runtime: reason=%q", request.Reason)
}

// handlePublish is the entire fanout primitive: every subscribed Redis
// channel's messages become an event on rt.events. The send is
// non-blocking: rt.run()'s actor loop is single-threaded, so a blocking
// send here would stall it on a full events channel -- and POST /run
// never drains Events() at all, so that channel filling up is a normal
// case, not an edge case. A stalled actor loop can't process
// control:shutdown either, so it would also break the one thing meant to
// end the runtime. Instead, mirror the drop-and-log-when-full pattern
// logma's callbackDispatcher.dispatch uses for the same reason
// (internal/routes/routes.go): a slow or absent consumer loses the
// oldest-pending events rather than wedging the whole runtime.
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
	default:
		log.Printf("events channel full; dropping message on %q", publish.Channel)
	}
}

func (rt *Runtime) startSubscription(channel string, subscriptions map[string]*subscription) error {
	if channel == "" {
		return errors.New("subscription channel is empty")
	}
	if _, exists := subscriptions[channel]; exists {
		return nil
	}

	ctx, cancel := context.WithCancel(rt.Context())
	subscriptions[channel] = &subscription{
		channel: channel,
		cancel:  cancel,
	}

	sub := pubsub.Subscribe(ctx, rt.Client, channel, func(payload string) {
		select {
		case rt.input <- runtimeMessage{channel: channel, payload: payload}:
		case <-ctx.Done():
		}
	})

	go func() {
		<-sub.Stopped()
		select {
		case rt.status <- subscriptionStopped{channel: channel}:
		case <-rt.Context().Done():
		}
	}()

	return nil
}
