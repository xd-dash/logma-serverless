// Package router implements a single-request, instance-local,
// bounded-lifetime Redis Pub/Sub runtime, exposed over SSE and hosted
// inside a Cloud Functions Gen 2 HTTP function pinned to
// maxInstanceRequestConcurrency=1. The HTTP request that starts the
// runtime owns its entire lifetime: it establishes control-plane
// subscriptions (control:add, control:shutdown) on both this
// container's own instance channel (via pubsub.ControlPlane, embedded
// below) and the shared global channel that reaches every container, so
// a control message can target one specific container or all of them,
// additionally subscribes to whatever literal channels
// REDIS_DEFAULT_SUBSCRIPTIONS names (see defaultSubscriptionsFromEnv) and
// whatever the claiming request itself asks for via ?channel=, lets Redis
// hot-load further subscriptions into the running container, fans every
// subscribed channel's messages out as one event stream, and shuts the
// runtime down (ending the request) on a control:shutdown publish or
// client disconnect.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/xd-dash/logma-serverless/pubsub"
)

const (
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

	// channels are additional, literal Redis channel names this Runtime
	// subscribes to on start, beyond the default Subscriptions registry
	// -- set via Subscribe, populated from the claiming request's
	// ?channel= query parameters. Unlike Subscriptions entries they get
	// no dedicated Handle: any message on them falls through to
	// handlePublish, the same fanout a control:add hot-load gets.
	channels []string

	// defaultChannels are literal Redis channel names, resolved once at
	// construction from REDIS_DEFAULT_SUBSCRIPTIONS, that every Runtime
	// this container creates subscribes to on start -- regardless of
	// what (if anything) the claiming request itself passes via
	// Subscribe. This is how a logma-serverless deployment gets wired to
	// a companion producer's well-known channels (e.g. a stonks
	// instance's control:add broadcast) without every caller needing to
	// know and repeat them via ?channel=. Like channels above, entries
	// here get no dedicated Handle and fall through to handlePublish.
	defaultChannels []string
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect or start any subscriptions until Start is called.
func NewRuntime() *Runtime {
	return &Runtime{
		ControlPlane:    pubsub.NewControlPlane(pubsub.NewClientFromEnv()),
		Session:         pubsub.NewSession(),
		input:           make(chan runtimeMessage, inputBufferSize),
		events:          make(chan PublishRequest, eventBufferSize),
		status:          make(chan subscriptionStopped, inputBufferSize),
		defaultChannels: defaultSubscriptionsFromEnv(),
	}
}

// defaultSubscriptionsFromEnv parses REDIS_DEFAULT_SUBSCRIPTIONS as a JSON
// array of literal Redis channel names, e.g. ["stonks:control:add:global"].
// An unset or empty env var returns nil (no defaults); invalid JSON logs a
// warning and also returns nil rather than failing construction -- a
// malformed deploy-time setting shouldn't take down every Runtime this
// container creates.
func defaultSubscriptionsFromEnv() []string {
	raw := os.Getenv("REDIS_DEFAULT_SUBSCRIPTIONS")
	if raw == "" {
		return nil
	}

	var channels []string
	if err := json.Unmarshal([]byte(raw), &channels); err != nil {
		log.Printf("invalid REDIS_DEFAULT_SUBSCRIPTIONS (must be a JSON array of channel name strings): %v", err)
		return nil
	}
	return channels
}

// RecordInvocation captures which Cloud Function instance and HTTP
// request are driving this Runtime. It must be called after a
// successful Claim and before Start -- Start records it in Redis as the
// first thing it does, strictly before the client's first Subscribe.
//
// This is also where a deployment that left REDIS_URI and/or
// REDISCLI_AUTH unset gets one more chance to connect: r's
// X-Redis-Uri/X-Rediscli-Auth headers, if present, replace the Client
// NewRuntime built at construction time (see pubsub.NewClientFromRequest).
// Safe to replace here specifically because nothing has used Client yet
// -- go-redis doesn't connect until the first command, and
// RecordInvocation always runs before Start. Unlike pubsub.Runtime, this
// Runtime has only one constructor (NewRuntime, always built from env),
// so there's no explicitly-supplied-client case to avoid clobbering.
func (rt *Runtime) RecordInvocation(r *http.Request, requestID string) {
	rt.invocation = pubsub.InvocationInfoFromRequest(r, requestID)
	if os.Getenv("REDIS_URI") == "" || os.Getenv("REDISCLI_AUTH") == "" {
		rt.Client = pubsub.NewClientFromRequest(r)
	}
}

// Subscribe adds channel names this Runtime establishes on start, in
// addition to the default Subscriptions registry. It must be called
// after a successful Claim and before Start.
func (rt *Runtime) Subscribe(channels []string) {
	rt.channels = channels
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

	// Recorded via plain Redis commands before the client issues its
	// first Subscribe below -- once it does, this client is never used
	// for anything but Publish/Subscribe again.
	if err := pubsub.RegisterInvocation(rt.Context(), rt.Client, rt.invocation); err != nil {
		log.Printf("failed to record invocation info: %v", err)
	}

	relayCtx, cancelRelays := context.WithCancel(rt.Context())
	var relays []*pubsub.Subscriber
	defer func() {
		cancelRelays()
		for _, relay := range relays {
			<-relay.Stopped()
		}
	}()

	// Subscriptions (subscriptions.go) maps every channel this Runtime
	// establishes on start to the Handle that processes it --
	// control:add and control:shutdown are registered by default, and
	// RegisterChannel can add more. Each gets an instance-scoped
	// subscription, a global-channel relay, and its Handle wired into
	// the dispatch table below.
	handlers := make(map[string]Handle, len(Subscriptions))
	for base, handle := range Subscriptions {
		instanceChannel := rt.InstanceChannel(base)
		handlers[instanceChannel] = handle

		relays = append(relays, rt.Relay(relayCtx, base))
		if err := startSubscription(instanceChannel); err != nil {
			log.Printf("failed to initialize %q: %v", instanceChannel, err)
			return
		}
	}

	// rt.defaultChannels (REDIS_DEFAULT_SUBSCRIPTIONS, resolved once at
	// construction) and rt.channels (literal channel names the claiming
	// request asked for via Subscribe/?channel=) are both outside the
	// Subscriptions registry above -- unlike those, a failure here
	// doesn't abort startup: a bad default or user-supplied channel
	// shouldn't take down the whole session. startSubscription no-ops on
	// a channel already subscribed, so a name appearing in both slices
	// is harmless.
	for _, channel := range rt.defaultChannels {
		if err := startSubscription(channel); err != nil {
			log.Printf("failed to subscribe to default channel %q: %v", channel, err)
		}
	}
	for _, channel := range rt.channels {
		if err := startSubscription(channel); err != nil {
			log.Printf("failed to subscribe to requested channel %q: %v", channel, err)
		}
	}

	log.Printf("Redis runtime started (instance=%s)", rt.InstanceID)

	for {
		select {
		case <-rt.Context().Done():
			return

		case message := <-rt.input:
			if handle, ok := handlers[message.channel]; ok {
				handle(&rt.Session, message.payload, startSubscription)
			} else {
				rt.handlePublish(message)
			}

		case stopped := <-rt.status:
			stopSubscription(stopped.channel)
			if rt.Context().Err() != nil {
				return
			}

			// A subscription outside the registered set that
			// disappears is restarted. The subscription worker itself
			// handles Redis connection recovery while it remains
			// alive, but a terminal worker failure is re-established
			// here.
			if _, registered := handlers[stopped.channel]; !registered {
				if err := startSubscription(stopped.channel); err != nil {
					log.Printf("failed to restart subscription %q: %v", stopped.channel, err)
				}
			}
		}
	}
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
