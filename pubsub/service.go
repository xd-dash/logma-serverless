package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
)

// ChannelHandlers maps base control-channel names to the function that
// handles messages arriving on them -- whether published directly to
// this container's instance channel, or relayed in from the channel's
// global (broadcast) variant.
type ChannelHandlers map[string]func(payload string)

// SubscribeAll subscribes every entry in handlers via Subscribe
// (instance channel + global relay, each) and returns a single teardown
// func that waits for all of them to stop. It's for callers with a
// small, static, known-up-front set of control channels; a caller that
// needs to add channels at runtime (logma-serverless's own
// router.Runtime, which hot-loads arbitrary channels via control:add)
// wires InstanceChannel/GlobalChannel/Relay itself instead, since a
// message arriving on one of those must stay serialized with its own
// dispatch loop rather than invoking a handler concurrently from this
// method's caller.
func (cp ControlPlane) SubscribeAll(ctx context.Context, handlers ChannelHandlers) (teardown func()) {
	subs := make([]*Subscriber, 0, len(handlers)*2)
	for baseChannel, onMessage := range handlers {
		instance, relay := cp.Subscribe(ctx, baseChannel, onMessage)
		subs = append(subs, instance, relay)
	}
	return func() {
		for _, s := range subs {
			<-s.Stopped()
		}
	}
}

// ServiceSpec is the declarative shape a container-scoped, Redis-backed
// service supplies to Run: which control channels it listens on and how,
// and the work it actually does once wired up.
type ServiceSpec struct {
	// Invocation is recorded in Redis before the first Subscribe, per
	// RegisterInvocation's ordering requirement.
	Invocation InvocationInfo
	// Channels is handed to SubscribeAll: every base channel name here
	// gets an instance-scoped subscription and a global-channel relay
	// onto it.
	Channels ChannelHandlers
	// Work is the service's actual job. It's called with a context that
	// ends when Run's own ctx does, or when a channel handler decides
	// to end the service early (typically by calling Cancel on the
	// Session Run's ControlPlane is embedded alongside). Work should
	// return once that context is done; a non-nil error is treated as
	// the reason the service stopped and returned as-is by Run.
	Work func(ctx context.Context) error
}

// Run is the shared orchestration every container-scoped, Redis-backed,
// control-plane-driven service follows: record invocation info, wire
// every channel spec.Channels names (instance subscription + global
// relay), run spec.Work, and tear every subscription down once it
// returns -- whether Work returned on its own (e.g. an upstream
// connection ending) or ctx was cancelled from outside (e.g. a control
// message's handler calling Cancel). Callers don't hand-roll any of
// this: they declare a ServiceSpec and call Run.
func (cp ControlPlane) Run(ctx context.Context, spec ServiceSpec) error {
	if err := RegisterInvocation(ctx, cp.Client, spec.Invocation); err != nil {
		log.Printf("pubsub: failed to record invocation info: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	teardown := cp.SubscribeAll(runCtx, spec.Channels)
	// cancel must run before teardown waits: teardown blocks until every
	// subscriber has stopped, and they only stop once runCtx is done.
	defer func() {
		cancel()
		teardown()
	}()

	return spec.Work(runCtx)
}

// Runtime is the full, ready-to-embed implementation of a claim-once,
// container-scoped, Redis-backed service with a fixed, known-up-front
// set of control channels: it bundles ControlPlane (channel
// naming/relay) and Session (claim/done/cancel) with
// Configure/Start/RecordInvocation, so a type that only adds its own
// state and a Work function needs no Start or run method of its own at
// all.
//
// This isn't the right base for every service, though: logma-serverless's
// own router.Runtime does NOT embed it, because its control:add hot-loads
// arbitrary channels at runtime and dispatches through a single
// in-process actor loop to safely mutate its subscriptions map --
// something a fixed, Configure-time ServiceSpec can't express. Embed
// Runtime when your control channels are known up front and their
// handlers don't need that kind of shared mutable state; wire
// ControlPlane and Session yourself (as logma-serverless's router.Runtime
// does) when they don't.
type Runtime struct {
	ControlPlane
	Session

	invocation InvocationInfo
	spec       ServiceSpec
}

// NewRuntime builds a Runtime using client and this process's
// InstanceID(). It has no ServiceSpec until Configure is called.
func NewRuntime(client *redis.Client) Runtime {
	return Runtime{
		ControlPlane: NewControlPlane(client),
		Session:      NewSession(),
	}
}

// NewRuntimeFromEnv is NewRuntime(NewClientFromEnv()) -- what nearly
// every embedder wants, since a container-scoped service's Redis client
// almost always comes from REDIS_URI/REDISCLI_AUTH. Call NewRuntime
// directly only when the client has to come from somewhere else (tests
// pointing at an unreachable address, a non-default Redis instance).
func NewRuntimeFromEnv() Runtime {
	return NewRuntime(NewClientFromEnv())
}

// RecordInvocation captures which Cloud Function instance and HTTP
// request are driving this runtime. It must be called after a
// successful Claim and before Start; Start fills it into the
// ServiceSpec's Invocation field automatically, so Configure doesn't
// need to set it.
func (sr *Runtime) RecordInvocation(r *http.Request, requestID string) {
	sr.invocation = InvocationInfoFromRequest(r, requestID)
}

// Configure attaches the ServiceSpec Start will run. It must be called
// after a successful Claim and before Start.
func (sr *Runtime) Configure(spec ServiceSpec) {
	sr.spec = spec
}

// ConfigureDefault is Configure for the common case: it merges
// extraChannels with the default control:shutdown handler every
// fixed-channel-set service wants (so callers stop repeating
// `rt.ShutdownChannel(): rt.DefaultShutdownHandler()` themselves), and
// runs work as the ServiceSpec's Work. Call Configure directly instead
// when a service genuinely needs to opt out of the default shutdown
// handling.
func (sr *Runtime) ConfigureDefault(work func(ctx context.Context) error, extraChannels ChannelHandlers) {
	channels := make(ChannelHandlers, len(extraChannels)+1)
	channels[sr.ShutdownChannel()] = sr.DefaultShutdownHandler()
	for channel, handler := range extraChannels {
		channels[channel] = handler
	}
	sr.Configure(ServiceSpec{Channels: channels, Work: work})
}

// Publish marshals event as JSON and publishes it to channel over this
// Runtime's Redis client, using Context() as the publish's context.
// It's the shared shape every fixed-channel-set service that streams
// structured events onto Redis channels wants: callers supply only the
// channel name and the event value, and get a single wrapped error back
// to log or handle however fits their own conventions.
func (sr *Runtime) Publish(channel string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal %T for %s: %w", event, channel, err)
	}
	if err := sr.Client.Publish(sr.Context(), channel, data).Err(); err != nil {
		return fmt.Errorf("publish to %s: %w", channel, err)
	}
	return nil
}

// DefaultShutdownHandler returns a handler suitable for a
// control:shutdown-shaped channel: parse payload as a ShutdownRequest,
// log its reason under this Runtime's own namespace (via the embedded
// ControlPlane's channels.Defaults), and Cancel it. It's the behavior
// every such channel wants unless a service needs to do more on
// shutdown than just stop -- which most don't, so most services never
// need to write their own handleShutdown at all.
func (sr *Runtime) DefaultShutdownHandler() func(payload string) {
	label := sr.Namespace
	if label == "" {
		label = "service"
	}
	return func(payload string) {
		request := ParseShutdownRequest(payload)
		log.Printf("%s: shutting down: reason=%q", label, request.Reason)
		sr.Cancel()
	}
}

// Start satisfies Lifecycle: it begins the Session and runs the
// configured ServiceSpec via ControlPlane.Run, logging any error Work
// returns. It must only be called once -- a second call is a no-op
// (guarded by the embedded Session.Begin), and Configure must have been
// called first.
func (sr *Runtime) Start(ctx context.Context) {
	sr.Begin(ctx, func() {
		spec := sr.spec
		spec.Invocation = sr.invocation
		if err := sr.ControlPlane.Run(sr.Context(), spec); err != nil {
			log.Printf("pubsub: %v", err)
		}
	})
}
