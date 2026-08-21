package pubsub

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"

	"github.com/xd-dash/logma-serverless/pubsub/channels"
)

// ControlPlane gives a container-scoped actor two ways to receive
// control messages on a named channel: publish directly to this
// specific container (InstanceChannel), to reach only it, or publish
// once to GlobalChannel to reach every container currently listening.
// Relay subscribes to the global channel and republishes whatever it
// receives onto this container's own instance channel, so a handler
// subscribed there (directly, or via the combined Subscribe) sees both
// kinds of message the same way, without ever needing to know which one
// arrived.
//
// The embedded channels.Defaults gives ShutdownChannel()/AddChannel()
// -- base channel names for the two control-plane purposes every
// service in this fleet uses -- promoted alongside InstanceChannel/
// GlobalChannel, so a Runtime never has to declare or namespace them by
// hand.
//
// Embed ControlPlane in a Runtime type to pick up channel naming and
// the relay wiring without hand-rolling it per service -- both
// logma-serverless's own Runtime and stonks's do this.
type ControlPlane struct {
	Client     *redis.Client
	InstanceID string
	channels.Defaults
}

// NewControlPlane builds a ControlPlane using client, this process's
// InstanceID(), and its discovered channels.Defaults namespace (see
// channels.Discover).
func NewControlPlane(client *redis.Client) ControlPlane {
	return ControlPlane{Client: client, InstanceID: InstanceID(), Defaults: channels.Discover()}
}

// InstanceChannel returns baseChannel's name scoped to this specific
// container: "<baseChannel>:<instanceID>". Publish here to reach only
// this container.
func (cp ControlPlane) InstanceChannel(baseChannel string) string {
	return baseChannel + ":" + cp.InstanceID
}

// GlobalChannel returns baseChannel's broadcast name:
// "<baseChannel>:global". Publish here to reach every container
// currently listening on baseChannel.
func (cp ControlPlane) GlobalChannel(baseChannel string) string {
	return baseChannel + ":global"
}

// Relay subscribes to baseChannel's global (broadcast) channel and
// republishes every message it receives onto this container's own
// instance channel for baseChannel, so a handler subscribed only to the
// instance channel still sees broadcast messages.
func (cp ControlPlane) Relay(ctx context.Context, baseChannel string) *Subscriber {
	instanceChannel := cp.InstanceChannel(baseChannel)
	globalChannel := cp.GlobalChannel(baseChannel)

	return Subscribe(ctx, cp.Client, globalChannel, func(payload string) {
		if err := cp.Client.Publish(ctx, instanceChannel, payload).Err(); err != nil {
			log.Printf("pubsub: failed to relay %s -> %s: %v", globalChannel, instanceChannel, err)
		}
	})
}

// Subscribe wires onMessage to baseChannel's instance channel and sets
// up Relay for its global channel in one call, for callers that don't
// need finer control over the two subscriptions individually. It
// returns both Subscribers so the caller can wait for them to stop
// during teardown.
func (cp ControlPlane) Subscribe(ctx context.Context, baseChannel string, onMessage func(payload string)) (instance, relay *Subscriber) {
	instance = Subscribe(ctx, cp.Client, cp.InstanceChannel(baseChannel), onMessage)
	relay = cp.Relay(ctx, baseChannel)
	return instance, relay
}
