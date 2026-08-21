// Package shutdown is one of logma-serverless's default subscriptions:
// registered against control:shutdown, it stops a running container in
// response to a publish.
package shutdown

import (
	"log"

	"github.com/xd-dash/logma-serverless/pubsub"
	"github.com/xd-dash/logma-serverless/pubsub/channels"
)

// Channel is the base control:shutdown channel name for this
// deployment, resolved once via pubsub/channels.Discover().
var Channel = channels.Discover().ShutdownChannel()

// Handle parses payload, logs the shutdown reason, and cancels
// session -- ending whatever it's embedded in. add is unused, but part
// of the signature every registrant shares (see router.Handle).
func Handle(session *pubsub.Session, payload string, _ func(channel string) error) {
	request := pubsub.ParseShutdownRequest(payload)
	log.Printf("shutdown: reason=%q", request.Reason)
	session.Cancel()
}
