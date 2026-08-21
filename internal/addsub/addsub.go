// Package addsub is one of logma-serverless's default subscriptions:
// registered against control:add, it hot-loads a new Redis channel
// subscription into a running container in response to a publish.
package addsub

import (
	"encoding/json"
	"log"

	"github.com/xd-dash/logma-serverless/pubsub"
	"github.com/xd-dash/logma-serverless/pubsub/channels"
)

// Channel is the base control:add channel name for this deployment,
// resolved once via pubsub/channels.Discover().
var Channel = channels.Discover().AddChannel()

// Request is the payload published to Channel to hot-load a new
// subscription into a running container.
type Request struct {
	Channel string `json:"channel"`
}

// Handle parses payload and calls add with the channel it names.
// session is unused -- add is all this handler needs -- but it's part
// of the signature every registrant shares (see router.Handle).
func Handle(_ *pubsub.Session, payload string, add func(channel string) error) {
	var request Request
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("addsub: invalid message: %v", err)
		return
	}
	if request.Channel == "" {
		log.Printf("addsub: message contained empty channel")
		return
	}
	if err := add(request.Channel); err != nil {
		log.Printf("addsub: failed to add subscription %q: %v", request.Channel, err)
	}
}
