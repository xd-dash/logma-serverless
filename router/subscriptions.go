package router

import (
	"github.com/xd-dash/logma-serverless/internal/addsub"
	"github.com/xd-dash/logma-serverless/internal/shutdown"
	"github.com/xd-dash/logma-serverless/pubsub"
)

// Handle processes one payload arriving on a subscribed channel. add
// lets a handler hot-load more subscriptions into the running
// container. session is *pubsub.Session, not *Runtime, specifically so
// a registrant package (internal/addsub, internal/shutdown, or a
// future one) never needs to import router to implement Handle --
// router imports them instead, the same one-way direction
// dash-xd/gospace's own registry uses.
type Handle func(session *pubsub.Session, payload string, add func(channel string) error)

// Subscriptions maps a base channel name (already namespaced per
// service -- see pubsub/channels) to the Handle that processes
// messages arriving on it.
var Subscriptions = make(map[string]Handle)

// RegisterChannel maps channel to handle. Call it -- from an init() in
// this file, another file in this package, or another package that
// imports router -- to compose an additional startup subscription into
// every Runtime.
func RegisterChannel(channel string, handle Handle) {
	Subscriptions[channel] = handle
}

func init() {
	RegisterChannel(addsub.Channel, addsub.Handle)
	RegisterChannel(shutdown.Channel, shutdown.Handle)
}
