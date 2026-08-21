package router

import (
	"encoding/json"
	"log"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// Handle processes one payload arriving on a subscribed channel. add
// lets a handler hot-load more subscriptions into the running
// container -- control:add's own handler is the only one that
// currently needs it, but it's threaded through uniformly so any
// handler registered here can use it too.
type Handle func(rt *Runtime, payload string, add func(channel string) error)

// Subscription pairs a channel with the Handle that processes messages
// arriving on it. Channel is a resolver, not a plain string, because a
// channel's real name is namespaced per service (pubsub/channels) --
// control:add and control:shutdown both need that; a literal channel
// just wraps a constant in a one-line closure.
type Subscription struct {
	Channel func(rt *Runtime) string
	Handle  Handle
}

// Subscriptions lists every channel a Runtime establishes on start. It
// starts empty; init below seeds control:add and control:shutdown
// through Register, the same entry point any other file or package
// uses to compose in more.
var Subscriptions []Subscription

// Register appends a channel/handler pair to Subscriptions. Call it --
// from an init() in this file, another file in this package, or another
// package that imports router -- to compose additional startup
// subscriptions into every Runtime.
func Register(channel func(rt *Runtime) string, handle Handle) {
	Subscriptions = append(Subscriptions, Subscription{Channel: channel, Handle: handle})
}

func init() {
	Register(
		func(rt *Runtime) string { return rt.AddChannel() },
		func(rt *Runtime, payload string, add func(string) error) { rt.handleAdd(payload, add) },
	)
	Register(
		func(rt *Runtime) string { return rt.ShutdownChannel() },
		func(rt *Runtime, payload string, _ func(string) error) { rt.handleShutdown(payload) },
	)
}

// AddSubscription is the payload published to control:add to hot-load a
// new subscription into a running container.
type AddSubscription struct {
	Channel string `json:"channel"`
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
	rt.Cancel()
}
