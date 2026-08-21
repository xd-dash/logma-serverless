package router

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

// Subscriptions lists every channel a Runtime establishes on start.
// control:add and control:shutdown anchor the list -- they're
// structurally required, expressed as ordinary entries here rather than
// special-cased in Runtime.run. Register appends more.
var Subscriptions = []Subscription{
	{
		Channel: func(rt *Runtime) string { return rt.AddChannel() },
		Handle: func(rt *Runtime, payload string, add func(string) error) {
			rt.handleAdd(payload, add)
		},
	},
	{
		Channel: func(rt *Runtime) string { return rt.ShutdownChannel() },
		Handle: func(rt *Runtime, payload string, _ func(string) error) {
			rt.handleShutdown(payload)
		},
	},
}

// Register appends a channel/handler pair to Subscriptions. Call it --
// from an init() in this file, another file in this package, or another
// package that imports router -- to compose additional startup
// subscriptions into every Runtime without editing the list above.
func Register(channel func(rt *Runtime) string, handle Handle) {
	Subscriptions = append(Subscriptions, Subscription{Channel: channel, Handle: handle})
}
