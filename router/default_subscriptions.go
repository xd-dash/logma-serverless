package router

// DefaultSubscriptions lists the channels every container instance
// subscribes to on startup, alongside the mandatory control:add and
// control:shutdown channels. Edit this list directly to change what
// gets bootstrapped; it takes effect on the next deploy.
var DefaultSubscriptions = []string{}
