// Package channels derives a service's base control-plane channel
// names from its namespace -- typically the service's own name, so
// nothing has to hardcode it.
package channels

import (
	"os"
	"path"
	"runtime/debug"
)

// Defaults derives a service's base control-plane channel names from
// its namespace. InstanceChannel/GlobalChannel (on pubsub.ControlPlane)
// turn a base name into the actual scoped channels a Runtime
// subscribes to; ShutdownChannel/AddChannel here just supply that base
// name -- they are not "the default channel" in the instance-channel
// sense.
type Defaults struct {
	Namespace string
}

// ForNamespace builds Defaults for an explicit namespace.
func ForNamespace(namespace string) Defaults {
	return Defaults{Namespace: namespace}
}

// Discover builds Defaults using this process's own namespace:
// K_SERVICE (Cloud Run/Cloud Functions Gen 2's own service-name env
// var) if set, otherwise the last path segment of the running binary's
// own Go module path (e.g. "stonks" for github.com/xd-dash/stonks),
// read from its embedded build info -- so a service never hardcodes
// its own name as a channel-naming literal, in a real deployment or in
// local dev/tests.
func Discover() Defaults {
	if service := os.Getenv("K_SERVICE"); service != "" {
		return Defaults{Namespace: service}
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
		return Defaults{Namespace: path.Base(info.Main.Path)}
	}
	return Defaults{}
}

// ShutdownChannel returns this namespace's base control:shutdown
// channel name.
func (d Defaults) ShutdownChannel() string {
	return d.channel("shutdown")
}

// AddChannel returns this namespace's base control:add channel name.
func (d Defaults) AddChannel() string {
	return d.channel("add")
}

func (d Defaults) channel(purpose string) string {
	if d.Namespace == "" {
		return "control:" + purpose
	}
	return d.Namespace + ":control:" + purpose
}
