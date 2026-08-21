package pubsub

// DefaultChannel returns the canonical base control-plane channel name
// a service under namespace uses for a given purpose (e.g. "add",
// "shutdown"), so every service's control channels are named and
// greppable the same way regardless of which repo declares them:
//
//	DefaultChannel("stonks", "shutdown") == "stonks:control:shutdown"
//	DefaultChannel("", "add")            == "control:add"
//
// An empty namespace is valid -- logma-serverless's own control
// channels aren't namespaced, since it's the only service using them.
func DefaultChannel(namespace, purpose string) string {
	if namespace == "" {
		return "control:" + purpose
	}
	return namespace + ":control:" + purpose
}
