package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
)

// InstanceID returns a value that uniquely identifies this running
// container/process for its whole lifetime: the Cloud Run/Cloud
// Functions Gen 2 instance hostname when running there (already unique
// per container instance), or a generated id for local/dev processes,
// where the hostname alone isn't guaranteed unique across concurrent
// runs on the same machine. It's computed once and cached for the life
// of the process, so every channel name derived from it stays stable
// for as long as this container is alive.
var InstanceID = sync.OnceValue(computeInstanceID)

func computeInstanceID() string {
	if os.Getenv("K_SERVICE") != "" {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			return hostname
		}
	}
	return "dev-" + randomHex(8)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is effectively unheard of, but a stable
		// fallback still keeps this process's channel names well-formed
		// and internally consistent, just not globally unique.
		return "static"
	}
	return hex.EncodeToString(buf)
}
