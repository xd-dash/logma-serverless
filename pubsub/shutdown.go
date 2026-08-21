package pubsub

import (
	"encoding/json"
	"log"
)

// ShutdownRequest is the payload published to a shutdown control
// channel to drain and terminate a running container.
type ShutdownRequest struct {
	Reason string `json:"reason"`
}

// ParseShutdownRequest decodes payload into a ShutdownRequest. An empty
// payload is treated as a shutdown with no reason given, not an error;
// invalid JSON is logged and treated the same way, so a malformed
// shutdown message still shuts the runtime down rather than getting
// silently dropped like other invalid control messages.
func ParseShutdownRequest(payload string) ShutdownRequest {
	var request ShutdownRequest
	if payload == "" {
		return request
	}
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("pubsub: invalid shutdown message: %v", err)
	}
	return request
}
