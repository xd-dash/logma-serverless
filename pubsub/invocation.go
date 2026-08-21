package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// invocationTTL bounds how long an invocation record survives in Redis --
// it's a debugging aid for an in-flight or recently finished container,
// not a durable log.
const invocationTTL = 24 * time.Hour

// InvocationInfo describes the Cloud Function instance and the HTTP
// request driving it.
type InvocationInfo struct {
	Service       string
	Revision      string
	Configuration string
	InstanceID    string
	RequestID     string
	Method        string
	Path          string
	RemoteAddr    string
	StartedAt     time.Time
}

// InvocationInfoFromRequest builds an InvocationInfo from the standard
// Cloud Run/Cloud Functions Gen 2 environment (K_SERVICE, K_REVISION,
// K_CONFIGURATION), this process's InstanceID(), and r. requestID is
// taken as an explicit argument rather than read from r here so this
// package doesn't need to know which router library produced it.
func InvocationInfoFromRequest(r *http.Request, requestID string) InvocationInfo {
	return InvocationInfo{
		Service:       os.Getenv("K_SERVICE"),
		Revision:      os.Getenv("K_REVISION"),
		Configuration: os.Getenv("K_CONFIGURATION"),
		InstanceID:    InstanceID(),
		RequestID:     requestID,
		Method:        r.Method,
		Path:          r.URL.Path,
		RemoteAddr:    r.RemoteAddr,
		StartedAt:     time.Now().UTC(),
	}
}

// InvocationKey derives the Redis key an InvocationInfo is stored under:
// instance:<service>:<instance-id>:<request-id>, falling back to
// "unknown" for any empty segment so the key is always well-formed.
func InvocationKey(info InvocationInfo) string {
	return fmt.Sprintf("instance:%s:%s:%s",
		orDefault(info.Service, "unknown"),
		orDefault(info.InstanceID, "unknown"),
		orDefault(info.RequestID, "unknown"))
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// RegisterInvocation records info in Redis as a hash (the idiomatic
// shape for a multi-field record, as opposed to a JSON blob in a
// string), with a bounded TTL. It uses plain, non-pubsub commands, so
// it must be called before client's first Subscribe -- once a
// *redis.Client commits to pubsub use, it's never used for anything
// else again.
func RegisterInvocation(ctx context.Context, client *redis.Client, info InvocationInfo) error {
	key := InvocationKey(info)

	fields := map[string]any{
		"service":       info.Service,
		"revision":      info.Revision,
		"configuration": info.Configuration,
		"instance_id":   info.InstanceID,
		"request_id":    info.RequestID,
		"method":        info.Method,
		"path":          info.Path,
		"remote_addr":   info.RemoteAddr,
		"started_at":    info.StartedAt.Format(time.RFC3339),
	}

	if err := client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("hset %s: %w", key, err)
	}
	if err := client.Expire(ctx, key, invocationTTL).Err(); err != nil {
		return fmt.Errorf("expire %s: %w", key, err)
	}
	return nil
}
