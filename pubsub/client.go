// Package pubsub provides the Redis Pub/Sub building blocks shared by
// every service that streams data into or out of Redis channels: an
// env-configured client, a reconnecting channel subscriber, and a
// single-active-instance-per-container lifecycle holder.
package pubsub

import (
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

// HeaderRedisAuth is the request header a caller can supply the Redis
// auth password through when the deployed function's own REDISCLI_AUTH
// env var is left unset -- see NewClientFromRequest.
const HeaderRedisAuth = "X-Rediscli-Auth"

// NewClientFromEnv builds a Redis client configured from REDIS_URI and
// REDISCLI_AUTH. It does not connect until the first command is issued.
func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDISCLI_AUTH"))
}

// NewClientFromRequest is NewClientFromEnv, except that when REDISCLI_AUTH
// itself is unset, the auth password instead comes from r's
// HeaderRedisAuth header, if present -- for a deployment that intentionally
// leaves REDISCLI_AUTH unconfigured and expects each caller to supply its
// own Redis credentials per request instead. REDIS_URI always comes from
// the environment either way; only the auth password has a per-request
// fallback, and only when REDISCLI_AUTH wasn't already set.
func NewClientFromRequest(r *http.Request) *redis.Client {
	auth := os.Getenv("REDISCLI_AUTH")
	if auth == "" {
		auth = r.Header.Get(HeaderRedisAuth)
	}
	return newClient(auth)
}

func newClient(auth string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URI"),
		Password: auth,
		DB:       0,
	})
}
