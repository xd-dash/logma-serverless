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

// HeaderRedisURI is the request header a caller can supply the Redis
// connection address through when the deployed function's own REDIS_URI
// env var is left unset -- see NewClientFromRequest.
const HeaderRedisURI = "X-Redis-Uri"

// NewClientFromEnv builds a Redis client configured from REDIS_URI and
// REDISCLI_AUTH. It does not connect until the first command is issued.
func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDIS_URI"), os.Getenv("REDISCLI_AUTH"))
}

// NewClientFromRequest is NewClientFromEnv, except that whichever of
// REDIS_URI/REDISCLI_AUTH is left unset falls back to r's
// HeaderRedisURI/HeaderRedisAuth header instead, independently of one
// another -- for a deployment that intentionally leaves one or both env
// vars unconfigured and expects each caller to supply them per request
// instead. Either env var, when set, always wins over its header.
func NewClientFromRequest(r *http.Request) *redis.Client {
	addr := os.Getenv("REDIS_URI")
	if addr == "" {
		addr = r.Header.Get(HeaderRedisURI)
	}

	auth := os.Getenv("REDISCLI_AUTH")
	if auth == "" {
		auth = r.Header.Get(HeaderRedisAuth)
	}

	return newClient(addr, auth)
}

func newClient(addr, auth string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: auth,
		DB:       0,
	})
}
