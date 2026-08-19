// Package pubsub provides the Redis Pub/Sub building blocks shared by
// every service that streams data into or out of Redis channels: an
// env-configured client, a reconnecting channel subscriber, and a
// single-active-instance-per-container lifecycle holder.
package pubsub

import (
	"os"

	"github.com/redis/go-redis/v9"
)

// NewClientFromEnv builds a Redis client configured from REDIS_URI and
// REDISCLI_AUTH. It does not connect until the first command is issued.
func NewClientFromEnv() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URI"),
		Password: os.Getenv("REDISCLI_AUTH"),
		DB:       0,
	})
}
