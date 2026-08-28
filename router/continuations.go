package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xd-dash/logma-serverless/internal/continuation"
	"github.com/xd-dash/logma-serverless/pubsub"
)

const maxContinuationBody = 128 << 10

type continuationPublishFunc func(context.Context, *http.Request, string, []byte) error

func continuationHandler(publish continuationPublishFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxContinuationBody))
		if err != nil {
			http.Error(w, "continuation body is too large", http.StatusRequestEntityTooLarge)
			return
		}

		var event continuation.Event
		if err := json.Unmarshal(body, &event); err != nil {
			http.Error(w, "invalid continuation JSON", http.StatusBadRequest)
			return
		}
		if err := event.Validate(time.Now().UTC()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		data, err := json.Marshal(event)
		if err != nil {
			http.Error(w, "failed to encode continuation", http.StatusInternalServerError)
			return
		}
		wrapped, err := json.Marshal(PublishRequest{Channel: event.Channel, Data: data})
		if err != nil {
			http.Error(w, "failed to encode publish request", http.StatusInternalServerError)
			return
		}
		if err := publish(r.Context(), r, event.Channel, wrapped); err != nil {
			http.Error(w, "failed to publish continuation", http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": event.ID, "status": "published"})
	}
}

func publishContinuation(ctx context.Context, r *http.Request, channel string, payload []byte) error {
	client := pubsub.NewClientFromRequest(r)
	defer client.Close()
	if err := client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("publish %s: %w", channel, err)
	}
	return nil
}
