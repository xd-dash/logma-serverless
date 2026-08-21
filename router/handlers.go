package router

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/xd-dash/logma-serverless/pubsub"
)

const sseKeepAlive = 15 * time.Second

// requestedChannels returns the channel names a client asked to
// subscribe to via repeated ?channel= query parameters -- the only way
// to pass this today, since GET /events is read by the browser's
// EventSource, which can't send a body or custom headers.
func requestedChannels(r *http.Request) []string {
	return r.URL.Query()["channel"]
}

// runHandler backs POST /run: it claims a runtime, blocks until it stops
// (via control:shutdown or client disconnect), and reports the outcome as
// JSON.
func runHandler(holder *pubsub.Holder[*Runtime]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rt, ok := holder.Claim()
		if !ok {
			http.Error(w, "runtime already claimed", http.StatusConflict)
			return
		}
		rt.RecordInvocation(r, middleware.GetReqID(r.Context()))
		rt.Subscribe(requestedChannels(r))

		ctx := r.Context()
		rt.Start(ctx)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
	}
}

// eventsHandler backs GET /events: it claims a runtime, starts it bound
// to the request's context, and streams every fanned-out Pub/Sub message
// as an SSE event until the runtime stops or the client disconnects.
// Client disconnect cancels the runtime -- the HTTP request is the
// runtime's lifetime owner.
func eventsHandler(holder *pubsub.Holder[*Runtime]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rt, ok := holder.Claim()
		if !ok {
			http.Error(w, "runtime already claimed", http.StatusConflict)
			return
		}
		rt.RecordInvocation(r, middleware.GetReqID(r.Context()))
		rt.Subscribe(requestedChannels(r))

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		go rt.Start(r.Context())

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)

		_, _ = w.Write([]byte(": connected\n\n"))
		flusher.Flush()

		keepAlive := time.NewTicker(sseKeepAlive)
		defer keepAlive.Stop()

		for {
			select {
			case <-rt.Done():
				return

			case <-r.Context().Done():
				rt.Cancel()
				<-rt.Done()
				return

			case event, ok := <-rt.Events():
				if !ok {
					return
				}
				data, err := json.Marshal(event)
				if err != nil {
					continue
				}
				_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
				flusher.Flush()

			case <-keepAlive.C:
				_, _ = w.Write([]byte(": keepalive\n\n"))
				flusher.Flush()
			}
		}
	}
}
