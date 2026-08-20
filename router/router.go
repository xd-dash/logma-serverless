package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/xd-dash/logma-serverless/httpserver"
	"github.com/xd-dash/logma-serverless/pubsub"
)

// NewRouter builds the router for this deployment. It's called once per
// container by gospace-minimal's generated routersource.Serve(), so the
// pubsub.Holder constructed here lives for the container's entire
// lifetime and is shared by every request it handles. A container
// instance lives across many sequential requests (Cloud Run keeps a warm
// instance around between invocations), but each request's runtime is
// single-use: once a session's Runtime finishes, the next request gets a
// fresh one rather than being permanently locked out.
// maxInstanceRequestConcurrency=1 is what guarantees only one request --
// and therefore only one live Runtime -- exists at a time; the holder's
// own locking just protects its bookkeeping.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewRuntime)

	r := httpserver.New()
	RegisterRoutes(r, holder)

	return r
}

// RegisterRoutes mounts this package's routes onto r, bound to holder.
//
// There is deliberately no "/" route: with concurrency pinned to 1, a
// request to a health-check path would itself consume the container's
// one request slot.
func RegisterRoutes(r chi.Router, holder *pubsub.Holder[*Runtime]) {
	r.Post("/run", runHandler(holder))
	r.Get("/events", eventsHandler(holder))
}
