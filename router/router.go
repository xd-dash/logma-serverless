package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/xd-dash/logma-serverless/httpserver"
	"github.com/xd-dash/logma-serverless/pubsub"
)

// Build constructs a router via httpserver.New()'s standard middleware
// stack, then calls register to mount whichever routes and handlers a
// deployment provides. This package owns building the router; register
// is the drop-in that describes what it serves -- the same shell/drop-in
// split gospace-minimal's routersource package uses one layer up
// (internal/function -> routersource/serve -> routersource/source),
// just expressed as a function value passed at import time instead of a
// file copied in at build time. Other repos (stonks) that want the same
// middleware stack import this package and call Build with their own
// routes, rather than re-declaring the middleware setup themselves.
//
// If concurrency ends up pinned to 1 per container (as this repo's own
// NewRouter below assumes), register should avoid mounting a "/" route
// -- a health check there would itself consume the container's one
// request slot.
func Build(register func(r chi.Router)) http.Handler {
	r := httpserver.New()
	register(r)
	return r
}

// NewRouter builds this repo's own deployment: logma-serverless's /run
// and /events routes, bound to a fresh pubsub.Holder[*Runtime]. It's
// called once per container by gospace-minimal's generated
// routersource.Serve(), so the holder constructed here lives for the
// container's entire lifetime and is shared by every request it
// handles. A container instance lives across many sequential requests
// (Cloud Run keeps a warm instance around between invocations), but
// each request's runtime is single-use: once a session's Runtime
// finishes, the next request gets a fresh one rather than being
// permanently locked out. maxInstanceRequestConcurrency=1 is what
// guarantees only one request -- and therefore only one live Runtime --
// exists at a time; the holder's own locking just protects its
// bookkeeping.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewRuntime)

	return Build(func(r chi.Router) {
		r.Post("/run", runHandler(holder))
		r.Get("/events", eventsHandler(holder))
	})
}
