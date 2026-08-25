package router

import (
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// Build constructs a router with this deployment's standard middleware
// stack (request ID, real IP, logging, panic recovery) attached, then
// calls register to mount whichever routes and handlers a deployment
// provides. This package owns building the router; register is the
// drop-in that describes what it serves -- the same shell/drop-in split
// gospace-minimal's routersource package uses one layer up
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
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

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
//
// This deployment is always public (--allow-unauthenticated at the GCP
// IAM layer) with no application-level auth otherwise, so requireRedisAuth
// is attached here -- scoped to just these two routes via this register
// callback, not to Build itself, since Build is shared with other repos
// (stonks imports it for its own, unrelated router) that shouldn't
// silently inherit a check they never asked for.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewRuntime)

	return Build(func(r chi.Router) {
		r.Use(requireRedisAuth)
		r.Post("/run", runHandler(holder))
		r.Get("/events", eventsHandler(holder))
	})
}

// requireRedisAuth rejects any request whose X-Rediscli-Auth header
// doesn't match this deployment's own REDISCLI_AUTH -- the same
// credential every real deployment already needs to reach Redis at all,
// reused here as this service's only app-level auth check rather than
// provisioning a separate API key. A request with no header at all is
// rejected the same as one with a wrong value.
//
// Deliberately a no-op when REDISCLI_AUTH itself is unset/empty: an
// unauthenticated-Redis deployment is an existing, supported mode (see
// pubsub.NewClientFromRequest's env-or-header fallback) and this
// shouldn't lock it out -- every real deployment of this service already
// sets REDISCLI_AUTH to reach its actual Redis instance, so this is
// enforced automatically wherever it matters.
func requireRedisAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := os.Getenv("REDISCLI_AUTH")
		if auth != "" && r.Header.Get(pubsub.HeaderRedisAuth) != auth {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
