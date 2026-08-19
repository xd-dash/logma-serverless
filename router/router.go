package router

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// runtimeHolder mints one Runtime per session and hands it to whichever
// request claims it. A container instance lives across many sequential
// requests (Cloud Run keeps a warm instance around between invocations),
// but each request's runtime is single-use: once a session's Runtime
// finishes, the next request gets a fresh one rather than being
// permanently locked out. maxInstanceRequestConcurrency=1 is what
// guarantees only one request -- and therefore only one live Runtime --
// exists at a time; the mutex here just protects the holder's own
// bookkeeping.
type runtimeHolder struct {
	mu sync.Mutex
	rt *Runtime
}

func (h *runtimeHolder) claim() *Runtime {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.rt == nil || h.rt.state.Load() == runtimeStateDone {
		h.rt = NewRuntime()
	}

	if h.rt.Claim() {
		return h.rt
	}
	return nil
}

// NewRouter builds the router for this deployment. It's called once per
// container by gospace-minimal's generated routersource.Serve(), so the
// runtimeHolder constructed here lives for the container's entire
// lifetime and is shared by every request it handles.
//
// There is deliberately no "/" route: with concurrency pinned to 1, a
// request to a health-check path would itself consume the container's
// one request slot.
func NewRouter() http.Handler {
	holder := &runtimeHolder{}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/run", runHandler(holder))
	r.Get("/events", eventsHandler(holder))

	return r
}
