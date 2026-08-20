// Package httpserver builds the chi router every deployment starts
// from: request ID, real IP, logging, and panic-recovery middleware,
// and nothing else. It's the one piece of router setup that's
// genuinely identical across services (logma-serverless's own router
// package and stonks both used to duplicate it verbatim) -- routes and
// handlers stay in each service's own package, registered onto the
// router this returns.
package httpserver

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// New returns a chi.Router with this deployment's standard middleware
// stack attached and no routes registered.
func New() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	return r
}
