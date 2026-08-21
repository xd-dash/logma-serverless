package pubsub

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

const (
	sessionIdle int32 = iota
	sessionRunning
	sessionDone
)

// Session is the reusable core of a claim-once, single-owner,
// cancelable actor: its own independently cancelable context, and the
// Claim/Done/Cancel lifecycle a Holder[T] needs to manage it. Every
// container-scoped Runtime needs this same bookkeeping regardless of
// what it actually does once claimed and started.
//
// Embed Session in a Runtime type and call Begin at the top of Start to
// pick up Claim/Cancel/Done and the dual-cancellation wiring (the ctx
// Start was called with, and this Session's own independent one --
// whichever is cancelled first cancels the other) without hand-rolling
// it per service.
type Session struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	state     atomic.Int32
	startOnce sync.Once
}

// NewSession builds a Session with its own context, derived from
// context.Background so it can be cancelled independently of whatever
// context Begin is eventually called with.
func NewSession() Session {
	ctx, cancel := context.WithCancel(context.Background())
	return Session{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// Context returns this Session's own context: done once Cancel is
// called, or once the ctx passed to Begin is done.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Claim attempts to take exclusive ownership of the Session for the
// calling request. It returns false if it's already been claimed (by a
// concurrent request, or a previous one that hasn't finished) or has
// already run to completion.
func (s *Session) Claim() bool {
	return s.state.CompareAndSwap(sessionIdle, sessionRunning)
}

// Done returns a channel that's closed once the work wrapped by Begin
// has returned.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Cancel ends the Session's lifetime, causing Context() to become done
// and Begin's wrapped work to return once it notices. Safe to call
// multiple times and from any goroutine.
func (s *Session) Cancel() {
	s.cancel()
}

// DefaultShutdownHandler returns a handler suitable for a
// control:shutdown-shaped channel: parse payload as a ShutdownRequest,
// log its reason under serviceName, and Cancel this Session. It's the
// behavior every such channel wants unless a service needs to do more
// on shutdown than just stop -- which most don't, so most services
// never need to write their own handleShutdown at all.
func (s *Session) DefaultShutdownHandler(serviceName string) func(payload string) {
	return func(payload string) {
		request := ParseShutdownRequest(payload)
		log.Printf("%s: shutting down: reason=%q", serviceName, request.Reason)
		s.Cancel()
	}
}

// Begin runs fn exactly once (guarded internally -- a second call is a
// no-op that returns immediately), bound to both ctx and this Session's
// own context: whichever is cancelled first cancels the other, so fn
// only ever needs to watch Context(). It marks the Session done once fn
// returns.
func (s *Session) Begin(ctx context.Context, fn func()) {
	s.startOnce.Do(func() {
		defer s.state.Store(sessionDone)
		defer close(s.done)

		go func() {
			select {
			case <-ctx.Done():
				s.cancel()
			case <-s.ctx.Done():
			}
		}()

		fn()
	})
}
