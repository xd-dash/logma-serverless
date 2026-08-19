package pubsub

import (
	"context"
	"sync"
)

// Lifecycle is implemented by any single-use, single-owner runtime a
// Holder can manage.
type Lifecycle interface {
	// Start runs the instance until ctx is cancelled or it reaches a
	// terminal state on its own. It blocks until the instance stops.
	Start(ctx context.Context)
	// Cancel ends the instance's lifetime, causing Start to return. Safe
	// to call multiple times and from any goroutine.
	Cancel()
	// Done returns a channel that's closed once Start has returned.
	Done() <-chan struct{}
	// Claim attempts to take exclusive ownership of the instance for the
	// calling request. It returns false if the instance is already
	// running or has already finished.
	Claim() bool
}

// Holder mints one T per session and hands it to whichever caller claims
// it first. A Holder is meant to live for a container's entire lifetime
// and be shared across every request it handles: once a session's
// instance finishes (its Done channel closes), the next Claim mints a
// fresh one rather than staying permanently locked out.
type Holder[T Lifecycle] struct {
	mu          sync.Mutex
	newFn       func() T
	cur         T
	initialized bool
}

// NewHolder builds a Holder that mints new instances via newFn.
func NewHolder[T Lifecycle](newFn func() T) *Holder[T] {
	return &Holder[T]{newFn: newFn}
}

// Claim attempts to take exclusive ownership of the current instance,
// minting a fresh one first if none exists yet or the previous one has
// finished. It returns the zero value of T and false if an instance is
// already claimed and running.
func (h *Holder[T]) Claim() (T, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.initialized || isDone(h.cur) {
		h.cur = h.newFn()
		h.initialized = true
	}

	if h.cur.Claim() {
		return h.cur, true
	}
	var zero T
	return zero, false
}

func isDone(l Lifecycle) bool {
	select {
	case <-l.Done():
		return true
	default:
		return false
	}
}
