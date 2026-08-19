package pubsub

import (
	"context"
	"sync/atomic"
	"testing"
)

// fakeLifecycle is a minimal Lifecycle for exercising Holder in
// isolation, without any real Redis or Runtime dependency.
type fakeLifecycle struct {
	state atomic.Int32
	done  chan struct{}
}

func newFakeLifecycle() *fakeLifecycle {
	return &fakeLifecycle{done: make(chan struct{})}
}

func (f *fakeLifecycle) Start(ctx context.Context) {
	<-ctx.Done()
	close(f.done)
}

func (f *fakeLifecycle) Cancel() {}

func (f *fakeLifecycle) Done() <-chan struct{} {
	return f.done
}

func (f *fakeLifecycle) Claim() bool {
	return f.state.CompareAndSwap(0, 1)
}

func TestHolderClaimRejectsConcurrentSecondClaim(t *testing.T) {
	holder := NewHolder(newFakeLifecycle)

	first, ok := holder.Claim()
	if !ok {
		t.Fatal("expected first claim to succeed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go first.Start(ctx)

	if _, ok := holder.Claim(); ok {
		t.Fatal("expected second claim to fail while the first instance is active")
	}
}

func TestHolderMintsFreshInstanceAfterDone(t *testing.T) {
	holder := NewHolder(newFakeLifecycle)

	first, ok := holder.Claim()
	if !ok {
		t.Fatal("expected first claim to succeed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go first.Start(ctx)
	cancel()
	<-first.Done()

	second, ok := holder.Claim()
	if !ok {
		t.Fatal("expected claim to succeed once the prior instance finished")
	}
	if second == first {
		t.Fatal("expected a fresh instance once the prior one finished")
	}
}
