package pubsub

import (
	"context"
	"testing"
)

func TestSessionClaimRejectsSecondClaimWhileRunning(t *testing.T) {
	s := NewSession()

	if !s.Claim() {
		t.Fatal("expected first claim to succeed")
	}
	if s.Claim() {
		t.Fatal("expected second claim to fail while running")
	}
}

func TestSessionBeginRunsFnOnce(t *testing.T) {
	s := NewSession()
	calls := 0

	s.Begin(context.Background(), func() { calls++ })
	s.Begin(context.Background(), func() { calls++ })

	if calls != 1 {
		t.Fatalf("expected fn to run exactly once, ran %d times", calls)
	}
}

func TestSessionBeginClosesDoneWhenFnReturns(t *testing.T) {
	s := NewSession()

	s.Begin(context.Background(), func() {})

	select {
	case <-s.Done():
	default:
		t.Fatal("expected Done() to be closed once Begin's fn returned")
	}
}

func TestSessionCancelUnblocksFnWatchingContext(t *testing.T) {
	s := NewSession()
	started := make(chan struct{})

	go s.Begin(context.Background(), func() {
		close(started)
		<-s.Context().Done()
	})

	<-started
	s.Cancel()

	select {
	case <-s.Done():
	case <-context.Background().Done():
		t.Fatal("expected Cancel to unblock fn and close Done")
	}
}

func TestDefaultShutdownHandlerCancelsSession(t *testing.T) {
	s := NewSession()

	handler := s.DefaultShutdownHandler("test-service")
	handler(`{"reason":"maintenance"}`)

	select {
	case <-s.Context().Done():
	default:
		t.Fatal("expected DefaultShutdownHandler to cancel the Session's context")
	}
}

func TestDefaultShutdownHandlerAcceptsEmptyPayload(t *testing.T) {
	s := NewSession()

	handler := s.DefaultShutdownHandler("test-service")
	handler("")

	select {
	case <-s.Context().Done():
	default:
		t.Fatal("expected DefaultShutdownHandler to cancel the Session's context even with no reason given")
	}
}

func TestSessionExternalContextCancelsSessionContext(t *testing.T) {
	s := NewSession()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	go s.Begin(ctx, func() {
		close(started)
		<-s.Context().Done()
	})

	<-started
	cancel()
	<-s.Done()

	if s.Context().Err() == nil {
		t.Fatal("expected the external ctx's cancellation to cancel Session's own context")
	}
}
