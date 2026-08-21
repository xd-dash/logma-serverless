package shutdown

import (
	"testing"

	"github.com/xd-dash/logma-serverless/pubsub"
)

func TestHandle(t *testing.T) {
	session := pubsub.NewSession()

	Handle(&session, `{"reason":"maintenance"}`, nil)

	select {
	case <-session.Context().Done():
	default:
		t.Fatal("expected Handle to cancel the session")
	}
}
