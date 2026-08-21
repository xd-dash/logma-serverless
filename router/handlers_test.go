package router

import (
	"net/http/httptest"
	"testing"
)

func TestRequestedChannelsReadsChannelQueryParam(t *testing.T) {
	t.Run("no query params yields no channels", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/events", nil)
		if got := requestedChannels(r); len(got) != 0 {
			t.Fatalf("expected no channels, got %v", got)
		}
	})

	t.Run("repeated channel params are all returned in order", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/events?channel=a&channel=b", nil)
		got := requestedChannels(r)
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("expected [a b], got %v", got)
		}
	})
}
