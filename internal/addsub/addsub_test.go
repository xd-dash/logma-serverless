package addsub

import "testing"

func TestHandle(t *testing.T) {
	t.Run("empty channel is rejected", func(t *testing.T) {
		called := false
		Handle(nil, `{"channel":""}`, func(string) error {
			called = true
			return nil
		})
		if called {
			t.Fatal("add should not be called for an empty channel")
		}
	})

	t.Run("invalid JSON is rejected", func(t *testing.T) {
		called := false
		Handle(nil, "not json", func(string) error {
			called = true
			return nil
		})
		if called {
			t.Fatal("add should not be called for invalid JSON")
		}
	})

	t.Run("valid channel is added", func(t *testing.T) {
		var got string
		Handle(nil, `{"channel":"dev:global:logs:2"}`, func(channel string) error {
			got = channel
			return nil
		})
		if got != "dev:global:logs:2" {
			t.Fatalf("expected dev:global:logs:2, got %q", got)
		}
	})
}
