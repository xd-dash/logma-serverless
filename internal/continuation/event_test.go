package continuation

import (
	"strings"
	"testing"
	"time"
)

func validEvent(now time.Time) Event {
	return Event{
		Version:  Version,
		ID:       "run-123",
		Channel:  ChannelPrefix + "bootstrap-abc",
		Status:   "success",
		IssuedAt: now,
		ChatURL:  "https://chatgpt.com/c/abc",
		Prompt:   "The action completed. Inspect its artifacts and continue.",
	}
}

func TestEventValidate(t *testing.T) {
	now := time.Now().UTC()
	if err := validEvent(now).Validate(now); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestEventRejectsUntrustedChatURL(t *testing.T) {
	now := time.Now().UTC()
	event := validEvent(now)
	event.ChatURL = "https://chatgpt.com.example/c/abc"
	if err := event.Validate(now); err == nil {
		t.Fatal("expected untrusted chat URL to be rejected")
	}
}

func TestEventRejectsStaleAndOversizedMessages(t *testing.T) {
	now := time.Now().UTC()
	event := validEvent(now)
	event.IssuedAt = now.Add(-25 * time.Hour)
	if err := event.Validate(now); err == nil {
		t.Fatal("expected stale event to be rejected")
	}

	event = validEvent(now)
	event.Prompt = strings.Repeat("x", MaxPromptSize+1)
	if err := event.Validate(now); err == nil {
		t.Fatal("expected oversized prompt to be rejected")
	}
}
