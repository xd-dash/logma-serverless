package continuation

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var channelPattern = regexp.MustCompile(`^chatgpt:continuation:[A-Za-z0-9._:-]{8,160}$`)

const (
	Version       = "v1"
	ChannelPrefix = "chatgpt:continuation:"
	MaxPromptSize = 32 << 10
)

type Event struct {
	Version     string         `json:"version"`
	ID          string         `json:"id"`
	Channel     string         `json:"channel"`
	Status      string         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	ChatURL     string         `json:"chat_url,omitempty"`
	Prompt      string         `json:"prompt"`
	Repository  string         `json:"repository,omitempty"`
	Ref         string         `json:"ref,omitempty"`
	WorkflowURL string         `json:"workflow_url,omitempty"`
	Artifacts   []string       `json:"artifacts,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
}

func (e Event) Validate(now time.Time) error {
	if e.Version != Version {
		return fmt.Errorf("version must be %q", Version)
	}
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("id is required")
	}
	if !channelPattern.MatchString(e.Channel) {
		return fmt.Errorf("channel must start with %q and have an 8-160 character safe identifier", ChannelPrefix)
	}
	if e.Status != "success" && e.Status != "failure" && e.Status != "cancelled" {
		return errors.New("status must be success, failure, or cancelled")
	}
	if e.IssuedAt.IsZero() || e.IssuedAt.Before(now.Add(-24*time.Hour)) || e.IssuedAt.After(now.Add(5*time.Minute)) {
		return errors.New("issued_at is outside the accepted window")
	}
	if strings.TrimSpace(e.Prompt) == "" || len(e.Prompt) > MaxPromptSize {
		return fmt.Errorf("prompt must contain between 1 and %d bytes", MaxPromptSize)
	}
	if e.ChatURL != "" && !allowedChatURL(e.ChatURL) {
		return errors.New("chat_url must be an https://chatgpt.com URL")
	}
	return nil
}

func allowedChatURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() == "chatgpt.com"
}
