package acp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// reviewWireWriter records actual client writes without a live cloud agent.
type reviewWireWriter struct{ messages chan wireMessage }

func (w *reviewWireWriter) Write(b []byte) (int, error) {
	var msg wireMessage
	if err := json.Unmarshal(b, &msg); err != nil {
		return 0, err
	}
	w.messages <- msg
	return len(b), nil
}
func (w *reviewWireWriter) Close() error { return nil }

// TestReviewAblationCancellation characterizes context-only cancellation
// against an explicit ACP cancellation control; it does not test cloud stop.
func TestReviewAblationCancellation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "context_only"
		if explicit {
			name = "explicit_acp_cancel"
		}
		t.Run(name, func(t *testing.T) {
			wire := &reviewWireWriter{messages: make(chan wireMessage, 8)}
			r, w := io.Pipe()
			c := newClientWithPipes(wire, r, strings.NewReader(""), nil, nil)
			defer func() { _ = c.Close(); _ = w.Close(); _ = r.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.Prompt(ctx, "review-session", NewTextPrompt("probe")); done <- err }()
			first := <-wire.messages
			if first.Method != "session/prompt" {
				t.Fatalf("unexpected method %s", first.Method)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
			cancels := 0
			if explicit {
				if err := c.Cancel("review-session"); err != nil {
					t.Fatal(err)
				}
			}
			for len(wire.messages) > 0 {
				if m := <-wire.messages; m.Method == "session/cancel" {
					cancels++
				}
			}
			expected := 0
			if explicit {
				expected = 1
			}
			if cancels != expected {
				t.Fatalf("cancel frames %d, expected %d", cancels, expected)
			}
			c.pendingMu.Lock()
			pending := len(c.pending)
			c.pendingMu.Unlock()
			t.Logf("context_cancelled=true explicit_cancel=%t cancel_frames=%d pending_calls=%d", explicit, cancels, pending)
		})
	}
}
