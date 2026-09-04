package executor

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// TestAntigravityAcpLive drives the real executor against the official
// daemon binary and a converted user profile. It is opt-in
// (CLI_PROXY_ACP_LIVE=1) so normal test runs stay hermetic: it spends real
// model quota and needs network plus a seeded profile.
func TestAntigravityAcpLive(t *testing.T) {
	if os.Getenv("CLI_PROXY_ACP_LIVE") == "" {
		t.Skip("live ACP test disabled without CLI_PROXY_ACP_LIVE=1")
	}
	binary := os.Getenv("CLI_PROXY_ACP_BINARY")
	home := os.Getenv("CLI_PROXY_ACP_HOME")
	if binary == "" || home == "" {
		t.Skip("CLI_PROXY_ACP_BINARY and CLI_PROXY_ACP_HOME must point at the daemon and a seeded profile")
	}
	if st, err := os.Stat(binary); err != nil || st.IsDir() {
		t.Skipf("daemon binary not usable: %v", err)
	}
	if _, err := os.Stat(home + "/antigravity-acp/acp_token.json"); err != nil {
		t.Skipf("seeded profile token missing: %v", err)
	}

	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": binary,
			"gemini_home": home,
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.7-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"Reply with exactly: LIVEOK"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	resp, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("live Execute failed: %v", err)
	}
	var data struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		t.Fatalf("unmarshal live response: %v", err)
	}
	if len(data.Choices) == 0 || !strings.Contains(data.Choices[0].Message.Content, "LIVEOK") {
		t.Fatalf("unexpected live content: %s", resp.Payload)
	}
}

// TestAntigravityAcpLiveResponses drives the real executor with a Responses
// API request and asserts the output is a responses object, not chat JSON.
// Same opt-in gate as TestAntigravityAcpLive.
func TestAntigravityAcpLiveResponses(t *testing.T) {
	if os.Getenv("CLI_PROXY_ACP_LIVE") == "" {
		t.Skip("live ACP test disabled without CLI_PROXY_ACP_LIVE=1")
	}
	binary := os.Getenv("CLI_PROXY_ACP_BINARY")
	home := os.Getenv("CLI_PROXY_ACP_HOME")
	if binary == "" || home == "" {
		t.Skip("CLI_PROXY_ACP_BINARY and CLI_PROXY_ACP_HOME must point at the daemon and a seeded profile")
	}
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": binary,
			"gemini_home": home,
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash-low",
		Payload: []byte(`{"model":"gemini-3.8-flash-low","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Reply with exactly: LIVERESP"}]}]}`),
	}
	opts := cliproxyexecutor.Options{ResponseFormat: sdktranslator.FormatOpenAIResponse}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	resp, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("live responses Execute failed: %v", err)
	}
	var data struct {
		Object string `json:"object"`
		Output []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		t.Fatalf("unmarshal live responses payload: %v\n%s", err, resp.Payload)
	}
	if data.Object != "response" {
		t.Fatalf("object = %q, want response: %s", data.Object, resp.Payload)
	}
	found := false
	for _, item := range data.Output {
		for _, c := range item.Content {
			if strings.Contains(c.Text, "LIVERESP") {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("responses output missing model text: %s", resp.Payload)
	}
}
