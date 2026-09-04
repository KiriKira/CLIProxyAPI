package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func createFakeAgentScript(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")

	// Minimal fake ACP agent in bash. It echoes the request id back so the
	// client matches replies regardless of call sequencing, and logs every
	// session-level method to methods.log for handshake-order assertions.
	script := "#!/usr/bin/env bash\n" +
		"LOG_DIR=\"$(dirname \"$0\")\"\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo initialize >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1,\"capabilities\":{},\"agentInfo\":{\"name\":\"fake-agent\",\"version\":\"1.0.0\"}}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo authenticate >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo session/new >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-123\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"options\":[{\"value\":\"gemini-3.8-flash\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    echo session/prompt >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-123\",\"update\":{\"sessionUpdate\":\"agent_thought_chunk\",\"content\":{\"type\":\"text\",\"text\":\"thinking step\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-123\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"hello from acp\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake agent script: %v", err)
	}

	return scriptPath
}

// methodsSeen returns the method names the fake agent observed, in order.
func methodsSeen(t *testing.T, script string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), "methods.log"))
	if err != nil {
		t.Fatalf("read methods log: %v", err)
	}
	var out []string
	for _, m := range strings.Split(string(raw), "\n") {
		if m != "" {
			out = append(out, m)
		}
	}
	return out
}

func assertHandshakeOrder(t *testing.T, script string) {
	t.Helper()
	got := methodsSeen(t, script)
	want := []string{"initialize", "authenticate", "session/new", "session/prompt"}
	if len(got) != len(want) {
		t.Fatalf("methods = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("methods = %v, want %v", got, want)
		}
	}
}

func TestAntigravityAcpExecutorExecute(t *testing.T) {
	script := createFakeAgentScript(t)
	geminiHome := t.TempDir()
	cfg := &internalconfig.Config{}
	exec := NewAntigravityAcpExecutor(cfg)

	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": geminiHome,
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(resp.Payload) == 0 {
		t.Fatalf("expected non-empty payload")
	}

	var data struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(data.Choices) == 0 {
		t.Fatalf("expected choices in response")
	}

	if data.Choices[0].Message.Content != "hello from acp" {
		t.Errorf("content = %q, want 'hello from acp'", data.Choices[0].Message.Content)
	}
	if data.Choices[0].Message.ReasoningContent != "thinking step" {
		t.Errorf("reasoning_content = %q, want 'thinking step'", data.Choices[0].Message.ReasoningContent)
	}
	if data.Choices[0].FinishReason != "end_turn" {
		t.Errorf("finish_reason = %q, want 'end_turn'", data.Choices[0].FinishReason)
	}

	assertHandshakeOrder(t, script)

	// Profile settings must pin the default auth method without credentials.
	raw, err := os.ReadFile(filepath.Join(geminiHome, "antigravity-acp", "settings.json"))
	if err != nil {
		t.Fatalf("read profile settings: %v", err)
	}
	var settings struct {
		Auth struct {
			Type string `json:"type"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(raw, &settings); err != nil {
		t.Fatalf("unmarshal profile settings: %v", err)
	}
	if settings.Auth.Type != "oauth-personal" {
		t.Errorf("settings auth.type = %q, want oauth-personal", settings.Auth.Type)
	}
}

func TestAntigravityAcpExecutorExecuteStream(t *testing.T) {
	script := createFakeAgentScript(t)
	cfg := &internalconfig.Config{}
	exec := NewAntigravityAcpExecutor(cfg)

	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}

	var receivedChunks []string
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		receivedChunks = append(receivedChunks, string(chunk.Payload))
	}

	if len(receivedChunks) == 0 {
		t.Fatalf("expected streamed chunks")
	}

	var hasThought, hasMessage, hasDone bool
	for _, c := range receivedChunks {
		if c == "data: [DONE]\n\n" {
			hasDone = true
		}
		if len(c) > 0 && c != "data: [DONE]\n\n" {
			if len(c) > 6 && c[:6] == "data: " {
				var delta struct {
					Choices []struct {
						Delta struct {
							Content          string `json:"content"`
							ReasoningContent string `json:"reasoning_content"`
						} `json:"delta"`
					} `json:"choices"`
				}
				_ = json.Unmarshal([]byte(c[6:]), &delta)
				if len(delta.Choices) > 0 {
					if delta.Choices[0].Delta.Content == "hello from acp" {
						hasMessage = true
					}
					if delta.Choices[0].Delta.ReasoningContent == "thinking step" {
						hasThought = true
					}
				}
			}
		}
	}

	if !hasThought {
		t.Errorf("missing expected thought chunk in stream")
	}
	if !hasMessage {
		t.Errorf("missing expected message chunk in stream")
	}
	if !hasDone {
		t.Errorf("missing [DONE] terminal chunk in stream")
	}

	assertHandshakeOrder(t, script)
}

func TestAntigravityAcpExecutorRejectsUnknownAuthMethod(t *testing.T) {
	script := createFakeAgentScript(t)
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
			"auth_method": "bogus",
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err == nil || !strings.Contains(err.Error(), "unknown Antigravity ACP auth method") {
		t.Fatalf("expected unknown-method error, got %v", err)
	}
}

func TestAntigravityAcpExecutorGeminiAPIKeyNeedsKey(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "")
	script := createFakeAgentScript(t)
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
			"auth_method": "gemini-api-key",
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err == nil || !strings.Contains(err.Error(), "needs an api_key") {
		t.Fatalf("expected missing-key error, got %v", err)
	}
}

func TestAntigravityAcpExecutorMissingBinary(t *testing.T) {
	t.Setenv("AGY_ACP_BINARY", "")
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": filepath.Join(t.TempDir(), "no-such-agent.par"),
			"gemini_home": t.TempDir(),
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err == nil || !strings.Contains(err.Error(), "binary not found") {
		t.Fatalf("expected binary-missing error, got %v", err)
	}
}

func TestSanitizeProfileName(t *testing.T) {
	if got := sanitizeProfileName("user@example.com"); got != "user_example.com" {
		t.Errorf("got %q", got)
	}
	if got := sanitizeProfileName(""); got != "default" {
		t.Errorf("got %q", got)
	}
}

func writeDaemonToken(t *testing.T, dir, name, blob string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "antigravity-acp"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "antigravity-acp", name), []byte(blob), 0600); err != nil {
		t.Fatal(err)
	}
}

func readDaemonToken(t *testing.T, dir, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "antigravity-acp", name))
	if err != nil {
		t.Fatal(err)
	}
	var blob map[string]any
	if err := json.Unmarshal(raw, &blob); err != nil {
		t.Fatal(err)
	}
	return blob
}

func TestEnsureDaemonTokenRefreshesStaleBlob(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"fresh-at","expires_in":3600,"token_type":"Bearer"}`))
	}))
	t.Cleanup(srv.Close)
	oldEndpoint := acpTokenEndpoint
	acpTokenEndpoint = srv.URL
	t.Cleanup(func() { acpTokenEndpoint = oldEndpoint })

	dir := t.TempDir()
	writeDaemonToken(t, dir, "acp_token.json", `{"client_id":"cid","client_secret":"csec","refresh_token":"rt","token_uri":"x","scopes":["s"],"project_id":"p"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ensureDaemonToken(ctx, dir, "oauth-personal"); err != nil {
		t.Fatalf("ensureDaemonToken: %v", err)
	}
	if hits != 1 {
		t.Fatalf("refresh hits = %d, want 1", hits)
	}
	blob := readDaemonToken(t, dir, "acp_token.json")
	if blob["token"] != "fresh-at" {
		t.Errorf("token = %v, want fresh-at", blob["token"])
	}
	if blob["refresh_token"] != "rt" {
		t.Errorf("refresh_token was clobbered: %v", blob["refresh_token"])
	}
	if blob["project_id"] != "p" {
		t.Errorf("project_id was dropped: %v", blob["project_id"])
	}
	expiry, _ := blob["expiry"].(string)
	exp, err := time.Parse(time.RFC3339, expiry)
	if err != nil || time.Until(exp) < 50*time.Minute {
		t.Errorf("expiry = %v, err = %v, want ~1h future", expiry, err)
	}
}

func TestEnsureDaemonTokenSkipsFreshBlob(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
	}))
	t.Cleanup(srv.Close)
	oldEndpoint := acpTokenEndpoint
	acpTokenEndpoint = srv.URL
	t.Cleanup(func() { acpTokenEndpoint = oldEndpoint })

	dir := t.TempDir()
	expiry := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	writeDaemonToken(t, dir, "acp_token.json", `{"refresh_token":"rt","token":"at","expiry":"`+expiry+`"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ensureDaemonToken(ctx, dir, "oauth-personal"); err != nil {
		t.Fatalf("ensureDaemonToken: %v", err)
	}
	if hits != 0 {
		t.Fatalf("refresh hits = %d, want 0", hits)
	}
}

func TestEnsureDaemonTokenToleratesMissingState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// No file at all.
	if err := ensureDaemonToken(ctx, t.TempDir(), "oauth-personal"); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	// API-key methods keep no file.
	if err := ensureDaemonToken(ctx, t.TempDir(), "gemini-api-key"); err != nil {
		t.Fatalf("api-key method: %v", err)
	}
	// Blob without refresh token.
	dir := t.TempDir()
	writeDaemonToken(t, dir, "acp_token.json", `{"client_id":"cid"}`)
	if err := ensureDaemonToken(ctx, dir, "oauth-personal"); err != nil {
		t.Fatalf("no refresh token: %v", err)
	}
}

// TestAntigravityAcpExecutorHangingAuthenticateFailsFast covers a fresh
// profile: the real agent prints a plain-text auth URL and blocks instead of
// answering authenticate, so the bounded step must surface 401 instead of
// hanging until the caller's context expires.
func TestAntigravityAcpExecutorHangingAuthenticateFailsFast(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "hanging_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1,\"capabilities\":{},\"agentInfo\":{\"name\":\"hanging\",\"version\":\"1.0.0\"}}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("write hanging agent script: %v", err)
	}

	oldTimeout := acpAuthenticateTimeout
	acpAuthenticateTimeout = 200 * time.Millisecond
	t.Cleanup(func() { acpAuthenticateTimeout = oldTimeout })

	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": scriptPath,
			"gemini_home": t.TempDir(),
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err == nil || !strings.Contains(err.Error(), "sign-in required") {
		t.Fatalf("expected sign-in-required error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("authenticate hang took %v, want a fast 401", elapsed)
	}
}
