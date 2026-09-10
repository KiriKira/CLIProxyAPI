package executor

import (
	"context"
	"encoding/base64"
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
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
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
				Content string `json:"content"`
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
	// The non-stream message must stay within the OpenAI schema: strict
	// client validators reject non-standard fields such as reasoning_content.
	if strings.Contains(string(resp.Payload), "reasoning_content") {
		t.Errorf("non-stream response must not carry reasoning_content (strict clients reject non-standard fields)")
	}
	if data.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want 'stop' (OpenAI-compatible normalization of the ACP end_turn)", data.Choices[0].FinishReason)
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

	var hasThought, hasMessage, hasPrefixedDone bool
	for _, c := range receivedChunks {
		if c == "data: [DONE]\n\n" {
			hasPrefixedDone = true
		}
		if len(c) > 0 {
			var delta struct {
				Choices []struct {
					Delta struct {
						Content          string `json:"content"`
						ReasoningContent string `json:"reasoning_content"`
					} `json:"delta"`
				} `json:"choices"`
			}
			_ = json.Unmarshal([]byte(c), &delta)
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

	if !hasThought {
		t.Errorf("missing expected thought chunk in stream")
	}
	if !hasMessage {
		t.Errorf("missing expected message chunk in stream")
	}
	// The Chat Completions outer writer appends its own `data: [DONE]` when
	// the stream channel closes. The executor must NOT emit a prefixed
	// terminal chunk here, or the client sees a `data: data: [DONE]` frame.
	if hasPrefixedDone {
		t.Errorf("executor must not emit a prefixed [DONE] chunk on the Chat Completions path (outer writer owns the terminal marker)")
	}

	assertHandshakeOrder(t, script)
}

// TestAntigravityAcpExecutorExecuteStreamResponsesTerminal verifies the
// Responses path: the executor emits a bare [DONE] terminal marker which the
// translator consumes to synthesize response.completed.
func TestAntigravityAcpExecutorExecuteStreamResponsesTerminal(t *testing.T) {
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
	opts := cliproxyexecutor.Options{ResponseFormat: sdktranslator.FormatOpenAIResponse}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}

	var sawCompleted bool
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if strings.Contains(string(chunk.Payload), "response.completed") {
			sawCompleted = true
		}
	}
	if !sawCompleted {
		t.Errorf("Responses streaming must synthesize response.completed from the bare [DONE] terminal marker")
	}
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

func TestResolveAntigravityModel(t *testing.T) {
	cases := []struct {
		name    string
		model   string
		payload string
		want    string
	}{
		{"bare family defaults high", "gemini-3.7-flash", `{}`, "gemini-3.7-flash-high"},
		{"explicit variant kept", "gemini-3.7-flash-low", `{}`, "gemini-3.7-flash-low"},
		{"body effort overrides variant", "gemini-3.7-flash-low", `{"reasoning_effort":"high"}`, "gemini-3.7-flash-high"},
		{"body effort fills bare family", "gemini-3.8-flash", `{"reasoning_effort":"low"}`, "gemini-3.8-flash-low"},
		{"level suffix", "gemini-3.8-flash(high)", `{}`, "gemini-3.8-flash-high"},
		{"suffix wins over body", "gemini-3.8-flash(low)", `{"reasoning_effort":"high"}`, "gemini-3.8-flash-low"},
		{"xhigh clamps to high", "gemini-3.6-flash(xhigh)", `{}`, "gemini-3.6-flash-high"},
		{"minimal clamps to low", "gemini-3.6-flash(minimal)", `{}`, "gemini-3.6-flash-low"},
		{"numeric budget means high", "gemini-3.7-flash(8192)", `{}`, "gemini-3.7-flash-high"},
		{"none means low", "gemini-3.7-flash(none)", `{}`, "gemini-3.7-flash-low"},
		{"auto means medium", "gemini-3.7-flash(auto)", `{}`, "gemini-3.7-flash-medium"},
		{"pro high id kept", "gemini-pro-agent", `{}`, "gemini-pro-agent"},
		{"pro low via body", "gemini-pro-agent", `{"reasoning_effort":"low"}`, "gemini-3.1-pro-low"},
		{"pro low via suffix", "gemini-3.1-pro(low)", `{}`, "gemini-3.1-pro-low"},
		{"pro medium clamps up", "gemini-3.1-pro(medium)", `{}`, "gemini-pro-agent"},
		{"bare pro means high id", "gemini-3.1-pro", `{}`, "gemini-pro-agent"},
		{"unknown family passes through", "some-other-model", `{}`, "some-other-model"},
		{"unknown suffix stripped", "some-other-model(ultra)", `{}`, "some-other-model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveAntigravityModel(tc.model, []byte(tc.payload)); got != tc.want {
				t.Errorf("resolveAntigravityModel(%q, %s) = %q, want %q", tc.model, tc.payload, got, tc.want)
			}
		})
	}
}

const acpTestPixelPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="

func TestBuildPromptBlocksTextCompat(t *testing.T) {
	blocks, cleanup, err := buildPromptBlocks([]byte(`{"messages":[{"role":"user","content":"hi"}]}`))
	defer cleanup()
	if err != nil {
		t.Fatalf("buildPromptBlocks: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Type != "text" || blocks[0].Text != "[USER]: hi" {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}
}

func TestBuildPromptBlocksAttachments(t *testing.T) {
	payload := `{"messages":[{"role":"user","content":[
		{"type":"text","text":"describe these"},
		{"type":"image_url","image_url":{"url":"data:image/png;base64,` + acpTestPixelPNG + `"}},
		{"type":"input_audio","input_audio":{"data":"AAAA","format":"mp3"}},
		{"type":"file","file":{"file_data":"data:application/pdf;base64,JVBERi0xLjQgZmFrZQ==","filename":"doc.pdf"}},
		{"type":"file","file":{"file_data":"data:text/plain;base64,aGVsbG8gd29ybGQ=","filename":"notes.txt"}}
	]}]}`
	blocks, cleanup, err := buildPromptBlocks([]byte(payload))
	if err != nil {
		t.Fatalf("buildPromptBlocks: %v", err)
	}
	if len(blocks) != 5 {
		t.Fatalf("got %d blocks, want 5: %+v", len(blocks), blocks)
	}
	if blocks[0].Type != "text" || blocks[0].Text != "[USER]: describe these" {
		t.Errorf("text block = %+v", blocks[0])
	}
	if blocks[1].Type != "image" || blocks[1].MimeType != "image/png" || blocks[1].Data != acpTestPixelPNG {
		t.Errorf("image block = %+v", blocks[1])
	}
	if blocks[2].Type != "audio" || blocks[2].MimeType != "audio/mpeg" {
		t.Errorf("audio block = %+v", blocks[2])
	}
	if blocks[3].Type != "resource_link" || blocks[3].MimeType != "application/pdf" || !strings.HasPrefix(blocks[3].URI, "file://") {
		t.Errorf("pdf block = %+v", blocks[3])
	} else if _, statErr := os.Stat(strings.TrimPrefix(blocks[3].URI, "file://")); statErr != nil {
		t.Errorf("staged pdf missing: %v", statErr)
	}
	if blocks[4].Type != "resource" || !strings.Contains(string(blocks[4].Resource), "hello world") {
		t.Errorf("text file block = %+v", blocks[4])
	}
	cleanup()
	if strings.HasPrefix(blocks[3].URI, "file://") {
		if _, statErr := os.Stat(strings.TrimPrefix(blocks[3].URI, "file://")); !os.IsNotExist(statErr) {
			t.Errorf("staged pdf not cleaned: %v", statErr)
		}
	}
}

func TestBuildPromptBlocksRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"unsupported image mime", `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/gif;base64,AAAA"}}]}]}`, "does not support"},
		{"remote url", `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]}`, "data URLs"},
		{"unknown audio format", `{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AAAA","format":"midi"}}]}]}`, "does not support"},
		{"oversize text", `{"messages":[{"role":"user","content":[{"type":"file","file":{"file_data":"data:text/plain;base64,` + base64.StdEncoding.EncodeToString(make([]byte, 2<<20)) + `","filename":"big.txt"}}]}]}`, "too large"},
		{"binary text file", `{"messages":[{"role":"user","content":[{"type":"file","file":{"file_data":"data:text/plain;base64,AABh","filename":"bin.txt"}}]}]}`, "not a UTF-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, cleanup, err := buildPromptBlocks([]byte(tc.payload))
			defer cleanup()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestBuildPromptBlocksResponsesInput(t *testing.T) {
	payload := `{"input":[
		{"type":"message","role":"user","content":[{"type":"input_text","text":"look"}]},
		{"type":"input_image","image_url":"data:image/png;base64,` + acpTestPixelPNG + `"}
	]}`
	blocks, cleanup, err := buildPromptBlocks([]byte(payload))
	defer cleanup()
	if err != nil {
		t.Fatalf("buildPromptBlocks: %v", err)
	}
	if len(blocks) != 2 || blocks[0].Type != "text" || blocks[1].Type != "image" {
		t.Fatalf("unexpected blocks: %+v", blocks)
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

func TestAntigravityAcpExecutorExecuteResponsesFormat(t *testing.T) {
	script := createFakeAgentScript(t)
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
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
	opts := cliproxyexecutor.Options{ResponseFormat: sdktranslator.FormatOpenAIResponse}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	var data struct {
		Object string `json:"object"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		t.Fatalf("unmarshal responses payload: %v\n%s", err, resp.Payload)
	}
	if data.Object != "response" {
		t.Errorf("object = %q, want response", data.Object)
	}
	found := false
	for _, item := range data.Output {
		for _, c := range item.Content {
			if c.Text == "hello from acp" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("responses output missing model text: %s", resp.Payload)
	}
}

func TestAntigravityAcpExecutorStreamResponsesFormat(t *testing.T) {
	script := createFakeAgentScript(t)
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
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
	opts := cliproxyexecutor.Options{ResponseFormat: sdktranslator.FormatOpenAIResponse}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}
	var joined strings.Builder
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		joined.Write(chunk.Payload)
	}
	out := joined.String()
	if !strings.Contains(out, "response.output_text.delta") {
		t.Errorf("missing output_text deltas in responses stream:\n%s", out)
	}
	if !strings.Contains(out, "response.completed") {
		t.Errorf("missing response.completed terminal event:\n%s", out)
	}
	if strings.Contains(out, `"choices"`) {
		t.Errorf("chat-style chunk leaked into responses stream:\n%s", out)
	}
}

func TestBuildPromptBlocksGeminiContents(t *testing.T) {
	payload := `{"contents":[
		{"role":"user","parts":[
			{"text":"see image"},
			{"inlineData":{"mimeType":"image/png","data":"` + acpTestPixelPNG + `"}},
			{"inlineData":{"mimeType":"application/pdf","data":"JVBERi0xLjQgZmFrZQ=="}}
		]},
		{"role":"model","parts":[{"text":"noted"}]}
	]}`
	blocks, cleanup, err := buildPromptBlocks([]byte(payload))
	defer cleanup()
	if err != nil {
		t.Fatalf("buildPromptBlocks: %v", err)
	}
	if len(blocks) != 3 {
		t.Fatalf("got %d blocks, want 3: %+v", len(blocks), blocks)
	}
	if blocks[0].Type != "text" || !strings.Contains(blocks[0].Text, "[USER]: see image") || !strings.Contains(blocks[0].Text, "[ASSISTANT]: noted") {
		t.Errorf("text block = %+v", blocks[0])
	}
	if blocks[1].Type != "image" || blocks[1].MimeType != "image/png" {
		t.Errorf("image block = %+v", blocks[1])
	}
	if blocks[2].Type != "resource_link" || blocks[2].MimeType != "application/pdf" {
		t.Errorf("pdf block = %+v", blocks[2])
	}
}

func TestBuildPromptBlocksClaudeImage(t *testing.T) {
	payload := `{"messages":[{"role":"user","content":[
		{"type":"text","text":"what"},
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + acpTestPixelPNG + `"}}
	]}]}`
	blocks, cleanup, err := buildPromptBlocks([]byte(payload))
	defer cleanup()
	if err != nil {
		t.Fatalf("buildPromptBlocks: %v", err)
	}
	if len(blocks) != 2 || blocks[0].Type != "text" || blocks[1].Type != "image" {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}
}

func TestAntigravityAcpExecutorPersistentWorkerReuse(t *testing.T) {
	script := createFakeAgentScript(t)
	geminiHome := t.TempDir()
	cfg := &internalconfig.Config{}
	exec := NewAntigravityAcpExecutor(cfg)

	auth := &cliproxyauth.Auth{
		ID: "test-auth-reuse",
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

	ctx := context.Background()

	// First execution
	resp1, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("Execute 1 failed: %v", err)
	}
	if len(resp1.Payload) == 0 {
		t.Fatalf("expected non-empty payload in resp1")
	}

	// Second execution on same executor & auth should reuse the process
	resp2, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("Execute 2 failed: %v", err)
	}
	if len(resp2.Payload) == 0 {
		t.Fatalf("expected non-empty payload in resp2")
	}

	// Fake agent logs methods to methods.log
	got := methodsSeen(t, script)
	// Should have initialized and authenticated once, but created session twice
	want := []string{"initialize", "authenticate", "session/new", "session/prompt", "session/new", "session/prompt"}
	if len(got) != len(want) {
		t.Fatalf("methods = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("methods[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
