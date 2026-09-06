package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	logrus "github.com/sirupsen/logrus"
)

// step1TTFTLogCapture captures ACP TTFT stage log lines. All methods are
// goroutine-safe: logrus fires hooks from whichever goroutine logs, and
// stream requests log from their release goroutines.
type step1TTFTLogCapture struct {
	mu    sync.Mutex
	lines []logrus.Fields
}

func (c *step1TTFTLogCapture) Fire(e *logrus.Entry) error {
	if strings.Contains(e.Message, "ACP TTFT stage timings") {
		cp := logrus.Fields{}
		for k, v := range e.Data {
			cp[k] = v
		}
		c.mu.Lock()
		c.lines = append(c.lines, cp)
		c.mu.Unlock()
	}
	return nil
}

func (c *step1TTFTLogCapture) snapshot() []logrus.Fields {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]logrus.Fields, len(c.lines))
	copy(out, c.lines)
	return out
}

// waitFor blocks until at least n lines arrived or the timeout expires.
func (c *step1TTFTLogCapture) waitFor(n int, timeout time.Duration) []logrus.Fields {
	deadline := time.Now().Add(timeout)
	for {
		lines := c.snapshot()
		if len(lines) >= n {
			return lines
		}
		if time.Now().After(deadline) {
			return lines
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (c *step1TTFTLogCapture) Levels() []logrus.Level { return logrus.AllLevels }

// step1AgentScript builds a fake ACP agent: session/new reports
// currentModel; every session/new, set_config_option and prompt call is
// appended to methods.log; prompts answer instantly with one text chunk.
func step1AgentScript(t *testing.T, currentModel string) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"LOG_DIR=\"$(dirname \"$0\")\"\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo session/new >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-1\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"" + currentModel + "\",\"options\":[{\"value\":\"" + currentModel + "\"},{\"value\":\"gemini-3.8-flash-high\"},{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo set_config_option >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash-low\",\"options\":[{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    echo prompt >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-1\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"ok\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	return scriptPath
}

// step1SessionNewFailsScript builds a fake agent whose session/new fails,
// to exercise the early-failure cleanup path (P0.3).
func step1SessionNewFailsScript(t *testing.T, withAttachment bool) (string, string) {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"while IFS= read -r line; do\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":1}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000,\"message\":\"session refused\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	payload := []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	if withAttachment {
		// A tiny valid BMP (1x1 pixel) staged by the attachment builder.
		const bmp = "QkEgAAAAAAAAAHgAAAAoAAAAAQAAAAEAAAABAAgAAAAAAAQAAAAsAAAAKwAAAAAABgBmAGYAAAD/AP8A/wAA/wD/AP8A/wAA"
		payload = []byte(`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/bmp;base64,` + bmp + `"}}]}]}`)
	}
	return scriptPath, string(payload)
}

func step1RunOne(t *testing.T, execer *AntigravityAcpExecutor, auth *cliproxyauth.Auth, model, payload string) {
	t.Helper()
	req := cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(payload),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
}

// TestStep1_SessionModePreparedAndMetrics verifies the P0.2 session_mode
// field end to end: request 1 reports fresh (cold, inline session/new),
// request 2 reports prepared (pop from the refill cache) and carries the
// derived derived-metric fields in the same log line.
func TestStep1_SessionModePreparedAndMetrics(t *testing.T) {
	script := step1AgentScript(t, "gemini-3.8-flash")
	cfg := &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	capture := &step1TTFTLogCapture{}
	logrus.AddHook(capture)
	defer logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{})

	step1RunOne(t, execer, auth, "gemini-3.8-flash", `{"messages":[{"role":"user","content":"hi"}]}`)

	// Wait for the post-release refill to land a prepared session before
	// firing request 2: an arrival inside the release->refill-start window
	// legitimately takes the inline fresh path (documented fallback).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		key := execer.authPoolKey(auth)
		if ws := execer.pool.Workers(key); len(ws) > 0 && len(ws[0].SnapshotReadySessions()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	step1RunOne(t, execer, auth, "gemini-3.8-flash", `{"messages":[{"role":"user","content":"hi"}]}`)

	lines := capture.waitFor(2, 3*time.Second)
	if len(lines) < 2 {
		t.Fatalf("expected 2 TTFT log lines, got %d", len(lines))
	}
	first, second := lines[0], lines[1]
	if got := first["session_mode"]; got != "fresh" {
		t.Fatalf("request 1 session_mode = %v, want fresh", got)
	}
	if got := second["session_mode"]; got != "prepared" {
		t.Fatalf("request 2 session_mode = %v, want prepared", got)
	}
	// The derived metrics must be present and non-negative.
	for _, f := range []string{"pool_wait_ms", "session_new_ms", "prompt_write_ms", "backend_to_first_output_ms", "first_token_ttft_ms"} {
		for i, line := range lines[:2] {
			v, ok := line[f]
			if !ok {
				t.Fatalf("request %d missing %s", i+1, f)
			}
			if n, ok := v.(int64); ok && n < 0 {
				t.Fatalf("request %d %s = %d, must not be negative", i+1, f, n)
			}
		}
	}
}

// TestStep1_PreparedModelAwareSkipsSetConfigOption verifies P0.5 end to
// end: after a low-variant request, the refill prepares for the SAME
// variant, so the next low request pops a prepared session whose model
// already matches and performs no session/set_config_option.
func TestStep1_PreparedModelAwareSkipsSetConfigOption(t *testing.T) {
	script := step1AgentScript(t, "gemini-3.8-flash")
	cfg := &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	// Request 1 (cold, inline new + set_config_option), then its release
	// refills model-aware (new + pre-select set_config_option off the hot
	// path). Request 2 must hit prepared with matching model: no session/new
	// and no set_config_option of its own.
	step1RunOne(t, execer, auth, "gemini-3.8-flash-low", `{"messages":[{"role":"user","content":"hi"}]}`)

	// Wait until a model-aware prepared session exists (refill ran to
	// completion) before firing request 2.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		key := execer.authPoolKey(auth)
		if ws := execer.pool.Workers(key); len(ws) > 0 {
			if ready := ws[0].SnapshotReadySessions(); len(ready) > 0 && ready[0].Variant == "gemini-3.8-flash-low" {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	step1RunOne(t, execer, auth, "gemini-3.8-flash-low", `{"messages":[{"role":"user","content":"hi"}]}`)

	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), "methods.log"))
	if err != nil {
		t.Fatalf("read methods log: %v", err)
	}
	logText := string(raw)
	news := strings.Count(logText, "session/new")
	setcfgs := strings.Count(logText, "set_config_option")
	prompts := strings.Count(logText, "prompt")
	// Expected ledger: request 1 inline (new + setcfg) + refill background
	// (new + pre-select setcfg) + request 2 prepared hit (prompt only).
	if news != 2 {
		t.Fatalf("session/new called %d times; want 2 (request 1 inline + refill): request 2 must pop the prepared session\nlog: %s", news, logText)
	}
	if setcfgs != 2 {
		t.Fatalf("set_config_option called %d times; want 2 (request 1 inline + refill pre-selection): request 2 must skip model config entirely\nlog: %s", setcfgs, logText)
	}
	if prompts != 2 {
		t.Fatalf("prompt called %d times; want 2\nlog: %s", prompts, logText)
	}
}

// TestStep1_SessionFailureCleansStagedAttachments verifies P0.3: when
// session/new fails after the parallel prompt build staged attachments,
// the request returns an error AND the staged temp directory is removed.
func TestStep1_SessionFailureCleansStagedAttachments(t *testing.T) {
	script, payload := step1SessionNewFailsScript(t, true)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	req := cliproxyexecutor.Request{Model: "gemini-3.8-flash", Payload: []byte(payload)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err == nil {
		t.Fatalf("expected session/new failure to surface as an error")
	}

	// No acp-attach-* staging dir may survive the failed request.
	matches, _ := filepath.Glob("/tmp/acp-attach-*")
	for _, m := range matches {
		info, statErr := os.Stat(m)
		if statErr == nil && time.Since(info.ModTime()) < time.Minute {
			t.Fatalf("staged attachment dir %s leaked after session failure", m)
		}
	}
}

// TestStep1_StreamSessionModePrepared mirrors the mode check on the
// streaming path, where logStageTimings runs in the release defer.
func TestStep1_StreamSessionModePrepared(t *testing.T) {
	script := step1AgentScript(t, "gemini-3.8-flash")
	cfg := &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	capture := &step1TTFTLogCapture{}
	logrus.AddHook(capture)
	defer logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{})

	streamOnce := func() {
		req := cliproxyexecutor.Request{
			Model:   "gemini-3.8-flash",
			Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := execer.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{})
		if err != nil {
			t.Fatalf("ExecuteStream failed: %v", err)
		}
		for c := range res.Chunks {
			if c.Err != nil {
				t.Fatalf("stream chunk error: %v", c.Err)
			}
		}
	}

	streamOnce() // fresh
	capture.waitFor(1, 3*time.Second)
	streamOnce() // prepared
	lines := capture.waitFor(2, 3*time.Second)
	if len(lines) < 2 {
		t.Fatalf("expected 2 TTFT log lines, got %d", len(lines))
	}
	if got := lines[0]["session_mode"]; got != "fresh" {
		t.Fatalf("stream request 1 session_mode = %v, want fresh", got)
	}
	if got := lines[1]["session_mode"]; got != "prepared" {
		t.Fatalf("stream request 2 session_mode = %v, want prepared", got)
	}
}

// TestStep1_WriteHookStampsPromptWrite verifies the P0.2 wiring: the
// promptWritten stage is stamped from the actual client write path, so it
// precedes Prompt() return and is recorded at all.
func TestStep1_WriteHookStampsPromptWrite(t *testing.T) {
	script := step1AgentScript(t, "gemini-3.8-flash")
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	capture := &step1TTFTLogCapture{}
	logrus.AddHook(capture)
	defer logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{})

	step1RunOne(t, execer, auth, "gemini-3.8-flash", `{"messages":[{"role":"user","content":"hi"}]}`)
	lines := capture.waitFor(1, 3*time.Second)
	if len(lines) < 1 {
		t.Fatalf("expected 1 TTFT log line")
	}
	v, ok := lines[0]["prompt_write_ms"]
	if !ok {
		t.Fatalf("prompt_write_ms missing")
	}
	if n, ok := v.(int64); !ok || n < 0 {
		t.Fatalf("prompt_write_ms = %v; the actual write-path stamp must be recorded", v)
	}
}
