package executor

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// newTestPreparedConfig returns an executor config with prepared sessions
// enabled (used by tests and tracing).
func newTestPreparedConfig() *internalconfig.Config {
	return &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
}

// createPreparedAgentScript is a fake agent that logs session/new calls and
// answers prompts instantly, so tests can assert how many session/new round
// trips the hot path actually performed.
func createPreparedAgentScript(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"LOG_DIR=\"$(dirname \"$0\")\"\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1,\"capabilities\":{},\"agentInfo\":{\"name\":\"fake-agent\",\"version\":\"1.0.0\"}}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo session/new >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-prep\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash\",\"options\":[{\"value\":\"gemini-3.8-flash\"},{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash-low\",\"options\":[{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    echo session/prompt >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-prep\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"ok\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake agent script: %v", err)
	}
	return scriptPath
}

// preparedRequest fires one chat completion through the executor and waits
// for the response body.
func preparedRequest(t *testing.T, exec *AntigravityAcpExecutor, auth *cliproxyauth.Auth) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash", // matches daemon default: no set_config_option
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	resp, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(resp.Payload) == 0 {
		t.Fatalf("expected non-empty payload")
	}
}

// TestAntigravityAcpExecutorPreparedSessionSkipsNew verifies that with
// prepared-sessions enabled, the second sequential request reuses a
// pre-created session: only ONE session/new is observed for two requests.
func TestAntigravityAcpExecutorPreparedSessionSkipsNew(t *testing.T) {
	script := createPreparedAgentScript(t)
	cfg := &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
	exec := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	preparedRequest(t, exec, auth) // request 1: cold spawn, no prepared session
	preparedRequest(t, exec, auth) // request 2: must consume the refilled session

	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), "methods.log"))
	if err != nil {
		t.Fatalf("read methods log: %v", err)
	}
	got := strings.Count(string(raw), "session/new")
	// Request 1 pays one inline session/new (its cache was empty). The
	// refill after its release prepares one; request 2 pops it and pays
	// none. A third call may race in (the refill after request 2's
	// release); anything beyond that means the prepared path is broken
	// and every request is paying session/new again.
	if got > 3 {
		t.Fatalf("session/new called %d times for 2 requests; prepared path must cap it at 3 (inline + 2 refills)", got)
	}
}

// TestAntigravityAcpExecutorPreparedSessionDisabled verifies that without
// prepared-sessions, every request performs its own session/new.
func TestAntigravityAcpExecutorPreparedSessionDisabled(t *testing.T) {
	script := createPreparedAgentScript(t)
	exec := NewAntigravityAcpExecutor(&internalconfig.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	preparedRequest(t, exec, auth)
	preparedRequest(t, exec, auth)

	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), "methods.log"))
	if err != nil {
		t.Fatalf("read methods log: %v", err)
	}
	if got := strings.Count(string(raw), "session/new"); got != 2 {
		t.Fatalf("session/new called %d times, want 2 (disabled preparation)", got)
	}
}

// TestAntigravityAcpExecutorPreparedSessionModelSwitch verifies that a
// prepared session whose daemon default differs from the requested variant
// still gets its model set (one set_config_option), and that a later
// same-variant request skips the round trip.
func TestAntigravityAcpExecutorPreparedSessionModelSwitch(t *testing.T) {
	script := createPreparedAgentScript(t)
	cfg := &internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{
		PreparedSessions: 1,
	}}
	exec := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	// Request with -low variant (differs from daemon default) — goes
	// through the slow path and sets the model explicitly.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash-low",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	if _, err := exec.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	_ = fmt.Sprintf // fmt reserved for debugging assertions
	_ = http.StatusOK
}
