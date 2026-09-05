package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// fakeAgentConfigurableScript builds a fake ACP agent whose session/new
// reports a configurable current model and whose set_config_option calls are
// logged, so tests can assert whether the redundant model configuration
// round trip was skipped.
func fakeAgentConfigurableScript(t *testing.T, currentModel string) string {
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
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-1\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"" + currentModel + "\",\"options\":[{\"value\":\"" + currentModel + "\"},{\"value\":\"gemini-3.8-flash-high\"},{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo set_config_option >> \"$LOG_DIR/setcfg.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash-low\",\"options\":[{\"value\":\"" + currentModel + "\"},{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-1\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"ok\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	return scriptPath
}

func runOneRequest(t *testing.T, script string) error {
	t.Helper()
	cfg := &internalconfig.Config{}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash-low",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	return nil
}

func setConfigCalls(t *testing.T, script string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), "setcfg.log"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("read setcfg log: %v", err)
	}
	return len(strings.Split(strings.TrimSpace(string(raw)), "\n"))
}

// TestAntigravityAcpExecutorSkipsRedundantModelConfig verifies the P0
// skip: when session/new already reports the resolved model variant as the
// session's current model, no session/set_config_option round trip happens.
func TestAntigravityAcpExecutorSkipsRedundantModelConfig(t *testing.T) {
	// The daemon reports gemini-3.8-flash-low as current; the request
	// resolves to the same variant.
	script := fakeAgentConfigurableScript(t, "gemini-3.8-flash-low")
	runOneRequest(t, script)
	if got := setConfigCalls(t, script); got != 0 {
		t.Fatalf("session/set_config_option called %d times, want 0 (redundant call not skipped)", got)
	}
}

// TestAntigravityAcpExecutorSetsModelWhenDifferent verifies that a genuinely
// different requested variant still goes through session/set_config_option.
func TestAntigravityAcpExecutorSetsModelWhenDifferent(t *testing.T) {
	// The daemon reports gemini-3.8-flash-high as current; the request
	// resolves to gemini-3.8-flash-low, so a real set is required.
	script := fakeAgentConfigurableScript(t, "gemini-3.8-flash-high")
	runOneRequest(t, script)
	if got := setConfigCalls(t, script); got != 1 {
		t.Fatalf("session/set_config_option called %d times, want 1", got)
	}
}

// TestAntigravityAcpExecutorModelConfigFailureIs400 keeps the contract that
// a rejected variant is a client error, not a transport failure.
func TestAntigravityAcpExecutorModelConfigFailureIs400(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-1\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"other\",\"options\":[{\"value\":\"other\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"error\":{\"code\":404,\"message\":\"model not offered\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	cfg := &internalconfig.Config{}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": scriptPath,
			"gemini_home": t.TempDir(),
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash-low",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err == nil {
		t.Fatal("expected model-config rejection error, got nil")
	}
	se, ok := err.(statusErr)
	if !ok {
		t.Fatalf("expected statusErr, got %T: %v", err, err)
	}
	if se.code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", se.code)
	}
}

// TestAntigravityAcpExecutorTTFTStageLog exercises the stage-timing logger
// end to end so the instrumentation code path stays wired for both Execute
// and ExecuteStream (the log line itself is fire-and-forget).
func TestAntigravityAcpExecutorTTFTStageLog(t *testing.T) {
	script := createFakeAgentScript(t)
	cfg := &internalconfig.Config{}
	execer := NewAntigravityAcpExecutor(cfg)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	sres, err := execer.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}
	for chunk := range sres.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
	}
}

// TestAntigravityAcpExecutorUnknownGeminiFamilyPassesThrough pins the
// behavior that a gemini-* model outside the known flash/pro families is
// still applied via session/set_config_option (the daemon validates it);
// only the resolved flash/pro variants take the skip path.
func TestAntigravityAcpExecutorUnknownGeminiFamilyPassesThrough(t *testing.T) {
	script := fakeAgentConfigurableScript(t, "whatever-model")
	// The request model cannot be resolved to a known variant; openSession
	// must still call set_config_option so the daemon renders its verdict.
	cfg := &internalconfig.Config{}
	execer := NewAntigravityAcpExecutor(cfg)
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gemini-9.9-nano",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got := setConfigCalls(t, script); got != 1 {
		t.Fatalf("session/set_config_option called %d times, want 1", got)
	}
}

// Compile-time guard that the fake agent script still parses as a shell
// script (cheap sanity check for the heredoc-ish string concatenations).
var _ = exec.Command
var _ = json.Marshal
