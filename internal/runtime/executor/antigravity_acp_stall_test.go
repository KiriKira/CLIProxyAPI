package executor

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// stallAgent writes a fake ACP agent whose session/prompt turn answers
// immediately EXCEPT the very first stalled turn (tracked by a marker
// file so the behavior survives the worker's death and respawn). In
// stall mode the agent stays alive (transport healthy) but produces no
// stdout after echoing the prompt line to prompts.log — the 2026-09-09
// wedged-daemon signature. This lets tests prove the recovery request
// succeeds on a FRESH worker after the wedged one is retired.
func stallAgent(t *testing.T, stall bool) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")
	stallVal := "0"
	if stall {
		stallVal = "1"
	}
	script := "#!/usr/bin/env bash\n" +
		"LOG_DIR=\"$(dirname \"$0\")\"\n" +
		"STALL=" + stallVal + "\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-stall\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash\",\"options\":[{\"value\":\"gemini-3.8-flash\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash-high\",\"options\":[{\"value\":\"gemini-3.8-flash\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    printf '%s\\n' \"$line\" >> \"$LOG_DIR/prompts.log\"\n" +
		"    if [[ \"$STALL\" == \"1\" && ! -f \"$LOG_DIR/stalled_once\" ]]; then\n" +
		"      # Wedged daemon: mark the stall (survives respawn), keep\n" +
		"      # reading stdin (transport alive), never reply. The abandoned\n" +
		"      # turn unblocks when retirement kills this process group.\n" +
		"      touch \"$LOG_DIR/stalled_once\"\n" +
		"      while IFS= read -r line; do :; done\n" +
		"    fi\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-stall\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"ok\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	return scriptPath
}

func stallExecutor(t *testing.T, script string, timeout string) (*AntigravityAcpExecutor, *cliproxyauth.Auth) {
	t.Helper()
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{
		Antigravity: internalconfig.AntigravityConfig{
			PersistentProcess:  boolPtr(true),
			PromptStallTimeout: timeout,
		},
	})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	return execer, auth
}

func stallRequest(t *testing.T, execer *AntigravityAcpExecutor, auth *cliproxyauth.Auth) error {
	t.Helper()
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"ping"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	return err
}

func isStallStatus(err error) bool {
	var se statusErr
	return errors.As(err, &se) && se.code == http.StatusGatewayTimeout
}

// TestPromptStall_WatchdogFailsAndRetiresWorker is the core 2026-09-09
// incident contract: a wedged daemon (alive transport, zero output) must
// fail the request with 504 instead of hanging, and the poisoned worker
// must be retired so the NEXT request bootstraps a fresh daemon and
// succeeds. The old behavior failed both halves: the request hung until
// the client gave up and every later request queued behind the wedge.
func TestPromptStall_WatchdogFailsAndRetiresWorker(t *testing.T) {
	script := stallAgent(t, true)
	execer, auth := stallExecutor(t, script, "5s")

	if err := stallRequest(t, execer, auth); !isStallStatus(err) {
		t.Fatalf("stalled request: want 504 statusErr, got %v", err)
	}

	// The wedged worker must be gone from the pool; a fresh one replaces
	// it on the next request, which must succeed end-to-end.
	if err := stallRequest(t, execer, auth); err != nil {
		t.Fatalf("request after stall did not recover with a fresh worker: %v", err)
	}
	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompt turns (wedged + recovered), got %d", len(prompts))
	}
}

// TestPromptStall_DisabledKeepsLegacyWait verifies prompt-stall-timeout
// unset keeps legacy semantics: a slow agent eventually answers and the
// request succeeds — no watchdog, no forced 504.
func TestPromptStall_DisabledKeepsLegacyWait(t *testing.T) {
	script := stallAgent(t, false)
	execer, auth := stallExecutor(t, script, "")

	if err := stallRequest(t, execer, auth); err != nil {
		t.Fatalf("disabled watchdog must keep legacy wait semantics: %v", err)
	}
}

// TestPromptStall_ActivityKeepsTurnAlive proves the watchdog measures
// OUTPUT SILENCE, not total turn time: an agent that streams periodic
// updates for well past the timeout must never fail. This is what makes
// a 90s default safe for long generations.
func TestPromptStall_ActivityKeepsTurnAlive(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "trickle_agent.sh")
	script := "#!/usr/bin/env bash\n" +
		"while IFS= read -r line; do\n" +
		"  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n" +
		"  [ -z \"$id\" ] && id=1\n" +
		"  if [[ \"$line\" == *'\"method\":\"initialize\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"protocolVersion\":1}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"authenticate\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/new\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-trickle\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash\",\"options\":[{\"value\":\"gemini-3.8-flash\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash-high\",\"options\":[{\"value\":\"gemini-3.8-flash\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    # 6 heartbeats, 1s apart: total turn 6s > 5s timeout, but the\n" +
		"    # silence gap never exceeds 1s, so the watchdog must not trip.\n" +
		"    for i in $(seq 1 6); do\n" +
		"      sleep 1\n" +
		"      echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-trickle\",\"update\":{\"sessionUpdate\":\"agent_thought_chunk\",\"content\":{\"type\":\"text\",\"text\":\".\"}}}}'\n" +
		"    done\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write trickle agent: %v", err)
	}
	execer, auth := stallExecutor(t, scriptPath, "5s")

	if err := stallRequest(t, execer, auth); err != nil {
		t.Fatalf("streaming activity must keep the turn alive: %v", err)
	}
}

// TestPromptStall_StreamPathRetiresWorker covers the streaming path: the
// stall must surface as a 504 stream chunk error and the next stream
// request must recover on a fresh worker.
func TestPromptStall_StreamPathRetiresWorker(t *testing.T) {
	script := stallAgent(t, true)
	execer, auth := stallExecutor(t, script, "5s")

	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"ping"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := execer.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream returned error before streaming: %v", err)
	}
	var streamErr error
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if !isStallStatus(streamErr) {
		t.Fatalf("stalled stream: want 504 statusErr chunk, got %v", streamErr)
	}

	if err := stallRequest(t, execer, auth); err != nil {
		t.Fatalf("stream request after stall did not recover: %v", err)
	}
}

// TestPromptStall_MinTimeoutDisarms verifies the <5s floor: a sub-floor
// value disables the watchdog instead of arming a guaranteed-false-positive
// timer (a 2s arm would kill healthy turns whose first token took 6s in
// production).
func TestPromptStall_MinTimeoutDisarms(t *testing.T) {
	script := stallAgent(t, true)
	execer, auth := stallExecutor(t, script, "1s")

	if execer.promptStallTimeout != 0 {
		t.Fatalf("sub-floor timeout must disarm the watchdog, got %v", execer.promptStallTimeout)
	}
	// No watchdog armed: the request cannot 504. It would hang forever
	// against the stalled agent, so bound it and assert only that the
	// failure (client timeout) is not our 504.
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"ping"}]}`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := execer.Execute(ctx, auth, req, cliproxyexecutor.Options{})
	if err != nil && isStallStatus(err) {
		t.Fatalf("disarmed watchdog must never produce stall 504: %v", err)
	}
}
