package executor

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// boolPtr returns a pointer to b (test config helper).
func boolPtr(b bool) *bool { return &b }

// statefulLogAgent is a fake ACP agent that records every method call AND
// every session/prompt text body to methods.log, so tests can assert exactly
// what reached the daemon per turn.
func statefulLogAgent(t *testing.T) string {
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
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"sessionId\":\"sess-live\",\"configOptions\":[{\"id\":\"model\",\"type\":\"select\",\"currentValue\":\"gemini-3.8-flash\",\"options\":[{\"value\":\"gemini-3.8-flash\"},{\"value\":\"gemini-3.8-flash-low\"}]}]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/set_config_option\"'* ]]; then\n" +
		"    echo set_config_option >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"configOptions\":[]}}'\n" +
		"  elif [[ \"$line\" == *'\"method\":\"session/prompt\"'* ]]; then\n" +
		"    printf '%s\\n' \"$line\" >> \"$LOG_DIR/prompts.log\"\n" +
		"    echo prompt >> \"$LOG_DIR/methods.log\"\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"sess-live\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"ok\"}}}}'\n" +
		"    echo '{\"jsonrpc\":\"2.0\",\"id\":'$id',\"result\":{\"stopReason\":\"end_turn\"}}'\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake agent script: %v", err)
	}
	return scriptPath
}

// statefulOpts builds explicit opt-in metadata for one turn.
func statefulOpts(logicalID string, turn int64) cliproxyexecutor.Options {
	return cliproxyexecutor.Options{
		Metadata: map[string]any{
			"logical_session_id": logicalID,
			"acp_stateful_reuse": true,
			"acp_stateful_turn":  turn,
		},
	}
}

// statefulRequest fires one chat completion turn with the given metadata.
func statefulRequest(t *testing.T, execer *AntigravityAcpExecutor, auth *cliproxyauth.Auth, opts cliproxyexecutor.Options, history string) {
	t.Helper()
	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash", // daemon default: no set_config_option anywhere
		Payload: []byte(history),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := execer.Execute(ctx, auth, req, opts); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
}

func readStatefulLog(t *testing.T, script, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(script), name))
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(raw)
}

// promptTexts extracts the session/prompt request bodies from prompts.log.
func promptTexts(t *testing.T, script string) []string {
	t.Helper()
	raw := readStatefulLog(t, script, "prompts.log")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var msg struct {
			Params struct {
				Prompt []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"prompt"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("decode prompt line %q: %v", line, err)
		}
		var texts []string
		for _, b := range msg.Params.Prompt {
			if b.Text != "" {
				texts = append(texts, b.Text)
			}
		}
		out = append(out, strings.Join(texts, "|"))
	}
	return out
}

// TestStatefulReuse_TwoTurnHitSendsOnlyNewTurn is the PLAN Step-2 exit
// criterion: turn 1 bootstraps with full history; turn 2 performs NO
// session/new and sends ONLY the new user turn to the SAME ACP session.
func TestStatefulReuse_TwoTurnHitSendsOnlyNewTurn(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	// Turn 0: full history (system + user + assistant + user).
	history1 := `{"messages":[` +
		`{"role":"system","content":"You translate."},` +
		`{"role":"user","content":"hello world"},` +
		`{"role":"assistant","content":"hola mundo"},` +
		`{"role":"user","content":"turn ONE marker"}]}`
	statefulRequest(t, execer, auth, statefulOpts("tsession-A", 0), history1)

	// Turn 1: the client still sends the FULL history (recovery source),
	// but the proxy must prompt only the newest user turn.
	history2 := `{"messages":[` +
		`{"role":"system","content":"You translate."},` +
		`{"role":"user","content":"hello world"},` +
		`{"role":"assistant","content":"hola mundo"},` +
		`{"role":"user","content":"turn ONE marker"},` +
		`{"role":"assistant","content":"ok one"},` +
		`{"role":"user","content":"turn TWO marker SECRET-7"}]}`
	statefulRequest(t, execer, auth, statefulOpts("tsession-A", 1), history2)

	methods := readStatefulLog(t, script, "methods.log")
	news := strings.Count(methods, "session/new")
	if news != 1 {
		t.Fatalf("session/new called %d times; turn 2 must reuse the bound session (want exactly 1)\nlog: %s", news, methods)
	}
	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompt turns, got %d: %v", len(prompts), prompts)
	}
	// Turn 1 prompt carried the full history (marker of turn 1 included).
	if !strings.Contains(prompts[0], "turn ONE marker") {
		t.Fatalf("turn 1 prompt missing full history: %q", prompts[0])
	}
	// Turn 2 prompt must contain ONLY the newest user turn: no system
	// prompt, no earlier turns (P1.2).
	if strings.Contains(prompts[1], "You translate") ||
		strings.Contains(prompts[1], "turn ONE marker") ||
		strings.Contains(prompts[1], "hello world") {
		t.Fatalf("turn 2 replayed history on a stateful hit: %q", prompts[1])
	}
	if !strings.Contains(prompts[1], "turn TWO marker SECRET-7") {
		t.Fatalf("turn 2 prompt missing the new user turn: %q", prompts[1])
	}
}

// TestStatefulReuse_TurnMismatchFallsBackToBootstrap covers P1.6 #5: a
// duplicated/skipped turn invalidates the binding and bootstraps from the
// full history.
func TestStatefulReuse_TurnMismatchFallsBackToBootstrap(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	h1 := `{"messages":[{"role":"user","content":"first turn"}]}`
	statefulRequest(t, execer, auth, statefulOpts("tsession-B", 0), h1)

	// Turn 2 arrives with a SKIPPED turn number (2 instead of 1).
	h2 := `{"messages":[{"role":"user","content":"first turn"},{"role":"assistant","content":"ok"},{"role":"user","content":"skipped-turn request"}]}`
	statefulRequest(t, execer, auth, statefulOpts("tsession-B", 2), h2)

	methods := readStatefulLog(t, script, "methods.log")
	if got := strings.Count(methods, "session/new"); got != 2 {
		t.Fatalf("turn mismatch must bootstrap a fresh session: session/new=%d, want 2\nlog: %s", got, methods)
	}
	// The bootstrap prompt carried the full history.
	prompts := promptTexts(t, script)
	if len(prompts) != 2 || !strings.Contains(prompts[1], "first turn") {
		t.Fatalf("mismatch fallback must replay full history: %v", prompts)
	}
}

// TestStatefulReuse_MissingReuseHeaderKeepsStateless covers P1.6 #4:
// without X-ACP-Session-Reuse, requests stay stateless even with a logical
// session id present — every request pays its own session/new.
func TestStatefulReuse_MissingReuseHeaderKeepsStateless(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	opts := cliproxyexecutor.Options{Metadata: map[string]any{
		"logical_session_id": "tsession-C",
		"acp_stateful_turn":  int64(0),
		// NO acp_stateful_reuse: the feature must be ignored.
	}}
	h1 := `{"messages":[{"role":"user","content":"one"}]}`
	h2 := `{"messages":[{"role":"user","content":"one"},{"role":"assistant","content":"ok"},{"role":"user","content":"two"}]}`
	statefulRequest(t, execer, auth, opts, h1)
	statefulRequest(t, execer, auth, opts, h2)

	methods := readStatefulLog(t, script, "methods.log")
	if got := strings.Count(methods, "session/new"); got != 2 {
		t.Fatalf("stateless semantics broken: session/new=%d, want 2 (no reuse header)\nlog: %s", got, methods)
	}
}

// TestStatefulReuse_DifferentLogicalSessionsDoNotCross covers P1.6 #3/#9:
// two logical sessions never share an ACP session, and each bootstraps its
// own conversation.
func TestStatefulReuse_DifferentLogicalSessionsDoNotCross(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	statefulRequest(t, execer, auth, statefulOpts("tsession-D1", 0), `{"messages":[{"role":"user","content":"session D one"}]}`)
	statefulRequest(t, execer, auth, statefulOpts("tsession-D2", 0), `{"messages":[{"role":"user","content":"session D two"}]}`)

	methods := readStatefulLog(t, script, "methods.log")
	if got := strings.Count(methods, "session/new"); got != 2 {
		t.Fatalf("two distinct logical sessions must each bootstrap: session/new=%d, want 2\nlog: %s", got, methods)
	}
}

// TestStatefulSignals_ExtractionCoversMalformed verifies the metadata
// extraction rules: reuse requires BOTH logical id and a valid turn.
func TestStatefulSignals_ExtractionCoversMalformed(t *testing.T) {
	base := map[string]any{
		"logical_session_id": "L",
		"acp_stateful_reuse": true,
		"acp_stateful_turn":  int64(3),
	}
	if s := statefulTurnSignalsFromOptions(cliproxyexecutor.Options{Metadata: base}); !s.reuse || s.turn != 3 {
		t.Fatalf("valid signals not extracted: %+v", s)
	}
	noTurn := map[string]any{"logical_session_id": "L", "acp_stateful_reuse": true}
	if s := statefulTurnSignalsFromOptions(cliproxyexecutor.Options{Metadata: noTurn}); s.reuse {
		t.Fatalf("reuse must be disabled without a turn number: %+v", s)
	}
	noID := map[string]any{"acp_stateful_reuse": true, "acp_stateful_turn": int64(1)}
	if s := statefulTurnSignalsFromOptions(cliproxyexecutor.Options{Metadata: noID}); s.reuse {
		t.Fatalf("reuse must be disabled without a logical id: %+v", s)
	}
	off := map[string]any{"logical_session_id": "L", "acp_stateful_reuse": false, "acp_stateful_turn": int64(1)}
	if s := statefulTurnSignalsFromOptions(cliproxyexecutor.Options{Metadata: off}); s.reuse {
		t.Fatalf("reuse must be off when the header is absent/false: %+v", s)
	}
}

// TestStatefulIncrementalTurn_Shapes verifies the incremental extraction
// for both supported payload shapes.
func TestStatefulIncrementalTurn_Shapes(t *testing.T) {
	chat := []byte(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"u1"},{"role":"assistant","content":"a1"},{"role":"user","content":"u2 FINAL"}]}`)
	turn, err := statefulIncrementalTurn(chat)
	if err != nil {
		t.Fatalf("chat shape: %v", err)
	}
	if !strings.Contains(string(turn), "u2 FINAL") || strings.Contains(string(turn), "u1") || strings.Contains(string(turn), "S") {
		t.Fatalf("chat incremental turn wrong: %s", turn)
	}
	responses := []byte(`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"old"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"a"}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"NEW TURN"}]}]}`)
	turn, err = statefulIncrementalTurn(responses)
	if err != nil {
		t.Fatalf("responses shape: %v", err)
	}
	if !strings.Contains(string(turn), "NEW TURN") || strings.Contains(string(turn), "old") {
		t.Fatalf("responses incremental turn wrong: %s", turn)
	}
}

// keep imports honest when the file evolves
var (
	_ = bufio.NewReader
	_ = io.EOF
	_ = sync.Mutex{}
	_ = http.StatusOK
	_ = exec.Command
)
