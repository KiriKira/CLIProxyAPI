package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// fakeAgent is a minimal in-test ACP agent speaking NDJSON over pipes.
// It replies to the four lifecycle methods and records inbound lines.
type fakeAgent struct {
	t       *testing.T
	r       *bufio.Reader
	w       io.Writer
	inbound chan wireMessage
}

func startFakeAgent(t *testing.T, toAgentR io.Reader, toAgentW io.WriteCloser, fromAgentR io.Reader, fromAgentW io.Writer) (*Client, *fakeAgent) {
	t.Helper()
	ag := &fakeAgent{t: t, r: bufio.NewReader(toAgentR), w: fromAgentW, inbound: make(chan wireMessage, 64)}
	// Agent loop: read NDJSON, respond.
	go func() {
		sc := bufio.NewScanner(ag.r)
		sc.Buffer(make([]byte, 0, 64*1024), maxACPMessageBytes)
		for sc.Scan() {
			var msg wireMessage
			if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
				continue
			}
			ag.inbound <- msg
			var resp wireMessage
			resp.JSONRPC = "2.0"
			resp.ID = msg.ID
			switch msg.Method {
			case "initialize":
				resp.Result = json.RawMessage(`{"protocolVersion":1,"agentCapabilities":{},"authMethods":[]}`)
			case "authenticate":
				resp.Result = json.RawMessage(`{}`)
			case "session/new":
				// Before replying, emit one session/update notification and
				// one session/request_permission to exercise default handlers.
				ag.sendLine(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk"}}}`)
				ag.sendLine(`{"jsonrpc":"2.0","id":"perm-1","method":"session/request_permission","params":{"sessionId":"sess-1"}}`)
				resp.Result = json.RawMessage(`{"sessionId":"sess-1","configOptions":[{"id":"model","type":"select","category":"model","name":"Model","currentValue":"gemini-3.7-flash","options":[{"name":"Gemini 3.8 Flash","value":"gemini-3.8-flash"},{"name":"Legacy","options":[{"name":"Gemini 3.7 Flash","value":"gemini-3.7-flash"}]}]}]}`)
			case "session/prompt":
				ag.sendLine(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk"}}}`)
				resp.Result = json.RawMessage(`{"stopReason":"end_turn"}`)
			default:
				resp.Result = json.RawMessage(`{}`)
			}
			if msg.ID != nil && string(msg.ID) != "null" && msg.Method != "" {
				ag.sendRaw(resp)
			}
		}
	}()
	c := newClientWithPipes(toAgentW, fromAgentR, strings.NewReader(""), nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	return c, ag
}

func (a *fakeAgent) sendLine(s string) {
	// Write errors are intentionally ignored: the pipe may already be
	// closed after the test finished, and calling t.Errorf after test
	// completion panics. A dead agent surfaces as a timeout in the
	// wait helpers instead.
	_, _ = io.WriteString(a.w, s+"\n")
}

func (a *fakeAgent) sendRaw(m wireMessage) {
	b, err := json.Marshal(m)
	if err != nil {
		panic("fakeAgent marshal: " + err.Error())
	}
	a.sendLine(string(b))
}

func nextInbound(t *testing.T, ag *fakeAgent) wireMessage {
	t.Helper()
	select {
	case m := <-ag.inbound:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inbound message")
		return wireMessage{}
	}
}

// waitMethod drains the inbound queue until a message with the given method
// arrives. Needed because the fake agent records every client->agent line,
// so requests and auto-replies accumulate ahead of the message under test.
func waitMethod(t *testing.T, ag *fakeAgent, method string) wireMessage {
	t.Helper()
	for i := 0; i < 16; i++ {
		m := nextInbound(t, ag)
		if m.Method == method {
			return m
		}
	}
	t.Fatalf("never saw %q in inbound queue", method)
	return wireMessage{}
}

func TestLifecycleHandshake(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c, ag := startFakeAgent(t, toAgentR, toAgentW, fromAgentR, fromAgentW)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	init, err := c.Initialize(ctx, "cli-proxy-api", "test")
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != 1 {
		t.Fatalf("protocolVersion = %d, want 1", init.ProtocolVersion)
	}
	// Verify what the client actually sent on the wire.
	m := nextInbound(t, ag)
	if m.Method != "initialize" {
		t.Fatalf("method = %q, want initialize", m.Method)
	}
	var p initializeRequest
	if err := json.Unmarshal(m.Params, &p); err != nil {
		t.Fatalf("decode initialize params: %v", err)
	}
	if p.ProtocolVersion != 1 {
		t.Fatalf("params protocolVersion = %d, want 1", p.ProtocolVersion)
	}
	if p.ClientCapabilities.FS.ReadTextFile || p.ClientCapabilities.FS.WriteTextFile || p.ClientCapabilities.Terminal {
		t.Fatalf("safe posture violated: %+v", p.ClientCapabilities)
	}

	if err := c.Authenticate(ctx, "oauth-personal"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	m = waitMethod(t, ag, "authenticate")

	sid, err := c.NewSession(ctx, "/tmp")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sid != "sess-1" {
		t.Fatalf("sessionId = %q, want sess-1", sid)
	}

	updates := make(chan SessionUpdate, 8)
	c.OnUpdate(func(u SessionUpdate) { updates <- u })

	stop, err := c.Prompt(ctx, sid, NewTextPrompt("hello"))
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != "end_turn" {
		t.Fatalf("stopReason = %q, want end_turn", stop)
	}
	select {
	case u := <-updates:
		if u.SessionID != "sess-1" || u.Kind != "agent_message_chunk" {
			t.Fatalf("unexpected update: %+v", u)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session/update")
	}

	if err := c.Cancel(sid); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	m = waitMethod(t, ag, "session/cancel")
}

func TestPermissionDeniedByDefault(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c, ag := startFakeAgent(t, toAgentR, toAgentW, fromAgentR, fromAgentW)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := c.Initialize(ctx, "test", "test"); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := c.NewSession(ctx, "/tmp"); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	// The fake agent records every client->agent line (requests plus the
	// auto-reply to perm-1) in ag.inbound. Consume the queue instead of
	// opening a second reader on toAgentR: io.Pipe pairs readers and
	// writers synchronously, so a second scanner would race the agent
	// loop and deadlock.
	for i := 0; i < 16; i++ {
		msg := nextInbound(t, ag)
		if msg.Method != "" || string(msg.ID) != `"perm-1"` {
			continue
		}
		var out struct {
			Outcome struct {
				Outcome string `json:"outcome"`
			} `json:"outcome"`
		}
		if err := json.Unmarshal(msg.Result, &out); err != nil {
			t.Fatalf("decode permission reply: %v", err)
		}
		if out.Outcome.Outcome != "cancelled" {
			t.Fatalf("permission outcome = %q, want cancelled", out.Outcome.Outcome)
		}
		return
	}
	t.Fatal("never saw the permission reply in inbound queue")
}

func TestMalformedLineIgnored(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c, ag := startFakeAgent(t, toAgentR, toAgentW, fromAgentR, fromAgentW)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ag.sendLine(`this is not json{{{`)
	if _, err := c.Initialize(ctx, "test", "test"); err != nil {
		t.Fatalf("Initialize after garbage line: %v", err)
	}
}

func TestCallContextCancel(t *testing.T) {
	// Agent that never replies: call must return ctx.Err without leaking.
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, _ := io.Pipe() // never written
	c := newClientWithPipes(toAgentW, fromAgentR, strings.NewReader(""), nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	go func() {
		// Drain stdin so writes never block.
		_, _ = io.Copy(io.Discard, toAgentR)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := c.Initialize(ctx, "test", "test")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestCloseThenCallFails(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c, _ := startFakeAgent(t, toAgentR, toAgentW, fromAgentR, fromAgentW)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err := c.Initialize(context.Background(), "test", "test")
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("err = %v, want closed error", err)
	}
}

// TestHelperProcess is the child side of the real-subprocess test: it
// behaves as a tiny ACP agent (initialize/authenticate/session-new/prompt).
func TestHelperProcess(t *testing.T) {
	if os.Getenv("ACP_TEST_HELPER") != "1" {
		return
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), maxACPMessageBytes)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for sc.Scan() {
		var msg wireMessage
		if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
			continue
		}
		resp := wireMessage{JSONRPC: "2.0", ID: msg.ID}
		switch msg.Method {
		case "initialize":
			resp.Result = json.RawMessage(`{"protocolVersion":1,"agentCapabilities":{},"authMethods":[]}`)
		case "authenticate":
			resp.Result = json.RawMessage(`{}`)
		case "session/new":
			resp.Result = json.RawMessage(`{"sessionId":"real-sess"}`)
		case "session/prompt":
			resp.Result = json.RawMessage(`{"stopReason":"end_turn"}`)
		default:
			resp.Result = json.RawMessage(`{}`)
		}
		b, _ := json.Marshal(resp)
		_, _ = w.Write(append(b, '\n'))
		_ = w.Flush()
	}
}

func TestRealSubprocessEnvIsolation(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Skipf("os.Executable: %v", err)
	}
	c, err := NewClient(SpawnConfig{
		Command: exe,
		Args:    []string{"-test.run=TestHelperProcess"},
		Env:     []string{"ACP_TEST_HELPER=1", "PATH=/usr/bin:/bin"},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	init, err := c.Initialize(ctx, "test", "test")
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != 1 {
		t.Fatalf("protocolVersion = %d, want 1", init.ProtocolVersion)
	}
	sid, err := c.NewSession(ctx, "/tmp")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sid != "real-sess" {
		t.Fatalf("sessionId = %q, want real-sess", sid)
	}
	stop, err := c.Prompt(ctx, sid, NewTextPrompt("hi"))
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != "end_turn" {
		t.Fatalf("stopReason = %q, want end_turn", stop)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-c.ExitErr():
		if err != nil {
			t.Fatalf("exit err = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for process exit")
	}
}
