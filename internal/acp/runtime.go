package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// maxACPMessageBytes caps a single NDJSON line from the agent process.
// bufio.Scanner defaults to a 64 KiB token limit, which a long agent reply
// chunk easily exceeds; raise it explicitly.
const maxACPMessageBytes = 8 * 1024 * 1024

// closeKillGrace bounds how long Close waits (without blocking) before
// asking the agent process group to terminate after stdin EOF.
const closeKillGrace = 5 * time.Second

// closeKillEscalationGrace bounds the SIGTERM-to-SIGKILL escalation window.
const closeKillEscalationGrace = time.Second

// clientLogFunc receives agent stderr lines. Implementations must not log
// them verbatim: upstream stderr can contain OAuth URLs, states, and
// redirect codes.
type clientLogFunc func(line string)

// SpawnConfig describes how to launch the ACP agent process.
type SpawnConfig struct {
	Command string
	Args    []string
	Env     []string // explicit environment; the parent env is NOT inherited
	Dir     string
	// UIDArg appends "--uid=" on linux (required by the Antigravity agent).
	UIDArg bool
	// LogStderr receives drained stderr lines. May be nil (lines dropped).
	LogStderr clientLogFunc
	// OnUpdate handles session/update notifications. Registered before
	// spawn so the reader goroutine never races with a later setter.
	// May be nil (updates dropped). Replaceable via OnUpdate.
	OnUpdate func(SessionUpdate)
}

// callResult is the outcome of one outbound request. Exactly one of the
// fields is set: a live agent answers with result/rpcErr, while a dead
// transport fails the waiter with transportErr.
type callResult struct {
	result       json.RawMessage
	rpcErr       *rpcErrorObject
	transportErr error
}

// pendingCall tracks one in-flight outbound request.
type pendingCall struct {
	ch chan callResult
}

// Client is a supervised stdio JSON-RPC 2.0 ACP client.
//
// Concurrency model: a single reader goroutine owns stdout and dispatches
// inbound messages. Outbound writes are serialized by writeMu. Agent-to-
// client requests are answered by safe default handlers inline (replies are
// small and non-blocking) so the reader never stalls. onUpdate is guarded
// by updateMu because registration may happen after spawn.
type Client struct {
	cmd    *exec.Cmd // nil for pipe-constructed test clients
	stdin  *stdinWriter
	reader *bufio.Scanner

	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]pendingCall

	nextID atomic.Uint64

	updateMu sync.RWMutex
	onUpdate func(SessionUpdate)

	// lineMu guards onFirstLine. The hook fires on every decoded inbound
	// line so request paths can timestamp the first backend output (TTFT
	// attribution) with their own sync.Once.
	lineMu      sync.RWMutex
	onFirstLine func()

	// writeHookMu guards onRequestWritten. The hook fires after each
	// successful outbound request write so callers can timestamp the
	// actual stdin write instead of a pre-call estimate.
	writeHookMu      sync.RWMutex
	onRequestWritten func(method string)

	cfgMu   sync.Mutex
	sessCfg map[string][]SessionConfigOption

	closedMu sync.Mutex
	closed   bool
	closeErr error
	exitCh   chan error
	// processDone closes when cmd.Wait returns. It is separate from exitCh so
	// the background process-group reaper does not consume the public result.
	processDone chan struct{}
	// processTreeDone closes after Close has completed process-group cleanup.
	// It is nil for pipe-constructed test clients without an owned process.
	processTreeDone chan struct{}

	logStderr clientLogFunc
}

// newClientWithPipes builds a Client over already-connected pipes and
// starts the supervision loops. NewClient delegates to it after spawning
// the process; tests use it directly with io.Pipe pairs so no real
// subprocess is needed.
func newClientWithPipes(stdin io.WriteCloser, stdout io.Reader, stderr io.Reader, logStderr clientLogFunc, onUpdate func(SessionUpdate)) *Client {
	c := &Client{
		stdin:     &stdinWriter{w: stdin},
		pending:   make(map[string]pendingCall),
		exitCh:    make(chan error, 1),
		logStderr: logStderr,
		onUpdate:  onUpdate,
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxACPMessageBytes)
	c.reader = scanner

	go c.drainStderr(stderr)
	go c.readLoop()
	return c
}

// NewClient spawns the agent process and starts the supervision loops.
// The caller must call Close when done. The process environment is exactly
// cfg.Env (no parent inheritance) so ambient GOOGLE_*/GEMINI_* keys cannot
// leak into the agent. WaitDelay bounds process reaping so a hung agent
// can never strand cmd.Wait forever.
func NewClient(cfg SpawnConfig) (*Client, error) {
	if cfg.Command == "" {
		return nil, errors.New("acp: spawn command is empty")
	}
	args := append([]string(nil), cfg.Args...)
	if cfg.UIDArg {
		args = append(args, "--uid=")
	}
	// CommandContext with a never-cancelled Background only to legalize the
	// Cancel hook below; real termination is owned by Close plus WaitDelay
	// and the reapAfterGrace fallback, not by context cancellation here.
	cmd := exec.CommandContext(context.Background(), cfg.Command, args...)
	if cfg.Dir != "" {
		cmd.Dir = cfg.Dir
	}
	cmd.Env = append([]string(nil), cfg.Env...)
	// Run each ACP daemon in its own process group so Close can reclaim
	// daemon-spawned harness descendants as one lifecycle unit.
	configureProcessGroup(cmd)
	// Forceful cancellation must reclaim daemon descendants as well. Normal
	// Close still gets the graceful stdin-EOF path before escalation.
	cmd.Cancel = func() error { return killProcessGroup(cmd.Process) }
	cmd.WaitDelay = closeKillGrace

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("acp: spawn %q: %w", cfg.Command, err)
	}

	c := newClientWithPipes(stdinPipe, stdoutPipe, stderrPipe, cfg.LogStderr, cfg.OnUpdate)
	c.cmd = cmd
	c.processDone = make(chan struct{})
	c.processTreeDone = make(chan struct{})
	go func() {
		err := cmd.Wait()
		c.exitCh <- err
		close(c.processDone)
		c.failAllPending(transportErrorf("agent process exited"))
	}()
	return c, nil
}

// OnUpdate replaces the session/update handler. It is safe for concurrent
// use with the reader goroutine.
func (c *Client) OnUpdate(fn func(SessionUpdate)) {
	c.updateMu.Lock()
	defer c.updateMu.Unlock()
	c.onUpdate = fn
}

// SetOnFirstLine registers a hook invoked for every decoded inbound stdout
// line. Callers wrap it in their own sync.Once to timestamp the first
// backend output of a turn. It is safe for concurrent use with the reader
// goroutine; replacing the hook never races with a firing one.
func (c *Client) SetOnFirstLine(fn func()) {
	c.lineMu.Lock()
	defer c.lineMu.Unlock()
	c.onFirstLine = fn
}

func (c *Client) getOnFirstLine() func() {
	c.lineMu.RLock()
	defer c.lineMu.RUnlock()
	return c.onFirstLine
}

// SetOnRequestWritten registers a hook invoked with the JSON-RPC method
// name after each successful outbound request write. Callers filter by
// method to timestamp the exact moment a prompt reached the agent's stdin.
// It is safe for concurrent use.
func (c *Client) SetOnRequestWritten(fn func(method string)) {
	c.writeHookMu.Lock()
	defer c.writeHookMu.Unlock()
	c.onRequestWritten = fn
}

func (c *Client) getOnRequestWritten() func(method string) {
	c.writeHookMu.RLock()
	defer c.writeHookMu.RUnlock()
	return c.onRequestWritten
}

func (c *Client) getOnUpdate() func(SessionUpdate) {
	c.updateMu.RLock()
	defer c.updateMu.RUnlock()
	return c.onUpdate
}

// ExitErr reports the process exit result once the agent terminates.
// It never blocks the caller: the channel receives at most one value.
func (c *Client) ExitErr() <-chan error { return c.exitCh }

// Close is idempotent: it closes stdin to signal EOF and fails all pending
// calls. It never blocks on process exit; a reaper kills a still-running
// agent after closeKillGrace so Close cannot leak a zombie. Lock order is
// closedMu then pendingMu and the two are never held together:
// failAllPending runs after closedMu is released so call() (which never
// nests the two locks) cannot deadlock with Close().
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closedMu.Lock()
	if c.closed {
		err := c.closeErr
		c.closedMu.Unlock()
		return err
	}
	c.closed = true
	c.closedMu.Unlock()
	var err error
	if c.stdin != nil {
		err = c.stdin.close()
	}
	c.closedMu.Lock()
	c.closeErr = err
	c.closedMu.Unlock()
	c.failAllPending(transportErrorf("client closed"))
	if c.cmd != nil && c.cmd.Process != nil {
		go c.reapProcessTree()
	}
	return err
}

// CloseAndWait closes the client and waits until its owned process group has
// completed cleanup. It is the strict lifecycle counterpart to Close: callers
// that are about to return a hard process-capacity token must use this method.
// Pipe-constructed clients without an owned process return after Close.
func (c *Client) CloseAndWait(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	closeErr := c.Close()
	if c.processTreeDone == nil {
		return closeErr
	}
	select {
	case <-c.processTreeDone:
		return closeErr
	case <-ctx.Done():
		if closeErr != nil {
			return errors.Join(closeErr, ctx.Err())
		}
		return ctx.Err()
	}
}

// reapProcessTree lets the daemon observe stdin EOF first, then terminates its
// entire process group. The escalation is asynchronous so Close remains
// non-blocking while still reclaiming descendants that ignore EOF or SIGTERM.
func (c *Client) reapProcessTree() {
	if c.processTreeDone != nil {
		defer close(c.processTreeDone)
	}
	if c.cmd == nil || c.cmd.Process == nil {
		return
	}
	if c.processDone != nil {
		select {
		case <-c.processDone:
		case <-time.After(closeKillGrace):
		}
	} else {
		time.Sleep(closeKillGrace)
	}

	_ = terminateProcessGroup(c.cmd.Process)
	timer := time.NewTimer(closeKillEscalationGrace)
	defer timer.Stop()
	<-timer.C
	_ = killProcessGroup(c.cmd.Process)
}

// isClosed reports whether the client is closed.
func (c *Client) isClosed() bool {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	return c.closed
}

// call sends one request and waits for its response or ctx cancellation.
// On ctx cancellation the pending waiter is removed so a late response
// cannot leak a goroutine.
func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if c.isClosed() {
		return nil, transportErrorf("client is closed")
	}
	id := c.nextID.Add(1)
	idRaw, _ := json.Marshal(id)

	var paramsRaw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("acp: marshal params for %s: %w", method, err)
		}
		paramsRaw = b
	}
	req := wireMessage{JSONRPC: "2.0", ID: idRaw, Method: method, Params: paramsRaw}
	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("acp: marshal request %s: %w", method, err)
	}

	ch := make(chan callResult, 1)
	key := string(idRaw)
	c.pendingMu.Lock()
	if c.isClosed() {
		c.pendingMu.Unlock()
		return nil, transportErrorf("client is closed")
	}
	c.pending[key] = pendingCall{ch: ch}
	c.pendingMu.Unlock()

	c.writeMu.Lock()
	_, werr := c.stdin.writeLine(line)
	c.writeMu.Unlock()
	if werr != nil {
		c.removePending(key)
		return nil, transportErrorf("write %s: %v", method, werr)
	}
	if fn := c.getOnRequestWritten(); fn != nil {
		fn(method)
	}

	select {
	case <-ctx.Done():
		c.removePending(key)
		return nil, ctx.Err()
	case res := <-ch:
		if res.transportErr != nil {
			return nil, res.transportErr
		}
		if res.rpcErr != nil {
			return nil, &RPCError{Code: res.rpcErr.Code, Message: res.rpcErr.Message}
		}
		return res.result, nil
	}
}

// notify sends a one-way notification; no response is expected.
func (c *Client) notify(method string, params any) error {
	if c.isClosed() {
		return errors.New("acp: client is closed")
	}
	var paramsRaw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("acp: marshal params for %s: %w", method, err)
		}
		paramsRaw = b
	}
	msg := wireMessage{JSONRPC: "2.0", Method: method, Params: paramsRaw}
	line, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("acp: marshal notification %s: %w", method, err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.stdin.writeLine(line)
	return err
}

func (c *Client) removePending(key string) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	delete(c.pending, key)
}

func (c *Client) failAllPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.failAllPendingLocked(err)
}

func (c *Client) failAllPendingLocked(err error) {
	for key, p := range c.pending {
		select {
		case p.ch <- callResult{transportErr: err}:
		default:
		}
		delete(c.pending, key)
	}
}
