package acp

import (
	"bufio"
	"encoding/json"
	"io"
)

// stdinWriter serializes newline-delimited writes to the agent's stdin.
type stdinWriter struct {
	w io.WriteCloser
}

func (s *stdinWriter) writeLine(b []byte) (int, error) {
	n, err := s.w.Write(append(b, '\n'))
	return n, err
}

func (s *stdinWriter) close() error { return s.w.Close() }

// readLoop owns stdout scanning; it runs on a single goroutine, so no lock
// is needed for the reader. Replies to agent-initiated requests are small
// and non-blocking, so answering inline here cannot stall the loop.
func (c *Client) readLoop() {
	for c.reader.Scan() {
		var msg wireMessage
		if err := json.Unmarshal(c.reader.Bytes(), &msg); err != nil {
			continue // malformed line: drop it, agent stderr carries diagnostics
		}
		if fn := c.getOnFirstLine(); fn != nil {
			fn()
		}
		switch {
		case msg.isResponse():
			c.dispatchResponse(&msg)
		case msg.isRequest():
			c.dispatchRequest(&msg)
		case msg.isNotification():
			c.dispatchNotification(&msg)
		}
	}
	c.failAllPending(transportErrorf("agent stdout closed"))
}

func (c *Client) dispatchResponse(m *wireMessage) {
	c.pendingMu.Lock()
	p, ok := c.pending[string(m.ID)]
	if ok {
		delete(c.pending, string(m.ID))
	}
	c.pendingMu.Unlock()
	if !ok {
		return
	}
	p.ch <- callResult{result: m.Result, rpcErr: m.Error}
}

func (c *Client) dispatchRequest(m *wireMessage) {
	var result json.RawMessage
	var rpcErr *rpcErrorObject
	switch m.Method {
	case "session/request_permission":
		// Safe posture: deny without killing the turn.
		result = cancelledPermissionResult()
	default:
		// fs/*, terminal/* are declared unsupported in initialize.
		rpcErr = &rpcErrorObject{Code: rpcMethodNotFoundCode,
			Message: "method not supported by client: " + m.Method}
	}
	resp := wireMessage{JSONRPC: "2.0", ID: m.ID}
	if rpcErr != nil {
		resp.Error = rpcErr
	} else {
		resp.Result = result
	}
	line, _ := json.Marshal(resp)
	// Reply asynchronously: the reader goroutine must never block on a
	// stdin write. If the agent floods stdout without draining stdin, a
	// full kernel pipe buffer would wedge the reader, which in turn stops
	// stdout draining — a mutual deadlock. writeMu still serializes all
	// writers; JSON-RPC imposes no response ordering.
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		_, _ = c.stdin.writeLine(line)
	}()
}

func (c *Client) dispatchNotification(m *wireMessage) {
	if m.Method != "session/update" {
		return
	}
	fn := c.getOnUpdate()
	if fn == nil {
		return
	}
	var params sessionUpdateParams
	if err := json.Unmarshal(m.Params, &params); err != nil {
		return
	}
	var env sessionUpdateEnvelope
	if err := json.Unmarshal(params.Update, &env); err != nil {
		return
	}
	fn(SessionUpdate{SessionID: params.SessionID, Kind: env.SessionUpdate, Raw: params.Update})
}

func (c *Client) drainStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxACPMessageBytes)
	for sc.Scan() {
		if c.logStderr != nil {
			c.logStderr(sc.Text()) // never log verbatim upstream
		}
	}
}
