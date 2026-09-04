package acp

import "fmt"

// RPCError is an error response returned by the agent to one of our
// requests. It carries the JSON-RPC error code and message verbatim.
type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("acp: agent error %d: %s", e.Code, e.Message)
}

// TransportError is a local client-side failure: process exit, stdout
// closed, client closed, write failure. It is deliberately distinct from
// RPCError so callers never mistake a dead transport for an agent refusal.
type TransportError struct {
	msg string
}

func (e *TransportError) Error() string { return e.msg }

func transportErrorf(format string, args ...any) *TransportError {
	return &TransportError{msg: fmt.Sprintf("acp: "+format, args...)}
}
