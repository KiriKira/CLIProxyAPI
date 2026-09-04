package acp

import (
	"errors"
	"fmt"
)

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

// signInRequiredCode is the JSON-RPC error code the Antigravity agent
// returns when a call needs authentication (e.g. session/new before
// authenticate, or an expired login). It mirrors the agent's contract,
// not the generic JSON-RPC reserved range.
const signInRequiredCode = -32000

// IsSignInRequired reports whether err is an agent authentication refusal.
// Callers map it to an actionable sign-in message instead of a generic
// upstream failure.
func IsSignInRequired(err error) bool {
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	return rpcErr.Code == signInRequiredCode
}
