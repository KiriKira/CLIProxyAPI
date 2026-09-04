package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestIsSignInRequired(t *testing.T) {
	if !IsSignInRequired(&RPCError{Code: -32000, Message: "Authentication required"}) {
		t.Fatal("expected -32000 to count as sign-in required")
	}
	if !IsSignInRequired(fmt.Errorf("agent call: %w", &RPCError{Code: -32000, Message: "nope"})) {
		t.Fatal("expected wrapped -32000 to count as sign-in required")
	}
	if IsSignInRequired(&RPCError{Code: -32601, Message: "not found"}) {
		t.Fatal("other RPC codes must not count as sign-in required")
	}
	if IsSignInRequired(transportErrorf("agent stdout closed")) {
		t.Fatal("transport errors must not count as sign-in required")
	}
	if IsSignInRequired(nil) {
		t.Fatal("nil must not count as sign-in required")
	}
}

func TestErrorTypesAreDistinct(t *testing.T) {
	rpcErr := &RPCError{Code: -32601, Message: "nope"}
	if rpcErr.Error() == "" {
		t.Fatal("empty RPCError message")
	}
	var asRPC *RPCError
	if !errors.As(rpcErr, &asRPC) {
		t.Fatal("RPCError does not match itself")
	}
	var asTransport *TransportError
	if errors.As(rpcErr, &asTransport) {
		t.Fatal("RPCError must not match TransportError")
	}

	transportErr := transportErrorf("agent stdout closed")
	if !errors.As(transportErr, &asTransport) {
		t.Fatal("TransportError does not match itself")
	}
	if errors.As(transportErr, &asRPC) {
		t.Fatal("TransportError must not match RPCError")
	}
}

// TestAgentErrorSurfacesRPCError drives a pipe-backed client against a
// scripted peer that answers initialize with a JSON-RPC error object.
func TestAgentErrorSurfacesRPCError(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c := newClientWithPipes(toAgentW, fromAgentR, strings.NewReader(""), nil, nil)
	t.Cleanup(func() { _ = c.Close() })

	// The reply loop is the sole reader of toAgentR. It waits for the
	// client's initialize request before answering: replying blind would
	// race pending-call registration and the response would be dropped.
	go func() {
		sc := bufio.NewScanner(toAgentR)
		sc.Buffer(make([]byte, 0, 64*1024), maxACPMessageBytes)
		for sc.Scan() {
			var msg wireMessage
			if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
				continue
			}
			if msg.Method != "initialize" || !msg.isRequest() {
				continue
			}
			reply, _ := json.Marshal(wireMessage{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Error:   &rpcErrorObject{Code: -32601, Message: "unknown method"},
			})
			_, _ = fromAgentW.Write(append(reply, '\n'))
			return
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Initialize(ctx, "test", "test")
	if err == nil {
		t.Fatal("expected agent error")
	}
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err type = %T (%v), want *RPCError", err, err)
	}
	if rpcErr.Code != -32601 {
		t.Fatalf("code = %d, want -32601", rpcErr.Code)
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		t.Fatalf("agent refusal must not surface as TransportError: %v", err)
	}
}

// TestTransportErrorOnAgentExit proves an EOF on stdout fails the in-flight
// call with a TransportError, not an RPCError.
func TestTransportErrorOnAgentExit(t *testing.T) {
	toAgentR, toAgentW := io.Pipe()
	fromAgentR, fromAgentW := io.Pipe()
	c := newClientWithPipes(toAgentW, fromAgentR, strings.NewReader(""), nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	go func() {
		_, _ = io.Copy(io.Discard, toAgentR)
	}()

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := c.Initialize(ctx, "test", "test")
		errCh <- err
	}()
	// Let the call register, then EOF the agent stdout side.
	time.Sleep(300 * time.Millisecond)
	_ = fromAgentW.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected transport error")
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("err type = %T (%v), want *TransportError", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for transport failure")
	}
}
