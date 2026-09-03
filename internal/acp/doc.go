// Package acp implements a minimal supervised stdio JSON-RPC 2.0 client
// for the Agent Client Protocol (ACP).
//
// Slice 1 scope: process supervision, bidirectional request/response
// dispatch, the minimal lifecycle (initialize, authenticate, session/new,
// session/prompt, session/cancel), safe default handlers for agent-to-client
// requests (deny permissions, method-not-found for fs/terminal), and
// idempotent close. It intentionally covers only the protocol subset needed
// by the future Antigravity ACP executor.
package acp
