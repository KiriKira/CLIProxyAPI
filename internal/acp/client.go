package acp

import (
	"context"
	"encoding/json"
	"fmt"
)

// InitializeResult is the exported outcome of the ACP initialize handshake:
// the agent-selected protocol version plus its capabilities and supported
// auth methods. RawMessage fields preserve unknown shapes for forward
// compatibility.
type InitializeResult struct {
	ProtocolVersion   int
	AgentCapabilities json.RawMessage
	AgentInfo         json.RawMessage
	AuthMethods       []json.RawMessage
}

// PromptBlock is the exported content block for prompt turns. The canonical
// type is promptBlock; this alias keeps cross-package references stable.
type PromptBlock = promptBlock

// NewTextPrompt builds a single text-block prompt turn.
func NewTextPrompt(text string) []PromptBlock {
	return []PromptBlock{{Type: "text", Text: text}}
}

// Initialize performs the ACP initialize handshake, advertising protocol
// version 1 with the safe text-generation-only capabilities (no fs, no
// terminal). It returns the agent-selected version and capabilities.
func (c *Client) Initialize(ctx context.Context, name, version string) (*InitializeResult, error) {
	raw, err := c.call(ctx, "initialize", initializeRequest{
		ProtocolVersion:    protocolVersion,
		ClientCapabilities: defaultClientCapabilities(),
		ClientInfo:         clientInfo{Name: name, Version: version},
	})
	if err != nil {
		return nil, err
	}
	var resp initializeResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("acp: decode initialize response: %w", err)
	}
	return &InitializeResult{
		ProtocolVersion:   resp.ProtocolVersion,
		AgentCapabilities: resp.AgentCapabilities,
		AgentInfo:         resp.AgentInfo,
		AuthMethods:       resp.AuthMethods,
	}, nil
}

// Authenticate selects one of the agent-advertised auth methods. The result
// payload is method-specific and intentionally not decoded here.
func (c *Client) Authenticate(ctx context.Context, methodID string) error {
	_, err := c.call(ctx, "authenticate", authenticateRequest{MethodID: methodID})
	if err != nil {
		return err
	}
	return nil
}

// NewSession opens a new agent session rooted at cwd. MCPServers is forced
// to an empty array (never null) because the ACP schema expects an array.
func (c *Client) NewSession(ctx context.Context, cwd string) (string, error) {
	raw, err := c.call(ctx, "session/new", newSessionRequest{
		CWD:        cwd,
		MCPServers: []json.RawMessage{},
	})
	if err != nil {
		return "", err
	}
	var resp newSessionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("acp: decode session/new response: %w", err)
	}
	if resp.SessionID == "" {
		return "", fmt.Errorf("acp: session/new returned empty sessionId")
	}
	return resp.SessionID, nil
}

// Prompt delivers one conversational turn and waits for the terminal
// result. Streaming updates arrive via the OnUpdate callback. It returns
// the terminal stop reason (e.g. "end_turn", "cancelled").
func (c *Client) Prompt(ctx context.Context, sessionID string, prompt []PromptBlock) (string, error) {
	raw, err := c.call(ctx, "session/prompt", promptRequest{
		SessionID: sessionID,
		Prompt:    prompt,
	})
	if err != nil {
		return "", err
	}
	var resp promptResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("acp: decode session/prompt response: %w", err)
	}
	return resp.StopReason, nil
}

// Cancel interrupts the in-flight prompt turn for sessionID. It is a
// one-way notification: completion is signaled by the prompt response
// carrying a cancelled stopReason.
func (c *Client) Cancel(sessionID string) error {
	return c.notify("session/cancel", cancelNotification{SessionID: sessionID})
}
