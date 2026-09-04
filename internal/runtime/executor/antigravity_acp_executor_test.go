package executor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func createFakeAgentScript(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake_agent.sh")

	// Create a minimal fake ACP agent script in bash
	script := `#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *"\"method\":\"initialize\""* ]]; then
    echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"capabilities":{},"agentInfo":{"name":"fake-agent","version":"1.0.0"}}}'
  elif [[ "$line" == *"\"method\":\"session/new\""* ]]; then
    echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"sess-123","configOptions":[{"id":"model","type":"select","options":[{"value":"gemini-3.8-flash"}]}]}}'
  elif [[ "$line" == *"\"method\":\"session/set_config_option\""* ]]; then
    echo '{"jsonrpc":"2.0","id":3,"result":{}}'
  elif [[ "$line" == *"\"method\":\"session/prompt\""* ]]; then
    # Emit thought update
    echo '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-123","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking step"}}}}'
    # Emit message update
    echo '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-123","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello from acp"}}}}'
    # Emit prompt response
    echo '{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn"}}'
  fi
done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake agent script: %v", err)
	}
	return scriptPath
}

func TestAntigravityAcpExecutorExecute(t *testing.T) {
	script := createFakeAgentScript(t)
	cfg := &internalconfig.Config{}
	exec := NewAntigravityAcpExecutor(cfg)

	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := exec.Execute(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(resp.Payload) == 0 {
		t.Fatalf("expected non-empty payload")
	}

	var data struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(data.Choices) == 0 {
		t.Fatalf("expected choices in response")
	}

	if data.Choices[0].Message.Content != "hello from acp" {
		t.Errorf("content = %q, want 'hello from acp'", data.Choices[0].Message.Content)
	}
	if data.Choices[0].Message.ReasoningContent != "thinking step" {
		t.Errorf("reasoning_content = %q, want 'thinking step'", data.Choices[0].Message.ReasoningContent)
	}
	if data.Choices[0].FinishReason != "end_turn" {
		t.Errorf("finish_reason = %q, want 'end_turn'", data.Choices[0].FinishReason)
	}
}

func TestAntigravityAcpExecutorExecuteStream(t *testing.T) {
	script := createFakeAgentScript(t)
	cfg := &internalconfig.Config{}
	exec := NewAntigravityAcpExecutor(cfg)

	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{
			"binary_path": script,
			"gemini_home": t.TempDir(),
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gemini-3.8-flash",
		Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}

	var receivedChunks []string
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		receivedChunks = append(receivedChunks, string(chunk.Payload))
	}

	if len(receivedChunks) == 0 {
		t.Fatalf("expected streamed chunks")
	}

	var hasThought, hasMessage, hasDone bool
	for _, c := range receivedChunks {
		if c == "data: [DONE]\n\n" {
			hasDone = true
		}
		if len(c) > 0 && c != "data: [DONE]\n\n" {
			if len(c) > 6 && c[:6] == "data: " {
				var delta struct {
					Choices []struct {
						Delta struct {
							Content          string `json:"content"`
							ReasoningContent string `json:"reasoning_content"`
						} `json:"delta"`
					} `json:"choices"`
				}
				_ = json.Unmarshal([]byte(c[6:]), &delta)
				if len(delta.Choices) > 0 {
					if delta.Choices[0].Delta.Content == "hello from acp" {
						hasMessage = true
					}
					if delta.Choices[0].Delta.ReasoningContent == "thinking step" {
						hasThought = true
					}
				}
			}
		}
	}

	if !hasThought {
		t.Errorf("missing expected thought chunk in stream")
	}
	if !hasMessage {
		t.Errorf("missing expected message chunk in stream")
	}
	if !hasDone {
		t.Errorf("missing [DONE] terminal chunk in stream")
	}
}
