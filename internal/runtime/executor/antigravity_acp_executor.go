package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// AntigravityAcpExecutor bridges incoming OpenAI/Claude requests to the local
// official Antigravity ACP stdio daemon.
type AntigravityAcpExecutor struct {
	cfg *internalconfig.Config
}

// NewAntigravityAcpExecutor creates a new ACP executor instance.
func NewAntigravityAcpExecutor(cfg *internalconfig.Config) *AntigravityAcpExecutor {
	return &AntigravityAcpExecutor{
		cfg: cfg,
	}
}

// Identifier returns the provider identifier for this executor.
func (e *AntigravityAcpExecutor) Identifier() string {
	return constant.Antigravity
}

// Refresh is a no-op for ACP as auth is handled locally or via binary profiles.
func (e *AntigravityAcpExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return auth, nil
	}
	return auth, nil
}

// HttpRequest executes an arbitrary HTTP request with auth credentials (not supported for ACP stdio).
func (e *AntigravityAcpExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("HttpRequest not supported for ACP stdio provider")
}

// CountTokens implements token estimation or fallback.
func (e *AntigravityAcpExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{
		Payload: []byte(`{"totalTokens": 0}`),
	}, nil
}

func (e *AntigravityAcpExecutor) getBinaryConfig(auth *cliproxyauth.Auth) (command string, args []string, geminiHome string, apiKey string) {
	command = "agy_acp_server.par"
	if auth != nil && auth.Attributes != nil {
		if cmd := auth.Attributes["binary_path"]; cmd != "" {
			command = cmd
		}
		if home := auth.Attributes["gemini_home"]; home != "" {
			geminiHome = home
		}
		if key := auth.Attributes["api_key"]; key != "" {
			apiKey = key
		}
	}
	if geminiHome == "" {
		geminiHome = os.Getenv("GEMINI_HOME")
		if geminiHome == "" {
			geminiHome = os.TempDir()
		}
	}
	args = []string{"--uid="}
	return
}

func (e *AntigravityAcpExecutor) buildClient(auth *cliproxyauth.Auth) (*acp.Client, error) {
	cmd, args, geminiHome, apiKey := e.getBinaryConfig(auth)

	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"GEMINI_HOME=" + geminiHome,
		"AGY_ACP_FORCE_FILE_STORAGE=1",
		"BROWSER=/bin/true",
		"PYTHONUNBUFFERED=1",
		"ELECTRON_RUN_AS_NODE=1",
	}
	if apiKey != "" {
		env = append(env, "GEMINI_API_KEY="+apiKey)
	}

	cfg := acp.SpawnConfig{
		Command: cmd,
		Args:    args,
		Env:     env,
	}
	return acp.NewClient(cfg)
}

// extractPromptBlocks converts raw OpenAI/Claude JSON payload to an ACP PromptBlock slice.
func extractPromptBlocks(payload []byte) []acp.PromptBlock {
	var blocks []acp.PromptBlock

	// Check if this is an OpenAI-style messages array
	msgs := gjson.GetBytes(payload, "messages")
	if msgs.Exists() && msgs.IsArray() {
		var sb strings.Builder
		for _, m := range msgs.Array() {
			role := m.Get("role").String()
			content := m.Get("content").String()
			if role != "" && content != "" {
				sb.WriteString(fmt.Sprintf("[%s]: %s\n\n", strings.ToUpper(role), content))
			}
		}
		if sb.Len() > 0 {
			blocks = append(blocks, acp.PromptBlock{
				Type: "text",
				Text: strings.TrimSpace(sb.String()),
			})
			return blocks
		}
	}

	// Check if this is a raw prompt or string
	prompt := gjson.GetBytes(payload, "prompt")
	if prompt.Exists() && prompt.String() != "" {
		blocks = append(blocks, acp.PromptBlock{
			Type: "text",
			Text: prompt.String(),
		})
		return blocks
	}

	// Fallback to raw string payload
	blocks = append(blocks, acp.PromptBlock{
		Type: "text",
		Text: string(payload),
	})
	return blocks
}

// Execute performs a non-streaming ACP prompt turn.
func (e *AntigravityAcpExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if opts.Alt == "responses/compact" {
		return resp, statusErr{code: http.StatusNotImplemented, msg: "/responses/compact not supported"}
	}

	client, err := e.buildClient(auth)
	if err != nil {
		return resp, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("failed to spawn ACP agent: %v", err)}
	}
	defer client.Close()

	if _, err := client.Initialize(ctx, "CLIProxyAPI", "1.0.0"); err != nil {
		return resp, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP initialize failed: %v", err)}
	}

	cwd, _ := os.Getwd()
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		return resp, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP session/new failed: %v", err)}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	if baseModel != "" {
		_, _ = client.SetConfigOption(ctx, sessionID, "model", baseModel)
	}

	var responseText strings.Builder
	var thoughtText strings.Builder
	var mu sync.Mutex

	client.OnUpdate(func(u acp.SessionUpdate) {
		if u.SessionID != sessionID {
			return
		}
		mu.Lock()
		defer mu.Unlock()

		switch u.Kind {
		case "agent_message_chunk":
			var chunk struct {
				Content struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
				responseText.WriteString(chunk.Content.Text)
			}
		case "agent_thought_chunk":
			var chunk struct {
				Content struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
				thoughtText.WriteString(chunk.Content.Text)
			}
		}
	})

	blocks := extractPromptBlocks(req.Payload)
	stopReason, err := client.Prompt(ctx, sessionID, blocks)
	if err != nil {
		return resp, statusErr{code: http.StatusInternalServerError, msg: fmt.Sprintf("ACP prompt error: %v", err)}
	}

	mu.Lock()
	finalText := responseText.String()
	finalThought := thoughtText.String()
	mu.Unlock()

	respPayload := map[string]interface{}{
		"id":      fmt.Sprintf("chatcmpl-acp-%d", time.Now().UnixNano()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   req.Model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]interface{}{
					"role":              "assistant",
					"content":           finalText,
					"reasoning_content": finalThought,
				},
				"finish_reason": stopReason,
			},
		},
	}

	rawResp, err := json.Marshal(respPayload)
	if err != nil {
		return resp, statusErr{code: http.StatusInternalServerError, msg: "failed to marshal response"}
	}

	return cliproxyexecutor.Response{
		Payload: rawResp,
		Headers: make(http.Header),
	}, nil
}

// ExecuteStream performs streaming execution via ACP session/prompt.
func (e *AntigravityAcpExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusNotImplemented, msg: "/responses/compact not supported"}
	}

	client, err := e.buildClient(auth)
	if err != nil {
		return nil, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("failed to spawn ACP agent: %v", err)}
	}

	if _, err := client.Initialize(ctx, "CLIProxyAPI", "1.0.0"); err != nil {
		client.Close()
		return nil, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP initialize failed: %v", err)}
	}

	cwd, _ := os.Getwd()
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		client.Close()
		return nil, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP session/new failed: %v", err)}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	if baseModel != "" {
		_, _ = client.SetConfigOption(ctx, sessionID, "model", baseModel)
	}

	chunkChan := make(chan cliproxyexecutor.StreamChunk, 128)
	result := &cliproxyexecutor.StreamResult{
		Headers: make(http.Header),
		Chunks:  chunkChan,
	}

	go func() {
		defer client.Close()
		defer close(chunkChan)

		client.OnUpdate(func(u acp.SessionUpdate) {
			if u.SessionID != sessionID {
				return
			}

			switch u.Kind {
			case "agent_message_chunk":
				var chunk struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
					ssePayload := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%s}}]}\n\n", string(mustMarshal(chunk.Content.Text)))
					chunkChan <- cliproxyexecutor.StreamChunk{
						Payload: []byte(ssePayload),
					}
				}
			case "agent_thought_chunk":
				var chunk struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
					ssePayload := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"reasoning_content\":%s}}]}\n\n", string(mustMarshal(chunk.Content.Text)))
					chunkChan <- cliproxyexecutor.StreamChunk{
						Payload: []byte(ssePayload),
					}
				}
			}
		})

		blocks := extractPromptBlocks(req.Payload)
		_, promptErr := client.Prompt(ctx, sessionID, blocks)
		if promptErr != nil {
			log.Errorf("ACP prompt stream error: %v", promptErr)
			chunkChan <- cliproxyexecutor.StreamChunk{
				Err: promptErr,
			}
			return
		}

		chunkChan <- cliproxyexecutor.StreamChunk{
			Payload: []byte("data: [DONE]\n\n"),
		}
	}()

	return result, nil
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
