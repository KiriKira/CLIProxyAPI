package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// Supported ACP authenticate method ids, mirroring the official agent's
// initialize advertisement.
const (
	acpAuthOAuthPersonal = "oauth-personal"
	acpAuthOAuthBusiness = "oauth-business"
	acpAuthGeminiAPIKey  = "gemini-api-key"
	acpAuthAgentPlatform = "agent-platform"
)

// acpAuthConfig is the resolved credential selection for one request. It
// never carries token material: API keys stay in the spawned environment,
// OAuth tokens stay in the agent profile directory.
type acpAuthConfig struct {
	method      string
	apiKey      string
	gcpProject  string
	gcpLocation string
}

func (e *AntigravityAcpExecutor) attr(auth *cliproxyauth.Auth, key string) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	return strings.TrimSpace(auth.Attributes[key])
}

// resolveAuthMethod picks the ACP authenticate method id.
func (e *AntigravityAcpExecutor) resolveAuthMethod(auth *cliproxyauth.Auth) string {
	if m := e.attr(auth, "auth_method"); m != "" {
		return m
	}
	if e.cfg != nil && strings.TrimSpace(e.cfg.Antigravity.AuthMethod) != "" {
		return strings.TrimSpace(e.cfg.Antigravity.AuthMethod)
	}
	return acpAuthOAuthPersonal
}

// resolveAuthConfig validates the credential selection for the method,
// mirroring the T3 provider's config-issue checks so misconfiguration fails
// fast with an actionable message instead of an opaque agent error.
func (e *AntigravityAcpExecutor) resolveAuthConfig(auth *cliproxyauth.Auth) (*acpAuthConfig, error) {
	method := e.resolveAuthMethod(auth)
	switch method {
	case acpAuthOAuthPersonal, acpAuthOAuthBusiness, acpAuthGeminiAPIKey, acpAuthAgentPlatform:
	default:
		return nil, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("unknown Antigravity ACP auth method %q (want oauth-personal, oauth-business, gemini-api-key or agent-platform)", method)}
	}
	cfg := &acpAuthConfig{method: method}
	cfg.apiKey = e.attr(auth, "api_key")
	cfg.gcpProject = e.attr(auth, "gcp_project")
	cfg.gcpLocation = e.attr(auth, "gcp_location")
	if e.cfg != nil {
		if cfg.gcpProject == "" {
			cfg.gcpProject = strings.TrimSpace(e.cfg.Antigravity.GcpProject)
		}
		if cfg.gcpLocation == "" {
			cfg.gcpLocation = strings.TrimSpace(e.cfg.Antigravity.GcpLocation)
		}
	}
	switch method {
	case acpAuthGeminiAPIKey:
		if cfg.apiKey == "" {
			cfg.apiKey = strings.TrimSpace(os.Getenv("GEMINI_API_KEY"))
		}
		if cfg.apiKey == "" {
			return nil, statusErr{code: http.StatusUnauthorized, msg: "Antigravity ACP gemini-api-key method needs an api_key auth attribute or GEMINI_API_KEY"}
		}
	case acpAuthOAuthBusiness:
		if cfg.gcpProject == "" || cfg.gcpLocation == "" {
			return nil, statusErr{code: http.StatusUnauthorized, msg: "Antigravity ACP oauth-business needs a GCP project and location (gcp_project/gcp_location)"}
		}
	case acpAuthAgentPlatform:
		if cfg.apiKey == "" {
			cfg.apiKey = strings.TrimSpace(os.Getenv("GOOGLE_API_KEY"))
		}
		if cfg.apiKey == "" && (cfg.gcpProject == "" || cfg.gcpLocation == "") {
			return nil, statusErr{code: http.StatusUnauthorized, msg: "Antigravity ACP agent-platform needs an api_key or a GCP project and location"}
		}
	}
	return cfg, nil
}

// resolveBinary locates the official ACP server executable. Explicit paths
// must exist; bare names fall back to PATH lookup.
func (e *AntigravityAcpExecutor) resolveBinary(auth *cliproxyauth.Auth) (string, error) {
	candidates := []string{e.attr(auth, "binary_path")}
	if e.cfg != nil {
		candidates = append(candidates, strings.TrimSpace(e.cfg.Antigravity.BinaryPath))
	}
	candidates = append(candidates, strings.TrimSpace(os.Getenv("AGY_ACP_BINARY")))
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if strings.Contains(c, string(os.PathSeparator)) {
			if st, err := os.Stat(c); err != nil || st.IsDir() {
				continue
			}
			return c, nil
		}
		if p, err := exec.LookPath(c); err == nil {
			return p, nil
		}
	}
	for _, name := range []string{"agy_acp_server.par", "agy_acp_server"} {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}
	return "", statusErr{code: http.StatusBadGateway, msg: "Antigravity ACP binary not found (set antigravity.binary-path, binary_path auth attribute or AGY_ACP_BINARY)"}
}

// resolveHarness returns the harness executable shipped alongside the agent
// binary, or "" when absent. The agent still starts without it.
func resolveHarness(binary string) string {
	dir := filepath.Dir(binary)
	for _, name := range []string{"localharness_external", "localharness_external.exe"} {
		p := filepath.Join(dir, name)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

// sanitizeProfileName keeps an auth ID safe as a single path segment.
func sanitizeProfileName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

// resolveProfileDir returns the GEMINI_HOME for this credential. An explicit
// gemini_home (auth attribute, config, or environment) is honored verbatim
// so owners of an existing Antigravity login keep using it; otherwise a
// per-auth isolated directory persists the agent token across requests.
func (e *AntigravityAcpExecutor) resolveProfileDir(auth *cliproxyauth.Auth) string {
	if home := e.attr(auth, "gemini_home"); home != "" {
		return home
	}
	if e.cfg != nil && strings.TrimSpace(e.cfg.Antigravity.GeminiHome) != "" {
		return strings.TrimSpace(e.cfg.Antigravity.GeminiHome)
	}
	if env := strings.TrimSpace(os.Getenv("GEMINI_HOME")); env != "" {
		return env
	}
	base, err := os.UserHomeDir()
	if err != nil || base == "" {
		return filepath.Join(os.TempDir(), "cli-proxy-api-antigravity-acp")
	}
	name := "default"
	if auth != nil && strings.TrimSpace(auth.ID) != "" {
		name = sanitizeProfileName(auth.ID)
	}
	return filepath.Join(base, ".cli-proxy-api", "antigravity-acp", name)
}

// writeProfileSettings rewrites the agent profile settings on every launch
// so method/project/location edits take effect immediately. It never stores
// credentials; the agent owns its token file after sign-in.
func writeProfileSettings(geminiHome string, cfg *acpAuthConfig) error {
	acpDir := filepath.Join(geminiHome, "antigravity-acp")
	if err := os.MkdirAll(acpDir, 0o700); err != nil {
		return fmt.Errorf("create ACP profile dir: %w", err)
	}
	settings := map[string]any{"auth": map[string]any{"type": cfg.method}}
	if cfg.gcpProject != "" || cfg.gcpLocation != "" {
		gcp := map[string]any{}
		if cfg.gcpProject != "" {
			gcp["project"] = cfg.gcpProject
		}
		if cfg.gcpLocation != "" {
			gcp["location"] = cfg.gcpLocation
		}
		settings["gcp"] = gcp
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal ACP profile settings: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(acpDir, "settings.json"), raw, 0o600); err != nil {
		return fmt.Errorf("write ACP profile settings: %w", err)
	}
	return nil
}

// acpAuthenticateTimeout bounds the authenticate step. It is the only ACP
// call allowed a timeout: it runs during credential acquisition, and a fresh
// profile makes the agent print a plain-text auth URL on stdout and block on
// a browser login that can never complete headless. Without the bound the
// request would hang until the caller's context expires. Overridable in tests.
var acpAuthenticateTimeout = 60 * time.Second

// spawnClient launches the agent with a method-scoped environment and runs
// initialize plus authenticate. The caller owns Close.
func (e *AntigravityAcpExecutor) spawnClient(ctx context.Context, auth *cliproxyauth.Auth) (*acp.Client, error) {
	ac, err := e.resolveAuthConfig(auth)
	if err != nil {
		return nil, err
	}
	binary, err := e.resolveBinary(auth)
	if err != nil {
		return nil, err
	}
	profileDir := e.resolveProfileDir(auth)
	if err := writeProfileSettings(profileDir, ac); err != nil {
		return nil, statusErr{code: http.StatusBadGateway, msg: err.Error()}
	}

	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"GEMINI_HOME=" + profileDir,
		"AGY_ACP_FORCE_FILE_STORAGE=1",
		"BROWSER=/bin/true",
		"PYTHONUNBUFFERED=1",
		"ELECTRON_RUN_AS_NODE=1",
	}
	// Only the selected method's credential reaches the agent.
	switch ac.method {
	case acpAuthGeminiAPIKey:
		env = append(env, "GEMINI_API_KEY="+ac.apiKey)
	case acpAuthAgentPlatform:
		if ac.apiKey != "" {
			env = append(env, "GOOGLE_API_KEY="+ac.apiKey)
		}
	}
	if harness := resolveHarness(binary); harness != "" {
		env = append(env, "ANTIGRAVITY_HARNESS_PATH="+harness)
	}

	client, err := acp.NewClient(acp.SpawnConfig{
		Command: binary,
		Args:    acpUIDArgs(),
		Env:     env,
	})
	if err != nil {
		return nil, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("failed to spawn ACP agent: %v", err)}
	}
	if _, err := client.Initialize(ctx, "CLIProxyAPI", "1.0.0"); err != nil {
		client.Close()
		return nil, statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP initialize failed: %v", err)}
	}
	authCtx, cancelAuth := context.WithTimeout(ctx, acpAuthenticateTimeout)
	defer cancelAuth()
	if err := client.Authenticate(authCtx, ac.method); err != nil {
		client.Close()
		if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return nil, statusErr{code: http.StatusUnauthorized, msg: "Antigravity sign-in required: complete the Google login for this profile, then retry"}
		}
		return nil, e.mapAuthError(ac, err)
	}
	return client, nil
}

// acpUIDArgs mirrors the official launch contract: --uid= on linux.
func acpUIDArgs() []string {
	if runtime.GOOS == "linux" {
		return []string{"--uid="}
	}
	return nil
}

// mapAuthError turns agent authentication refusals into actionable 401s.
func (e *AntigravityAcpExecutor) mapAuthError(ac *acpAuthConfig, err error) error {
	if acp.IsSignInRequired(err) {
		switch ac.method {
		case acpAuthOAuthPersonal, acpAuthOAuthBusiness:
			return statusErr{code: http.StatusUnauthorized, msg: "Antigravity sign-in required: complete the Google login for this profile, then retry"}
		default:
			return statusErr{code: http.StatusUnauthorized, msg: fmt.Sprintf("Antigravity %s credential rejected: %v", ac.method, err)}
		}
	}
	return statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP authenticate failed: %v", err)}
}

// openSession creates one isolated session and applies the requested model.
func openSession(ctx context.Context, client *acp.Client, model string) (string, error) {
	cwd, _ := os.Getwd()
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		if acp.IsSignInRequired(err) {
			return "", statusErr{code: http.StatusUnauthorized, msg: "Antigravity sign-in required: complete the Google login for this profile, then retry"}
		}
		return "", statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP session/new failed: %v", err)}
	}
	if baseModel := thinking.ParseSuffix(model).ModelName; baseModel != "" {
		_, _ = client.SetConfigOption(ctx, sessionID, "model", baseModel)
	}
	return sessionID, nil
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

	client, err := e.spawnClient(ctx, auth)
	if err != nil {
		return resp, err
	}
	defer client.Close()

	sessionID, err := openSession(ctx, client, req.Model)
	if err != nil {
		return resp, err
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

	client, err := e.spawnClient(ctx, auth)
	if err != nil {
		return nil, err
	}

	sessionID, err := openSession(ctx, client, req.Model)
	if err != nil {
		client.Close()
		return nil, err
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
