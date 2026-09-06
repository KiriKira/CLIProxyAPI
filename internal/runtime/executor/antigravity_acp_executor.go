package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// AntigravityAcpExecutor bridges incoming OpenAI/Claude requests to the local
// official Antigravity ACP stdio daemon.
type AntigravityAcpExecutor struct {
	cfg  *internalconfig.Config
	pool *helps.AntigravityAcpPool
}

func (e *AntigravityAcpExecutor) authPoolKey(auth *cliproxyauth.Auth) string {
	method := e.resolveAuthMethod(auth)
	profileDir := e.resolveProfileDir(auth)
	binary, _ := e.resolveBinary(auth)
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	return fmt.Sprintf("%s|%s|%s|%s", authID, method, profileDir, binary)
}

// antigravityPreparedSessionsPerWorker bounds the ready-session cache kept
// on each idle worker when prepared-sessions is enabled. One session per
// worker covers the sequential-request pattern without pre-creating a
// server-side session storm.
const antigravityPreparedSessionsPerWorker = 1

// NewAntigravityAcpExecutor creates a new ACP executor instance.
func NewAntigravityAcpExecutor(cfg *internalconfig.Config) *AntigravityAcpExecutor {
	exec := &AntigravityAcpExecutor{
		cfg: cfg,
	}

	persistent := true
	maxWorkers := 1
	var maxTotal int
	prepareSessions := false
	var idleTimeout time.Duration

	if cfg != nil {
		if cfg.Antigravity.PersistentProcess != nil {
			persistent = *cfg.Antigravity.PersistentProcess
		}
		if cfg.Antigravity.MaxWorkers > 0 {
			maxWorkers = cfg.Antigravity.MaxWorkers
		}
		if cfg.Antigravity.MaxWorkersTotal > 0 {
			maxTotal = cfg.Antigravity.MaxWorkersTotal
		}
		if cfg.Antigravity.PreparedSessions > 0 {
			prepareSessions = true
		}
		if d, err := time.ParseDuration(cfg.Antigravity.IdleTimeout); err == nil && d > 0 {
			idleTimeout = d
		}
	}

	if persistent {
		var prepareLimit int
		if prepareSessions {
			prepareLimit = antigravityPreparedSessionsPerWorker
		}
		exec.pool = helps.NewAntigravityAcpPoolWithLimits(maxWorkers, maxTotal, prepareLimit, idleTimeout, nil)
	}

	return exec
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
	// Best effort: keep a fresh access token in the daemon file. The agent
	// normalizes the file to refresh-only after each success, so without
	// this every spawn after the first would fall back to browser login.
	_ = ensureDaemonToken(ctx, profileDir, ac.method)

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

// acpTokenEndpoint is the Google OAuth token endpoint used to refresh the
// daemon token file. Overridable in tests.
var acpTokenEndpoint = "https://oauth2.googleapis.com/token"

// acpDaemonTokenFreshness is the minimum remaining token lifetime that
// counts as fresh. Below it the executor refreshes proactively.
const acpDaemonTokenFreshness = 10 * time.Minute

// acpTokenRefreshTimeout bounds the proactive refresh HTTP call. It runs
// during credential acquisition, the only phase where timeouts are allowed.
const acpTokenRefreshTimeout = 30 * time.Second

// acpDaemonOAuthClientID/Secret are the public installed-app OAuth client
// shipped inside the official ACP daemon (copied from the Go Antigravity CLI
// constants, as the daemon's own default). They are not user secrets: the
// client ID appears in every browser login URL the daemon prints, and the
// codebase already bundles its own OAuth clients the same way.
const (
	acpDaemonOAuthClientID     = "[REDACTED]"
	acpDaemonOAuthClientSecret = "[REDACTED]"
)

var acpDaemonDefaultScopes = []string{
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/userinfo.email",
	"https://www.googleapis.com/auth/aicode",
}

// daemonTokenFile returns the OAuth token filename for the method, or "" when
// the method keeps no file credential.
func daemonTokenFile(method string) string {
	switch method {
	case acpAuthOAuthPersonal:
		return "acp_token.json"
	case acpAuthOAuthBusiness:
		return "acp_business_token.json"
	default:
		return ""
	}
}

// daemonTokenFresh reports whether the blob already carries an access token
// valid well past now. Unknown shapes count as stale so the caller refreshes.
func daemonTokenFresh(blob map[string]any) bool {
	token, _ := blob["token"].(string)
	expiryRaw, _ := blob["expiry"].(string)
	if token == "" || expiryRaw == "" {
		return false
	}
	expiry, err := time.Parse(time.RFC3339, expiryRaw)
	if err != nil {
		return false
	}
	return time.Until(expiry) > acpDaemonTokenFreshness
}

// refreshDaemonAccessToken mints a fresh access token for a daemon-issued
// refresh token. It returns the access token, its expiry, and a rotated
// refresh token (empty when the server did not rotate).
func refreshDaemonAccessToken(ctx context.Context, clientID, clientSecret, refreshToken string) (accessToken string, expiry time.Time, rotatedRefresh string, err error) {
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"refresh_token": {refreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, acpTokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", time.Time{}, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", time.Time{}, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, "", fmt.Errorf("token refresh status %d", resp.StatusCode)
	}
	var decoded struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int64  `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", time.Time{}, "", err
	}
	if decoded.AccessToken == "" || decoded.ExpiresIn <= 0 {
		return "", time.Time{}, "", fmt.Errorf("token refresh returned no usable token")
	}
	return decoded.AccessToken, time.Now().Add(time.Duration(decoded.ExpiresIn) * time.Second).UTC(), decoded.RefreshToken, nil
}

// ensureDaemonToken keeps a fresh access token in the daemon token file so
// the agent never needs its in-daemon refresh path on spawn. Unknown or
// missing state is left alone (nil error): the daemon then reports sign-in
// required through the normal bounded authenticate step.
func ensureDaemonToken(ctx context.Context, profileDir, method string) error {
	name := daemonTokenFile(method)
	if name == "" {
		return nil
	}
	path := filepath.Join(profileDir, "antigravity-acp", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var blob map[string]any
	if err := json.Unmarshal(raw, &blob); err != nil || blob == nil {
		return nil
	}
	if daemonTokenFresh(blob) {
		return nil
	}
	refresh, _ := blob["refresh_token"].(string)
	if refresh == "" {
		return nil
	}
	clientID, _ := blob["client_id"].(string)
	clientSecret, _ := blob["client_secret"].(string)
	if clientID == "" {
		clientID = acpDaemonOAuthClientID
	}
	if clientSecret == "" {
		clientSecret = acpDaemonOAuthClientSecret
	}
	rctx, cancel := context.WithTimeout(ctx, acpTokenRefreshTimeout)
	defer cancel()
	accessToken, expiry, rotated, err := refreshDaemonAccessToken(rctx, clientID, clientSecret, refresh)
	if err != nil {
		log.Warnf("Antigravity ACP proactive token refresh failed: %v", err)
		return nil
	}
	blob["token"] = accessToken
	blob["expiry"] = expiry.Format(time.RFC3339)
	if rotated != "" {
		blob["refresh_token"] = rotated
	}
	blob["client_id"] = clientID
	blob["client_secret"] = clientSecret
	if _, ok := blob["token_uri"]; !ok {
		blob["token_uri"] = acpTokenEndpoint
	}
	if _, ok := blob["scopes"]; !ok {
		blob["scopes"] = acpDaemonDefaultScopes
	}
	out, err := json.Marshal(blob)
	if err != nil {
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Join(profileDir, "antigravity-acp"), "token-*.tmp")
	if err != nil {
		log.Warnf("Antigravity ACP token write failed: %v", err)
		return nil
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(append(out, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return nil
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil
	}
	_ = os.Chmod(tmpName, 0o600)
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		log.Warnf("Antigravity ACP token write failed: %v", err)
		return nil
	}
	_ = os.Chmod(path, 0o600)
	return nil
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

// Antigravity encodeless reasoning effort in the model name: every Flash
// family ships -high/-medium/-low variants and 3.1 Pro splits into
// gemini-pro-agent (high) / gemini-3.1-pro-low. There is no separate effort
// knob, so the proxy folds the repo-wide effort convention (model(value)
// suffix levels plus the reasoning_effort body field) into the variant name.
// This keeps "set model and effort independently" working end to end.
func acpTierForLevel(level thinking.ThinkingLevel) string {
	switch level {
	case thinking.LevelLow, thinking.LevelMinimal:
		return "low"
	case thinking.LevelMedium:
		return "medium"
	default:
		return "high"
	}
}

// acpTierFromSuffix interprets a model(value) suffix as an effort tier.
// Numeric budgets collapse to high (any positive budget) or low (zero);
// none maps to low because the daemon offers no off switch.
func acpTierFromSuffix(raw string) (string, bool) {
	if level, ok := thinking.ParseLevelSuffix(raw); ok {
		return acpTierForLevel(level), true
	}
	if mode, ok := thinking.ParseSpecialSuffix(raw); ok {
		switch mode {
		case thinking.ModeNone:
			return "low", true
		case thinking.ModeAuto:
			return "medium", true
		}
		return "", false
	}
	if budget, ok := thinking.ParseNumericSuffix(raw); ok {
		if budget <= 0 {
			return "low", true
		}
		return "high", true
	}
	return "", false
}

// acpTierFromBody reads the reasoning_effort body field, same vocabulary as
// the repo-wide thinking pipeline (none/minimal/low/medium/high/xhigh/max,
// plus auto which settles on medium).
func acpTierFromBody(payload []byte) (string, bool) {
	v := gjson.GetBytes(payload, "reasoning_effort")
	if !v.Exists() {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(v.String())) {
	case "none":
		return "low", true
	case "minimal", "low":
		return "low", true
	case "medium", "auto":
		return "medium", true
	case "high", "xhigh", "max":
		return "high", true
	}
	return "", false
}

// splitFlashVariant splits a Flash model name into family and tier. A bare
// family (gemini-3.7-flash) matches with an empty tier; anything else is
// left for the daemon to validate.
func splitFlashVariant(base string) (family, tier string, ok bool) {
	for _, t := range []string{"-high", "-medium", "-low"} {
		if strings.HasSuffix(base, t) {
			return strings.TrimSuffix(base, t), strings.TrimPrefix(t, "-"), true
		}
	}
	if strings.HasPrefix(base, "gemini-") && strings.Contains(base, "-flash") {
		return base, "", true
	}
	return "", "", false
}

func isProFamily(base string) bool {
	switch base {
	case "gemini-pro-agent", "gemini-3.1-pro", "gemini-3.1-pro-low", "gemini-3.1-pro-high":
		return true
	}
	return false
}

// resolveAntigravityModel folds model + effort into a daemon model variant.
// Suffix tier wins over the body field; without any effort signal an
// explicit variant stays untouched and a bare family defaults to high,
// matching the daemon-side default selection.
func resolveAntigravityModel(model string, payload []byte) string {
	suffix := thinking.ParseSuffix(model)
	base := suffix.ModelName
	tier := ""
	if suffix.HasSuffix {
		t, ok := acpTierFromSuffix(suffix.RawSuffix)
		if !ok {
			return base
		}
		tier = t
	} else if t, ok := acpTierFromBody(payload); ok {
		tier = t
	}
	if isProFamily(base) {
		// 3.1 Pro has no medium variant; medium clamps up to the high id.
		if tier == "low" {
			return "gemini-3.1-pro-low"
		}
		return "gemini-pro-agent"
	}
	family, cur, ok := splitFlashVariant(base)
	if !ok {
		return base
	}
	if tier == "" {
		tier = cur
		if tier == "" {
			tier = "high"
		}
	}
	return family + "-" + tier
}

// Attachment limits mirror the official client: text 1 MiB, images 10 MiB,
// audio 20 MiB, 50 MiB total per turn.
const (
	acpMaxImageBytes = 10 << 20
	acpMaxAudioBytes = 20 << 20
	acpMaxTextBytes  = 1 << 20
	acpMaxTotalBytes = 50 << 20
)

var acpImageMIMEs = map[string]bool{
	"image/bmp": true, "image/jpeg": true, "image/png": true, "image/webp": true,
}

var acpAudioMIMEs = map[string]bool{
	"audio/aac": true, "audio/flac": true, "audio/mpeg": true, "audio/mp4": true,
	"audio/m4a": true, "audio/x-m4a": true, "audio/ogg": true, "audio/wav": true,
	"audio/x-wav": true, "audio/webm": true,
}

var acpTextMIMEs = map[string]bool{
	"application/json": true, "application/ld+json": true, "application/javascript": true,
	"application/typescript": true, "application/xml": true, "application/yaml": true,
	"application/x-yaml": true, "application/x-sh": true,
}

var acpTextExtensions = map[string]bool{
	".txt": true, ".md": true, ".markdown": true, ".json": true, ".jsonl": true,
	".csv": true, ".tsv": true, ".log": true, ".xml": true, ".yaml": true, ".yml": true,
	".js": true, ".ts": true, ".tsx": true, ".py": true, ".rb": true, ".go": true,
	".rs": true, ".java": true, ".c": true, ".h": true, ".cpp": true, ".cs": true,
	".css": true, ".html": true, ".sql": true, ".sh": true, ".toml": true, ".ini": true,
}

// acpAudioFormatMIMEs maps OpenAI input_audio formats to MIME types.
var acpAudioFormatMIMEs = map[string]string{
	"mp3": "audio/mpeg", "mp4": "audio/mp4", "m4a": "audio/m4a", "aac": "audio/aac",
	"flac": "audio/flac", "ogg": "audio/ogg", "wav": "audio/wav", "webm": "audio/webm",
}

// decodeB64 decodes bare base64, tolerating whitespace folding.
func decodeB64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}
		return r
	}, s))
}

// parseACPDataURL splits a data:<mime>;base64,... URL. Only base64 payloads
// are accepted; remote URLs must be fetched by the caller first.
func parseACPDataURL(raw string) (mime string, data []byte, err error) {
	if !strings.HasPrefix(raw, "data:") {
		return "", nil, fmt.Errorf("not a data URL: send files as base64 data URLs")
	}
	head, b64, ok := strings.Cut(raw[len("data:"):], ",")
	if !ok || strings.TrimSpace(b64) == "" {
		return "", nil, fmt.Errorf("malformed data URL")
	}
	mime = strings.ToLower(strings.TrimSpace(strings.Split(head, ";")[0]))
	if mime == "image/jpg" {
		mime = "image/jpeg"
	}
	data, err = decodeB64(b64)
	if err != nil {
		return "", nil, fmt.Errorf("invalid base64 payload: %v", err)
	}
	return mime, data, nil
}

// acpAttachmentBuilder accumulates text and attachment blocks for one turn,
// staging PDFs/text files on disk so resource links stay real file URIs.
type acpAttachmentBuilder struct {
	text   strings.Builder
	blocks []acp.PromptBlock
	total  int
	staged string
}

func (b *acpAttachmentBuilder) fail(format string, args ...any) error {
	return statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf(format, args...)}
}

func (b *acpAttachmentBuilder) charge(name string, n int) error {
	b.total += n
	if b.total > acpMaxTotalBytes {
		return b.fail("Attachment '%s' is too large. Antigravity accepts text files up to 1 MiB, images up to 10 MiB, audio up to 20 MiB, and 50 MiB total attachments.", name)
	}
	return nil
}

func (b *acpAttachmentBuilder) stage(name string, data []byte) (string, error) {
	if b.staged == "" {
		dir, err := os.MkdirTemp("", "acp-attach-*")
		if err != nil {
			return "", b.fail("ACP attachment staging failed: %v", err)
		}
		b.staged = dir
	}
	safe := filepath.Base(name)
	if safe == "" || safe == "." || safe == "/" {
		safe = "attachment"
	}
	path := filepath.Join(b.staged, safe)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", b.fail("ACP attachment staging failed: %v", err)
	}
	return "file://" + path, nil
}

func (b *acpAttachmentBuilder) addImage(name, mime string, data []byte) error {
	if !acpImageMIMEs[mime] {
		return b.fail("Antigravity does not support '%s' (%s). Attach a BMP, JPEG, PNG, WebP, PDF, audio, or text file.", name, mime)
	}
	if len(data) > acpMaxImageBytes {
		return b.fail("Attachment '%s' is too large. Antigravity accepts text files up to 1 MiB, images up to 10 MiB, audio up to 20 MiB, and 50 MiB total attachments.", name)
	}
	if err := b.charge(name, len(data)); err != nil {
		return err
	}
	b.blocks = append(b.blocks, acp.PromptBlock{Type: "image", Data: base64.StdEncoding.EncodeToString(data), MimeType: mime})
	return nil
}

func (b *acpAttachmentBuilder) addAudio(name, mime string, data []byte) error {
	if !acpAudioMIMEs[mime] {
		return b.fail("Antigravity does not support '%s' (%s). Attach a BMP, JPEG, PNG, WebP, PDF, audio, or text file.", name, mime)
	}
	if len(data) > acpMaxAudioBytes {
		return b.fail("Attachment '%s' is too large. Antigravity accepts text files up to 1 MiB, images up to 10 MiB, audio up to 20 MiB, and 50 MiB total attachments.", name)
	}
	if err := b.charge(name, len(data)); err != nil {
		return err
	}
	b.blocks = append(b.blocks, acp.PromptBlock{Type: "audio", Data: base64.StdEncoding.EncodeToString(data), MimeType: mime})
	return nil
}

func (b *acpAttachmentBuilder) addFile(name, mime string, data []byte) error {
	if mime == "application/pdf" {
		if len(data) > acpMaxTotalBytes {
			return b.fail("Attachment '%s' is too large. Antigravity accepts text files up to 1 MiB, images up to 10 MiB, audio up to 20 MiB, and 50 MiB total attachments.", name)
		}
		if err := b.charge(name, len(data)); err != nil {
			return err
		}
		uri, err := b.stage(name, data)
		if err != nil {
			return err
		}
		b.blocks = append(b.blocks, acp.PromptBlock{Type: "resource_link", URI: uri, Name: name, MimeType: mime})
		return nil
	}
	if acpAudioMIMEs[mime] {
		return b.addAudio(name, mime, data)
	}
	textOK := strings.HasPrefix(mime, "text/") || acpTextMIMEs[mime] ||
		acpTextExtensions[strings.ToLower(filepath.Ext(name))]
	if !textOK {
		return b.fail("Antigravity does not support '%s' (%s). Attach a BMP, JPEG, PNG, WebP, PDF, audio, or text file.", name, mime)
	}
	if len(data) > acpMaxTextBytes {
		return b.fail("Attachment '%s' is too large. Antigravity accepts text files up to 1 MiB, images up to 10 MiB, audio up to 20 MiB, and 50 MiB total attachments.", name)
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) != -1 {
		return b.fail("Attachment '%s' is not a UTF-8 text file.", name)
	}
	if err := b.charge(name, len(data)); err != nil {
		return err
	}
	uri, err := b.stage(name, data)
	if err != nil {
		return err
	}
	resource, err := json.Marshal(map[string]string{"uri": uri, "mimeType": mime, "text": string(data)})
	if err != nil {
		return b.fail("ACP attachment encoding failed: %v", err)
	}
	b.blocks = append(b.blocks, acp.PromptBlock{Type: "resource", Resource: resource})
	return nil
}

// addInlineData routes bare inline bytes (Gemini inlineData) by MIME prefix;
// PDFs and text fall through to addFile validation, anything else is rejected.
func (b *acpAttachmentBuilder) addInlineData(name, mime string, data []byte) error {
	mime = strings.ToLower(strings.TrimSpace(mime))
	switch {
	case strings.HasPrefix(mime, "image/"):
		return b.addImage(name, mime, data)
	case strings.HasPrefix(mime, "audio/"):
		return b.addAudio(name, mime, data)
	default:
		return b.addFile(name, mime, data)
	}
}

// handlePart converts one OpenAI content part (chat or Responses shape) to
// text or attachment blocks. Unknown part types are ignored so provider
// extensions (reasoning blocks, tool calls) pass through untouched.
func (b *acpAttachmentBuilder) handlePart(part gjson.Result) error {
	switch part.Get("type").String() {
	case "text", "input_text":
		b.text.WriteString(part.Get("text").String())
		return nil
	case "image_url", "input_image":
		raw := part.Get("image_url.url")
		if !raw.Exists() {
			raw = part.Get("image_url")
		}
		mime, data, err := parseACPDataURL(raw.String())
		if err != nil {
			return b.fail("Image attachment: %v", err)
		}
		return b.addImage("image", mime, data)
	case "image":
		// Claude shape: {type:"image", source:{type:"base64", media_type, data}}.
		// URL/file sources are ignored; callers must inline base64 first.
		src := part.Get("source")
		if src.Get("type").String() != "base64" {
			return nil
		}
		mime := strings.ToLower(strings.TrimSpace(src.Get("media_type").String()))
		if mime == "image/jpg" {
			mime = "image/jpeg"
		}
		data, err := decodeB64(src.Get("data").String())
		if err != nil {
			return b.fail("Invalid base64 image payload: %v", err)
		}
		return b.addImage("image", mime, data)
	case "input_audio":
		format := strings.ToLower(strings.TrimSpace(part.Get("input_audio.format").String()))
		mime, ok := acpAudioFormatMIMEs[format]
		if !ok {
			return b.fail("Antigravity does not support audio format '%s'. Attach a BMP, JPEG, PNG, WebP, PDF, audio, or text file.", format)
		}
		data, err := decodeB64(part.Get("input_audio.data").String())
		if err != nil {
			return b.fail("Invalid base64 audio payload: %v", err)
		}
		return b.addAudio("audio."+format, mime, data)
	case "file", "input_file":
		raw := part.Get("file.file_data")
		if !raw.Exists() {
			raw = part.Get("file_data")
		}
		name := part.Get("file.filename").String()
		if name == "" {
			name = part.Get("filename").String()
		}
		if name == "" {
			name = "attachment"
		}
		mime, data, err := parseACPDataURL(raw.String())
		if err != nil {
			return b.fail("File attachment '%s': %v", name, err)
		}
		return b.addFile(name, mime, data)
	}
	return nil
}

// buildPromptBlocks converts the request payload to ACP prompt blocks and
// returns a cleanup func for staged attachments (always non-nil; call it
// after the prompt turn completes). Text merges into one leading block in
// the historical [ROLE] shape; attachments follow in encounter order.
func buildPromptBlocks(payload []byte) (blocks []acp.PromptBlock, cleanup func(), err error) {
	b := &acpAttachmentBuilder{}
	cleanup = func() {
		if b.staged != "" {
			_ = os.RemoveAll(b.staged)
		}
	}
	handleMessage := func(role string, content gjson.Result) error {
		if content.Type == gjson.String {
			if t := strings.TrimSpace(content.String()); t != "" {
				b.text.WriteString(fmt.Sprintf("[%s]: %s\n\n", strings.ToUpper(role), t))
			}
			return nil
		}
		if !content.IsArray() {
			return nil
		}
		var msgText strings.Builder
		for _, part := range content.Array() {
			if part.Get("type").String() == "text" || part.Get("type").String() == "input_text" {
				msgText.WriteString(part.Get("text").String())
				continue
			}
			if err := b.handlePart(part); err != nil {
				return err
			}
		}
		if t := strings.TrimSpace(msgText.String()); t != "" {
			b.text.WriteString(fmt.Sprintf("[%s]: %s\n\n", strings.ToUpper(role), t))
		}
		return nil
	}
	msgs := gjson.GetBytes(payload, "messages")
	if msgs.Exists() && msgs.IsArray() {
		for _, m := range msgs.Array() {
			if err := handleMessage(m.Get("role").String(), m.Get("content")); err != nil {
				cleanup()
				return nil, cleanup, err
			}
		}
	} else if input := gjson.GetBytes(payload, "input"); input.Exists() && input.IsArray() {
		for _, item := range input.Array() {
			switch item.Get("type").String() {
			case "message":
				if err := handleMessage(item.Get("role").String(), item.Get("content")); err != nil {
					cleanup()
					return nil, cleanup, err
				}
			default:
				if err := b.handlePart(item); err != nil {
					cleanup()
					return nil, cleanup, err
				}
			}
		}
	} else if contents := gjson.GetBytes(payload, "contents"); contents.Exists() && contents.IsArray() {
		for _, turn := range contents.Array() {
			role := turn.Get("role").String()
			if role == "model" {
				role = "assistant"
			}
			if role == "" {
				role = "user"
			}
			for _, gp := range turn.Get("parts").Array() {
				if t := gp.Get("text"); t.Exists() {
					if s := strings.TrimSpace(t.String()); s != "" {
						b.text.WriteString(fmt.Sprintf("[%s]: %s\n\n", strings.ToUpper(role), s))
					}
					continue
				}
				if inline := gp.Get("inlineData"); inline.Exists() {
					data, err := decodeB64(inline.Get("data").String())
					if err != nil {
						cleanup()
						return nil, cleanup, b.fail("Invalid base64 inline payload: %v", err)
					}
					if err := b.addInlineData("attachment", inline.Get("mimeType").String(), data); err != nil {
						cleanup()
						return nil, cleanup, err
					}
					continue
				}
				if fd := gp.Get("fileData"); fd.Exists() {
					cleanup()
					return nil, cleanup, b.fail("Gemini fileData URIs are not readable here: inline the file bytes instead")
				}
			}
		}
	}
	if t := strings.TrimSpace(b.text.String()); t != "" {
		blocks = append(blocks, acp.PromptBlock{Type: "text", Text: t})
	}
	blocks = append(blocks, b.blocks...)
	if len(blocks) > 0 {
		return blocks, cleanup, nil
	}
	// Compatibility fallbacks for prompt-shaped payloads.
	if prompt := gjson.GetBytes(payload, "prompt"); prompt.Exists() && prompt.String() != "" {
		return acp.NewTextPrompt(prompt.String()), cleanup, nil
	}
	return acp.NewTextPrompt(string(payload)), cleanup, nil
}

// openSession binds a session for the request: it pops a pre-created
// fresh session from the pool worker when available (skipping the
// session/new round trip entirely), falling back to creating one now.
// The requested model variant is applied only when the session's current
// model differs, so the common warm path reaches session/prompt with zero
// ACP round trips before it. A rejected variant is a client error: the
// message carries the daemon verdict so callers can pick an offered model.
// It returns the session id, the serving mode ("prepared" or "fresh") and
// the resolved variant, and stamps the session stages on the TTFT tracker.
// The resolved variant is recorded as the worker's preferred variant so the
// next prepared session is created model-aware (P0.5).
func openSession(ctx context.Context, client *acp.Client, worker *helps.AntigravityAcpWorker, model string, payload []byte, stages *acpTTFTStage) (string, string, string, error) {
	variant := resolveAntigravityModel(model, payload)
	if variant != "" {
		worker.SetPreferredVariant(variant)
	}

	// Fast path: a prepared fresh session (never prompted, zero context).
	if ps := worker.AcquirePreparedSession(); ps != nil {
		popDone := time.Now()
		if variant == "" || variant == ps.Variant || acp.CurrentModel(client.ConfigOptions(ps.SessionID)) == variant {
			// Model-aware prepared hit: no set_config_option round trip.
			stages.markSessionSetup(popDone, popDone, "prepared")
			return ps.SessionID, "prepared", variant, nil
		}
		if _, err := client.SetConfigOption(ctx, ps.SessionID, "model", variant); err != nil {
			return "", "", "", statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("Antigravity model %q unavailable: %v", variant, err)}
		}
		stages.markSessionSetup(popDone, time.Now(), "prepared")
		return ps.SessionID, "prepared", variant, nil
	}

	// Slow path: create a session now.
	cwd, _ := os.Getwd()
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		if acp.IsSignInRequired(err) {
			return "", "", "", statusErr{code: http.StatusUnauthorized, msg: "Antigravity sign-in required: complete the Google login for this profile, then retry"}
		}
		return "", "", "", statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("ACP session/new failed: %v", err)}
	}
	// Stamp immediately after session/new itself, not after all session
	// setup (P0.2: session_new_done must mean the session/new round trip).
	newDone := time.Now()
	if variant == "" {
		stages.markSessionSetup(newDone, newDone, "fresh")
		return sessionID, "fresh", variant, nil
	}
	if acp.CurrentModel(client.ConfigOptions(sessionID)) == variant {
		// The daemon-created session already runs the requested model
		// variant; setting it again would be a wasted round trip.
		stages.markSessionSetup(newDone, newDone, "fresh")
		return sessionID, "fresh", variant, nil
	}
	if _, err := client.SetConfigOption(ctx, sessionID, "model", variant); err != nil {
		return "", "", "", statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("Antigravity model %q unavailable: %v", variant, err)}
	}
	stages.markSessionSetup(newDone, time.Now(), "fresh")
	return sessionID, "fresh", variant, nil
}

// acpTTFTStage records one stage boundary of the ACP request path. Stage
// timings distinguish proxy overhead from backend/model latency so
// optimization effort follows the dominant cost instead of guesswork.
type acpTTFTStage struct {
	requestEnter time.Time
	poolAcquired time.Time
	// sessionNewDone records completion of session/new itself (or the
	// prepared-session pop), not the whole session setup (P0.2).
	sessionNewDone time.Time
	// modelConfigDone records completion of the optional model
	// session/set_config_option round trip, separately from session/new.
	modelConfigDone time.Time
	promptBuilt     time.Time
	// promptWritten is stamped from the actual ACP client write path
	// (SetOnRequestWritten), not immediately before calling Prompt (P0.2).
	promptWritten time.Time
	// firstACPStdoutLine is wired from the ACP transport reader (P0.2).
	firstACPStdoutLine time.Time
	// firstACPUpdate is the first session/update notification of any kind
	// (thought, status, or text).
	firstACPUpdate time.Time
	// firstTextUpdate is the first MEANINGFUL text token: the first
	// non-empty agent_message_chunk. User-visible TTFT is measured to
	// this point (P0.2).
	firstTextUpdate    time.Time
	firstChunkEnqueued time.Time
	// sessionMode exposes which session path served the request:
	// fresh | prepared | stateful_reuse.
	sessionMode string

	mu sync.Mutex
}

// markFirstACPUpdate stamps the first session/update of any kind (once).
// Safe for the reader-goroutine callback.
func (s *acpTTFTStage) markFirstACPUpdate(t time.Time) {
	s.mu.Lock()
	if s.firstACPUpdate.IsZero() {
		s.firstACPUpdate = t
	}
	s.mu.Unlock()
}

// markPoolAcquired stamps worker acquisition. Main path only, but routed
// through the mutex so the snapshot reader can never race it.
func (s *acpTTFTStage) markPoolAcquired() {
	s.mu.Lock()
	s.poolAcquired = time.Now()
	s.mu.Unlock()
}

// markSessionSetup stamps the session/new completion, serving mode and
// model-config completion as one atomic step (openSession owns the
// sequence and already knows the correct boundaries).
func (s *acpTTFTStage) markSessionSetup(newDone, modelDone time.Time, mode string) {
	s.mu.Lock()
	s.sessionNewDone = newDone
	s.modelConfigDone = modelDone
	s.sessionMode = mode
	s.mu.Unlock()
}

// markSessionNewDone stamps only the session/new boundary (failure paths).
func (s *acpTTFTStage) markSessionNewDone(t time.Time) {
	s.mu.Lock()
	if s.sessionNewDone.IsZero() {
		s.sessionNewDone = t
	}
	s.mu.Unlock()
}

// markPromptBuilt stamps prompt-build completion.
func (s *acpTTFTStage) markPromptBuilt() {
	s.mu.Lock()
	s.promptBuilt = time.Now()
	s.mu.Unlock()
}

// markChunkEnqueued stamps the first downstream chunk (once).
func (s *acpTTFTStage) markChunkEnqueued() {
	s.mu.Lock()
	if s.firstChunkEnqueued.IsZero() {
		s.firstChunkEnqueued = time.Now()
	}
	s.mu.Unlock()
}

// markFirstTextUpdate stamps the first non-empty agent_message_chunk (once).
// Safe for the reader-goroutine callback.
func (s *acpTTFTStage) markFirstTextUpdate(t time.Time) {
	s.mu.Lock()
	if s.firstTextUpdate.IsZero() {
		s.firstTextUpdate = t
	}
	s.mu.Unlock()
}

// markPromptWritten stamps the actual stdin write of session/prompt. Called
// from the ACP client write path for that method only.
func (s *acpTTFTStage) markPromptWritten(t time.Time) {
	s.mu.Lock()
	if s.promptWritten.IsZero() {
		s.promptWritten = t
	}
	s.mu.Unlock()
}

// markStdoutLine stamps the first backend stdout line observed after this
// request's prompt was written, so a late response of canceled background
// work (e.g. an interrupted refill) cannot fake a TTFT stage.
func (s *acpTTFTStage) markStdoutLine(t time.Time) {
	s.mu.Lock()
	if !s.promptWritten.IsZero() && s.firstACPStdoutLine.IsZero() {
		s.firstACPStdoutLine = t
	}
	s.mu.Unlock()
}

// acpTTFTSnapshot is a locked copy of the stage timestamps. The reader
// goroutine keeps marking stages while a prompt streams, so the log dump
// must read a consistent snapshot instead of racing individual fields.
type acpTTFTSnapshot struct {
	requestEnter       time.Time
	poolAcquired       time.Time
	sessionNewDone     time.Time
	modelConfigDone    time.Time
	promptBuilt        time.Time
	promptWritten      time.Time
	firstACPStdoutLine time.Time
	firstACPUpdate     time.Time
	firstTextUpdate    time.Time
	firstChunkEnqueued time.Time
	sessionMode        string
}

// snapshot returns a consistent copy of the stage timestamps.
func (s *acpTTFTStage) snapshot() acpTTFTSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return acpTTFTSnapshot{
		requestEnter:       s.requestEnter,
		poolAcquired:       s.poolAcquired,
		sessionNewDone:     s.sessionNewDone,
		modelConfigDone:    s.modelConfigDone,
		promptBuilt:        s.promptBuilt,
		promptWritten:      s.promptWritten,
		firstACPStdoutLine: s.firstACPStdoutLine,
		firstACPUpdate:     s.firstACPUpdate,
		firstTextUpdate:    s.firstTextUpdate,
		firstChunkEnqueued: s.firstChunkEnqueued,
		sessionMode:        s.sessionMode,
	}
}

// stageElapsed returns the millisecond offset of t from the request-enter
// timestamp, or -1 when the stage was never reached.
func stageElapsed(snap acpTTFTSnapshot, t time.Time) int64 {
	if t.IsZero() || snap.requestEnter.IsZero() {
		return -1
	}
	return t.Sub(snap.requestEnter).Milliseconds()
}

// stageDelta returns the millisecond gap from t back to the first non-zero
// reference boundary in bases (falling back to requestEnter), attributing
// exactly one hop to each derived metric. Returns -1 when t was never
// reached; clamps to 0 when t predates its base (contaminated boundary).
func stageDelta(snap acpTTFTSnapshot, t time.Time, bases ...time.Time) int64 {
	if t.IsZero() || snap.requestEnter.IsZero() {
		return -1
	}
	for _, b := range bases {
		if !b.IsZero() {
			if t.Before(b) {
				return 0
			}
			return t.Sub(b).Milliseconds()
		}
	}
	return t.Sub(snap.requestEnter).Milliseconds()
}

// logStageTimings dumps the stage table. Absolute stages stay relative to
// request_enter (elapsed); the derived metrics in the PLAN follow the stage
// sequence so each number attributes exactly one hop. Stages that never
// happened stay at -1. One log line per request keeps the cost negligible
// while making proxy-vs-backend attribution possible offline.
func (s *acpTTFTStage) logStageTimings(model string, stream bool) {
	snap := s.snapshot()
	mode := snap.sessionMode
	if mode == "" {
		mode = "fresh"
	}
	log.WithFields(log.Fields{
		"provider":                          "antigravity-acp",
		"model":                             model,
		"stream":                            stream,
		"session_mode":                      mode,
		"pool_wait_ms":                      stageElapsed(snap, snap.poolAcquired),
		"session_new_ms":                    stageDelta(snap, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"model_config_ms":                   stageDelta(snap, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"prompt_build_ms":                   stageDelta(snap, snap.promptBuilt, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"prompt_write_ms":                   stageDelta(snap, snap.promptWritten, snap.promptBuilt, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"backend_to_first_output_ms":        stageDelta(snap, snap.firstACPStdoutLine, snap.promptWritten, snap.promptBuilt, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"first_output_to_first_text_ms":     stageDelta(snap, snap.firstTextUpdate, snap.firstACPStdoutLine, snap.promptWritten, snap.promptBuilt, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"first_text_to_downstream_chunk_ms": stageDelta(snap, snap.firstChunkEnqueued, snap.firstTextUpdate, snap.firstACPStdoutLine, snap.promptWritten, snap.promptBuilt, snap.modelConfigDone, snap.sessionNewDone, snap.poolAcquired, snap.requestEnter),
		"first_update_ms":                   stageElapsed(snap, snap.firstACPUpdate),
		"first_token_ttft_ms":               stageElapsed(snap, snap.firstTextUpdate),
	}).Info("ACP TTFT stage timings")
}

// Execute performs a non-streaming ACP prompt turn.
func (e *AntigravityAcpExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if opts.Alt == "responses/compact" {
		return resp, statusErr{code: http.StatusNotImplemented, msg: "/responses/compact not supported"}
	}

	stages := &acpTTFTStage{requestEnter: time.Now()}

	var client *acp.Client
	var worker *helps.AntigravityAcpWorker

	if e.pool != nil {
		key := e.authPoolKey(auth)
		w, acqErr := e.pool.Acquire(ctx, key, func(spawnCtx context.Context) (*acp.Client, error) {
			return e.spawnClient(spawnCtx, auth)
		})
		if acqErr != nil {
			return resp, acqErr
		}
		worker = w
		client = w.Client()
	} else {
		c, spawnErr := e.spawnClient(ctx, auth)
		if spawnErr != nil {
			return resp, spawnErr
		}
		client = c
		defer client.Close()
	}
	stages.markPoolAcquired()

	healthy := true
	defer func() {
		if worker != nil {
			e.pool.Release(worker, healthy)
		}
	}()

	// Prompt construction runs concurrently with session setup: the request
	// payload is already fully available, so pre-prompt latency approaches
	// max(session setup, prompt build) instead of their sum. This matters
	// for large histories and attachment staging.
	type promptResult struct {
		blocks  []acp.PromptBlock
		cleanup func()
		err     error
	}
	promptCh := make(chan promptResult, 1)
	go func() {
		blocks, cleanupAttachments, buildErr := buildPromptBlocks(req.Payload)
		promptCh <- promptResult{blocks, cleanupAttachments, buildErr}
	}()

	sessionID, _, _, err := openSession(ctx, client, worker, req.Model, req.Payload, stages)
	if err != nil {
		if acp.IsTransportError(err) {
			healthy = false
		}
		// P0.3: the prompt build may still be running in parallel; drain
		// its result and always clean staged attachments when the session
		// path fails, without giving up the session/prompt parallelism.
		if pr := <-promptCh; pr.cleanup != nil {
			pr.cleanup()
		}
		return resp, err
	}

	prompt := <-promptCh
	if prompt.err != nil {
		if prompt.cleanup != nil {
			prompt.cleanup()
		}
		return resp, prompt.err
	}
	defer prompt.cleanup()
	stages.markPromptBuilt()

	var responseText strings.Builder
	var thoughtText strings.Builder
	var mu sync.Mutex

	client.SetOnRequestWritten(func(method string) {
		if method == "session/prompt" {
			stages.markPromptWritten(time.Now())
		}
	})
	client.SetOnFirstLine(func() {
		stages.markStdoutLine(time.Now())
	})
	client.OnUpdate(func(u acp.SessionUpdate) {
		if u.SessionID != sessionID {
			return
		}
		now := time.Now()
		stages.markFirstACPUpdate(now)
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
				stages.markFirstTextUpdate(now)
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

	promptDone := make(chan struct{})
	defer close(promptDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = client.Cancel(sessionID)
		case <-promptDone:
		}
	}()

	stopReason, err := client.Prompt(ctx, sessionID, prompt.blocks)
	if err != nil {
		if acp.IsTransportError(err) {
			healthy = false
		}
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
	if cliproxyexecutor.ResponseFormatOrSource(opts) == sdktranslator.FormatOpenAIResponse {
		var param any
		rawResp = sdktranslator.TranslateNonStream(ctx, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse, req.Model, opts.OriginalRequest, req.Payload, rawResp, &param)
		rawResp = helps.EnsureResponsesUsageDetails(rawResp)
	}

	stages.markChunkEnqueued()
	stages.logStageTimings(req.Model, false)
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

	stages := &acpTTFTStage{requestEnter: time.Now()}

	var client *acp.Client
	var worker *helps.AntigravityAcpWorker

	if e.pool != nil {
		key := e.authPoolKey(auth)
		w, acqErr := e.pool.Acquire(ctx, key, func(spawnCtx context.Context) (*acp.Client, error) {
			return e.spawnClient(spawnCtx, auth)
		})
		if acqErr != nil {
			return nil, acqErr
		}
		worker = w
		client = w.Client()
	} else {
		c, spawnErr := e.spawnClient(ctx, auth)
		if spawnErr != nil {
			return nil, spawnErr
		}
		client = c
	}
	stages.markPoolAcquired()

	// Prompt construction runs concurrently with session setup so the
	// pre-prompt latency approaches max(session setup, prompt build).
	type promptResult struct {
		blocks  []acp.PromptBlock
		cleanup func()
		err     error
	}
	promptCh := make(chan promptResult, 1)
	go func() {
		blocks, cleanupAttachments, buildErr := buildPromptBlocks(req.Payload)
		promptCh <- promptResult{blocks, cleanupAttachments, buildErr}
	}()

	sessionID, _, _, err := openSession(ctx, client, worker, req.Model, req.Payload, stages)
	if err != nil {
		if worker != nil {
			e.pool.Release(worker, !acp.IsTransportError(err))
		} else {
			client.Close()
		}
		// P0.3: the parallel prompt build may still be running; drain its
		// result and always clean staged attachments on the session-failure
		// path, without giving up the parallelism.
		if pr := <-promptCh; pr.cleanup != nil {
			pr.cleanup()
		}
		return nil, err
	}

	chunkChan := make(chan cliproxyexecutor.StreamChunk, 128)
	result := &cliproxyexecutor.StreamResult{
		Headers: make(http.Header),
		Chunks:  chunkChan,
	}
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	var streamParam any
	firstChunkOnce := sync.Once{}
	emitChunk := func(payload []byte) {
		firstChunkOnce.Do(func() { stages.markChunkEnqueued() })
		if responseFormat != sdktranslator.FormatOpenAIResponse {
			chunkChan <- cliproxyexecutor.StreamChunk{Payload: payload}
			return
		}
		for _, c := range sdktranslator.TranslateStream(ctx, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse, req.Model, opts.OriginalRequest, req.Payload, payload, &streamParam) {
			chunkChan <- cliproxyexecutor.StreamChunk{Payload: helps.EnsureResponsesUsageDetails(c)}
		}
	}

	go func() {
		healthy := true
		defer func() {
			if worker != nil {
				e.pool.Release(worker, healthy)
			} else {
				client.Close()
			}
			close(chunkChan)
			stages.logStageTimings(req.Model, true)
		}()

		prompt := <-promptCh
		if prompt.err != nil {
			if prompt.cleanup != nil {
				prompt.cleanup()
			}
			chunkChan <- cliproxyexecutor.StreamChunk{
				Err: prompt.err,
			}
			return
		}
		defer prompt.cleanup()
		stages.markPromptBuilt()

		client.SetOnRequestWritten(func(method string) {
			if method == "session/prompt" {
				stages.markPromptWritten(time.Now())
			}
		})
		client.SetOnFirstLine(func() {
			stages.markStdoutLine(time.Now())
		})
		client.OnUpdate(func(u acp.SessionUpdate) {
			if u.SessionID != sessionID {
				return
			}
			now := time.Now()
			stages.markFirstACPUpdate(now)

			switch u.Kind {
			case "agent_message_chunk":
				var chunk struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
					stages.markFirstTextUpdate(now)
					rawJSON := fmt.Sprintf("{\"choices\":[{\"delta\":{\"content\":%s}}]}", string(mustMarshal(chunk.Content.Text)))
					emitChunk([]byte(rawJSON))
				}
			case "agent_thought_chunk":
				var chunk struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(u.Raw, &chunk); err == nil && chunk.Content.Text != "" {
					rawJSON := fmt.Sprintf("{\"choices\":[{\"delta\":{\"reasoning_content\":%s}}]}", string(mustMarshal(chunk.Content.Text)))
					emitChunk([]byte(rawJSON))
				}
			}
		})

		promptDone := make(chan struct{})
		defer close(promptDone)
		go func() {
			select {
			case <-ctx.Done():
				_ = client.Cancel(sessionID)
			case <-promptDone:
			}
		}()

		_, promptErr := client.Prompt(ctx, sessionID, prompt.blocks)
		if promptErr != nil {
			if acp.IsTransportError(promptErr) {
				healthy = false
			}
			log.Errorf("ACP prompt stream error: %v", promptErr)
			chunkChan <- cliproxyexecutor.StreamChunk{
				Err: promptErr,
			}
			return
		}

		emitChunk([]byte("data: [DONE]\n\n"))
	}()

	return result, nil
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
