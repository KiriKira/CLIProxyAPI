package executor

import (
	"strconv"
	"strings"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func documentOpts(client string) cliproxyexecutor.Options {
	return cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.ACPDocumentScopeMetadataKey:  true,
		cliproxyexecutor.ACPDocumentReuseMetadataKey:  true,
		cliproxyexecutor.ACPDocumentClientMetadataKey: client,
	}}
}

func TestDocumentReuseFiftySequentialRequestsUseOneSession(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	opts := documentOpts("immersive-translate")
	for i := 0; i < 50; i++ {
		payload := `{"messages":[{"role":"system","content":"You translate."},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"Example video - YouTube\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nsequential batch ` + strconv.Itoa(i) + `"}]}`
		statefulRequest(t, execer, auth, opts, payload)
	}
	methods := readStatefulLog(t, script, "methods.log")
	if got := strings.Count(methods, "session/new"); got != 1 {
		t.Fatalf("50 sequential document requests created %d sessions, want 1\nlog: %s", got, methods)
	}
}

func TestDocumentReuseSameTitleSharesSessionAndSendsOnlyNewestBatch(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	marker := func(title, batch string) string {
		return `{"messages":[{"role":"system","content":"You translate."},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"` + title + `\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\n` + batch + `"}]}`
	}

	statefulRequest(t, execer, auth, documentOpts("immersive-translate"), marker("(132) Demo - YouTube", "batch ONE"))
	statefulRequest(t, execer, auth, documentOpts("immersive-translate"), marker("(133) Demo - YouTube", "batch TWO SECRET"))

	methods := readStatefulLog(t, script, "methods.log")
	if got := strings.Count(methods, "session/new"); got != 1 {
		t.Fatalf("same document should reuse one ACP session, session/new=%d\nlog: %s", got, methods)
	}
	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected two prompts, got %d: %v", len(prompts), prompts)
	}
	if strings.Contains(prompts[1], "batch ONE") || strings.Contains(prompts[1], documentTitlePromptStart) || !strings.Contains(prompts[1], "batch TWO SECRET") {
		t.Fatalf("document hit did not send only newest cleaned batch: %q", prompts[1])
	}
}
