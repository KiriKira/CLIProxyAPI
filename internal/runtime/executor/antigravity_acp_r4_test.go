package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// documentPayload builds one Immersive-Translate-shaped request body with
// the wrapped title marker and a single user batch.
func documentPayload(title, batch string) string {
	return `{"messages":[{"role":"system","content":"You translate."},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"` + title + `\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\n` + batch + `"}]}`
}

// TestR4_DocumentLateInvalidationBootstrapsFullHistory covers the R4
// transition: the binding dies after the request starts but before the
// prompt payload decision would previously have been re-made. With the fix
// the miss is decided inside acquisition, so the bootstrap prompt contains
// the FULL request context and no stale incremental body can leak into a
// fresh session (non-stream path).
func TestR4_DocumentLateInvalidationBootstrapsFullHistory(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	opts := documentOpts("immersive-translate")

	// Turn 0: bootstrap creates the binding.
	statefulRequest(t, execer, auth, opts, documentPayload("R4 video - YouTube", "batch zero"))

	// Simulate the late invalidation racing the request: the binding is
	// gone by the time the next request validates. The pre-fix code path
	// built the incremental payload first and switched to bootstrap after
	// the builder started; the fixed path decides inside acquisition.
	execer.document.InvalidateAllForTest()

	statefulRequest(t, execer, auth, opts, documentPayload("R4 video - YouTube", "batch one"))

	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompts, got %d: %v", len(prompts), prompts)
	}
	// The bootstrap must replay the full context (system + title + batch).
	if !strings.Contains(prompts[1], "You translate") || !strings.Contains(prompts[1], "batch one") {
		t.Fatalf("post-invalidation bootstrap lost full context: %q", prompts[1])
	}
	if !strings.Contains(prompts[1], "R4 video - YouTube") {
		t.Fatalf("post-invalidation bootstrap lost the title context: %q", prompts[1])
	}
	if strings.Contains(prompts[1], documentTitlePromptStart) {
		t.Fatalf("bootstrap leaked marker delimiters: %q", prompts[1])
	}
	// The first prompt was a bootstrap too; the second must contain the
	// newest batch, not the original one.
	if strings.Contains(prompts[1], "batch zero") {
		t.Fatalf("bootstrap replayed the previous batch: %q", prompts[1])
	}
}

// TestR4_StreamAndNonStreamAgreeOnLateInvalidation runs the same
// invalidation transition through ExecuteStream and asserts the stream
// bootstrap replays the full context exactly like the non-stream path.
func TestR4_StreamAndNonStreamAgreeOnLateInvalidation(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}
	opts := documentOpts("immersive-translate")
	payload := documentPayload("R4 stream - YouTube", "stream batch one")

	// First request bootstraps and binds (non-stream).
	statefulRequest(t, execer, auth, opts, payload)

	// Invalidate, then stream: the stream path must ALSO bootstrap with the
	// full context rather than sending an incremental-only payload.
	execer.document.InvalidateAllForTest()
	req := cliproxyexecutor.Request{Model: "gemini-3.8-flash", Payload: []byte(payload)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := execer.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
	}

	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompts, got %d: %v", len(prompts), prompts)
	}
	if !strings.Contains(prompts[1], "You translate") || !strings.Contains(prompts[1], "stream batch one") || !strings.Contains(prompts[1], "R4 stream - YouTube") {
		t.Fatalf("stream bootstrap after invalidation lost full context: %q", prompts[1])
	}
}

// TestR4_StrictLateInvalidationBootstrapsFullHistory covers the analogous
// strict-stateful transition: a purged binding (forced retirement, R1) must
// make the next turn bootstrap from full history, never send the
// incremental turn into a fresh session.
func TestR4_StrictLateInvalidationBootstrapsFullHistory(t *testing.T) {
	script := statefulLogAgent(t)
	execer := NewAntigravityAcpExecutor(&internalconfig.Config{Antigravity: internalconfig.AntigravityConfig{PersistentProcess: boolPtr(true)}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"binary_path": script,
		"gemini_home": t.TempDir(),
	}}

	h1 := `{"messages":[{"role":"system","content":"You translate."},{"role":"user","content":"first turn"}]}`
	statefulRequest(t, execer, auth, statefulOpts("r4-strict", 0), h1)

	// Late invalidation (e.g. R1 forced retirement purged the table).
	execer.stateful.InvalidateAllForTest()

	h2 := `{"messages":[{"role":"system","content":"You translate."},{"role":"user","content":"first turn"},{"role":"assistant","content":"ok"},{"role":"user","content":"second turn after purge"}]}`
	statefulRequest(t, execer, auth, statefulOpts("r4-strict", 1), h2)

	prompts := promptTexts(t, script)
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompts, got %d: %v", len(prompts), prompts)
	}
	// The post-purge turn must bootstrap with FULL history, not the
	// incremental "second turn" alone.
	if !strings.Contains(prompts[1], "You translate") || !strings.Contains(prompts[1], "first turn") || !strings.Contains(prompts[1], "second turn after purge") {
		t.Fatalf("strict post-purge turn lost full-history bootstrap: %q", prompts[1])
	}
}
