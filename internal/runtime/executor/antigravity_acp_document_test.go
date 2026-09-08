package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestPrepareDocumentRequestNormalizesTitleAndStripsMarker(t *testing.T) {
	payload := []byte(`{"model":"gemini-3.7-flash-low","messages":[{"role":"system","content":"translate"},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"(132) Demo - YouTube\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nBatch one"},{"role":"assistant","content":"old"},{"role":"user","content":"Batch two"}]}`)
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	info, err := prepareDocumentRequest(payload, "auth", "gemini-3.7-flash-low", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(info.cleanedPayload), "Title: \\\"(132) Demo - YouTube\\\"") {
		t.Fatalf("cleaned payload lost title context: %s", info.cleanedPayload)
	}
	if strings.Contains(string(info.cleanedPayload), documentTitlePromptStart) || strings.Contains(string(info.cleanedPayload), documentTitlePromptEnd) {
		t.Fatalf("marker delimiters leaked into model payload: %s", info.cleanedPayload)
	}
	var incremental map[string]any
	if err := json.Unmarshal(info.incrementalBody, &incremental); err != nil {
		t.Fatal(err)
	}
	messages := incremental["messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["content"] != "Batch two" {
		t.Fatalf("incremental payload was not newest user batch: %s", info.incrementalBody)
	}

	other := []byte(`{"model":"gemini-3.7-flash-low","messages":[{"role":"system","content":"translate"},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"(133) Demo - YouTube\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nAnother batch"}]}`)
	otherInfo, err := prepareDocumentRequest(other, "auth", "gemini-3.7-flash-low", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if info.key != otherInfo.key {
		t.Fatalf("notification count changed document key: %q != %q", info.key, otherInfo.key)
	}
}

func TestDocumentRequestSemanticAndExplicitIdentityIsolation(t *testing.T) {
	base := []byte(`{"messages":[{"role":"system","content":"translate to English"},{"role":"user","content":"Document Metadata:\nTitle: \"Demo - YouTube\"\nBatch"}]}`)
	signals := documentSignals{enabled: true, client: "client-a"}
	one, err := prepareDocumentRequest(base, "auth", "model-a", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	changedSystem := []byte(`{"messages":[{"role":"system","content":"translate to Japanese"},{"role":"user","content":"Document Metadata:\nTitle: \"Demo - YouTube\"\nBatch"}]}`)
	two, err := prepareDocumentRequest(changedSystem, "auth", "model-a", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if one.key == two.key {
		t.Fatal("semantic system prompt change reused the document key")
	}

	explicit := signals
	explicit.documentID = "youtube:abc123"
	three, err := prepareDocumentRequest([]byte(`{"messages":[{"role":"user","content":"no title marker"}]}`), "auth", "model-a", explicit, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(three.key, "document:") {
		t.Fatalf("explicit document id did not produce a document key: %s", three.key)
	}
}

func TestDocumentSemanticFingerprintStripsVolatileSystemTitle(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	build := func(counter, instruction string) []byte {
		return []byte(`{"messages":[{"role":"system","content":"` + instruction + `\n[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"(` + counter + `) Demo - YouTube\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nsummary/terms"},{"role":"user","content":"batch"}]}`)
	}
	one, err := prepareDocumentRequest(build("132", "translate to English"), "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	two, err := prepareDocumentRequest(build("133", "translate to English"), "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if one.key != two.key {
		t.Fatalf("volatile system title changed document key: %q != %q", one.key, two.key)
	}
	three, err := prepareDocumentRequest(build("133", "translate to Japanese"), "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if two.key == three.key {
		t.Fatal("stable translation instruction did not rotate document key")
	}
}

func TestDocumentDirectTitleMarkerIsSupported(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	payload := []byte(`{"messages":[{"role":"system","content":"translate"},{"role":"user","content":"[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]\nDemo - YouTube\n[[/CLIPROXY_ACP_DOCUMENT_TITLE]]\nbatch"}]}`)
	info, err := prepareDocumentRequest(payload, "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if info.title != "Demo - YouTube" {
		t.Fatalf("direct title marker = %q, want Demo - YouTube", info.title)
	}
	if strings.Contains(string(info.cleanedPayload), documentDocumentTitleStart) || strings.Contains(string(info.cleanedPayload), documentDocumentTitleEnd) {
		t.Fatalf("direct marker delimiters leaked into cleaned payload: %s", info.cleanedPayload)
	}
}

// TestDocumentDirectTitleMarkerIsMachineOnly covers the R6 blocker: the
// direct CLIPROXY_ACP_DOCUMENT_TITLE block is machine-only routing identity
// and must disappear from the model payload in full (delimiters AND title
// body), while ordinary model-visible title context and the wrapped
// CLIPROXY_ACP_TITLE_PROMPT content survive cleaning.
func TestDocumentDirectTitleMarkerIsMachineOnly(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	payload := []byte(`{"messages":[{"role":"system","content":"stable translation instructions\n[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]\nExample Video - YouTube\n[[/CLIPROXY_ACP_DOCUMENT_TITLE]]\nDocument Metadata:\nTitle: \"Example Video - YouTube\""},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"Example Video - YouTube\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nbatch"}]}`)
	info, err := prepareDocumentRequest(payload, "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if info.title != "Example Video - YouTube" {
		t.Fatalf("routing title = %q, want Example Video - YouTube", info.title)
	}
	cleaned := string(info.cleanedPayload)
	if strings.Contains(cleaned, documentDocumentTitleStart) || strings.Contains(cleaned, documentDocumentTitleEnd) {
		t.Fatalf("direct marker delimiters survived cleaning: %s", cleaned)
	}
	if strings.Contains(cleaned, "Example Video - YouTube\n\nDocument Metadata") {
		t.Fatalf("direct marker body survived as a standalone title line: %s", cleaned)
	}
	if strings.Count(cleaned, "Example Video - YouTube") != 2 {
		t.Fatalf("model-visible title context lost or duplicated (want exactly Document Metadata + wrapped title_prompt occurrences): %s", cleaned)
	}
	if !strings.Contains(cleaned, "Document Metadata") || !strings.Contains(cleaned, "Title: \\\"Example Video - YouTube\\\"") {
		t.Fatalf("ordinary Document Metadata title context must stay model-visible: %s", cleaned)
	}
	if strings.Contains(cleaned, documentTitlePromptStart) || strings.Contains(cleaned, documentTitlePromptEnd) {
		t.Fatalf("wrapped marker delimiters survived cleaning: %s", cleaned)
	}
	if !strings.Contains(cleaned, "Title: \\\"Example Video - YouTube\\\"\\n[[") && strings.Count(cleaned, "Title: \\\"Example Video - YouTube\\\"") < 2 {
		t.Fatalf("wrapped CLIPROXY_ACP_TITLE_PROMPT inner context must stay model-visible: %s", cleaned)
	}
}

// TestDocumentDirectMarkerBodyExcludedFromSemanticFingerprint proves the
// direct machine block cannot rotate the document key via the semantic
// projection: two payloads differing only inside the direct marker (same
// normalized YouTube identity) must produce the same key.
func TestDocumentDirectMarkerBodyExcludedFromSemanticFingerprint(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	build := func(counter string) []byte {
		return []byte(`{"messages":[{"role":"system","content":"translate\n[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]\n(` + counter + `) Demo - YouTube\n[[/CLIPROXY_ACP_DOCUMENT_TITLE]]"},{"role":"user","content":"batch"}]}`)
	}
	one, err := prepareDocumentRequest(build("132"), "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	two, err := prepareDocumentRequest(build("133"), "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if one.key != two.key {
		t.Fatalf("direct marker body leaked into the semantic fingerprint: %q != %q", one.key, two.key)
	}
}

// TestDocumentDirectMarkerUnterminatedBlock verifies an unterminated direct
// marker (start without end) still loses its delimiter token and falls back
// to ordinary title parsing instead of keeping the body as identity.
func TestDocumentDirectMarkerUnterminatedBlock(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	payload := []byte(`{"messages":[{"role":"system","content":"translate\n[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]\nUnfinished - YouTube\nDocument Metadata:\nTitle: \"Real - YouTube\""},{"role":"user","content":"batch"}]}`)
	info, err := prepareDocumentRequest(payload, "auth", "model", signals, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if info.title != "Real - YouTube" {
		t.Fatalf("unterminated marker should fall back to metadata title, got %q", info.title)
	}
	if strings.Contains(string(info.cleanedPayload), documentDocumentTitleStart) {
		t.Fatalf("unterminated direct marker delimiter survived cleaning: %s", info.cleanedPayload)
	}
}

func TestDocumentLaneOverflowMapsToRetryable429(t *testing.T) {
	err := documentLaneBackpressureError(helps.ErrDocumentLaneBusy)
	status, ok := err.(requestScopedStatusErr)
	if !ok {
		t.Fatalf("lane overflow error type = %T, want requestScopedStatusErr", err)
	}
	if status.code != http.StatusTooManyRequests {
		t.Fatalf("lane overflow status = %d, want %d", status.code, http.StatusTooManyRequests)
	}
	if status.retryAfter == nil || *status.retryAfter != time.Second {
		t.Fatalf("lane overflow retry-after = %v, want 1s", status.retryAfter)
	}
	if !status.IsRequestScoped() {
		t.Fatal("lane overflow must be request-scoped")
	}
	if got := documentLaneBackpressureError(context.Canceled); got != context.Canceled {
		t.Fatalf("non-lane error was remapped: %v", got)
	}
}

func TestDocumentSignalsRequireExplicitScopeAndReuse(t *testing.T) {
	if got := documentSignalsFromOptions(cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.ACPDocumentScopeMetadataKey: true,
	}}); got.enabled {
		t.Fatal("scope alone activated document mode")
	}
	if got := documentSignalsFromOptions(cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.ACPDocumentReuseMetadataKey: true,
	}}); got.enabled {
		t.Fatal("reuse alone activated document mode")
	}
}

// TestDocumentIdentityR3YouTubeGate covers the R3 normalization rules:
//   - YouTube tab titles keep their notification-counter normalization;
//   - ordinary numbered titles stay distinct (never merged);
//   - a "(year)" prefix on a non-YouTube title is untouched.
func TestDocumentIdentityR3YouTubeGate(t *testing.T) {
	signals := documentSignals{enabled: true, client: "immersive-translate"}
	build := func(title string) []byte {
		return []byte(`{"messages":[{"role":"system","content":"t"},{"role":"user","content":"[[CLIPROXY_ACP_TITLE_PROMPT:v1]]\nTitle: \"` + title + `\"\n[[/CLIPROXY_ACP_TITLE_PROMPT]]\nbatch"}]}`)
	}
	keyOf := func(t *testing.T, title string) string {
		t.Helper()
		info, err := prepareDocumentRequest(build(title), "auth", "model", signals, "openai")
		if err != nil {
			t.Fatalf("prepare %q: %v", title, err)
		}
		return info.key
	}

	// (132)/(133) Foo - YouTube normalize to the same identity.
	if k1, k2 := keyOf(t, "(132) Foo - YouTube"), keyOf(t, "(133) Foo - YouTube"); k1 != k2 {
		t.Fatalf("YouTube notification counters must not split identity")
	}
	// (1) Introduction and (2) Introduction remain distinct documents.
	if k1, k2 := keyOf(t, "(1) Introduction"), keyOf(t, "(2) Introduction"); k1 == k2 {
		t.Fatalf("legitimate numbered article titles were merged (R3 regression)")
	}
	// (2024) Annual Report remains unchanged (also distinct from others).
	if k1, k2 := keyOf(t, "(2024) Annual Report"), keyOf(t, "Annual Report"); k1 == k2 {
		t.Fatalf("year prefix was stripped from a non-YouTube title (R3 regression)")
	}
}

func TestDocumentSessionTableLaneSerializesSameKey(t *testing.T) {
	table := helps.NewDocumentSessionTable(time.Minute, 10)
	first := table.LockLane("same")
	entered := make(chan struct{})
	released := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		second := table.LockLane("same")
		close(entered)
		second()
	}()
	select {
	case <-entered:
		t.Fatal("same document lane was not serialized")
	case <-time.After(20 * time.Millisecond):
	}
	first()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("second document lane waiter was not released")
	}
	close(released)
	wg.Wait()
}
