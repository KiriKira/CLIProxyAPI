package executor

import (
	"encoding/json"
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
