package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

const (
	documentTitlePromptStart = "[[CLIPROXY_ACP_TITLE_PROMPT:v1]]"
	documentTitlePromptEnd   = "[[/CLIPROXY_ACP_TITLE_PROMPT]]"
)

// youtubeTitleDecoration strips a leading YouTube notification counter
// (e.g. "(132) ") from a page title. R3: this is a YouTube tab-title
// decoration, NOT a generic title pattern — a bare "(1) Introduction" vs
// "(2) Introduction" are legitimate distinct documents and must never be
// merged. Callers must gate it on recognizable YouTube evidence
// (see normalizeDocumentTitle).
var youtubeTitleDecoration = regexp.MustCompile(`^\(\d+\)\s*`)

// youtubeTitleSuffix is the recognizability gate for YouTube decoration
// stripping: browser tab titles for youtube.com pages end with this marker.
const youtubeTitleSuffix = " - YouTube"

type documentSignals struct {
	enabled    bool
	client     string
	documentID string
}

type documentRequestInfo struct {
	key             string
	title           string
	cleanedPayload  []byte
	incrementalBody []byte
}

func documentSignalsFromOptions(opts cliproxyexecutor.Options) documentSignals {
	var signals documentSignals
	if len(opts.Metadata) == 0 {
		return signals
	}
	scope, _ := opts.Metadata[cliproxyexecutor.ACPDocumentScopeMetadataKey].(bool)
	reuse, _ := opts.Metadata[cliproxyexecutor.ACPDocumentReuseMetadataKey].(bool)
	if !scope || !reuse {
		return signals
	}
	signals.enabled = true
	if client, ok := opts.Metadata[cliproxyexecutor.ACPDocumentClientMetadataKey].(string); ok {
		signals.client = strings.TrimSpace(client)
	}
	if documentID, ok := opts.Metadata[cliproxyexecutor.ACPDocumentIDMetadataKey].(string); ok {
		signals.documentID = strings.TrimSpace(documentID)
	}
	return signals
}

func prepareDocumentRequest(payload []byte, authKey, modelVariant string, signals documentSignals, sourceFormat string) (documentRequestInfo, error) {
	var root any
	if err := json.Unmarshal(payload, &root); err != nil {
		return documentRequestInfo{}, fmt.Errorf("document mode requires a JSON request payload: %w", err)
	}

	var title string
	var marked bool
	var rewrite func(any) any
	rewrite = func(value any) any {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				typed[key] = rewrite(child)
			}
		case []any:
			for i, child := range typed {
				typed[i] = rewrite(child)
			}
		case string:
			if strings.Contains(typed, documentTitlePromptStart) && strings.Contains(typed, documentTitlePromptEnd) {
				start := strings.Index(typed, documentTitlePromptStart) + len(documentTitlePromptStart)
				end := strings.Index(typed[start:], documentTitlePromptEnd)
				if end >= 0 {
					if title == "" {
						title = parseDocumentTitle(typed[start : start+end])
					}
					marked = true
				}
			}
			if !marked && title == "" {
				title = parseDocumentTitle(typed)
			}
			return strings.ReplaceAll(strings.ReplaceAll(typed, documentTitlePromptStart, ""), documentTitlePromptEnd, "")
		}
		return value
	}
	cleanedRoot := rewrite(root)
	cleanedPayload, err := json.Marshal(cleanedRoot)
	if err != nil {
		return documentRequestInfo{}, fmt.Errorf("marshal cleaned document request: %w", err)
	}
	if signals.documentID == "" && strings.TrimSpace(title) == "" {
		return documentRequestInfo{}, fmt.Errorf("document mode requires a page title or X-ACP-Document-ID")
	}
	semantic := documentSemanticFingerprint(cleanedRoot)
	incremental, err := newestDocumentUserBatch(cleanedRoot)
	if err != nil {
		return documentRequestInfo{}, err
	}

	identity := signals.documentID
	if identity == "" {
		identity = normalizeDocumentTitle(title)
	}
	material := strings.Join([]string{authKey, signals.client, identity, modelVariant, sourceFormat, semantic}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return documentRequestInfo{
		key:             "document:" + hex.EncodeToString(digest[:]),
		title:           title,
		cleanedPayload:  cleanedPayload,
		incrementalBody: incremental,
	}, nil
}

func parseDocumentTitle(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Title:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "Title:"))
		value = strings.Trim(value, "\"“”")
		return strings.TrimSpace(value)
	}
	return ""
}

func normalizeDocumentTitle(title string) string {
	title = strings.Join(strings.Fields(strings.TrimSpace(title)), " ")
	// R3: strip the YouTube notification counter only when the title is
	// recognizably a YouTube tab title. Ordinary documents beginning with
	// "(number)" (e.g. "(1) Introduction", "(2024) Annual Report") stay
	// distinct. A future real URL/document id should replace this heuristic.
	if strings.HasSuffix(title, youtubeTitleSuffix) {
		return youtubeTitleDecoration.ReplaceAllString(title, "")
	}
	return title
}

func newestDocumentUserBatch(root any) ([]byte, error) {
	if obj, ok := root.(map[string]any); ok {
		if messages, ok := obj["messages"].([]any); ok {
			for i := len(messages) - 1; i >= 0; i-- {
				item, ok := messages[i].(map[string]any)
				if ok && item["role"] == "user" {
					obj["messages"] = []any{item}
					return json.Marshal(obj)
				}
			}
		}
		if input, ok := obj["input"].([]any); ok {
			for i := len(input) - 1; i >= 0; i-- {
				item, ok := input[i].(map[string]any)
				if ok && item["type"] == "message" && item["role"] == "user" {
					obj["input"] = []any{item}
					return json.Marshal(obj)
				}
			}
		}
	}
	return nil, fmt.Errorf("document mode requires a user translation batch")
}

func documentSemanticFingerprint(root any) string {
	projection := documentSemanticProjection(root)
	raw, _ := json.Marshal(projection)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func documentSemanticProjection(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			switch key {
			case "messages", "input":
				if items, ok := child.([]any); ok {
					filtered := make([]any, 0, len(items))
					for _, item := range items {
						if itemMap, ok := item.(map[string]any); ok {
							role, _ := itemMap["role"].(string)
							if role != "system" && role != "developer" {
								continue
							}
						}
						filtered = append(filtered, documentSemanticProjection(item))
					}
					out[key] = filtered
					continue
				}
			}
			out[key] = documentSemanticProjection(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = documentSemanticProjection(child)
		}
		return out
	default:
		return value
	}
}

// documentAcquire carries the fully resolved acquisition state for one
// document request: the prepared request info plus, on a verified hit, the
// live binding components. R4: binding validation completes INSIDE
// acquireDocumentSession, so the hit decision (and its incremental payload)
// is final before the caller starts any prompt build.
type documentAcquire struct {
	info        documentRequestInfo
	authKey     string
	variant     string
	sessionID   string // bound ACP session; valid only when hit
	worker      *helps.AntigravityAcpWorker
	client      *acp.Client
	hit         bool
	releaseLane func()
}

func (e *AntigravityAcpExecutor) acquireDocumentSession(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stages *acpTTFTStage) (documentAcquire, error) {
	noRelease := func() {}
	signals := documentSignalsFromOptions(opts)
	if !signals.enabled {
		return documentAcquire{releaseLane: noRelease}, nil
	}
	authKey := e.authPoolKey(auth)
	variant := resolveAntigravityModel(req.Model, req.Payload)
	info, err := prepareDocumentRequest(req.Payload, authKey, variant, signals, string(opts.SourceFormat))
	if err != nil {
		return documentAcquire{authKey: authKey, variant: variant, releaseLane: noRelease}, err
	}
	if e.pool == nil || e.document == nil {
		return documentAcquire{info: info, authKey: authKey, variant: variant, releaseLane: noRelease}, nil
	}
	releaseLane, laneErr := e.document.AcquireLane(info.key, ctx, 0)
	if laneErr != nil {
		// R2 backpressure: the per-document queue is full (or the client
		// went away). Return a retryable error instead of creating another
		// ACP session behind an unbounded wait queue.
		return documentAcquire{info: info, authKey: authKey, variant: variant, releaseLane: func() {}}, laneErr
	}
	acq := documentAcquire{info: info, authKey: authKey, variant: variant, releaseLane: releaseLane}
	binding, ok := e.document.Lookup(info.key, nil, authKey, variant)
	if !ok {
		return acq, nil
	}
	worker, err := e.pool.AcquireSpecific(ctx, binding.Worker)
	if err != nil {
		e.document.Invalidate(info.key)
		return acq, nil
	}
	// Authoritative post-lease validation: this is the FINAL binding check.
	// A hit returned here is verified live; the caller must not re-decide
	// the payload after its prompt builder started (R4).
	binding, ok = e.document.Lookup(info.key, worker, authKey, variant)
	if !ok {
		e.pool.Release(worker, true)
		return acq, nil
	}
	now := time.Now()
	stages.markPoolAcquired()
	stages.markSessionSetup(now, now, "document_reuse")
	acq.hit = true
	acq.sessionID = binding.ACPSessionID
	acq.worker = worker
	acq.client = worker.Client()
	return acq, nil
}
