package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// statefulTurnSignals is the per-request stateful-reuse signal set carried
// in Options.Metadata. Zero value (reuse=false) keeps stateless semantics:
// the feature is strictly opt-in via X-ACP-Session-Reuse: 1 (PLAN Phase 1).
type statefulTurnSignals struct {
	logicalID string
	reuse     bool
	turn      int64
	turnValid bool
}

// statefulTurnSignalsFromOptions extracts the explicit stateful-continuation
// signals. Malformed/missing turn numbers disable reuse for the request:
// a stateful turn without ordering is unsafe (P1.3).
func statefulTurnSignalsFromOptions(opts cliproxyexecutor.Options) statefulTurnSignals {
	var s statefulTurnSignals
	if len(opts.Metadata) == 0 {
		return s
	}
	if v, ok := opts.Metadata[cliproxyexecutor.LogicalSessionIDMetadataKey].(string); ok {
		s.logicalID = v
	}
	if v, ok := opts.Metadata[cliproxyexecutor.ACPStatefulReuseMetadataKey].(bool); ok && v {
		s.reuse = true
	}
	switch v := opts.Metadata[cliproxyexecutor.ACPStatefulTurnMetadataKey].(type) {
	case int64:
		s.turn = v
		s.turnValid = true
	case int:
		// gin/encoding-json decodes JSON numbers into any as int when the
		// value fits; accept every Go integer shape defensively.
		s.turn = int64(v)
		s.turnValid = true
	case int32:
		s.turn = int64(v)
		s.turnValid = true
	case float64:
		// JSON decode into interface{} yields float64.
		if v == float64(int64(v)) {
			s.turn = int64(v)
			s.turnValid = true
		}
	}
	if s.logicalID == "" || !s.turnValid {
		s.reuse = false
	}
	return s
}

// statefulIncrementalTurn extracts ONLY the newest user turn from the
// request payload for a stateful cache hit. The full history stays
// available to the caller for recovery bootstrapping; on a hit none of the
// previous system/user/assistant messages are replayed (P1.2).
// Supported shapes: OpenAI chat messages[] and Responses input[].
func statefulIncrementalTurn(payload []byte) ([]byte, error) {
	msgs := gjson.GetBytes(payload, "messages")
	if msgs.Exists() && msgs.IsArray() {
		arr := msgs.Array()
		for i := len(arr) - 1; i >= 0; i-- {
			m := arr[i]
			if m.Get("role").String() != "user" {
				continue
			}
			turn := map[string]any{
				"messages": []any{map[string]any{
					"role":    "user",
					"content": m.Get("content").Value(),
				}},
			}
			raw, err := json.Marshal(turn)
			if err != nil {
				return nil, fmt.Errorf("stateful turn marshal: %w", err)
			}
			return raw, nil
		}
		return nil, fmt.Errorf("stateful: no user message in history")
	}
	input := gjson.GetBytes(payload, "input")
	if input.Exists() && input.IsArray() {
		arr := input.Array()
		for i := len(arr) - 1; i >= 0; i-- {
			item := arr[i]
			if item.Get("type").String() != "message" || item.Get("role").String() != "user" {
				continue
			}
			turn := map[string]any{"input": []any{item.Value()}}
			raw, err := json.Marshal(turn)
			if err != nil {
				return nil, fmt.Errorf("stateful turn marshal: %w", err)
			}
			return raw, nil
		}
		return nil, fmt.Errorf("stateful: no user item in input")
	}
	return nil, fmt.Errorf("stateful: unsupported payload shape")
}

// acquireStatefulSession runs the stateful cache-hit path for one opted-in
// turn: validate the binding (turn order, auth key, variant), reacquire the
// SAME worker that owns the live ACP session, and return it with the bound
// session id and resolved variant. Any miss (no binding, turn mismatch,
// worker gone) returns hit=false with a nil worker and the caller
// bootstraps statelessly from the full history the client re-sent —
// recovery stays transparent (P1.2).
func (e *AntigravityAcpExecutor) acquireStatefulSession(ctx context.Context, auth *cliproxyauth.Auth, signals statefulTurnSignals, model string, payload []byte, stages *acpTTFTStage) (worker *helps.AntigravityAcpWorker, sessionID string, variant string, hit bool, err error) {
	key := e.authPoolKey(auth)
	tableKey := logicalLookupKey(signals.logicalID, key)
	resolvedVariant := resolveAntigravityModel(model, payload)

	// Turn-order gate before any lease (P1.3): reuse only when
	// incoming_turn == binding.LastTurn + 1.
	binding, present := e.stateful.Lookup(tableKey, nil, key, resolvedVariant)
	if !present {
		return nil, "", "", false, nil
	}
	if signals.turn != binding.LastTurn+1 {
		// Duplicated, skipped, stale or malformed turn: invalidate and
		// bootstrap from the full request history.
		e.stateful.Invalidate(tableKey)
		return nil, "", "", false, nil
	}
	target := e.stateful.WorkerOf(tableKey)
	if target == nil {
		return nil, "", "", false, nil
	}
	// Reacquire the SAME worker: the ACP session id is process-local. When
	// the worker is busy serving another prompt, this turn waits for it
	// rather than silently moving to a different process (P1.1 worker rule).
	w, acqErr := e.pool.AcquireSpecific(ctx, target)
	if acqErr != nil {
		// Worker died or left the pool: drop the binding, bootstrap.
		e.stateful.Invalidate(tableKey)
		return nil, "", "", false, nil
	}
	// Authoritative worker-matched validation after the lease.
	binding, present = e.stateful.Lookup(tableKey, w, key, resolvedVariant)
	if !present {
		e.pool.Release(w, true)
		return nil, "", "", false, nil
	}
	stages.markPoolAcquired()
	return w, binding.ACPSessionID, resolvedVariant, true, nil
}

// logicalLookupKey scopes the logical session id by auth pool key so two
// credentials never share an ACP session binding (P1.2 namespace rule).
func logicalLookupKey(logicalID, authKey string) string {
	return authKey + "|" + logicalID
}

// bindStatefulTurn records a successfully completed turn on the binding
// table. Called only after session/prompt completes successfully (P1.4):
// canceled or failed prompts never advance LastTurn. A fresh or prepared
// session that served its first opted-in prompt becomes the stateful
// session; a continued turn advances the binding.
func (e *AntigravityAcpExecutor) bindStatefulTurn(signals statefulTurnSignals, authKey, acpSessionID, modelVariant string, worker *helps.AntigravityAcpWorker, turn int64) {
	if e.stateful == nil || worker == nil {
		return
	}
	e.stateful.Bind(logicalLookupKey(signals.logicalID, authKey), acpSessionID, authKey, modelVariant, worker, turn)
}

// invalidateStateful drops the binding after an ambiguous prompt outcome
// (cancellation, transport failure): provider-side session state cannot be
// trusted, so the next turn bootstraps from full history (P1.4).
func (e *AntigravityAcpExecutor) invalidateStateful(signals statefulTurnSignals, authKey string) {
	if e.stateful == nil {
		return
	}
	e.stateful.Invalidate(logicalLookupKey(signals.logicalID, authKey))
}
