# ACP TTFT and Stateful Session Reuse Plan

## Goals

1. Minimize warm-path ACP time-to-first-token (TTFT).
2. Preserve the current stateless OpenAI-compatible behavior by default.
3. Add an **explicit opt-in stateful session mode** for compatible clients such as TranslateNow, so multiple turns in one logical client conversation can reuse the same already-used ACP session safely.
4. Keep every stateful optimization recoverable: if a worker/session disappears, the next request must be able to bootstrap a fresh ACP session from the full request history.

The normal stateless target remains:

```text
warm authenticated ACP process
        +
unused fresh session already prepared
        |
        v
HTTP request
        |
        v
session/prompt
        |
        v
first meaningful agent_message_chunk
        |
        v
first downstream token
```

For explicitly stateful clients, the stronger target is:

```text
HTTP turn N
  -> canonical logical session lookup
  -> acquire the worker that owns the live ACP session
  -> reuse the existing ACP session
  -> send only the new turn
  -> first meaningful agent_message_chunk
  -> first downstream token
```

No `session/new` or `session/set_config_option` should be required on a healthy stateful-session cache hit.

---

## Current Status

As of `main` commit `fbc35dd3c3de5ed6a7451f3d80263baa3e5329cd`, the original TTFT plan is substantially implemented.

### Completed

- Persistent authenticated ACP daemon workers.
- Same-auth multi-worker pool (`map[authKey][]*Worker`) so concurrent requests do not always serialize behind one generation.
- Per-auth and global worker limit plumbing.
- TTFT stage logging infrastructure.
- Redundant `session/set_config_option` skip when `session/new` already selected the requested model.
- Prompt construction overlapped with session setup.
- Prepared fresh-session cache so an idle worker can hide `session/new` outside the request critical path.
- Raw JSON stream chunks to avoid double SSE framing.

### Measured prepared-session result

The prepared-session implementation measured approximately:

- local `session/new`: ~1.4 s;
- 1-core VPS `session/new`: ~2.0-2.9 s;
- warm prepared first token: ~2.24-2.31 s;
- fresh path: ~2.34-2.74 s in the local comparison.

This confirms that session lifecycle work is large enough to justify session preparation and, for real conversations, used-session reuse.

---

# Phase 0 — Fix Correctness and Measurement Before Adding Stateful Reuse

These fixes come first because sticky sessions depend on worker lifetime, cancellation semantics, and trustworthy measurements.

## P0.1 — Fix `max-workers-total: 0`

The documented meaning is:

```yaml
max-workers-total: 0   # no global cap; only per-auth caps apply
```

The pool constructor currently normalizes every `maxTotal < maxWorkers` to `maxWorkers`, which also converts `0` into a real global cap.

Change the normalization to apply only when a positive global cap was explicitly configured:

```go
if maxTotal > 0 && maxTotal < maxWorkers {
    maxTotal = maxWorkers
}
```

### Tests

Cover at least:

- `maxTotal == 0` permits independent per-auth worker growth;
- positive `maxTotal < maxWorkers` is normalized safely;
- positive global cap remains strict.

---

## P0.2 — Make TTFT Stage Measurements Accurate

The current stage fields exist, but several boundaries are not yet measuring what their names imply.

Required fixes:

- record `session_new_done` immediately after `session/new`, not after all session setup;
- record `model_config_done` separately after the optional model configuration;
- wire `first_acp_stdout_line` from the ACP transport/reader rather than leaving it unset;
- record prompt write completion from the actual ACP client write path, not immediately before calling `Prompt`;
- distinguish first ACP update from first **meaningful text token**;
- define user-visible TTFT as first non-empty `agent_message_chunk`, not an arbitrary thought/status/update packet.

Recommended derived metrics:

```text
pool_wait_ms
session_new_ms
model_config_ms
prompt_build_ms
prompt_write_ms
backend_to_first_output_ms
first_output_to_first_text_ms
first_text_to_downstream_chunk_ms
first_token_ttft_ms
```

Prepared-session and stateful-session hits should also expose a mode field, for example:

```text
session_mode=fresh|prepared|stateful_reuse
```

This makes regressions directly visible in production logs.

---

## P0.3 — Fix Prompt-Build Cleanup on Early Session Failure

`buildPromptBlocks` can stage temporary attachments while it runs in parallel with session setup.

If session setup fails before the prompt result is consumed, the request can return without calling the prompt builder's cleanup function.

Refactor so every completed prompt-build result is eventually drained and cleaned, including:

- `session/new` failure;
- model configuration failure;
- request cancellation while session setup is running.

Do not give up the parallelism; only make ownership/cleanup unconditional.

---

## P0.4 — Prevent Prepared-Session Refill From Competing With a New Request

Background refill currently marks `refilling`, but request acquisition is based on `inUse` and can lease a worker while a refill `session/new` is still running.

Even if the ACP transport supports multiple pending JSON-RPC calls, the daemon may serialize or contend internally, turning a background optimization into visible TTFT.

Required invariant:

> No background prepared-session refill may overlap the request-critical ACP operations of a newly acquired worker unless benchmarks explicitly prove that overlap is beneficial and safe.

Possible implementations:

- make `Acquire` wait for/interrupt refill before leasing the worker; or
- perform refill under the same worker-exclusive operation gate used by prompt turns.

Cancellation must not leave a worker permanently marked as refilling.

---

## P0.5 — Make Prepared Sessions Model-Aware

The current prepared cache primarily hides `session/new`; if the prepared session's default model differs from the requested variant, the request still pays `session/set_config_option`.

Track the worker's most recently used/dominant resolved variant and prepare the next fresh session for that variant when practical.

Suggested first policy:

- one prepared session per worker;
- prepare for the most recently completed request's resolved model variant;
- if a request asks for another variant, consume/create normally and update the preferred variant for the next refill;
- never reuse a prompted session as a prepared session.

---

# Phase 1 — Explicit Stateful ACP Session Reuse

This is the main cross-project optimization with TranslateNow.

## Design Rule: Stateful Reuse Must Be Opt-In

Do **not** automatically reuse a prompted ACP session just because a generic request happens to contain a session/conversation identifier.

Many OpenAI-compatible clients resend their entire history on every HTTP request. Reusing an ACP session while also replaying that history duplicates context and can change output semantics.

Require an explicit signal in addition to the already-supported canonical session identity.

Initial protocol:

```http
X-Session-ID: <stable logical client session id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn index>
```

`X-Session-ID` already participates in canonical session identity extraction; no second affinity namespace is required.

`X-ACP-Session-Reuse: 1` means:

> The client deliberately supports stateful ACP continuation. It will still send the full history as a recovery source, but on a verified cache hit the proxy may send only the new turn to the existing ACP session.

The feature must be ignored for requests without the explicit reuse signal.

---

## P1.1 — Add Stateful Session Bindings

Maintain a bounded mapping conceptually equivalent to:

```go
type StatefulACPBinding struct {
    LogicalSessionID string
    ACPSessionID     string
    Worker           *AntigravityAcpWorker
    AuthKey          string
    ModelVariant     string
    LastTurn         int64
    LastUsedAt       time.Time
}
```

The binding is only valid while its owning ACP worker/client remains alive.

### Important worker rule

A binding does **not** reserve the worker between turns. One worker may host many ACP sessions and serve them sequentially.

However, when a bound logical session returns, the request must reacquire the **same worker/client**, because the ACP session ID is process-local unless the daemon later exposes a proven load/resume primitive.

If that worker is busy serving another prompt, the stateful turn waits for that worker rather than silently moving to a different process.

---

## P1.2 — Bootstrap on Miss, Increment on Hit

The client continues sending the full OpenAI-compatible history on every request.

### Binding miss

```text
full HTTP history
  -> fresh/prepared ACP session
  -> build normal full prompt
  -> session/prompt
  -> on successful completion, bind logical ID -> worker + ACP session ID + turn
```

A prepared fresh session may become the stateful session after its first successful prompt.

### Binding hit

Validate:

- same auth namespace / selected credential expectations;
- same resolved model variant;
- expected monotonic turn;
- worker healthy and still present;
- no incompatible configuration change.

Then:

```text
full HTTP request remains available for recovery
  -> extract only the newest user turn
  -> existing ACP session/prompt(new turn only)
```

Do not replay the previous `system`, user, or assistant messages on a hit.

---

## P1.3 — Turn Sequencing and Duplicate Protection

Stateful reuse is unsafe without ordering checks.

Use `X-ACP-Session-Turn` as a monotonic logical turn number.

Suggested semantics:

- first successful turn: `0`;
- next turn: `1`;
- and so on.

Reuse only when:

```text
incoming_turn == binding.last_turn + 1
```

If the turn is duplicated, skipped, stale, or malformed:

1. invalidate or bypass the existing binding;
2. bootstrap from the full request history;
3. establish a new valid binding after successful completion.

Never append the same logical turn twice to a live ACP conversation.

Concurrent requests for the same logical session must be serialized by logical-session order.

---

## P1.4 — Cancellation and Error Semantics

A canceled or failed prompt may leave provider-side session state ambiguous.

Use the conservative rule:

> Advance a binding's `LastTurn` only after `session/prompt` completes successfully.

Invalidate the stateful binding on:

- request cancellation after prompt dispatch;
- ACP transport failure;
- worker death/restart;
- prompt error where provider-side mutation is uncertain;
- incompatible model/config change;
- turn-sequence mismatch.

The next request then bootstraps from its full history, so recovery is transparent.

Do not invalidate merely because downstream response formatting fails after a successfully completed ACP turn; handle that case explicitly so provider state and local turn tracking do not diverge.

---

## P1.5 — Binding Lifetime and Bounds

Stateful bindings must not grow without limit.

Initial configuration proposal:

```yaml
antigravity:
  stateful-session-reuse: true
  stateful-session-ttl: "30m"
  max-stateful-sessions-per-worker: 64
```

Behavior:

- refresh TTL on successful continued use;
- remove every binding owned by a worker when that worker dies/closes;
- LRU-evict idle bindings above the configured bound;
- eviction only drops the proxy binding; do not assume an unsupported ACP `session/close` RPC.

If long-term daemon-side abandoned-session memory becomes material, measure it separately and consider controlled worker recycling.

---

## P1.6 — Tests for Stateful Reuse

Use a fake ACP agent that logs methods and prompt bodies.

Required tests:

1. First opted-in turn creates/consumes one ACP session and sends full history.
2. Second turn with the same logical ID and expected turn number performs **no `session/new`** and sends only the new user turn.
3. Different logical session does not reuse the first ACP session.
4. Missing `X-ACP-Session-Reuse` preserves current stateless semantics.
5. Turn mismatch falls back to full-history bootstrap.
6. Worker death invalidates the binding and bootstraps successfully.
7. Cancellation invalidates the binding.
8. Same-session concurrent turns are serialized in order.
9. Different stateful sessions can still use the normal multi-worker concurrency policy.
10. Prepared fresh sessions can become stateful sessions without being returned to the prepared pool after use.

---

# Phase 2 — TranslateNow Integration Contract

TranslateNow will be the first intentional client of stateful reuse.

The proxy-side feature should be implemented and tested **before** TranslateNow enables it.

Expected client behavior:

```http
X-Session-ID: translatenow:<TranslationSession.id>:<ApiConfig.id>:<semantic-config-hash>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <TranslationSession.turns.length>
```

The semantic configuration hash should change when continuation semantics change, including at least:

- endpoint identity;
- model;
- relevant body/model parameters;
- system prompt;
- glossary;
- source language;
- target language;
- reading-output mode.

Do not include secrets such as the API key in the visible session identifier.

### Why the client still sends full history

The full message list remains the recovery source for:

- proxy restart;
- worker eviction;
- binding TTL expiry;
- cancellation;
- daemon crash;
- model/config change;
- deployment rolling restart.

A cache hit uses only the incremental turn, while a cache miss can always reconstruct the conversation from the same HTTP request.

---

# Phase 3 — Remaining P2 Micro-Optimizations

Only do these after the corrected stage metrics show proxy CPU/copy overhead is meaningful.

## P2.1 — Single-Pass Outbound ACP JSON Encoding

`Client.call()` still performs multiple full-buffer operations:

1. marshal params;
2. store params as `json.RawMessage`;
3. marshal the whole JSON-RPC message again;
4. append a newline for NDJSON framing.

For large histories/attachments, move toward one encode directly to the serialized writer.

Possible shape:

```go
type outboundRequest struct {
    JSONRPC string `json:"jsonrpc"`
    ID      uint64 `json:"id"`
    Method  string `json:"method"`
    Params  any    `json:"params,omitempty"`
}
```

Use a serialized writer/encoder that emits the final newline exactly once.

---

## P2.2 — Reduce Repeated Session-Update Decoding

The same session update is decoded through multiple layers before text reaches the downstream chunk.

Move toward a typed update path that decodes session ID, update kind, and text once.

This is expected to be microsecond/sub-millisecond work and remains below session lifecycle/backend latency in priority.

---

## Not a Priority

### Replacing `bufio.Scanner`

ACP stdio is NDJSON, so the newline is the protocol message boundary. Scanner waiting for a complete line is not itself a TTFT bug.

### Tiny formatting/channel changes

Do not prioritize `fmt.Sprintf`, channel-capacity changes, or similar micro-allocation work unless corrected stage timing proves they are material.

---

# Cross-Repository Implementation Order

The implementation order is intentionally strict so both repositories remain deployable at every step.

## Step 1 — CLIProxyAPI correctness fixes

Implement first:

1. fix `max-workers-total: 0`;
2. correct TTFT stage boundaries;
3. fix prompt-build cleanup on early return;
4. remove prepared-refill/request overlap;
5. make prepared refill model-aware.

**Exit criterion:** existing stateless/prepared behavior is stable, race tests are green, and TTFT logs are trustworthy.

## Step 2 — CLIProxyAPI stateful-session core

Implement the opt-in headers, logical-session binding table, same-worker reacquisition, turn sequencing, incremental prompt extraction, invalidation, TTL/LRU bounds, and fake-agent tests.

Keep the feature disabled unless `X-ACP-Session-Reuse: 1` is present.

**Exit criterion:** synthetic two-turn requests prove that turn 2 performs no `session/new` and only the new user turn reaches the existing ACP session.

## Step 3 — TranslateNow client metadata, initially primary API only

After the proxy is ready, update TranslateNow to emit the stable logical session ID, turn index, and explicit reuse header while continuing to send the full history.

Enable stateful continuation only for the canonical/primary API in the first version so current multi-API conversation semantics do not change.

**Exit criterion:** disabling the reuse option/header restores byte-for-byte stateless request semantics apart from harmless metadata.

## Step 4 — End-to-end benchmark and failure testing

Test at least:

- first turn;
- immediate second/third turns;
- long idle gap;
- proxy restart;
- worker crash;
- browser refresh with persisted TranslateNow session;
- cancellation;
- model/language/glossary change;
- two TranslateNow sessions interleaved;
- concurrent non-TranslateNow traffic.

Compare corrected `session_mode=fresh|prepared|stateful_reuse` TTFT distributions.

## Step 5 — Only then do P2 JSON/decode optimizations

If backend generation dominates after stateful reuse, stop. Do not trade complexity for insignificant proxy-side savings.

## Step 6 — Optional multi-API stateful histories in TranslateNow

TranslateNow currently persists one canonical message history based on its primary API output. Stateful reuse for every parallel API would cause each API's provider-side conversation to diverge from that canonical history.

If per-API continuation is desired later, first introduce explicit per-API conversation histories/session identities in TranslateNow. Do not enable it implicitly in the first implementation.
