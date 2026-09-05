# ACP TTFT Optimization Plan

## Goal

Reduce warm-path ACP time-to-first-token (TTFT) without changing request semantics or weakening session isolation.

The current implementation already keeps ACP daemon processes warm and emits raw JSON chunks, but the request hot path still performs avoidable synchronous ACP work before `session/prompt`.

## Current Hot Path

For a warm pooled worker, streaming requests currently look roughly like:

```text
HTTP request
  -> Acquire warm ACP worker
  -> session/new
  -> session/set_config_option(model)   # usually
  -> buildPromptBlocks
  -> session/prompt
  -> first session/update
  -> first downstream chunk
```

The main remaining optimization target is to move the warm path toward:

```text
HTTP request
  -> Acquire warm ACP worker + prepared fresh session
  -> session/prompt
  -> first session/update
  -> first downstream chunk
```

## P0 — Instrument the TTFT Stages

Before making larger architectural changes, add stage-level timing so optimizations can be validated against real ACP/cloud latency.

Record at least:

```text
request_enter
pool_acquired
session_new_done
model_config_done
prompt_built
prompt_written_to_stdin
first_acp_stdout_line
first_session_update
first_chunk_enqueued
```

Derived timings:

- pool wait
- `session/new`
- `session/set_config_option`
- prompt construction
- prompt write -> first ACP output
- ACP first output -> downstream first chunk

Use the first meaningful token event for user-visible TTFT, while retaining first-packet timing for diagnostics.

### Success criterion

We should be able to distinguish proxy overhead from backend/model latency and avoid spending effort on sub-millisecond Go optimizations when the dominant cost is remote generation.

---

## P0 — Skip Redundant Model Configuration

`session/new` already returns `configOptions`, and the client caches them. Before calling:

```text
session/set_config_option(model)
```

compare the requested resolved variant against the session's current model.

If:

```text
CurrentModel(session.ConfigOptions) == requestedVariant
```

skip `SetConfigOption` entirely.

### Expected benefit

Removes one complete ACP JSON-RPC round trip whenever the daemon-created session already uses the requested model variant.

### Risk

Low. Request semantics remain unchanged.

---

## P0 — Make `max-workers` Effective for Same-Auth Concurrency

The current pool stores one worker per auth key. If that worker is busy, another request for the same auth joins the wait queue instead of spawning another worker, even when `max-workers > 1`.

Refactor from conceptually:

```go
map[authKey]*Worker
```

to something equivalent to:

```go
map[authKey][]*Worker
```

Acquire policy:

1. Reuse an idle worker for the auth key.
2. If all matching workers are busy and capacity remains, spawn another worker.
3. Otherwise queue the request.

Consider splitting the limits into:

```yaml
max-workers-per-auth: 2
max-workers-total: 4
```

### Expected benefit

Potentially very large under Hermes/subagent concurrency. A queued request can otherwise inherit the entire generation time of the currently active request as artificial TTFT.

### Additional fix

Enforce the global worker cap strictly. At present, if the pool is at capacity and no idle worker can be evicted, spawning may still continue.

---

## P1 — Prepared Fresh Session Cache

Do not reuse a session that has already handled a prompt, because ACP sessions carry conversational state and could leak or duplicate context across independent OpenAI-compatible requests.

Instead, maintain a small cache of **unused fresh sessions** on each warm worker.

Example:

```text
ACP worker
  -> fresh session configured for gemini-3.8-flash-low
  -> fresh session configured for gemini-3.8-flash-high
```

Request flow:

```text
Acquire worker
  -> pop matching fresh configured session
  -> session/prompt immediately
```

After a prepared session is consumed, refill asynchronously while the worker is idle.

### Suggested first implementation

Keep this deliberately small:

- one ready session per worker for the most recently/frequently used model variant;
- never return a used session to the ready pool;
- discard prepared sessions if the worker becomes unhealthy;
- fall back to normal `session/new` + configuration when no matching prepared session exists.

### Expected benefit

Removes `session/new` and usually `session/set_config_option` from the visible warm-request critical path.

### Validation required

Benchmark actual daemon cost of `session/new` and `set_config_option` first. If their combined cost is only a few milliseconds, keep this lower priority; if it is tens or hundreds of milliseconds, this becomes the main TTFT optimization.

---

## P1 — Build Prompt in Parallel With Session Preparation

Currently prompt construction happens after the session has already been opened/configured.

Run `buildPromptBlocks(req.Payload)` concurrently with:

- worker acquisition where practical;
- `session/new`;
- model configuration.

The pre-prompt latency then approaches:

```text
max(session setup, prompt build)
```

instead of:

```text
session setup + prompt build
```

### Expected benefit

Small for short text requests, potentially meaningful for large histories and requests containing attachments/base64 processing or temporary-file staging.

---

## P2 — Reduce Large-Prompt JSON Copies

`Client.call()` currently performs multiple full-buffer operations:

1. marshal params;
2. embed params into `wireMessage`;
3. marshal the entire request again;
4. append newline before write.

For large agent contexts, replace the outbound path with a single encode directly to the serialized writer where practical.

Potential shape:

```go
type outboundRequest struct {
    JSONRPC string `json:"jsonrpc"`
    ID      uint64 `json:"id"`
    Method  string `json:"method"`
    Params  any    `json:"params,omitempty"`
}
```

Then serialize once while holding the write lock.

### Expected benefit

Mostly CPU/allocation savings for very large prompts. Not expected to matter much for small requests.

---

## P2 — Reduce Repeated Decode Work on Session Updates

The first token currently passes through several JSON decode layers before being emitted downstream.

Possible future simplification:

```text
wire message
  -> decode sessionId / update kind / text once
  -> emit typed SessionUpdate
```

Avoid repeated unmarshalling of the same update payload in both the ACP transport and executor layer.

### Expected benefit

Likely microsecond/sub-millisecond territory. Only pursue after stage instrumentation shows proxy-side post-processing is material.

---

## Not a Priority

### Replacing `bufio.Scanner`

ACP stdio uses NDJSON: one complete JSON-RPC message per line. The scanner therefore naturally emits at the ACP framing boundary. The stdin path writes directly to the pipe and does not have an unflushed `bufio.Writer` problem.

Do not replace the scanner merely for TTFT unless profiling demonstrates measurable parser/buffer overhead.

### Tiny allocation / formatting changes

Avoid prioritizing changes such as replacing `fmt.Sprintf`, changing channel sizes, or other micro-optimizations until the stage timings prove they matter.

---

## Recommended Implementation Order

1. Add stage-level TTFT instrumentation.
2. Skip redundant `session/set_config_option` calls.
3. Fix same-auth multi-worker concurrency and enforce worker limits.
4. Benchmark warm-path `session/new` + model configuration cost.
5. If significant, add a prepared fresh-session cache.
6. Parallelize prompt construction with session setup.
7. Only then optimize JSON allocation/copy/decode paths.

## Target Warm Path

The long-term target is:

```text
warm authenticated ACP process
        +
unused fresh session already pinned to requested model
        |
        v
HTTP request
        |
        v
session/prompt
        |
        v
first session/update
        |
        v
first downstream token
```

The main objective is to reach **zero ACP round trips before `session/prompt` on the normal warm path** while preserving request/session isolation.
