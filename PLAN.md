# ACP Session Lifecycle, Document Affinity, and TTFT Plan

## Goals

1. Keep warm-path ACP TTFT low.
2. Make ACP daemon/session/harness resource usage **bounded over time**, including purely stateless traffic.
3. Preserve stateless OpenAI-compatible semantics by default.
4. Keep the existing strict opt-in stateful conversation mode for clients that can provide a stable logical session id, monotonic turn number, and full-history recovery source.
5. Add a second, deliberately different **document-affinity mode** for clients such as Immersive Translate, where one video/article is delivered as many independent translation batches and the client does not send conversational history.
6. Make the document-affinity design generic enough for YouTube subtitles, long web articles, PDF/EPUB-style chunked translation, and similar segmented workloads.
7. Never recover throughput by creating an unbounded number of ACP sessions, workers, or `localharness_external` children.

The desired steady state for a segmented document is:

```text
first batch of document D
  -> derive/parse stable document key
  -> create or consume one fresh ACP session
  -> send full system + current batch
  -> bind document D -> worker + ACP session

later batch of document D
  -> same document key
  -> reacquire the owning worker
  -> reuse the same ACP session
  -> send only the new translation batch
  -> no session/new

switch to document E
  -> create/bind a different document session
```

For long documents the session may be rolled over deliberately, but rollover must be bounded and tied into worker recycling so old daemon-side harnesses cannot accumulate forever.

---

## Current Status and New Evidence

As of `main` commit `909ede2d88a52edc26c112b7cf02bc994165bfa4`:

### Already implemented

- Persistent authenticated ACP daemon workers.
- Same-auth multi-worker pool.
- Per-auth and global worker caps.
- Corrected TTFT stage instrumentation and `session_mode` logging.
- Prepared fresh-session cache.
- Model-aware prepared sessions.
- Refill/request exclusion.
- Prompt-build cleanup fixes.
- Strict opt-in stateful session reuse using:

```http
X-Session-ID: <logical conversation id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn>
```

- Same-worker reacquisition for bound ACP sessions.
- TTL/LRU stateful binding table.
- Incremental newest-turn prompting on a strict stateful hit.

### Newly demonstrated production blocker

The Immersive Translate YouTube stress test shows that the current session lifecycle is still unsafe for long-running stateless workloads:

- approximately one translation request every 25-30 seconds;
- almost every request consumes/creates a new ACP session;
- one ACP session corresponds to a new `localharness_external` child in the observed daemon;
- old harnesses do not disappear while the daemon stays alive;
- each old harness leaves roughly 40-63 MiB swapped out in the observed 1 GiB VPS workload;
- 37 harnesses filled roughly 1.9 GiB swap and caused severe swap thrashing and client timeouts;
- `prepared-sessions: 1` bounds only the **ready cache**, not the number of sessions/harnesses already created inside the daemon;
- `prepared-sessions: 0` removes asynchronous refill but does not solve the underlying per-request `session/new` growth for stateless requests.

Therefore session lifecycle/resource safety is now P0 and must be solved before further TTFT micro-optimization.

### Immersive Translate request identity evidence

The captured requests establish the following useful properties:

- request headers do not contain a video URL, video id, Referer, cookie, or any existing per-video custom header;
- the browser-extension `Origin` is stable and can identify the client class, but not the current document;
- the request body contains a stable `Document Metadata -> Title` for all batches of one video/page;
- YouTube titles may have a changing notification-count prefix such as `(132) ` and a ` - YouTube` suffix;
- subtitle batches restart their local item ids (`p0`-`p7`) on each request, so those ids are not a cross-request sequence number;
- Immersive Translate supports custom prompts and custom request headers, so a personal deployment can provide an explicit opt-in signal and a prompt-level document marker instead of relying on a global heuristic.

This is enough to build document affinity without patching the extension itself.

---

# Phase A — P0: Bound the ACP Daemon/Session/Harness Lifecycle

Document reuse reduces session creation dramatically, but it is not a substitute for a hard lifecycle bound. Arbitrary stateless clients must also be safe.

## A1 — Kill the whole ACP process tree, not only the daemon PID

`Client.Close()` currently owns the ACP daemon process, but on Linux the daemon can own `localharness_external` children. Recycling only the parent PID is not a sufficient cleanup contract.

Required Linux behavior:

1. spawn each ACP daemon in its own process group;
2. graceful close first (stdin EOF / normal daemon exit);
3. if it does not exit within the grace period, signal the **process group**;
4. escalate to group `SIGKILL` after the existing hard grace period;
5. verify no harness child survives as an orphan.

Conceptually:

```text
CLIProxyAPI
  -> ACP process group
       -> agy_acp_server.par
            -> localharness_external
            -> localharness_external
            -> ...

worker recycle
  -> terminate ACP process group
  -> every child disappears
```

Keep platform-specific process-group handling behind small OS-specific helpers; do not spread syscall conditionals through the ACP client.

### A1 tests

Use a fake agent that deliberately spawns a long-lived child process.

Verify:

- `Close()` removes the parent and child;
- forced timeout cleanup removes both;
- repeated close is idempotent;
- no zombie/orphan remains after worker eviction/recycling.

---

## A2 — Track the lifecycle state of every session created on a worker

A simple prepared-cache length is not enough. Track daemon-side session pressure per worker.

Conceptual state:

```go
type WorkerSessionStats struct {
    CreatedTotal      uint64
    Prepared          int
    BoundStrict       int
    BoundDocument     int
    Abandoned         int
}
```

A session becomes **abandoned** when CLIProxyAPI no longer has any path that can reuse it while the daemon still lives, for example:

- a normal stateless request completes;
- a strict stateful binding is invalidated/expired/evicted;
- a document binding is invalidated/expired/rolled over;
- a prepared session is dropped without being used;
- a failed/canceled prompt leaves the provider-side session state ambiguous and the binding is discarded.

Every `session/new`, including background preparation, must be registered exactly once.

The accounting is a safety ledger, not an assertion that the daemon exposes a usable `session/release` RPC.

---

## A3 — Add hard worker session budgets and a draining state

Initial configuration shape:

```yaml
antigravity:
  max-sessions-per-worker: 0              # 0 = disabled/backward-compatible until validated
  max-abandoned-sessions-per-worker: 0    # 0 = disabled/backward-compatible until validated
```

For the observed low-memory VPS, the first validation target should be deliberately small (for example 4-8 total/abandoned sessions), then benchmark upward rather than assuming a large default is safe.

When a worker reaches a configured budget:

```text
healthy -> draining
```

A draining worker:

- does not run prepared-session refill;
- does not create another fresh ACP session;
- may continue serving already-bound strict/document sessions while that does not increase session count;
- is not selected for unrelated fresh stateless work;
- is recycled once a new session would otherwise be required and no safe alternate worker is available;
- returns its global worker slot only after the worker/process tree has been removed.

For `max-workers: 1`, this naturally becomes a sawtooth lifecycle:

```text
spawn worker
 -> create bounded number of sessions
 -> drain
 -> kill process group (all harnesses reclaimed)
 -> spawn replacement
```

That converts unbounded growth into a hard upper bound even for clients that never opt into reuse.

### Important prepared-session rule

A prepared session consumes the same daemon-side session budget as any other session.

Do not refill when doing so would cross a hard worker budget. If a prepared session becomes a stateful/document session after its first prompt, change its accounting state rather than counting a second session.

---

## A4 — Integrate binding eviction with session accounting

Strict stateful and document bindings keep a daemon session reachable.

When a binding disappears:

```text
bound session -> abandoned session
```

This must happen on:

- TTL expiry;
- LRU eviction;
- model/config mismatch;
- cancellation after prompt dispatch;
- transport failure;
- explicit rollover;
- worker retirement.

Worker retirement itself clears the ledger because the process group and all daemon-side sessions are gone.

---

## A5 — Lifecycle observability

Add structured fields/counters sufficient to prove the bound in production:

```text
worker_sessions_created
worker_sessions_prepared
worker_sessions_bound_strict
worker_sessions_bound_document
worker_sessions_abandoned
worker_state=active|draining
worker_recycle_reason=session_cap|abandoned_cap|idle|dead|shutdown
```

Also retain the existing TTFT session mode and extend it later for document reuse.

Do not add Linux `/proc` memory polling to the core logic in the first implementation. Session-count bounds are deterministic and portable; process RSS/swap monitoring remains a diagnostic signal until needed for a second safety layer.

---

# Phase B — P1: Generic Document-Affinity Session Reuse

This phase targets clients that send a document as many independent batches rather than a real chat history.

It is intentionally separate from strict stateful conversations.

## B0 — Two different continuation contracts

### Strict conversation mode (already implemented)

Properties:

- client provides stable logical id;
- client provides monotonic turn number;
- client still sends full history;
- cache miss can reconstruct the full conversation exactly;
- duplicate/out-of-order turns are protocol errors/fallback conditions.

### Document-affinity mode (new)

Properties:

- client sends only the current translation batch, not previous batches;
- the proxy groups batches by a stable document key;
- the proxy assigns/serializes server-side document turns;
- on a healthy hit, only the current user batch is appended to the existing ACP session;
- on binding loss, the next request starts a new document session from the current full request; previous document context is not reconstructable and continuity is therefore opportunistic rather than exact.

That difference must remain explicit in code and naming. Do not silently treat a document stream as a strict conversation.

---

## B1 — Explicit wire opt-in, dynamic key from body/prompt

Preferred request contract:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate        # optional diagnostic/profile label
X-ACP-Document-ID: <stable id>           # optional, preferred when a client can provide one
```

Rules:

- `X-ACP-Session-Scope: document` selects the new semantics;
- `X-ACP-Session-Turn` is **not required** in document mode;
- if `X-ACP-Document-ID` exists, it is the preferred identity source;
- if no dynamic header id is available, parse a deliberately formatted prompt marker;
- if neither explicit id nor marker exists, a configured Immersive Translate fallback may parse `Document Metadata -> Title`;
- without document scope/explicit opt-in, preserve stateless behavior.

Do not globally auto-enable reuse just because a request resembles Immersive Translate.

### Prompt marker

Use a marker that is easy for the proxy to parse and remove before the prompt reaches the model, for example:

```text
[[CLIPROXY_ACP_DOCUMENT:v1]]
<document identity text>
[[/CLIPROXY_ACP_DOCUMENT]]
```

The marker payload can initially be `{{imt_title}}` for Immersive Translate.

The proxy should strip only the machine marker block, while leaving the normal human-readable title/context prompt intact.

This makes routing metadata non-semantic to the model.

---

## B2 — Canonical document key

Build a namespaced hash instead of storing the raw title as the lookup key:

```text
hash(
  auth namespace
  + client profile
  + scope=document
  + normalized document id/title
  + resolved model variant
  + source/target language when detectable
  + semantic prompt/config fingerprint
)
```

### Immersive Translate title normalization

For the observed YouTube case:

- trim Unicode/ASCII surrounding whitespace;
- collapse repeated internal whitespace where safe;
- strip a leading notification counter matching `^\(\d+\)\s*`;
- strip a trailing ` - YouTube` for the document identity;
- retain the original title in model-visible document context.

Do not apply YouTube-specific normalization to unrelated clients unless the client profile/document kind says it is appropriate.

### Collision rule

A title is not globally unique.

For the personal Immersive Translate integration, title + auth/client/model/language + idle TTL is acceptable as an initial fallback because the captured request has no URL/video id.

For a general-purpose interface:

1. explicit `X-ACP-Document-ID` wins;
2. a future URL/video-id/page-id variable should replace title-only identity when available;
3. title-only fallback must remain opt-in and documented as heuristic.

---

## B3 — Reuse the existing ACP session without replaying the system prompt

### First batch / binding miss

Send the normal full request:

```text
SYSTEM translation instructions + document context
USER current batch
```

After successful completion, bind the document key to the worker/session.

### Later batch / binding hit

Reuse the same worker/session and send only the newest user translation batch.

Do not replay the stable system prompt/title/translation instructions on every hit. They are already in the ACP conversation and replaying them wastes tokens and changes context semantics.

The existing newest-user-turn extraction logic can be factored into a shared helper, but strict and document modes must keep separate validation rules.

A document hit should log:

```text
session_mode=document_reuse
```

A new document binding can distinguish:

```text
session_mode=document_bootstrap
```

---

## B4 — Per-document bootstrap singleflight and serialization

This is critical for long articles.

A page translator may issue many requests concurrently. Without a per-document gate, ten simultaneous cold requests can all observe a binding miss and create ten ACP sessions before the first binding is established.

Required invariant:

> At most one lane may bootstrap a given document key at a time unless bounded multi-lane mode has been explicitly enabled.

For the initial one-lane implementation:

```text
request A (doc D) -> acquires document gate -> bootstraps session
request B (doc D) -> waits -> sees completed binding -> reuses session
request C (doc D) -> waits -> reuses session
```

Different documents must not share the same gate.

Within one document/session, prompts are serialized. Arrival order is the best available order when the client provides no cross-request sequence number.

### Queue bound

Add a configurable bound for waiting document requests. Do not turn overflow into new unbounded ACP sessions.

Possible initial behavior:

- bounded queue;
- overflow returns a retryable 429/503 rather than silently creating another session;
- instrument queue wait separately from ACP pool wait.

---

## B5 — Bounded multi-lane mode for article bursts

YouTube subtitle traffic is naturally low-rate and should stay on one session lane.

Long articles/PDFs may create enough parallel batches that one 4-10 second ACP generation lane causes unacceptable queueing.

Support an optional bounded lane count per document:

```yaml
antigravity:
  document-session-max-lanes: 1   # conservative default
```

Design:

- lane 1 is canonical and always created first;
- additional lanes are created only when queue pressure exceeds a configured threshold;
- never exceed `document-session-max-lanes`;
- each lane is a separate ACP session and therefore consumes worker session budget;
- select the least-loaded existing lane for new batches;
- each lane preserves context for the subset of batches routed through it;
- resource safety always wins over throughput.

For the personal integration, use one lane for subtitles. Evaluate two lanes for large article translation only if one-lane queue timings show real client timeouts.

If the prompt marker can classify the workload (`subtitle` vs `document`), keep separate lane policies later without hard-coding a web site.

---

## B6 — Duplicate/retry protection without a client turn number

Document mode has no monotonic client turn, so retries can otherwise append the same batch twice.

Compute a short-lived batch fingerprint from at least:

```text
document key
+ normalized newest-user batch
+ resolved model/config fingerprint
```

Maintain a bounded recent-fingerprint cache per document binding.

Desired behavior:

- concurrent identical requests coalesce onto one in-flight prompt when practical;
- a recently completed identical retry may return the cached translated response instead of appending a duplicate turn;
- cache size/TTL remains small and bounded;
- a fingerprint collision must never route across different document keys.

This is especially useful when a browser request times out after the provider already completed the turn.

---

## B7 — Document binding lifetime and long-context rollover

One session per entire document eliminates harness growth but can create a different problem: a multi-hour video or very large article can accumulate a large model context.

Track per document lane:

```go
type DocumentBindingStats struct {
    Turns                 uint64
    ApproxInputTokens     uint64
    ApproxOutputTokens    uint64
    CreatedAt             time.Time
    LastUsedAt            time.Time
}
```

Configuration shape:

```yaml
antigravity:
  document-session-idle-ttl: "0"          # choose after live measurement
  document-session-max-turns: 0            # 0 = disabled until measured
  document-session-max-estimated-tokens: 0 # 0 = disabled until measured
  max-document-sessions: 0                 # separate global/LRU bound if needed
```

Rollover conditions may include:

- idle TTL expired;
- max turns reached;
- estimated context budget reached;
- incompatible model/system-prompt/language config change;
- cancellation/transport ambiguity;
- worker retirement.

On rollover:

1. mark the old binding/session abandoned in the worker ledger;
2. bootstrap a fresh ACP session from the current request;
3. bind the same document key to the new session;
4. let Phase A recycle the worker before abandoned sessions can accumulate beyond its budget.

Do **not** attempt automatic summarization/carry-forward in v1. First make rollover correct and bounded. A small rolling translation-context handoff can be evaluated later if terminology consistency measurably suffers.

---

## B8 — Cancellation/error semantics for document mode

Use the same conservative provider-state rule as strict stateful reuse:

- cancellation before `session/prompt` dispatch: keep the binding;
- cancellation/error after prompt dispatch where provider mutation is ambiguous: invalidate the document binding and mark the session abandoned;
- ACP transport death: invalidate all bindings owned by that worker;
- downstream formatting failure after a successfully completed ACP turn: keep the binding and retain the completed batch fingerprint/response if possible;
- retry of a completed-but-lost response should hit the dedupe cache rather than append the same batch again.

Document mode can continue after invalidation by starting a fresh session, even though previous cross-batch context is lost.

---

# Phase C — P1: Immersive Translate Personal Integration

The generic document mode should be implemented in CLIProxyAPI; Immersive Translate only needs configuration.

## C1 — Static opt-in headers

Immersive Translate supports custom request headers. Configure the personal OpenAI-compatible service to send static metadata similar to:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Do not rely on a dynamic page id in the header unless Immersive Translate is proven to interpolate page variables there. Static headers are sufficient because the dynamic identity is carried in the prompt.

This also avoids hard-coding the extension id or browser User-Agent in CLIProxyAPI.

---

## C2 — Add a machine-readable document marker to the custom prompt

Immersive Translate exposes `{{imt_title}}` through its prompt/env machinery and already injects title context.

Modify the personal prompt/env so every translation request also carries a marker such as:

```text
[[CLIPROXY_ACP_DOCUMENT:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT]]
```

Keep the existing human-readable `Document Metadata -> Title` too.

For future configuration, optionally add a second stripped marker for workload kind:

```text
[[CLIPROXY_ACP_DOCUMENT_KIND:subtitle]]
```

or

```text
[[CLIPROXY_ACP_DOCUMENT_KIND:article]]
```

Because Immersive Translate has separate subtitle and multi-paragraph prompts, this can distinguish low-rate subtitles from bursty page/article translation without relying on URL-specific code.

The proxy strips these machine markers before forwarding the prompt to ACP.

---

## C3 — Existing-title fallback for immediate testing

Before the custom prompt is deployed, document mode may support a narrowly gated fallback parser for the already observed text:

```text
Document Metadata:
Title: "..."
```

or the equivalent localized/title quoting forms used by Immersive Translate.

Only use this fallback when:

- document scope was explicitly requested; or
- a dedicated `immersive-translate-auto-document-session` config flag is enabled.

Never use generic `Title:` text in arbitrary prompts as an automatic session key.

---

## C4 — YouTube-specific behavior

For the captured video:

```text
(132) <video title> - YouTube
```

normalize to:

```text
<video title>
```

so a changing notification counter does not split one video into multiple ACP sessions.

Expected live behavior:

```text
video A batch 1 -> document_bootstrap -> session X
video A batch 2 -> document_reuse     -> session X
video A batch N -> document_reuse     -> session X
video B batch 1 -> document_bootstrap -> session Y
```

The harness count should therefore remain roughly proportional to the small number of concurrently active documents/lanes, not the number of subtitle batches watched.

---

## C5 — Long article behavior

The same mechanism must work without YouTube assumptions:

```text
article title/key D
  paragraph batch 1 -> lane/session D1
  paragraph batch 2 -> reuse D1
  ...
```

If Immersive Translate sends many article batches at once:

- cold-start singleflight prevents one-session-per-request fan-out;
- the per-document queue provides backpressure;
- optional bounded lanes provide controlled throughput;
- every lane still counts toward the hard worker session budget;
- title collision risk remains documented until a URL/page-id marker is available.

This article case is part of the initial acceptance test, not a later afterthought.

---

# Phase D — P2: Tests and Production Exit Criteria

## D1 — Lifecycle regression tests

Required fake-agent/process tests:

1. Stateless requests cannot make one worker exceed the configured session cap.
2. Prepared refill stops at the worker cap.
3. Dropped prepared sessions increase abandoned accounting.
4. Stateful/document binding eviction increases abandoned accounting.
5. Draining worker does not accept a new session.
6. Worker recycle kills daemon + child process tree.
7. Global/per-auth worker limits remain correct while workers drain/recycle.
8. No race leaks worker slots or session ledger entries.

---

## D2 — Document-affinity functional tests

Required request-level tests:

1. First document request bootstraps exactly one ACP session.
2. Second request with the same document key performs no `session/new` and sends only the new user batch.
3. Different document key creates a different binding.
4. Missing document opt-in preserves stateless behavior.
5. Model/language/system-prompt fingerprint change rotates the document binding.
6. Idle TTL/turn/token rollover creates a fresh binding and marks the old session abandoned.
7. Cancellation after dispatch invalidates the binding.
8. Worker death invalidates all document bindings on that worker.
9. Duplicate completed batch is not appended twice.
10. Concurrent identical batch coalesces when dedupe singleflight is enabled.
11. Concurrent cold requests for one document create one lane/session in one-lane mode.
12. Queue overflow never falls back to unbounded session creation.
13. Optional two-lane mode never creates a third lane.

---

## D3 — Immersive Translate live acceptance test: YouTube

Repeat the original long-running subtitle test.

Exit criteria:

- same normalized video title repeatedly logs `document_reuse` after the first batch;
- `session/new` count for the video remains 1 per active lane until an intentional rollover/recycle;
- harness count does not grow linearly with subtitle request count;
- swap usage reaches a stable bounded range rather than monotonic growth;
- no 20-30 minute functional collapse;
- changing to another video creates a separate document binding;
- changing the YouTube notification counter prefix does not create a new binding.

Collect TTFT distributions for:

```text
fresh
prepared
document_bootstrap
document_reuse
stateful_reuse
```

Document reuse should also reduce repeated system-prompt token traffic, not only session creation.

---

## D4 — Immersive Translate live acceptance test: long article

Use a long article large enough to produce many translation batches, preferably with enough initial concurrency to exercise cold-start races.

Exit criteria:

- one-lane mode creates only one initial document session despite concurrent first requests;
- queued batches continue reusing the bound session;
- queue wait is observable and bounded;
- if one-lane translation causes client timeout, test a bounded two-lane policy and compare completion time/context consistency;
- session/harness count remains bounded by lane + worker lifecycle limits;
- after switching articles, the old binding expires or is evicted according to policy rather than growing forever.

---

# Phase E — P3: Remaining TTFT/Serialization Micro-Optimizations

Only return to these after lifecycle safety and document reuse are proven.

Candidates retained from the earlier plan:

- single-pass outbound ACP JSON encoding;
- reduce repeated session-update decoding;
- other allocation/copy reductions shown to matter by corrected stage timings.

Do not prioritize these while backend generation or session/harness lifecycle dominates the user-visible result.

---

# Implementation Order

## Step 1 — Process-tree cleanup

Implement process-group spawn/termination and prove a worker recycle removes `agy_acp_server.par` plus every harness child.

**Exit criterion:** fake child-process test is deterministic and green.

## Step 2 — Worker session ledger + hard caps + draining

Add total/abandoned accounting, prepared-session integration, draining selection rules, and recycle behavior.

**Exit criterion:** a synthetic stateless loop can run indefinitely without exceeding the configured per-worker session bound.

## Step 3 — Generic document-affinity core

Add document scope parsing, explicit id/prompt-marker extraction, canonical keying, binding table, per-document singleflight/serialization, incremental user-batch prompting, cancellation semantics, and logs.

**Exit criterion:** 50 sequential batches with one document key perform one `session/new` in one-lane mode.

## Step 4 — Immersive Translate prompt/header configuration

Add the static opt-in headers and title marker to the personal client configuration. Keep title parsing fallback available for A/B testing.

**Exit criterion:** captured live requests expose a stable normalized document key without changing translation output format.

## Step 5 — YouTube soak test

Run long enough to exceed the previous ~25-minute failure window by a wide margin.

**Exit criterion:** harness/swap usage is bounded and translation remains responsive.

## Step 6 — Long article burst test

Validate cold-start singleflight, queueing, and optional bounded multi-lane behavior.

**Exit criterion:** article translation cannot recreate one-session-per-request growth even under burst concurrency.

## Step 7 — Add document rollover limits from measurement

Use observed context size, memory, TTFT, and translation consistency to choose practical defaults for idle TTL, max turns/tokens, lane count, and low-memory worker session caps.

**Exit criterion:** multi-hour video/very long document use remains bounded without unacceptable context degradation.

## Step 8 — Only then resume micro-optimization

If `backend_to_first_output_ms` still dominates, stop. Do not add complexity for sub-millisecond proxy savings.

---

# Non-Goals / Guardrails

- Do not reuse prompted sessions for arbitrary clients without explicit opt-in.
- Do not infer document identity globally from any random `Title:` text.
- Do not rely on title-only identity as a universal solution; it is an explicitly accepted personal-client fallback until a real URL/page id is available.
- Do not let article concurrency bypass resource limits by silently spawning unlimited lanes/sessions.
- Do not make `prepared-sessions: 0` the claimed fix for the leak; it only changes when `session/new` occurs.
- Do not assume a usable ACP `session/release` exists unless verified against the actual Antigravity daemon. If a reliable release/dispose RPC is discovered later, integrate it and reduce the need for worker recycling.
- Do not sacrifice stateless compatibility for clients that do not opt into strict/document reuse.
