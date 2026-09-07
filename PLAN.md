# ACP Session Lifecycle, Page/Document Affinity, and TTFT Plan

## Goals

1. Keep warm-path ACP TTFT low.
2. Make ACP daemon/session/harness resource use **bounded over time**, including fully stateless traffic.
3. Preserve ordinary OpenAI-compatible stateless behavior by default.
4. Keep strict stateful conversation reuse for clients that can provide a stable logical session id, monotonic turn number, and full-history recovery source.
5. Add a separate **page/document-affinity** mode for segmented translation workloads such as Immersive Translate.
6. Use an explicit header contract as the primary, maintainable way to opt into document affinity.
7. Use the browser-tab/page title as the initial document identity for the personal Immersive Translate integration, so one mechanism covers YouTube subtitles and long articles.
8. Prevent article burst concurrency from recreating one-session-per-request growth.
9. Record the exact Immersive Translate header and prompt changes required before live testing.

Target behavior:

```text
Immersive Translate request
  -> explicit X-ACP-* headers select document-affinity semantics
  -> wrapped {{title_prompt}} provides page/document identity

first batch for document D
  -> create/consume one fresh ACP session
  -> send normal full system prompt + current batch
  -> bind D -> worker + ACP session

later batch for document D
  -> reacquire owning worker
  -> reuse same ACP session
  -> send only newest user translation batch
  -> no session/new

switch to document E
  -> bootstrap another bounded binding/session
```

A very long document may eventually roll over to a fresh session, but rollover must participate in the same hard worker/session lifecycle budget so old `localharness_external` processes cannot accumulate forever.

---

# 1. Current Evidence and Design Consequences

## 1.1 Production resource-growth result

The 2026-09-07 Immersive Translate YouTube test showed:

- one request roughly every 25-30 seconds;
- almost every stateless request consumes or creates one new ACP session;
- observed ACP sessions correspond closely to new `localharness_external` children;
- old harnesses did not disappear while the daemon remained alive;
- each old harness retained roughly 40-63 MiB swap in the tested 1 GiB RAM / 2 GiB swap VPS;
- roughly 37 harnesses consumed about 1.9 GiB swap and caused swap thrashing, high load, TTFT degradation, and 499 timeouts;
- `prepared-sessions: 1` bounds only the ready cache, not historically created daemon sessions/harnesses;
- `prepared-sessions: 0` removes refill-created sessions but still leaves one `session/new` per ordinary stateless request.

Therefore resource lifecycle is P0 even if document affinity later makes Immersive Translate itself much cheaper.

## 1.2 Immersive Translate request evidence

Captured requests show:

- no current page URL, YouTube video id, Referer, or dynamic page identifier in ordinary headers;
- Chromium extension requests currently expose:

```http
Origin: chrome-extension://amkbmndfnliijdhojkpoglbnaaahippg
Sec-Fetch-Site: none
```

- this `Origin` identifies the client class, not the current page;
- subtitle ids such as `p0`-`p7` restart for every batch and are not cross-request turns;
- expanded prompt context contains:

```text
## Context Awareness
Document Metadata:
Title: "<browser tab/page title>"
```

- Immersive Translate supports advanced custom request headers through `translationServices.<service>.headerConfigs`;
- the user can also modify the translation prompt.

## 1.3 Maintainability decision

The **primary supported integration must use explicit headers**, not extension fingerprint detection.

Reasoning:

- the proxy should route based on requested session semantics, not on which browser extension happened to send the request;
- extension ids/origins may differ across Chromium stores, Firefox, development builds, proxies, or future versions;
- an explicit protocol is easier to test, log, document, and reuse from other segmented-translation clients;
- `X-ACP-Session-Scope: document` directly expresses the intended behavior;
- `X-ACP-Client` can remain diagnostic/profile metadata rather than the source of core semantics.

Activation priority:

```text
1. explicit X-ACP document headers      <- primary/recommended
2. optional configured client fallback <- compatibility only
3. ordinary stateless behavior
```

The captured `Origin` may still be supported as an **optional compatibility fallback**, but must not be the normal implementation path.

## 1.4 Prompt-variable decision

Do not depend on a direct `{{imt_title}}` variable.

The user-confirmed variables include:

```text
{{title_prompt}}
{{summary_prompt}}
{{terms_prompt}}
{{imt_style_guide}}
```

The observed `{{title_prompt}}` expansion already includes the browser-tab/page title. Wrap and parse that block instead of depending on a lower-level variable that may not be exposed consistently in the prompt UI.

## 1.5 One affinity model for video and article

Do not design separate primary routing for:

- YouTube subtitle translation;
- ordinary web articles;
- long-form page translation.

For the personal integration they are all segmented translations of one current page/document:

```text
explicit document mode
+ same normalized browser-tab/page title
+ same translation semantic configuration
+ live binding
= same ACP document session
```

YouTube-specific handling is only optional normalization of volatile tab-title decoration.

---

# 2. P0 — Bound ACP Daemon / Session / Harness Lifecycle

Document affinity reduces session creation, but arbitrary stateless clients must still be unable to exhaust the VPS.

## 2.1 Kill the whole ACP process tree

`Client.Close()` must own daemon descendants reliably.

Linux target:

1. spawn each ACP daemon in its own process group;
2. request graceful shutdown first;
3. after grace expiry, signal the process group rather than only the daemon PID;
4. escalate to group `SIGKILL` when required;
5. verify `localharness_external` descendants cannot survive as orphans.

```text
CLIProxyAPI
  -> ACP process group
       -> agy_acp_server.par
            -> localharness_external
            -> localharness_external

worker recycle
  -> terminate process group
  -> reclaim every daemon-side session/harness
```

Keep OS-specific process handling behind small helpers.

### Tests

Use a fake ACP process that spawns a long-lived child and verify:

- graceful close removes parent and child;
- forced cleanup removes parent and child;
- repeated close is idempotent;
- no zombie/orphan survives worker recycling.

## 2.2 Per-worker session lifecycle ledger

Track daemon-side session pressure explicitly:

```go
type WorkerSessionStats struct {
    CreatedTotal  uint64
    Prepared      int
    BoundStrict   int
    BoundDocument int
    Abandoned     int
}
```

Every successful `session/new`, including background preparation, is counted exactly once.

A session becomes `Abandoned` when the daemon remains alive but CLIProxyAPI no longer has a valid path to reuse that session, including:

- completed ordinary stateless request;
- dropped prepared session;
- strict-stateful binding TTL/LRU/invalidation;
- document binding TTL/LRU/rollover/invalidation;
- failed/canceled prompt where provider-side mutation is ambiguous.

If a prepared session becomes a strict/document-bound session, transition its state rather than counting another session.

## 2.3 Hard session budgets and worker draining

Configuration shape:

```yaml
antigravity:
  max-sessions-per-worker: 0
  max-abandoned-sessions-per-worker: 0
```

`0` may initially preserve backward compatibility until measured defaults are chosen.

For the low-memory VPS, validate deliberately small limits first, e.g. 4-8 sessions per worker.

When a configured hard budget is reached:

```text
active -> draining
```

A draining worker:

- does not refill prepared sessions;
- does not create unrelated fresh sessions;
- may continue already-bound strict/document sessions when reuse does not increase session count;
- is not selected for fresh stateless traffic;
- is recycled when another `session/new` would otherwise be required;
- releases its worker slot only after process-tree cleanup completes.

For a single-worker VPS:

```text
spawn worker
 -> create bounded sessions
 -> drain
 -> terminate daemon + harness tree
 -> spawn replacement
```

Prepared sessions consume the same hard budget as any other session.

## 2.4 Binding eviction -> abandoned accounting

When a strict/document binding disappears while its daemon remains alive:

```text
bound session -> abandoned session
```

Causes include:

- TTL expiry;
- LRU eviction;
- model/config mismatch;
- page/document rollover;
- cancellation after prompt dispatch;
- ACP transport ambiguity/failure.

When the whole worker is retired, its ledger disappears because all daemon-side sessions/harnesses are physically gone.

## 2.5 Lifecycle observability

Add structured fields/counters such as:

```text
worker_sessions_created
worker_sessions_prepared
worker_sessions_bound_strict
worker_sessions_bound_document
worker_sessions_abandoned
worker_state=active|draining
worker_recycle_reason=session_cap|abandoned_cap|idle|dead|shutdown
```

Deterministic session caps + process-tree teardown are the first correctness mechanism. Linux RSS/swap polling remains diagnostic rather than part of the core safety rule.

---

# 3. Reuse Protocols

## 3.1 Strict stateful conversation mode

Existing contract:

```http
X-Session-ID: <stable logical conversation id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn index>
```

Properties:

- client sends full history as recovery source;
- proxy validates monotonic turns;
- healthy hit reuses the existing ACP session and sends only the newest turn;
- worker/session loss can bootstrap exactly from full history.

Suitable for TranslateNow/chat-like clients.

## 3.2 Page/document-affinity mode — primary explicit contract

Recommended request headers:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Semantics:

- `X-ACP-Session-Reuse: 1` permits reuse;
- `X-ACP-Session-Scope: document` selects document-affinity behavior;
- `X-ACP-Client: immersive-translate` is a diagnostic/profile label, not the core semantic switch;
- `X-ACP-Session-Turn` is not required;
- the client sends only the current translation batch, not previous batches;
- the proxy derives the document identity from the current request;
- a healthy hit sends only the current user batch into the live ACP session;
- binding loss starts a new session from the current request only.

Optional future field:

```http
X-ACP-Document-ID: <real URL/page/video/document id>
```

If available later, this should take precedence over title-based identity.

### Generic behavior

A different segmented-translation client can use the same mode:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: some-other-translator
```

No Immersive Translate-specific extension id is required in the generic routing path.

## 3.3 Optional compatibility fallback: known client fingerprint

For convenience only, CLIProxyAPI may optionally support a configured fallback such as:

```text
Origin == "chrome-extension://amkbmndfnliijdhojkpoglbnaaahippg"
```

This fallback may select the Immersive Translate document profile when explicit headers are absent.

Rules:

- disabled by default or clearly marked compatibility-only;
- configurable, not hard-wired into generic OpenAI routing;
- do not use browser `User-Agent` alone;
- do not use API-key value as the client detector;
- unknown extension origins remain stateless;
- explicit headers always win.

Possible config shape:

```yaml
antigravity:
  document-affinity:
    immersive-translate-auto-detect: false
    immersive-translate-origins:
      - "chrome-extension://amkbmndfnliijdhojkpoglbnaaahippg"
```

---

# 4. Page/Document Identity

## 4.1 Primary Immersive Translate identity

Use the browser-tab/page title contained in the expanded `{{title_prompt}}` block.

The same title naturally groups:

- all batches of one YouTube subtitle translation;
- all batches of one normal article;
- all batches of one long page/document translation.

No `subtitle` versus `article` classifier is required for basic affinity.

## 4.2 Machine-readable wrapper around `{{title_prompt}}`

The user will modify the Immersive Translate prompt so CLIProxyAPI can locate the title context deterministically.

Recommended form:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]

{{summary_prompt}}{{terms_prompt}}{{imt_style_guide}}
```

If the existing prompt has other instructions, keep them unchanged. Replace only the existing bare `{{title_prompt}}` occurrence with the wrapped version.

Forwarding behavior:

1. headers select document mode;
2. CLIProxyAPI locates the marker block;
3. after Immersive Translate expands `{{title_prompt}}`, parse the page title from the enclosed content;
4. remove **only the two marker delimiter lines**;
5. leave the actual expanded `title_prompt` content visible to ACP/model.

Expected expanded shape:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
## Context Awareness
Document Metadata:
Title: "Some Page Title"
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Routing key material:

```text
Some Page Title
```

Model-visible text after marker removal:

```text
## Context Awareness
Document Metadata:
Title: "Some Page Title"
```

## 4.3 Exact user-side Immersive Translate changes

These changes are **recommended and part of the primary integration**, not optional workarounds.

### A. Add custom request headers

In Immersive Translate Developer Settings -> Edit Full User Config, use the relevant service's `headerConfigs` to add:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Exact surrounding JSON/config shape should be taken from the active service configuration when this step is executed; do not assume the service key in advance.

### B. Wrap `{{title_prompt}}`

Change:

```text
{{title_prompt}}
```

to:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Keep variables such as:

```text
{{summary_prompt}}
{{terms_prompt}}
{{imt_style_guide}}
```

in their existing semantic positions.

Minimal combined example:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

Do not require a separate `{{imt_title}}` dependency unless later testing proves it is directly available and more reliable.

### C. Migration fallback

During initial A/B testing, CLIProxyAPI may parse the existing unmarked expanded form:

```text
Document Metadata:
Title: "..."
```

but only after the request has already explicitly selected document mode (or entered through a configured compatibility profile).

Once the wrapped prompt works, the explicit marker is preferred.

## 4.4 Canonical document key

Do not use raw title alone as a global key.

Conceptually:

```text
hash(
  auth namespace
  + client profile
  + scope=document
  + normalized page/document title
  + resolved model variant
  + source/target language when available
  + semantic translation-prompt/config fingerprint
)
```

This prevents incompatible model/language/prompt contexts from sharing one ACP session.

## 4.5 Title normalization

Keep core normalization conservative:

- trim surrounding Unicode/ASCII whitespace;
- normalize obvious irrelevant whitespace;
- avoid generic site-specific rewriting.

The captured value:

```text
(132) <page/video title> - YouTube
```

is a browser tab title.

A known site may decorate the tab title with volatile UI state. Optional site-profile normalization may remove only known volatile decoration, for example:

```regex
^\(\d+\)\s*
```

for YouTube notification-count prefixes.

The stable ` - YouTube` suffix does not need to be stripped; retaining it can help namespace otherwise similar titles.

YouTube normalization is a refinement, not the identity algorithm.

## 4.6 Collision limitations

Two unrelated pages can share titles such as `Home` or `Index`.

For the personal deployment, the following is initially acceptable:

```text
title + auth/client namespace + model/language/config fingerprint + idle TTL
```

Long-term identity preference:

1. explicit real `X-ACP-Document-ID`;
2. future prompt variable exposing URL/page id;
3. title-based affinity fallback.

Never claim title-only identity is universally collision-free.

---

# 5. Reusing the ACP Session

## 5.1 First batch / miss

For a new page key, send:

```text
normal full SYSTEM prompt
+ title/summary/terms/style context
+ current USER translation batch
```

Create or consume one fresh prepared ACP session.

Only bind the page/document key after the first prompt succeeds.

Log:

```text
session_mode=document_bootstrap
```

## 5.2 Later batch / hit

For a healthy binding with the same semantic key:

- reacquire the same worker/client that owns the ACP session;
- do not call `session/new`;
- do not replay the full system prompt;
- send only the newest user translation batch.

Log:

```text
session_mode=document_reuse
```

Expected benefits:

- stop one-harness-per-translation-batch growth;
- preserve cross-batch context such as terminology, names, and tone;
- avoid repeatedly appending the same system/title/summary/terms context.

Strict conversation and document mode may share incremental newest-user extraction helpers, but their validation/recovery semantics stay separate.

---

# 6. Concurrency — Long Articles Must Not Fan Out Sessions

## 6.1 Cold-start singleflight per document

A long page may generate many requests almost simultaneously.

Without a per-key gate:

```text
10 requests
 -> all see binding miss
 -> 10 session/new
 -> 10 harnesses
```

Required one-lane invariant:

> Only one request may bootstrap a given document key.

```text
A(doc D) -> acquire document gate -> session/new -> bind D
B(doc D) -> wait
C(doc D) -> wait
B/C       -> observe binding -> reuse same session
```

Different document keys use independent gates.

## 6.2 Serialize prompts within one lane

One ACP session is one ordered context. Requests routed to the same document lane must be prompted serially.

Without a client sequence id, arrival/gate order is the best available ordering.

## 6.3 Bounded queue/backpressure

Do not respond to queue pressure by creating unlimited fallback sessions.

Use a bounded queue per document/lane and expose queue-wait metrics.

If the queue is full, return a retryable 429/503 (or another explicit backpressure result) rather than spawning unbounded sessions.

## 6.4 Optional bounded multi-lane mode

One lane is the conservative default because it maximizes context consistency and minimizes harness count.

Optional configuration:

```yaml
antigravity:
  document-session-max-lanes: 1
```

If measurements justify >1:

- create extra lanes only under queue pressure;
- never exceed the configured lane cap;
- every lane is a real ACP session and consumes worker session budget;
- select the least-loaded existing lane;
- each lane retains context only for its own subset of batches;
- resource safety wins over throughput.

Do not require subtitle/article classification in v1.

---

# 7. Duplicate / Retry Protection

Document mode has no monotonic client turn.

A timed-out browser request may be retried after ACP already processed it, which can otherwise append the same batch twice.

Compute a bounded short-lived fingerprint from at least:

```text
document key
+ normalized newest-user batch
+ resolved model/config fingerprint
```

Maintain a small recent fingerprint/response cache per document binding.

Desired behavior:

- concurrent identical requests coalesce where practical;
- a recently completed identical retry returns the cached result instead of appending another ACP turn;
- cache size and TTL remain bounded;
- fingerprints never dedupe across document keys.

---

# 8. Binding Lifetime and Very Long Documents

One session per page fixes harness growth but can make a multi-hour video or huge article accumulate a large ACP/model context.

Track per lane:

```go
type DocumentBindingStats struct {
    Turns              uint64
    ApproxInputTokens  uint64
    ApproxOutputTokens uint64
    CreatedAt          time.Time
    LastUsedAt         time.Time
}
```

Configuration to evaluate after measurement:

```yaml
antigravity:
  document-session-idle-ttl: "0"
  document-session-max-turns: 0
  document-session-max-estimated-tokens: 0
  max-document-sessions: 0
```

Possible rollover causes:

- idle TTL expired;
- maximum turns reached;
- estimated context budget reached;
- model/language/system-prompt semantic fingerprint changed;
- cancellation/transport ambiguity;
- worker retirement.

Rollover sequence:

1. old binding/session becomes abandoned in the worker ledger;
2. current request bootstraps a fresh session;
3. the same document key points to the new session;
4. P0 hard session budgets ensure abandoned old harnesses cannot accumulate indefinitely.

Do not add automatic context summarization/carry-forward in v1. First make rollover safe and measurable.

---

# 9. Cancellation and Failure Semantics

Document mode uses conservative provider-state rules:

- canceled before `session/prompt` dispatch: keep binding;
- canceled/failed after dispatch with ambiguous provider mutation: invalidate binding and mark session abandoned;
- ACP worker/transport death: invalidate all bindings owned by that worker;
- downstream formatting failure after ACP completed successfully: keep binding if provider-side state is known good;
- completed-but-lost response should be recoverable from the short-lived batch-response cache when possible.

On document-binding loss, the next batch starts a fresh session. Exact previous-context reconstruction is not promised because Immersive Translate did not send prior batches.

---

# 10. Tests and Production Exit Criteria

## 10.1 Lifecycle regression tests

1. Stateless loops cannot exceed configured worker session cap.
2. Prepared refill stops before crossing a hard session cap.
3. Dropped prepared sessions enter abandoned accounting.
4. Strict/document binding eviction enters abandoned accounting.
5. Draining workers do not accept a new session.
6. Worker recycle kills daemon and child harness tree.
7. Global/per-auth worker caps remain correct during draining/replacement.
8. Race tests show no leaked worker slots or session ledger entries.

## 10.2 Document protocol activation tests

1. Explicit `X-ACP-Session-Reuse: 1` + `X-ACP-Session-Scope: document` activates document mode.
2. `X-ACP-Client` changes diagnostics/profile namespace but is not required to define document semantics.
3. Missing document headers stays stateless by default.
4. Explicit headers win over compatibility fingerprint settings.
5. Optional known-Origin fallback activates only when configured/enabled.
6. Generic browser `User-Agent` never activates document mode by itself.
7. Unknown extension origins remain stateless.

## 10.3 Document-affinity functional tests

1. First page request bootstraps exactly one ACP session.
2. Second request with same normalized title/config performs no `session/new`.
3. Hit sends only newest user translation batch.
4. Different title creates a different binding.
5. Same title with different model/language/prompt fingerprint does not reuse incompatible context.
6. Prompt marker is removed while expanded `title_prompt` remains model-visible.
7. Unmarked `Document Metadata -> Title` fallback works only for an explicitly selected/configured document client.
8. Binding TTL/rollover marks the old session abandoned.
9. Cancellation after dispatch invalidates binding.
10. Worker death invalidates every binding on that worker.
11. Duplicate completed batch is not appended twice.
12. Concurrent identical batch coalesces when enabled.
13. Concurrent cold requests for one page create one session in one-lane mode.
14. Queue overflow never creates an unbounded fallback session.
15. Optional two-lane mode never creates lane 3.

## 10.4 Immersive Translate YouTube soak test

Repeat the original subtitle workload well beyond the previous ~25-minute collapse window.

Exit criteria:

- configured Immersive Translate requests carry the explicit document headers;
- first batch logs `document_bootstrap`;
- later batches with the same tab title log `document_reuse`;
- `session/new` count remains one per active lane until intentional rollover/recycle;
- harness count no longer grows with subtitle batch count;
- swap reaches a bounded/stable range rather than monotonic growth;
- changing to another YouTube page/title creates a separate binding;
- if notification count `(N)` changes, optional YouTube decoration normalization avoids an unnecessary split;
- translation remains responsive well beyond the original failure window.

Collect TTFT distributions for:

```text
fresh
prepared
document_bootstrap
document_reuse
stateful_reuse
```

## 10.5 Immersive Translate long-article burst test

Use a page large enough to generate many translation requests and enough initial concurrency to exercise a cold-start race.

Exit criteria:

- all batches with the same tab/page title group into the same binding in one-lane mode;
- concurrent first requests produce one bootstrap, not N sessions;
- queued requests reuse the bound session;
- queue wait is observable and bounded;
- session/harness count remains bounded by lane + worker lifecycle limits;
- if one lane causes extension timeouts, compare an explicitly bounded two-lane configuration;
- switching to another article creates another binding while the old one eventually expires/evicts.

---

# 11. Implementation Order

## Step 1 — Process-tree cleanup

Implement process-group spawn/termination and prove worker recycle removes `agy_acp_server.par` plus all harness descendants.

**Exit criterion:** deterministic fake child-process tests pass.

## Step 2 — Worker session ledger + hard caps + draining

Integrate fresh/prepared/strict/document accounting and bounded worker recycling.

**Exit criterion:** indefinite synthetic stateless traffic cannot exceed configured daemon-side session pressure.

## Step 3 — Generic explicit document-affinity protocol

Implement:

- `X-ACP-Session-Reuse` parsing;
- `X-ACP-Session-Scope: document` routing;
- optional `X-ACP-Client` profile/diagnostic namespace;
- optional future `X-ACP-Document-ID` precedence;
- `title_prompt` marker extraction;
- page-title parsing;
- semantic canonical key;
- binding table;
- same-worker reacquisition;
- per-document bootstrap singleflight;
- serialized prompt queue;
- incremental user-batch prompting;
- cancellation/invalidation;
- document reuse logging.

**Exit criterion:** 50 sequential explicit-document requests with one title/config perform one `session/new` in one-lane mode.

## Step 4 — Configure Immersive Translate explicitly

Apply both user-side changes from section 4.3.

Headers:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Prompt wrapper:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Keep `{{summary_prompt}}`, `{{terms_prompt}}`, `{{imt_style_guide}}`, and other translation semantics unchanged.

**Exit criterion:** a live request explicitly selects document mode, produces a deterministic page-title key, and preserves translation output structure.

## Step 5 — Optional compatibility fallback

Only after the explicit path is working, optionally add configurable Immersive Translate `Origin` detection for convenience/backward compatibility.

This is not required for the primary deployment.

## Step 6 — YouTube soak test

Run well past the old failure horizon.

**Exit criterion:** bounded harness/swap usage and stable translation.

## Step 7 — Long article burst test

Validate title affinity, bootstrap singleflight, queueing, dedupe, and optional bounded lanes.

**Exit criterion:** article translation cannot recreate one-session-per-request growth under concurrency.

## Step 8 — Tune TTL / rollover / lane defaults

Choose practical values only after observing:

- context growth;
- translation consistency;
- queue wait;
- TTFT;
- harness count;
- RAM/swap behavior.

## Step 9 — Resume micro-optimization only if useful

Possible remaining work:

- single-pass outbound ACP JSON encoding;
- reduce repeated session-update decoding;
- other allocation/copy improvements justified by corrected TTFT stages.

If backend generation dominates, stop rather than adding complexity for insignificant proxy-side savings.

---

# 12. Guardrails / Non-Goals

- Do not auto-reuse prompted sessions for arbitrary clients.
- Explicit document headers are the primary/recommended integration contract.
- Treat Immersive Translate `Origin` recognition only as optional compatibility logic.
- Do not use generic browser `User-Agent` as sufficient identification.
- Do not make `X-ACP-Client` itself the semantic switch; `X-ACP-Session-Scope: document` defines the behavior.
- Do not treat document mode as an exactly recoverable conversation.
- Do not require direct `{{imt_title}}` for the initial Immersive Translate integration.
- Do not split the core design into separate YouTube-video and article affinity mechanisms.
- Do not interpret arbitrary `Title:` text globally; explicit document mode/recognized compatibility profile must gate parsing.
- Do not claim raw title alone is globally unique.
- Do not let article concurrency bypass worker/session budgets by spawning unlimited lanes.
- Do not claim `prepared-sessions: 0` fixes the session/harness lifecycle problem.
- Do not assume a usable ACP `session/release` exists unless verified against the real Antigravity daemon.
- If a reliable daemon-side session release/dispose RPC is discovered later, integrate it and reduce reliance on whole-worker recycling.
- Preserve stateless compatibility for clients outside explicit/recognized document profiles.
