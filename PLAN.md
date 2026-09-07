# ACP Session Lifecycle, Page/Document Affinity, and TTFT Plan

## Goals

1. Keep warm-path ACP TTFT low.
2. Make ACP daemon/session/harness resource use **bounded over time**, including for fully stateless traffic.
3. Preserve ordinary stateless OpenAI-compatible behavior unless a client explicitly opts into reuse.
4. Keep strict stateful conversation reuse for clients that can provide a stable logical session id, monotonic turn number, and full-history recovery source.
5. Add a separate **page/document-affinity** mode for segmented translation workloads such as Immersive Translate.
6. Treat one browser tab/document title as the primary affinity unit for the personal Immersive Translate integration, so the same mechanism covers YouTube subtitles and ordinary long articles instead of implementing separate video/article routing.
7. Prevent article burst concurrency from recreating one-session-per-request growth.
8. Record the exact Immersive Translate prompt/header changes required from the user before live testing.

The target behavior is:

```text
first translation batch for page/document D
  -> derive page/document key from the expanded title context
  -> create or consume one fresh ACP session
  -> send normal full system prompt + current batch
  -> bind D -> worker + ACP session

later batch for the same page/document D
  -> same key
  -> reacquire owning worker
  -> reuse same ACP session
  -> send only the new user translation batch
  -> no session/new

switch to page/document E
  -> bootstrap a different binding/session
```

A long-running document may eventually roll over to a new session, but rollover must participate in the same hard worker/session lifecycle budget so old `localharness_external` processes cannot accumulate forever.

---

# 1. Current Evidence and Design Consequences

## 1.1 Production resource-growth result

The 2026-09-07 Immersive Translate YouTube test showed:

- one request roughly every 25-30 seconds;
- almost every stateless request consumes or creates one new ACP session;
- observed ACP sessions correspond closely to new `localharness_external` children;
- old harnesses did not disappear while the daemon remained alive;
- each old harness retained roughly 40-63 MiB swap in the tested 1 GiB RAM / 2 GiB swap VPS;
- roughly 37 harnesses consumed about 1.9 GiB swap and produced swap thrashing, high load, TTFT degradation, and 499 timeouts;
- `prepared-sessions: 1` bounds only the ready-session cache, not the number of sessions/harnesses historically created inside the daemon;
- `prepared-sessions: 0` removes refill-created sessions but does not fix the underlying one-`session/new`-per-stateless-request lifecycle.

Therefore resource lifecycle remains P0 even if page/document reuse later makes Immersive Translate itself much cheaper.

## 1.2 Immersive Translate identity evidence

Captured requests establish:

- headers contain no current page URL, YouTube video id, Referer, or other dynamic page identifier;
- the extension `Origin` identifies the client class, not the current page;
- every request contains title context produced by Immersive Translate's system-prompt variables;
- the observed expanded prompt contains:

```text
## Context Awareness
Document Metadata:
Title: "<browser tab/page title>"
```

- subtitle item ids such as `p0`-`p7` restart on every batch and are not usable as cross-request turns;
- Immersive Translate allows the user to customize system prompts and request headers.

Important correction to the earlier plan:

> Do not require a direct `{{imt_title}}` variable.

The user-confirmed prompt variables include constructs such as:

```text
{{title_prompt}}
{{summary_prompt}}
{{terms_prompt}}
{{imt_style_guide}}
```

The observed `{{title_prompt}}` expansion already contains the browser-tab/page title. The integration should therefore wrap and parse `{{title_prompt}}` instead of depending on a lower-level title variable that may not be directly exposed in the configured prompt UI.

## 1.3 Core simplification

Do **not** design separate primary affinity mechanisms for:

- YouTube video subtitles;
- normal web articles;
- long-form page translation.

For the personal integration, all are simply segmented translations of the current browser page/document:

```text
same normalized browser-tab/page title
+ same translation semantic configuration
+ binding still alive/in TTL
= same document-affinity ACP session
```

YouTube-specific handling is only an optional normalization detail for unstable tab-title decoration. It is not the core routing model.

---

# 2. P0 — Bound ACP Daemon / Session / Harness Lifecycle

Document affinity dramatically reduces session creation for Immersive Translate, but arbitrary stateless clients must still be unable to exhaust the VPS.

## 2.1 Kill the whole ACP process tree

`Client.Close()` must have a reliable ownership contract over daemon descendants.

Linux target:

1. spawn each ACP daemon in its own process group;
2. close stdin / request graceful daemon shutdown first;
3. after the grace period, signal the process group rather than only the direct daemon PID;
4. escalate to group `SIGKILL` if required;
5. verify `localharness_external` descendants do not survive as orphans.

Conceptually:

```text
CLIProxyAPI
  -> ACP process group
       -> agy_acp_server.par
            -> localharness_external
            -> localharness_external
            -> ...

worker recycle
  -> terminate process group
  -> reclaim every daemon-side session/harness
```

Use small OS-specific process helpers so Unix process-group logic does not leak through the generic ACP client implementation.

### Tests

A fake ACP process should spawn a deliberately long-lived child. Verify:

- graceful `Close()` removes parent and child;
- forced cleanup removes parent and child;
- repeated close is idempotent;
- no zombie/orphan survives worker recycling.

## 2.2 Session lifecycle ledger per worker

Track daemon-side session pressure explicitly.

Conceptual state:

```go
type WorkerSessionStats struct {
    CreatedTotal  uint64
    Prepared      int
    BoundStrict   int
    BoundDocument int
    Abandoned     int
}
```

Every successful `session/new`, including background preparation, must be counted exactly once.

A session becomes `Abandoned` when CLIProxyAPI still keeps the daemon alive but no longer has a valid path to reuse that session, including:

- completed ordinary stateless request;
- dropped prepared session;
- strict-stateful binding TTL/LRU/invalidation;
- document binding TTL/LRU/rollover/invalidation;
- failed/canceled prompt where provider-side session mutation is ambiguous.

If a prepared session becomes a strict/document-bound session, transition its accounting state; do not count another session.

## 2.3 Hard session budgets and worker draining

Configuration shape:

```yaml
antigravity:
  max-sessions-per-worker: 0
  max-abandoned-sessions-per-worker: 0
```

`0` may initially retain backward compatibility until safe measured defaults are chosen.

For the low-memory test VPS, validate deliberately small limits first, e.g. 4-8 sessions per worker.

When a worker reaches a configured hard budget:

```text
active -> draining
```

A draining worker:

- does not refill prepared sessions;
- does not create unrelated fresh sessions;
- may continue already-bound strict/document sessions if reuse does not increase session count;
- is not selected for fresh stateless traffic;
- is recycled when another `session/new` would otherwise be required;
- releases its global worker slot only after process-tree cleanup completes.

For a single-worker VPS this gives bounded sawtooth behavior:

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

When the whole worker is retired, its ledger is discarded because all daemon-side sessions/harnesses are physically gone.

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

Do not make Linux `/proc` memory polling part of the first correctness mechanism. Deterministic session caps + process-tree teardown are the primary safety invariant; RSS/swap remains a production diagnostic.

---

# 3. Two Explicit Reuse Contracts

Do not conflate real conversational state with segmented document translation.

## 3.1 Strict stateful conversation mode

Existing contract:

```http
X-Session-ID: <stable logical conversation id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn index>
```

Properties:

- client sends full history as recovery source;
- proxy validates monotonic turn order;
- healthy hit reuses existing ACP session and sends only the newest turn;
- worker/session loss can bootstrap exactly from full request history.

Suitable for TranslateNow/chat-like clients.

## 3.2 Page/document-affinity mode

New contract:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Optional future field:

```http
X-ACP-Document-ID: <real URL/page/video/document id>
```

Properties:

- client sends only the current translation batch, not prior batches;
- proxy groups requests by a page/document identity extracted from the current request;
- proxy serializes/assigns ordering itself;
- healthy hit sends only the current user batch into the already-live ACP session;
- binding loss bootstraps a fresh session from the current request only;
- lost previous translation context is acceptable recovery behavior because the client did not supply reconstructable history.

`X-ACP-Session-Turn` is not required for document mode.

The static opt-in headers prevent accidental reuse for arbitrary OpenAI-compatible clients.

---

# 4. Page/Document Identity

## 4.1 Personal Immersive Translate rule

Primary initial identity source:

> the browser-tab/page title contained in the expanded `{{title_prompt}}` block.

The same page title therefore naturally groups:

- all batches of one YouTube subtitle translation;
- all batches of one normal article;
- all batches of a long page/document translation;
- other segmented translations generated from the same browser tab/page.

No `subtitle` versus `article` classifier is required for basic affinity.

## 4.2 Machine-readable marker around `{{title_prompt}}`

The user will later modify the Immersive Translate system prompt so CLIProxyAPI can locate the title context deterministically.

Recommended form:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]

{{summary_prompt}}{{terms_prompt}}{{imt_style_guide}}
```

If the user's existing system prompt has additional translation instructions, keep them unchanged around this block. Only replace the existing bare `{{title_prompt}}` occurrence with the wrapped version.

Important forwarding behavior:

1. CLIProxyAPI locates the marker block.
2. After Immersive Translate expands `{{title_prompt}}`, CLIProxyAPI parses the page title from the enclosed content.
3. CLIProxyAPI removes **only the two marker delimiter lines**.
4. The actual expanded `title_prompt` content remains in the system prompt forwarded to ACP/model.

Therefore the marker supplies routing metadata without removing useful title context from the model and without duplicating `{{title_prompt}}`.

Expected expanded shape:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
## Context Awareness
Document Metadata:
Title: "Some Page Title"
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Proxy routing key material:

```text
Some Page Title
```

Model-visible text after marker removal:

```text
## Context Awareness
Document Metadata:
Title: "Some Page Title"
```

## 4.3 Exact user-side changes to perform later

Record these now so the live-test step is reproducible.

### Request headers

In the Immersive Translate custom OpenAI-compatible service, add:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

These can be static headers. No dynamic title/page value needs to be interpolated into a header.

### System prompt

Find the existing use of:

```text
{{title_prompt}}
```

and change it to:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Keep the other existing variables, including for example:

```text
{{summary_prompt}}
{{terms_prompt}}
{{imt_style_guide}}
```

in their existing semantic positions.

A minimal combined template is:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

Do **not** make the user add a separate `{{imt_title}}` dependency unless a later test proves that variable is directly available and more reliable in the relevant Immersive Translate prompt configuration.

### Migration/fallback

For initial A/B testing, CLIProxyAPI may also parse the old unmarked expanded form:

```text
Document Metadata:
Title: "..."
```

but only when document scope is explicitly opted in (or behind a dedicated personal-client compatibility flag).

Once the wrapped prompt is confirmed working, the explicit marker should be preferred because it avoids accidentally interpreting unrelated `Title:` text inside arbitrary prompts.

## 4.4 Canonical key

Do not use the raw title alone as a global map key.

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

This prevents a page translated with different language/model/prompt semantics from accidentally inheriting incompatible ACP context.

## 4.5 Title normalization

Core behavior should be generic and conservative:

- Unicode/ASCII trim;
- normalize obvious surrounding whitespace;
- optionally collapse clearly irrelevant repeated whitespace.

Do **not** define YouTube video-title extraction as the identity algorithm.

The captured value such as:

```text
(132) <page/video title> - YouTube
```

is a **browser tab title**.

A known site may decorate that tab title with volatile UI state. For example, YouTube may prepend a changing notification counter such as `(132) `.

Optional client/site-profile normalization may remove only known volatile decoration such as:

```regex
^\(\d+\)\s*
```

for YouTube pages, so notification-count changes do not split a session.

The stable ` - YouTube` suffix does not have to be removed; retaining it can actually help namespace otherwise-similar titles. Site-specific normalization is an optional refinement, not a prerequisite for document affinity.

## 4.6 Collision limitations

Two unrelated pages can share the same tab title (`Home`, `Index`, etc.).

For this personal integration, the following combination is initially acceptable:

```text
title + auth/client namespace + model/language/config fingerprint + idle TTL
```

because it drastically reduces practical collision risk and the user controls the endpoint.

Long term preference order:

1. real explicit `X-ACP-Document-ID` derived from URL/page/video/document identity;
2. a future prompt variable exposing URL/page id;
3. title-based affinity as the personal compatibility fallback.

Never claim title-only identity is universally collision-free.

---

# 5. Reusing the ACP Session

## 5.1 First batch / miss

For a new page key:

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

This should both:

- stop one-harness-per-translation-batch growth;
- preserve useful cross-batch context such as terminology/person names/tone;
- avoid repeatedly sending the same system/title/summary/terms prompt into the model context.

Strict conversation mode and document mode may share incremental newest-user extraction helpers, but their validation/recovery semantics must remain separate.

---

# 6. Concurrency: Long Articles Must Not Fan Out Sessions

The article case is part of the first design, not a future edge case.

## 6.1 Cold-start singleflight per document

A long page may cause many batches to arrive nearly simultaneously.

Without a per-key gate:

```text
10 requests
 -> all see binding miss
 -> 10 session/new
 -> 10 harnesses
```

Required invariant in one-lane mode:

> Only one request may bootstrap a given document key.

Example:

```text
A(doc D) -> acquire document gate -> session/new -> bind D
B(doc D) -> wait
C(doc D) -> wait
B/C       -> observe binding -> reuse same session
```

Different page/document keys use independent gates.

## 6.2 Serialize prompts within one lane

An ACP session is one ordered context. Requests routed to the same document lane must prompt it serially.

If the client has no cross-request sequence id, arrival/gate order is the best available ordering.

## 6.3 Bounded queue/backpressure

Do not handle queue pressure by silently bootstrapping more sessions without limit.

Use a bounded queue per document/lane and expose queue-wait metrics.

If the queue limit is exceeded, return a retryable 429/503 (or another explicitly chosen backpressure behavior) rather than generating unbounded ACP sessions.

## 6.4 Optional bounded multi-lane mode

One lane is the conservative default because it maximizes context consistency and minimizes harness count.

For large articles, one lane may be too slow if individual ACP generations take several seconds and the extension emits many batches in parallel.

Optional configuration:

```yaml
antigravity:
  document-session-max-lanes: 1
```

If measurement later justifies >1:

- create additional lanes only under queue pressure;
- never exceed the configured lane cap;
- each lane is a real ACP session and consumes the worker session budget;
- route new batches to the least-loaded existing lane;
- each lane keeps context only for its own subset of batches;
- resource safety wins over throughput.

Do not require explicit `subtitle`/`article` markers in v1. If later measurements justify different default lane policies, workload classification can be added separately.

---

# 7. Duplicate / Retry Protection

Document mode lacks the strict client's monotonic turn index.

A timed-out browser request may be retried after ACP already processed it, which could append the same batch twice.

Compute a bounded short-lived fingerprint from at least:

```text
document key
+ normalized newest-user batch
+ resolved model/config fingerprint
```

Maintain a small recent fingerprint/response cache per document binding.

Desired behavior:

- concurrent identical requests coalesce when practical;
- a recently completed identical retry returns the cached translated result instead of appending another ACP turn;
- cache size and TTL remain bounded;
- fingerprints never dedupe across document keys.

---

# 8. Binding Lifetime and Very Long Documents

One session per current page fixes harness growth but can make a multi-hour video or very large article accumulate a large ACP/model context.

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

Configuration shape to evaluate after measurement:

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
3. same page/document key points to the new session;
4. P0 hard session budgets ensure abandoned old harnesses cannot accumulate indefinitely before worker recycle.

Do not add automatic context summarization/carry-forward in v1. First make rollover safe and measurable.

---

# 9. Cancellation and Failure Semantics

Document mode uses conservative provider-state rules:

- canceled before `session/prompt` dispatch: keep binding;
- canceled/failed after prompt dispatch with ambiguous provider mutation: invalidate binding and mark session abandoned;
- ACP worker/transport death: invalidate all strict/document bindings owned by that worker;
- downstream formatting failure after ACP completed successfully: keep binding if provider-side state is known good;
- completed-but-lost response should be recoverable from the short-lived batch response cache when possible.

On document-binding loss, the next batch can always start a fresh session. Exact previous-context reconstruction is not promised because Immersive Translate did not send previous batches.

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

## 10.2 Document-affinity functional tests

1. First page request bootstraps exactly one ACP session.
2. Second request with same normalized title/config performs no `session/new`.
3. Hit sends only newest user translation batch.
4. Different title creates a different binding.
5. Same title under a different model/language/prompt semantic fingerprint does not reuse incompatible context.
6. Missing document opt-in stays stateless.
7. Prompt marker is removed while expanded `title_prompt` remains model-visible.
8. Unmarked `Document Metadata -> Title` fallback works only when explicitly enabled.
9. Binding TTL/rollover marks old session abandoned.
10. Cancellation after dispatch invalidates binding.
11. Worker death invalidates every binding on that worker.
12. Duplicate completed batch is not appended twice.
13. Concurrent identical batch coalesces when enabled.
14. Concurrent cold requests for one page create one session in one-lane mode.
15. Queue overflow never creates an unbounded fallback session.
16. Optional two-lane mode never creates lane 3.

## 10.3 Immersive Translate YouTube soak test

Repeat the original subtitle workload well beyond the previous ~25-minute collapse window.

Exit criteria:

- first batch logs `document_bootstrap`;
- later batches with the same page/tab title log `document_reuse`;
- `session/new` count remains one per active lane until intentional rollover/recycle;
- harness count no longer grows with subtitle batch count;
- swap reaches a bounded/stable range rather than monotonic growth;
- changing to another YouTube page/title creates a separate binding;
- if notification count `(N)` changes, optional YouTube decoration normalization prevents unnecessary session split;
- translation remains responsive for substantially longer than the original failure window.

Collect TTFT distributions for:

```text
fresh
prepared
document_bootstrap
document_reuse
stateful_reuse
```

## 10.4 Immersive Translate long-article burst test

Use a page large enough to generate many translation requests and enough initial concurrency to exercise a cold-start race.

Exit criteria:

- all batches with the same tab/page title group into the same document binding in one-lane mode;
- concurrent first requests produce one bootstrap, not N sessions;
- queued requests reuse the bound session;
- queue wait is observable and bounded;
- session/harness count remains bounded by lane + worker lifecycle limits;
- if one lane causes extension timeouts, compare an explicitly bounded two-lane configuration;
- switching to another article creates another binding while the old one eventually expires/evicts instead of remaining forever.

---

# 11. Implementation Order

## Step 1 — Process-tree cleanup

Implement process-group spawn/termination and prove worker recycle removes `agy_acp_server.par` and all harness descendants.

**Exit criterion:** deterministic fake child-process tests pass.

## Step 2 — Worker session ledger + hard caps + draining

Integrate fresh/prepared/strict/document session accounting and bounded worker recycling.

**Exit criterion:** indefinite synthetic stateless traffic cannot exceed configured daemon-side session pressure.

## Step 3 — Generic document-affinity core

Implement:

- document-scope header parsing;
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

**Exit criterion:** 50 sequential batches with one title/config perform one `session/new` in one-lane mode.

## Step 4 — User modifies Immersive Translate configuration

Apply the exact recorded changes from section 4.3:

Headers:

```http
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

Keep `{{summary_prompt}}`, `{{terms_prompt}}`, `{{imt_style_guide}}`, and the rest of the translation prompt semantics unchanged.

**Exit criterion:** live captured request produces a deterministic page-title key and unchanged translation output structure.

## Step 5 — YouTube soak test

Run well past the old failure horizon.

**Exit criterion:** bounded harness/swap usage and stable translation.

## Step 6 — Long article burst test

Validate title affinity, bootstrap singleflight, queueing, dedupe, and optional bounded lanes.

**Exit criterion:** article translation cannot recreate one-session-per-request growth under concurrency.

## Step 7 — Tune TTL / rollover / lane defaults from measurement

Choose practical values only after observing:

- context growth;
- translation consistency;
- queue wait;
- TTFT;
- harness count;
- RAM/swap behavior.

## Step 8 — Resume micro-optimization only if useful

Possible remaining work:

- single-pass outbound ACP JSON encoding;
- reduce repeated session-update decoding;
- other allocation/copy improvements justified by corrected TTFT stages.

If backend generation dominates, stop rather than adding complexity for insignificant proxy-side savings.

---

# 12. Guardrails / Non-Goals

- Do not auto-reuse prompted sessions for arbitrary clients without explicit opt-in.
- Do not treat document mode as an exactly recoverable conversation.
- Do not require a direct `{{imt_title}}` variable for the initial Immersive Translate integration.
- Do not split the core design into separate YouTube-video versus article affinity mechanisms; both are page/document title affinity.
- Do not interpret arbitrary `Title:` text globally; marker or explicit compatibility mode must gate parsing.
- Do not claim raw title alone is globally unique.
- Do not let article concurrency bypass worker/session budgets by spawning unlimited lanes.
- Do not claim `prepared-sessions: 0` fixes the session/harness lifecycle problem.
- Do not assume a usable ACP `session/release` exists unless verified against the actual Antigravity daemon.
- If a reliable daemon-side session dispose/release RPC is later discovered, integrate it and reduce reliance on whole-worker recycling.
- Preserve stateless compatibility for clients that do not opt into strict/document reuse.
