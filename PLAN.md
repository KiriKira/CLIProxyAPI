# ACP Session Lifecycle, Page/Document Affinity, and TTFT Plan

## Goals

1. Keep warm-path ACP TTFT low.
2. Make ACP daemon/session/harness resource use **bounded over time**, including fully stateless traffic.
3. Preserve ordinary OpenAI-compatible stateless behavior by default.
4. Keep strict stateful conversation reuse for clients that provide a stable logical session id, monotonic turn number, and full-history recovery source.
5. Add a separate **page/document-affinity** mode for segmented translation workloads such as Immersive Translate.
6. Use an explicit header contract as the primary, maintainable opt-in to document affinity.
7. Use one stable page/document identity for both YouTube subtitles and long articles; do not build separate video/article session systems.
8. Prevent article burst concurrency, retries, lifecycle caps, LRU eviction, and worker recycling from recreating unbounded sessions, goroutines, stale bindings, or metadata maps.
9. Preserve forward progress and real OS-resource bounds on the intended low-memory `max-workers: 1` deployment.

Target behavior:

```text
Immersive Translate request
  -> explicit X-ACP-* headers select document-affinity semantics
  -> machine-readable page/document marker provides identity

first batch for document D
  -> create/consume one fresh ACP session
  -> send normal full translation context + current batch
  -> bind D -> worker + ACP session

later batch for document D
  -> reacquire owning worker
  -> revalidate binding after lease acquisition
  -> reuse same ACP session
  -> send only newest user translation batch
  -> no session/new

switch to document E
  -> bootstrap another bounded binding/session

worker reaches hard session pressure
  -> worker becomes draining
  -> existing bound sessions may continue while useful
  -> fresh-session demand MUST still make progress
  -> retire an idle draining worker when necessary
  -> purge all of its bindings through one unified retirement path
  -> physically terminate daemon + harness process tree
  -> only then return hard process capacity / spawn replacement
```

---

# 0. Current Review Gate — second review of implementation branch

Reviewed branch head:

```text
fb55bbe778c9b7916102a303d6a4b74aa4251e58
```

Commits reviewed since the previous review-plan commit `9450b2b28f1b2e88353c7e8f71df273ef3f7dd65`:

```text
a696f488  R1  guarantee forward progress at draining session cap
21152194  R4  finalize reuse binding before prompt build
26beb1f8  R2  bound and cancel document lane waiters
fb55bbe7  R3  gate YouTube title decoration normalization
```

## Status summary

- **R1 core forward-progress escape hatch: implemented, but NOT complete.** The single-worker draining-worker deadlock has a targeted fix, but one worker-removal path still bypasses binding purge, and hard process capacity is currently returned before process-tree teardown finishes.
- **R4 document full-vs-incremental prompt race: fixed.** Document hit/miss is now finalized before the prompt builder starts.
- **R4 strict-stateful equivalent: partially fixed.** Worker/binding revalidation moved before prompt build, but monotonic turn order is not rechecked after waiting for the worker; concurrent duplicate turns can still append twice.
- **R2 bounded/cancelable document lanes: core implementation is good.** Lane entries are reference-counted and garbage-collected, waiters are bounded and context-aware. HTTP-level retryable backpressure semantics still need to be made explicit/tested.
- **R3 title-prefix gate: local normalization function is fixed, but real document-key stability is NOT fixed yet.** The dynamic raw title in the system prompt still enters the semantic fingerprint, so `(132)` -> `(133)` can still change the final key in the real Immersive Translate prompt shape.
- **R5/R6/R7 live validation remains pending.** Real harness PGID/SID, direct `{{imt_title}}` probe, and semantic-key stability must still be checked before the long soak.

Do **not** start the final YouTube soak yet. Continue implementation with the review items below.

---

## R1a — BLOCKER: unify every worker-removal path through retirement hooks

The new `SetWorkerRetiredHook` correctly lets the executor purge strict/document bindings when workers are deliberately retired. However, every path that removes a worker from the pool must use that same retirement primitive.

One remaining cross-key eviction path in `Release()` still effectively does:

```text
remove worker
-> return global slot
-> close client asynchronously
```

when another auth/key has waiters and the global cap is full. That path does not currently run the worker-retired hook before removal/close.

This can leave binding-table entries pointing at a worker that has already disappeared from the pool/process layer.

### Required rule

> There must be one logical worker-retirement operation, and every non-dead removal path must go through it.

At minimum cover:

- forced idle-draining retirement for same-key fresh progress;
- cross-key oldest-idle eviction;
- `Release()` cross-key waiter eviction;
- idle-timeout retirement;
- pool shutdown where binding tables are still relevant;
- future administrative/limit eviction paths.

Preferred structure:

```text
select victim under pool lock
-> detach from routing
-> record retirement reason
-> notify/purge owner bindings exactly once
-> close/wait process tree outside pool lock
-> finish capacity handoff/wake waiters
```

Avoid copying slightly different `removeWorkerLocked + globalSlotRelease + closeClientAsync` sequences across call sites.

### Required tests

- Create a bound document session on worker A, fill global capacity, queue another auth/key, then release A so the `Release()` cross-key eviction path fires; document binding must be purged immediately.
- Same test for strict binding.
- Every worker-removal reason invokes the retired hook exactly once.
- No removal hook is invoked while holding locks in a way that can deadlock with binding-table -> worker accounting transitions.

---

## R1b — P0 resource invariant: do not return hard process capacity before process-tree cleanup completes

Current retirement removes the worker and returns the logical worker/global slot before `Client.Close()` has physically finished process-group cleanup. `Client.Close()` is intentionally non-blocking and its reaper may still spend the EOF grace period plus TERM/KILL escalation time reclaiming descendants.

On a 1 GiB VPS this permits a transient state such as:

```text
old daemon + old harnesses still alive
+
replacement daemon + new harness/session starts
```

That violates the strongest interpretation of the PLAN's resource bound and can create the exact short-lived RAM/swap spike that the lifecycle work is meant to prevent.

### Required decision/invariant

For configured **hard** worker/process limits, distinguish:

```text
routing slot        = worker may receive requests
process capacity    = daemon/process tree is physically alive
```

A retiring worker may leave routing immediately, but its hard process-capacity token should not become reusable until its owned process tree is confirmed gone.

Possible implementation:

- add a `treeDone`/`CloseAndWait(ctx)` style completion signal to the ACP client that closes only after EOF/TERM/KILL reaping is complete;
- keep a `retiring` process count/token in the pool;
- wake fresh acquirers after physical cleanup returns capacity;
- never hold pool locks while waiting for process exit.

If the project deliberately chooses to allow one bounded overlap during replacement, document and test that explicit bound instead of calling the process limit strict.

### Required tests

- `maxWorkers=1`: forced retirement cannot spawn replacement while fake old child tree is still deliberately alive inside the cleanup grace period, if strict mode is chosen.
- After cleanup completion replacement starts promptly.
- Repeated retire/spawn cycles never produce more concurrently live owned ACP process trees than the documented bound.

---

## R4a — Correctness: strict stateful turn order must be rechecked after worker acquisition

`acquireStatefulSession()` checks:

```text
incoming turn == LastTurn + 1
```

before `AcquireSpecific()` waits for the owning worker. After the wait, it performs an authoritative binding lookup, but does not re-check the turn number against the binding's now-current `LastTurn`.

Race example:

```text
binding LastTurn = 4
request A turn=5 -> precheck passes -> acquires worker
request B turn=5 -> precheck passes -> waits for same worker
A succeeds -> binding LastTurn becomes 5
B acquires worker -> worker/auth/model lookup still passes
B appends duplicate turn 5
```

### Required fix

After `AcquireSpecific()` succeeds and the authoritative binding is looked up again:

```text
if incomingTurn != binding.LastTurn + 1:
    release worker
    invalidate/fallback according to strict-stateful recovery semantics
    do NOT send incremental turn
```

The post-lease check is the authoritative sequence check. The earlier check remains useful as a fast rejection but cannot be the only one.

### Required tests

- Two concurrent requests with the same next turn: exactly one may continue incrementally; the second must not append the same turn.
- Concurrent turn N and N+1 serialize and produce the intended result under the chosen recovery semantics.
- Stream and non-stream strict paths behave consistently.

---

## R2a — Backpressure must surface as an explicit retryable HTTP result

`AcquireLane()` now has bounded waiters and returns `ErrDocumentLaneBusy` on overflow. This is the right internal primitive.

Do not leave the public behavior to generic error mapping. At the executor/handler boundary, map lane saturation to an explicit retryable response, for example:

```text
429 Too Many Requests
```

or deliberately chosen:

```text
503 Service Unavailable
```

The selected status should be stable/documented so Immersive Translate retries rather than treating queue pressure as a permanent translation failure.

Also make cancellation deterministic: if context is already canceled when lane ownership becomes available, do not unnecessarily run the prompt just because Go `select` happened to choose the token branch.

### Required tests

- lane overflow produces the chosen HTTP status, not generic 500;
- canceled waiter never reaches `session/new`/`session/prompt`;
- the historical-key GC test uses genuinely distinct document keys, not one repeated key.

---

## R3a / R7 — BLOCKER before soak: remove dynamic document context from the semantic fingerprint

The local title normalizer now correctly gates YouTube notification-prefix removal to titles ending in ` - YouTube`.

However, the final document key is approximately:

```text
hash(
  auth
  + client
  + normalized identity
  + model/source format
  + documentSemanticFingerprint(cleaned request)
)
```

`documentSemanticFingerprint()` retains system/developer messages. In the real Immersive Translate integration, `{{title_prompt}}` normally lives in the **system prompt**, so the raw title text remains inside the semantic fingerprint even after the separate identity has been normalized.

Therefore these two requests can still produce different final keys:

```text
Title: "(132) Example video - YouTube"
Title: "(133) Example video - YouTube"
```

because the normalized identity matches but the system-message semantic hash changes.

The current R3 unit test does not catch this because its marker/title is placed in a user message, and user messages are excluded from the semantic projection.

### Required architectural rule

> Document identity and dynamic document context must not be hashed again as if they were stable translation configuration.

Before hashing semantic configuration, replace/remove the machine identity block and other verified volatile document-derived context from the projection.

Conceptually split:

```text
DocumentKey = hash(
  auth/client namespace
  + normalized document identity
  + model/source-target language
  + stable translation semantics
)

stable translation semantics != full system prompt bytes
```

Likely stable semantic material includes:

- fixed translation instructions;
- chosen source/target language where available;
- model/effort variant;
- user style configuration when it actually changes translation semantics.

Potentially volatile document material includes:

- page title / notification count;
- page summary/theme generated from current page;
- extracted terms if they are generated/refined dynamically across batches.

Do not blindly exclude summary/terms yet: first capture safe component hashes over 20-50 real batches and confirm whether they vary. But the title machine block is already known document identity and should not be duplicated into the semantic hash.

### Required tests

Use a **real-shaped system prompt fixture**:

```text
SYSTEM:
  stable translation instructions
  [[CLIPROXY_ACP_TITLE_PROMPT:v1]]
  Document Metadata:
  Title: "(132) Example video - YouTube"
  [[/CLIPROXY_ACP_TITLE_PROMPT]]
  summary/terms/style...

USER:
  current p0..p7 translation batch
```

Then change only `(132)` -> `(133)` and assert the final document key stays identical.

Also assert:

- changing a genuinely stable translation instruction changes key;
- changing model/language changes key;
- ordinary `(1) Introduction` and `(2) Introduction` remain different identities;
- any observed summary/terms variation follows the policy chosen after measurement.

---

## R6 — Identity robustness: prefer direct `{{imt_title}}` marker if live probe succeeds

Before the live soak, test one real Immersive Translate request with:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

If `{{imt_title}}` expands to the real current page title in the user's active version/configuration:

- use this marker as the primary document identity path;
- strip the entire machine identity block before forwarding to ACP;
- keep ordinary `title_prompt` untouched/model-visible;
- stop depending on localized `Title:` parsing for the primary path.

If it does not expand reliably, retain wrapped `{{title_prompt}}` as the compatibility path:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

Either way, R3a's semantic projection rule still applies: machine routing identity must not destabilize the semantic hash.

---

## R5 — Validation gate: verify the real harness remains in the owned process group

The fake Linux process-tree tests prove cleanup when descendants inherit the ACP daemon process group. They do not prove the real `localharness_external` never calls `setsid()` / `setpgid()`.

On the VPS inspect:

```text
PID  PPID  PGID  SID  COMMAND
```

for CLIProxyAPI, `agy_acp_server.par`, and several `localharness_external` children.

Then force a controlled worker retirement and verify:

- every harness PID owned by that worker disappears;
- no detached child remains;
- repeated retire/spawn cycles do not accumulate old harnesses;
- swap/RSS returns toward a stable bounded range.

If harnesses detach, process-group kill is not sufficient; evaluate cgroup/process-tree containment only after confirming that fact.

---

## R9 — Lifecycle consistency: do not LRU/TTL-abandon a binding while its session is actively leased

The strict/document binding tables may evict an LRU binding and call `worker.AbandonSession(sessionID)`. A long-running request can still be actively using that bound session while other documents/sessions push the table over its max size.

Possible failure sequence:

```text
request D looks up binding -> worker/session leased -> D prompt running
many other bindings are inserted
-> D becomes LRU and is evicted
-> worker ledger deletes D session as abandoned
D prompt later succeeds
-> executor re-binds same session ID
-> worker.BindSession may not restore accounting because ledger entry was deleted
```

This can leave binding-table state and worker session ledger inconsistent, and a draining worker could become recyclable while a logically live binding exists.

### Required rule

Binding eviction/expiry must distinguish **idle binding** from **actively leased binding**.

Options:

- pin/refcount a binding while its worker/session is leased;
- defer eviction until the active turn releases;
- mark eviction pending and finalize it after lease completion.

Do not solve this by silently recreating a ledger record for an unknown provider-side session unless its lifecycle is provably owned and counted.

### Required tests

- force a very small binding-table max, keep one binding actively leased, insert enough others to trigger LRU pressure; active binding must not disappear/account as abandoned until release.
- after deferred eviction, binding and worker ledger transition exactly once.
- repeat for strict and document tables or factor a shared invariant.

This is less likely in the personal one-video workload than R1/R3, but it should be fixed before calling the general lifecycle implementation complete.

---

# 1. Current Architecture Decisions

## 1.1 Explicit document protocol

Primary request contract:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

`X-ACP-Session-Scope: document` defines semantics. `X-ACP-Client` is a profile/logging namespace, not the semantic switch.

Optional stronger identity:

```http
X-ACP-Document-ID: <stable page/video/document id>
```

Activation priority:

```text
1. explicit document headers            <- primary/recommended
2. optional configured client fallback <- compatibility only
3. ordinary stateless behavior
```

Do not hard-wire generic document affinity to one extension Origin.

## 1.2 Strict stateful conversation

Existing contract:

```http
X-Session-ID: <stable logical conversation id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn index>
```

Strict mode has reconstructable full-history recovery and monotonic turn semantics. Document mode does not; do not merge their validation rules even if they share worker/session helpers.

## 1.3 Lifecycle accounting

Track every successful `session/new` exactly once:

```go
type WorkerSessionStats struct {
    CreatedTotal  uint64
    Prepared      int
    BoundStrict   int
    BoundDocument int
    Abandoned     int
}
```

A session becomes abandoned only when the daemon remains alive but the proxy no longer has a valid reusable path to it. Prepared -> leased -> bound transitions do not double-count creation.

Hard budgets:

```yaml
antigravity:
  max-sessions-per-worker: 0
  max-abandoned-sessions-per-worker: 0
```

For the low-memory VPS, validate small values such as 4-8 first.

---

# 2. Document Identity and Immersive Translate Configuration

## 2.1 Identity preference order

```text
1. X-ACP-Document-ID when a true stable id exists
2. direct machine marker containing {{imt_title}} after live verification
3. wrapped {{title_prompt}} parser
4. unmarked Document Metadata/Title parsing only as migration compatibility inside explicit document mode
```

## 2.2 User-side headers

Configure Immersive Translate `headerConfigs` for the active translation service:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

## 2.3 Preferred prompt if direct variable probe succeeds

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

## 2.4 Fallback prompt

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

The proxy removes machine routing delimiters/identity metadata as designed, while keeping normal human/model context visible.

## 2.5 Conservative title normalization

Core normalization only trims/collapses clearly irrelevant whitespace.

For known YouTube tab titles ending in:

```text
 - YouTube
```

it may remove a leading volatile notification counter:

```regex
^\(\d+\)\s*
```

Never apply that rewrite globally.

---

# 3. Document Reuse Semantics

## 3.1 Bootstrap

For a new document key:

```text
full stable SYSTEM/developer translation instructions
+ current document context available in that request
+ current USER translation batch
```

Create/consume one fresh ACP session and bind only after `session/prompt` succeeds.

Log:

```text
session_mode=document_bootstrap
```

## 3.2 Hit

For a live binding:

- serialize on the document lane;
- reacquire the same worker;
- authoritatively revalidate binding after lease acquisition;
- do not call `session/new`;
- send only the newest user translation batch;
- do not replay system/title/summary/terms context.

Log:

```text
session_mode=document_reuse
```

If binding/worker is gone, bootstrap from the current full document request. Exact previous document context reconstruction is not promised.

---

# 4. Concurrency, Backpressure, and Retry

## 4.1 One-lane default

Concurrent cold requests for one document must singleflight into one bootstrap session in one-lane mode.

Production lane requirements:

- one holder per document lane;
- context-aware wait;
- bounded waiters;
- explicit retryable overflow result;
- lane entry garbage-collected after final holder/waiter/reference leaves;
- no ABA release race.

## 4.2 Optional multi-lane

Do not implement until one-lane live measurements prove necessary.

Possible later config:

```yaml
antigravity:
  document-session-max-lanes: 1
```

Every extra lane is a real ACP session and consumes the worker session budget. Never create an unbounded overflow lane.

## 4.3 Duplicate/retry protection

Before production timeout/retry testing, add a bounded recent request fingerprint/response cache:

```text
document key
+ normalized newest-user batch
+ relevant model/config identity
```

Desired behavior:

- concurrent identical requests coalesce where practical;
- recently completed retry returns cached result rather than appending same batch again;
- cache size/TTL are bounded;
- no cross-document dedupe.

---

# 5. Very Long Documents

A document session can eventually accumulate excessive context. Track at least:

```go
type DocumentBindingStats struct {
    Turns              uint64
    ApproxInputTokens  uint64
    ApproxOutputTokens uint64
    CreatedAt          time.Time
    LastUsedAt         time.Time
}
```

Evaluate after the basic soak:

```yaml
antigravity:
  document-session-idle-ttl: "0"
  document-session-max-turns: 0
  document-session-max-estimated-tokens: 0
  max-document-sessions: 0
```

Rollover abandons the old binding/session, bootstraps the current request, and remains bounded by worker lifecycle caps. Do not add automatic summary carry-forward in v1.

---

# 6. Failure Semantics

- canceled before prompt dispatch -> keep an existing binding;
- canceled/failed after dispatch with ambiguous provider mutation -> invalidate binding/session;
- worker/transport death -> purge every strict/document binding on that worker;
- forced idle draining-worker retirement -> purge owned bindings and let clients bootstrap;
- downstream formatting failure after a known-successful ACP prompt -> keep binding when provider state is known good;
- completed-but-lost retry -> recover from bounded response cache when available.

---

# 7. Required Tests / Acceptance

## 7.1 Lifecycle

- stateless loop remains bounded by configured worker session pressure;
- prepared refill cannot cross cap;
- prepared/bound transitions do not double-count;
- same-key single-worker cap scenario always makes forward progress;
- all cross-key/same-key retirement paths purge bindings through one hook;
- hard process capacity is not silently returned while old owned process tree still lives, unless a documented bounded-overlap policy is chosen;
- forced retirement never kills an `inUse` prompt;
- active binding cannot be LRU-abandoned while leased.

## 7.2 Strict stateful

- post-lease authoritative turn recheck prevents concurrent duplicate next-turn append;
- late invalidation bootstraps full history;
- stream/non-stream behavior agrees.

## 7.3 Document identity

- explicit headers activate document mode; missing headers stay stateless;
- direct `imt_title` marker works if supported by the live plugin configuration;
- fallback parser handles observed quoting/localization;
- real-shaped system-title fixture `(132)` -> `(133)` yields same key for YouTube;
- ordinary numbered titles remain distinct;
- changing stable translation semantics/model/language changes key.

## 7.4 Document concurrency

- 10 simultaneous cold same-document requests -> one session in one-lane mode;
- canceled waiter never prompts;
- queue overflow returns documented 429/503;
- 1000 genuinely distinct historical document keys do not leave lane entries;
- different documents can proceed subject to worker limits.

## 7.5 Real process containment

On VPS:

- inspect daemon/harness PID/PPID/PGID/SID;
- force retire and verify every owned harness disappears;
- repeat several cycles and verify no orphan accumulation;
- verify RAM/swap reaches a bounded range.

## 7.6 Semantic-key probe

For one YouTube page and one long article, log only safe component hashes for 20-50 batches:

- normalized document identity;
- stable semantic projection hash;
- optional title/summary/terms component hashes for diagnostics.

No raw prompt text or sensitive content is required in these diagnostics.

Exit criterion: repeated batches do not generate new document keys merely because volatile document context changed.

---

# 8. Revised Implementation Order After Second Review

## Step 1 — R1a: unify all worker retirement/removal paths

Fix the remaining `Release()` cross-key eviction path and factor a single retirement primitive.

**Exit criterion:** every worker removal that can invalidate bindings triggers the same binding-purge lifecycle exactly once.

## Step 2 — R1b: enforce/document physical process-capacity handoff

Do not let a logical slot imply the old process tree is already gone.

**Exit criterion:** replacement-process overlap never exceeds the documented hard bound.

## Step 3 — R4a: recheck strict turn sequence after worker lease

**Exit criterion:** concurrent duplicate next-turn requests cannot append twice.

## Step 4 — R3a + R7: separate document context from stable semantic fingerprint

Use real-shaped **system prompt** fixtures. Ensure YouTube notification counter changes do not alter the final key.

**Exit criterion:** `(132)` -> `(133)` on same YouTube page stays one document key/session while genuine translation-config changes rotate key.

## Step 5 — R2a: expose deterministic retryable lane backpressure

Map lane saturation to documented 429/503 and tighten canceled-acquire behavior/test fixture quality.

## Step 6 — R6: one real `{{imt_title}}` probe

Prefer direct machine title marker if it expands correctly; otherwise keep wrapped `title_prompt` fallback.

## Step 7 — R5: real VPS process-group/cleanup validation

Inspect PGID/SID and force a controlled recycle.

## Step 8 — R9: active-binding eviction/ledger consistency

Pin/defer eviction for actively leased strict/document bindings.

This can follow the immediate personal YouTube blockers, but must be complete before declaring the general lifecycle implementation finished.

## Step 9 — Short semantic/request sampling

Capture 20-50 safe hashes from one video and one article and finalize semantic projection policy.

## Step 10 — Configure Immersive Translate

Headers:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Use the direct `imt_title` marker if Step 6 succeeds; otherwise use wrapped `title_prompt`.

## Step 11 — YouTube soak

Run well past the previous ~25-minute collapse window with hard session limits enabled.

Exit criteria:

- first batch is `document_bootstrap`, subsequent same-page batches are `document_reuse`;
- `session/new` does not grow with subtitle batch count except deliberate rollover/recycle;
- harness count and swap remain bounded;
- switching videos still makes progress when worker is at cap;
- no title-counter key churn.

## Step 12 — Retry dedupe + long-article burst

Add/verify duplicate response protection, then stress cold concurrent article batches and bounded queueing.

## Step 13 — Tune TTL / rollover / lane / worker-cap defaults

Choose from measured context growth, TTFT, queue wait, harness count, RAM, and swap.

## Step 14 — Optional Origin compatibility detection

Only after explicit protocol is stable. Keep secondary/configurable.

## Step 15 — Micro-optimization only if TTFT data justifies it

Possible work:

- single-pass outbound ACP JSON encoding;
- reduce repeated session-update decoding;
- other measured allocation/copy reductions.

Do not optimize tiny proxy overhead while backend generation dominates.

---

# 9. Guardrails / Non-Goals

- Do not auto-reuse sessions for arbitrary clients.
- Explicit document headers remain the primary/recommended integration contract.
- `X-ACP-Session-Scope: document` defines semantics; `X-ACP-Client` does not.
- Do not let a draining bound worker block fresh requests indefinitely.
- Do not kill an `inUse` worker merely to free a slot.
- Do not leave stale bindings after any worker-removal path.
- Do not claim a hard OS-process bound if replacement can start before old process-tree cleanup without an explicit bounded-overlap policy.
- Do not leave per-document lane metadata or waiting goroutines unbounded.
- Do not expose lane overflow as an accidental generic 500.
- Do not globally strip `(number)` from document titles.
- Do not hash dynamic document identity/context twice in the canonical key.
- Do not send an incremental-only payload after binding invalidation.
- Do not trust only a pre-wait strict turn check; revalidate after acquiring the owning worker.
- Do not LRU/TTL-abandon an actively leased binding.
- Do not treat document mode as exactly recoverable conversation state.
- Do not split the core mechanism into separate YouTube/video and article affinity systems.
- Do not claim title-only identity is universally collision-free.
- Do not assume fake process-group tests prove the real harness cannot detach.
- Do not claim `prepared-sessions: 0` fixes lifecycle growth.
- Do not assume a daemon `session/release` exists unless verified.
- If a reliable session release/dispose RPC appears later, integrate it and reduce reliance on whole-worker recycling.
- Preserve normal stateless compatibility for clients outside explicit/recognized document profiles.
