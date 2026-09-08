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
  -> authoritatively revalidate binding after lease acquisition
  -> keep binding lease pinned for the complete prompt/stream lifetime
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

# 0. Current Review Gate — third review of implementation branch

Reviewed branch head:

```text
d27e9936331434e4ead593fc70d28d5ecf604906
```

Relevant commits since the second-review PLAN commit `f6676a2c8dd61016e5ccfc9768ec66f2ec234022`:

```text
6e3ece55  fix(acp): unify worker retirement paths
f78cd655  feat(acp): complete lifecycle and document safety gates
1371e0ec  fix(acp): pin active stateful bindings
17c3e3ec  fix(acp): complete physical retirement handoff
d27e9936  fix(acp): hold binding leases across streams
```

## Status summary

- **R1 / forward progress: DONE in code.** A draining single-worker deployment has an explicit force-retire escape hatch for fresh-session demand.
- **R1a / unified worker retirement: DONE in code.** Worker-removal paths now converge on one retirement flow that purges bindings exactly once and finishes cleanup outside pool locks.
- **R1b / hard process-capacity handoff: DONE in code, pending R5 live validation.** A retiring worker leaves routing immediately but its hard capacity is not returned until `CloseAndWait()` completes and the owned process tree is gone.
- **R2 / bounded cancelable document lanes: DONE in code.** Same-document work is serialized; waiters are context-aware, bounded, reference-counted, and lane metadata is garbage-collected.
- **R2a / retryable backpressure: DONE in code.** Lane saturation maps to request-scoped HTTP 429 with retry guidance; canceled acquisition is rechecked after token acquisition.
- **R3 / YouTube title normalization: DONE in code.** Notification-prefix stripping is gated to recognizable ` - YouTube` tab titles; ordinary numbered titles stay distinct.
- **R3a / volatile title excluded from semantic fingerprint: DONE for the currently observed prompt shape, pending R7 real sampling.** The semantic projection strips marked title context and the observed `Document Metadata` title line while preserving stable translation instructions.
- **R4 / full-vs-incremental decision before prompt build: DONE in code.** No post-start payload switch remains.
- **R4a / strict monotonic turn race: DONE in code.** Turn order is rechecked after `AcquireSpecific()` and authoritative binding lookup, so concurrent duplicate turns cannot both append incrementally.
- **R9 / active binding eviction consistency: DONE in code.** Strict and document bindings are pinned while leased; eviction/expiry can be deferred. The latest streaming fix holds those leases until the stream goroutine actually finishes.
- **R6 / direct document-title marker stripping: FIXED in code (commit `f076ec37`, pending fourth review + live probe).** `stripDocumentMarkers()` now treats the two marker families differently: the direct `CLIPROXY_ACP_DOCUMENT_TITLE` block is removed ENTIRELY (delimiters + title body) before ACP prompt construction, while the wrapped `CLIPROXY_ACP_TITLE_PROMPT` keeps delimiter-only stripping with its inner expanded content model-visible. Unterminated direct blocks fall back to delimiter removal; the semantic fingerprint already excluded complete marker blocks, which the new fingerprint test now proves.
- **R5 / real process containment: PARTIALLY OBSERVED LIVE (2026-09-08, moecloud).** Document soak showed harness count bounded (3 harnesses across 6 requests, no 1:1 growth), cliproxy swap 2.3 MB, avail 366 MB. Controlled retire/spawn cycle test still pending.
- **R7 / real semantic-key stability: PARTIALLY OBSERVED LIVE (2026-09-08, moecloud).** Real Immersive Translate YouTube batches: 1 `document_bootstrap` + 5 `document_reuse` over ~5 min on one video — same document key across all batches, zero unexpected re-bootstraps. Counter-increment case ((133)->(134)) not yet observed; summary/terms stability not yet sampled.
- **R6 live probe: PASSED (2026-09-08, moecloud).** With the direct marker in the production prompt: conversation DBs show zero `CLIPROXY_ACP` tokens and zero bare title lines reaching the model payload (pre-fix DBs show 3 direct blocks each). Only the ordinary `Title: "..."` metadata line from `{{title_prompt}}` remains model-visible. Step 2 adopted the direct marker as primary. Note: whether `{{imt_title}}` expands is not directly observable (the block is stripped before ACP by design); identity works and the wrapped-title fallback covers the non-expanding case automatically.

GitHub `pr-test-build` for `d27e993...` completed successfully, but that workflow is currently a build gate rather than proof of all unit/race/live tests.

R6 code fix landed as commit `f076ec37` on this branch; its `pr-test-build` run is the build gate for the fix. Do **not** start the final long YouTube soak yet. Next: run the short R5/R6/R7 probes below (Step 2 live `{{imt_title}}` probe first — it decides direct-marker vs wrapped-fallback adoption). No additional lifecycle architecture rewrite is currently indicated.

---

## R6 — BLOCKER: direct `{{imt_title}}` marker must be machine-only

The direct marker is intended to carry routing identity only:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
```

Current `stripDocumentMarkers()` removes only the two delimiter strings. Therefore an expanded direct marker such as:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
Example Video - YouTube
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
```

becomes approximately:

```text
Example Video - YouTube
```

instead of disappearing from the model payload.

When ordinary `{{title_prompt}}` is also present, the model can therefore receive the page title twice:

```text
Example Video - YouTube

Document Metadata:
Title: "Example Video - YouTube"
```

### Required semantics

Treat the two supported markers differently:

```text
CLIPROXY_ACP_DOCUMENT_TITLE
  -> machine-only identity
  -> extract its title
  -> remove the ENTIRE marked block, including the title body, before ACP prompt construction

CLIPROXY_ACP_TITLE_PROMPT
  -> compatibility wrapper around normal title_prompt
  -> extract/parse routing title
  -> remove marker delimiters only
  -> keep expanded title_prompt content model-visible
```

Do not use one generic delimiter-only stripper for both semantics.

### Required tests

Use a payload containing both direct identity and ordinary model-visible title context:

```text
SYSTEM:
  stable translation instructions

  [[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
  Example Video - YouTube
  [[/CLIPROXY_ACP_DOCUMENT_TITLE]]

  Document Metadata:
  Title: "Example Video - YouTube"
```

Assert:

- extracted routing title is `Example Video - YouTube`;
- both direct marker delimiters are absent from cleaned payload;
- the direct marker's body does not survive as a standalone title line;
- ordinary `Document Metadata / Title` remains model-visible;
- semantic fingerprint excludes the direct machine block;
- wrapped `CLIPROXY_ACP_TITLE_PROMPT` continues to preserve its inner expanded context.

Exit criterion: direct `{{imt_title}}` can be used without adding duplicate title text to the model prompt.

---

## R5 — Validation gate: verify real harness process containment

The fake Linux process-tree tests prove cleanup when descendants inherit the ACP daemon process group. They do not prove the real `localharness_external` never calls `setsid()` / `setpgid()`.

On the VPS inspect:

```text
PID  PPID  PGID  SID  COMMAND
```

for CLIProxyAPI, `agy_acp_server.par`, and several `localharness_external` children.

Then force a controlled worker retirement and verify:

- every harness PID owned by that worker disappears;
- no detached child remains;
- replacement worker does not spawn until the old owned process group is gone;
- repeated retire/spawn cycles do not accumulate old harnesses;
- swap/RSS returns toward a stable bounded range.

Also record the retirement sequence:

```text
retirement begins
-> worker removed from routing
-> bindings purged
-> TERM/KILL/reap completes
-> process group disappears
-> retiringCount/global process slot released
-> replacement may spawn
```

If harnesses detach, process-group kill is not sufficient; evaluate cgroup/process-tree containment only after confirming that fact.

Operational hardening after live validation: add an ERROR/watchdog log for unusually long retirement (for example >10 s) without weakening the hard-cap rule. A stuck retirement should be observable rather than silently allowing process overlap.

---

## R7 — Validation gate: real semantic-key stability

The current semantic projection intentionally separates document identity from stable translation configuration. This fixes the known raw-title duplication problem for the observed Immersive Translate prompt shape.

Still validate 20-50 real batches because `summary_prompt`, `terms_prompt`, localization, or future prompt-format changes may vary between batches.

For one YouTube page and one long article, log only safe component hashes:

```text
normalized document identity
stable semantic projection hash
optional title component hash
optional summary component hash
optional terms component hash
resolved model/variant
final document key
session_mode=document_bootstrap|document_reuse
```

No raw page text or sensitive prompt content is required.

Required observations:

- YouTube notification count `(132)` -> `(133)` does not rotate the document key;
- repeated batches on the same page remain one document key when translation semantics are unchanged;
- ordinary numbered documents such as `(1) Introduction` and `(2) Introduction` remain distinct;
- changing model/language/stable translation instructions rotates the key;
- determine whether summary/terms are stable, batch-varying, or page-varying before deciding whether they belong in semantic identity.

If summary/terms vary across batches, do not immediately hash the raw generated values into the canonical key. Prefer a stable configuration fingerprint unless the varying component genuinely changes the semantic contract enough to require a new session.

Exit criterion: 20-50 real batches do not create new document keys merely because volatile page-derived context changes.

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

## 1.4 Worker retirement invariant

There is one logical retirement model:

```text
select/detach victim under pool lock
-> mark retiring / remove from routing
-> purge owner bindings exactly once
-> close and wait for process tree outside pool lock
-> only after physical cleanup return process capacity
-> wake waiters / permit replacement spawn
```

Routing capacity and physically alive process capacity are not the same concept.

## 1.5 Binding lease invariant

Strict/document LRU or TTL eviction must not abandon a session actively used by a prompt or stream.

```text
lookup/revalidate
-> acquire binding lease
-> execute complete request/stream
-> release binding lease
-> finalize any pending eviction
```

A streaming lease lives until the stream goroutine actually drains/exits, not merely until `ExecuteStream()` returns its channel.

---

# 2. Document Identity and Immersive Translate Configuration

## 2.1 Identity preference order

```text
1. X-ACP-Document-ID when a true stable id exists
2. direct machine marker containing {{imt_title}} after R6 live verification
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

## 2.3 Preferred prompt — only after direct variable probe succeeds and R6 stripper is fixed

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

The direct marker is machine-only and must be removed in full before forwarding the prompt to ACP. `title_prompt` remains ordinary model-visible context.

## 2.4 Fallback prompt

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

For this compatibility wrapper, remove only marker delimiters and preserve the expanded `title_prompt` content for the model.

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
- authoritatively revalidate binding after worker lease acquisition;
- pin the binding for the full request/stream lifetime;
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
- request-scoped HTTP 429 on queue saturation;
- deterministic cancellation check after ownership acquisition;
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
- recently completed retry returns cached result rather than appending the same batch again;
- cache size/TTL are bounded;
- no cross-document dedupe.

This remains a later reliability enhancement; it is not a prerequisite for R5/R6/R7 validation.

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
- completed-but-lost retry -> recover from bounded response cache when available;
- strict duplicate/stale turn after post-lease recheck -> never append incrementally; follow strict fallback/invalidation semantics;
- active binding under LRU/TTL pressure -> defer eviction until lease release.

---

# 7. Required Tests / Acceptance

## 7.1 Lifecycle — code gate largely complete, live gate remains

- stateless loop remains bounded by configured worker session pressure;
- prepared refill cannot cross cap;
- prepared/bound transitions do not double-count;
- same-key single-worker cap scenario always makes forward progress;
- all cross-key/same-key retirement paths purge bindings through one hook exactly once;
- hard process capacity is not returned while the old owned process tree still lives;
- forced retirement never kills an `inUse` prompt;
- active strict/document binding cannot be LRU/TTL-abandoned while leased;
- stream lease remains active until stream completion.

## 7.2 Strict stateful

- post-lease authoritative turn recheck prevents concurrent duplicate next-turn append;
- late invalidation bootstraps full history;
- two-turn strict streaming reuse preserves binding correctly;
- stream/non-stream behavior agrees.

## 7.3 Document identity

- explicit headers activate document mode; missing headers stay stateless;
- direct `imt_title` marker works only after R6 direct-variable live probe;
- direct machine marker content is removed in full from model payload;
- wrapped `title_prompt` marker preserves its inner model-visible context;
- fallback parser handles observed quoting/localization;
- real-shaped system-title fixture `(132)` -> `(133)` yields same key for YouTube;
- ordinary numbered titles remain distinct;
- changing stable translation semantics/model/language changes key.

## 7.4 Document concurrency

- 10 simultaneous cold same-document requests -> one session in one-lane mode;
- canceled waiter never prompts;
- queue overflow returns request-scoped 429 with retry guidance;
- 1000 genuinely distinct historical document keys do not leave lane entries;
- different documents can proceed subject to worker limits.

## 7.5 Real process containment — R5

On VPS:

- inspect daemon/harness PID/PPID/PGID/SID;
- force retire and verify every owned harness disappears;
- replacement does not overlap old process tree under hard cap;
- repeat several cycles and verify no orphan accumulation;
- verify RAM/swap reaches a bounded range.

## 7.6 Semantic-key probe — R7

For one YouTube page and one long article, log safe component hashes for 20-50 batches.

Exit criterion: repeated batches do not generate new document keys merely because volatile document context changed.

---

# 8. Revised Implementation Order After Third Review

## Step 1 — R6 code fix: split direct-marker and wrapped-title stripping semantics

Implement full-block removal for `CLIPROXY_ACP_DOCUMENT_TITLE` while preserving inner content for `CLIPROXY_ACP_TITLE_PROMPT`.

**Exit criterion:** direct machine title is usable for routing without appearing as an extra standalone title in the ACP prompt.

## Step 2 — R6 live probe: verify `{{imt_title}}`

Send one real Immersive Translate request with:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

If `{{imt_title}}` expands to the real current browser-tab/page title, adopt the direct marker as primary. If not, use the wrapped `{{title_prompt}}` fallback.

## Step 3 — R5 live process-group validation

Inspect PGID/SID and force controlled retirements on the VPS.

**Exit criterion:** all owned harnesses disappear before hard capacity is returned; repeated cycles do not accumulate orphan processes.

## Step 4 — R7 short semantic sampling

Capture 20-50 safe hashes from one video and one article.

**Exit criterion:** same-page batches remain one key under stable translation settings; identify whether summary/terms are stable enough to participate in semantic identity.

## Step 5 — Configure Immersive Translate for normal use

Headers:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Prompt:

- direct `imt_title` marker if Step 2 succeeds;
- otherwise wrapped `title_prompt` compatibility marker.

Do not ask the user to modify the prompt before Step 1 is merged and ready.

## Step 6 — YouTube soak

Run well past the previous ~25-minute collapse window with hard session limits enabled.

Exit criteria:

- first batch is `document_bootstrap`, subsequent same-page batches are `document_reuse`;
- `session/new` does not grow with subtitle batch count except deliberate rollover/recycle;
- harness count and swap remain bounded;
- switching videos still makes progress when worker is at cap;
- no title-counter key churn;
- no duplicate direct-title context reaches the model.

## Step 7 — Long-article burst

Stress concurrent article batches with the one-lane bounded queue.

Exit criteria:

- one cold document bootstrap per lane;
- bounded waiters;
- overflow is retryable 429;
- no lane-map growth after historical keys disappear;
- worker/session counts remain bounded.

## Step 8 — Retry dedupe

Add/verify a bounded recent-request fingerprint/response cache before aggressive timeout/retry tests.

## Step 9 — Tune TTL / rollover / lane / worker-cap defaults

Choose from measured context growth, TTFT, queue wait, harness count, RAM, and swap.

Measured baseline + recommended caps recorded in `docs/antigravity-acp-memory-caps_CN.md` (2026-09-08, moecloud): document-bound harnesses cost 60-120MB each (not the 7-9MB empty-session figure); recommended `max-sessions-per-worker: 6`, `max-stateful-sessions: 16`, `stateful-session-ttl: 30m`, `idle-timeout: 45m` are now applied on the VPS. Single-session unbounded growth still needs the §5 rollover knobs.

## Step 10 — Optional Origin compatibility detection

Only after explicit protocol is stable. Keep secondary/configurable.

## Step 11 — Micro-optimization only if TTFT data justifies it

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
- Do not return hard process capacity before old owned process-tree cleanup completes.
- Do not leave per-document lane metadata or waiting goroutines unbounded.
- Do not expose lane overflow as an accidental generic 500.
- Do not globally strip `(number)` from document titles.
- Do not hash dynamic document identity/context twice in the canonical key.
- Do not send an incremental-only payload after binding invalidation.
- Do not trust only a pre-wait strict turn check; revalidate after acquiring the owning worker.
- Do not LRU/TTL-abandon an actively leased binding.
- Do not release a streaming binding lease when `ExecuteStream()` merely returns; hold it through stream completion.
- Do not treat `CLIPROXY_ACP_DOCUMENT_TITLE` and `CLIPROXY_ACP_TITLE_PROMPT` as having identical stripping semantics.
- Do not let the machine-only direct title body remain model-visible.
- Do not treat document mode as exactly recoverable conversation state.
- Do not split the core mechanism into separate YouTube/video and article affinity systems.
- Do not claim title-only identity is universally collision-free.
- Do not assume fake process-group tests prove the real harness cannot detach.
- Do not claim `prepared-sessions: 0` fixes lifecycle growth.
- Do not assume a daemon `session/release` exists unless verified.
- If a reliable session release/dispose RPC appears later, integrate it and reduce reliance on whole-worker recycling.
- Preserve normal stateless compatibility for clients outside explicit/recognized document profiles.
