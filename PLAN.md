# ACP Session Lifecycle, Page/Document Affinity, and TTFT Plan

## Goals

1. Keep warm-path ACP TTFT low.
2. Make ACP daemon/session/harness resource use **bounded over time**, including fully stateless traffic.
3. Preserve ordinary OpenAI-compatible stateless behavior by default.
4. Keep strict stateful conversation reuse for clients that can provide a stable logical session id, monotonic turn number, and full-history recovery source.
5. Add a separate **page/document-affinity** mode for segmented translation workloads such as Immersive Translate.
6. Use an explicit header contract as the primary, maintainable way to opt into document affinity.
7. Use one stable page/document identity for both YouTube subtitles and long articles; do not build separate video/article session systems.
8. Prevent article burst concurrency, retries, and lifecycle caps from recreating unbounded sessions, goroutines, or metadata maps.
9. Keep recovery and forward progress correct even on the intended low-memory `max-workers: 1` deployment.

Target behavior:

```text
Immersive Translate request
  -> explicit X-ACP-* headers select document-affinity semantics
  -> explicit machine-readable page title/id marker provides identity

first batch for document D
  -> create/consume one fresh ACP session
  -> send normal full translation context + current batch
  -> bind D -> worker + ACP session

later batch for document D
  -> reacquire owning worker
  -> reuse same ACP session
  -> send only newest user translation batch
  -> no session/new

switch to document E
  -> bootstrap another bounded binding/session

worker reaches hard session pressure
  -> old worker becomes draining
  -> fresh-session demand MUST still make progress
  -> retire an idle draining worker when necessary
  -> purge/rebootstrap its old bindings
  -> terminate daemon process tree
  -> spawn replacement
```

---

# 0. Review Gate for `feat/antigravity-session-lifecycle-step1`

Reviewed branch head: `ee3b2605a9e93bb4f3a162751ec582cbcbf80cc5`.

### Implementation status (updated after R1 fix)

- **R1 — FIXED.** `forceRetireIdleDrainingLocked` in
  `internal/runtime/executor/helps/antigravity_acp_pool.go` retires the least
  recently used **idle draining** worker of the key (ignoring its bindings)
  right before the fresh acquirer would join the wait queue; the retirement
  removes the worker, returns its global slot, purges strict/document
  bindings through the `SetWorkerRetiredHook` registered by the executor
  (both binding tables), wakes same-key waiters, and closes the ACP client.
  `Release` of a draining worker now wakes queued waiters so the escape
  hatch runs promptly even when the cap was hit while a prompt was in
  flight. Forced retirement never touches `inUse` workers. The cross-key
  eviction and idle-expiry paths also purge bindings now. Tests:
  `antigravity_acp_r1_test.go` (pool level: single-worker fresh progress,
  hook purge, never-in-use, evicted-bound purge; executor level: both
  binding tables emptied and lookups fail after forced retirement).
- **R4 — FIXED.** Binding validation moved fully inside
  `acquireDocumentSession` / `acquireStatefulSession` (see Step 2 below);
  the prompt builder starts only after the hit/miss verdict is final, so no
  incremental payload can leak into a fresh session and the captured
  `promptPayload` is never mutated after the goroutine starts.
- **R3 — FIXED.** YouTube notification-prefix normalization gated on the
  ` - YouTube` tab-title suffix; ordinary `(number)` titles stay distinct
  (see Step 4 below).
- **R2 — FIXED.** Raw per-key mutex lanes replaced with context-aware,
  reference-counted, bounded token-channel lanes (`AcquireLane`): ctx
  cancellation while waiting, bounded waiter queue with retryable
  `ErrDocumentLaneBusy`, lane GC on last reference, ABA-safe releases (see
  Step 3 below).
- R3 — FIXED (code side). R5/R6/R7 need the live VPS / real plugin probes;
  order below is unchanged.

The branch contains the right overall direction:

- Unix ACP process-group ownership and TERM/KILL escalation;
- per-worker session ledger and hard-cap plumbing;
- worker draining state;
- explicit document-affinity headers;
- document binding table and same-worker reuse;
- per-document serialization;
- a 50-sequential-request test proving one document can reuse one ACP session.

However, **do not treat Step 1-3 as complete yet**. The following review findings must be resolved before the live VPS soak test.

## R1 — BLOCKER: draining bound worker can prevent all fresh-session progress

Current worker recycling requires a draining worker to have no strict/document bindings:

```text
draining
+ idle
+ no refill/prepared session
+ boundStrict == 0
+ boundDocument == 0
=> recyclable
```

That is safe for existing bindings but breaks the intended single-worker deployment:

```text
max-workers: 1
max-sessions-per-worker: N

worker reaches N
-> worker becomes draining
-> existing document D remains bound
-> user switches to new document E
-> E needs session/new
-> Acquire skips draining worker
-> per-auth worker slot is still occupied
-> replacement cannot spawn
-> request waits
```

The binding TTL does not save this path reliably because binding expiry is currently lazy: if document D is never looked up again, its expired binding does not automatically disappear.

### Required rule

> A hard resource cap must never sacrifice fresh-request forward progress.

When a request needs a new ACP session and all per-auth capacity is occupied only by **idle draining workers**, the pool must be allowed to retire an idle draining victim even when it still owns strict/document bindings.

Retirement sequence:

1. never retire an `inUse` worker;
2. remove the idle draining worker from the pool;
3. purge/invalidate strict and document bindings owned by it;
4. close its ACP client/process group so all daemon sessions/harnesses disappear;
5. return the worker/global slot;
6. wake/retry the fresh acquirer and spawn a replacement.

Recovery semantics are already compatible with this:

- strict stateful client -> next turn bootstraps from full history;
- document client -> next batch bootstraps from the current request and loses only opportunistic prior-document context.

### Required tests

- `maxWorkers=1`, `maxSessionsPerWorker=1`: create + bind session A, release worker, then request a fresh session B; B must receive a replacement worker promptly rather than waiting forever.
- forced retirement purges strict bindings; next strict request bootstraps from full history.
- forced retirement purges document bindings; next document request bootstraps from its current full request.
- never force-retire a worker while a prompt is `inUse`.

This is the first fix before continuing.

---

## R2 — BLOCKER before article testing: document lane map is unbounded and waits ignore context

Current `DocumentSessionTable` stores:

```go
lanes map[string]*sync.Mutex
```

A lane is created for every unique document key and never deleted. The binding table is TTL/LRU bounded, but the lane table is not.

The raw `Mutex.Lock()` also means:

- a request canceled while waiting for the same document cannot stop waiting;
- waiter count is unbounded;
- a large article burst can accumulate arbitrarily many waiting goroutines;
- the planned 429/503 backpressure does not yet exist.

### Required replacement

Use a context-aware keyed lane/gate with reference-counted lifecycle, conceptually:

```go
type documentLane struct {
    token   chan struct{} // capacity 1 for one-lane mode
    waiters int
    refs    int
}
```

Exact implementation may differ, but it must provide:

- one holder per document lane in v1;
- `ctx.Done()` cancels a waiting request;
- configurable/bounded waiter count;
- queue overflow returns a retryable error instead of creating another session;
- lane entry is removed when it has no holder/waiters/references;
- no ABA deletion race if a new lane for the same key is created while an old release runs.

### Required tests

- 1000 unique document keys acquired/released do not leave 1000 permanent lane entries.
- canceled waiter exits promptly without waiting for current generation completion.
- queue limit is enforced.
- 10 simultaneous cold requests for one document still perform exactly one bootstrap/session-new in one-lane mode.

Do this before a long-article burst test.

---

## R3 — Correctness: YouTube notification-prefix normalization is currently applied globally

Current normalization strips:

```regex
^\(\d+\)\s*
```

from every document title.

That is too broad. It can incorrectly merge legitimate documents such as:

```text
(1) Introduction
(2) Introduction
```

The PLAN requirement remains:

> Core document normalization is generic and conservative; site-specific decoration removal is gated by known site/profile evidence.

For the personal YouTube case, a simple initial gate is sufficient:

```text
only strip ^\(\d+\)\s* when the title is recognizably a YouTube tab title,
for example when it ends with " - YouTube"
```

A future real URL/document id should replace this heuristic.

### Required tests

- `(132) Foo - YouTube` and `(133) Foo - YouTube` normalize to the same identity.
- `(1) Introduction` and `(2) Introduction` remain distinct.
- `(2024) Annual Report` remains unchanged.

---

## R4 — Correctness/data-race risk: late binding invalidation can bootstrap with the incremental payload

Both non-stream and stream paths currently launch the prompt-builder goroutine using the presumed hit payload before the final binding validation.

Conceptually:

```text
binding appears to hit
-> prompt goroutine starts building incremental newest-user payload
-> second Lookup says binding is no longer valid
-> code switches statefulHit=false and intends to bootstrap
-> fresh session is opened
```

Changing `promptPayload` after the goroutine has started does not safely rebuild the prompt. Depending on scheduling, the fresh session can receive only the incremental turn instead of the full bootstrap request; the unsynchronized variable access is also a race-prone pattern.

This affects document mode and the analogous strict-stateful late-invalidation path.

### Required fix

Choose one:

1. finish all binding validation before starting the prompt builder; **preferred if it does not hurt useful overlap**, or
2. if late validation fails, explicitly drain/clean the incremental build and start a new full-payload build before prompting the fresh session.

Never rely on mutating a captured `promptPayload` variable after a goroutine has started.

### Required tests

Create deterministic hooks/fakes that invalidate a binding between initial lookup/acquisition and final validation, then verify:

- a fresh session is created;
- bootstrap prompt contains full system/developer/history context;
- only a verified live hit receives the incremental newest-user payload;
- stream and non-stream paths behave identically;
- race test covers this transition.

---

## R5 — Validation gate: process-group tests do not yet prove the real harness stays in the daemon group

The fake Linux test is useful and should stay. It proves cleanup when daemon children inherit the ACP process group.

It does **not** prove that the real `localharness_external` never calls `setsid`/`setpgid` or otherwise detaches itself.

Before declaring process-tree cleanup complete, run one live validation on the VPS:

```text
PID  PPID  PGID  SID  COMMAND
```

for:

- CLIProxyAPI;
- `agy_acp_server.par`;
- several `localharness_external` children.

Then force one controlled worker recycle and confirm all harness PIDs disappear and swap is reclaimed/stabilizes.

If real harnesses detach from the process group, process-group kill is insufficient. Linux fallback options then include:

- capture/kill descendants before the parent disappears;
- cgroup-based worker ownership/cleanup;
- another verified process-tree containment mechanism.

Do not redesign this prematurely; first inspect the real PGID/SID behavior.

---

## R6 — Identity robustness: prefer direct `{{imt_title}}` marker if one-request verification succeeds

Immersive Translate's current prompt documentation defines `imt_title` as a system-injected special variable, and default `title_prompt` itself expands from it.

The current implementation parses literal `Title:` text from the expanded `title_prompt`. That is unnecessarily coupled to formatting/localization:

```text
Title: "..."
Title: 《...》
标题：《...》
```

It also interacts badly with the current YouTube-prefix stripper when the title is wrapped in `《》`.

### Preferred user configuration after a one-request probe

First verify that this expands correctly in the user's active Immersive Translate configuration:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

If the captured request contains the real title inside this marker, make this the primary identity path:

- proxy strips the machine marker block entirely before ACP;
- `title_prompt` remains untouched and model-visible;
- identity parsing no longer depends on English `Title:` formatting;
- title normalization operates directly on the raw page title.

Keep the existing wrapped-`title_prompt` parser as a compatibility fallback if direct `imt_title` expansion fails in the actual installed version/config.

Do not block R1-R5 on this probe; perform it before the live Immersive Translate soak test.

---

## R7 — Measure semantic-key stability before relying on full system-prompt fingerprinting

The current document semantic fingerprint retains system/developer messages. In Immersive Translate this can include:

- title context;
- page summary (`summary_prompt` / `imt_theme`);
- extracted terminology (`terms_prompt` / `imt_terms`);
- style guide and static translation instructions.

If summary/terms are populated or changed between batches, the same page can generate a new document key and unexpectedly create another ACP session.

### Required measurement

For one YouTube page and one long article, log only safe hashes/diagnostics for 20-50 batches and answer:

- does the semantic fingerprint remain stable?
- if it changes, which semantic component changed?
- does the change happen only once during initial context enrichment or repeatedly?

If dynamic page-derived context causes churn, split the key conceptually into:

```text
stable translation semantics
+ document identity
```

and exclude volatile document-derived summary/terms from the affinity key, or define a deliberate bounded rollover policy for those changes.

Do not guess the final exclusion set before observing real requests.

---

## R8 — Documentation/test fixture alignment

The current document-affinity documentation/test examples place the title marker in a user prompt, while the intended Immersive Translate configuration normally carries title context in the system prompt.

The recursive parser happens to accept both, but acceptance tests should match the real integration shape.

Add a captured-style fixture:

```text
SYSTEM:
  translation instructions
  machine title marker
  title/summary/terms/style context

USER:
  p0..p7 / YAML / current translation batch
```

Assert that a document hit sends only the user batch and does not replay the system title/context.

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

Therefore lifecycle bounding remains mandatory even after document reuse works.

## 1.2 Maintainability decision

The primary supported document integration uses explicit protocol headers:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

`X-ACP-Session-Scope: document` defines semantics. `X-ACP-Client` is a profile/logging namespace, not the semantic switch.

Activation priority:

```text
1. explicit document headers            <- primary/recommended
2. optional configured client fallback <- compatibility only
3. ordinary stateless behavior
```

Do not hard-wire the generic document mechanism to one browser-extension Origin.

---

# 2. P0 — Bound ACP Daemon / Session / Harness Lifecycle

## 2.1 Process ownership and cleanup

Linux target:

1. start the ACP daemon in a dedicated process group;
2. close stdin/request graceful exit first;
3. after grace expiry send TERM to the process group;
4. escalate to process-group KILL;
5. prove the real harness remains inside the owned group or add stronger containment.

The current branch implements the first four items and fake child-process tests. R5 is the remaining live validation gate.

## 2.2 Per-worker session ledger

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

A session becomes abandoned when the daemon remains alive but the proxy no longer has a reusable binding/path to it.

Prepared -> leased -> bound transitions must not double-count creation.

## 2.3 Hard budgets and draining

Configuration:

```yaml
antigravity:
  max-sessions-per-worker: 0
  max-abandoned-sessions-per-worker: 0
```

For the low-memory VPS, first validate small values such as 4-8 rather than selecting a large default.

A draining worker:

- never creates/refills another session;
- may serve existing bindings while no fresh session is needed;
- is excluded from ordinary fresh traffic;
- **may be forcibly retired while idle when fresh demand otherwise cannot make progress** (R1);
- is never forcibly retired while `inUse`.

This gives the intended bounded sawtooth:

```text
spawn
-> create <= budget sessions
-> drain
-> retire/purge bindings when replacement is required
-> kill process tree
-> spawn replacement
```

---

# 3. Reuse Protocols

## 3.1 Strict stateful conversation

Existing contract:

```http
X-Session-ID: <stable logical conversation id>
X-ACP-Session-Reuse: 1
X-ACP-Session-Turn: <monotonic turn index>
```

A hit reuses the owning worker/session and sends only the newest turn. A miss/retired worker bootstraps from full history.

## 3.2 Document affinity

Primary contract:

```http
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Optional stronger identity:

```http
X-ACP-Document-ID: <real stable page/video/document id>
```

Semantics:

- no client turn number is required;
- first request sends full current translation context and binds only after success;
- healthy hit sends only the newest user translation batch;
- binding loss starts a new session from the current request;
- previous document context is opportunistic, not exactly recoverable.

---

# 4. Document Identity and Immersive Translate Configuration

## 4.1 Identity preference order

Use the strongest available identity:

```text
1. X-ACP-Document-ID when a real stable page/document id exists
2. direct machine marker containing {{imt_title}} after live verification
3. wrapped {{title_prompt}} compatibility parser
4. unmarked Document Metadata/Title parsing only for explicit document mode during migration
```

A raw title is namespaced with auth/client/model/source-format and the chosen stable semantic fingerprint before hashing.

## 4.2 Recommended Immersive Translate headers

Configure the active service's `headerConfigs` with:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Inspect the user's actual service entry when applying this; do not guess its config key.

## 4.3 Preferred prompt after direct-variable probe

Try:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

If the request shows the real page title inside the marker, use this permanently.

The proxy removes only the machine identity block; normal `title_prompt`, summary, terms, and style remain model-visible.

## 4.4 Compatibility prompt if direct `imt_title` does not expand

Use:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

The proxy extracts the title from the wrapped expanded block and removes only marker delimiters.

Parsing must handle the actually observed/default quoting/localization forms, including `《...》`; do not assume only `Title: "..."`.

## 4.5 Conservative normalization

Core normalization:

- trim surrounding whitespace;
- collapse clearly irrelevant whitespace only;
- no global site-specific prefix stripping.

YouTube notification-prefix normalization is gated to a known YouTube title/profile, e.g. title ending in ` - YouTube`.

Do not merge ordinary titles beginning with `(number)`.

---

# 5. Document Session Reuse

## 5.1 Bootstrap

For a new document:

```text
full stable SYSTEM/developer translation instructions
+ title/summary/terms/style context available in that request
+ current USER batch
```

Create/consume one fresh session and bind only after `session/prompt` succeeds.

Log:

```text
session_mode=document_bootstrap
```

## 5.2 Hit

For a live matching binding:

- reacquire the same worker;
- no `session/new`;
- do not replay system/title/summary/terms context;
- send only the newest user translation batch.

Log:

```text
session_mode=document_reuse
```

The late-binding transition must obey R4: only a verified hit may use the incremental payload.

---

# 6. Concurrency and Backpressure

## 6.1 One-lane bootstrap singleflight

In default one-lane mode, concurrent cold requests for one document must produce one bootstrap session, not N sessions.

## 6.2 Context-aware bounded lane

Replace permanent raw mutex lanes per R2.

Required behavior:

- serialized prompts per ACP document session;
- context cancellation while waiting;
- bounded waiter count;
- retryable backpressure on overflow;
- automatic lane metadata cleanup.

## 6.3 Optional bounded multi-lane mode

Do not implement until one-lane measurements show it is necessary.

Possible later config:

```yaml
antigravity:
  document-session-max-lanes: 1
```

If raised above 1, every lane is a real ACP session and counts against the worker session budget. Never create an unbounded overflow lane.

---

# 7. Duplicate / Retry Protection

A browser timeout can cause the same translation batch to be retried after ACP has already committed it to the document conversation.

Before the timeout/retry soak test, add a bounded short-lived fingerprint/response cache:

```text
document key
+ normalized newest-user batch
+ relevant model/config identity
```

Desired behavior:

- identical concurrent work coalesces where practical;
- recently completed retry returns cached result instead of appending the same turn again;
- cache count and TTL are bounded;
- no cross-document dedupe.

This is not required to fix R1-R5 but should precede production timeout/retry testing.

---

# 8. Binding Lifetime and Very Long Documents

A single document session can itself grow too much in context.

Track at least:

```go
type DocumentBindingStats struct {
    Turns              uint64
    ApproxInputTokens  uint64
    ApproxOutputTokens uint64
    CreatedAt          time.Time
    LastUsedAt         time.Time
}
```

Evaluate after the basic soak tests:

```yaml
antigravity:
  document-session-idle-ttl: "0"
  document-session-max-turns: 0
  document-session-max-estimated-tokens: 0
  max-document-sessions: 0
```

Rollover makes the old binding/session abandoned, bootstraps the current request, and remains bounded by worker lifecycle caps.

Do not add automatic summarization/carry-forward in v1.

---

# 9. Cancellation and Failure Semantics

- canceled before prompt dispatch -> keep existing binding;
- canceled/failed after prompt dispatch with ambiguous provider mutation -> invalidate binding/session;
- worker/transport death -> purge every strict/document binding on that worker;
- downstream formatting failure after a known-successful ACP prompt -> keep provider binding when safe;
- completed-but-lost duplicate -> recover from bounded response cache when available;
- forced idle draining-worker retirement -> purge bindings before/with worker removal and let clients bootstrap.

---

# 10. Tests and Acceptance Criteria

## 10.1 Lifecycle / forward progress

- stateless loop never exceeds configured session pressure for a worker;
- prepared refill cannot cross cap;
- prepared/bound session transitions do not double-count;
- abandoned cap drains/recycles correctly;
- R1 single-worker bound-session scenario returns a replacement rather than hanging;
- forced retirement never kills an in-use prompt;
- stale strict/document binding after forced retirement bootstraps correctly.

## 10.2 Real process containment

- fake child tests remain green;
- inspect real `agy_acp_server.par` + `localharness_external` PGID/SID;
- controlled worker recycle removes all real harness PIDs;
- repeat recycle several times and verify no orphan accumulation.

## 10.3 Document identity

- explicit document headers activate document mode;
- missing headers remain stateless;
- direct `imt_title` marker works if supported by live configuration;
- fallback `title_prompt` parser accepts actual default/localized quoting;
- marker metadata is not forwarded when specified for stripping;
- YouTube notification counters normalize only for YouTube;
- legitimate `(number)` article titles stay distinct.

## 10.4 Document reuse

- first request -> one bootstrap session;
- 50 sequential same-document requests -> one `session/new` before intentional rollover;
- real-shaped system-marker + user-batch fixture hits reuse correctly;
- different document -> different binding;
- model/config incompatibility does not cross-reuse;
- R4 late invalidation always rebuilds full bootstrap prompt in stream and non-stream paths.

## 10.5 Concurrency

- 10 simultaneous cold same-document requests -> one session in one-lane mode;
- waiting request honors context cancellation;
- bounded queue overflow returns retryable error;
- many historical document keys do not leave an unbounded lane map;
- different documents may proceed independently subject to worker limits.

## 10.6 Semantic-key stability probe

For one video and one article, observe safe hashes for 20-50 batches.

Exit criterion before long soak:

- repeated batches do not unexpectedly generate new document keys, or
- any deliberate key change has a documented bounded reason.

## 10.7 YouTube soak

Run well beyond the prior ~25-minute collapse window.

Exit criteria:

- first batch `document_bootstrap`, later batches `document_reuse`;
- harness count does not grow with subtitle batch count;
- swap stays bounded/stable rather than monotonic;
- switching video/page makes progress even if the current worker has reached its cap;
- translation remains responsive well past the old failure horizon.

## 10.8 Long article burst

Only after R2 is complete.

Exit criteria:

- cold burst singleflights;
- wait queue is bounded/cancelable;
- same document reuses one lane/session by default;
- session/harness count stays bounded;
- no goroutine/lane-map growth proportional to every historical page;
- optional second lane is evaluated only if one lane causes real timeout problems.

---

# 11. Revised Implementation Order

## Step 1 — Fix R1 fresh-demand forward progress

This is the current hard blocker. **Status: implemented, tests green.**

Add forced retirement of an **idle draining** worker when fresh-session demand otherwise cannot obtain a worker slot. Purge bindings and spawn replacement.

**Exit criterion:** `maxWorkers=1` cannot deadlock after reaching a session cap while holding a bound document/stateful session. *(Met by `TestAntigravityAcpPool_R1SingleWorkerFreshProgressAfterCap`; the full live VPS soak in Step 8 remains the production confirmation.)*

## Step 2 — Fix R4 late-hit fallback correctness

Remove the incremental/full-payload race and add deterministic stream/non-stream tests.

**Status: implemented, tests green.**

All binding validation now completes INSIDE `acquireDocumentSession` /
`acquireStatefulSession` (the PLAN's preferred option 1): both helpers
return a fully resolved acquire result whose `hit` verdict is final, and
the caller selects the incremental vs. full payload BEFORE starting the
prompt builder goroutine. The old post-build `Lookup`-and-switch blocks
(the data race and the incremental-payload leak into fresh sessions) are
removed from both `Execute` and `ExecuteStream`. A hit lease is never
re-decided after the builder starts; every miss bootstraps from the full
request via the normal `openSession` path.

**Exit criterion:** every bootstrap sends the full recovery payload; every incremental payload belongs to a verified hit. *(Met by `TestR4_DocumentLateInvalidationBootstrapsFullHistory`, `TestR4_StreamAndNonStreamAgreeOnLateInvalidation`, and `TestR4_StrictLateInvalidationBootstrapsFullHistory`.)*

## Step 3 — Replace document raw mutex lanes (R2)

Implement context-aware bounded keyed lanes with automatic lifecycle cleanup.

**Status: implemented, tests green.**

`DocumentSessionTable.lanes` is now `map[string]*documentLane`: a
capacity-1 token channel (empty = free, full = held) with reference-counted
lifecycle. `AcquireLane(key, ctx, maxWaiters)` provides one-lane bootstrap
singleflight, `ctx.Done()` cancellation while waiting, a bounded waiter
queue (`DefaultDocumentLaneMaxWaiters`, overflow returns retryable
`ErrDocumentLaneBusy`), automatic lane removal when the last reference
drops (no growth proportional to historical pages), and pointer-identity
staleness checks (ABA-safe releases from removed-and-recreated lanes). The
executor's document acquisition uses `AcquireLane`; the raw-mutex
`LockLane` remains only as a deprecated test helper.

**Exit criterion:** cancellation/backpressure/lane-GC tests pass and cold burst still creates one session. *(Met by `TestDocumentLane_GCManyHistoricalKeys`, `TestDocumentLane_CanceledWaiterExitsPromptly`, `TestDocumentLane_QueueLimitEnforced`, `TestDocumentLane_ColdBurstSingleflights` under `-race`, plus the existing document-reuse integration tests.)*

## Step 4 — Fix title identity correctness (R3 + R6)

- gate YouTube prefix normalization; **done** — `normalizeDocumentTitle`
  strips `^\(\d+\)\s*` only when the title ends with ` - YouTube`;
  ordinary numbered titles stay distinct
  (`TestDocumentIdentityR3YouTubeGate`).
- test direct `{{imt_title}}` marker in one live Immersive Translate request; **pending live probe (R6)**.
- prefer direct marker if successful; **pending same probe**.
- retain robust `title_prompt` fallback; **already in place** (`parseDocumentTitle` handles `Title: "..."` and `《...》` quoting).

**Exit criterion:** identity is stable for real YouTube request and does not merge legitimate numbered article titles. *(Code-side criterion met and unit-tested; the live Immersive Translate probe below remains before the soak test.)*

### R6 probe instructions (before Step 7)

Capture one real request with the preferred prompt below and check whether
the real page title appears inside the machine marker:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

If yes: add the marker parser (`documentTitleMarkerStart/End`), strip the
block before ACP, and make it the primary identity path, keeping the
wrapped-`title_prompt` parser as fallback. If no: keep the current parser
as primary and continue.

## Step 5 — Validate real process containment (R5)

Inspect real PGID/SID, force recycle, confirm daemon+harness cleanup.

**Exit criterion:** real `localharness_external` processes are physically reclaimed by worker retirement.

## Step 6 — Measure semantic fingerprint stability (R7)

Run short video/article request samples before a long soak.

**Exit criterion:** one page does not churn document sessions because summary/terms/system context changes unexpectedly; if it does, revise key projection first.

## Step 7 — Configure final Immersive Translate integration

Headers:

```text
X-ACP-Session-Reuse: 1
X-ACP-Session-Scope: document
X-ACP-Client: immersive-translate
```

Preferred prompt if direct variable probe succeeds:

```text
[[CLIPROXY_ACP_DOCUMENT_TITLE:v1]]
{{imt_title}}
[[/CLIPROXY_ACP_DOCUMENT_TITLE]]
{{title_prompt}}{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

Fallback:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
{{title_prompt}}
[[/CLIPROXY_ACP_TITLE_PROMPT]]
{{summary_prompt}}{{terms_prompt}}
{{imt_style_guide}}
```

## Step 8 — YouTube soak test

Run well past the old failure horizon with hard worker session limits enabled.

## Step 9 — Add/verify retry dedupe, then long-article burst test

Do not run the article concurrency test against an unbounded raw-mutex queue.

## Step 10 — Tune TTL / rollover / lane / worker-cap defaults

Choose values from measured context growth, queue wait, TTFT, harness count, RAM, and swap.

## Step 11 — Optional compatibility Origin detection

Only after the explicit contract is stable. Keep it disabled/configurable and secondary to explicit headers.

## Step 12 — Resume micro-optimization only if useful

Potential remaining work:

- single-pass outbound ACP JSON encoding;
- reduce repeated session-update decoding;
- other allocation/copy improvements justified by TTFT stages.

If backend generation dominates, stop rather than adding complexity for insignificant proxy-side savings.

---

# 12. Guardrails / Non-Goals

- Do not auto-reuse sessions for arbitrary clients.
- Explicit document headers remain the primary/recommended integration contract.
- `X-ACP-Session-Scope: document` defines semantics; `X-ACP-Client` does not.
- Do not let a draining bound worker block fresh requests indefinitely.
- Do not kill an in-use worker merely to free a slot.
- Do not leave per-document lane metadata unbounded after bindings expire.
- Do not use a non-cancelable unbounded mutex queue for article translation.
- Do not globally strip `(number)` from document titles.
- Do not send an incremental-only payload after a binding has been invalidated.
- Do not treat document mode as exactly recoverable conversation state.
- Do not split the core mechanism into separate YouTube/video and article affinity systems.
- Do not claim title-only identity is universally collision-free.
- Do not assume fake process-group tests prove the real harness cannot detach.
- Do not claim `prepared-sessions: 0` fixes lifecycle growth.
- Do not assume a daemon `session/release` exists unless verified.
- If a reliable session release/dispose RPC appears later, integrate it and reduce reliance on whole-worker recycling.
- Preserve normal stateless compatibility for clients outside explicit/recognized document profiles.
