package helps

import (
	"container/list"
	"sync"
	"time"
)

// StatefulACPBinding maps one logical client session onto the live ACP
// session that continues it. The binding is only valid while its owning
// worker/client remains alive; the ACP session id is process-local, so a
// hit must reacquire the SAME worker (the binding does not reserve the
// worker between turns — one worker hosts many sessions sequentially).
type StatefulACPBinding struct {
	LogicalSessionID string
	ACPSessionID     string
	AuthKey          string
	ModelVariant     string
	// LastTurn is the highest successfully completed turn index. A turn is
	// bound only after session/prompt completes (P1.4): canceled or failed
	// prompts never advance it, so the next request bootstraps from full
	// history instead of continuing an ambiguous provider-side state.
	LastTurn int64
	// WorkerRef identifies the owning worker. The executor compares its
	// leased worker pointer identity against this snapshot; a mismatch
	// (worker recycled or gone) invalidates the binding.
	worker *AntigravityAcpWorker
	// lastUsedAt drives TTL expiry and LRU eviction.
	lastUsedAt time.Time
}

// DefaultMaxStatefulSessions bounds the binding table when the config does
// not override it.
const DefaultMaxStatefulSessions = 256

// StatefulSessionTable is the bounded logical-session -> ACP-session
// registry. All operations are goroutine-safe. A hit validates auth key,
// model variant and monotonic turn; any mismatch invalidates the binding
// and the caller bootstraps from the full request history (P1.2).
type StatefulSessionTable struct {
	mu   sync.Mutex
	m    map[string]*list.Element
	lru  *list.List // front = most recently used
	ttl  time.Duration
	max  int
	auth *AntigravityAcpPool
}

// bindingEntry pairs the key with its binding inside the LRU list.
type bindingEntry struct {
	key      string
	binding  *StatefulACPBinding
	worker   *AntigravityAcpWorker
	deadline time.Time
}

// NewStatefulSessionTable builds a table with the given TTL and LRU bound.
// max <= 0 disables bounds enforcement (tests); ttl <= 0 disables expiry.
func NewStatefulSessionTable(ttl time.Duration, max int) *StatefulSessionTable {
	return &StatefulSessionTable{
		m:   make(map[string]*list.Element),
		lru: list.New(),
		ttl: ttl,
		max: max,
	}
}

// getLocked looks up a live binding, enforcing TTL. Callers hold mu.
func (t *StatefulSessionTable) getLocked(logicalSessionID string) (*StatefulACPBinding, bool) {
	el, ok := t.m[logicalSessionID]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*bindingEntry)
	if t.ttl > 0 && time.Now().After(entry.deadline) {
		t.removeLocked(el)
		return nil, false
	}
	return entry.binding, true
}

// removeLocked drops one element and its map entry. Callers hold mu.
func (t *StatefulSessionTable) removeLocked(el *list.Element) {
	entry := el.Value.(*bindingEntry)
	t.lru.Remove(el)
	delete(t.m, entry.key)
}

// WorkerOf returns the worker currently owning the binding for a key,
// or nil when the binding is absent. Diagnostics/executor aid.
func (t *StatefulSessionTable) WorkerOf(key string) *AntigravityAcpWorker {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.getLocked(key)
	if !ok {
		return nil
	}
	return b.worker
}

// Lookup returns the binding for a key if it is live, owned by the given
// worker, and matches the auth key and resolved model variant. A mismatch
// invalidates the binding (the caller must bootstrap). A live, matching hit
// refreshes TTL and LRU position. A nil worker skips the worker check
// (used by the executor's pre-lease validation pass).
func (t *StatefulSessionTable) Lookup(key string, worker *AntigravityAcpWorker, authKey, modelVariant string) (*StatefulACPBinding, bool) {
	if t == nil || key == "" {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.getLocked(key)
	if !ok {
		return nil, false
	}
	el := t.m[key]
	invalid := (worker != nil && b.worker != worker) ||
		b.AuthKey != authKey ||
		(modelVariant != "" && b.ModelVariant != modelVariant) ||
		b.worker == nil
	if invalid {
		t.removeLocked(el)
		return nil, false
	}
	if t.ttl > 0 {
		el.Value.(*bindingEntry).deadline = time.Now().Add(t.ttl)
	}
	b.lastUsedAt = time.Now()
	t.lru.MoveToFront(el)
	return b, true
}

// Bind records a successfully completed turn: logical session -> ACP
// session on the given worker. Refreshing an existing binding moves it to
// the LRU front and refreshes TTL.
func (t *StatefulSessionTable) Bind(logicalSessionID, acpSessionID, authKey, modelVariant string, worker *AntigravityAcpWorker, turn int64) {
	if t == nil || logicalSessionID == "" || acpSessionID == "" || worker == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if el, ok := t.m[logicalSessionID]; ok {
		entry := el.Value.(*bindingEntry)
		entry.binding.ACPSessionID = acpSessionID
		entry.binding.AuthKey = authKey
		entry.binding.ModelVariant = modelVariant
		entry.binding.LastTurn = turn
		entry.binding.worker = worker
		entry.worker = worker
		if t.ttl > 0 {
			entry.deadline = time.Now().Add(t.ttl)
		}
		entry.binding.lastUsedAt = time.Now()
		t.lru.MoveToFront(el)
		return
	}
	b := &StatefulACPBinding{
		LogicalSessionID: logicalSessionID,
		ACPSessionID:     acpSessionID,
		AuthKey:          authKey,
		ModelVariant:     modelVariant,
		LastTurn:         turn,
		worker:           worker,
		lastUsedAt:       time.Now(),
	}
	entry := &bindingEntry{key: logicalSessionID, binding: b, worker: worker}
	if t.ttl > 0 {
		entry.deadline = time.Now().Add(t.ttl)
	}
	t.m[logicalSessionID] = t.lru.PushFront(entry)
	t.enforceBoundLocked()
}

// Invalidate drops the binding for a logical session (cancellation,
// transport failure, turn mismatch, worker death).
func (t *StatefulSessionTable) Invalidate(logicalSessionID string) {
	if t == nil || logicalSessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if el, ok := t.m[logicalSessionID]; ok {
		t.removeLocked(el)
	}
}

// enforceBoundLocked LRU-evicts idle bindings above the configured bound.
// Callers hold mu. Eviction only drops the proxy binding; no ACP
// session/close RPC is assumed.
func (t *StatefulSessionTable) enforceBoundLocked() {
	if t.max <= 0 {
		return
	}
	for t.lru.Len() > t.max {
		back := t.lru.Back()
		if back == nil {
			return
		}
		t.removeLocked(back)
	}
}

// PurgeWorker drops every binding owned by the given worker (worker death,
// release with unhealthy state). Recovery is transparent: the next request
// for any purged session bootstraps from its full history.
func (t *StatefulSessionTable) PurgeWorker(worker *AntigravityAcpWorker) {
	if t == nil || worker == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var doomed []*list.Element
	for el := t.lru.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*bindingEntry)
		if entry.worker == worker {
			doomed = append(doomed, el)
		}
	}
	for _, el := range doomed {
		t.removeLocked(el)
	}
}

// Len reports the number of live bindings (diagnostics/tests).
func (t *StatefulSessionTable) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lru.Len()
}
