package helps

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// DocumentACPBinding maps a normalized page/document identity to one live ACP
// session. Unlike strict stateful reuse, document mode has no client turn.
type DocumentACPBinding struct {
	DocumentKey  string
	ACPSessionID string
	AuthKey      string
	ModelVariant string
	Worker       *AntigravityAcpWorker
	lastUsedAt   time.Time
}

type documentBindingEntry struct {
	key             string
	binding         *DocumentACPBinding
	deadline        time.Time
	activeLeases    int
	evictionPending bool
}

// DocumentSessionTable is a bounded document-key -> ACP-session registry.
// Each key also has a lane gate so bootstrap and prompts for one document
// are serialized; different documents remain independent lanes. Lanes use
// context-aware, reference-counted gates with bounded waiters (R2);
// legacyLanes is the raw-mutex path retained only for tests.
type DocumentSessionTable struct {
	mu    sync.Mutex
	m     map[string]*list.Element
	lru   *list.List
	ttl   time.Duration
	max   int
	lanes map[string]*documentLane
	// legacyLanes backs the deprecated LockLane test helper; production
	// traffic uses the AcquireLane gate above.
	legacyLanes map[string]*sync.Mutex
}

func NewDocumentSessionTable(ttl time.Duration, max int) *DocumentSessionTable {
	return &DocumentSessionTable{
		m:           make(map[string]*list.Element),
		lru:         list.New(),
		ttl:         ttl,
		max:         max,
		lanes:       make(map[string]*documentLane),
		legacyLanes: make(map[string]*sync.Mutex),
	}
}

// documentLane is one serialization gate for a document key. The capacity-1
// token channel models the lane: EMPTY = free, FULL = held. Acquiring is a
// send (instant on a free lane, blocks while held); releasing is a receive.
// Reference-counted lifecycle (R2): entries exist only while a holder or
// waiter references them and are removed on release when the last reference
// drops. Because a lane can be removed and recreated for the same key,
// staleness is decided by pointer identity (closures capture their lane
// struct), which is ABA-safe.
type documentLane struct {
	token   chan struct{} // empty = lane free, full = lane held
	waiters int           // requests currently blocked in the select
	refs    int           // holder + waiters; drives removal on release
}

// ErrDocumentLaneBusy means the per-document wait queue is at its bound.
// The caller should return a retryable backpressure error instead of
// creating another ACP session (R2).
var ErrDocumentLaneBusy = errors.New("acp document lane queue full")

// DefaultDocumentLaneMaxWaiters bounds the number of concurrent waiters on
// one document lane. A long-article burst cannot accumulate unbounded
// goroutines; overflow returns ErrDocumentLaneBusy.
const DefaultDocumentLaneMaxWaiters = 16

// AcquireLane serializes all work for one document key and returns its
// release function. The key is expected to be a canonical, namespaced key.
//
// Behavior (R2):
//   - one holder per document lane (v1 one-lane mode);
//   - ctx.Done() cancels a waiting request promptly, without waiting for
//     the current holder to finish;
//   - waiter count is bounded (maxWaiters <= 0 falls back to the default);
//     overflow returns ErrDocumentLaneBusy instead of queueing forever;
//   - the lane entry is removed once it has no holder/waiters/refs left;
//   - a release from a superseded (removed-and-recreated) lane is a no-op.
//
// Lock ordering: AcquireLane takes only t.mu around bookkeeping; the token
// wait happens without any lock.
func (t *DocumentSessionTable) AcquireLane(key string, ctx context.Context, maxWaiters int) (func(), error) {
	noRelease := func() {}
	if t == nil || key == "" {
		return noRelease, nil
	}
	if maxWaiters <= 0 {
		maxWaiters = DefaultDocumentLaneMaxWaiters
	}
	if err := ctx.Err(); err != nil {
		return noRelease, err
	}
	t.mu.Lock()
	lane, ok := t.lanes[key]
	if !ok {
		lane = &documentLane{token: make(chan struct{}, 1)}
		t.lanes[key] = lane
	}
	if lane.waiters >= maxWaiters {
		t.mu.Unlock()
		return noRelease, ErrDocumentLaneBusy
	}
	lane.waiters++
	lane.refs++
	t.mu.Unlock()

	// Take the lane: a send completes only while the lane is free (empty
	// channel). Cancellation exits the wait immediately, regardless of the
	// current holder's remaining work (R2).
	select {
	case lane.token <- struct{}{}:
	case <-ctx.Done():
		t.releaseLaneRef(key, lane, true)
		return noRelease, ctx.Err()
	}

	// Holding now; no longer a waiter. Prefer cancellation over a
	// simultaneous token-ready select result so a canceled request never
	// reaches document prompt setup.
	t.mu.Lock()
	lane.waiters--
	canceledErr := ctx.Err()
	t.mu.Unlock()
	if canceledErr != nil {
		t.releaseLaneRef(key, lane, false)
		return noRelease, canceledErr
	}
	return func() { t.releaseLaneRef(key, lane, false) }, nil
}

// releaseLaneRef drops one reference from the lane. A canceled waiter
// (canceled=true) never took the lane; the holder frees it (drain). When
// the last reference is gone the lane entry is removed so historical
// document keys cannot accumulate (R2 lane GC). A release for a lane that
// has already been removed and replaced is a no-op.
func (t *DocumentSessionTable) releaseLaneRef(key string, lane *documentLane, canceled bool) {
	t.mu.Lock()
	if t.lanes[key] != lane {
		// Superseded lane: its references no longer matter.
		t.mu.Unlock()
		return
	}
	lane.refs--
	if canceled {
		lane.waiters--
	}
	if lane.refs <= 0 {
		delete(t.lanes, key)
	}
	t.mu.Unlock()
	if !canceled {
		// Free the lane for the next waiter (non-blocking drain).
		select {
		case <-lane.token:
		default:
		}
	}
}

// LaneCount reports the number of live document lanes (diagnostics/tests).
func (t *DocumentSessionTable) LaneCount() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.lanes)
}

// LockLane serializes all work for one document key and returns its unlock
// function. Legacy raw-mutex path kept for tests that do not need
// cancellation or bounded queues; production callers use AcquireLane (R2).
func (t *DocumentSessionTable) LockLane(key string) func() {
	if t == nil || key == "" {
		return func() {}
	}
	t.mu.Lock()
	lane := t.legacyLanes[key]
	if lane == nil {
		lane = &sync.Mutex{}
		t.legacyLanes[key] = lane
	}
	t.mu.Unlock()
	lane.Lock()
	return lane.Unlock
}

func (t *DocumentSessionTable) removeElementLocked(el *list.Element) {
	entry := el.Value.(*documentBindingEntry)
	t.lru.Remove(el)
	delete(t.m, entry.key)
	if entry.binding != nil && entry.binding.Worker != nil {
		entry.binding.Worker.AbandonSession(entry.binding.ACPSessionID)
	}
}

func (t *DocumentSessionTable) removeLocked(el *list.Element) bool {
	entry := el.Value.(*documentBindingEntry)
	if entry.activeLeases > 0 {
		entry.evictionPending = true
		return false
	}
	t.removeElementLocked(el)
	return true
}

func (t *DocumentSessionTable) getLocked(key string) (*DocumentACPBinding, bool) {
	el, ok := t.m[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*documentBindingEntry)
	if t.ttl > 0 && time.Now().After(entry.deadline) {
		if entry.activeLeases > 0 {
			entry.evictionPending = true
			return entry.binding, true
		}
		t.removeElementLocked(el)
		return nil, false
	}
	return entry.binding, true
}

// Lookup validates the owning worker, auth namespace and model variant. Any
// mismatch invalidates the binding so the caller bootstraps from the current
// batch.
func (t *DocumentSessionTable) Lookup(key string, worker *AntigravityAcpWorker, authKey, modelVariant string) (*DocumentACPBinding, bool) {
	if t == nil || key == "" {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	binding, ok := t.getLocked(key)
	if !ok {
		return nil, false
	}
	el := t.m[key]
	if binding.Worker == nil || (worker != nil && binding.Worker != worker) || binding.AuthKey != authKey || (modelVariant != "" && binding.ModelVariant != modelVariant) {
		t.removeElementLocked(el)
		return nil, false
	}
	if t.ttl > 0 {
		el.Value.(*documentBindingEntry).deadline = time.Now().Add(t.ttl)
	}
	binding.lastUsedAt = time.Now()
	t.lru.MoveToFront(el)
	return binding, true
}

// Bind records a successful document bootstrap or refreshes an existing hit.
func (t *DocumentSessionTable) Bind(key, sessionID, authKey, modelVariant string, worker *AntigravityAcpWorker) {
	if t == nil || key == "" || sessionID == "" || worker == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if el, ok := t.m[key]; ok {
		entry := el.Value.(*documentBindingEntry)
		if entry.binding.ACPSessionID != sessionID && entry.binding.Worker != nil {
			entry.binding.Worker.AbandonSession(entry.binding.ACPSessionID)
			worker.BindSession(sessionID, "document")
		}
		entry.binding.ACPSessionID = sessionID
		entry.binding.AuthKey = authKey
		entry.binding.ModelVariant = modelVariant
		entry.binding.Worker = worker
		entry.binding.lastUsedAt = time.Now()
		if t.ttl > 0 {
			entry.deadline = time.Now().Add(t.ttl)
		}
		t.lru.MoveToFront(el)
		return
	}
	binding := &DocumentACPBinding{
		DocumentKey:  key,
		ACPSessionID: sessionID,
		AuthKey:      authKey,
		ModelVariant: modelVariant,
		Worker:       worker,
		lastUsedAt:   time.Now(),
	}
	entry := &documentBindingEntry{key: key, binding: binding}
	if t.ttl > 0 {
		entry.deadline = time.Now().Add(t.ttl)
	}
	t.m[key] = t.lru.PushFront(entry)
	worker.BindSession(sessionID, "document")
	t.enforceBoundLocked()
}

func (t *DocumentSessionTable) Invalidate(key string) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if el, ok := t.m[key]; ok {
		t.removeElementLocked(el)
	}
}

func (t *DocumentSessionTable) PurgeWorker(worker *AntigravityAcpWorker) {
	if t == nil || worker == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var doomed []*list.Element
	for el := t.lru.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*documentBindingEntry)
		if entry.binding != nil && entry.binding.Worker == worker {
			doomed = append(doomed, el)
		}
	}
	for _, el := range doomed {
		t.removeElementLocked(el)
	}
}

// PurgeAll drops every binding in the table. Test/diagnostics aid that
// mirrors what PurgeWorker does for one worker, without worker filtering
// (e.g. R4 tests simulating a late table-wide invalidation).
func (t *DocumentSessionTable) PurgeAll() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for el := t.lru.Front(); el != nil; el = t.lru.Front() {
		t.removeElementLocked(el)
	}
}

// PurgeAllForTest drops every binding; test-only naming keeps intent clear.
func (t *DocumentSessionTable) InvalidateAllForTest() { t.PurgeAll() }

// InvalidateAllForTest drops every strict-stateful binding; test-only
// helper for R4-style late invalidation.
func (t *StatefulSessionTable) InvalidateAllForTest() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for el := t.lru.Front(); el != nil; el = t.lru.Front() {
		t.removeElementLocked(el)
	}
}

// AcquireLease pins a live document binding while its worker/session is used
// by a request. LRU/TTL eviction defers until the returned release function.
func (t *DocumentSessionTable) AcquireLease(key string, worker *AntigravityAcpWorker, authKey, modelVariant string) (func(), bool) {
	if t == nil || key == "" {
		return func() {}, false
	}
	t.mu.Lock()
	el, ok := t.m[key]
	if !ok {
		t.mu.Unlock()
		return func() {}, false
	}
	entry := el.Value.(*documentBindingEntry)
	if entry.binding == nil || entry.binding.Worker != worker || entry.binding.AuthKey != authKey || (modelVariant != "" && entry.binding.ModelVariant != modelVariant) {
		t.mu.Unlock()
		return func() {}, false
	}
	entry.activeLeases++
	t.mu.Unlock()
	return func() { t.releaseLease(key, el) }, true
}

func (t *DocumentSessionTable) releaseLease(key string, el *list.Element) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current, ok := t.m[key]
	if !ok || current != el {
		return
	}
	entry := el.Value.(*documentBindingEntry)
	if entry.activeLeases > 0 {
		entry.activeLeases--
	}
	if entry.activeLeases == 0 && entry.evictionPending {
		t.removeElementLocked(el)
		return
	}
	t.enforceBoundLocked()
}

func (t *DocumentSessionTable) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lru.Len()
}

func (t *DocumentSessionTable) enforceBoundLocked() {
	if t.max <= 0 {
		return
	}
	for t.lru.Len() > t.max {
		var candidate *list.Element
		for el := t.lru.Back(); el != nil; el = el.Prev() {
			entry := el.Value.(*documentBindingEntry)
			if entry.activeLeases == 0 {
				candidate = el
				break
			}
			entry.evictionPending = true
		}
		if candidate == nil {
			return
		}
		t.removeLocked(candidate)
	}
}
