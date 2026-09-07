package helps

import (
	"container/list"
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
	key      string
	binding  *DocumentACPBinding
	deadline time.Time
}

// DocumentSessionTable is a bounded document-key -> ACP-session registry.
// Each key also has a mutex so bootstrap and prompts for one document are
// serialized; different documents remain independent lanes.
type DocumentSessionTable struct {
	mu    sync.Mutex
	m     map[string]*list.Element
	lru   *list.List
	ttl   time.Duration
	max   int
	lanes map[string]*sync.Mutex
}

func NewDocumentSessionTable(ttl time.Duration, max int) *DocumentSessionTable {
	return &DocumentSessionTable{
		m:     make(map[string]*list.Element),
		lru:   list.New(),
		ttl:   ttl,
		max:   max,
		lanes: make(map[string]*sync.Mutex),
	}
}

// LockLane serializes all work for one document key and returns its unlock
// function. The key is expected to be a canonical, namespaced key.
func (t *DocumentSessionTable) LockLane(key string) func() {
	if t == nil || key == "" {
		return func() {}
	}
	t.mu.Lock()
	lane := t.lanes[key]
	if lane == nil {
		lane = &sync.Mutex{}
		t.lanes[key] = lane
	}
	t.mu.Unlock()
	lane.Lock()
	return lane.Unlock
}

func (t *DocumentSessionTable) removeLocked(el *list.Element) {
	entry := el.Value.(*documentBindingEntry)
	t.lru.Remove(el)
	delete(t.m, entry.key)
	if entry.binding != nil && entry.binding.Worker != nil {
		entry.binding.Worker.AbandonSession(entry.binding.ACPSessionID)
	}
}

func (t *DocumentSessionTable) getLocked(key string) (*DocumentACPBinding, bool) {
	el, ok := t.m[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*documentBindingEntry)
	if t.ttl > 0 && time.Now().After(entry.deadline) {
		t.removeLocked(el)
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
		t.removeLocked(el)
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
		t.removeLocked(el)
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
		t.removeLocked(el)
	}
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
		back := t.lru.Back()
		if back == nil {
			return
		}
		t.removeLocked(back)
	}
}
