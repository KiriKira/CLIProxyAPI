package helps

import "errors"

// ErrSessionCapReached means a worker is draining and cannot create another
// daemon-side ACP session. The caller should retry on a replacement worker.
var ErrSessionCapReached = errors.New("acp worker session cap reached")

// WorkerSessionStats describes daemon-side session pressure for one worker.
// CreatedTotal counts every successful session/new exactly once.
type WorkerSessionStats struct {
	CreatedTotal  uint64
	Prepared      int
	BoundStrict   int
	BoundDocument int
	Abandoned     int
	Draining      bool
	DrainReason   string
}

type workerSessionState uint8

const (
	workerSessionLeased workerSessionState = iota + 1
	workerSessionPrepared
	workerSessionBoundStrict
	workerSessionBoundDocument
)

func (w *AntigravityAcpWorker) initSessionLedgerLocked() {
	if w.sessions == nil {
		w.sessions = make(map[string]workerSessionState)
	}
}

func (w *AntigravityAcpWorker) markDrainingLocked(reason string) {
	if !w.draining {
		w.draining = true
	}
	if w.drainReason == "" && reason != "" {
		w.drainReason = reason
	}
}

// AllowSessionCreation reports whether another successful session/new may be
// attempted on this worker. The worker is marked draining at the hard cap so
// future fresh traffic is routed to a replacement while existing bindings can
// continue to use this worker.
func (w *AntigravityAcpWorker) AllowSessionCreation() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dead || w.draining {
		return false
	}
	if w.maxSessionsPerWorker > 0 && w.createdTotal >= uint64(w.maxSessionsPerWorker) {
		w.markDrainingLocked("session_cap")
		return false
	}
	return true
}

// RegisterSessionCreated records one successful session/new. mode is
// "prepared" for a background session and any other value for a request lease.
func (w *AntigravityAcpWorker) RegisterSessionCreated(sessionID, mode string) {
	if w == nil || sessionID == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.initSessionLedgerLocked()
	if _, exists := w.sessions[sessionID]; exists {
		return
	}
	state := workerSessionLeased
	if mode == "prepared" {
		state = workerSessionPrepared
	}
	w.sessions[sessionID] = state
	w.createdTotal++
	if state == workerSessionPrepared {
		w.prepared++
	}
	if w.maxSessionsPerWorker > 0 && w.createdTotal >= uint64(w.maxSessionsPerWorker) {
		w.markDrainingLocked("session_cap")
	}
}

// MarkPreparedSessionConsumed transitions a cached session to a leased session.
func (w *AntigravityAcpWorker) MarkPreparedSessionConsumed(sessionID string) {
	if w == nil || sessionID == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.markPreparedSessionConsumedLocked(sessionID)
}

func (w *AntigravityAcpWorker) markPreparedSessionConsumedLocked(sessionID string) {
	if w.sessions[sessionID] == workerSessionPrepared {
		w.sessions[sessionID] = workerSessionLeased
		w.prepared--
	}
}

// BindSession transitions a leased session into a strict or document binding.
func (w *AntigravityAcpWorker) BindSession(sessionID, scope string) {
	if w == nil || sessionID == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.initSessionLedgerLocked()
	old, ok := w.sessions[sessionID]
	if !ok {
		return
	}
	if old == workerSessionBoundStrict || old == workerSessionBoundDocument {
		return
	}
	if old == workerSessionPrepared {
		w.prepared--
	}
	state := workerSessionBoundStrict
	if scope == "document" {
		state = workerSessionBoundDocument
		w.boundDocument++
	} else {
		w.boundStrict++
	}
	w.sessions[sessionID] = state
}

func (w *AntigravityAcpWorker) abandonSessionLocked(sessionID string) bool {
	if sessionID == "" || w.sessions == nil {
		return false
	}
	state, ok := w.sessions[sessionID]
	if !ok {
		return false
	}
	delete(w.sessions, sessionID)
	switch state {
	case workerSessionPrepared:
		w.prepared--
	case workerSessionBoundStrict:
		w.boundStrict--
	case workerSessionBoundDocument:
		w.boundDocument--
	}
	w.abandoned++
	if w.maxAbandonedPerWorker > 0 && w.abandoned >= w.maxAbandonedPerWorker {
		w.markDrainingLocked("abandoned_cap")
	}
	return true
}

// AbandonSession drops the proxy's last reusable path to a session. There is
// no ACP session/release RPC, so the session remains provider-side until the
// worker is recycled.
func (w *AntigravityAcpWorker) AbandonSession(sessionID string) {
	if w == nil || sessionID == "" {
		return
	}
	w.mu.Lock()
	changed := w.abandonSessionLocked(sessionID)
	recyclable := changed && w.canRecycleLocked()
	pool := w.pool
	w.mu.Unlock()
	if recyclable && pool != nil {
		pool.tryRecycleWorker(w, "draining")
	}
}

// AbandonPreparedSessionsLocked discards cached sessions during worker
// retirement. The worker is going away, so these do not count against its
// abandoned-session budget.
func (w *AntigravityAcpWorker) AbandonPreparedSessionsLocked() {
	if w == nil {
		return
	}
	for _, session := range w.readySessions {
		if session == nil || w.sessions == nil {
			continue
		}
		if state, ok := w.sessions[session.SessionID]; ok && state == workerSessionPrepared {
			delete(w.sessions, session.SessionID)
			w.prepared--
		}
	}
	w.readySessions = nil
}

// SessionStats returns a consistent ledger snapshot.
func (w *AntigravityAcpWorker) SessionStats() WorkerSessionStats {
	if w == nil {
		return WorkerSessionStats{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return WorkerSessionStats{
		CreatedTotal:  w.createdTotal,
		Prepared:      w.prepared,
		BoundStrict:   w.boundStrict,
		BoundDocument: w.boundDocument,
		Abandoned:     w.abandoned,
		Draining:      w.draining,
		DrainReason:   w.drainReason,
	}
}

func (w *AntigravityAcpWorker) canRecycleLocked() bool {
	return w.draining && !w.dead && !w.inUse && !w.refilling &&
		len(w.readySessions) == 0 && w.boundStrict == 0 && w.boundDocument == 0
}

func (w *AntigravityAcpWorker) canRecycle() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.canRecycleLocked()
}
