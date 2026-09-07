package helps

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
)

// PreparedSession is a fresh, never-prompted ACP session created ahead of
// request arrival so the hot path can skip the session/new round trip
// (plus any session/set_config_option round trip when the variant differs).
// Pop one, prompt immediately, refill asynchronously while the worker idles.
type PreparedSession struct {
	SessionID string
	// Variant is the model variant this session was pre-configured with
	// (empty when the daemon default was left untouched). The hot path can
	// skip session/set_config_option entirely when the request resolves to
	// the same variant (model-aware preparation, P0.5).
	Variant string
	// CreatedAt supports eviction ordering (oldest first when trimming).
	CreatedAt time.Time
}

// AcquirePreparedSession pops a pre-created session for the worker, or nil
// when none is ready. Callers fall back to the normal openSession path.
// The returned session is removed from the cache; never hand the same
// session out twice.
func (w *AntigravityAcpWorker) AcquirePreparedSession() *PreparedSession {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.readySessions) > 0 {
		s := w.readySessions[0]
		w.readySessions = w.readySessions[1:]
		if s == nil || s.SessionID == "" {
			continue
		}
		w.markPreparedSessionConsumedLocked(s.SessionID)
		return s
	}
	return nil
}

// PutPreparedSession stores a freshly created session in the worker's
// ready cache, bounded by limit (oldest dropped when the cache overflows).
// Sessions are not released on the daemon: the client only exposes
// session/new, prompt and cancel, so a dropped prepared session is simply
// abandoned server-side (bounded by the cache limit).
func (w *AntigravityAcpWorker) PutPreparedSession(s *PreparedSession, limit int) {
	if w == nil || s == nil || s.SessionID == "" {
		return
	}
	if limit <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dead {
		return
	}
	w.readySessions = append(w.readySessions, s)
	w.trimPreparedSessionsLocked(limit)
}

// trimPreparedSessionsLocked evicts the oldest cached sessions down to
// limit. Dropped sessions are abandoned (no session/release RPC exists).
// Callers must hold w.mu.
func (w *AntigravityAcpWorker) trimPreparedSessionsLocked(limit int) {
	if limit <= 0 {
		w.readySessions = nil
		return
	}
	for len(w.readySessions) > limit {
		dropped := w.readySessions[0]
		w.readySessions = w.readySessions[1:]
		if dropped != nil {
			w.abandonSessionLocked(dropped.SessionID)
		}
	}
}

// dropPreparedSessionsLocked discards every cached session of the worker.
// Dropped sessions are abandoned server-side (no session/release RPC).
// Callers must hold w.mu.
func (w *AntigravityAcpWorker) dropPreparedSessionsLocked() {
	w.AbandonPreparedSessionsLocked()
}

// Workers returns a snapshot of the live workers for a key. Introspection
// aid for tests and diagnostics; order matches the internal slice.
func (p *AntigravityAcpPool) Workers(key string) []*AntigravityAcpWorker {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*AntigravityAcpWorker, len(p.workers[key]))
	copy(out, p.workers[key])
	return out
}

// SnapshotReadySessions returns a copy of the worker's cached prepared
// sessions. Introspection aid for tests and diagnostics.
func (w *AntigravityAcpWorker) SnapshotReadySessions() []*PreparedSession {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*PreparedSession, len(w.readySessions))
	copy(out, w.readySessions)
	return out
}

// SetPreferredVariant records the resolved model variant of the most recent
// request so the next prepared session is created with that variant already
// selected (model-aware preparation, P0.5).
func (w *AntigravityAcpWorker) SetPreferredVariant(variant string) {
	if w == nil || variant == "" {
		return
	}
	w.mu.Lock()
	w.preferredVariant = variant
	w.mu.Unlock()
}

// PreferredVariant returns the variant the next prepared session should
// target, or "" when unknown (daemon default applies).
func (w *AntigravityAcpWorker) PreferredVariant() string {
	if w == nil {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.preferredVariant
}

// acpRefillTimeout bounds one background session/new call so a wedged
// daemon cannot leak refill goroutines.
const acpRefillTimeout = 30 * time.Second

// acpRefillWaitBudget bounds how long an acquirer waits for an in-flight
// background refill to land before interrupting it. Short enough to keep
// TTFT bounded, long enough for a same-host daemon to answer session/new.
const acpRefillWaitBudget = 750 * time.Millisecond

// refillPreparedSessions keeps fresh sessions ready on an idle worker.
// It never overlaps request-critical ACP operations: a request that leases
// this worker first cancels the in-flight refill and waits for it to settle
// on refillFinished (P0.4). Failures are logged and retried on the next
// release/idle tick.
func (p *AntigravityAcpPool) refillPreparedSessions(w *AntigravityAcpWorker) {
	if p == nil || w == nil || p.sessionPrepare == nil {
		return
	}
	w.mu.Lock()
	if w.dead || w.draining || w.inUse || w.refilling || len(w.readySessions) >= p.prepareLimit {
		w.mu.Unlock()
		return
	}
	if w.maxSessionsPerWorker > 0 && w.createdTotal >= uint64(w.maxSessionsPerWorker) {
		w.markDrainingLocked("session_cap")
		w.mu.Unlock()
		return
	}
	variant := w.preferredVariant
	w.refilling = true
	finished := make(chan struct{})
	w.refillFinished = finished
	ctx, cancel := context.WithTimeout(context.Background(), acpRefillTimeout)
	w.refillCancel = cancel
	w.mu.Unlock()

	defer func() {
		cancel() // no-op when already fired
		w.mu.Lock()
		w.refilling = false
		w.refillCancel = nil
		w.refillFinished = nil
		w.mu.Unlock()
		// Close only after refilling=false is visible so an Acquire that
		// waited on finished never observes a stale refilling state.
		close(finished)
	}()

	sess, err := p.sessionPrepare(ctx, w)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.WithFields(map[string]interface{}{
				"provider": "antigravity-acp",
				"error":    err.Error(),
			}).Debug("ACP prepared-session refill failed")
		}
		return
	}
	if sess == nil || sess.SessionID == "" {
		return
	}
	w.RegisterSessionCreated(sess.SessionID, "prepared")
	w.mu.Lock()
	if w.dead {
		w.mu.Unlock()
		return
	}
	sess.CreatedAt = time.Now()
	if variant != "" {
		sess.Variant = variant
	}
	w.readySessions = append(w.readySessions, sess)
	w.trimPreparedSessionsLocked(p.prepareLimit)
	w.mu.Unlock()
}

// maybeRefill kicks a non-blocking refill goroutine for a worker that just
// became idle (or is freshly created). No-op when preparation is disabled.
func (p *AntigravityAcpPool) maybeRefill(w *AntigravityAcpWorker) {
	if p == nil || w == nil || p.sessionPrepare == nil || p.prepareLimit <= 0 {
		return
	}
	go p.refillPreparedSessions(w)
}
