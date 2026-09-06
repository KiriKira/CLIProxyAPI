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
		w.readySessions = w.readySessions[1:]
	}
}

// dropPreparedSessionsLocked discards every cached session of the worker.
// Dropped sessions are abandoned server-side (no session/release RPC).
// Callers must hold w.mu.
func (w *AntigravityAcpWorker) dropPreparedSessionsLocked() {
	w.readySessions = nil
}

// acpRefillTimeout bounds one background session/new call so a wedged
// daemon cannot leak refill goroutines.
const acpRefillTimeout = 30 * time.Second

// refillPreparedSessions keeps fresh sessions ready on an idle worker.
// It never blocks the request path: it runs on a goroutine kicked from
// Release (and worker creation). Failures are logged and retried on the
// next release/idle tick.
func (p *AntigravityAcpPool) refillPreparedSessions(w *AntigravityAcpWorker) {
	if p == nil || w == nil || p.sessionPrepare == nil {
		return
	}
	w.mu.Lock()
	if w.dead || w.inUse || w.refilling || len(w.readySessions) >= p.prepareLimit {
		w.mu.Unlock()
		return
	}
	w.refilling = true
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.refilling = false
		w.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), acpRefillTimeout)
	defer cancel()
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
	w.mu.Lock()
	if w.dead {
		w.mu.Unlock()
		return
	}
	sess.CreatedAt = time.Now()
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
