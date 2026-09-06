package helps

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
	log "github.com/sirupsen/logrus"
)

// AntigravityAcpWorker represents a persistent daemon worker running the official ACP server.
type AntigravityAcpWorker struct {
	pool       *AntigravityAcpPool
	key        string
	client     *acp.Client
	createdAt  time.Time
	lastUsedAt time.Time

	mu      sync.Mutex
	inUse   bool
	dead    bool
	deadErr error

	// readySessions caches fresh, never-prompted sessions created
	// asynchronously while the worker idles, so a request can skip the
	// session/new round trip entirely. Populated only when the pool has
	// session preparation enabled (prepareLimit > 0).
	readySessions []*PreparedSession
	// refilling marks an in-flight background session/new for this worker,
	// preventing refill goroutine pile-up.
	refilling bool
	// refillCancel cancels the in-flight background refill so Acquire can
	// interrupt it before leasing the worker: request-critical ACP
	// operations must never overlap a background session/new (P0.4).
	refillCancel context.CancelFunc
	// refillFinished is closed once the in-flight refill has fully unwound;
	// Acquire waits on it (settle) after interrupting. Nil when no refill
	// is in flight.
	refillFinished chan struct{}
	// preferredVariant records the resolved model variant of the most recent
	// request so the next prepared session is created with that variant
	// already selected (P0.5, model-aware preparation).
	preferredVariant string
}

// Client returns the underlying *acp.Client.
func (w *AntigravityAcpWorker) Client() *acp.Client {
	if w == nil {
		return nil
	}
	return w.client
}

// MarkDead marks the worker as unusable so that Release or subsequent calls discard it.
func (w *AntigravityAcpWorker) MarkDead(err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dead = true
	if w.deadErr == nil {
		w.deadErr = err
	}
}

// AntigravityAcpPool manages persistent ACP daemon workers.
//
// Workers are grouped per auth key. A single auth may own several workers up
// to the configured limits, so one slow generation never makes a concurrent
// same-auth request inherit the full generation time as artificial queueing
// delay. The sum of workers across all keys is capped by maxTotal; each live
// worker holds one slot token from spawn until removal, which makes the cap
// strict even across per-key spawns.
type AntigravityAcpPool struct {
	mu         sync.Mutex
	workers    map[string][]*AntigravityAcpWorker
	spawning   map[string]chan struct{}
	waitQueues map[string][]chan *AntigravityAcpWorker
	// maxWorkers caps workers per auth key (per-key map growth bound).
	maxWorkers int
	// maxTotal caps the sum of workers across all keys. Zero means no
	// additional global bound beyond maxWorkers.
	maxTotal int
	// spawnGate holds one token per admitted worker (capacity maxTotal).
	// Spawning takes a token non-blockingly; a worker's removal returns it.
	spawnGate chan struct{}
	// slotFreed is broadcast (non-blocking send) whenever a global slot is
	// returned, so globally-blocked waiters can retry their spawn attempt.
	slotFreed   chan struct{}
	idleTimeout time.Duration
	closed      bool

	// prepareLimit bounds the per-worker cache of pre-created fresh
	// sessions (0 disables preparation). Sessions are created
	// asynchronously while the worker idles via sessionPrepare.
	prepareLimit int
	// sessionPrepare creates one fresh session on the worker's client.
	// It is called off the request path; errors are non-fatal.
	sessionPrepare func(ctx context.Context, w *AntigravityAcpWorker) (*PreparedSession, error)

	// Factory launches a new ACP client for the given key/identity.
	Factory func(ctx context.Context, key string) (*acp.Client, error)
}

// NewAntigravityAcpPool creates a new pool.
func NewAntigravityAcpPool(maxWorkers int, idleTimeout time.Duration, factory func(ctx context.Context, key string) (*acp.Client, error)) *AntigravityAcpPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	p := &AntigravityAcpPool{
		workers:     make(map[string][]*AntigravityAcpWorker),
		spawning:    make(map[string]chan struct{}),
		waitQueues:  make(map[string][]chan *AntigravityAcpWorker),
		maxWorkers:  maxWorkers,
		idleTimeout: idleTimeout,
		Factory:     factory,
	}
	if idleTimeout > 0 {
		go p.idleCleanupLoop()
	}
	return p
}

// NewAntigravityAcpPoolWithLimits creates a pool with a per-auth cap and a
// strict global cap. maxTotal <= 0 falls back to plain per-auth capping.
// prepareLimit > 0 enables asynchronous fresh-session preparation: idle
// workers keep up to prepareLimit ready sessions so requests skip the
// session/new round trip.
func NewAntigravityAcpPoolWithLimits(maxWorkersPerAuth, maxTotal, prepareLimit int, idleTimeout time.Duration, factory func(ctx context.Context, key string) (*acp.Client, error)) *AntigravityAcpPool {
	p := newAntigravityAcpPool(maxWorkersPerAuth, maxTotal, idleTimeout, factory)
	p.prepareLimit = prepareLimit
	if prepareLimit > 0 {
		p.sessionPrepare = defaultSessionPrepare
	}
	return p
}

// defaultSessionPrepare creates one fresh ACP session on the worker's
// client and pre-selects the worker's preferred model variant when one is
// known, so the hot path can skip session/set_config_option too (P0.5,
// model-aware preparation). Used when the pool is constructed with
// preparation enabled and no custom prepare function is injected (tests
// inject their own). A failed variant selection degrades gracefully: the
// session is still returned with an empty Variant and the request path
// applies (and properly surfaces) the model error itself.
func defaultSessionPrepare(ctx context.Context, w *AntigravityAcpWorker) (*PreparedSession, error) {
	client := w.Client()
	if client == nil {
		return nil, fmt.Errorf("acp worker client is nil")
	}
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		return nil, fmt.Errorf("acp prepared session/new failed: %w", err)
	}
	variant := w.PreferredVariant()
	if variant != "" && acp.CurrentModel(client.ConfigOptions(sessionID)) != variant {
		if _, err := client.SetConfigOption(ctx, sessionID, "model", variant); err != nil {
			log.WithFields(map[string]interface{}{
				"provider": "antigravity-acp",
				"variant":  variant,
				"error":    err.Error(),
			}).Debug("ACP prepared-session model pre-selection failed; request path will retry")
			return &PreparedSession{SessionID: sessionID}, nil
		}
	}
	return &PreparedSession{SessionID: sessionID, Variant: variant}, nil
}

// NewAntigravityAcpPool creates a new pool.

// newAntigravityAcpPool builds the base pool shared by all constructors.
func newAntigravityAcpPool(maxWorkers, maxTotal int, idleTimeout time.Duration, factory func(ctx context.Context, key string) (*acp.Client, error)) *AntigravityAcpPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	// Only a positive global cap is normalized upward: maxTotal <= 0 means
	// "no global cap; only per-auth caps apply" and must stay uncapped
	// (P0.1), so an unset max-workers-total can never turn into a real cap
	// and evict idle workers cross-key.
	if maxTotal > 0 && maxTotal < maxWorkers {
		maxTotal = maxWorkers
	}
	p := &AntigravityAcpPool{
		workers:     make(map[string][]*AntigravityAcpWorker),
		spawning:    make(map[string]chan struct{}),
		waitQueues:  make(map[string][]chan *AntigravityAcpWorker),
		maxWorkers:  maxWorkers,
		maxTotal:    maxTotal,
		idleTimeout: idleTimeout,
		Factory:     factory,
	}
	if maxTotal > 0 {
		p.spawnGate = make(chan struct{}, maxTotal)
		p.slotFreed = make(chan struct{}, 1)
	}
	if idleTimeout > 0 {
		go p.idleCleanupLoop()
	}
	return p
}

// notifySlotFreed signals globally-blocked waiters that a slot came back.
// Callers must hold p.mu; the signal itself is a non-blocking broadcast.
func (p *AntigravityAcpPool) notifySlotFreed() {
	if p.slotFreed == nil {
		return
	}
	select {
	case p.slotFreed <- struct{}{}:
	default:
	}
}

// totalWorkersLocked sums live workers across all keys. Callers must hold p.mu.
func (p *AntigravityAcpPool) totalWorkersLocked() int {
	total := 0
	for _, ws := range p.workers {
		total += len(ws)
	}
	return total
}

// globalSlotTryAcquire takes one global spawn slot without blocking. Callers
// must hold p.mu. Always false when no global cap is configured.
func (p *AntigravityAcpPool) globalSlotTryAcquire() bool {
	if p.maxTotal <= 0 {
		return true
	}
	select {
	case p.spawnGate <- struct{}{}:
		return true
	default:
		return false
	}
}

// globalSlotReleaseLocked returns one global spawn slot. Callers must hold p.mu.
func (p *AntigravityAcpPool) globalSlotReleaseLocked() {
	if p.maxTotal <= 0 {
		return
	}
	select {
	case <-p.spawnGate:
	default:
	}
	p.notifySlotFreed()
}

// removeWorkerLocked removes a worker from its per-key slice, reporting
// whether it was present. Callers must hold p.mu.
func (p *AntigravityAcpPool) removeWorkerLocked(w *AntigravityAcpWorker) bool {
	ws, ok := p.workers[w.key]
	if !ok {
		return false
	}
	for i, cand := range ws {
		if cand == w {
			rest := append(ws[:i], ws[i+1:]...)
			if len(rest) == 0 {
				delete(p.workers, w.key)
			} else {
				p.workers[w.key] = rest
			}
			return true
		}
	}
	return false
}

// closeClientAsync closes a client on a goroutine so pool locks are never
// held across blocking stdio shutdowns.
func closeClientAsync(c *acp.Client) {
	if c == nil {
		return
	}
	go func() { _ = c.Close() }()
}

// pruneDeadLocked removes dead workers of a key and closes their clients.
// It replaces the per-key slice wholesale; every removed worker returns its
// global slot. Callers must hold p.mu.
func (p *AntigravityAcpPool) pruneDeadLocked(key string) {
	ws := p.workers[key]
	live := make([]*AntigravityAcpWorker, 0, len(ws))
	for _, w := range ws {
		w.mu.Lock()
		dead := w.dead
		w.mu.Unlock()
		if dead {
			p.globalSlotReleaseLocked()
			closeClientAsync(w.client)
			continue
		}
		live = append(live, w)
	}
	if len(live) != len(ws) {
		if len(live) == 0 {
			delete(p.workers, key)
		} else {
			p.workers[key] = live
		}
	}
}

// evictOldestIdleFromOtherKeys frees one global slot by removing the oldest
// idle worker that belongs to a different key. Callers must hold p.mu.
func (p *AntigravityAcpPool) evictOldestIdleFromOtherKeys(key string) {
	if p.maxTotal <= 0 {
		return
	}
	var victim *AntigravityAcpWorker
	var oldest time.Time
	for k, ws := range p.workers {
		if k == key {
			continue
		}
		for _, cand := range ws {
			cand.mu.Lock()
			idle := !cand.inUse && !cand.dead
			last := cand.lastUsedAt
			cand.mu.Unlock()
			if idle && (victim == nil || last.Before(oldest)) {
				victim = cand
				oldest = last
			}
		}
	}
	if victim == nil {
		return
	}
	if p.removeWorkerLocked(victim) {
		p.globalSlotReleaseLocked()
	}
	closeClientAsync(victim.client)
}

// Acquire gets an exclusive lease on an active worker for the given key/identity.
// It reuses any idle worker of the auth key; when all matching workers are busy
// and capacity remains, a fresh worker is spawned; otherwise the caller waits
// on a per-key queue bound by ctx.
func (p *AntigravityAcpPool) Acquire(ctx context.Context, key string, spawnFn func(ctx context.Context) (*acp.Client, error)) (*AntigravityAcpWorker, error) {
	if p == nil {
		return nil, fmt.Errorf("acp pool is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("acp pool is closed")
		}

		// 1. If another goroutine is currently spawning a worker for this key, wait on barrier
		if spawnCh, isSpawning := p.spawning[key]; isSpawning {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-spawnCh:
				// Spawning finished (or failed), re-check in next iteration
				continue
			}
		}

		// 2. Reuse an idle worker for this key when one exists. A leased
		// worker must never run request-critical ACP operations while a
		// background refill session/new is still in flight (P0.4): cancel
		// the refill and wait for it to settle before handing out the lease.
		for _, w := range p.workers[key] {
			w.mu.Lock()
			if w.dead || w.inUse {
				w.mu.Unlock()
				continue
			}
			var finished chan struct{}
			var cancel context.CancelFunc
			if w.refilling {
				finished = w.refillFinished
				cancel = w.refillCancel
			}
			w.mu.Unlock()
			if finished != nil {
				// Give the in-flight refill a short budget to land its
				// session: a settled refill means the lease comes with a
				// ready prepared session (zero session/new on the hot
				// path). Past the budget, interrupt it and wait for it to
				// unwind so the leased worker never runs request-critical
				// ACP operations concurrently with a background refill
				// (P0.4 invariant holds on every path).
				select {
				case <-finished:
				case <-time.After(acpRefillWaitBudget):
					if cancel != nil {
						cancel()
					}
					<-finished
				}
			}
			w.mu.Lock()
			if w.dead || w.inUse || w.refilling {
				// Lost the race (another acquirer took it, it died, or the
				// settled refill already restarted); retry the scan.
				w.mu.Unlock()
				continue
			}
			// Re-check ctx before giving out the lease
			if err := ctx.Err(); err != nil {
				w.mu.Unlock()
				p.mu.Unlock()
				return nil, err
			}
			w.inUse = true
			w.lastUsedAt = time.Now()
			w.mu.Unlock()
			p.mu.Unlock()
			return w, nil
		}

		// 3. All matching workers are busy (or none exist). Spawn a fresh one
		// when per-auth capacity remains and the strict global cap allows it.
		p.pruneDeadLocked(key)
		if len(p.workers[key]) < p.maxWorkers {
			// Take exactly one global slot for the whole spawn attempt. When
			// the cap is full, try to free one by evicting the oldest idle
			// worker from another key, then take the slot once.
			spawnOK := false
			if p.globalSlotTryAcquire() {
				spawnOK = true
			} else {
				p.evictOldestIdleFromOtherKeys(key)
				spawnOK = p.globalSlotTryAcquire()
			}
			if spawnOK {
				// Set spawning barrier for this key
				spawnCh := make(chan struct{})
				p.spawning[key] = spawnCh
				p.mu.Unlock()

				// Launch the worker outside of lock
				client, err := p.launchSpawn(ctx, key, spawnFn)

				p.mu.Lock()
				delete(p.spawning, key)
				close(spawnCh)

				if err != nil {
					p.globalSlotReleaseLocked()
					p.mu.Unlock()
					return nil, err
				}

				if p.closed {
					p.globalSlotReleaseLocked()
					p.mu.Unlock()
					closeClientAsync(client)
					return nil, fmt.Errorf("acp pool is closed")
				}

				if err := ctx.Err(); err != nil {
					// Caller canceled during spawn. Save the fresh worker as
					// idle so other waiters/callers can use it. The worker
					// owns its global slot from here on.
					worker := &AntigravityAcpWorker{
						pool:       p,
						key:        key,
						client:     client,
						createdAt:  time.Now(),
						lastUsedAt: time.Now(),
						inUse:      false,
					}
					p.workers[key] = append(p.workers[key], worker)
					// Handoff to any waiter immediately
					if q := p.waitQueues[key]; len(q) > 0 {
						next := q[0]
						p.waitQueues[key] = q[1:]
						worker.inUse = true
						next <- worker
					}
					p.maybeRefill(worker)
					p.mu.Unlock()
					return nil, err
				}

				worker := &AntigravityAcpWorker{
					pool:       p,
					key:        key,
					client:     client,
					createdAt:  time.Now(),
					lastUsedAt: time.Now(),
					inUse:      true,
				}
				p.workers[key] = append(p.workers[key], worker)
				p.mu.Unlock()

				return worker, nil
			}
		}

		// 4. Capacity exhausted for this key (or globally): join the per-key
		// wait queue. Release hands a worker back or wakes us to retry.
		waitCh := make(chan *AntigravityAcpWorker, 1)
		p.waitQueues[key] = append(p.waitQueues[key], waitCh)
		var slotFreed <-chan struct{}
		if p.maxTotal > 0 {
			slotFreed = p.slotFreed
		}
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.mu.Lock()
			q := p.waitQueues[key]
			for i, ch := range q {
				if ch == waitCh {
					p.waitQueues[key] = append(q[:i], q[i+1:]...)
					break
				}
			}
			p.mu.Unlock()
			return nil, ctx.Err()
		case worker, ok := <-waitCh:
			if !ok || worker == nil {
				// Woken up because a worker died or pool was closed, loop to retry acquire or observe closure
				continue
			}
			return worker, nil
		case <-slotFreed:
			// A global slot may have come back: retry the whole acquire loop.
			continue
		}
	}
}

// launchSpawn resolves the spawn function and builds the client.
func (p *AntigravityAcpPool) launchSpawn(ctx context.Context, key string, spawnFn func(ctx context.Context) (*acp.Client, error)) (*acp.Client, error) {
	if spawnFn != nil {
		return spawnFn(ctx)
	}
	if p.Factory != nil {
		return p.Factory(ctx, key)
	}
	return nil, fmt.Errorf("acp pool factory is nil")
}

// Release returns the worker to the pool or hands it off to the next waiting caller.
func (p *AntigravityAcpPool) Release(worker *AntigravityAcpWorker, healthy bool) {
	if p == nil || worker == nil {
		return
	}

	worker.mu.Lock()
	if !healthy {
		worker.dead = true
	}
	isDead := worker.dead
	worker.inUse = false
	worker.lastUsedAt = time.Now()
	worker.mu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || isDead {
		// Remove from the pool and reclaim the global slot exactly once,
		// tied to actual map membership so double releases stay harmless.
		if p.removeWorkerLocked(worker) {
			p.globalSlotReleaseLocked()
		}
		worker.mu.Lock()
		worker.dropPreparedSessionsLocked()
		worker.mu.Unlock()
		closeClientAsync(worker.client)

		// If there are waiters for this key and worker died, wake all up to re-acquire / spawn fresh worker
		if q := p.waitQueues[worker.key]; len(q) > 0 {
			for _, ch := range q {
				close(ch)
			}
			delete(p.waitQueues, worker.key)
		}
		return
	}

	// Check if there are waiters for this key
	if q := p.waitQueues[worker.key]; len(q) > 0 {
		next := q[0]
		p.waitQueues[worker.key] = q[1:]
		worker.mu.Lock()
		worker.inUse = true
		worker.lastUsedAt = time.Now()
		worker.mu.Unlock()
		next <- worker
		return
	}

	// Worker went idle: top up its prepared-session cache off the hot path
	// so the next request skips session/new entirely.
	p.maybeRefill(worker)

	// Under a global cap with no same-key waiter, an idle worker of this key
	// starves other keys: the cap stays full and cross-key waiters only wake
	// on slot removal. Evict this now-idle worker (returning its slot) so a
	// blocked other-key acquirer can spawn; it will be recreated on demand.
	if p.maxTotal > 0 && p.totalWorkersLocked() >= p.maxTotal {
		hasOtherWaiters := false
		for k, q := range p.waitQueues {
			if k != worker.key && len(q) > 0 {
				hasOtherWaiters = true
				break
			}
		}
		if hasOtherWaiters && p.removeWorkerLocked(worker) {
			p.globalSlotReleaseLocked()
			closeClientAsync(worker.client)
		}
	}
}

// Close gracefully closes all workers in the pool.
func (p *AntigravityAcpPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	// Wake all waiters
	for k, q := range p.waitQueues {
		for _, ch := range q {
			close(ch)
		}
		delete(p.waitQueues, k)
	}

	workersToClose := make([]*acp.Client, 0, p.totalWorkersLocked())
	for k, ws := range p.workers {
		for _, w := range ws {
			if w != nil {
				w.mu.Lock()
				w.dropPreparedSessionsLocked()
				w.mu.Unlock()
				if w.client != nil {
					workersToClose = append(workersToClose, w.client)
				}
			}
		}
		delete(p.workers, k)
	}
	if p.maxTotal > 0 {
		// All slots are reclaimed at once; the pool never admits again.
		p.globalSlotReleaseLockedAll()
	}
	p.mu.Unlock()

	for _, c := range workersToClose {
		_ = c.Close()
	}
	return nil
}

// globalSlotReleaseLockedAll drains every global slot token. Callers must hold p.mu.
func (p *AntigravityAcpPool) globalSlotReleaseLockedAll() {
	if p.maxTotal <= 0 {
		return
	}
	for {
		select {
		case <-p.spawnGate:
			continue
		default:
		}
		break
	}
	p.notifySlotFreed()
}

func (p *AntigravityAcpPool) idleCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		if p.idleTimeout <= 0 {
			p.mu.Unlock()
			continue
		}

		now := time.Now()
		for k, ws := range p.workers {
			live := make([]*AntigravityAcpWorker, 0, len(ws))
			for _, w := range ws {
				w.mu.Lock()
				expired := !w.inUse && now.Sub(w.lastUsedAt) > p.idleTimeout
				if expired {
					w.dead = true
				}
				w.mu.Unlock()
				if expired {
					p.globalSlotReleaseLocked()
					client := w.client
					go func(key string, c *acp.Client) {
						log.Infof("ACP persistent worker for %s idle-timed out; closing", key)
						if c != nil {
							_ = c.Close()
						}
					}(k, client)
					continue
				}
				live = append(live, w)
			}
			if len(live) != len(ws) {
				if len(live) == 0 {
					delete(p.workers, k)
				} else {
					p.workers[k] = live
				}
			}
		}
		p.mu.Unlock()
	}
}
