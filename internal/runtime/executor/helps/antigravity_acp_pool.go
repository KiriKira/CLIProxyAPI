package helps

import (
	"context"
	"fmt"
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
type AntigravityAcpPool struct {
	mu          sync.Mutex
	workers     map[string]*AntigravityAcpWorker
	waitQueues  map[string][]chan *AntigravityAcpWorker
	maxWorkers  int
	idleTimeout time.Duration
	closed      bool

	// Factory launches a new ACP client for the given key/identity.
	Factory func(ctx context.Context, key string) (*acp.Client, error)
}

// NewAntigravityAcpPool creates a new pool.
func NewAntigravityAcpPool(maxWorkers int, idleTimeout time.Duration, factory func(ctx context.Context, key string) (*acp.Client, error)) *AntigravityAcpPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	p := &AntigravityAcpPool{
		workers:     make(map[string]*AntigravityAcpWorker),
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

// Acquire gets an exclusive lease on an active worker for the given key/identity.
// If an existing worker is idle, it is reused. If busy, callers wait on a queue bound by ctx.
func (p *AntigravityAcpPool) Acquire(ctx context.Context, key string, spawnFn func(ctx context.Context) (*acp.Client, error)) (*AntigravityAcpWorker, error) {
	if p == nil {
		return nil, fmt.Errorf("acp pool is nil")
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("acp pool is closed")
	}

	// Check if a worker exists for this key
	w, exists := p.workers[key]
	if exists && !w.dead {
		w.mu.Lock()
		if !w.inUse && !w.dead {
			w.inUse = true
			w.lastUsedAt = time.Now()
			w.mu.Unlock()
			p.mu.Unlock()
			return w, nil
		}
		w.mu.Unlock()

		// Worker is busy, join the wait queue for this key
		waitCh := make(chan *AntigravityAcpWorker, 1)
		p.waitQueues[key] = append(p.waitQueues[key], waitCh)
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.mu.Lock()
			// Remove from waitQueue
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
				return nil, fmt.Errorf("acp pool closed or wait canceled")
			}
			return worker, nil
		}
	}

	// If a dead worker was present, remove and close it
	if exists && w.dead {
		delete(p.workers, key)
		go func(oldClient *acp.Client) {
			if oldClient != nil {
				_ = oldClient.Close()
			}
		}(w.client)
	}

	// If we are at capacity across all keys, clean up idle workers from other keys
	if len(p.workers) >= p.maxWorkers {
		var evictedKey string
		var evictedWorker *AntigravityAcpWorker
		var oldestIdle time.Time

		for k, cand := range p.workers {
			cand.mu.Lock()
			if !cand.inUse {
				if evictedWorker == nil || cand.lastUsedAt.Before(oldestIdle) {
					evictedKey = k
					evictedWorker = cand
					oldestIdle = cand.lastUsedAt
				}
			}
			cand.mu.Unlock()
		}

		if evictedWorker != nil {
			delete(p.workers, evictedKey)
			go func(c *acp.Client) {
				if c != nil {
					_ = c.Close()
				}
			}(evictedWorker.client)
		}
	}

	p.mu.Unlock()

	var client *acp.Client
	var err error
	if spawnFn != nil {
		client, err = spawnFn(ctx)
	} else if p.Factory != nil {
		client, err = p.Factory(ctx, key)
	} else {
		return nil, fmt.Errorf("acp pool factory is nil")
	}

	if err != nil {
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

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		return nil, fmt.Errorf("acp pool is closed")
	}
	p.workers[key] = worker
	p.mu.Unlock()

	return worker, nil
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
		if cur, ok := p.workers[worker.key]; ok && cur == worker {
			delete(p.workers, worker.key)
		}
		go func(c *acp.Client) {
			if c != nil {
				_ = c.Close()
			}
		}(worker.client)

		// If there are waiters for this key and worker died, wake one up so it can trigger a fresh spawn
		if q := p.waitQueues[worker.key]; len(q) > 0 {
			next := q[0]
			p.waitQueues[worker.key] = q[1:]
			close(next) // Wakes up waiter to retry / observe closure
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

	workersToClose := make([]*acp.Client, 0, len(p.workers))
	for k, w := range p.workers {
		if w != nil && w.client != nil {
			workersToClose = append(workersToClose, w.client)
		}
		delete(p.workers, k)
	}
	p.mu.Unlock()

	for _, c := range workersToClose {
		_ = c.Close()
	}
	return nil
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
		for k, w := range p.workers {
			w.mu.Lock()
			if !w.inUse && now.Sub(w.lastUsedAt) > p.idleTimeout {
				w.dead = true
				delete(p.workers, k)
				w.mu.Unlock()
				go func(c *acp.Client) {
					if c != nil {
						log.Infof("ACP persistent worker for %s idle-timed out; closing", k)
						_ = c.Close()
					}
				}(w.client)
			} else {
				w.mu.Unlock()
			}
		}
		p.mu.Unlock()
	}
}
