package helps

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// r1TestPool builds a single-worker pool with hard session budgets and a
// spawn-counting factory, wired for R1 forward-progress tests.
func r1TestPool(t *testing.T, maxSessionsPerWorker int, retired func(*AntigravityAcpWorker)) *AntigravityAcpPool {
	t.Helper()
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 0, 0, 0, maxSessionsPerWorker, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	if retired != nil {
		pool.SetWorkerRetiredHook(retired)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// fillToSessionCap drives the given worker to its max-sessions cap so it
// becomes draining while holding boundDocument bindings.
func fillToSessionCap(t *testing.T, pool *AntigravityAcpPool, w *AntigravityAcpWorker, capSessions int) {
	t.Helper()
	for i := 0; i < capSessions; i++ {
		w.RegisterSessionCreated("session-bound", "fresh")
	}
	w.BindSession("session-bound", "document")
	stats := w.SessionStats()
	if !stats.Draining || stats.BoundDocument != 1 {
		t.Fatalf("worker did not reach capped+bound state: %+v", stats)
	}
}

// TestAntigravityAcpPool_R1SingleWorkerFreshProgressAfterCap is the R1 exit
// criterion: maxWorkers=1, maxSessionsPerWorker=1. A worker that reached its
// session cap while holding a bound document session must not deadlock a
// fresh-session request; the request receives a replacement worker promptly.
func TestAntigravityAcpPool_R1SingleWorkerFreshProgressAfterCap(t *testing.T) {
	pool := r1TestPool(t, 1, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	fillToSessionCap(t, pool, w1, 1)
	pool.Release(w1, true)

	spawned := make(chan struct{}, 1)
	w2, err := pool.Acquire(ctx, "k1", func(ctx context.Context) (*acp.Client, error) {
		spawned <- struct{}{}
		return &acp.Client{}, nil
	})
	if err != nil {
		t.Fatalf("fresh acquire after cap must not deadlock (R1): %v", err)
	}
	select {
	case <-spawned:
	case <-time.After(100 * time.Millisecond):
		// w2 may reuse an existing replacement; not an error.
	}
	if w2 == w1 {
		t.Fatalf("fresh demand received the capped draining worker")
	}
	if w2.SessionStats().Draining {
		t.Fatalf("replacement worker must not be draining: %+v", w2.SessionStats())
	}
	pool.Release(w2, true)
}

// TestAntigravityAcpPool_R1ForcedRetirementPurgesBindingsViaHook verifies
// that a forced retirement of an idle draining worker invokes the retired
// hook exactly once, so the owner can purge strict/document bindings.
func TestAntigravityAcpPool_R1ForcedRetirementPurgesBindingsViaHook(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 4)
	pool := r1TestPool(t, 1, func(w *AntigravityAcpWorker) { retiredCh <- w })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	fillToSessionCap(t, pool, w1, 1)
	pool.Release(w1, true)

	w2, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("fresh acquire after cap: %v", err)
	}
	select {
	case victim := <-retiredCh:
		if victim != w1 {
			t.Fatalf("retired victim mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("forced retirement never invoked the purge hook (R1)")
	}
	if w2 == w1 {
		t.Fatalf("fresh demand received the retired worker")
	}
	pool.Release(w2, true)
}

// TestAntigravityAcpPool_R1NeverForcesInUseWorker verifies the safety rule:
// forced retirement only ever targets idle draining workers, never one that
// is currently serving a prompt.
func TestAntigravityAcpPool_R1NeverForcesInUseWorker(t *testing.T) {
	retired := make(chan *AntigravityAcpWorker, 4)
	pool := r1TestPool(t, 1, func(w *AntigravityAcpWorker) { retired <- w })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	fillToSessionCap(t, pool, w1, 1)
	// Keep the worker leased (inUse): a second acquire must wait, not kill.
	go func() {
		w, err := pool.Acquire(ctx, "k1", nil)
		if err == nil {
			pool.Release(w, true)
		}
	}()

	select {
	case victim := <-retired:
		t.Fatalf("in-use worker was force-retired: %v", victim != nil)
	case <-time.After(300 * time.Millisecond):
		// Correct: nothing retired while the worker is in use.
	}
	pool.Release(w1, true)
	// Now the release path may retire it; that is allowed.
	select {
	case <-retired:
	case <-time.After(2 * time.Second):
	}
}

// TestAntigravityAcpPool_R1EvictedIdleWorkerPurgesBindings covers the
// cross-key eviction path: an evicted idle worker that still owns bindings
// must also go through the retired hook.
func TestAntigravityAcpPool_R1EvictedIdleWorkerPurgesBindings(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 4)
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 1, 0, 0, 1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	pool.SetWorkerRetiredHook(func(w *AntigravityAcpWorker) { retiredCh <- w })
	t.Cleanup(func() { _ = pool.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire k1: %v", err)
	}
	fillToSessionCap(t, pool, w1, 1)
	pool.Release(w1, true)

	w2, err := pool.Acquire(ctx, "k2", nil)
	if err != nil {
		t.Fatalf("acquire k2 under global cap: %v", err)
	}
	select {
	case victim := <-retiredCh:
		if victim != w1 {
			t.Fatalf("evicted victim mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("evicted bound worker did not purge bindings")
	}
	pool.Release(w2, true)
}
