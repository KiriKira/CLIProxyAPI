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

func TestAntigravityAcpPool_R1bAsyncRecycleReservesPerKeyCapacity(t *testing.T) {
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 0, 0, 0, 1, 0, func(context.Context, string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer func() { _ = pool.Close() }()

	closeStarted := make(chan struct{}, 1)
	allowClose := make(chan struct{})
	pool.closeWorkerAndWait = func(context.Context, *acp.Client) error {
		closeStarted <- struct{}{}
		<-allowClose
		return nil
	}

	w1, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	w1.RegisterSessionCreated("session-1", "fresh")
	table := NewStatefulSessionTable(0, 8)
	table.Bind("logical-1", "session-1", "auth", "model", w1, 0)
	pool.Release(w1, true)
	// Invalidate calls AbandonSession while holding the table lock. The
	// resulting recycle must defer its finalizer rather than re-entering it.
	table.Invalidate("logical-1")

	select {
	case <-closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("async recycle did not begin process-tree cleanup")
	}

	spawned := make(chan struct{}, 1)
	acquired := make(chan *AntigravityAcpWorker, 1)
	acquireErr := make(chan error, 1)
	go func() {
		worker, errAcquire := pool.Acquire(context.Background(), "k1", func(context.Context) (*acp.Client, error) {
			spawned <- struct{}{}
			return &acp.Client{}, nil
		})
		if errAcquire != nil {
			acquireErr <- errAcquire
			return
		}
		acquired <- worker
	}()
	select {
	case <-spawned:
		t.Fatal("same-key replacement spawned before async process cleanup")
	case <-acquireErr:
		t.Fatal("same-key acquire failed while retirement was pending")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowClose)
	select {
	case worker := <-acquired:
		if worker == w1 {
			t.Fatal("replacement returned retired worker")
		}
		pool.Release(worker, true)
	case err := <-acquireErr:
		t.Fatalf("replacement acquire: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not start after async cleanup")
	}
}

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

// TestAntigravityAcpPool_R1bWaitsForProcessTreeBeforeReplacement verifies
// that hard worker capacity is not handed to a replacement until the retired
// worker's owned process-tree close has completed.
func TestAntigravityAcpPool_R1bWaitsForProcessTreeBeforeReplacement(t *testing.T) {
	pool := r1TestPool(t, 1, nil)
	closeStarted := make(chan struct{}, 1)
	allowClose := make(chan struct{})
	pool.closeWorkerAndWait = func(ctx context.Context, client *acp.Client) error {
		closeStarted <- struct{}{}
		<-allowClose
		return nil
	}

	w1, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire initial worker: %v", err)
	}
	fillToSessionCap(t, pool, w1, 1)
	pool.Release(w1, true)

	acquireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	replacementStarted := make(chan struct{}, 1)
	acquired := make(chan *AntigravityAcpWorker, 1)
	acquireErr := make(chan error, 1)
	go func() {
		w2, errAcquire := pool.Acquire(acquireCtx, "k1", func(ctx context.Context) (*acp.Client, error) {
			replacementStarted <- struct{}{}
			return &acp.Client{}, nil
		})
		if errAcquire != nil {
			acquireErr <- errAcquire
			return
		}
		acquired <- w2
	}()

	select {
	case <-closeStarted:
	case errAcquire := <-acquireErr:
		t.Fatalf("replacement acquire failed before cleanup completed: %v", errAcquire)
	case <-time.After(2 * time.Second):
		t.Fatal("retirement did not start process-tree close")
	}
	select {
	case <-replacementStarted:
		t.Fatal("replacement spawned before old process-tree cleanup completed")
	default:
	}

	close(allowClose)
	select {
	case w2 := <-acquired:
		if w2 == w1 {
			t.Fatal("replacement acquire returned retired worker")
		}
		pool.Release(w2, true)
	case errAcquire := <-acquireErr:
		t.Fatalf("replacement acquire after cleanup failed: %v", errAcquire)
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not start after process-tree cleanup completed")
	}
}

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

// TestAntigravityAcpPool_R1ReleaseCrossKeyEvictionPurgesBothBindingTables
// covers the Release-specific cross-key eviction path. A bound idle worker is
// removed only after another auth/key is waiting, and both binding scopes must
// be purged exactly once.
func TestAntigravityAcpPool_R1ReleaseCrossKeyEvictionPurgesBothBindingTables(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 2)
	strict := NewStatefulSessionTable(0, 0)
	document := NewDocumentSessionTable(0, 0)
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 1, 0, 0, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	pool.SetWorkerRetiredHook(func(w *AntigravityAcpWorker) {
		strict.PurgeWorker(w)
		document.PurgeWorker(w)
		retiredCh <- w
	})
	t.Cleanup(func() { _ = pool.Close() })

	w1, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire k1: %v", err)
	}
	w1.RegisterSessionCreated("strict-session", "fresh")
	w1.RegisterSessionCreated("document-session", "fresh")
	strict.Bind("strict-key", "strict-session", "k1", "", w1, 0)
	document.Bind("document-key", "document-session", "k1", "", w1)

	acquireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acquired := make(chan *AntigravityAcpWorker, 1)
	acquireErr := make(chan error, 1)
	go func() {
		w2, errAcquire := pool.Acquire(acquireCtx, "k2", nil)
		if errAcquire != nil {
			acquireErr <- errAcquire
			return
		}
		acquired <- w2
	}()
	waitFor(t, time.Second, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return len(pool.waitQueues["k2"]) == 1
	})

	pool.Release(w1, true)
	select {
	case victim := <-retiredCh:
		if victim != w1 {
			t.Fatalf("retired victim = %p, want %p", victim, w1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Release cross-key eviction did not invoke retirement hook")
	}
	if got := strict.Len(); got != 0 {
		t.Fatalf("strict bindings after retirement = %d, want 0", got)
	}
	if got := document.Len(); got != 0 {
		t.Fatalf("document bindings after retirement = %d, want 0", got)
	}

	select {
	case w2 := <-acquired:
		pool.Release(w2, true)
	case errAcquire := <-acquireErr:
		t.Fatalf("waiting k2 acquire failed after slot release: %v", errAcquire)
	case <-time.After(2 * time.Second):
		t.Fatal("waiting k2 acquire did not make progress")
	}
	select {
	case duplicate := <-retiredCh:
		t.Fatalf("retirement hook called more than once for %p", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestAntigravityAcpPool_R1DeadReleasePurgesBinding verifies that an unhealthy
// Release also uses the unified retirement hook rather than silently dropping
// a worker with live strict/document bindings.
func TestAntigravityAcpPool_R1DeadReleasePurgesBinding(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 1)
	strict := NewStatefulSessionTable(0, 0)
	pool := r1TestPool(t, 0, func(w *AntigravityAcpWorker) {
		strict.PurgeWorker(w)
		retiredCh <- w
	})

	w, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	w.RegisterSessionCreated("dead-session", "fresh")
	strict.Bind("dead-key", "dead-session", "k1", "", w, 0)
	pool.Release(w, false)

	select {
	case victim := <-retiredCh:
		if victim != w {
			t.Fatalf("retired victim = %p, want %p", victim, w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dead Release did not invoke retirement hook")
	}
	if got := strict.Len(); got != 0 {
		t.Fatalf("strict bindings after dead release = %d, want 0", got)
	}
	select {
	case <-retiredCh:
		t.Fatal("dead Release invoked retirement hook more than once")
	case <-time.After(20 * time.Millisecond):
	}
}

// TestAntigravityAcpPool_R1IdleTimeoutUsesUnifiedRetirement verifies the
// idle-timeout path through the deterministic locked helper rather than
// waiting for the production ticker.
func TestAntigravityAcpPool_R1IdleTimeoutUsesUnifiedRetirement(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 1)
	pool := NewAntigravityAcpPool(1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	pool.SetWorkerRetiredHook(func(w *AntigravityAcpWorker) { retiredCh <- w })
	t.Cleanup(func() { _ = pool.Close() })
	pool.idleTimeout = time.Second

	w, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pool.Release(w, true)
	now := time.Unix(100, 0)
	w.mu.Lock()
	w.lastUsedAt = now.Add(-2 * time.Second)
	w.mu.Unlock()

	pool.mu.Lock()
	retirements := pool.expireIdleWorkersLocked(now)
	pool.mu.Unlock()
	pool.finishWorkerRetirements(retirements)

	select {
	case victim := <-retiredCh:
		if victim != w {
			t.Fatalf("retired victim = %p, want %p", victim, w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle timeout did not invoke retirement hook")
	}
	if got := len(pool.Workers("k1")); got != 0 {
		t.Fatalf("workers after idle timeout = %d, want 0", got)
	}
}

// TestAntigravityAcpPool_R1SessionAbandonRecycleUsesUnifiedRetirement
// covers the binding-free draining path, where AbandonSession directly asks
// the pool to recycle an idle worker.
func TestAntigravityAcpPool_R1SessionAbandonRecycleUsesUnifiedRetirement(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 1)
	pool := r1TestPool(t, 1, func(w *AntigravityAcpWorker) { retiredCh <- w })

	w, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pool.Release(w, true)
	w.RegisterSessionCreated("abandoned-session", "fresh")
	w.AbandonSession("abandoned-session")

	select {
	case victim := <-retiredCh:
		if victim != w {
			t.Fatalf("retired victim = %p, want %p", victim, w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session-abandon recycle did not invoke retirement hook")
	}
	if got := len(pool.Workers("k1")); got != 0 {
		t.Fatalf("workers after session-abandon recycle = %d, want 0", got)
	}
	select {
	case <-retiredCh:
		t.Fatal("session-abandon recycle invoked retirement hook more than once")
	case <-time.After(20 * time.Millisecond):
	}
}

// the unified retirement operation for every live worker.
func TestAntigravityAcpPool_R1ClosePurgesEveryWorker(t *testing.T) {
	retiredCh := make(chan *AntigravityAcpWorker, 2)
	pool := NewAntigravityAcpPoolWithLimits(1, 2, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	pool.SetWorkerRetiredHook(func(w *AntigravityAcpWorker) { retiredCh <- w })

	w1, err := pool.Acquire(context.Background(), "k1", nil)
	if err != nil {
		t.Fatalf("acquire k1: %v", err)
	}
	w2, err := pool.Acquire(context.Background(), "k2", nil)
	if err != nil {
		t.Fatalf("acquire k2: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seen := map[*AntigravityAcpWorker]bool{}
	for i := 0; i < 2; i++ {
		select {
		case worker := <-retiredCh:
			seen[worker] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("Close invoked %d retirement hooks, want 2", i)
		}
	}
	if !seen[w1] || !seen[w2] {
		t.Fatalf("Close retired workers = %#v, want both %p and %p", seen, w1, w2)
	}
	select {
	case worker := <-retiredCh:
		t.Fatalf("Close invoked retirement hook more than once for %p", worker)
	case <-time.After(20 * time.Millisecond):
	}
}
