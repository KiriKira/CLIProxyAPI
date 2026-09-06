package helps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// TestAntigravityAcpPool_SameAuthSpawnsSecondWorkerWhenBusy verifies that a
// second same-auth request spawns another worker instead of queueing behind
// the busy one, up to the per-auth cap, and that the third request queues
// (gets cancelled while waiting) rather than exceeding the cap.
func TestAntigravityAcpPool_SameAuthSpawnsSecondWorkerWhenBusy(t *testing.T) {
	var spawnCount atomic.Int32
	pool := NewAntigravityAcpPool(2, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}
	w2, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 2 (while w1 busy) failed: %v", err)
	}
	if w1 == w2 {
		t.Fatal("expected distinct workers for concurrent same-auth requests")
	}
	if got := spawnCount.Load(); got != 2 {
		t.Fatalf("spawnCount = %d, want 2", got)
	}

	// A third request must queue, not spawn a third worker.
	ctxQ, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		w3, acqErr := pool.Acquire(ctxQ, "auth-1", nil)
		if acqErr == nil {
			pool.Release(w3, true)
		}
		done <- acqErr
	}()
	time.Sleep(50 * time.Millisecond)
	if got := spawnCount.Load(); got != 2 {
		cancel()
		t.Fatalf("spawnCount = %d while third request waits, want 2 (per-auth cap)", got)
	}
	// Cancel the queued waiter: it must return promptly with ctx.Err.
	cancel()
	if err := <-done; err == nil {
		t.Fatal("queued waiter unexpectedly acquired a worker")
	}

	pool.Release(w1, true)
	pool.Release(w2, true)
}

// TestAntigravityAcpPool_GlobalCapStrict verifies that
// NewAntigravityAcpPoolWithLimits refuses to spawn beyond the global total
// even across different keys, and admits a waiting key after a slot frees up.
func TestAntigravityAcpPool_GlobalCapStrict(t *testing.T) {
	var spawnCount atomic.Int32
	pool := NewAntigravityAcpPoolWithLimits(1, 2, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	wa, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire a failed: %v", err)
	}
	wb, err := pool.Acquire(ctx, "b", nil)
	if err != nil {
		t.Fatalf("Acquire b failed: %v", err)
	}

	// Third key must not spawn while the global cap is reached. It should
	// block until a slot frees, then acquire successfully.
	results := make(chan error, 1)
	go func() {
		wc, acqErr := pool.Acquire(ctx, "c", nil)
		if acqErr == nil {
			pool.Release(wc, true)
		}
		results <- acqErr
	}()

	// Let the c-acquirer park in the wait queue, then free a slot.
	time.Sleep(50 * time.Millisecond)
	pool.Release(wa, true)

	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("Acquire c after slot freed failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire c never completed after slot release")
	}

	if got := spawnCount.Load(); got != 3 {
		t.Fatalf("spawnCount = %d, want 3 (a, b, then c after slot freed)", got)
	}
	pool.mu.Lock()
	total := pool.totalWorkersLocked()
	pool.mu.Unlock()
	if total > 2 {
		t.Fatalf("live workers = %d, exceeds global cap 2", total)
	}
	pool.Release(wb, true)
}

// TestAntigravityAcpPool_GlobalCapEvictsOldestIdleFromOtherKey verifies that
// when the global cap is full, an idle worker of another key is evicted to
// admit the incoming key instead of leaving it queued forever.
func TestAntigravityAcpPool_GlobalCapEvictsOldestIdleFromOtherKey(t *testing.T) {
	var spawnCount atomic.Int32
	pool := NewAntigravityAcpPoolWithLimits(1, 2, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	wa, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire a failed: %v", err)
	}
	wb, err := pool.Acquire(ctx, "b", nil)
	if err != nil {
		t.Fatalf("Acquire b failed: %v", err)
	}
	// b goes idle; a stays busy.
	pool.Release(wb, true)

	// c should evict idle b and spawn immediately, without queueing.
	wc, err := pool.Acquire(ctx, "c", nil)
	if err != nil {
		t.Fatalf("Acquire c failed: %v", err)
	}
	if got := spawnCount.Load(); got != 3 {
		t.Fatalf("spawnCount = %d, want 3 (eviction should admit c)", got)
	}
	pool.mu.Lock()
	total := pool.totalWorkersLocked()
	_, bTracked := pool.workers["b"]
	pool.mu.Unlock()
	if total != 2 {
		t.Fatalf("live workers = %d, want 2", total)
	}
	if bTracked {
		t.Fatal("idle worker b should have been evicted")
	}
	pool.Release(wa, true)
	pool.Release(wc, true)
}

// TestAntigravityAcpPool_WaitQueueWakesOnHealthyHandoff keeps the established
// contract: a queued same-auth waiter receives the released worker directly.
func TestAntigravityAcpPool_WaitQueueWakesOnHealthyHandoff(t *testing.T) {
	var spawnCount atomic.Int32
	pool := NewAntigravityAcpPoolWithLimits(1, 1, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k", nil)
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}

	result := make(chan *AntigravityAcpWorker, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w, acqErr := pool.Acquire(ctx, "k", nil)
		if acqErr == nil {
			result <- w
		}
	}()
	time.Sleep(30 * time.Millisecond)
	pool.Release(w1, true)

	select {
	case w := <-result:
		if w != w1 {
			t.Fatal("expected handoff of the same worker")
		}
		pool.Release(w, true)
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never received the released worker")
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawnCount = %d, want 1", got)
	}
}
