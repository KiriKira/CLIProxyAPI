package helps

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// TestAntigravityAcpPool_P0_Fixes verifies the three P0 fixes:
// 1. Spawning barrier prevents duplicate spawns for same key under cold concurrency.
// 2. Pre-check ctx.Err() prevents cancelled context from holding an idle worker.
// 3. Dead worker wake-up wakes all queued waiters so they retry cleanly.
func TestAntigravityAcpPool_P0_SpawningBarrier(t *testing.T) {
	var spawnCount atomic.Int32
	spawnStarted := make(chan struct{}, 2)
	spawnBlock := make(chan struct{})

	pool := NewAntigravityAcpPool(2, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		spawnStarted <- struct{}{}
		<-spawnBlock
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type res struct {
		w   *AntigravityAcpWorker
		err error
	}
	results := make(chan res, 2)

	// Launch two concurrent Acquire calls for same key
	go func() {
		w, err := pool.Acquire(ctx, "k1", nil)
		results <- res{w, err}
	}()
	go func() {
		w, err := pool.Acquire(ctx, "k1", nil)
		results <- res{w, err}
	}()

	// Only 1 spawn should start due to spawning barrier
	select {
	case <-spawnStarted:
	case <-ctx.Done():
		t.Fatal("timed out waiting for spawn to start")
	}

	// Ensure no second spawn started
	select {
	case <-spawnStarted:
		t.Fatal("second spawn started concurrently for same key! Spawning barrier broken")
	case <-time.After(50 * time.Millisecond):
		// Expected: second caller is waiting on barrier
	}

	// Unblock spawn
	close(spawnBlock)

	// One should get the spawned worker, the second will either queue or receive it sequentially
	r1 := <-results
	if r1.err != nil || r1.w == nil {
		t.Fatalf("r1 failed: %v", r1.err)
	}

	// Release r1 so second caller can acquire
	pool.Release(r1.w, true)

	r2 := <-results
	if r2.err != nil || r2.w == nil {
		t.Fatalf("r2 failed: %v", r2.err)
	}
	pool.Release(r2.w, true)

	if spawnCount.Load() != 1 {
		t.Fatalf("expected exactly 1 spawn, got %d", spawnCount.Load())
	}
}

func TestAntigravityAcpPool_P0_CancelledContextDoesNotHoldIdleWorker(t *testing.T) {
	pool := NewAntigravityAcpPool(1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}
	pool.Release(w1, true)

	// Acquire with already-cancelled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	w2, err := pool.Acquire(canceledCtx, "k1", nil)
	if err == nil {
		t.Fatal("expected error with cancelled context, got nil")
	}
	if w2 != nil {
		t.Fatal("expected nil worker with cancelled context")
	}

	// Verify worker was NOT marked inUse and remains immediately acquirable
	w3, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("subsequent acquire failed: %v", err)
	}
	if w3 != w1 {
		t.Fatalf("expected reused worker w1")
	}
	pool.Release(w3, true)
}

func TestAntigravityAcpPool_P0_DeadWorkerWakesAllWaiters(t *testing.T) {
	var spawnCount atomic.Int32
	pool := NewAntigravityAcpPool(1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		spawnCount.Add(1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	var wg sync.WaitGroup
	type waitRes struct {
		w   *AntigravityAcpWorker
		err error
	}
	waitChan := make(chan waitRes, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
			w, err := pool.Acquire(ctx, "k1", nil)
			waitChan <- waitRes{w, err}
		}()
	}

	wg.Wait()
	time.Sleep(30 * time.Millisecond) // Let goroutines queue up

	// Release w1 as UNHEALTHY (dead)
	pool.Release(w1, false)

	// Both queued waiters should wake up and re-acquire (or spawn fresh worker)
	res1 := <-waitChan
	if res1.err != nil || res1.w == nil {
		t.Fatalf("waiter 1 failed after dead worker release: %v", res1.err)
	}
	pool.Release(res1.w, true)

	res2 := <-waitChan
	if res2.err != nil || res2.w == nil {
		t.Fatalf("waiter 2 failed after dead worker release: %v", res2.err)
	}
	pool.Release(res2.w, true)
}
