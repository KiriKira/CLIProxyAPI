package helps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

func TestAntigravityAcpPool_AcquireReleaseReuse(t *testing.T) {
	var spawnCount int32
	pool := NewAntigravityAcpPool(2, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		atomic.AddInt32(&spawnCount, 1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}
	if atomic.LoadInt32(&spawnCount) != 1 {
		t.Fatalf("spawnCount = %d, want 1", spawnCount)
	}

	pool.Release(w1, true)

	// Acquire again for same key, should reuse without spawning
	w2, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	if w2 != w1 {
		t.Fatalf("expected reused worker")
	}
	if atomic.LoadInt32(&spawnCount) != 1 {
		t.Fatalf("spawnCount = %d, want 1 after reuse", spawnCount)
	}

	pool.Release(w2, true)
}

func TestAntigravityAcpPool_DiscardDeadWorker(t *testing.T) {
	var spawnCount int32
	pool := NewAntigravityAcpPool(2, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		atomic.AddInt32(&spawnCount, 1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Release with healthy=false
	pool.Release(w1, false)

	// Next acquire must spawn fresh worker
	w2, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	if w2 == w1 {
		t.Fatalf("expected different worker after discard")
	}
	if atomic.LoadInt32(&spawnCount) != 2 {
		t.Fatalf("spawnCount = %d, want 2", spawnCount)
	}

	pool.Release(w2, true)
}

func TestAntigravityAcpPool_QueueWaiting(t *testing.T) {
	pool := NewAntigravityAcpPool(1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}

	acquiredCh := make(chan *AntigravityAcpWorker)
	go func() {
		w2, err := pool.Acquire(ctx, "auth-1", nil)
		if err != nil {
			return
		}
		acquiredCh <- w2
	}()

	// Ensure goroutine has entered queue
	time.Sleep(20 * time.Millisecond)
	select {
	case <-acquiredCh:
		t.Fatalf("w2 acquired before w1 released")
	default:
	}

	pool.Release(w1, true)

	select {
	case w2 := <-acquiredCh:
		if w2 != w1 {
			t.Fatalf("expected handed-off worker w1")
		}
		pool.Release(w2, true)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker hand-off")
	}
}
