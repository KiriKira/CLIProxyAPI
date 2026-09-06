package helps

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// preparedTestPool builds a pool with an injected session-prepare function
// that records each call and returns distinct session ids.
func preparedTestPool(t *testing.T, prepareLimit int, calls *atomic.Int32, failAfter int32) *AntigravityAcpPool {
	t.Helper()
	var mu sync.Mutex
	n := 0
	pool := NewAntigravityAcpPoolWithLimits(1, 0, prepareLimit, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	t.Cleanup(func() { pool.Close() })
	pool.sessionPrepare = func(ctx context.Context, w *AntigravityAcpWorker) (*PreparedSession, error) {
		if failAfter >= 0 && calls.Load() >= failAfter {
			return nil, errors.New("prepared session/new refused")
		}
		calls.Add(1)
		mu.Lock()
		n++
		id := "psess-" + itoa(n)
		mu.Unlock()
		return &PreparedSession{SessionID: id}, nil
	}
	return pool
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPreparedSession_PopAfterRelease verifies the end-to-end refill flow:
// a released healthy worker gets a background refill, and the next Acquire
// pops the prepared session instead of paying session/new on the hot path.
func TestPreparedSession_PopAfterRelease(t *testing.T) {
	var calls atomic.Int32
	pool := preparedTestPool(t, 1, &calls, -1)

	ctx := context.Background()
	w, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if ps := w.AcquirePreparedSession(); ps != nil {
		t.Fatalf("fresh worker should have no prepared session yet")
	}
	pool.Release(w, true)

	waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 1 })
	if calls.Load() < 1 {
		t.Fatalf("release should kick a background refill; calls = %d", calls.Load())
	}

	// The refilled session must be visible on the next acquire.
	w2, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	ps := w2.AcquirePreparedSession()
	if ps == nil || ps.SessionID == "" {
		t.Fatalf("expected a prepared session after refill")
	}
	pool.Release(w2, true)
}

// TestPreparedSession_BoundedAndDistinct verifies the cache never exceeds
// prepareLimit and never hands the same session out twice.
func TestPreparedSession_BoundedAndDistinct(t *testing.T) {
	pool := NewAntigravityAcpPoolWithLimits(1, 0, 1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer pool.Close()
	w := &AntigravityAcpWorker{pool: pool, key: "a", client: &acp.Client{}}

	for i := 0; i < 5; i++ {
		w.PutPreparedSession(&PreparedSession{SessionID: itoa(i)}, 1)
	}
	first := w.AcquirePreparedSession()
	if first == nil || first.SessionID != "4" {
		t.Fatalf("oldest sessions should be trimmed; got %+v", first)
	}
	if next := w.AcquirePreparedSession(); next != nil {
		t.Fatalf("cache must be empty after the pop; got %+v", next)
	}
}

// TestPreparedSession_RefillFailsGracefully verifies a failing prepare call
// does not wedge the worker: the next release retries the refill.
func TestPreparedSession_RefillFailsGracefully(t *testing.T) {
	var calls atomic.Int32
	pool := preparedTestPool(t, 1, &calls, 1) // first ok, then fail

	ctx := context.Background()
	w, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	pool.Release(w, true)
	waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 1 })

	// Consume the one good session.
	w2, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	w2.AcquirePreparedSession()
	pool.Release(w2, true)

	// Refill now fails; worker stays usable, and AcquirePreparedSession
	// just returns nil (fallback path in the executor).
	waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
	w3, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire 3 after failed refill: %v", err)
	}
	if ps := w3.AcquirePreparedSession(); ps != nil {
		t.Fatalf("no session should be available after failed refill")
	}
	pool.Release(w3, true)
}

// TestPreparedSession_ClosedPoolDrops verifies Close discards cached
// sessions and stops refills.
func TestPreparedSession_ClosedPoolDrops(t *testing.T) {
	var calls atomic.Int32
	pool := preparedTestPool(t, 1, &calls, -1)

	ctx := context.Background()
	w, err := pool.Acquire(ctx, "a", nil)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	pool.Release(w, true)
	waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 1 })

	_ = pool.Close()
	time.Sleep(50 * time.Millisecond)

	w2, err := pool.Acquire(ctx, "a", nil)
	if err == nil {
		w2.AcquirePreparedSession()
		t.Fatal("acquire on a closed pool must fail")
	}
}
