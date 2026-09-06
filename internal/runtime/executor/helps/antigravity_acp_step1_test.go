package helps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// TestAntigravityAcpPool_ZeroGlobalCapIsUncapped verifies P0.1: with
// max-workers-total unset (0), per-auth worker growth stays independent and
// a global cap must not materialize out of the normalization fallback.
func TestAntigravityAcpPool_ZeroGlobalCapIsUncapped(t *testing.T) {
	var spawns int32
	pool := NewAntigravityAcpPoolWithLimits(1, 0, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		atomic.AddInt32(&spawns, 1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire k1: %v", err)
	}
	w2, err := pool.Acquire(ctx, "k2", nil)
	if err != nil {
		t.Fatalf("acquire k2 while k1 busy: %v (zero global cap must not serialize distinct keys)", err)
	}
	w3, err := pool.Acquire(ctx, "k3", nil)
	if err != nil {
		t.Fatalf("acquire k3: %v", err)
	}
	if atomic.LoadInt32(&spawns) != 3 {
		t.Fatalf("spawns = %d, want 3 independent per-auth workers", spawns)
	}
	pool.Release(w1, true)
	pool.Release(w2, true)
	pool.Release(w3, true)
}

// TestAntigravityAcpPool_PositiveCapBelowPerAuthNormalized verifies the
// P0.1 companion rule: a positive maxTotal below the per-auth cap is
// normalized up to the per-auth cap, and then remains strict.
func TestAntigravityAcpPool_PositiveCapBelowPerAuthNormalized(t *testing.T) {
	var spawns int32
	pool := NewAntigravityAcpPoolWithLimits(2, 1, 0, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		atomic.AddInt32(&spawns, 1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire k1: %v", err)
	}
	w2, err := pool.Acquire(ctx, "k2", nil)
	if err != nil {
		t.Fatalf("acquire k2: %v (positive maxTotal=1 must normalize up to per-auth cap 2)", err)
	}
	if atomic.LoadInt32(&spawns) != 2 {
		t.Fatalf("spawns = %d, want 2", spawns)
	}

	// The normalized cap (2) must remain strict: a third distinct key
	// cannot spawn while both slots are leased.
	blockCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(blockCtx, "k3", nil); err == nil {
		t.Fatalf("third acquire succeeded; normalized global cap 2 must stay strict")
	} else if err != context.DeadlineExceeded {
		t.Fatalf("third acquire error = %v, want context.DeadlineExceeded", err)
	}
	pool.Release(w1, true)
	pool.Release(w2, true)
}

// TestAntigravityAcpPool_AcquireInterruptsRefillBeforeLease verifies P0.4:
// a request that leases a worker cancels the in-flight background refill
// and takes the lease only after the refill has fully settled. The acquirer
// must not wait for the refill to complete on its own.
func TestAntigravityAcpPool_AcquireInterruptsRefillBeforeLease(t *testing.T) {
	pool := NewAntigravityAcpPoolWithLimits(1, 0, 1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer pool.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	pool.sessionPrepare = func(ctx context.Context, w *AntigravityAcpWorker) (*PreparedSession, error) {
		close(started)
		select {
		case <-release:
			return &PreparedSession{SessionID: "late"}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ctx := context.Background()
	w1, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	pool.Release(w1, true)

	<-started // background refill for the released worker is now in flight

	leaseDeadline := time.Now().Add(2 * time.Second)
	w2, err := pool.Acquire(ctx, "k1", nil)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if time.Now().After(leaseDeadline) {
		t.Fatalf("acquire 2 waited too long; refill must be interrupted, not awaited")
	}
	if w2 != w1 {
		t.Fatalf("expected the same idle worker to be leased")
	}
	// The refill must have settled BEFORE the lease was granted.
	w2.mu.Lock()
	refilling := w2.refilling
	w2.mu.Unlock()
	if refilling {
		t.Fatalf("worker leased while refill still in flight (P0.4 invariant broken)")
	}
	close(release) // let the interrupted refill goroutine exit
}

// TestAntigravityAcpWorker_PreferredVariant verifies the P0.5 plumbing:
// the most recent resolved variant is recorded and exposed for the next
// model-aware preparation.
func TestAntigravityAcpWorker_PreferredVariant(t *testing.T) {
	w := &AntigravityAcpWorker{}
	if got := w.PreferredVariant(); got != "" {
		t.Fatalf("initial preferred variant = %q, want empty", got)
	}
	w.SetPreferredVariant("gemini-3.8-flash-low")
	if got := w.PreferredVariant(); got != "gemini-3.8-flash-low" {
		t.Fatalf("preferred variant = %q, want gemini-3.8-flash-low", got)
	}
	w.SetPreferredVariant("gemini-3.7-flash")
	if got := w.PreferredVariant(); got != "gemini-3.7-flash" {
		t.Fatalf("preferred variant after update = %q, want gemini-3.7-flash", got)
	}
	// Empty updates must never clear a known preference.
	w.SetPreferredVariant("")
	if got := w.PreferredVariant(); got != "gemini-3.7-flash" {
		t.Fatalf("empty update must not clear the preference, got %q", got)
	}
}
