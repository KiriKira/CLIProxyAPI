package helps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// These are LOCAL bookkeeping characterizations, not ACP transport or cloud
// latency benchmarks. Nil clients avoid launching or replacing the official
// ACP server. Characterizations of defects that were subsequently fixed
// (cold-spawn reservation, dead-worker wake-up, cancelled-context leasing)
// live on as regression tests in antigravity_acp_p0_test.go and have been
// removed from here.
func reviewAblationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func reviewAblationPool(t *testing.T, max int, count *atomic.Int32) *AntigravityAcpPool {
	t.Helper()
	p := NewAntigravityAcpPool(max, 0, func(context.Context, string) (*acp.Client, error) {
		count.Add(1)
		return nil, nil
	})
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return p
}

func reviewAblationAcquire(t *testing.T, p *AntigravityAcpPool, ctx context.Context, key string) *AntigravityAcpWorker {
	t.Helper()
	w, err := p.Acquire(ctx, key, nil)
	if err != nil || w == nil {
		t.Fatalf("Acquire(%q): worker=%v error=%v", key, w, err)
	}
	return w
}

func reviewAblationReceive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("synchronization guard expired: %v", ctx.Err())
		var zero T
		return zero
	}
}

type reviewAblationResult struct {
	worker *AntigravityAcpWorker
	err    error
}

// TestReviewAblationPerAuthOnlyCap documents that NewAntigravityAcpPool caps
// workers per auth key and does not add a global bound: busy workers of other
// keys are never evicted to admit a new key. The executor path that needs a
// strict global cap uses NewAntigravityAcpPoolWithLimits instead.
func TestReviewAblationPerAuthOnlyCap(t *testing.T) {
	ctx := reviewAblationContext(t)
	var count atomic.Int32
	p := reviewAblationPool(t, 1, &count)
	a := reviewAblationAcquire(t, p, ctx, "a")
	b := reviewAblationAcquire(t, p, ctx, "b")
	p.mu.Lock()
	registered := p.totalWorkersLocked()
	p.mu.Unlock()
	if registered != 2 || count.Load() != 2 || a == b {
		t.Fatalf("expected two busy workers with per-auth max=1: registered=%d calls=%d", registered, count.Load())
	}
	p.Release(a, true)
	p.Release(b, true)
	t.Logf("LOCAL characterization=per_auth_only_cap max_workers=1 registered_busy_workers=%d factory_calls=%d", registered, count.Load())
}

func TestReviewAblationSerialPooledVersusPerRequestFactoryCountControl(t *testing.T) {
	ctx := reviewAblationContext(t)
	const requests = 8
	var pooledCalls, perRequestCalls atomic.Int32
	p := reviewAblationPool(t, 1, &pooledCalls)
	var first *AntigravityAcpWorker
	for range requests {
		w := reviewAblationAcquire(t, p, ctx, "same")
		if first == nil {
			first = w
		} else if first != w {
			t.Fatal("serial pooled control did not reuse worker")
		}
		p.Release(w, true)
	}
	// This is a count-only ablation: directly call the same nil-client factory
	// once per request, then exercise nil-safe Close. No daemon is launched.
	perRequestFactory := func(context.Context, string) (*acp.Client, error) {
		perRequestCalls.Add(1)
		return nil, nil
	}
	for range requests {
		client, err := perRequestFactory(ctx, "same")
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if pooledCalls.Load() != 1 || perRequestCalls.Load() != requests {
		t.Fatalf("unexpected factory counts: pooled=%d per_request=%d", pooledCalls.Load(), perRequestCalls.Load())
	}
	t.Logf("LOCAL control=serial_factory_count requests=%d pooled_factory_calls=%d per_request_factory_calls=%d cloud_latency_measured=false", requests, pooledCalls.Load(), perRequestCalls.Load())
}
