package helps

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

// These are LOCAL bookkeeping characterizations, not ACP transport or cloud
// latency benchmarks. Nil clients avoid launching or replacing the official
// ACP server. ObservedBug tests deliberately assert current defective behavior.
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

// Acquire evaluates Done only after publishing its waiter under p.mu. The
// notification is a queue-registration barrier, not a timing-based guess.
// The embedded context retains its original cancellation/deadline semantics.
type reviewAblationQueueContext struct {
	context.Context
	once   sync.Once
	queued chan struct{}
}

func (c *reviewAblationQueueContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.queued) })
	return c.Context.Done()
}

func TestReviewAblationObservedBugSameKeyColdSpawnHasNoReservation(t *testing.T) {
	ctx := reviewAblationContext(t)
	var count atomic.Int32
	p := reviewAblationPool(t, 1, &count)
	entered := make(chan struct{}, 2)
	proceed := make(chan struct{})
	p.Factory = func(ctx context.Context, _ string) (*acp.Client, error) {
		count.Add(1)
		entered <- struct{}{}
		select {
		case <-proceed:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	results := make(chan reviewAblationResult, 2)
	for range 2 {
		go func() {
			w, err := p.Acquire(ctx, "same", nil)
			results <- reviewAblationResult{w, err}
		}()
	}
	for range 2 {
		reviewAblationReceive(t, ctx, entered)
	}
	p.mu.Lock()
	registeredBeforeCompletion := len(p.workers)
	p.mu.Unlock()
	close(proceed)
	a := reviewAblationReceive(t, ctx, results)
	b := reviewAblationReceive(t, ctx, results)
	if a.err != nil || b.err != nil || a.worker == nil || b.worker == nil || a.worker == b.worker {
		t.Fatalf("expected two distinct successful leases: a=%+v b=%+v", a, b)
	}
	p.mu.Lock()
	registeredAfterCompletion := len(p.workers)
	tracked := p.workers["same"]
	p.mu.Unlock()
	if count.Load() != 2 || registeredBeforeCompletion != 0 || registeredAfterCompletion != 1 || (tracked != a.worker && tracked != b.worker) {
		t.Fatalf("unexpected reservation bookkeeping: calls=%d before=%d after=%d", count.Load(), registeredBeforeCompletion, registeredAfterCompletion)
	}
	p.Release(a.worker, true)
	p.Release(b.worker, true)
	t.Logf("LOCAL observed_bug=same_key_cold_spawn factory_calls=%d concurrent_distinct_leases=2 registered_during_spawn=%d registered_after=%d untracked_leases=1", count.Load(), registeredBeforeCompletion, registeredAfterCompletion)
}

func TestReviewAblationObservedBugBusyCrossKeyExceedsGlobalMax(t *testing.T) {
	ctx := reviewAblationContext(t)
	var count atomic.Int32
	p := reviewAblationPool(t, 1, &count)
	a := reviewAblationAcquire(t, p, ctx, "a")
	b := reviewAblationAcquire(t, p, ctx, "b")
	p.mu.Lock()
	registered := len(p.workers)
	p.mu.Unlock()
	if registered != 2 || count.Load() != 2 || a == b {
		t.Fatalf("expected two busy workers with max=1: registered=%d calls=%d", registered, count.Load())
	}
	p.Release(a, true)
	p.Release(b, true)
	t.Logf("LOCAL observed_bug=busy_cross_key_capacity max_workers=1 registered_busy_workers=%d factory_calls=%d", registered, count.Load())
}

func TestReviewAblationObservedBugDeadWorkerWakesOneWithoutRetry(t *testing.T) {
	ctx := reviewAblationContext(t)
	var count atomic.Int32
	p := reviewAblationPool(t, 1, &count)
	original := reviewAblationAcquire(t, p, ctx, "same")
	var results [3]chan reviewAblationResult
	for i := range results {
		results[i] = make(chan reviewAblationResult, 1)
		queueCtx := &reviewAblationQueueContext{Context: ctx, queued: make(chan struct{})}
		go func(out chan<- reviewAblationResult) {
			w, err := p.Acquire(queueCtx, "same", nil)
			out <- reviewAblationResult{w, err}
		}(results[i])
		reviewAblationReceive(t, ctx, queueCtx.queued)
	}
	p.mu.Lock()
	queuedBefore := len(p.waitQueues["same"])
	p.mu.Unlock()
	if queuedBefore != 3 {
		t.Fatalf("queue barrier failed: queued=%d", queuedBefore)
	}
	p.Release(original, false)
	first := reviewAblationReceive(t, ctx, results[0])
	if first.worker != nil || first.err == nil || first.err.Error() != "acp pool closed or wait canceled" {
		t.Fatalf("expected first waiter to error without retry: %+v", first)
	}
	p.mu.Lock()
	remaining, registered := len(p.waitQueues["same"]), len(p.workers)
	p.mu.Unlock()
	if remaining != 2 || registered != 0 || count.Load() != 1 {
		t.Fatalf("expected two stranded waiters, no automatic spawn: remaining=%d registered=%d calls=%d", remaining, registered, count.Load())
	}
	for _, out := range results[1:] {
		select {
		case unexpected := <-out:
			t.Fatalf("remaining waiter unexpectedly completed: %+v", unexpected)
		default:
		}
	}
	t.Logf("LOCAL observed_bug=dead_worker_waiters queued_before=%d woken_with_error=1 automatic_retries=0 remaining_queued=%d registered_workers=%d factory_calls=%d error=%q", queuedBefore, remaining, registered, count.Load(), first.err.Error())
	// A NEW explicit Acquire is necessary to restart work; its healthy release
	// then hands off to each remaining waiter. No cancellation is used to make
	// the observed failure happen, and all waiter goroutines are drained.
	replacement := reviewAblationAcquire(t, p, ctx, "same")
	if replacement == original || count.Load() != 2 {
		t.Fatal("explicit retry did not create the replacement")
	}
	p.Release(replacement, true)
	for _, out := range results[1:] {
		got := reviewAblationReceive(t, ctx, out)
		if got.err != nil || got.worker != replacement {
			t.Fatalf("replacement handoff failed: %+v", got)
		}
		p.Release(got.worker, true)
	}
	p.mu.Lock()
	remaining = len(p.waitQueues["same"])
	p.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("waiters not drained: %d", remaining)
	}
	t.Logf("LOCAL recovery=explicit_new_acquire factory_calls_after_retry=%d remaining_waiters_recovered=2 remaining_queued=%d", count.Load(), remaining)
}

func TestReviewAblationObservedBugCancelledContextAcquiresIdleWorker(t *testing.T) {
	ctx := reviewAblationContext(t)
	var count atomic.Int32
	p := reviewAblationPool(t, 1, &count)
	original := reviewAblationAcquire(t, p, ctx, "same")
	p.Release(original, true)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	w := reviewAblationAcquire(t, p, cancelled, "same")
	if cancelled.Err() != context.Canceled || w != original || count.Load() != 1 {
		t.Fatal("expected cancelled context to successfully lease idle worker")
	}
	p.Release(w, true)
	t.Logf("LOCAL observed_bug=cancelled_idle_acquire context_error=%q acquire_error=nil reused_worker=true factory_calls=%d", cancelled.Err(), count.Load())
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
