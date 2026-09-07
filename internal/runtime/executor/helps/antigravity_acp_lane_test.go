package helps

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDocumentLane_GCManyHistoricalKeys is the R2 lane-map bound test:
// acquiring and releasing 1000 unique document keys leaves no permanent
// lane entries behind.
func TestDocumentLane_GCManyHistoricalKeys(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("document:historical-key-%d", i)
		release, err := table.AcquireLane(key, ctx, 0)
		if err != nil {
			t.Fatalf("acquire lane %d: %v", i, err)
		}
		release()
	}
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("lane map kept %d entries after releases, want 0 (R2 lane GC broken)", got)
	}
}

// TestDocumentLane_CanceledWaiterExitsPromptly verifies R2 cancellation:
// a request waiting for a held lane returns as soon as its context is
// canceled, without waiting for the current holder to finish.
func TestDocumentLane_CanceledWaiterExitsPromptly(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	ctx := context.Background()
	release, err := table.AcquireLane("doc-a", ctx, 0)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = table.AcquireLane("doc-a", waitCtx, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled waiter error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("canceled waiter blocked %.2fs; must exit promptly (R2)", elapsed.Seconds())
	}
	release()
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("lane survived after holder release + canceled waiter: %d", got)
	}
}

func TestDocumentLane_PreCanceledContextNeverAcquires(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := table.AcquireLane("doc-pre-canceled", ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled acquire error = %v, want context.Canceled", err)
	}
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("pre-canceled acquire left %d lanes", got)
	}
}

func TestDocumentLane_CancelWinsWhenTokenBecomesAvailable(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	release, err := table.AcquireLane("doc-race", context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := table.AcquireLane("doc-race", waitCtx, 0)
		result <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		table.mu.Lock()
		registered := table.lanes["doc-race"] != nil && table.lanes["doc-race"].waiters == 1
		table.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not register")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	release()
	select {
	case acquireErr := <-result:
		if !errors.Is(acquireErr, context.Canceled) {
			t.Fatalf("canceled simultaneous acquire error = %v, want context.Canceled", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled simultaneous acquire did not return")
	}
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("canceled simultaneous acquire left %d lanes", got)
	}
}

// maxWaiters=2 and one holder, the third concurrent acquire gets
// ErrDocumentLaneBusy (retryable backpressure) instead of queueing.
func TestDocumentLane_QueueLimitEnforced(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	release, err := table.AcquireLane("doc-b", context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	waitCtx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	results := make(chan error, 2)
	for _, waitCtx := range []context.Context{waitCtx1, waitCtx2} {
		go func(ctx context.Context) {
			r, acquireErr := table.AcquireLane("doc-b", ctx, 2)
			if acquireErr == nil {
				r()
			}
			results <- acquireErr
		}(waitCtx)
	}
	deadline := time.Now().Add(time.Second)
	for {
		table.mu.Lock()
		registered := table.lanes["doc-b"] != nil && table.lanes["doc-b"].waiters == 2
		table.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("queue waiters did not register")
		}
		time.Sleep(time.Millisecond)
	}

	if _, err = table.AcquireLane("doc-b", context.Background(), 2); !errors.Is(err, ErrDocumentLaneBusy) {
		release()
		t.Fatalf("overflow acquire error = %v, want ErrDocumentLaneBusy (R2 bounded queue)", err)
	}
	cancel1()
	cancel2()
	release()
	for i := 0; i < 2; i++ {
		select {
		case acquireErr := <-results:
			if !errors.Is(acquireErr, context.Canceled) {
				t.Fatalf("waiter %d error = %v, want context.Canceled", i, acquireErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d did not exit", i)
		}
	}
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("queue test left %d lanes", got)
	}
}

// TestDocumentLane_ColdBurstSingleflights covers the R2 one-lane exit
// criterion: 10 simultaneous cold requests for one document serialize into
// exactly one at-a-time execution — a bootstrap counter observes the lane,
// not the burst size.
func TestDocumentLane_ColdBurstSingleflights(t *testing.T) {
	table := NewDocumentSessionTable(time.Minute, 10)
	var inside atomic.Int32
	var maxInside atomic.Int32
	var bootstraps atomic.Int32

	worker := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		release, err := table.AcquireLane("doc-cold", ctx, 0)
		if err != nil {
			t.Errorf("burst acquire: %v", err)
			return
		}
		defer release()
		bootstraps.Add(1) // one bootstrap per serialized critical section
		cur := inside.Add(1)
		for {
			old := maxInside.Load()
			if cur <= old || maxInside.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inside.Add(-1)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}
	wg.Wait()

	if got := maxInside.Load(); got != 1 {
		t.Fatalf("max concurrent lane holders = %d, want 1 (one-lane mode)", got)
	}
	if got := table.LaneCount(); got != 0 {
		t.Fatalf("lane map not drained after burst: %d", got)
	}
	// Each serialized section performs its own bootstrap work; the point is
	// that they never overlapped, so 10 sequential sections is expected.
	if got := bootstraps.Load(); got != 10 {
		t.Fatalf("bootstraps = %d, want 10 sequential sections", got)
	}
}

func TestDocumentTable_ActiveLeaseDefersLRUEviction(t *testing.T) {
	table := NewDocumentSessionTable(0, 1)
	worker := &AntigravityAcpWorker{}
	table.Bind("document-a", "session-a", "auth", "model", worker)
	release, ok := table.AcquireLease("document-a", worker, "auth", "model")
	if !ok {
		t.Fatal("AcquireLease(document-a) failed")
	}
	table.Bind("document-b", "session-b", "auth", "model", worker)
	if got := table.Len(); got != 1 {
		t.Fatalf("binding count while active lease = %d, want 1", got)
	}
	if _, ok := table.Lookup("document-a", worker, "auth", "model"); !ok {
		t.Fatal("active document binding was evicted while leased")
	}
	release()
	if got := table.Len(); got != 0 {
		t.Fatalf("binding count after deferred eviction = %d, want 0", got)
	}
}
