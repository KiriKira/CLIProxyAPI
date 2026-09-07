package helps

import (
	"testing"
	"time"
)

func newStatefulTestWorker() *AntigravityAcpWorker {
	return &AntigravityAcpWorker{key: "auth|method|profile|bin"}
}

// TestStatefulTable_BindLookupTurnAdvance covers the happy path: bind turn 0,
// look up, bind turn 1; LastTurn advances monotonically.
func TestStatefulTable_ActiveLeaseDefersLRUEviction(t *testing.T) {
	table := NewStatefulSessionTable(0, 1)
	worker := newStatefulTestWorker()
	table.Bind("logical-a", "session-a", "auth", "model", worker, 0)
	release, ok := table.AcquireLease("logical-a", worker, "auth", "model")
	if !ok {
		t.Fatal("AcquireLease(logical-a) failed")
	}
	table.Bind("logical-b", "session-b", "auth", "model", worker, 0)
	if got := table.Len(); got != 1 {
		t.Fatalf("binding count while active lease = %d, want 1", got)
	}
	if _, ok := table.Lookup("logical-a", worker, "auth", "model"); !ok {
		t.Fatal("active binding was evicted while leased")
	}
	release()
	if got := table.Len(); got != 0 {
		t.Fatalf("binding count after deferred eviction = %d, want 0", got)
	}
}

func TestStatefulTable_BindLookupTurnAdvance(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "auth|method|profile|bin", "gemini-3.8-flash-low", w, 0)

	b, ok := tb.Lookup("logical-1", w, "auth|method|profile|bin", "gemini-3.8-flash-low")
	if !ok {
		t.Fatalf("expected hit after bind")
	}
	if b.ACPSessionID != "acp-sess-1" || b.LastTurn != 0 {
		t.Fatalf("binding mismatch: %+v", b)
	}

	tb.Bind("logical-1", "acp-sess-1", "auth|method|profile|bin", "gemini-3.8-flash-low", w, 1)
	b, ok = tb.Lookup("logical-1", w, "auth|method|profile|bin", "gemini-3.8-flash-low")
	if !ok || b.LastTurn != 1 {
		t.Fatalf("turn did not advance: ok=%v binding=%+v", ok, b)
	}
}

// TestStatefulTable_WorkerDeathInvalidates covers P1.6 #6: a binding whose
// worker is gone (purged or pool membership changed) is invalid.
func TestStatefulTable_WorkerDeathInvalidates(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "variant", w, 0)

	tb.PurgeWorker(w)
	if _, ok := tb.Lookup("logical-1", w, "k", "variant"); ok {
		t.Fatalf("binding survived worker purge (P1.6 #6 violated)")
	}
}

// TestStatefulTable_WorkerMismatchInvalidates covers P1.1: a hit must
// reacquire the SAME worker; a lookup against another worker is a miss.
func TestStatefulTable_WorkerMismatchInvalidates(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w1 := newStatefulTestWorker()
	w2 := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "variant", w1, 0)

	if _, ok := tb.Lookup("logical-1", w2, "k", "variant"); ok {
		t.Fatalf("binding matched a different worker (P1.1 violated)")
	}
	// The mismatched lookup invalidated the binding.
	if _, ok := tb.Lookup("logical-1", w1, "k", "variant"); ok {
		t.Fatalf("binding survived a worker-mismatch lookup; must invalidate")
	}
}

// TestStatefulTable_VariantMismatchInvalidates covers P1.2: an incompatible
// model change must not silently continue the session.
func TestStatefulTable_VariantMismatchInvalidates(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "gemini-3.8-flash-low", w, 0)

	if _, ok := tb.Lookup("logical-1", w, "k", "gemini-3.7-flash"); ok {
		t.Fatalf("binding matched despite variant change (P1.2 violated)")
	}
}

// TestStatefulTable_TTLExpiry covers P1.5: bindings expire after the TTL.
func TestStatefulTable_TTLExpiry(t *testing.T) {
	tb := NewStatefulSessionTable(20*time.Millisecond, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "variant", w, 0)

	if _, ok := tb.Lookup("logical-1", w, "k", "variant"); !ok {
		t.Fatalf("expected immediate hit")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := tb.Lookup("logical-1", w, "k", "variant"); ok {
		t.Fatalf("binding survived TTL expiry (P1.5 violated)")
	}
}

// TestStatefulTable_LRUBound covers P1.5: bindings above the bound are
// LRU-evicted; eviction only drops the proxy binding.
func TestStatefulTable_LRUBound(t *testing.T) {
	tb := NewStatefulSessionTable(0, 2)
	w := newStatefulTestWorker()
	tb.Bind("a", "s-a", "k", "", w, 0)
	tb.Bind("b", "s-b", "k", "", w, 0)
	// Touch "a" so "b" becomes LRU.
	if _, ok := tb.Lookup("a", w, "k", ""); !ok {
		t.Fatalf("expected hit on a")
	}
	tb.Bind("c", "s-c", "k", "", w, 0)

	if _, ok := tb.Lookup("b", w, "k", ""); ok {
		t.Fatalf("binding b survived LRU eviction (bound=2)")
	}
	if _, ok := tb.Lookup("a", w, "k", ""); !ok {
		t.Fatalf("recently used binding a was evicted")
	}
	if _, ok := tb.Lookup("c", w, "k", ""); !ok {
		t.Fatalf("newest binding c was evicted")
	}
}

// TestStatefulTable_Invalidate covers P1.4 invalidation hook.
func TestStatefulTable_Invalidate(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "", w, 0)
	tb.Invalidate("logical-1")
	if _, ok := tb.Lookup("logical-1", w, "k", ""); ok {
		t.Fatalf("binding survived explicit invalidation")
	}
}

// TestStatefulTable_MissingWorkerNeverServed covers the nil-worker guard.
func TestStatefulTable_MissingWorkerNeverServed(t *testing.T) {
	tb := NewStatefulSessionTable(30*time.Minute, 8)
	w := newStatefulTestWorker()
	tb.Bind("logical-1", "acp-sess-1", "k", "", w, 0)
	// Simulate an internal inconsistency by clearing the worker on the
	// binding through a purged-worker registration: bind with a nil worker
	// must be a no-op.
	tb.Bind("logical-2", "acp-sess-2", "k", "", nil, 0)
	if tb.Len() != 1 {
		t.Fatalf("nil-worker bind must be rejected; len=%d", tb.Len())
	}
}
