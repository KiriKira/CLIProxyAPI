package executor

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
)

// TestAntigravityAcpExecutor_R1RetiredHookPurgesBothBindingTables verifies
// the executor-side wiring of the pool's worker-retired hook: forced
// retirement of a bound worker purges BOTH the strict-stateful table and
// the document table, so every affected client bootstraps from full history
// instead of reacquiring a dead worker (R1 recovery semantics).
func TestAntigravityAcpExecutor_R1RetiredHookPurgesBothBindingTables(t *testing.T) {
	exec := &AntigravityAcpExecutor{}
	exec.stateful = helps.NewStatefulSessionTable(time.Minute, 16)
	exec.document = helps.NewDocumentSessionTable(time.Minute, 16)
	fakeAgent := statefulLogAgent(t)
	exec.pool = helps.NewAntigravityAcpPoolWithSessionLimits(1, 0, 0, 0, 1, 0, func(ctx context.Context, key string) (*acp.Client, error) {
		return acp.NewClient(acp.SpawnConfig{Command: fakeAgent, Dir: t.TempDir()})
	})
	purged := make(chan struct{}, 1)
	exec.pool.SetWorkerRetiredHook(func(w *helps.AntigravityAcpWorker) {
		exec.stateful.PurgeWorker(w)
		exec.document.PurgeWorker(w)
		purged <- struct{}{}
	})
	t.Cleanup(func() { _ = exec.pool.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Worker A becomes draining while owning one strict and one document
	// binding (single worker reached its session cap).
	wa, err := exec.pool.Acquire(ctx, "auth|acp|profile|binary", nil)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	exec.stateful.Bind("logical-1", "acp-s1", "auth|acp|profile|binary", "gemini-3.7-flash-low", wa, 3)
	wa.RegisterSessionCreated("acp-s1", "fresh")
	wa.BindSession("acp-s1", "strict")
	wa.RegisterSessionCreated("acp-d1", "fresh")
	wa.BindSession("acp-d1", "document")
	exec.document.Bind("document:key-a", "acp-d1", "auth|acp|profile|binary", "gemini-3.7-flash-low", wa)
	if exec.stateful.Len() != 1 || exec.document.Len() != 1 {
		t.Fatalf("precondition: stateful=%d document=%d, want 1/1", exec.stateful.Len(), exec.document.Len())
	}

	// Drive the worker to its session cap so it is draining while bound,
	// then release it: the next fresh demand must force-retire it.
	for i := 0; i < 2; i++ {
		wa.RegisterSessionCreated("cap-fill", "fresh")
	}
	exec.pool.Release(wa, true)

	_, err = exec.pool.Acquire(ctx, "auth|acp|profile|binary", nil)
	if err != nil {
		t.Fatalf("fresh acquire after cap (R1): %v", err)
	}
	select {
	case <-purged:
	case <-time.After(2 * time.Second):
		t.Fatalf("retired hook never fired")
	}
	if exec.stateful.Len() != 0 {
		t.Fatalf("strict bindings survived forced retirement: %d", exec.stateful.Len())
	}
	if exec.document.Len() != 0 {
		t.Fatalf("document bindings survived forced retirement: %d", exec.document.Len())
	}
	// The purged strict binding must no longer resolve, so the client
	// bootstraps from full history on its next turn.
	if _, ok := exec.stateful.Lookup("logical-1", nil, "auth|acp|profile|binary", ""); ok {
		t.Fatalf("purged strict binding still resolves; client would skip full-history bootstrap")
	}
	if _, ok := exec.document.Lookup("document:key-a", nil, "auth|acp|profile|binary", ""); ok {
		t.Fatalf("purged document binding still resolves; client would skip bootstrap")
	}
}
