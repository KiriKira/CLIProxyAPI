package helps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/acp"
)

func TestWorkerSessionLedgerTransitionsAndCaps(t *testing.T) {
	worker := &AntigravityAcpWorker{
		sessions:              make(map[string]workerSessionState),
		maxSessionsPerWorker:  2,
		maxAbandonedPerWorker: 1,
	}

	if !worker.AllowSessionCreation() {
		t.Fatal("first session creation should be allowed")
	}
	worker.RegisterSessionCreated("prepared-1", "prepared")
	stats := worker.SessionStats()
	if stats.CreatedTotal != 1 || stats.Prepared != 1 {
		t.Fatalf("after prepared create: %+v", stats)
	}

	worker.MarkPreparedSessionConsumed("prepared-1")
	worker.BindSession("prepared-1", "strict")
	stats = worker.SessionStats()
	if stats.Prepared != 0 || stats.BoundStrict != 1 || stats.Draining {
		t.Fatalf("after strict bind: %+v", stats)
	}

	worker.AbandonSession("prepared-1")
	stats = worker.SessionStats()
	if stats.BoundStrict != 0 || stats.Abandoned != 1 || !stats.Draining || stats.DrainReason != "abandoned_cap" {
		t.Fatalf("after abandoned cap: %+v", stats)
	}
	if worker.AllowSessionCreation() {
		t.Fatal("draining worker must reject new session creation")
	}
}

func TestWorkerSessionLedgerPreparedAndFreshCap(t *testing.T) {
	worker := &AntigravityAcpWorker{
		sessions:             make(map[string]workerSessionState),
		maxSessionsPerWorker: 1,
	}
	worker.RegisterSessionCreated("fresh-1", "fresh")
	if worker.AllowSessionCreation() {
		t.Fatal("session cap must reject the next session")
	}
	stats := worker.SessionStats()
	if stats.CreatedTotal != 1 || !stats.Draining || stats.DrainReason != "session_cap" {
		t.Fatalf("unexpected cap stats: %+v", stats)
	}
}

func TestAntigravityAcpPool_RecyclesWorkerAtSessionCap(t *testing.T) {
	var spawns int32
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 0, 0, 0, 1, 0, func(context.Context, string) (*acp.Client, error) {
		atomic.AddInt32(&spawns, 1)
		return &acp.Client{}, nil
	})
	defer pool.Close()

	worker, err := pool.Acquire(context.Background(), "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	worker.RegisterSessionCreated("fresh-1", "fresh")
	worker.AbandonSession("fresh-1")
	pool.Release(worker, true)

	if got := len(pool.Workers("auth-1")); got != 0 {
		t.Fatalf("draining worker remained in pool: %d", got)
	}
	replacement, err := pool.Acquire(context.Background(), "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire replacement: %v", err)
	}
	if replacement == worker || atomic.LoadInt32(&spawns) != 2 {
		t.Fatalf("replacement worker/spawn count mismatch: replacement=%p old=%p spawns=%d", replacement, worker, spawns)
	}
	pool.Release(replacement, true)
}

func TestWorkerSessionLedgerDoesNotRecycleBoundSession(t *testing.T) {
	pool := NewAntigravityAcpPoolWithSessionLimits(1, 0, 0, 0, 1, 0, func(context.Context, string) (*acp.Client, error) {
		return &acp.Client{}, nil
	})
	defer pool.Close()

	worker, err := pool.Acquire(context.Background(), "auth-1", nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	worker.RegisterSessionCreated("strict-1", "fresh")
	worker.BindSession("strict-1", "strict")
	pool.Release(worker, true)
	if got := len(pool.Workers("auth-1")); got != 1 {
		t.Fatalf("bound draining worker should remain until binding removal: %d", got)
	}
	worker.AbandonSession("strict-1")
	deadline := time.After(time.Second)
	for {
		if len(pool.Workers("auth-1")) == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("worker was not recycled after bound session removal")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
