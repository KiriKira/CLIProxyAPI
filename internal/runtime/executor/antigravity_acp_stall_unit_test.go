package executor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWatchdogUnit_TripOnSilence(t *testing.T) {
	wd := newACPPromptWatchdog(300 * time.Millisecond)
	start := time.Now()
	err := wd.Wait(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	if !errors.Is(err, errStallDetected) {
		t.Fatalf("want errStallDetected, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("watchdog fired too late: %v", elapsed)
	}
}

func TestWatchdogUnit_KickKeepsAlive(t *testing.T) {
	wd := newACPPromptWatchdog(300 * time.Millisecond)
	// Kick every 100ms for 1.2s: silence never exceeds 300ms, so the fn's
	// own completion (nil) must win — never errStallDetected.
	done := make(chan error, 1)
	go func() {
		done <- wd.Wait(context.Background(), func(ctx context.Context) error {
			for i := 0; i < 12; i++ {
				time.Sleep(100 * time.Millisecond)
				wd.kick()
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("kicked watchdog must not trip: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after fn completed")
	}
}

func TestWatchdogUnit_NilDisabled(t *testing.T) {
	var wd *acpPromptWatchdog
	err := wd.Wait(context.Background(), func(ctx context.Context) error {
		return errors.New("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("nil watchdog must pass fn error through, got %v", err)
	}
}
