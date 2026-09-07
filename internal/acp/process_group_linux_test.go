//go:build linux

package acp

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestACPProcessTreeHelper(t *testing.T) {
	if os.Getenv("ACP_PROCESS_TREE_HELPER") != "1" {
		return
	}
	if os.Getenv("ACP_PROCESS_TREE_CHILD") == "1" {
		if os.Getenv("ACP_PROCESS_TREE_IGNORE_TERM") == "1" {
			signal.Ignore(syscall.SIGTERM)
		}
		for {
			time.Sleep(time.Hour)
		}
	}

	pidFile := os.Getenv("ACP_PROCESS_TREE_PID_FILE")
	if pidFile == "" {
		t.Fatal("ACP_PROCESS_TREE_PID_FILE is empty")
	}
	child := exec.Command(os.Args[0], "-test.run=TestACPProcessTreeHelper", "--")
	child.Env = append(os.Environ(), "ACP_PROCESS_TREE_HELPER=1", "ACP_PROCESS_TREE_CHILD=1")
	if os.Getenv("ACP_PROCESS_TREE_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
		child.Env = append(child.Env, "ACP_PROCESS_TREE_IGNORE_TERM=1")
	}
	if err := child.Start(); err != nil {
		t.Fatalf("start child helper: %v", err)
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		t.Fatalf("write child pid: %v", err)
	}

	if os.Getenv("ACP_PROCESS_TREE_IGNORE_CLOSE") == "1" {
		for {
			time.Sleep(time.Hour)
		}
	}
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
}

func TestClientCloseTerminatesProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	env := append(os.Environ(),
		"ACP_PROCESS_TREE_HELPER=1",
		"ACP_PROCESS_TREE_PID_FILE="+pidFile,
	)
	client, err := NewClient(SpawnConfig{
		Command:   os.Args[0],
		Args:      []string{"-test.run=TestACPProcessTreeHelper", "--"},
		Env:       env,
		LogStderr: func(line string) { t.Logf("helper stderr: %s", line) },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	childPID := waitForChildPID(t, pidFile)
	if err := client.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case <-client.ExitErr():
	case <-time.After(2 * time.Second):
		t.Fatal("ACP parent did not exit after stdin close")
	}

	waitForProcessGone(t, childPID)
}

func TestClientCloseForceKillsProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	env := append(os.Environ(),
		"ACP_PROCESS_TREE_HELPER=1",
		"ACP_PROCESS_TREE_PID_FILE="+pidFile,
		"ACP_PROCESS_TREE_IGNORE_CLOSE=1",
		"ACP_PROCESS_TREE_IGNORE_TERM=1",
	)
	client, err := NewClient(SpawnConfig{
		Command:   os.Args[0],
		Args:      []string{"-test.run=TestACPProcessTreeHelper", "--"},
		Env:       env,
		LogStderr: func(line string) { t.Logf("helper stderr: %s", line) },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	childPID := waitForChildPID(t, pidFile)
	select {
	case exitErr := <-client.ExitErr():
		t.Fatalf("ACP parent exited before Close: %v", exitErr)
	default:
	}
	if err := client.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait: %v", err)
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", childPID)); !os.IsNotExist(err) {
		t.Fatalf("CloseAndWait returned while child process %d still exists (err=%v)", childPID, err)
	}
	select {
	case <-client.ExitErr():
	case <-time.After(8 * time.Second):
		t.Fatal("ACP parent did not exit after process-group escalation")
	}

	waitForProcessGone(t, childPID)
}

func waitForChildPID(t *testing.T, pidFile string) int {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if raw, err := os.ReadFile(pidFile); err == nil {
			pid, parseErr := strconv.Atoi(string(raw))
			if parseErr != nil {
				t.Fatalf("parse child pid %q: %v", raw, parseErr)
			}
			return pid
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for child pid")
		case <-ticker.C:
		}
	}
}

func waitForProcessGone(t *testing.T, pid int) {
	t.Helper()
	procPath := fmt.Sprintf("/proc/%d", pid)
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(procPath); os.IsNotExist(err) {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("child process %d still exists", pid)
		case <-ticker.C:
		}
	}
}
