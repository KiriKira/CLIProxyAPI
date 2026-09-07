//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package acp

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

// configureProcessGroup puts the ACP daemon in a dedicated process group so
// descendants such as localharness_external can be terminated together.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// terminateProcessGroup asks the daemon and all of its descendants to exit.
func terminateProcessGroup(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return nil
	}
	return syscall.Kill(-process.Pid, syscall.SIGTERM)
}

// waitProcessGroupGone blocks until the dedicated process group no longer
// exists. cmd.Wait only covers the leader; the hard-capacity handoff must wait
// for descendants as well. The caller runs this outside all pool locks.
func waitProcessGroupGone(process *os.Process) {
	if process == nil || process.Pid <= 0 {
		return
	}
	for {
		err := syscall.Kill(-process.Pid, 0)
		if err == syscall.ESRCH {
			return
		}
		// EPERM still means that a member exists; any other result is
		// likewise not proof that the group is gone. Keep the strict handoff
		// conservative and retry.
		_ = killProcessGroup(process)
		time.Sleep(10 * time.Millisecond)
	}
}

// killProcessGroup forcefully terminates any descendant that ignored SIGTERM.
func killProcessGroup(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return nil
	}
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}
