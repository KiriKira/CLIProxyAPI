//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package acp

import (
	"os"
	"os/exec"
	"syscall"
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

// killProcessGroup forcefully terminates any descendant that ignored SIGTERM.
func killProcessGroup(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return nil
	}
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}
