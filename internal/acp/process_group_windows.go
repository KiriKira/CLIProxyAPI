//go:build windows

package acp

import (
	"os"
	"os/exec"
)

// Windows does not expose Unix-style process groups through os/exec. Keep the
// platform hook explicit; the daemon process itself is still terminated.
func configureProcessGroup(cmd *exec.Cmd) {}

func terminateProcessGroup(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}

func killProcessGroup(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}
