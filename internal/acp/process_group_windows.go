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

func waitProcessGroupGone(process *os.Process) {
	// cmd.Wait is already awaited by the common reaper on Windows. There is
	// no Unix-style descendant group to probe here; true descendant ownership
	// would require a Job Object and is outside this platform-neutral client.
}

func killProcessGroup(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}
