package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"unsafe"

	"golang.org/x/sys/windows"
)

// startServer runs the server as a child that dies with this shim: it is put
// in a KILL_ON_JOB_CLOSE job, like the venv launcher does. SILENT_BREAKAWAY_OK
// keeps the server's own children (the vanthd daemon, runners) out of the job
// so they outlive the MCP session exactly as they do under the launcher.
func startServer(plan serverPlan, argv []string) int {
	// The console delivers Ctrl+C to the whole group; the server handles it.
	signal.Notify(make(chan os.Signal, 1), os.Interrupt)
	cmd := exec.Command(plan.Exe, argv[1:]...)
	cmd.Args = argv
	cmd.Env = serverEnv(plan)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "vanth-mcp: cannot start %s: %v\n", plan.Exe, err)
		return 1
	}
	if err := containChild(uint32(cmd.Process.Pid)); err != nil {
		// Same as the CPython launcher: containment is best effort; the MCP
		// watchdog still ends an orphaned server.
		fmt.Fprintf(os.Stderr, "vanth-mcp: job object unavailable (%v); server may outlive this shim\n", err)
	}
	err := cmd.Wait()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "vanth-mcp: %v\n", err)
		return 1
	}
	return 0
}

// jobHandle stays open for the shim's lifetime; the kernel closes it when the
// shim exits (however it exits), which kills the contained server.
var jobHandle windows.Handle

func containChild(pid uint32) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE |
		windows.JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(job)
		return err
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, pid)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		windows.CloseHandle(job)
		return err
	}
	jobHandle = job
	return nil
}
