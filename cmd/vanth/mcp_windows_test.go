package main

import (
	"bufio"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func alive(pid uint32) bool {
	h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, pid)
	if err != nil {
		return false
	}
	defer windows.CloseHandle(h)
	event, _ := windows.WaitForSingleObject(h, 0)
	return event == uint32(windows.WAIT_TIMEOUT)
}

// Closing the job (what happens when the shim exits) kills the contained
// server, while a process the server started (the vanthd daemon) survives.
func TestJobKillsServerButNotItsChildren(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "VANTH_TEST_HELPER=spawn")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := containChild(uint32(cmd.Process.Pid)); err != nil {
		cmd.Process.Kill()
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := strconv.Atoi(line[:len(line)-1])
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if p, err := os.FindProcess(grandchild); err == nil {
			p.Kill()
		}
	}()
	windows.CloseHandle(jobHandle)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("server survived the job closing")
	}
	if !alive(uint32(grandchild)) {
		t.Fatal("the server's own child was killed with the job; it must break away")
	}
}
