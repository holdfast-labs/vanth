//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"
)

// startServer replaces this process with the server: one process, same PID,
// so the MCP client's own process supervision applies to the server directly.
func startServer(plan serverPlan, argv []string) int {
	err := syscall.Exec(plan.Exe, argv, serverEnv(plan))
	fmt.Fprintf(os.Stderr, "vanth-mcp: cannot exec %s: %v\n", plan.Exe, err)
	return 1
}
