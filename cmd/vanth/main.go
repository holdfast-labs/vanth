package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "charm.land/bubbletea/v2"

	"vanth/internal/config"
	"vanth/internal/monitor"
)

func versionJSON() (string, error) {
	payload := map[string]string{
		"name":    "vanth",
		"version": config.Version,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// runMonitor opens the canonical home read-only and runs the Bubble Tea
// monitor. Missing or busy databases degrade to empty-state or stale screens;
// Bubble Tea restores the terminal on quit, Ctrl+C, and panics caught at the
// program boundary.
func runMonitor(home string) int {
	cfg := monitor.DefaultConfig(home)
	q := monitor.NewQuerier(cfg)
	defer q.Close()
	m := monitor.New(cfg, q)
	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "vanth monitor: %v\n", err)
		return 1
	}
	return 0
}

// programName is argv[0]'s basename without .exe, lowercased: the binary is
// multi-call and the name it was installed under selects its role.
func programName(argv0 string) string {
	base := strings.ToLower(filepath.Base(strings.ReplaceAll(argv0, "\\", "/")))
	return strings.TrimSuffix(base, ".exe")
}

// dispatchArgs maps the program name onto the subcommand form: `vanth-mcp`
// is the MCP launcher (every arg forwarded to the server); `vanth-monitor`
// defaults to the monitor subcommand, as its former Python wrapper did.
func dispatchArgs(argv []string) (mcp bool, args []string) {
	args = argv[1:]
	switch programName(argv[0]) {
	case "vanth-mcp":
		return true, args
	case "vanth-monitor":
		if len(args) == 0 || (args[0] != "monitor" && args[0] != "--version") {
			args = append([]string{"monitor"}, args...)
		}
	}
	return false, args
}

func main() {
	mcp, args := dispatchArgs(os.Args)
	if mcp {
		os.Exit(runMCP(args))
	}
	os.Args = append([]string{os.Args[0]}, args...)
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		asJSON := len(os.Args) > 2 && os.Args[2] == "--json"
		if asJSON {
			text, err := versionJSON()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println(text)
			return
		}
		fmt.Printf("vanth %s\n", config.Version)
		return
	}
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "monitor":
			home, err := config.CanonicalHome()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			os.Exit(runMonitor(home))
		default:
			fmt.Fprintf(os.Stderr, "vanth: unknown subcommand %q\n", os.Args[1])
			fmt.Fprintln(os.Stderr, "Usage: vanth [--version [--json]] monitor")
			os.Exit(2)
		}
	}
	fmt.Fprintln(os.Stderr, "vanth: a subcommand is required")
	fmt.Fprintln(os.Stderr, "Usage: vanth [--version [--json]] monitor")
	os.Exit(2)
}
