package main

import (
	"encoding/json"
	"fmt"
	"os"

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

func main() {
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
