// Package config resolves Vanth's canonical state root and runtime limits.
//
// Home resolution must match src/vanth/paths.py: VANTH_HOME is canonical,
// AGENT_BG_HOME is a supported alias, both must agree when both are set, and
// the result is an absolute, symlink-resolved path.
package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// Version is stamped at build time via -ldflags. It mirrors the Python
// package version so daemon discovery metadata stays consistent.
var Version = "1.0.0"

// DefaultHome returns the state root used when no home is configured.
func DefaultHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".vanth"), nil
}

// CanonicalHome resolves the one state root shared by daemon, client, runner,
// and monitor. It mirrors src/vanth/paths.canonical_home: VANTH_HOME is
// canonical, AGENT_BG_HOME is a supported alias, both must agree when both
// are set, and the result is absolute with `~` expanded and symlinks
// resolved (falling back to the cleaned absolute path when resolution fails,
// e.g. a not-yet-created home — matching Python resolve() strict=False).
func CanonicalHome() (string, error) {
	vant := os.Getenv("VANTH_HOME")
	agent := os.Getenv("AGENT_BG_HOME")
	if vant != "" && agent != "" {
		vantPath, err := resolveHome(vant)
		if err != nil {
			return "", fmt.Errorf("resolve VANTH_HOME: %w", err)
		}
		agentPath, err := resolveHome(agent)
		if err != nil {
			return "", fmt.Errorf("resolve AGENT_BG_HOME: %w", err)
		}
		if vantPath != agentPath {
			return "", fmt.Errorf("VANTH_HOME and AGENT_BG_HOME refer to different state directories")
		}
		return vantPath, nil
	}
	configured := vant
	if configured == "" {
		configured = agent
	}
	if configured == "" {
		return DefaultHome()
	}
	return resolveHome(configured)
}

// resolveHome expands a leading `~`, absolutizes, and resolves symlinks,
// mirroring Python's Path.expanduser().resolve(). A bare `~` or `~/...`
// resolves against the current user's home; anything else passes through.
func resolveHome(raw string) (string, error) {
	if raw == "~" || len(raw) > 1 && (raw[0] == '~' && (raw[1] == '/' || raw[1] == '\\')) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand ~: %w", err)
		}
		raw = filepath.Join(home, raw[1:])
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, nil
	}
	return filepath.Clean(abs), nil
}
