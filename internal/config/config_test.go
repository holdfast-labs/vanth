package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Absolute-looking paths that are valid on every platform so the alias tests
// exercise the same "given an absolute path, return it cleaned" behavior.
var absHome = filepath.Join(string(filepath.Separator), "state", "vanth")
var absAgent = filepath.Join(string(filepath.Separator), "state", "agent")
var absOther = filepath.Join(string(filepath.Separator), "state", "other")

func setHomeEnv(t *testing.T, vant, agent string) {
	t.Helper()
	for _, name := range []string{"VANTH_HOME", "AGENT_BG_HOME"} {
		t.Setenv(name, "")
	}
	if vant != "" {
		t.Setenv("VANTH_HOME", vant)
	}
	if agent != "" {
		t.Setenv("AGENT_BG_HOME", agent)
	}
}

func TestCanonicalHomeVanthTakesPrecedence(t *testing.T) {
	if runtime.GOOS == "windows" {
		absHome = `C:\state\vanth`
	}
	setHomeEnv(t, absHome, "")
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(absHome) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeAgentAlias(t *testing.T) {
	if runtime.GOOS == "windows" {
		absAgent = `C:\state\agent`
	}
	setHomeEnv(t, "", absAgent)
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(absAgent) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeConflictingAliasesRejected(t *testing.T) {
	if runtime.GOOS == "windows" {
		absHome = `C:\a`
		absAgent = `C:\b`
	}
	setHomeEnv(t, absHome, absAgent)
	if _, err := CanonicalHome(); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestCanonicalHomeMatchingAliasesOK(t *testing.T) {
	if runtime.GOOS == "windows" {
		absOther = `C:\same`
	}
	setHomeEnv(t, absOther, absOther)
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(absOther) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeDefaultsToUserVanth(t *testing.T) {
	setHomeEnv(t, "", "")
	if os.Getenv("VANTH_HOME") != "" || os.Getenv("AGENT_BG_HOME") != "" {
		t.Fatal("test environment must clear both home vars")
	}
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := DefaultHome()
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestCanonicalHomeExpandsTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no user home")
	}
	setHomeEnv(t, "~/.vanth", "")
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	want, err := resolveHome(filepath.Join(home, ".vanth"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("tilde not expanded like Python expanduser: got %q want %q", got, want)
	}
}

func TestCanonicalHomeTildeAndAbsoluteAliasAgree(t *testing.T) {
	// `~/.vanth` and its absolute spelling must compare equal after
	// resolution instead of raising a false conflict.
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no user home")
	}
	abs := filepath.Join(home, ".vanth")
	if runtime.GOOS == "windows" {
		abs = filepath.Clean(abs)
	}
	setHomeEnv(t, "~/.vanth", abs)
	got, err := CanonicalHome()
	if err != nil {
		t.Fatalf("tilde/absolute aliases must agree: %v", err)
	}
	want, _ := resolveHome(abs)
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
