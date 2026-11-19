package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// serverPlan is how vanth-mcp starts the Python MCP server.
type serverPlan struct {
	// Exe is the interpreter to run with "-m vanth.server".
	Exe string
	// Launcher, when set, is the env's own python (Windows Scripts\python.exe):
	// Exe is then the base interpreter and adopts the env through
	// __PYVENV_LAUNCHER__, exactly as the venv launcher (and multiprocessing)
	// would, without the launcher's extra process.
	Launcher string
	// Console, when set, means Exe is the sibling `vanth` console script: run
	// it with the caller's args (bare `vanth` serves MCP) instead of
	// `-m vanth.server`.
	Console bool
	// Shim is the absolute path of the running vanth-mcp shim, passed to the
	// server as VANTH_MCP_SHIM so its watchdog can step over the shim layer.
	Shim string
	// Notes are stderr diagnostics for every fallback taken.
	Notes []string
}

// resolveServer finds the interpreter for the env vanth-mcp is installed in.
// binDir is the directory holding the (symlink-resolved) vanth-mcp executable:
// the env's Scripts\ (Windows) or bin/ (POSIX). shimDir is the directory the
// running shim was launched from (the uv tool bin dir for a copied shim); its
// sibling `vanth` console script is the last-resort fallback before PATH.
// Nothing is hard-coded: the base interpreter comes from the env's
// pyvenv.cfg at run time.
func resolveServer(binDir, shimDir, goos string, lookPath func(string) (string, error)) (serverPlan, error) {
	var plan serverPlan
	exe := ""
	names := []string{"python", "python3"}
	if goos == "windows" {
		exe = ".exe"
		names = []string{"python"}
	}
	var envPython string
	for _, name := range names {
		if candidate := filepath.Join(binDir, name+exe); isFile(candidate) {
			envPython = candidate
			break
		}
	}
	if goos != "windows" {
		// POSIX bin/python is the interpreter itself (a symlink); exec it.
		if envPython != "" {
			plan.Exe = envPython
			return plan, nil
		}
	} else {
		root := filepath.Dir(binDir)
		home, err := pyvenvHome(filepath.Join(root, "pyvenv.cfg"))
		switch {
		case err == nil && envPython != "":
			if base := filepath.Join(home, "python.exe"); isFile(base) {
				plan.Exe, plan.Launcher = base, envPython
				return plan, nil
			}
			plan.Notes = append(plan.Notes, fmt.Sprintf(
				"base interpreter %s from %s not found; using the env launcher %s (one extra process)",
				filepath.Join(home, "python.exe"), filepath.Join(root, "pyvenv.cfg"), envPython))
			plan.Exe = envPython
			return plan, nil
		case envPython != "":
			plan.Notes = append(plan.Notes, fmt.Sprintf(
				"no usable pyvenv.cfg next to %s (%v); running %s directly", binDir, err, envPython))
			plan.Exe = envPython
			return plan, nil
		default:
			// System (non-venv) install: Scripts\ sits under the interpreter dir.
			if candidate := filepath.Join(root, "python.exe"); isFile(candidate) && os.IsNotExist(err) {
				plan.Exe = candidate
				return plan, nil
			}
		}
	}
	// A uv tool copy has no env next to it. Its sibling `vanth` console script
	// is a trampoline with the env path baked in, so it works even when the
	// runtime environment lost UV_TOOL_DIR (agents often launch with a
	// different environment than the install shell).
	if console := siblingConsole(shimDir, goos); console != "" {
		plan.Notes = append(plan.Notes, fmt.Sprintf(
			"no Python env found next to %s; falling back to the sibling console script %s", binDir, console))
		plan.Exe, plan.Console = console, true
		return plan, nil
	}
	for _, name := range names {
		if found, err := lookPath(name); err == nil {
			plan.Notes = append(plan.Notes, fmt.Sprintf(
				"no Python env found next to %s; falling back to %s on PATH", binDir, found))
			plan.Exe = found
			return plan, nil
		}
	}
	return plan, fmt.Errorf("no Python interpreter found next to %s, no sibling vanth console script in %s, and none on PATH; reinstall vanth (uv tool install --force vanth) or run `python -m vanth.server`", binDir, shimDir)
}

// siblingConsole returns the `vanth` console script next to the shim, or "".
func siblingConsole(shimDir, goos string) string {
	name := "vanth"
	if goos == "windows" {
		name = "vanth.exe"
	}
	if shimDir == "" {
		return ""
	}
	candidate := filepath.Join(shimDir, name)
	if isFile(candidate) {
		return candidate
	}
	return ""
}

// pyvenvHome returns the `home =` directory from a pyvenv.cfg.
func pyvenvHome(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return parsePyvenvHome(f, path)
}

func parsePyvenvHome(r io.Reader, path string) (string, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "=")
		if ok && strings.EqualFold(strings.TrimSpace(key), "home") {
			if home := strings.TrimSpace(value); home != "" {
				return home, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s has no home = entry", path)
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// selfPath is the running executable with symlinks resolved, so a uv tool
// bin-dir symlink (POSIX) leads back to the tool env's bin/.
func selfPath() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return path, nil
}

// serverEnv is the child environment: the current one, plus the launcher
// variable when the base interpreter is started directly, plus the shim path
// so the server's watchdog can step over the shim. VANTH_MCP_SHIM is set only
// here, by the shim, and the server removes it at startup.
func serverEnv(plan serverPlan) []string {
	env := make([]string, 0, len(os.Environ())+2)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "VANTH_MCP_SHIM=") {
			continue
		}
		env = append(env, entry)
	}
	if plan.Launcher != "" {
		env = append(env, "__PYVENV_LAUNCHER__="+plan.Launcher)
	}
	if plan.Shim != "" {
		env = append(env, "VANTH_MCP_SHIM="+plan.Shim)
	}
	return env
}

// runMCP is the vanth-mcp entry point: start `python -m vanth.server` for the
// env this binary is installed in, forwarding args and stdio.
func runMCP(args []string) int {
	self, err := selfPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "vanth-mcp: cannot locate own executable: %v\n", err)
		return 1
	}
	selfDir := filepath.Dir(self)
	dir := selfDir
	if !hasEnvPython(dir, runtime.GOOS) {
		if bin := uvToolEnvBin(self, uvToolDirs(os.Getenv, runtime.GOOS), runtime.GOOS); bin != "" {
			dir = bin
		}
	}
	plan, err := resolveServer(dir, selfDir, runtime.GOOS, exec.LookPath)
	plan.Shim = self
	for _, note := range plan.Notes {
		fmt.Fprintln(os.Stderr, "vanth-mcp: "+note)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vanth-mcp: "+err.Error())
		return 1
	}
	serverArgs := args
	if !plan.Console {
		serverArgs = append([]string{"-m", "vanth.server"}, args...)
	}
	argv := append([]string{plan.Exe}, serverArgs...)
	return startServer(plan, argv)
}
