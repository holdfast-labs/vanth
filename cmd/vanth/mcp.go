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
	// Notes are stderr diagnostics for every fallback taken.
	Notes []string
}

// resolveServer finds the interpreter for the env vanth-mcp is installed in.
// binDir is the directory holding the (symlink-resolved) vanth-mcp executable:
// the env's Scripts\ (Windows) or bin/ (POSIX). Nothing is hard-coded: the base
// interpreter comes from the env's pyvenv.cfg at run time.
func resolveServer(binDir, goos string, lookPath func(string) (string, error)) (serverPlan, error) {
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
	for _, name := range names {
		if found, err := lookPath(name); err == nil {
			plan.Notes = append(plan.Notes, fmt.Sprintf(
				"no Python env found next to %s; falling back to %s on PATH", binDir, found))
			plan.Exe = found
			return plan, nil
		}
	}
	return plan, fmt.Errorf("no Python interpreter found next to %s or on PATH; reinstall vanth (uv tool install --force vanth) or run `python -m vanth.server`", binDir)
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
// variable when the base interpreter is started directly.
func serverEnv(plan serverPlan) []string {
	env := os.Environ()
	if plan.Launcher != "" {
		env = append(env, "__PYVENV_LAUNCHER__="+plan.Launcher)
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
	dir := filepath.Dir(self)
	if !hasEnvPython(dir, runtime.GOOS) {
		if bin := uvToolEnvBin(self, uvToolDirs(os.Getenv, runtime.GOOS), runtime.GOOS); bin != "" {
			dir = bin
		}
	}
	plan, err := resolveServer(dir, runtime.GOOS, exec.LookPath)
	for _, note := range plan.Notes {
		fmt.Fprintln(os.Stderr, "vanth-mcp: "+note)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vanth-mcp: "+err.Error())
		return 1
	}
	argv := append([]string{plan.Exe, "-m", "vanth.server"}, args...)
	return startServer(plan, argv)
}
