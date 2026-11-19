package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestMain lets the test binary play the installed roles: copied as
// vanth-mcp it is the shim; copied as python it is a fake interpreter that
// reports what it was started with.
func TestMain(m *testing.M) {
	switch programName(os.Args[0]) {
	case "vanth-mcp":
		os.Exit(runMCP(os.Args[1:]))
	case "python", "python3":
		self, _ := os.Executable()
		io.WriteString(os.Stdout, "exe="+self+"\nargs="+strings.Join(os.Args[1:], " ")+
			"\nlauncher="+os.Getenv("__PYVENV_LAUNCHER__")+"\n")
		os.Exit(7)
	}
	switch os.Getenv("VANTH_TEST_HELPER") {
	case "sleep":
		time.Sleep(60 * time.Second)
		os.Exit(0)
	case "spawn":
		// Start a grandchild, report its pid, then wait to be killed.
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), "VANTH_TEST_HELPER=sleep")
		if err := cmd.Start(); err != nil {
			os.Exit(3)
		}
		fmt.Println(cmd.Process.Pid)
		time.Sleep(60 * time.Second)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestDispatchArgs(t *testing.T) {
	cases := []struct {
		argv []string
		mcp  bool
		args []string
	}{
		{[]string{`C:\env\Scripts\vanth-mcp.exe`, "--x"}, true, []string{"--x"}},
		{[]string{"/env/bin/vanth-mcp"}, true, []string{}},
		{[]string{"VANTH-MONITOR.EXE"}, false, []string{"monitor"}},
		{[]string{"/bin/vanth-monitor", "--version", "--json"}, false, []string{"--version", "--json"}},
		{[]string{"vanth-monitor", "monitor"}, false, []string{"monitor"}},
		{[]string{"vanth-monitor", "--flag"}, false, []string{"monitor", "--flag"}},
		{[]string{"vanth", "monitor"}, false, []string{"monitor"}},
		{[]string{"vanth"}, false, []string{}},
	}
	for _, c := range cases {
		mcp, args := dispatchArgs(c.argv)
		if mcp != c.mcp || strings.Join(args, "|") != strings.Join(c.args, "|") {
			t.Errorf("dispatchArgs(%q) = %v %q, want %v %q", c.argv, mcp, args, c.mcp, c.args)
		}
	}
}

func touch(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o755); err != nil {
		t.Fatal(err)
	}
}

func noPath(string) (string, error) { return "", errors.New("not found") }

func TestResolveWindowsVenvUsesBaseWithLauncher(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "base")
	touch(t, filepath.Join(base, "python.exe"), "")
	touch(t, filepath.Join(root, "env", "pyvenv.cfg"), "implementation = CPython\nhome = "+base+"\n")
	scripts := filepath.Join(root, "env", "Scripts")
	touch(t, filepath.Join(scripts, "python.exe"), "")
	plan, err := resolveServer(scripts, "windows", noPath)
	if err != nil || plan.Exe != filepath.Join(base, "python.exe") || plan.Launcher != filepath.Join(scripts, "python.exe") || len(plan.Notes) != 0 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}

func TestResolveWindowsFallbacks(t *testing.T) {
	root := t.TempDir()
	scripts := filepath.Join(root, "env", "Scripts")
	touch(t, filepath.Join(scripts, "python.exe"), "")
	// pyvenv.cfg names a base that is gone: the env launcher, with a note.
	touch(t, filepath.Join(root, "env", "pyvenv.cfg"), "home = "+filepath.Join(root, "gone")+"\n")
	plan, err := resolveServer(scripts, "windows", noPath)
	if err != nil || plan.Exe != filepath.Join(scripts, "python.exe") || plan.Launcher != "" || len(plan.Notes) != 1 {
		t.Fatalf("missing base: plan=%+v err=%v", plan, err)
	}
	// No pyvenv.cfg at all: still the env's own python, with a note.
	os.Remove(filepath.Join(root, "env", "pyvenv.cfg"))
	plan, err = resolveServer(scripts, "windows", noPath)
	if err != nil || plan.Exe != filepath.Join(scripts, "python.exe") || len(plan.Notes) != 1 {
		t.Fatalf("no cfg: plan=%+v err=%v", plan, err)
	}
	// System install: <prefix>\Scripts\vanth-mcp.exe with <prefix>\python.exe.
	sys := filepath.Join(root, "Python314")
	touch(t, filepath.Join(sys, "python.exe"), "")
	plan, err = resolveServer(filepath.Join(sys, "Scripts"), "windows", noPath)
	if err != nil || plan.Exe != filepath.Join(sys, "python.exe") || len(plan.Notes) != 0 {
		t.Fatalf("system: plan=%+v err=%v", plan, err)
	}
	// Nothing next to the shim (copied into a bare bin dir): PATH, with a note.
	plan, err = resolveServer(filepath.Join(root, "bare"), "windows",
		func(name string) (string, error) { return `C:\py\` + name + ".exe", nil })
	if err != nil || plan.Exe != `C:\py\python.exe` || len(plan.Notes) != 1 {
		t.Fatalf("path: plan=%+v err=%v", plan, err)
	}
	if _, err := resolveServer(filepath.Join(root, "bare"), "windows", noPath); err == nil {
		t.Fatal("expected an error when no interpreter exists anywhere")
	}
}

func TestResolvePOSIX(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "env", "bin")
	touch(t, filepath.Join(bin, "python3"), "")
	plan, err := resolveServer(bin, "linux", noPath)
	if err != nil || plan.Exe != filepath.Join(bin, "python3") || plan.Launcher != "" || len(plan.Notes) != 0 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	touch(t, filepath.Join(bin, "python"), "")
	if plan, _ = resolveServer(bin, "darwin", noPath); plan.Exe != filepath.Join(bin, "python") {
		t.Fatalf("prefers bin/python: %+v", plan)
	}
	plan, err = resolveServer(filepath.Join(root, "usr-local-bin"), "linux",
		func(name string) (string, error) {
			if name == "python3" {
				return "/usr/bin/python3", nil
			}
			return "", errors.New("no")
		})
	if err != nil || plan.Exe != "/usr/bin/python3" || len(plan.Notes) != 1 {
		t.Fatalf("path fallback: plan=%+v err=%v", plan, err)
	}
}

func TestParsePyvenvHome(t *testing.T) {
	home, err := parsePyvenvHome(strings.NewReader("Home = C:\\Py 3\\ \nversion = 3.14\n"), "cfg")
	if err != nil || home != `C:\Py 3\` {
		t.Fatalf("home=%q err=%v", home, err)
	}
	if _, err := parsePyvenvHome(strings.NewReader("version = 3\n"), "cfg"); err == nil {
		t.Fatal("expected missing-home error")
	}
}

// TestShimEndToEnd installs the test binary as an env's vanth-mcp and python
// and checks interpreter choice, arg forwarding, launcher env and exit code.
// On Windows the env python is the launcher and the base runs; on POSIX the
// shim execs bin/python in place.
func TestShimEndToEnd(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	exe, binName := "", "bin"
	if runtime.GOOS == "windows" {
		exe, binName = ".exe", "Scripts"
	}
	bin := filepath.Join(root, "env", binName)
	base := filepath.Join(root, "base")
	for _, path := range []string{filepath.Join(bin, "vanth-mcp"+exe), filepath.Join(bin, "python"+exe), filepath.Join(base, "python"+exe)} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	touch(t, filepath.Join(root, "env", "pyvenv.cfg"), "home = "+base+"\n")
	out, err := exec.Command(filepath.Join(bin, "vanth-mcp"+exe), "--flag", "x").Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("want exit 7, got %v (out=%s)", err, out)
	}
	text := string(out)
	if !strings.Contains(text, "args=-m vanth.server --flag x\n") {
		t.Fatalf("args not forwarded: %s", text)
	}
	want := "exe=" + filepath.Join(bin, "python"+exe) + "\nargs=-m vanth.server --flag x\nlauncher=\n"
	if runtime.GOOS == "windows" {
		want = "exe=" + filepath.Join(base, "python.exe") + "\nargs=-m vanth.server --flag x\nlauncher=" + filepath.Join(bin, "python.exe") + "\n"
	}
	if text != want {
		t.Fatalf("got\n%s\nwant\n%s", text, want)
	}
}

func TestUVToolCopyFindsItsEnvThroughTheReceipt(t *testing.T) {
	root := t.TempDir()
	tools := filepath.Join(root, "tools")
	self := filepath.Join(root, "bin", "vanth-mcp.exe")
	receipt := "[tool]\nentrypoints = [\n" +
		`    { name = "vanth", install-path = "` + filepath.ToSlash(filepath.Join(root, "bin", "vanth.exe")) + `", from = "vanth" },` + "\n" +
		`    { name = "vanth-mcp", install-path = "` + strings.ToUpper(filepath.ToSlash(self)) + `", from = "vanth" },` + "\n]\n"
	touch(t, filepath.Join(tools, "other", "uv-receipt.toml"), "[tool]\nentrypoints = []\n")
	touch(t, filepath.Join(tools, "vanth", "uv-receipt.toml"), receipt)
	if got := uvToolEnvBin(self, []string{filepath.Join(root, "missing"), tools}, "windows"); got != filepath.Join(tools, "vanth", "Scripts") {
		t.Fatalf("got %q", got)
	}
	// A receipt for some other bin dir does not vouch for this copy.
	if got := uvToolEnvBin(filepath.Join(root, "elsewhere", "vanth-mcp.exe"), []string{tools}, "windows"); got != "" {
		t.Fatalf("unrelated copy matched %q", got)
	}
	env := map[string]string{"UV_TOOL_DIR": `D:\t`, "APPDATA": `C:\a`, "HOME": "/h"}
	dirs := uvToolDirs(func(k string) string { return env[k] }, "windows")
	if strings.Join(dirs, "|") != `D:\t|`+filepath.Join(`C:\a`, "uv", "tools") {
		t.Fatalf("windows dirs %q", dirs)
	}
	dirs = uvToolDirs(func(k string) string { return env[k] }, "linux")
	if strings.Join(dirs, "|") != `D:\t|`+filepath.Join("/h", ".local", "share", "uv", "tools") {
		t.Fatalf("posix dirs %q", dirs)
	}
}
