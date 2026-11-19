package main

import (
	"os"
	"path/filepath"
	"strings"
)

// uvToolDirs are the places uv keeps tool envs: UV_TOOL_DIR, else its
// platform data dir. They are only candidates: a receipt must vouch for us.
func uvToolDirs(getenv func(string) string, goos string) []string {
	var dirs []string
	if dir := getenv("UV_TOOL_DIR"); dir != "" {
		dirs = append(dirs, dir)
	}
	if dir := getenv("XDG_DATA_HOME"); dir != "" {
		dirs = append(dirs, filepath.Join(dir, "uv", "tools"))
	}
	if goos == "windows" {
		if dir := getenv("APPDATA"); dir != "" {
			dirs = append(dirs, filepath.Join(dir, "uv", "tools"))
		}
	} else if dir := getenv("HOME"); dir != "" {
		dirs = append(dirs, filepath.Join(dir, ".local", "share", "uv", "tools"))
	}
	return dirs
}

// uvToolEnvBin returns the bin dir of the uv tool env whose uv-receipt.toml
// lists self as its installed vanth-mcp entry point, or "". `uv tool install`
// on Windows COPIES entry points into its bin dir, so the copy has no env
// next to it; the receipt is uv's own record of which env the copy belongs to.
func uvToolEnvBin(self string, toolDirs []string, goos string) string {
	binName := "bin"
	if goos == "windows" {
		binName = "Scripts"
	}
	want := normPath(self, goos)
	for _, dir := range toolDirs {
		receipts, _ := filepath.Glob(filepath.Join(dir, "*", "uv-receipt.toml"))
		for _, receipt := range receipts {
			data, err := os.ReadFile(receipt)
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(data), "\n") {
				if !strings.Contains(line, `name = "vanth-mcp"`) {
					continue
				}
				_, rest, found := strings.Cut(line, `install-path = "`)
				path, _, closed := strings.Cut(rest, `"`)
				// TOML basic strings escape backslashes; uv writes forward slashes.
				path = strings.ReplaceAll(path, `\\`, `\`)
				if found && closed && normPath(path, goos) == want {
					return filepath.Join(filepath.Dir(receipt), binName)
				}
			}
		}
	}
	return ""
}

func normPath(path, goos string) string {
	if goos == "windows" {
		return strings.ToLower(filepath.Clean(strings.ReplaceAll(path, "/", `\`)))
	}
	return filepath.Clean(path)
}

func hasEnvPython(dir, goos string) bool {
	if goos == "windows" {
		return isFile(filepath.Join(dir, "python.exe"))
	}
	return isFile(filepath.Join(dir, "python")) || isFile(filepath.Join(dir, "python3"))
}
