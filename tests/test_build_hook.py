"""Unit tests for the Hatchling build hook that bundles the Go monitor.

These tests exercise the hook's pure helpers and its ``initialize`` mode
selection without requiring a Go toolchain or a real wheel build.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "build-hooks"))

from bundle_monitor import (  # noqa: E402
    BundleMonitorBuildHook,
    platform_tag_for,
    wheel_platform_tag,
)

CROSS_TARGET_TAGS = {
    ("windows", "amd64"): "win_amd64",
    ("linux", "amd64"): "manylinux_2_17_x86_64",
    ("linux", "arm64"): "manylinux_2_17_aarch64",
    ("darwin", "amd64"): "macosx_10_15_x86_64",
    ("darwin", "arm64"): "macosx_11_0_arm64",
}


class _FakeApp:
    def display_success(self, message: str) -> None:
        self.message = message


class _FakeMetadata:
    version = "1.2.1"


def _make_hook(root: str, target_name: str = "wheel") -> BundleMonitorBuildHook:
    # The hook's properties are defined on BuildHookInterface, so the mangled
    # private names use that class even though we instantiate the subclass.
    hook = BundleMonitorBuildHook.__new__(BundleMonitorBuildHook)
    hook._BuildHookInterface__app = _FakeApp()
    hook._BuildHookInterface__root = root
    hook._BuildHookInterface__target_name = target_name
    hook._BuildHookInterface__metadata = _FakeMetadata()
    return hook


def _fake_go_output(cmd) -> None:
    with open(cmd[cmd.index("-o") + 1], "wb") as out:
        out.write(b"built")


@pytest.mark.parametrize(
    ("goos", "goarch", "expected"),
    [
        ("windows", "amd64", "win_amd64"),
        ("linux", "amd64", "manylinux_2_17_x86_64"),
        ("linux", "arm64", "manylinux_2_17_aarch64"),
        ("darwin", "amd64", "macosx_10_15_x86_64"),
        ("darwin", "arm64", "macosx_11_0_arm64"),
    ],
)
def test_platform_tag_for_mapping(goos: str, goarch: str, expected: str) -> None:
    assert platform_tag_for(goos, goarch) == expected


def test_platform_tag_for_unknown_combination() -> None:
    assert platform_tag_for("freebsd", "riscv64") == "freebsd_riscv64"


def test_prebuilt_bin_injection(tmp_path, monkeypatch) -> None:
    """VANTH_MONITOR_BIN injects the file without ever invoking go."""
    dummy = tmp_path / "vanth-monitor-fake"
    dummy.write_bytes(b"fake monitor binary")

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_TAG", "win_amd64")

    calls = {}

    def no_go(*args, **kwargs):
        calls["go"] = True

    monkeypatch.setattr(subprocess, "run", no_go)

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    assert "go" not in calls
    exe = ".exe" if os.name == "nt" else ""
    binary = str(tmp_path / "dist" / f"vanth-monitor{exe}")
    shim = str(tmp_path / "dist" / f"vanth-mcp{exe}")
    # One multi-call binary shipped as two native scripts (no monitor-bin copy).
    assert build_data["shared_scripts"] == {binary: f"vanth-monitor{exe}", shim: f"vanth-mcp{exe}"}
    assert "force_include" not in build_data
    assert build_data["tag"] == "py3-none-win_amd64"
    assert os.path.isfile(binary)
    assert open(shim, "rb").read() == b"fake monitor binary"


def test_prebuilt_injection_names_binary_for_target_goos(tmp_path, monkeypatch) -> None:
    """A Linux-host wheel build for Windows must bundle the binary as
    vanth-monitor.exe (the runtime's Windows lookup), not vanth-monitor.

    Regression: CI assembles every wheel on ubuntu-latest, so before this fix a
    Windows wheel contained a non-.exe binary and the vanth-monitor console
    script failed to find the TUI binary.
    """
    dummy = tmp_path / "prebuilt-win.exe"
    dummy.write_bytes(b"windows monitor")

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "windows")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "amd64")
    monkeypatch.setenv("VANTH_MONITOR_TAG", "win_amd64")
    # Simulate a Linux CI host.
    monkeypatch.setattr("bundle_monitor.os.name", "posix")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    binary = str(tmp_path / "dist" / "vanth-monitor.exe")
    assert build_data["shared_scripts"] == {
        binary: "vanth-monitor.exe", str(tmp_path / "dist" / "vanth-mcp.exe"): "vanth-mcp.exe"
    }
    assert os.path.isfile(binary)


def test_prebuilt_injection_names_binary_for_target_posix(tmp_path, monkeypatch) -> None:
    """A Linux-host wheel build for Linux keeps the no-suffix name."""
    dummy = tmp_path / "prebuilt-linux"
    dummy.write_bytes(b"linux monitor")

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "linux")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "amd64")
    monkeypatch.setenv("VANTH_MONITOR_TAG", "manylinux_2_17_x86_64")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    binary = str(tmp_path / "dist" / "vanth-monitor")
    assert build_data["shared_scripts"] == {
        binary: "vanth-monitor", str(tmp_path / "dist" / "vanth-mcp"): "vanth-mcp"
    }
    assert os.path.isfile(binary)


def test_prebuilt_injection_sets_executable_mode_for_posix(tmp_path, monkeypatch) -> None:
    """Review P1-1: published POSIX wheels stored vanth-monitor as 0644, so
    running it failed with PermissionError. copyfile does not preserve the
    source's executable bit; the hook must chmod 0755 for non-Windows targets.

    Verifies the chmod call directly because a Windows host cannot observe
    POSIX mode bits via stat."""
    dummy = tmp_path / "prebuilt-linux"
    dummy.write_bytes(b"linux monitor")
    os.chmod(dummy, 0o644)

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "linux")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "amd64")
    monkeypatch.setattr("bundle_monitor.os.name", "posix")

    chmod_calls = []

    def fake_chmod(path, mode):
        chmod_calls.append((path, mode))

    monkeypatch.setattr("bundle_monitor.os.chmod", fake_chmod)

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    binary = str(tmp_path / "dist" / "vanth-monitor")
    assert (binary, 0o755) in chmod_calls, f"expected chmod 0755 on {binary}, got {chmod_calls}"
    assert os.path.isfile(binary)


def test_prebuilt_injection_keeps_windows_non_executable(tmp_path, monkeypatch) -> None:
    """Windows wheels bundle vanth-monitor.exe; chmod semantics are irrelevant
    there and the hook must not fail on them."""
    dummy = tmp_path / "prebuilt-win.exe"
    dummy.write_bytes(b"windows monitor")

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "windows")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "amd64")
    monkeypatch.setenv("VANTH_MONITOR_TAG", "win_amd64")
    monkeypatch.setattr("bundle_monitor.os.name", "posix")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    binary = str(tmp_path / "dist" / "vanth-monitor.exe")
    assert os.path.isfile(binary)
    assert build_data["tag"] == "py3-none-win_amd64"


def test_prebuilt_injection_falls_back_to_target_tag(tmp_path, monkeypatch) -> None:
    """Review P2-1: with a prebuilt binary + target GOOS/GOARCH and NO explicit
    VANTH_MONITOR_TAG, the wheel tag must come from the target (never the build
    host), so a Windows binary is never paired with a Linux wheel tag."""
    dummy = tmp_path / "prebuilt-win.exe"
    dummy.write_bytes(b"windows monitor")

    monkeypatch.setenv("VANTH_MONITOR_BIN", str(dummy))
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "windows")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "amd64")
    monkeypatch.delenv("VANTH_MONITOR_TAG", raising=False)
    # Build host is Linux; the target tag must win.
    monkeypatch.setattr("bundle_monitor.os.name", "posix")
    monkeypatch.setattr("bundle_monitor.wheel_platform_tag", lambda: "manylinux_2_17_x86_64")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    binary = str(tmp_path / "dist" / "vanth-monitor.exe")
    assert build_data["tag"] == "py3-none-win_amd64"
    assert os.path.isfile(binary)
    assert build_data["shared_scripts"][binary] == "vanth-monitor.exe"


def test_prebuilt_missing_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANTH_MONITOR_BIN", str(tmp_path / "does-not-exist"))
    hook = _make_hook(str(tmp_path))
    with pytest.raises(FileNotFoundError, match="VANTH_MONITOR_BIN"):
        hook.initialize("1.2.1", {})


def test_host_build_fallback(tmp_path, monkeypatch) -> None:
    """No env overrides -> go build runs and the host tag is used."""
    monkeypatch.delenv("VANTH_MONITOR_BIN", raising=False)
    monkeypatch.delenv("VANTH_MONITOR_GOOS", raising=False)
    monkeypatch.delenv("VANTH_MONITOR_GOARCH", raising=False)
    monkeypatch.delenv("VANTH_MONITOR_TAG", raising=False)

    calls = {}

    def fake_run(cmd, **kwargs):
        calls["cmd"] = cmd
        calls["cwd"] = kwargs.get("cwd")
        calls["env"] = kwargs.get("env")
        _fake_go_output(cmd)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("bundle_monitor.wheel_platform_tag", lambda: "win_amd64")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    assert calls["cmd"][0] == "go"
    assert calls["cmd"][1] == "build"
    exe = ".exe" if os.name == "nt" else ""
    binary = str(tmp_path / "dist" / f"vanth-monitor{exe}")
    assert calls["cmd"][calls["cmd"].index("-o") + 1] == binary
    assert build_data["tag"] == "py3-none-win_amd64"


def test_cross_build_uses_goos_goarch(tmp_path, monkeypatch) -> None:
    """GOOS/GOARCH env drives a cross build and its platform tag."""
    monkeypatch.delenv("VANTH_MONITOR_BIN", raising=False)
    monkeypatch.setenv("VANTH_MONITOR_GOOS", "linux")
    monkeypatch.setenv("VANTH_MONITOR_GOARCH", "arm64")
    monkeypatch.delenv("VANTH_MONITOR_TAG", raising=False)

    calls = {}

    def fake_run(cmd, **kwargs):
        calls["cmd"] = cmd
        calls["env"] = kwargs.get("env")
        _fake_go_output(cmd)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("bundle_monitor.wheel_platform_tag", lambda: "win_amd64")

    build_data: dict[str, dict] = {}
    hook = _make_hook(str(tmp_path))
    hook.initialize("1.2.1", build_data)

    assert calls["env"]["GOOS"] == "linux"
    assert calls["env"]["GOARCH"] == "arm64"
    assert build_data["tag"] == "py3-none-manylinux_2_17_aarch64"
    # Linux binary must not get an .exe suffix.
    binary = str(tmp_path / "dist" / "vanth-monitor")
    assert calls["cmd"][calls["cmd"].index("-o") + 1] == binary
