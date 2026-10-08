"""Autostart registration for the Vanth daemon.

Lets the daemon survive reboots by registering it as a background service
per platform:

- Windows: a Task Scheduler task (``VanthDaemon``) that runs at logon and
  startup, defined via a generated XML task file that sets ``VANTH_HOME``.
- macOS: a launchd LaunchAgent plist
  (``~/Library/LaunchAgents/com.vanth.daemon.plist``) with RunAtLoad,
  KeepAlive, and the ``VANTH_HOME`` environment variable.
- Linux: a systemd user unit
  (``~/.config/systemd/user/vanth-daemon.service``) wanted by
  ``default.target``.

Nothing here runs ``schtasks``/``launchctl``/``systemctl`` at import time.
Every side-effecting function accepts injectable ``_run``/``_write``/
``_remove`` callables so the registration logic is unit-testable and safe
to dry-run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

HOME_ENV = "VANTH_HOME"
TASK_NAME = "VanthDaemon"
LAUNCHAGENT_NAME = "com.vanth.daemon"
LINUX_UNIT_NAME = "vanth-daemon.service"

_Run = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]
# File writer seam; `encoding` selects the file encoding (the Windows task
# XML is UTF-16, its wrapper batch file UTF-8 with BOM).
_Write = Callable[..., None]
_Remove = Callable[[Path], None]
_Exists = Callable[[Path], bool]


def platform() -> str:
    """Return the current platform key: "windows", "macos", or "linux"."""
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    return "linux"


def _command_line(home: Path) -> list[str]:
    """The exact command that launches the daemon for this home.

    Prefers the ``vanthd`` console script on PATH; falls back to
    ``sys.executable -m vanth.daemon``. ``VANTH_HOME`` is always passed in
    the environment so the autostart uses this home regardless of the
    registering shell's env; ``VANTH_DAEMON_PORT``/``VANTH_DAEMON_HOST`` are
    deliberately left unset so the daemon picks them up from its own
    environment at start.
    """
    vanthd = shutil.which("vanthd")
    if vanthd:
        return [vanthd]
    return [sys.executable, "-m", "vanth.daemon"]


def _windows_xml(home: Path, wrapper: Path) -> str:
    """A Task Scheduler task definition that runs the daemon at logon+startup.

    The Task Scheduler schema has no environment-variable block, so the task
    runs a small wrapper script (``wrapper``) via ``cmd /d /c call``, and the
    wrapper sets ``VANTH_HOME`` before exec'ing the daemon. ``call`` (rather
    than a bare quoted path) keeps wrapper paths with spaces working under
    cmd.exe's ``/c`` quote-stripping rules.
    """
    comspec = os.environ.get("COMSPEC", r"C:\Windows\System32\cmd.exe")
    return f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Vanth background-job daemon</Description>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
    </BootTrigger>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{_escape_xml(comspec)}</Command>
      <Arguments>/d /c call {_escape_xml(_quote_cmd_arg(str(wrapper)))}</Arguments>
    </Exec>
  </Actions>
</Task>
"""


def _windows_cmd(home: Path) -> str:
    """The wrapper script a Windows scheduled task runs (sets VANTH_HOME).

    A batch file (not inline ``set ... &&``) so home paths with spaces,
    trailing backslashes, or ``&`` need no fragile inline quoting: the quoted
    ``set "NAME=value"`` form takes everything literally. ``%`` is doubled
    because cmd.exe expands it even inside quotes.
    """
    exe, *args = _command_line(home)
    lines = [
        "@echo off",
        f'set "VANTH_HOME={_escape_cmd_value(str(home))}"',
        " ".join([_quote_cmd_arg(exe), *(_quote_cmd_arg(a) for a in args)]),
    ]
    return "\r\n".join(lines) + "\r\n"


def _escape_cmd_value(text: str) -> str:
    """Escape a value embedded in a batch file (percent expansion is active)."""
    if '"' in text or "\n" in text or "\r" in text:
        raise ValueError(f"cannot encode in a batch file: {text!r}")
    return text.replace("%", "%%")


def _quote_cmd_arg(text: str) -> str:
    """Quote one argv token for cmd.exe (sibling of cli._quote_for_cmd)."""
    if '"' in text or "\n" in text or "\r" in text:
        raise ValueError(f"cannot encode in a batch file: {text!r}")
    if text and all(c.isalnum() or c in r"/._-+:\=" for c in text):
        return text
    return '"' + text.replace("%", "%%") + '"'


def _escape_xml(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _macos_plist(home: Path) -> str:
    """A launchd LaunchAgent plist that keeps the daemon alive."""
    exe, *args = _command_line(home)
    argv = [exe, *args]
    lines = "".join(f"      <string>{_escape_plist(a)}</string>\n" for a in argv)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHAGENT_NAME}</string>
  <key>ProgramArguments</key>
  <array>
{lines}  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>{HOME_ENV}</key>
    <string>{_escape_plist(str(home))}</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{_escape_plist(str(home / "daemon.log"))}</string>
  <key>StandardErrorPath</key>
  <string>{_escape_plist(str(home / "daemon.log"))}</string>
</dict>
</plist>
"""


def _escape_plist(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _linux_unit(home: Path) -> str:
    """A systemd user unit that keeps the daemon alive after login."""
    exe, *args = _command_line(home)
    exec_line = " ".join(_shell_quote(a) for a in [exe, *args])
    return f"""[Unit]
Description=Vanth background-job daemon
After=default.target

[Service]
Type=simple
ExecStart={exec_line}
Restart=on-failure
RestartSec=5
Environment={HOME_ENV}={_shell_quote(str(home))}

[Install]
WantedBy=default.target
"""


def _shell_quote(text: str) -> str:
    if text and all(c.isalnum() or c in "/._-+" for c in text):
        return text
    return "'" + text.replace("'", "'\\''") + "'"


def _write_file(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.write_text(content, encoding=encoding)


def _remove_file(path: Path) -> None:
    if path.exists():
        path.unlink()


def _path_exists(path: Path) -> bool:
    return path.exists()


def _run_cmd(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command), capture_output=True, text=True, shell=False
    )


def _targets(home: Path) -> dict[str, Any]:
    platform_key = platform()
    if platform_key == "windows":
        return {
            "kind": "task",
            "target": TASK_NAME,
            "file": home / "vanthd-task.xml",
            "wrapper": home / "vanthd-task.cmd",
            "enable_cmd": ["schtasks", "/Create", "/XML", str(home / "vanthd-task.xml"), "/TN", TASK_NAME, "/F"],
            "disable_cmd": ["schtasks", "/Delete", "/F", "/TN", TASK_NAME],
            "query_cmd": ["schtasks", "/Query", "/TN", TASK_NAME],
        }
    if platform_key == "macos":
        target = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHAGENT_NAME}.plist"
        return {
            "kind": "launchagent",
            "target": str(target),
            "file": target,
            "enable_cmd": ["launchctl", "load", str(target)],
            "disable_cmd": ["launchctl", "unload", str(target)],
            "query_cmd": [],
        }
    target = Path.home() / ".config" / "systemd" / "user" / LINUX_UNIT_NAME
    return {
        "kind": "systemd-user",
        "target": str(target),
        "file": home / LINUX_UNIT_NAME,
        "enable_cmd": ["systemctl", "--user", "enable", "--now", str(home / LINUX_UNIT_NAME)],
        "disable_cmd": ["systemctl", "--user", "disable", "--now", LINUX_UNIT_NAME],
        "query_cmd": ["systemctl", "--user", "is-enabled", LINUX_UNIT_NAME],
    }


def _render(home: Path, platform_key: str) -> str:
    if platform_key == "windows":
        targets = _targets(home)
        return _windows_xml(home, Path(targets["wrapper"]))
    if platform_key == "macos":
        return _macos_plist(home)
    return _linux_unit(home)


def _ensure_home(home: Path) -> Path:
    home = Path(home)
    if not home.exists():
        home.mkdir(parents=True, exist_ok=True)
    if not home.is_dir() or not os.access(home, os.W_OK):
        raise OSError(f"vanth home is not writable: {home}")
    return home


def plan(home: Path) -> dict[str, Any]:
    """Describe what autostart would do, without changing anything."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    return {
        "platform": platform_key,
        "target": targets["target"],
        "command": _command_line(home),
        "home": str(home),
        "enabled": detect(home).get("enabled", False),
    }


def detect(
    home: Path,
    *,
    _run: _Run = _run_cmd,
    _exists: _Exists = _path_exists,
) -> dict[str, Any]:
    """Detect the current registration state without modifying anything."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    result: dict[str, Any] = {"platform": platform_key, "target": targets["target"], "enabled": False}
    try:
        if platform_key == "windows":
            proc = _run(targets["query_cmd"])
            result["enabled"] = proc.returncode == 0
            if proc.returncode != 0:
                stderr = proc.stderr.strip()
                if not stderr or "cannot find the file specified" in stderr.lower() or "does not exist" in stderr.lower():
                    result["error"] = "task not registered"
                else:
                    result["error"] = stderr
        elif platform_key == "macos":
            result["enabled"] = _exists(Path(targets["target"]))
        else:
            result["enabled"] = _exists(Path(targets["target"]))
            if not result["enabled"]:
                result["error"] = "unit file not found"
            else:
                try:
                    proc = _run(targets["query_cmd"])
                    result["enabled"] = proc.returncode == 0
                    if proc.returncode != 0:
                        result["error"] = proc.stderr.strip() or "unit not enabled"
                except Exception as exc:  # noqa: BLE001 - tool missing or broken
                    result["error"] = f"systemctl unavailable: {exc}"
    except Exception as exc:  # noqa: BLE001 - tool missing or broken
        result["error"] = str(exc)
    return result


def enable(
    home: Path,
    *,
    dry_run: bool = False,
    _run: _Run = _run_cmd,
    _write: _Write = _write_file,
) -> dict[str, Any]:
    """Register the daemon to start automatically, or describe doing so."""
    home = _ensure_home(home)
    platform_key = platform()
    targets = _targets(home)
    content = _render(home, platform_key)
    if dry_run:
        files = [targets["file"]] + ([targets["wrapper"]] if "wrapper" in targets else [])
        return {
            "dry_run": True,
            "would_install": f"write {[str(p) for p in files]!r} and run {' '.join(targets['enable_cmd'])}",
            "target": targets["target"],
            "platform": platform_key,
        }
    if platform_key == "windows":
        # The task XML declares UTF-16 (Task Scheduler's canonical encoding);
        # the wrapper batch file is UTF-8 with BOM, which cmd.exe honors.
        _write(Path(targets["file"]), content, encoding="utf-16")
        _write(Path(targets["wrapper"]), _windows_cmd(home), encoding="utf-8-sig")
    else:
        _write(Path(targets["file"]), content)
    proc = _run(targets["enable_cmd"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"autostart enable failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return {"enabled": True, "target": targets["target"], "platform": platform_key}


def disable(
    home: Path,
    *,
    dry_run: bool = False,
    _run: _Run = _run_cmd,
    _remove: _Remove = _remove_file,
) -> dict[str, Any]:
    """Remove the autostart registration, or describe doing so."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    if dry_run:
        return {
            "dry_run": True,
            "would_uninstall": f"run {' '.join(targets['disable_cmd'])} and remove {targets['target']!r}",
            "target": targets["target"],
            "platform": platform_key,
        }
    proc = _run(targets["disable_cmd"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"autostart disable failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    # Remove what enable() wrote. On Windows the registration "target" is a
    # task NAME, not a path — removing it would unlink a relative
    # `./VanthDaemon` file in the caller's CWD (or silently do nothing) while
    # leaving the real XML (`targets["file"]`) behind. Only unlink the target
    # when it is an absolute path (the macOS plist / Linux unit symlink).
    # The Windows wrapper batch file is removed alongside its task XML.
    _remove(Path(targets["file"]))
    if "wrapper" in targets:
        _remove(Path(targets["wrapper"]))
    target_path = Path(targets["target"])
    if target_path.is_absolute() and target_path != Path(targets["file"]):
        _remove(target_path)
    return {"enabled": False, "target": targets["target"], "platform": platform_key}
