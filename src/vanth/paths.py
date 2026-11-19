from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path


def canonical_home(home: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the one state root shared by daemon, client, runner, and manager."""
    if home is not None:
        return Path(home).expanduser().resolve()
    vant_home = os.environ.get("VANTH_HOME")
    agent_home = os.environ.get("AGENT_BG_HOME")
    if vant_home and agent_home:
        vant_path = Path(vant_home).expanduser().resolve()
        agent_path = Path(agent_home).expanduser().resolve()
        if vant_path != agent_path:
            raise ValueError("VANTH_HOME and AGENT_BG_HOME refer to different state directories")
        return vant_path
    return Path(vant_home or agent_home or Path.home() / ".vanth").expanduser().resolve()


def self_python() -> tuple[str, dict[str, str]]:
    """Return ``(executable, env)`` for spawning this interpreter as one process.

    On Windows a venv's ``Scripts\\python.exe`` (plain venv, uv venv, uv tool
    install) is a launcher that re-executes the base interpreter and waits, so
    every ``sys.executable -m vanth.<module>`` costs two processes. Like
    ``multiprocessing`` does, start the base interpreter directly and set
    ``__PYVENV_LAUNCHER__`` so it adopts the venv (``sys.executable``,
    ``sys.prefix``, site-packages) exactly as the launcher would; CPython
    consumes and clears the variable at startup. Everywhere else (POSIX,
    non-venv, frozen builds) this is ``sys.executable`` and the current env.
    """
    env = os.environ.copy()
    base = getattr(sys, "_base_executable", None)
    if (
        os.name == "nt"
        and not getattr(sys, "frozen", False)
        and sys.prefix != sys.base_prefix
        and base
        and os.path.normcase(base) != os.path.normcase(sys.executable)
        and os.path.isfile(base)
    ):
        env["__PYVENV_LAUNCHER__"] = sys.executable
        return base, env
    return sys.executable, env


def secure_home_permissions(home: str | os.PathLike[str] | None = None) -> None:
    """Restrict the state directory so only its owner can read it.

    The home holds the bearer token and per-job env/spec data; if it inherits a
    broad ACL (e.g. a sandbox/other-user read grant on the parent profile
    directory), any process able to read the token gains full control of the
    daemon. This re-applies an owner-only permission set on every daemon start
    so a previously-loose home is tightened even if it was created elsewhere.

    - Unix: ``chmod 0700`` on the home, ``0600`` on the token file.
    - Windows: ``icacls`` disables ACL inheritance and grants only the owner,
      ``SYSTEM``, and ``Administrators`` on the home and the token file.
      ``os.chmod`` is a no-op for ACLs on Windows, so icacls is required.

    Best-effort: never raises for permission problems on unusual filesystems.
    """
    home = canonical_home(home)
    try:
        home.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    if os.name == "nt":
        _secure_windows(home)
    else:
        _secure_unix(home)


def _secure_unix(home: Path) -> None:
    try:
        os.chmod(home, stat.S_IRWXU)
    except OSError:
        pass
    try:
        os.chmod(home / "token", stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def _current_user_sid() -> str | None:
    """Return the SID of the current Windows user, or None on failure."""
    try:
        result = subprocess.run(
            ["whoami", "/user"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        for line in result.stdout.splitlines():
            if "S-1-" in line:
                return line.strip().split()[-1]
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def _secure_windows(home: Path) -> None:
    owner = _current_user_sid()
    if not owner:
        return
    # Grant only the owner, SYSTEM, and Administrators. Disabling inheritance
    # first removes broad ACEs inherited from the profile dir (e.g. sandbox
    # read grants); the owner is then re-granted explicitly.
    #
    # Container ACEs (OI)(CI) apply to the home directory and everything under
    # it; the token file gets explicit (F) ACEs of its own because it has no
    # inheritance after /inheritance:r.
    dir_grants = [
        f"*{owner}:(OI)(CI)F",
        "*S-1-5-18:(OI)(CI)F",  # NT AUTHORITY\SYSTEM
        "*S-1-5-32-544:(OI)(CI)F",  # BUILTIN\Administrators
    ]
    file_grants = [
        f"*{owner}:F",
        "*S-1-5-18:F",
        "*S-1-5-32-544:F",
    ]
    try:
        subprocess.run(
            ["icacls", str(home), "/inheritance:r", "/grant:r", *dir_grants],
            capture_output=True,
            timeout=15,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        pass
    token = home / "token"
    try:
        subprocess.run(
            ["icacls", str(token), "/inheritance:r", "/grant:r", *file_grants],
            capture_output=True,
            timeout=15,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        pass
