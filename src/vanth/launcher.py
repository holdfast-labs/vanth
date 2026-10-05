"""Frozen-binary entry point for Vanth.

A wheel installs several console scripts (``vanth``, ``vanthd``,
``vanth-monitor``, ...) and Vanth spawns its own long-lived helpers as
``sys.executable -m vanth.<module>`` (the runner, the daemon, the Codex pipe
helper). A PyInstaller binary has one executable and no Python ``-m``, so this
launcher restores both behaviours:

* ``-m vanth.<module>`` is dispatched to that module's ``main()`` in-process;
* the program name (``vanthd``, ``vanth-monitor``, ...) selects the role, so a
  copied/symlinked binary still acts like the console script it is named after;
* anything else is the normal ``vanth`` CLI / MCP server.

Everything is import-time-light: the role's module is imported only when chosen.
"""

from __future__ import annotations

import importlib
import os
import sys

# Modules Vanth re-invokes itself as. Keep in sync with the spawn sites
# (client.py, autostart.py, server.py, codex_pipe.py) and pyproject [project.scripts].
_MODULE_ENTRYPOINTS = {
    "vanth.server",
    "vanth.daemon",
    "vanth.runner",
    "vanth.codex_pipe",
    "vanth.codex_bridge",
    "vanth.monitor",
    "vanth.remote.helper",
}

# Console scripts that share one frozen binary, keyed by program basename.
_PROG_ROLES = {
    "vanthd": "vanth.daemon",
    "vanthd.exe": "vanth.daemon",
    "vanth-monitor": "vanth.monitor",
    "vanth-monitor.exe": "vanth.monitor",
    "vanth-codex-notify": "vanth.codex_bridge",
    "vanth-codex-notify.exe": "vanth.codex_bridge",
    "vanth-remote-helper": "vanth.remote.helper",
    "vanth-remote-helper.exe": "vanth.remote.helper",
}


def resolve_target(prog: str, argv: list[str]) -> tuple[str | None, list[str]]:
    """Return ``(module_name, remaining_argv)`` for an invocation.

    ``module_name`` is ``None`` for the default CLI/MCP server role.
    """
    if len(argv) >= 2 and argv[0] == "-m" and argv[1] in _MODULE_ENTRYPOINTS:
        return argv[1], argv[2:]
    return _PROG_ROLES.get(os.path.basename(prog).lower()), argv


def clear_frozen_env() -> None:
    """Make each self-spawned frozen process extract its own copy.

    A one-file build extracts to ``_MEI<...>`` and asks the bootloader to reuse
    that directory for its own child via ``_MEIPASS2``. Vanth spawns fresh
    processes as ``sys.executable -m vanth.<module>`` (daemon, runner, ...); if
    they inherit the parent's extraction dir, the parent deletes it on exit and
    the child's package data vanishes mid-run (the observed "runner orphaned"
    crash: ``jsonschema_specifications/schemas`` disappeared). Setting
    ``PYINSTALLER_RESET_ENVIRONMENT`` makes each spawned instance reset and
    extract a clean copy; the bootloader consumes both it and ``_MEIPASS2``.
    """
    if getattr(sys, "frozen", False):
        os.environ.pop("_MEIPASS2", None)
        os.environ["PYINSTALLER_RESET_ENVIRONMENT"] = "1"


def main(argv: list[str] | None = None) -> int:
    clear_frozen_env()
    argv = list(sys.argv[1:] if argv is None else argv)
    # `sys.executable -c <code>` is how Vanth launches trivial inline workers
    # (e.g. `vanth sleep`); a frozen binary has no `-c`, so run the code here.
    if len(argv) >= 2 and argv[0] == "-c":
        sys.argv = ["-c", *argv[2:]]
        exec(compile(argv[1], "<string>", "exec"), {"__name__": "__main__"})
        return 0
    module_name, argv = resolve_target(sys.argv[0], argv)
    if module_name is None:
        from .server import main as server_main

        return server_main(argv)
    sys.argv = [module_name, *argv]
    result = importlib.import_module(module_name).main()
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
