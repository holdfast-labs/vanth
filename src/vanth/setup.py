"""One-shot MCP client registration for Vanth.

`vanth setup` connects the MCP server to the agent clients installed on this
machine. It detects known clients, shows what it found, lets the user pick
which to configure (interactively, or via flags for scripting), backs up any
existing config it changes, and upserts the Vanth MCP entry without clobbering
the rest of the file. For a selected client with no config, it safely creates
the standard config file.

It writes the exact format each client expects:

- opencode: ``~/.config/opencode/opencode.json`` -> ``mcp.vanth``
- codex:    ``~/.codex/config.toml`` -> ``[mcp_servers.vanth]``
- generic ``mcpServers`` JSON clients (Claude Code, Cursor, ...) ->
  ``mcpServers.vanth``

Only the user's own config files are modified; nothing is installed or run.
A timestamped ``.vanth-setup-<ts>.bak`` backup is written before changing an
existing file; newly created files have no prior contents to back up.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from .paths import canonical_home

SETUP_KEY = "vanth"
BACKUP_SUFFIX = ".vanth-setup"

# client id -> (display name, path resolver)
_CLIENTS: dict[str, tuple[str, str]] = {
    "opencode": ("opencode", "~/.config/opencode/opencode.json"),
    "codex": ("codex", "~/.codex/config.toml"),
    "claude": ("Claude Code / Cursor (mcpServers)", "~/.claude.json"),
}


def client_config_paths(home: Path | None = None) -> dict[str, list[Path]]:
    """Return the config file paths for each known client that exists."""
    home = home or canonical_home()
    found: dict[str, list[Path]] = {}
    # opencode loads and MERGES several files, later ones winning on conflicts:
    # config.json -> opencode.json -> opencode.jsonc. List them so setup/status
    # can see the whole effective picture instead of reporting "not configured"
    # for a client whose entry lives in a file we did not look at.
    candidates: dict[str, list[str]] = {
        "opencode": [
            "~/.config/opencode/config.json",
            "~/.config/opencode/opencode.json",
            "~/.config/opencode/opencode.jsonc",
            "~/.opencode.json",
        ],
        "codex": ["~/.codex/config.toml"],
        "claude": ["~/.claude.json"],
    }
    for client, paths in candidates.items():
        for raw in paths:
            path = Path(raw).expanduser()
            if path.is_file():
                found.setdefault(client, []).append(path)
    return found


def _default_config_path(client: str) -> Path:
    """Return the usual config path for a selected client."""
    return Path(_CLIENTS[client][1]).expanduser()


OPENCODE_PLUGIN_FILENAME = "vanth.ts"


def plugin_source() -> Path:
    """The wake plugin shipped with this package."""
    return Path(__file__).resolve().parent / "opencode_plugin" / OPENCODE_PLUGIN_FILENAME


def plugin_target() -> Path:
    """Where OpenCode loads plugins from (``VANTH_OPENCODE_PLUGIN_DIR`` overrides)."""
    directory = os.environ.get("VANTH_OPENCODE_PLUGIN_DIR") or "~/.config/opencode/plugins"
    return Path(directory).expanduser() / OPENCODE_PLUGIN_FILENAME


def install_opencode_plugin() -> tuple[bool, str]:
    """Install the OpenCode wake relay plugin; idempotent.

    A plain TUI session exposes no ``attach`` URL and injects no session id, so
    without this in-process plugin an ``opencode_thread`` wake can never reach
    the session the user is watching.
    """
    source = plugin_source()
    if not source.is_file():
        raise FileNotFoundError(f"plugin source not found: {source}")
    text = source.read_text(encoding="utf-8")
    target = plugin_target()
    if target.is_file() and target.read_text(encoding="utf-8") == text:
        return False, "already installed"
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_atomic(target, text)
    return True, f"installed {target}"


def remove_opencode_plugin() -> tuple[bool, str]:
    target = plugin_target()
    if not target.is_file():
        return False, "not configured"
    target.unlink()
    return True, f"removed {target}"


def _backup(path: Path) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%S")
    backup = path.with_name(path.name + f"{BACKUP_SUFFIX}-{stamp}.bak")
    shutil.copy2(path, backup)
    return backup


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _write_new(path: Path, text: str) -> None:
    """Create a config only if it is still absent; never replace a racing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False
    ) as stream:
        tmp = Path(stream.name)
        stream.write(text)
    try:
        # A hard link publishes the complete file atomically and fails if a
        # client created its config after setup inspected the path.
        os.link(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _merge_json(path: Path, merge_fn) -> tuple[bool, str]:
    """Merge into a JSON file, preserving everything except the key set by
    ``merge_fn``. Returns (changed, summary)."""
    existed = path.exists()
    original = path.read_text(encoding="utf-8") if existed else "{}"
    try:
        data = json.loads(original)
    except json.JSONDecodeError:
        return False, "invalid JSON"
    before = json.dumps(data, sort_keys=True)
    data = merge_fn(data)
    after = json.dumps(data, sort_keys=True)
    if after == before:
        return False, "already configured"
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    if existed:
        _backup(path)
        _write_atomic(path, text)
    else:
        _write_new(path, text)
    return True, "updated"


def mcp_command() -> str:
    """The bare command MCP clients should launch (never a path).

    ``vanth-mcp`` is the native launcher shim installed next to the Python
    console scripts: it starts the env's interpreter directly, one process
    fewer than the ``vanth`` console script. Used only when it is installed in
    this env and is what PATH resolves (directly, or as the copy `uv tool
    install` puts in its bin dir on Windows), so the entry runs this install.
    """
    import sysconfig

    name = "vanth-mcp.exe" if os.name == "nt" else "vanth-mcp"
    shim = Path(sysconfig.get_path("scripts")) / name
    found = shutil.which("vanth-mcp")
    if not shim.is_file() or not found:
        return "vanth"
    if os.path.samefile(found, shim) or _uv_tool_installed(Path(found)):
        return "vanth-mcp"
    return "vanth"


def _uv_tool_installed(path: Path) -> bool:
    """Whether this env's uv tool receipt lists ``path`` as its vanth-mcp."""
    import tomllib

    try:
        receipt = tomllib.loads((Path(sys.prefix) / "uv-receipt.toml").read_text(encoding="utf-8"))
        entries = receipt["tool"]["entrypoints"]
        return any(
            entry.get("name") == "vanth-mcp" and os.path.samefile(entry.get("install-path", ""), path)
            for entry in entries
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _mcp_vanth_entry(home: Path) -> dict[str, Any]:
    """The MCP entry used for mcpServers-style clients."""
    return {
        "command": mcp_command(),
        "env": {"VANTH_HOME": str(home)},
    }


def register_opencode(path: Path, home: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        mcp = data.setdefault("mcp", {})
        mcp[SETUP_KEY] = {
            "type": "local",
            "command": [mcp_command()],
            "enabled": True,
            # 60 s matches the reference MCP client budget. Blocking tools
            # (job_wait, job_start_and_wait, job_stop, job_tail --follow) return
            # before the wait slice (VANTH_MCP_WAIT_SLICE, 25 s) plus a ~5 s
            # socket margin, and a daemon cold start can take longer than 15 s.
            "timeout": 60000,
            # Review P0-3: without VANTH_HOME, a custom-state installation could
            # reach a different daemon (the default home). Always pin the
            # configured home so MCP talks to THIS daemon.
            "environment": {"VANTH_HOME": str(home)},
        }
        return data

    return _merge_json(path, merge)


def register_mcp_servers(path: Path, home: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        servers = data.setdefault("mcpServers", {})
        servers[SETUP_KEY] = _mcp_vanth_entry(home)
        return data

    return _merge_json(path, merge)


def register_codex(path: Path, home: Path) -> tuple[bool, str]:
    """Register under ``[mcp_servers.vanth]`` in a Codex config.toml.

    Uses tomllib to inspect, and a line-oriented rewrite to add/replace only
    the ``[mcp_servers.vanth]`` section without touching any other content,
    comments, or ordering.
    """
    try:
        import tomllib
    except ImportError:  # pragma: no cover - 3.11+ always has it
        return False, "tomllib unavailable"
    existed = path.exists()
    original = path.read_text(encoding="utf-8") if existed else ""
    try:
        data = tomllib.loads(original)
    except tomllib.TOMLDecodeError:
        return False, "invalid TOML"

    section = f"[mcp_servers.{SETUP_KEY}]"
    escaped_home = str(home).replace("\\", "\\\\")
    new_lines = [
        section,
        f'command = "{mcp_command()}"',
        "",
        f"[mcp_servers.{SETUP_KEY}.env]",
        f'VANTH_HOME = "{escaped_home}"',
        "",
    ]

    lines = original.splitlines()
    out: list[str] = []
    replaced = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.rstrip() == section:
            # Skip the existing section and any sub-sections of it
            # (e.g. [mcp_servers.vanth.env]) up to the next unrelated header.
            j = i + 1
            while j < len(lines):
                candidate = lines[j].lstrip()
                if candidate.startswith("["):
                    header = candidate[1:].rstrip().split("]")[0].strip()
                    if header.startswith(f"mcp_servers.{SETUP_KEY}."):
                        j += 1
                        continue
                    break
                j += 1
            out.extend(new_lines)
            replaced = True
            i = j
            continue
        out.append(line)
        i += 1

    if not replaced:
        # Append the section at the end (with a blank line separator if the
        # file doesn't already end with one).
        if out and out[-1] != "":
            out.append("")
        out.extend(new_lines)

    new_text = "\n".join(out).rstrip() + "\n"
    if new_text == original.rstrip("\n") + "\n":
        return False, "already configured"
    if existed:
        _backup(path)
        _write_atomic(path, new_text)
    else:
        _write_new(path, new_text)
    return True, "updated"


def remove_opencode(path: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        mcp = data.get("mcp")
        if mcp and SETUP_KEY in mcp:
            del mcp[SETUP_KEY]
        return data

    return _merge_json(path, merge)


def remove_mcp_servers(path: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        servers = data.get("mcpServers")
        if servers and SETUP_KEY in servers:
            del servers[SETUP_KEY]
        return data

    return _merge_json(path, merge)


def register_codex_desktop(home: Path) -> tuple[bool, str]:
    """Provision Codex Desktop wake (review rc37 P0).

    The real Codex Desktop MCP children do NOT inherit
    ``CODEX_APP_TOOLS_PIPE_PATH``/``CODEX_THREAD_ID`` ambiently — the pipe is
    granted only to the bundled ``codex_app`` MCP integration. This writes a
    per-home ``codex_desktop.json`` capability file (pipe path + caller thread
    identity) that the relay reads, so Desktop wake can be enabled through a
    supported handoff: run `vanth setup desktop` while a Desktop session with
    the app-tools capability is active in this environment.

    The capability file is written with the same restrictive permissions as the
    auth token (the pipe path is sensitive). It fails explicitly (no silent
    no-op) when neither a pipe nor a caller thread identity is available.
    """
    import stat as _stat

    pipe_path = os.environ.get("VANTH_CODEX_DESKTOP_PIPE") or os.environ.get("CODEX_APP_TOOLS_PIPE_PATH")
    thread_id = os.environ.get("VANTH_CODEX_DESKTOP_THREAD") or os.environ.get("CODEX_THREAD_ID")
    if not pipe_path or not thread_id:
        missing = []
        if not pipe_path:
            missing.append("pipe (CODEX_APP_TOOLS_PIPE_PATH / VANTH_CODEX_DESKTOP_PIPE)")
        if not thread_id:
            missing.append("thread id (CODEX_THREAD_ID / VANTH_CODEX_DESKTOP_THREAD)")
        return False, f"Desktop capability unavailable (missing {', '.join(missing)}); run this inside a Codex Desktop session with app-tools active"
    path = home / "codex_desktop.json"
    from datetime import datetime, timezone

    payload = {
        "pipe_path": pipe_path,
        "thread_id": thread_id,
        "caller_thread_id": thread_id,
        "provisioned_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    try:
        os.chmod(tmp, _stat.S_IRUSR | _stat.S_IWUSR)
    except OSError:
        pass
    os.replace(tmp, path)
    return True, f"Desktop wake provisioned (thread={thread_id})"


def remove_codex_desktop(home: Path) -> tuple[bool, str]:
    path = home / "codex_desktop.json"
    if not path.exists():
        return False, "not configured"
    try:
        path.unlink()
    except OSError as exc:
        return False, f"failed to remove: {exc}"
    return True, "removed"


def remove_codex(path: Path) -> tuple[bool, str]:
    section = f"[mcp_servers.{SETUP_KEY}]"
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    i = 0
    removed = False
    while i < len(lines):
        line = lines[i]
        if line.rstrip() == section:
            j = i + 1
            while j < len(lines):
                candidate = lines[j].lstrip()
                if candidate.startswith("["):
                    header = candidate[1:].rstrip().split("]")[0].strip()
                    if header.startswith(f"mcp_servers.{SETUP_KEY}."):
                        j += 1
                        continue
                    break
                j += 1
            i = j
            removed = True
            continue
        out.append(line)
        i += 1
    if not removed:
        return False, "not configured"
    _backup(path)
    _write_atomic(path, "\n".join(out).rstrip() + "\n")
    return True, "removed"


def detect_status(home: Path | None = None) -> dict[str, list[dict[str, Any]]]:
    """Return per-client status: present config files and whether vanth is
    already registered in each."""
    home = home or canonical_home()
    found = client_config_paths(home)
    result: dict[str, list[dict[str, Any]]] = {}
    for client, paths in found.items():
        entries = []
        for path in paths:
            state = config_state(client, path)
            entries.append({"path": str(path), "state": state, "configured": state == "configured"})
        result[client] = entries
    return result


#: Config key holding the vanth entry, per client.
_CLIENT_KEYS = {"opencode": "mcp", "claude": "mcpServers", "codex": "mcp_servers"}


def _strip_jsonc(text: str) -> str:
    """Remove ``//`` and ``/* */`` comments from JSONC text.

    A scanner, not a regex, so a ``//`` inside a string literal is preserved.
    READ-ONLY: this lets us detect keys in a commented config without ever
    rewriting the user's file.
    """
    out: list[str] = []
    index = 0
    length = len(text)
    in_string = False
    while index < length:
        char = text[index]
        if in_string:
            out.append(char)
            if char == "\\" and index + 1 < length:
                out.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
        elif char == "/" and index + 1 < length and text[index + 1] == "/":
            index += 2
            while index < length and text[index] not in "\r\n":
                index += 1
            continue
        elif char == "/" and index + 1 < length and text[index + 1] == "*":
            index += 2
            while index + 1 < length and not (text[index] == "*" and text[index + 1] == "/"):
                index += 1
            index += 2
            continue
        out.append(char)
        index += 1
    return "".join(out)


def _remove_trailing_commas(text: str) -> str:
    """Drop commas immediately before a closing brace/bracket (string-aware)."""
    out: list[str] = []
    index = 0
    length = len(text)
    in_string = False
    while index < length:
        char = text[index]
        if in_string:
            out.append(char)
            if char == "\\" and index + 1 < length:
                out.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            out.append(char)
            index += 1
            continue
        if char == ",":
            look = index + 1
            while look < length and text[look] in " \t\r\n":
                look += 1
            if look < length and text[look] in "}]":
                index += 1
                continue
        out.append(char)
        index += 1
    return "".join(out)


def _jsonc_is_plain_json(path: Path) -> bool:
    """True when a ``.jsonc`` file carries no comments or trailing commas, so a
    normal JSON round-trip loses nothing and it can be edited safely."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return _strip_jsonc(text) == text and _remove_trailing_commas(text) == text


def _load_client_config(client: str, path: Path) -> dict[str, Any] | None:
    """Parse a client config (JSON, JSONC, or TOML). ``None`` means unreadable."""
    try:
        text = path.read_text(encoding="utf-8")
        if client == "codex":
            import tomllib

            return tomllib.loads(text)
        if path.suffix.lower() == ".jsonc":
            text = _remove_trailing_commas(_strip_jsonc(text))
        return json.loads(text)
    except (OSError, ValueError):
        return None


def config_state(client: str, path: Path) -> str:
    """Classify a client config file: ``configured`` / ``disabled`` /
    ``not-configured`` / ``unreadable`` / ``missing``.

    ``unreadable`` is deliberately distinct from ``not-configured``: a file we
    could not parse is unknown, and reporting it as unconfigured would be wrong
    (and is exactly the case where we refuse to write a shadowed entry).
    """
    if not path.is_file():
        return "missing"
    data = _load_client_config(client, path)
    if not isinstance(data, dict):
        return "unreadable"
    entry = (data.get(_CLIENT_KEYS[client]) or {}).get(SETUP_KEY)
    if not entry:
        return "not-configured"
    # An entry that is explicitly disabled is present but not effective.
    if isinstance(entry, dict) and entry.get("enabled") is False:
        return "disabled"
    return "configured"


def _is_configured(client: str, path: Path) -> bool:
    return config_state(client, path) == "configured"


#: opencode merges these global files in order; later entries override earlier
#: ones for conflicting keys (config.json -> opencode.json -> opencode.jsonc).
_OPENCODE_PRECEDENCE = ["config.json", "opencode.json", "opencode.jsonc"]

def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _shadowing_opencode_file(paths: list[Path], target: Path) -> Path | None:
    """Return a higher-precedence opencode file that defines (or may define)
    ``mcp.vanth``, i.e. one that would shadow an entry written to ``target``."""
    rank = {name: index for index, name in enumerate(_OPENCODE_PRECEDENCE)}
    target_rank = rank.get(target.name, -1)
    for path in paths:
        if path == target or rank.get(path.name, -1) <= target_rank:
            continue
        if config_state("opencode", path) in {"configured", "disabled", "unreadable"}:
            return path
    return None


def effective_state(client: str, paths: list[Path]) -> str:
    """The state that actually applies to a client.

    opencode DEEP-MERGES its config files, so the outcome is the merge of the
    ``mcp.vanth`` entries in precedence order — not "the first file that has
    one". A lower file's ``enabled: false`` survives a higher file that only
    adds a ``command``, and reporting "configured" there would claim a server is
    connected when it is not.
    """
    if client != "opencode":
        return config_state(client, paths[0]) if paths else "missing"
    if not paths:
        return "missing"
    rank = {name: index for index, name in enumerate(_OPENCODE_PRECEDENCE)}
    merged: dict[str, Any] = {}
    present = False
    for path in sorted(paths, key=lambda item: rank.get(item.name, -1)):
        data = _load_client_config(client, path)
        if data is None:
            # An unparseable file could override anything; report uncertainty.
            return "unreadable"
        entry = (data.get("mcp") or {}).get(SETUP_KEY)
        if isinstance(entry, dict):
            merged = _deep_merge(merged, entry)
            present = True
        elif entry:
            present = True
    if not present:
        return "not-configured"
    return "disabled" if merged.get("enabled") is False else "configured"


def _select_write_target(client: str, paths: list[Path]) -> tuple[Path | None, str]:
    """Pick the ONE config file `vanth setup` should edit.

    For opencode, prefer a plain-JSON file we can safely round-trip
    (``opencode.json``, then ``config.json``). A JSONC-only install is reported
    instead of edited: comment/trailing-comma preservation is not implemented,
    so rewriting it would destroy the user's comments. A plain-JSON target is
    also refused when a HIGHER-precedence file may define ``mcp.vanth`` — writing
    the lower file there would be shadowed and the registration would silently
    not take effect.
    """
    if client != "opencode":
        return (paths[0] if paths else None), ""
    by_name = {path.name: path for path in paths}
    target = None
    for name in ("opencode.json", "config.json"):
        if name in by_name:
            target = by_name[name]
            break
    if target is None:
        jsonc = [path for path in paths if path.suffix.lower() == ".jsonc"]
        plain = [path for path in jsonc if _jsonc_is_plain_json(path)]
        if plain:
            # No comments to lose, so it round-trips like a normal JSON config.
            return plain[0], ""
        if jsonc:
            return None, (
                f"only a commented JSONC config is present ({jsonc[0]}); add `mcp.{SETUP_KEY}` "
                "manually — vanth will not rewrite a commented file"
            )
        return (paths[0] if paths else None), ""

    shadow = _shadowing_opencode_file(paths, target)
    if shadow is not None:
        return None, (
            f"{shadow.name} overrides {target.name} and defines (or cannot be ruled out as "
            f"defining) `mcp.{SETUP_KEY}`; refusing to write a shadowed duplicate — edit "
            f"{shadow.name} directly instead"
        )
    return target, ""


_REGISTRARS = {
    "opencode": register_opencode,
    "codex": register_codex,
    "claude": register_mcp_servers,
}
_REMOVERS = {
    "opencode": remove_opencode,
    "codex": remove_codex,
    "claude": remove_mcp_servers,
}


def run_setup(
    clients: list[str] | None = None,
    *,
    home: Path | None = None,
    remove: bool = False,
    assume_yes: bool = False,
    json_out: bool = False,
) -> int:
    """Configure (or remove) the Vanth MCP entry for the given clients.

    With no clients, detect and configure everything found. Returns 0 on
    success, 1 if any registration failed, 2 on usage errors.
    """
    home = home or canonical_home()
    found = client_config_paths(home)

    requested = list(clients or dict.fromkeys([
        *found,
        *(client for client in _CLIENTS if shutil.which(client)),
    ]))
    if clients:
        unknown = set(clients) - {"opencode", "codex", "claude"}
        if unknown:
            if json_out:
                print(json.dumps({"ok": False, "error": f"unknown client(s): {', '.join(sorted(unknown))}"}))
            else:
                print(f"vanth setup: unknown client(s): {', '.join(sorted(unknown))}", file=sys.stderr)
                print("  known clients: opencode, codex, claude", file=sys.stderr)
            return 2

    verb = "remove" if remove else "register"
    targets: list[tuple[str, Path]] = []
    skipped: list[tuple[str, str]] = []
    for client in requested:
        paths = found.get(client, [])
        if not paths:
            # A requested client with no config gets its standard config path.
            # Bare setup requests only detected configs and installed clients.
            if not remove:
                targets.append((client, _default_config_path(client)))
            continue
        if remove:
            # Removal must clean EVERY safe registration: selecting a single
            # file would leave an entry behind in another one. Only a file we
            # cannot parse is skipped (with a warning) because we cannot tell
            # whether it holds a registration.
            for path in paths:
                if (
                    client == "opencode"
                    and path.suffix.lower() == ".jsonc"
                    and not _jsonc_is_plain_json(path)
                ):
                    skipped.append(
                        (client, f"{path} has comments; remove `mcp.{SETUP_KEY}` manually if present")
                    )
                    continue
                targets.append((client, path))
            continue
        path, note = _select_write_target(client, paths)
        if path is None:
            if note:
                skipped.append((client, note))
            continue
        targets.append((client, path))

    if skipped and not json_out:
        for client, note in skipped:
            print(f"vanth setup: {client}: {note}", file=sys.stderr)

    if not targets:
        if remove and not skipped:
            # Nothing is registered in any discovered file: removal is complete.
            if json_out:
                print(json.dumps({"ok": True, "verb": verb, "results": [], "skipped": []}, indent=2))
            else:
                print("vanth setup: nothing to remove")
            return 0
        if json_out:
            print(json.dumps({
                "ok": False,
                "error": "no writable client config found",
                "skipped": [{"client": client, "reason": note} for client, note in skipped],
            }))
        else:
            print("vanth setup: no writable client config found.", file=sys.stderr)
            print("  searched: ~/.config/opencode/{config.json,opencode.json,opencode.jsonc},", file=sys.stderr)
            print("            ~/.codex/config.toml, ~/.claude.json", file=sys.stderr)
            print("  pass an explicit client, e.g. `vanth setup opencode`", file=sys.stderr)
        return 1

    if not json_out:
        print(f"vanth setup: {verb} MCP server in {len(targets)} config file(s):")
        for client, path in targets:
            print(f"  - {client}: {path} ({config_state(client, path)})")

    if not assume_yes:
        if not sys.stdin.isatty():
            print("vanth setup: no interactive terminal; pass --yes to apply", file=sys.stderr)
            return 1
        answer = input(f"Continue with {verb}? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("aborted")
            return 1

    failures = 0
    results: list[dict[str, Any]] = []
    for client, path in targets:
        fn = _REMOVERS[client] if remove else _REGISTRARS[client]
        try:
            changed, summary = fn(path, home) if not remove else fn(path)
        except Exception as exc:
            results.append({"client": client, "path": str(path), "changed": False, "ok": False, "error": str(exc)})
            if not json_out:
                print(f"  FAILED {client}: {exc}")
            failures += 1
            continue
        # "not configured" is a successful no-op for removal.
        ok = changed or summary in {"already configured", "not configured"}
        results.append({"client": client, "path": str(path), "changed": changed, "ok": ok, "summary": summary})
        if not json_out:
            action = "configured" if changed else "skipped"
            print(f"  {client}: {action} ({summary})")
        if not ok:
            failures += 1
    # The OpenCode wake plugin is part of onboarding OpenCode: TUI sessions have
    # no attach URL, so opencode_thread wakes are undeliverable without it.
    if "opencode" in requested and (found.get("opencode") or (not remove and any(c == "opencode" for c, _ in targets))):
        try:
            changed, summary = remove_opencode_plugin() if remove else install_opencode_plugin()
        except Exception as exc:
            results.append({"client": "opencode-plugin", "changed": False, "ok": False, "error": str(exc)})
            if not json_out:
                print(f"  FAILED opencode-plugin: {exc}")
            failures += 1
        else:
            # "not configured" is a successful no-op for removal.
            ok = changed or summary in {"already installed", "not configured"}
            results.append({"client": "opencode-plugin", "changed": changed, "ok": ok, "summary": summary})
            if not json_out:
                action = "installed" if changed else "skipped"
                print(f"  opencode-plugin: {action} ({summary})")
            if not ok:
                failures += 1

    skipped_payload = [{"client": client, "reason": note} for client, note in skipped]
    if failures or skipped:
        # A skipped client (e.g. a JSONC-only install) means the requested
        # onboarding is INCOMPLETE: report it as a failure, not silent success.
        detail = f"{failures} file(s) failed" if failures else "some clients were skipped"
        if json_out:
            print(json.dumps({
                "ok": False, "error": detail, "verb": verb,
                "results": results, "skipped": skipped_payload,
            }, indent=2))
        else:
            print(f"vanth setup: {detail}", file=sys.stderr)
            for client, note in skipped:
                print(f"  {client}: {note}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps({"ok": True, "verb": verb, "results": results, "skipped": []}, indent=2))
    else:
        print("vanth setup: done")
    return 0
