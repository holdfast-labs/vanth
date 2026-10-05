"""Best-effort run-overview metadata capture, mirroring W&B's overview tab.

Gathered at job start so an agent can answer "what is this job?" — author,
host/OS/Python, git state, and system hardware. Every lookup is optional and
must never raise: a missing field is simply omitted. Machine-wide values (GPU,
CPU) are captured once and cached.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
from typing import Any

_gpu_cache: list[dict[str, str]] | None | bool = False
_cpu_cache: int | None = None
_git_cache: dict[str, tuple[str | None, str | None, str | None] | None] = {}

# Interpreters/tools that identify the language a command runs. Matching is done
# on the basename (path stripped, trailing version digits dropped), so
# python3.12 / node18 / go1.25 all resolve. `uv` implies Python; generic build
# drivers (make, cmake) and shells are deliberately omitted — they say nothing
# about the job's language and every job may use them.
_TOOLCHAIN_TOKENS = {
    "python": "python", "py": "python", "pip": "python", "pytest": "python",
    "poetry": "python", "uv": "python", "conda": "python", "ipython": "python",
    "node": "node", "npm": "node", "npx": "node", "yarn": "node", "pnpm": "node",
    "bun": "node", "deno": "node", "tsc": "node",
    "go": "go", "gofmt": "go",
    "cargo": "rust", "rustc": "rust", "rustup": "rust",
    "java": "java", "javac": "java", "mvn": "java", "mvnw": "java", "gradle": "java", "gradlew": "java",
    "kotlin": "java", "kotlinc": "java", "scala": "java",
    "dotnet": "dotnet", "msbuild": "dotnet",
    "ruby": "ruby", "rails": "ruby", "gem": "ruby", "bundle": "ruby", "irb": "ruby",
    "php": "php", "composer": "php",
    "rscript": "r", "julia": "julia",
    "gcc": "c", "g++": "c++", "clang": "c", "clang++": "c++", "cc": "c",
}

# Shells whose `-c`/`/c` payload holds the real command, e.g. `bash -lc 'go test'`.
_SHELLS = {"sh", "bash", "zsh", "dash", "ash", "ksh", "fish", "cmd", "powershell", "pwsh"}
# Prefix wrappers to skip before the real program, e.g. `sudo go test`.
_PREFIX_WRAPPERS = {"env", "sudo", "nice", "nohup", "command", "exec", "time", "busybox"}
_ASSIGNMENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=.*")

# Project marker files that identify a language by what lives in the cwd.
_MARKER_FILES = {
    "pyproject.toml": "python", "requirements.txt": "python", "setup.py": "python",
    "setup.cfg": "python", "pipfile": "python", "poetry.lock": "python", "uv.lock": "python",
    "go.mod": "go", "go.sum": "go",
    "cargo.toml": "rust",
    "package.json": "node",
    "pom.xml": "java", "build.gradle": "java", "build.gradle.kts": "java", "settings.gradle": "java",
    "gemfile": "ruby",
    "composer.json": "php",
}
_MARKER_SUFFIXES = {".csproj": "dotnet", ".sln": "dotnet"}
_TOKEN_RE = re.compile(r"[A-Za-z0-9_@.+\\/:=-]+")


def _token_language(token: str) -> str | None:
    name = re.split(r"[\\/]", token)[-1].lower()
    if name.endswith(".exe"):
        name = name[:-4]
    name = re.sub(r"[0-9.]+$", "", name)
    return _TOOLCHAIN_TOKENS.get(name)


def _command_language(command: str | None) -> str | None:
    """Language of the command's PROGRAM, not of an arbitrary argument.

    Scanning every token mislabels `rm -rf node` as Node and `echo go` as Go, so
    only the leading program is considered, unwrapping a shell's `-c`/`/c`
    payload (`bash -lc 'go test'`) to the program it runs.
    """
    if not command:
        return None
    tokens = _TOKEN_RE.findall(command)
    index = 0
    for _ in range(4):
        # Skip prefix wrappers (`sudo`, `env`) and leading assignments
        # (`FOO=1 python x`); quoted paths containing spaces are not parsed.
        while index < len(tokens) and (
            tokens[index] in _PREFIX_WRAPPERS or _ASSIGNMENT_RE.fullmatch(tokens[index])
        ):
            index += 1
        if index >= len(tokens):
            return None
        token = tokens[index]
        language = _token_language(token)
        if language:
            return language
        name = re.split(r"[\\/]", token)[-1].lower()
        if name.endswith(".exe"):
            name = name[:-4]
        if name not in _SHELLS:
            return None
        # Find the shell's command-string flag (-c / -lc / /c / -Command). Only
        # then is the following token a program; without it, `bash script` runs a
        # script (not a language we can name), so report nothing.
        index += 1
        saw_command_flag = False
        while index < len(tokens):
            flag = tokens[index].lower()
            normalized = flag.lstrip("-/")
            if normalized in {"c", "k", "command", "commandwithargs"} or (
                flag.startswith("-") and not flag.startswith("--") and flag.endswith("c")
            ):
                index += 1
                saw_command_flag = True
                break
            if flag.startswith("-") or flag.startswith("/"):
                index += 1
                continue
            break
        if not saw_command_flag:
            return None
    return None


def _marker_language(cwd: str | None) -> tuple[str, str] | None:
    """Return ``(language, marker_path)`` for the project containing ``cwd``.

    The walk stops at the repository root (a ``.git`` file or directory) and
    never enters the user's home directory, so a stray ``~/requirements.txt``
    cannot mislabel an unrelated job.
    """
    if not cwd:
        return None
    home = os.path.normcase(os.path.realpath(os.path.expanduser("~")))
    current = os.path.realpath(cwd)
    for _ in range(12):
        if os.path.normcase(current) == home:
            return None  # never label a job from the user's home directory
        try:
            entries = os.listdir(current)
        except OSError:
            return None
        lowered = {entry.lower(): entry for entry in entries}
        for filename, language in _MARKER_FILES.items():
            if filename in lowered:
                return language, os.path.join(current, lowered[filename])
        for entry in entries:
            suffix = os.path.splitext(entry)[1].lower()
            if suffix in _MARKER_SUFFIXES:
                return _MARKER_SUFFIXES[suffix], os.path.join(current, entry)
        if os.path.lexists(os.path.join(current, ".git")):
            return None  # reached the repo root; never look above the project
        parent = os.path.dirname(current)
        if parent == current or os.path.normcase(parent) == home:
            return None
        current = parent
    return None


def infer_toolchain(command: str | None, cwd: str | None) -> dict[str, str] | None:
    """Best-effort language/toolchain for a job, from its command then its cwd.

    The command's program wins (it says what actually runs); a project marker
    file is the fallback. Returns ``None`` when nothing is recognizable, so the
    field is simply omitted.
    """
    language = _command_language(command)
    if language:
        return {"language": language, "detected_from": "command"}
    marker = _marker_language(cwd)
    if marker:
        return {"language": marker[0], "detected_from": "file", "marker": marker[1]}
    return None


def _git(cwd: str | None, *args: str) -> str | None:
    if not cwd:
        return None
    try:
        result = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True, text=True, timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _git_state(cwd: str | None) -> dict[str, str] | None:
    """Return git repo/branch/commit for a directory, cached per cwd."""
    if not cwd:
        return None
    cached = _git_cache.get(cwd)
    if cwd in _git_cache:
        if cached is None:
            return None
        return dict(cached)
    repo = _git(cwd, "remote", "get-url", "origin")
    branch = _git(cwd, "rev-parse", "--abbrev-ref", "HEAD")
    commit = _git(cwd, "rev-parse", "HEAD")
    state: dict[str, str] = {}
    if branch:
        state["branch"] = branch
    if commit:
        state["commit"] = commit
    if repo:
        state["repository"] = repo
    _git_cache[cwd] = state or None
    return state or None


def _gpu_info() -> list[dict[str, str]] | None:
    global _gpu_cache
    if _gpu_cache is not False:
        return _gpu_cache
    smi = shutil.which("nvidia-smi")
    if smi:
        try:
            result = subprocess.run(
                [smi, "--query-gpu=name,driver_version", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=3,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
        else:
            if result.returncode == 0:
                gpus = []
                for line in result.stdout.splitlines():
                    parts = [p.strip() for p in line.split(",")]
                    if parts and parts[0]:
                        gpus.append({"name": parts[0], "driver": parts[1] if len(parts) > 1 else ""})
                _gpu_cache = gpus or None
                return _gpu_cache
    try:
        import torch  # type: ignore[import-not-found]
    except ImportError:
        _gpu_cache = None
        return None
    try:
        count = torch.cuda.device_count()
    except Exception:
        _gpu_cache = None
        return None
    if count <= 0:
        _gpu_cache = None
        return None
    gpus = []
    for index in range(count):
        try:
            gpus.append({"name": torch.cuda.get_device_name(index)})
        except Exception:
            gpus.append({"name": f"cuda:{index}"})
    _gpu_cache = gpus or None
    return _gpu_cache


def capture_run_metadata(
    cwd: str | None = None, notes: str | None = None, command: str | None = None
) -> dict[str, Any]:
    """Capture the run-overview fields W&B shows in its overview tab.

    ``toolchain`` describes the JOB (inferred from its command and project
    markers), not the daemon's own interpreter — a Go/Rust/Node job must never
    report a Python version.
    """
    global _cpu_cache
    if _cpu_cache is None:
        _cpu_cache = os.cpu_count() or None
    git_state = _git_state(cwd)
    info: dict[str, Any] = {
        "author": os.environ.get("USERNAME") or os.environ.get("USER") or None,
        "hostname": platform.node() or None,
        "os": platform.system() or None,
        "os_release": platform.release() or None,
        "machine": platform.machine() or None,
        "cpu_count": _cpu_cache,
        "gpus": _gpu_info(),
        "cwd": cwd,
    }
    toolchain = infer_toolchain(command, cwd)
    if toolchain:
        info["toolchain"] = toolchain
    if git_state:
        info["git"] = git_state
    if notes:
        info["notes"] = notes
    return info


def serialize_run_metadata(info: dict[str, Any]) -> str:
    return json.dumps(info, separators=(",", ":"), ensure_ascii=False)
