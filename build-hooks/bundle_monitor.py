"""Hatchling build hook that bundles the native Go binary into the wheel.

Runs ``go build`` for the host platform and ships the resulting multi-call
binary as two wheel *scripts* (``<name>.data/scripts/``), which installers put
in the env's ``Scripts``/``bin`` directory next to the Python console scripts:

- ``vanth-monitor`` — the terminal monitor (selected by the program name), and
- ``vanth-mcp`` — the MCP launcher shim, which starts ``python -m vanth.server``
  for the env it is installed in with one process fewer than the ``vanth``
  console script.

No Go toolchain is needed at runtime. The binary is built with ``-s -w``
(no symbol/DWARF tables; panics still print stack traces) because it ships
twice.

The hook runs only for the ``wheel`` target (not ``sdist``). ``go`` must be on
PATH during the build; it is not required at install or run time.

Three modes
-----------
1. Host ``go build`` (default): compiles the monitor for the host platform and
   tags the wheel with the host PEP 425 platform tag (:func:`wheel_platform_tag`).
2. Cross ``go build``: when both ``VANTH_MONITOR_GOOS`` and
   ``VANTH_MONITOR_GOARCH`` are set (and ``VANTH_MONITOR_BIN`` is not), they are
   exported to the ``go build`` subprocess so a single machine can cross-compile
   every target. The wheel platform tag is derived from the target via
   :func:`platform_tag_for`.
3. Prebuilt binary injection: when ``VANTH_MONITOR_BIN`` points at an existing
   file, that file is copied into the build output and ``go`` is never invoked
   (no Go toolchain needed in the build environment). The binary is named for
   the host platform (where the wheel will be installed) and the wheel tag
   honors ``VANTH_MONITOR_TAG`` if set, else falls back to the host tag.

``VANTH_MONITOR_TAG`` overrides the wheel platform tag in every mode.
"""

from __future__ import annotations

import os
import shutil
import subprocess

try:
    from hatchling.builders.hooks.plugin.interface import BuildHookInterface
except ImportError:  # pragma: no cover - only required inside a real build
    class BuildHookInterface:
        """Minimal stand-in for hatchling's hook interface.

        Kept so the module can be imported (and its pure helpers unit-tested)
        in environments without a build backend installed. Mirrors the
        attribute surface that ``initialize`` relies on.
        """

        PLUGIN_NAME = ""

        @property
        def app(self) -> object:
            return self._BuildHookInterface__app

        @property
        def root(self) -> str:
            return self._BuildHookInterface__root

        @property
        def target_name(self) -> str:
            return self._BuildHookInterface__target_name

        @property
        def metadata(self) -> object:
            return self._BuildHookInterface__metadata

try:
    from packaging.tags import platform_tags
except ImportError:  # pragma: no cover - packaging is a hatchling dependency
    platform_tags = None


def get_build_hook():
    return BundleMonitorBuildHook


def wheel_platform_tag() -> str:
    """Return the PEP 425 platform tag for the host, e.g. ``win_amd64``."""
    if platform_tags is not None:
        tags = list(platform_tags())
        if tags:
            return tags[0]
    # Fallback: packaging is unavailable; derive a coarse tag.
    machine = os.environ.get("PROCESSOR_ARCHITECTURE") or os.uname().machine
    if os.name == "nt":
        return {"AMD64": "win_amd64", "ARM64": "win_arm64"}.get(machine, machine.lower())
    sysname = os.uname().sysname.lower()
    return f"{sysname}_{machine.lower()}"


_CROSS_TARGET_TAGS = {
    ("windows", "amd64"): "win_amd64",
    ("linux", "amd64"): "manylinux_2_17_x86_64",
    ("linux", "arm64"): "manylinux_2_17_aarch64",
    ("darwin", "amd64"): "macosx_10_15_x86_64",
    ("darwin", "arm64"): "macosx_11_0_arm64",
}


def platform_tag_for(goos: str, goarch: str) -> str:
    """Return a coarse PEP 425 platform tag for a GOOS/GOARCH cross target.

    Unknown combinations fall back to a ``<goos>_<goarch>`` tag so the wheel
    still builds with a deterministic name.
    """
    return _CROSS_TARGET_TAGS.get((goos, goarch), f"{goos}_{goarch}")


def monitor_filename(goos: str | None = None) -> str:
    """Return the bundled monitor filename for a target ``GOOS``.

    ``None`` means "host platform"; the ``.exe`` suffix is applied for Windows
    (both the native host build and any ``GOOS=windows`` cross build).
    """
    if goos is None:
        exe = ".exe" if os.name == "nt" else ""
    else:
        exe = ".exe" if goos == "windows" else ""
    return f"vanth-monitor{exe}"


class BundleMonitorBuildHook(BuildHookInterface):
    PLUGIN_NAME = "bundle-monitor"

    def initialize(self, version: str, build_data: dict[str, dict]) -> None:
        if self.target_name != "wheel":
            return

        root = self.root
        prebuilt = os.environ.get("VANTH_MONITOR_BIN")
        goos = os.environ.get("VANTH_MONITOR_GOOS")
        goarch = os.environ.get("VANTH_MONITOR_GOARCH")
        tag_override = os.environ.get("VANTH_MONITOR_TAG")

        if prebuilt is not None:
            binary = self._inject_prebuilt(root, prebuilt, goos)
            if tag_override:
                tag = tag_override
            elif goos and goarch:
                # A prebuilt binary + target GOOS/GOARCH describe a specific
                # target wheel even without an explicit tag; name both the
                # binary and the wheel for the TARGET (review P2-1). Never fall
                # back to the BUILD host tag, which would pair a Windows binary
                # with a Linux wheel tag.
                tag = platform_tag_for(goos, goarch)
            else:
                tag = wheel_platform_tag()
        elif goos and goarch:
            binary = self._compile(root, goos, goarch)
            tag = tag_override or platform_tag_for(goos, goarch)
        else:
            binary = self._compile(root, None, None)
            tag = tag_override or wheel_platform_tag()

        # One multi-call binary, two names: wheels cannot hold links, so the
        # shim is a copy. argv[0] selects the role at run time.
        shim = os.path.join(os.path.dirname(binary), os.path.basename(binary).replace("vanth-monitor", "vanth-mcp"))
        shutil.copyfile(binary, shim)
        shutil.copymode(binary, shim)
        build_data["shared_scripts"] = {
            binary: os.path.basename(binary),
            shim: os.path.basename(shim),
        }
        build_data["tag"] = f"py3-none-{tag}"
        self.app.display_success(f"Bundled native scripts -> {os.path.basename(binary)}, {os.path.basename(shim)}")

    def _compile(self, root: str, goos: str | None, goarch: str | None) -> str:
        """Compile the monitor with the Go toolchain (host or cross target)."""
        env = dict(os.environ)
        if goos and goarch:
            env["GOOS"] = goos
            env["GOARCH"] = goarch

        binary = os.path.join(root, "dist", monitor_filename(goos))
        os.makedirs(os.path.dirname(binary), exist_ok=True)

        ldflags = f"-s -w -X vanth/internal/config.Version={self.metadata.version}"
        cmd = [
            "go",
            "build",
            "-trimpath",
            "-ldflags",
            ldflags,
            "-o",
            binary,
            "./cmd/vanth",
        ]
        subprocess.run(cmd, cwd=root, check=True, env=env)
        return binary

    def _inject_prebuilt(self, root: str, source: str, goos: str | None = None) -> str:
        """Copy a prebuilt monitor binary into the build output without compiling.

        The bundled filename must match what the runtime lookup expects on the
        INSTALL platform: ``vanth-monitor`` on POSIX, ``vanth-monitor.exe`` on
        Windows. In CI all wheels are assembled on a Linux host, so the name
        must come from the TARGET GOOS (``VANTH_MONITOR_GOOS``), not the build
        host — otherwise a Windows wheel would contain a binary named
        ``vanth-monitor`` (no .exe) and the runtime ``vanth-monitor.exe``
        lookup would fail.
        """
        if not os.path.isfile(source):
            raise FileNotFoundError(
                f"VANTH_MONITOR_BIN is set but no file exists at: {source}"
            )
        # The wheel is tagged for the target platform; name the bundled binary
        # for the TARGET so the installed wheel's runtime lookup finds it.
        binary = os.path.join(root, "dist", monitor_filename(goos))
        os.makedirs(os.path.dirname(binary), exist_ok=True)
        shutil.copyfile(source, binary)
        if not (goos == "windows" or (goos is None and os.name == "nt")):
            # Review P1-1: artifact download and copyfile do not preserve
            # executable bits. The published POSIX wheels stored the monitor as
            # mode 0644, so running it failed with PermissionError. chmod 0755
            # explicitly for every non-Windows target.
            os.chmod(binary, 0o755)
        return binary
