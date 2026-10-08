__all__ = ["__version__"]


def _dist_info_version() -> str | None:
    """Fast path for the distribution version: parse METADATA directly.

    ``importlib.metadata.version()`` costs ~100ms here (it imports the
    email/csv parsing machinery); a dist-info ``METADATA`` file is one small
    read with a ``Version:`` header. Entries are scanned in ``sys.path``
    order so the winner matches what importlib would resolve. Any layout
    this does not understand falls through to the slow path (``None``).
    """
    import os
    import sys

    for entry in sys.path:
        try:
            names = os.listdir(entry or ".")
        except OSError:
            continue
        for name in names:
            if name.startswith("vanth-") and name.endswith(".dist-info"):
                try:
                    with open(
                        os.path.join(entry, name, "METADATA"),
                        encoding="utf-8",
                        errors="replace",
                    ) as handle:
                        for line in handle:
                            if line.startswith("Version:"):
                                version = line.split(":", 1)[1].strip()
                                if version:
                                    return version
                                break
                except OSError:
                    continue
    return None


def __getattr__(name: str) -> str:
    # NOTE: resolving the distribution version costs an importlib.metadata
    # scan (~80ms on Windows) and runs on EVERY `import vanth` — including
    # short-lived processes that never read the version (each job runner,
    # each CLI invocation). Resolve it lazily on first attribute access and
    # cache it in module globals, so later reads are a plain dict lookup.
    # (Previously `__version__ = _package_version()` ran eagerly at import.)
    if name == "__version__":
        resolved = _dist_info_version()
        if resolved is None:
            try:
                from importlib.metadata import PackageNotFoundError, version

                resolved = version("vanth")
            except PackageNotFoundError:  # source checkout, not installed
                resolved = "0.0.0"
        globals()["__version__"] = resolved
        return resolved
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
