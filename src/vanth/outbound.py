"""Outbound destination policy for webhooks and HTTP readiness probes.

Vanth is local-first, so loopback and RFC1918 destinations are *allowed* by
default (ntfy/Gotify/local health endpoints). What is always denied is the SSRF
tripwire set — link-local / cloud-metadata / unspecified addresses — and callers
can opt into a strict allowlist or a block-private policy.

Policy (environment):
- ``VANTH_OUTBOUND_ALLOW``: comma list of hosts, IPs, or CIDRs. When set, only
  those destinations are allowed (host-name match, or every resolved IP falls in
  a listed network).
- ``VANTH_OUTBOUND_BLOCK_PRIVATE=1``: additionally deny loopback + private.

The host is always resolved and *every* resolved address is checked, so an
allowlisted name cannot point at a denied IP without being refused. Note the
remaining TOCTOU gap: the check and the subsequent connect resolve
independently, so a hostile DNS that rebinds between the two could still steer
the connection. That is out of scope for a loopback single-user daemon, but
callers must not treat a passed check as a pinned address.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlsplit

_METADATA_HOSTS = {"metadata.google.internal", "metadata.goog", "instance-data"}


class OutboundDenied(ValueError):
    """The destination is refused by the configured outbound policy."""


def _allowlist() -> list[str]:
    raw = os.environ.get("VANTH_OUTBOUND_ALLOW", "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def _block_private() -> bool:
    return os.environ.get("VANTH_OUTBOUND_BLOCK_PRIVATE", "0").strip().lower() not in {"", "0", "false", "no"}


def _normalize(ip: ipaddress._BaseAddress) -> ipaddress._BaseAddress:
    # Collapse IPv4-mapped IPv6 (::ffff:127.0.0.1) to its IPv4 form so the
    # loopback/private/link-local checks see the real address.
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def _tripwire(ip: ipaddress._BaseAddress) -> bool:
    # Always-denied: link-local (cloud metadata), unspecified, multicast,
    # reserved. Loopback/private are intentionally NOT here (local-first), but
    # note IPv6 ::1 is classed `reserved`, so loopback is checked first.
    ip = _normalize(ip)
    if ip.is_loopback:
        return False
    return ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved


def _matches_allow(ip: ipaddress._BaseAddress, allow: list[str]) -> bool:
    for entry in allow:
        try:
            if "/" in entry:
                if ip in ipaddress.ip_network(entry, strict=False):
                    return True
            elif ip == ipaddress.ip_address(entry):
                return True
        except ValueError:
            continue  # a hostname entry; handled by name match
    return False


def check_outbound_url(url: str) -> None:
    """Raise :class:`OutboundDenied` if ``url`` violates the destination policy."""
    if not isinstance(url, str) or not url:
        raise OutboundDenied("outbound URL must be a non-empty string")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        raise OutboundDenied(f"unsupported outbound scheme: {parts.scheme!r}")
    host = parts.hostname
    if not host:
        raise OutboundDenied("outbound URL has no host")
    lowered = host.lower().rstrip(".")
    allow = _allowlist()
    allow_by_name = bool(allow) and lowered in allow
    port = parts.port or (443 if parts.scheme == "https" else 80)
    # Always resolve — an allowlisted NAME must not bypass the address checks
    # (a name can resolve to link-local/metadata, or rebind). The tripwire set
    # is denied even in allowlist mode.
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise OutboundDenied(f"cannot resolve outbound host {host!r}") from exc
    addresses = {_normalize(ipaddress.ip_address(info[4][0])) for info in infos}
    if not addresses:
        raise OutboundDenied(f"outbound host {host!r} resolved to no address")
    if lowered in _METADATA_HOSTS:
        raise OutboundDenied(f"cloud metadata host is not allowed: {host}")
    for ip in addresses:
        if _tripwire(ip):
            raise OutboundDenied(f"outbound address {ip} is blocked (link-local/metadata/unspecified)")
    if allow:
        if allow_by_name:
            return
        # Strict: EVERY resolved address must fall in the allowlist.
        if not all(_matches_allow(ip, allow) for ip in addresses):
            raise OutboundDenied(f"outbound host {host!r} is not in VANTH_OUTBOUND_ALLOW")
        return
    if _block_private() and any(ip.is_private or ip.is_loopback for ip in addresses):
        raise OutboundDenied("outbound address is blocked (VANTH_OUTBOUND_BLOCK_PRIVATE)")
