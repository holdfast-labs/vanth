"""Client-side outbound relay for Desktop wake (review rc36 P0).

The persistent daemon never discovers client processes or stores rotating Codex
pipe names. Instead, the Vanth MCP/client integration owns an outbound relay:

1. The relay opens a durable local subscription to the daemon (registering the
   client/session/task identities it can wake) and long-polls for due
   ``codex_desktop`` deliveries.
2. The daemon publishes a generic delivery envelope (delivery_id, destination
   identity, prompt/event payload) plus an opaque lease token.
3. The relay acknowledges ONLY after the client-native prompt admission
   succeeds, presenting BOTH its client identity AND the opaque lease token, so
   only the claiming client can ack (review rc37 P1).
4. A disconnect leaves the delivery pending; a reconnect re-registers and
   resumes from the last acknowledged delivery id.

Only the final adapter differs between clients: OpenCode uses its documented
attached-server/session prompt API; Codex Desktop uses the host-provided
``codex_app/send_message_to_thread`` capability through the inherited
``CODEX_APP_TOOLS_PIPE_PATH``. That private pipe stays inside the Codex MCP
process — it is never sent to the daemon or written to the job database.

Provisioning (review rc37 P0): the real Codex Desktop MCP children do NOT
inherit ``CODEX_APP_TOOLS_PIPE_PATH`` or ``CODEX_THREAD_ID`` ambiently — the
pipe is granted only to the bundled ``codex_app`` MCP integration. The relay
therefore accepts the capability through an explicit, supported handoff:

- ``VANTH_CODEX_DESKTOP_PIPE``: the named-pipe path (set by ``vanth setup``
  when it provisions a Desktop wake integration, or by a host wrapper that
  launches Vanth with the granted capability).
- ``VANTH_CODEX_DESKTOP_THREAD``: the caller/executor thread identity (the
  outer ``params.threadId`` the host requires). If unset, ``CODEX_THREAD_ID``
  is used (CLI MCP processes).
- The pipe path is ALSO read from a per-home ``codex_desktop.json`` capability
  file written by ``vanth setup desktop``.

The relay NEVER silently no-ops: if the capability is absent it records an
explicit diagnostic (returned by ``start_desktop_relay``) so Desktop wake is
known to be unavailable rather than silently pending.

EXPERIMENTAL (review rc38 P2): the private host-pipe contract is not documented
by official Codex material. This integration is explicitly experimental and
scoped to ONE provisioned task per Desktop lifetime: one per-home capability
file stores one pipe/thread tuple, provisioning a second Desktop task overwrites
the first, and a Desktop restart invalidates the private pipe (stale capability
is detected and surfaced, not silently used). Automatic or durable multi-task
Desktop wake is NOT supported yet.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from .client import VanthClient

# A Desktop wake capability older than this is treated as stale (a Desktop
# restart invalidates the private app-tools pipe). 24h is generous for a
# long-lived Desktop session while still catching stale tuples promptly.
_DESKTOP_CAPABILITY_TTL_SECONDS = 24 * 3600


class RelayError(RuntimeError):
    pass


class RelayCapabilityLost(RelayError):
    """The Desktop pipe is unavailable and no re-provisioned capability exists.

    Raised by ``_deliver`` after the delivery has been released back to pending
    (no attempt consumed). ``_run`` must back off and keep polling — never
    ack-failed, which would terminally consume the wake at max_attempts=1.
    """


def _desktop_capability_file() -> dict[str, Any] | None:
    """Read the per-home Desktop wake capability file, if present.

    ``vanth setup desktop`` writes this file with the pipe path and thread
    identity it was granted when Desktop integration was active. The private
    app-tools pipe is invalidated by a Desktop restart, so a capability that is
    stale is surfaced (not silently used) — the relay logs an explicit
    diagnostic and does NOT register a destination it cannot deliver to (review
    rc38 P2: stale-capability detection; this integration is explicitly
    experimental and scoped to one provisioned task per Desktop lifetime).
    """
    try:
        from datetime import datetime, timezone

        from .paths import canonical_home

        path = canonical_home() / "codex_desktop.json"
        if not path.is_file():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        # Stale-capability detection: a capability provisioned more than
        # `_DESKTOP_CAPABILITY_TTL_SECONDS` ago is treated as stale. Desktop
        # restart invalidates the private pipe path, so an old capability is
        # almost certainly pointing at a dead pipe. The relay treats a stale
        # file as ABSENT (fail closed) and logs a diagnostic so the operator
        # re-runs `vanth setup desktop`.
        provisioned_at = data.get("provisioned_at")
        if provisioned_at:
            try:
                parsed = datetime.fromisoformat(provisioned_at.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - parsed).total_seconds()
                if age > _DESKTOP_CAPABILITY_TTL_SECONDS:
                    import logging

                    logging.getLogger("vanth").warning(
                        "Desktop wake capability file is STALE (provisioned %s, "
                        "over %ss old); a Desktop restart invalidates the private pipe. "
                        "Re-run `vanth setup desktop` inside an active Desktop session.",
                        provisioned_at,
                        _DESKTOP_CAPABILITY_TTL_SECONDS,
                    )
                    return None
            except (ValueError, TypeError):
                pass
        return data
    except Exception:
        return None


def _codex_thread_identity() -> str | None:
    """Resolve the caller/executor thread identity for Desktop wake.

    Order: explicit ``VANTH_CODEX_DESKTOP_THREAD`` (host handoff), then
    ``CODEX_THREAD_ID`` (CLI MCP processes), then the capability file. The
    caller thread id is REQUIRED by the native host as the outer
    ``params.threadId`` (review rc37 P0).
    """
    for name in ("VANTH_CODEX_DESKTOP_THREAD", "CODEX_THREAD_ID"):
        value = os.environ.get(name)
        if value:
            return value
    capability = _desktop_capability_file()
    if capability:
        thread = capability.get("thread_id") or capability.get("caller_thread_id")
        if thread:
            return thread
    return None


def _desktop_pipe_path() -> str | None:
    """Resolve the Desktop app-tools pipe path.

    Order: explicit ``VANTH_CODEX_DESKTOP_PIPE`` (host handoff), then the
    inherited ``CODEX_APP_TOOLS_PIPE_PATH``, then the capability file.
    """
    for name in ("VANTH_CODEX_DESKTOP_PIPE", "CODEX_APP_TOOLS_PIPE_PATH"):
        value = os.environ.get(name)
        if value:
            return value
    capability = _desktop_capability_file()
    if capability:
        path = capability.get("pipe_path") or capability.get("pipe")
        if path:
            return path
    return None


def _destinations() -> list[dict[str, Any]]:
    """The client identities this MCP process can wake.

    The caller/executor thread (used as the outer ``params.threadId``) is the
    relay's registered destination. Registration only happens when the pipe
    capability is actually present — fail closed when the Desktop capability is
    absent (review rc37 P0).
    """
    codex_thread = _codex_thread_identity()
    if not codex_thread:
        return []
    if not _desktop_pipe_path():
        return []
    return [{"client_type": "codex_desktop", "thread_id": codex_thread}]


class DesktopRelay:
    """Long-poll the daemon for codex_desktop deliveries and deliver them."""

    def __init__(
        self,
        client: VanthClient,
        client_id: str,
        *,
        destinations: list[dict[str, Any]] | None = None,
        caller_thread_id: str | None = None,
        pipe_path: str | None = None,
        poll_interval: float = 0.5,
        max_reconnect_delay: float = 15.0,
        activity_tracker: Any = None,
    ) -> None:
        self.client = client
        self.client_id = client_id
        self.destinations = destinations if destinations is not None else _destinations()
        # The executor/caller thread identity used as the outer params.threadId.
        self.caller_thread_id = caller_thread_id or _codex_thread_identity()
        # The resolved app-tools pipe path (explicit handoff env or capability
        # file). This MUST be carried into delivery: the delivery path reads only
        # CODEX_APP_TOOLS_PIPE_PATH, which real MCP children do not inherit
        # (review rc38 P0 — the provisioned pipe was discarded at delivery).
        self.pipe_path = pipe_path or _desktop_pipe_path()
        self.poll_interval = poll_interval
        self.max_reconnect_delay = max_reconnect_delay
        # Optional watchdog activity tracker: while the relay owns an active
        # subscription it marks activity so the MCP idle watchdog does not reap
        # a healthy process that is still delivering wake notifications (review
        # rc37 P1). Parent-death cleanup is untouched.
        self.activity_tracker = activity_tracker
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._backoff = 0.5
        self._outage_backoff = 0.5
        self._registered = False

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="vanth-desktop-relay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._unregister()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)

    def _register(self) -> bool:
        try:
            result = self.client.post(
                "/relay/register",
                {
                    "client_id": self.client_id,
                    "client_type": "codex_desktop",
                    "destinations": self.destinations,
                },
            )
            ok = bool(result and result.get("result") == "ok")
            self._registered = ok
            return ok
        except Exception:
            self._registered = False
            return False

    def _unregister(self) -> None:
        if not self._registered:
            return
        try:
            self.client.post("/relay/unregister", {"client_id": self.client_id})
        except Exception:
            pass
        self._registered = False

    def _poll(self) -> list[dict[str, Any]]:
        # Long-poll up to 15s per request so a reconnect doesn't spin. The poll
        # response includes an opaque lease token per delivery; ack requires it
        # so only the claiming client can complete a delivery (review rc37 P1).
        params = {"client_id": self.client_id, "timeout_seconds": "15"}
        result = self.client.get("/relay/poll", params)
        return result.get("deliveries") if isinstance(result, dict) else []

    def _reload_capability(self) -> bool:
        """Re-resolve the Desktop capability (pipe + thread) after a failure.

        A Desktop restart invalidates the private pipe; re-running
        ``vanth setup desktop`` rewrites ``codex_desktop.json`` with a fresh
        pipe. On pipe-unavailable failure the relay re-reads the capability and
        returns True only when a NEW usable pipe appeared (then the caller
        re-registers and retries). A vanished capability is NOT treated as a
        change — retrying the stale dead path would just spin (self-review
        rc40).
        """
        new_pipe = _desktop_pipe_path()
        new_thread = _codex_thread_identity()
        if not new_pipe:
            return False
        if new_pipe == self.pipe_path and new_thread == self.caller_thread_id:
            return False
        self.pipe_path = new_pipe
        if new_thread:
            self.caller_thread_id = new_thread
        self.destinations = (
            [{"client_type": "codex_desktop", "thread_id": self.caller_thread_id}]
            if self.caller_thread_id and self.pipe_path
            else []
        )
        self._registered = False
        import logging

        logging.getLogger("vanth").warning(
            "Desktop wake capability changed (pipe/thread re-provisioned); re-registering relay"
        )
        return True

    def _ack(self, delivery: dict[str, Any], status: str, error: str | None = None) -> None:
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "delivery_id": delivery["delivery_id"],
            "lease_token": delivery.get("lease_token"),
            "status": status,
        }
        if error is not None:
            payload["error"] = error
        result = self.client.post("/relay/ack", payload)
        # VanthClient.post() converts HTTP errors into JSON error objects rather
        # than raising. A stale lease, ownership mismatch, or other rejected
        # acknowledgement MUST NOT be treated as success — otherwise the relay
        # believes the wake is complete while the row remains 'dispatching' and
        # the lease expiry reclaims/delivers it again (review rc38 P1).
        if not isinstance(result, dict) or result.get("result") != "ok":
            detail = ""
            if isinstance(result, dict):
                detail = result.get("error") or result.get("message") or ""
            raise RelayError(
                f"acknowledgement rejected for delivery {delivery['delivery_id']} "
                f"(lease may be stale or reclaimed): {detail}".rstrip()
            )

    def _release(self, delivery: dict[str, Any]) -> None:
        """Return a claimed delivery to pending WITHOUT consuming an attempt.

        Best-effort: if the daemon is unreachable the lease simply expires and
        the delivery is reclaimed later; if the claim already moved on, the
        rejection is ignored. Never raises.
        """
        try:
            self.client.post(
                "/relay/release",
                {
                    "client_id": self.client_id,
                    "delivery_id": delivery["delivery_id"],
                    "lease_token": delivery.get("lease_token"),
                },
            )
        except Exception:
            pass

    def _deliver(self, delivery: dict[str, Any]) -> None:
        from .codex_bridge import send_delivery_to_codex_desktop
        from .codex_pipe import CodexPipeUnavailable

        # Deliver through the native pipe, injecting the relay's authenticated
        # caller/executor thread identity (required as outer params.threadId) and
        # the relay's RESOLVED pipe path (the provisioned handoff pipe — delivery
        # must not fall back to the un-inherited CODEX_APP_TOOLS_PIPE_PATH).
        # At-least-once semantics: the Vanth delivery id is the stable call/turn
        # correlation key. If host admission succeeds but the ack is lost, the
        # lease expires and the delivery is reclaimed for retry.
        try:
            send_delivery_to_codex_desktop(
                delivery["payload"],
                caller_thread_id=self.caller_thread_id,
                pipe_path=self.pipe_path,
            )
            self._ack(delivery, "delivered")
            self._outage_backoff = 0.5
            return
        except CodexPipeUnavailable:
            pass  # outage handling below
        except Exception:
            # Ack failures are handled at the _run boundary (bounded reconnect),
            # not here, so a transient daemon outage cannot kill the relay.
            raise
        # Pipe outage (Desktop restart invalidated the pipe). If the capability
        # was re-provisioned meanwhile, re-register and retry ONCE with the
        # fresh pipe so an already-running relay delivers the pending wake
        # without an MCP restart (review rc39 P1).
        if self._reload_capability() and self._register():
            try:
                send_delivery_to_codex_desktop(
                    delivery["payload"],
                    caller_thread_id=self.caller_thread_id,
                    pipe_path=self.pipe_path,
                )
                self._ack(delivery, "delivered")
                self._outage_backoff = 0.5
                return
            except CodexPipeUnavailable:
                pass  # still down; fall through to release
            except Exception:
                raise  # a non-pipe error on retry takes the normal failed path
        # Persistent outage with no re-provision: release the delivery back to
        # pending WITHOUT consuming an attempt (a failed ack would be terminal
        # at the default max_attempts=1) and signal _run to back off and keep
        # polling. (self-review rc40: the old code acked failed here, losing
        # the wake on the first outage poll.)
        import logging

        logging.getLogger("vanth").warning(
            "Codex Desktop pipe unavailable (delivery %s released, not consumed); "
            "if Desktop restarted, re-run `vanth setup desktop` to re-provision.",
            delivery.get("delivery_id"),
        )
        self._release(delivery)
        raise RelayCapabilityLost(
            f"Codex Desktop pipe unavailable for delivery {delivery.get('delivery_id')}; "
            "released back to pending"
        )

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                if self.activity_tracker is not None:
                    self.activity_tracker.notify_activity()
                if not self._register():
                    # Registration failed (daemon not up, or no destinations).
                    # Wait with bounded backoff and retry.
                    time.sleep(self._backoff)
                    self._backoff = min(self._backoff * 2, self.max_reconnect_delay)
                    continue
                self._backoff = 0.5
                try:
                    # The long-poll can block for up to 15s; hold the tracker as
                    # a CONTEXT MANAGER so the watchdog sees the poll as
                    # in-flight for its whole duration (review rc39 P1 — a
                    # single instant notify_activity() before a long poll is
                    # invisible if VANTH_WATCH_IDLE < poll duration).
                    if self.activity_tracker is not None:
                        with self.activity_tracker:
                            deliveries = self._poll()
                    else:
                        deliveries = self._poll()
                except Exception:
                    # Daemon unreachable: reconnect with backoff. Do NOT let the
                    # exception escape the loop (review rc37 P1).
                    time.sleep(self._backoff)
                    self._backoff = min(self._backoff * 2, self.max_reconnect_delay)
                    continue
                for index, delivery in enumerate(deliveries):
                    if self._stop.is_set():
                        break
                    try:
                        if self.activity_tracker is not None:
                            # A Desktop delivery (helper subprocess) can run far
                            # longer than any idle timeout; keep the tracker
                            # active for the entire delivery + ack (review rc39
                            # P1).
                            with self.activity_tracker:
                                self._deliver(delivery)
                        else:
                            self._deliver(delivery)
                    except RelayCapabilityLost:
                        # Pipe outage: the delivery was already released back to
                        # pending (no attempt consumed). Back off and return to
                        # register/poll so a re-provision is picked up without
                        # an MCP restart — and NEVER ack-failed here, which
                        # would terminally consume the wake at max_attempts=1.
                        # Poll claims a batch up front. Return every unprocessed
                        # sibling immediately rather than holding its lease until
                        # expiry while this relay already knows the pipe is down.
                        for remaining in deliveries[index + 1:]:
                            self._release(remaining)
                        time.sleep(self._outage_backoff)
                        self._outage_backoff = min(self._outage_backoff * 2, self.max_reconnect_delay)
                        break
                    except Exception as exc:
                        # A failed delivery (or failed ack) must not kill the
                        # relay: mark the delivery failed so it retries per the
                        # normal backoff, then continue polling. If even the ack
                        # cannot be sent (daemon down), the lease expires and the
                        # daemon reclaims the delivery later.
                        from .codex_pipe import CodexPipeError
                        import logging

                        # Bridge/relay exceptions are suitable for diagnostics;
                        # unexpected exceptions may contain paths or payloads.
                        if isinstance(exc, (CodexPipeError, RelayError)):
                            error = f"{type(exc).__name__}: {exc}"
                        else:
                            error = f"Desktop delivery failed ({type(exc).__name__})"
                        if self.pipe_path:
                            error = error.replace(self.pipe_path, "<desktop endpoint>")
                        error = error[:1000]
                        logging.getLogger("vanth").warning(
                            "Desktop wake delivery %s failed: %s", delivery.get("delivery_id"), error,
                        )
                        try:
                            self._ack(delivery, "failed", error)
                        except Exception:
                            # Ack unavailable (daemon down); the lease will
                            # expire and the daemon reclaims the delivery.
                            pass
                        time.sleep(0.2)
        finally:
            self._unregister()


def start_desktop_relay(activity_tracker: Any = None) -> DesktopRelay | None:
    """Start the Desktop wake relay if this process can wake a Desktop task.

    Returns the relay when the capability is present and a thread identity is
    available, otherwise ``None``. Prerequisite absence is not silent: the
    diagnostic is written to the ``vanth`` logger (and surfaced in ``doctor``
    via ``relay_status``) so Desktop wake being unavailable is observable
    (review rc37 P0). A relay with no registered destinations exits its loop
    without registering, so it never blocks shutdown.

    ``activity_tracker`` is the MCP watchdog's ``_InFlight`` tracker; the relay
    marks activity around each poll so an active subscription is never
    idle-reaped (review rc37 P1).
    """
    codex_thread = _codex_thread_identity()
    pipe_path = _desktop_pipe_path()
    if not codex_thread or not pipe_path:
        reason = []
        if not pipe_path:
            reason.append("no Codex Desktop pipe capability (CODEX_APP_TOOLS_PIPE_PATH / VANTH_CODEX_DESKTOP_PIPE / codex_desktop.json)")
        if not codex_thread:
            reason.append("no caller thread identity (CODEX_THREAD_ID / VANTH_CODEX_DESKTOP_THREAD)")
        import logging

        logging.getLogger("vanth").warning(
            "Desktop wake relay not started: %s. Provision with `vanth setup desktop` or a host handoff.",
            "; ".join(reason),
        )
        return None
    try:
        client = VanthClient()
        client.ensure()
    except Exception:
        import logging

        logging.getLogger("vanth").warning("Desktop wake relay not started: daemon unreachable")
        return None
    relay = DesktopRelay(
        client,
        f"mcp-{os.getpid()}-{codex_thread}",
        activity_tracker=activity_tracker,
    )
    relay.start()
    return relay
