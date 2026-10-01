import concurrent.futures
import http.client
import json
import os
import socket
import signal
import stat
import subprocess
import sys
import threading
import time

import vanth.daemon as daemon


import shellcmd


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_daemon(tmp_path, max_request_bytes=1024 * 1024):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={
            **os.environ,
            "VANTH_HOME": str(tmp_path / "state"),
            "VANTH_DAEMON_PORT": str(port),
            "VANTH_MAX_REQUEST_BYTES": str(max_request_bytes),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            status, payload = request(port, "GET", "/health")
            if status == 200 and payload == {"ok": True}:
                return proc, port
        except OSError:
            time.sleep(0.05)
    proc.terminate()
    try:
        _, err = proc.communicate(timeout=5)
    except Exception:
        err = b""
    log_tail = ""
    log_path = tmp_path / "state" / "logs" / "daemon.log"
    if log_path.exists():
        log_tail = log_path.read_text(encoding="utf-8", errors="replace")[-3000:]
    raise AssertionError(
        f"daemon did not start (rc={proc.returncode}): stderr={err[-2000:]!r} log={log_tail!r}"
    )


def request(port, method, path, body=None, headers=None):
    # 20s: spawning a runner / DB work can exceed 5s on a loaded Windows runner;
    # connection-refused is immediate, so the health-poll deadline still holds.
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def stop_daemon(proc):
    proc.terminate()
    proc.wait(timeout=5)
    return proc.stderr.read().decode(errors="replace")


def test_malformed_post_bodies_return_json_errors_and_daemon_stays_healthy(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        cases = [
            (b'{"command":', {"Content-Length": "11"}),
            (b"\xff\xfe", {"Content-Length": "2"}),
            (b"[]", {"Content-Length": "2"}),
        ]
        for body, headers in cases:
            status, payload = request(port, "POST", "/jobs", body, headers)
            assert status == 400
            assert payload["result"] == "error"

        status, payload = request(
            port,
            "POST",
            "/jobs",
            b'{"command":"echo ok","bogus":1}',
            {"Content-Type": "application/json"},
        )
        assert status == 400
        assert payload["result"] == "error"
        assert request(port, "GET", "/health") == (200, {"ok": True})
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_content_length_validation_and_body_cap(tmp_path):
    proc, port = start_daemon(tmp_path, max_request_bytes=16)
    try:
        for raw_length in (None, "wat", "-1"):
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            header = "" if raw_length is None else f"Content-Length: {raw_length}\r\n"
            sock.sendall(f"POST /jobs HTTP/1.1\r\nHost: localhost\r\n{header}Connection: close\r\n\r\n".encode())
            response = sock.makefile("rb").read()
            sock.close()
            assert b" 400 " in response
            assert b'"result": "error"' in response

        status, payload = request(port, "POST", "/jobs", b"x" * 17, {"Content-Length": "17"})
        assert status == 413
        assert payload["result"] == "error"

        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        sock.sendall(b"POST /jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{}")
        sock.shutdown(socket.SHUT_WR)
        response = sock.makefile("rb").read()
        sock.close()
        assert b" 400 " in response
        assert b"shorter than Content-Length" in response
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_route_conversion_errors_return_json_instead_of_reset(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        status, payload = request(port, "GET", "/jobs?limit=" + "9" * 100)
        assert status == 400
        assert payload["result"] == "error"
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_concurrent_first_requests_initialize_one_manager(monkeypatch):
    created = []

    def make_manager():
        time.sleep(0.02)
        created.append(object())
        return created[0]

    monkeypatch.setattr(daemon, "manager", None)
    monkeypatch.setattr(daemon, "JobManager", make_manager)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        managers = list(executor.map(lambda _: daemon.get_manager(), range(20)))

    assert len(created) == 1
    assert all(manager is created[0] for manager in managers)


def test_authenticated_routes_and_home_token_isolation(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        token = (tmp_path / "state" / "token").read_text(encoding="utf-8")
        assert request(port, "GET", "/jobs")[0] == 401
        assert request(port, "GET", "/jobs", headers={"Authorization": "Bearer wrong"})[0] == 401
        status, payload = request(port, "GET", "/jobs", headers={"Authorization": f"Bearer {token}"})
        assert status == 200 and payload == {"jobs": []}
    finally:
        stop_daemon(proc)


def test_agent_bg_home_alias_cannot_bypass_daemon_lock(tmp_path):
    home = tmp_path / "state"
    first, port = start_daemon(tmp_path)
    second_port = free_port()
    env = {key: value for key, value in os.environ.items() if key not in {"VANTH_HOME", "AGENT_BG_HOME"}}
    env.update({"AGENT_BG_HOME": str(home), "VANTH_DAEMON_PORT": str(second_port)})
    second = subprocess.Popen([sys.executable, "-m", "vanth.daemon"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        assert second.wait(timeout=5) != 0
        assert "already owns" in second.stderr.read().decode(errors="replace")
    finally:
        if first.poll() is None:
            stop_daemon(first)


def test_tracking_server_counts_request_before_thread_entry():
    server = daemon.TrackingHTTPServer(("127.0.0.1", 0), daemon.Handler)
    entered = threading.Event()
    release = threading.Event()

    def paused_finish(request, client_address):
        entered.set()
        release.wait(timeout=2)

    server.finish_request = paused_finish
    request_socket = socket.socket()
    try:
        server.process_request(request_socket, ("127.0.0.1", 0))
        assert entered.wait(timeout=1)
        assert not server.wait_for_requests(0.05)
        release.set()
        assert server.wait_for_requests(2)
    finally:
        request_socket.close()
        server.server_close()


def test_shutdown_returns_controlled_result_to_active_wait(tmp_path):
    proc, port = start_daemon(tmp_path)
    token = (tmp_path / "state" / "token").read_text(encoding="utf-8")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        command = shellcmd.join([sys.executable, "-c", "import time; time.sleep(30)"])
        status, started = request(port, "POST", "/jobs", json.dumps({"command": command}).encode(), headers)
        assert status == 200
        result = []

        def wait_for_job():
            result.append(request(port, "POST", f"/jobs/{started['job_id']}/wait", json.dumps({"filters": ["completed"], "timeout_seconds": 30}).encode(), headers))

        thread = threading.Thread(target=wait_for_job)
        thread.start()
        time.sleep(0.2)
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=5)
        thread.join(timeout=5)
        assert result and result[0][1]["result"] == "shutdown"
    finally:
        if proc.poll() is None:
            stop_daemon(proc)


def test_home_permissions_are_owner_only(tmp_path):
    from vanth.paths import secure_home_permissions

    home = tmp_path / "state"
    home.mkdir(parents=True)
    (home / "token").write_text("abc", encoding="utf-8")
    secure_home_permissions(home)

    if os.name == "nt":
        def ace_identities(path):
            result = subprocess.run(
                ["icacls", str(path)],
                capture_output=True,
                text=True,
                timeout=15,
            )
            return result.stdout

        acl = ace_identities(home / "token")
        # The token must not be readable by BUILTIN\Users or the Everyone /
        # sandbox-style group. Only the owner, SYSTEM, and Administrators are
        # allowed (their SIDs appear in the explicit grant). The first ACE
        # shares the filename header line, so scan every line.
        lines = [line for line in acl.splitlines() if line.strip()]
        assert not any("BUILTIN\\Users" in line for line in lines)
        assert not any("Everyone" in line for line in lines)
        # Inheritance must be disabled on the token (explicit ACEs, not (I)).
        assert not any("(I)" in line for line in lines)
        # Owner / SYSTEM / Administrators retain full control.
        assert any("SYSTEM" in line and "(F)" in line for line in lines)
        assert any("Administrators" in line and "(F)" in line for line in lines)
    else:
        mode = stat.S_IMODE(os.stat(home / "token").st_mode)
        assert mode & 0o077 == 0  # no group/other access
        home_mode = stat.S_IMODE(os.stat(home).st_mode)
        assert home_mode & 0o077 == 0


def test_secure_home_permissions_idempotent(tmp_path):
    from vanth.paths import secure_home_permissions

    home = tmp_path / "state"
    home.mkdir(parents=True)
    (home / "token").write_text("abc", encoding="utf-8")
    secure_home_permissions(home)
    secure_home_permissions(home)  # second run must not raise
    if os.name != "nt":
        assert stat.S_IMODE(os.stat(home).st_mode) & 0o077 == 0


def test_reuse_address_disabled_on_windows(tmp_path):
    if os.name != "nt":
        return  # only Windows has the phantom-listener problem
    assert daemon.TrackingHTTPServer.allow_reuse_address is False


def test_bind_failure_releases_lock_and_exits_cleanly(tmp_path):
    import socket as _socket

    home = tmp_path / "state"
    with _socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.listen(1)
        env = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(port)}
        proc = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate(timeout=10)
        assert proc.returncode == 1
        assert b"cannot bind" in stderr
        assert b"already owns" not in stderr
    # The OS lock must not be left held: a fresh daemon on a free port must be
    # able to start (proving the lock was released on the failed bind). The
    # lock file itself may linger; only the OS-level lock matters.
    second_port = free_port()
    env2 = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(second_port)}
    proc2 = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env2,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                status, _ = request(second_port, "GET", "/health")
                if status == 200:
                    break
            except OSError:
                time.sleep(0.05)
        else:
            raise AssertionError("lock was still held after failed bind")
    finally:
        if proc2.poll() is None:
            proc2.terminate()
            proc2.wait(timeout=5)


def test_signal_handler_accepts_signum_frame():
    """Unix signal handlers receive (signum, frame); _stop_httpd must accept both.

    Regression: registering _stop_httpd with signal.signal then receiving a real
    signal raised TypeError on Linux ("takes 0 positional arguments but 2 were
    given"), which crashed daemon shutdown and leaked a traceback into stderr.
    """
    daemon._stop_httpd(15, None)
    daemon.shutdown_event.clear()
    daemon._stop_httpd()


def test_daemon_writes_a_startup_log_line(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        log_path = tmp_path / "state" / "logs" / "daemon.log"
        deadline = time.monotonic() + 5
        text = ""
        while time.monotonic() < deadline:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            if "vanthd started" in text:
                break
            time.sleep(0.05)
        assert "vanthd started" in text
    finally:
        stop_daemon(proc)
