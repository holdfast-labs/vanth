"""Opt-in real SSH validation against an explicitly configured disposable host."""
from __future__ import annotations

import hashlib
import json
import os
import shlex
import sqlite3
import tempfile
import time
from pathlib import Path

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations
from vanth.remote import ssh
from vanth.remote.control import DefaultSessionTransport, RemoteControl
from vanth.remote.pairing import _target_argv, pair_remote, remove_remote
from vanth.remote.protocol import decode_frame
from vanth.remote.store import RemoteStore
from vanth.remote.transfer import RemoteArtifactBroker


ENV_KEYS = ("VANTH_SMOKE_SSH_TARGET", "VANTH_SMOKE_SSH_FINGERPRINT",
            "VANTH_SMOKE_SSH_PYTHON", "VANTH_SMOKE_SSH_HELPER")


class DropAcceptedResponse(DefaultSessionTransport):
    """Deliver through real SSH, then simulate losing the acceptance reply."""
    accepted_job_id: str | None = None

    def open_session(self, remote_row, *, home):
        session = super().open_session(remote_row, home=home)
        outer = self

        class Session:
            def exchange(self, frame_bytes):
                response = session.exchange(frame_bytes)
                request = decode_frame(frame_bytes.decode())
                if request.get("method") == "job.start" and outer.accepted_job_id is None:
                    frame = decode_frame(response) if response else {}
                    if frame.get("kind") != "response":
                        raise RuntimeError("start was not accepted before response-loss injection")
                    outer.accepted_job_id = frame["result"]["job_id"]
                    return None
                return response
        return Session()


def run_live() -> dict:
    missing = [key for key in ENV_KEYS if not os.environ.get(key)]
    if missing:
        if os.environ.get(ENV_KEYS[0]):
            raise ValueError("Configured target requires: " + ", ".join(missing))
        return {"result": "skipped", "reason": "No VANTH_SMOKE_SSH_TARGET configured"}
    target, fingerprint, python, helper = [os.environ[key] for key in ENV_KEYS]
    info = ssh.parse_target(target)
    if not python.startswith("/") or not helper.startswith("/"):
        raise ValueError("Remote Python and helper must be absolute executable paths")
    with tempfile.TemporaryDirectory(prefix="vanth-ssh-smoke-") as local:
        home = Path(local)
        keys = ssh.fetch_host_keys(info["hostname"], info["port"])
        if fingerprint not in ssh.host_key_fingerprints(keys):
            raise RuntimeError("SSH host fingerprint mismatch")
        known = home / "known_hosts"
        known.write_text("\n".join(keys) + "\n", encoding="utf-8")
        config = ssh.allowlist_config(hostname=info["hostname"], user=info["user"],
                                      port=info["port"], identity_file="", known_hosts=str(known),
                                      include_identity=False)

        def remote(code):
            result = ssh.run_ssh([*_target_argv(info), shlex.join([python, "-c", code])],
                                 config_dir=home / "bootstrap", config=config, timeout=45)
            if result.returncode:
                raise RuntimeError("Disposable remote operation failed: " + result.stderr.decode(errors="replace"))
            return json.loads(result.stdout)

        remote_home = None
        store = None
        ops = None
        remote_id = None
        cleanup_errors = []
        try:
            # Only this new directory and daemon belong to the smoke test.
            created = remote("""import json,os,pathlib,socket,subprocess,tempfile,time
h=pathlib.Path(tempfile.mkdtemp(prefix='vanth-ssh-smoke-'))
s=socket.socket(); s.bind(('127.0.0.1',0)); port=s.getsockname()[1]; s.close()
env={**os.environ,'VANTH_HOME':str(h),'VANTH_DAEMON_PORT':str(port),'VANTH_REMOTE_WAKE_SYNC_SECONDS':'0'}
with (h/'smoke-daemon.log').open('wb') as log:
 p=subprocess.Popen([__import__('sys').executable,'-m','vanth.daemon'],env=env,stdout=log,stderr=log,start_new_session=True)
(h/'smoke-owner.json').write_text(json.dumps({'pid':p.pid,'home':str(h)}))
print(json.dumps({'home':str(h),'pid':p.pid}))
""")
            remote_home = created["home"]
            ready = remote(f"""import json,pathlib,time
h=pathlib.Path({remote_home!r})
for attempt in range(150):
 if (h/'daemon.json').exists(): break
 time.sleep(.1)
print(json.dumps({{'ready':(h/'daemon.json').exists()}}))
""")
            assert ready["ready"], "remote isolated daemon failed to start"
            db = sqlite3.connect(home / "controller.sqlite")
            db.row_factory = sqlite3.Row
            store = RemoteStore(db)
            paired = pair_remote(target=target, home=home, store=store,
                                 host_fingerprint=fingerprint, helper_command=helper,
                                 remote_home=remote_home)
            remote_id = paired["remote_id"]
            binding = {"expected_state_epoch": paired["state_epoch"],
                       "expected_instance_id": paired["instance_id"]}
            transport = DropAcceptedResponse()
            control = RemoteControl(store, home=home, transport=transport)
            code = "from pathlib import Path; p=Path('executions'); p.open('a').write('once\\n'); print('ssh-smoke-completed')"
            payload = {"command": shlex.join([python, "-c", code]), "cwd": remote_home,
                       "timeout_seconds": 20}
            request = control.submit(remote_id, "job.start", payload,
                                     idempotency_key="smoke-start-replay", **binding)
            lost = control.run_request(remote_id, request)
            assert lost["status"] == "submitting", lost
            # A new transport opens a fresh SSH connection with the original key.
            control = RemoteControl(store, home=home)
            replay = control.run_request(remote_id, control.replay(remote_id, "smoke-start-replay"))
            assert replay["status"] == "completed", replay
            job_id = replay["response"]["job_id"]
            assert job_id == transport.accepted_job_id, "replay launched a different job"
            deadline = time.monotonic() + 40
            status = None
            while time.monotonic() < deadline:
                result = control.status(remote_id, job_id, idempotency_key="status-" + str(time.monotonic_ns()), **binding)
                status = (result.get("response") or {}).get("status")
                if status in {"completed", "failed", "timeout", "cancelled", "orphaned"}:
                    break
                time.sleep(.2)
            assert status == "completed", status
            executions = remote(f"import json,pathlib; print(json.dumps(pathlib.Path({remote_home!r},'executions').read_text()))")
            assert executions == "once\n", "lost-response replay executed twice"
            catalog = open_catalog(home)
            ops = ArtifactOperations(catalog, LocalBlobStore(default_store_root(home), catalog))
            data = bytes(range(256)) * 1024
            version = ops.put_file("ssh-smoke.bin", data=data, idempotency_key="smoke-source-artifact")
            broker = RemoteArtifactBroker(control, ops)
            pushed = broker.push_blob(remote_id, version["version_id"], idempotency_key="smoke-push-artifact")
            assert pushed["completed"], pushed
            dest = home / "received.bin"
            pulled = broker.pull_blob(remote_id, pushed["version_id"], dest, idempotency_key="smoke-pull-artifact")
            assert pulled["completed"] and dest.read_bytes() == data, pulled
            outcome = {"result": "passed", "job_id": job_id, "replayed_same_job": True,
                       "execution_count": 1, "artifact_sha256": hashlib.sha256(data).hexdigest(),
                       "response_loss": "discarded after real SSH acceptance; fresh connection replay"}
        finally:
            if store is not None:
                for row in store.list_remotes():
                    result = remove_remote(home=home, remote_id=row["remote_id"], store=store)
                    if result.get("result") == "error":
                        cleanup_errors.append(result["error"])
                store.db.close()
            if ops is not None:
                ops.catalog.db.close()
            if remote_home:
                try:
                    remote(f"""import json,os,pathlib,shutil,sqlite3,tempfile,time,urllib.request
h=pathlib.Path({remote_home!r})
owner=json.loads((h/'smoke-owner.json').read_text())
assert owner['home']==str(h) and h.parent==pathlib.Path(tempfile.gettempdir()) and h.name.startswith('vanth-ssh-smoke-')
if (h/'daemon.json').exists():
 meta=json.loads((h/'daemon.json').read_text())
 headers={{'Authorization':'Bearer '+(h/'token').read_text().strip(),'Content-Type':'application/json'}}
 if (h/'jobs.sqlite').exists():
  db=sqlite3.connect(h/'jobs.sqlite')
  active=db.execute("SELECT job_id FROM jobs WHERE status IN ('running','launching','queued','retrying','paused','stopping')").fetchall(); db.close()
  for (jid,) in active:
   req=urllib.request.Request(meta['url']+'/jobs/'+jid+'/stop',data=b'{{"kill_after_seconds":1}}',headers=headers)
   urllib.request.urlopen(req,timeout=15).close()
 req=urllib.request.Request(meta['url']+'/shutdown',data=b'{{}}',headers=headers)
 urllib.request.urlopen(req,timeout=15).close()
for attempt in range(100):
 if not (h/'daemon.json').exists(): break
 time.sleep(.1)
assert not (h/'daemon.json').exists(), 'daemon did not stop; retaining smoke directory'
shutil.rmtree(h)
print(json.dumps({{'cleaned':True}}))
""")
                except Exception as exc:
                    cleanup_errors.append(str(exc))
            if cleanup_errors:
                raise RuntimeError("Smoke cleanup failed: " + "; ".join(cleanup_errors))
        return outcome


if __name__ == "__main__":
    print(json.dumps(run_live(), indent=2))
