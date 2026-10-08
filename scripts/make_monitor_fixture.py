"""Create a real progress/metric dataset for the Go monitor test."""
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time

from vanth.server import JobManager


def cmd(code: str) -> str:
    argv = [sys.executable, "-c", code]
    if sys.platform == "win32":
        return subprocess.list2cmdline(argv)
    return shlex.join(argv)


def main():
    home = tempfile.mkdtemp(prefix="vanth-monitor-e2e-")
    manager = JobManager(home)
    with open(os.path.join(os.path.dirname(__file__), "..", ".monitor_e2e_home"), "w") as f:
        f.write(home)
    code = (
        "import json,time;"
        "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':100,'percent':i}}), flush=True),"
        "print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'_step':i,'loss':0.5-i*0.004,'acc':i*0.009}}), flush=True),"
        "time.sleep(0.01));"
        "[f(i) for i in range(1,101)]"
    )
    job_id = manager.start(cmd(code), name="training run")["job_id"]
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = manager.status(job_id)["status"]
        if status == "completed":
            break
        time.sleep(0.1)
    assert manager.status(job_id)["status"] == "completed"
    events = manager.events(job_id, limit=500)["events"]
    types = {}
    for ev in events:
        types[ev["type"]] = types.get(ev["type"], 0) + 1
    print("HOME=" + home)
    print("JOB=" + job_id)
    print("event_counts=" + json.dumps(types))
    print("sample=" + json.dumps(events[2]))
    manager.close()


if __name__ == "__main__":
    main()
