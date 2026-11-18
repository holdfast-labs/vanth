"""A job that declares its own monitor panels.

Run it under vanth (e.g. `vanth start --name panels-demo -- python examples/panels_job.py`
or the `job_start` MCP tool) and open `vanth-monitor`: its card shows a GPU
gauge, a loss chart, a big tokens/s number and the current phase.
"""
import math
import random
import time

from vanth.agent_events import agent_event, progress

# One line declares the view; the panels read the metric events below.
agent_event("panels", panels=[
    {"kind": "gauge", "metric": "gpu", "label": "GPU", "unit": "%"},
    {"kind": "line", "metric": "loss"},
    {"kind": "stat", "metric": "tokens_s", "label": "tokens/s"},
    {"kind": "text", "key": "phase"},
])

steps = 60
for step in range(1, steps + 1):
    time.sleep(0.5)
    phase = "warmup" if step <= 10 else "train"
    agent_event("metric", _step=step, loss=round(2.0 * math.exp(-step / 20) + 0.1, 4),
                gpu=random.randint(60, 95), tokens_s=random.randint(1500, 2100), phase=phase)
    progress(step, steps, unit="step", stage=phase, message=f"step {step}/{steps}")
