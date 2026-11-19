"""Demo: an evaluation that hangs for a while partway (a stall), then finishes."""
import random
import time

from _events import duration_scale, emit, progress, steps

scale = duration_scale()
cases = 1200
hung = False
for step in steps(0.5 * scale):
    done = step * 6
    if done >= cases * 0.4 and not hung:
        hung = True
        time.sleep(150 * scale)  # no output at all: the monitor flags it as stalled
    emit("metric", _step=step, acc=round(0.86 + random.uniform(-0.015, 0.015), 4),
         gpu=round(71 + random.uniform(-7, 7), 1), gpu_mem=round(min(38, 10 + step * 0.4), 1),
         cases_min=round(720 / scale + random.uniform(-40, 40)))
    progress(min(done, cases), cases, "cases", "eval")
    if done >= cases:
        break
emit("checkpoint", "eval report written")
