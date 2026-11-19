"""Demo: downloads files with a throughput metric."""
import math
import random

from _events import duration_scale, emit, progress, steps

scale = duration_scale()
files = 4000
for step in steps(0.5 * scale):
    done = min(step * 12, files)
    emit("metric", _step=step, mb_s=round(24 + 6 * math.sin(step / 9) + random.uniform(-2, 2), 2))
    progress(done, files, "files", "download")
    if done >= files:
        break
