"""Demo: a model fine-tune that declares its own monitor panels."""
import math
import random

from _events import duration_scale, emit, progress, steps

scale = duration_scale()
total, epochs = 400, 5
emit("panels", panels=[
    {"kind": "line", "metric": "loss", "label": "train/loss"},
    {"kind": "line", "metric": "acc", "label": "accuracy"},
    {"kind": "gauge", "metric": "gpu", "label": "gpu util", "unit": "%"},
    {"kind": "line", "metric": "gpu_mem", "label": "gpu mem", "unit": "GB", "min": 0, "max": 80},
    {"kind": "stat", "metric": "tokens_s", "label": "tokens/s"},
    {"kind": "line", "metric": "lr", "label": "learning rate"},
    {"kind": "text", "key": "phase", "label": "phase"},
])
for step in steps(0.6 * scale):
    u = step / total
    lr = 3e-4 * u / 0.05 if u < 0.05 else 3e-4 * (0.5 + 0.5 * math.cos(math.pi * (u - 0.05) / 0.95))
    phase = f"epoch {min(int(u * epochs) + 1, epochs)}/{epochs}"
    emit("metric", _step=step,
         loss=round(2.3 * math.exp(-u * 4.2) + 0.19 + random.uniform(-0.06, 0.06) * (1 - u * 0.85), 4),
         acc=round(0.914 - 0.52 * math.exp(-u * 3.5) + random.uniform(-0.01, 0.01), 4),
         gpu=round(92 + random.uniform(-6, 6), 1), gpu_mem=round(min(61, 20 + u * 200), 1),
         tokens_s=round(1930 + random.uniform(-90, 90) + 120 * math.sin(u * 30)), lr=lr, phase=phase)
    progress(step, total, "step", "train", f"step {step}/{total}")
    if step >= total:
        break
emit("checkpoint", "saved checkpoint final.pt")
