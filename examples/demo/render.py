"""Demo: a render that waits in the GPU pool behind the training job."""
from _events import duration_scale, progress, steps

scale = duration_scale()
frames = 120
for step in steps(0.5 * scale):
    progress(step, frames, "frames", "render")
    if step >= frames:
        break
