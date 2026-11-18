"""Demo: a deploy that fails with exit 1 after a while."""
import sys

from _events import duration_scale, emit, progress, steps

scale = duration_scale()
for step in steps(1.0 * scale):
    progress(step, 60, "checks", "rollout")
    if step == 40:
        emit("log", "health check failed: 3/5 pods not ready")
        print("error: rollout aborted", file=sys.stderr)
        sys.exit(1)
