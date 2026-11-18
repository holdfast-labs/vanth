"""Tiny AGENT_EVENT helpers for the demo jobs (no vanth import needed).

Every demo job takes ``--duration SCALE`` (1.0 is a few minutes) and prints
one JSON event per line, which Vanth parses from stdout.
"""
import argparse
import json
import time


def emit(event_type, message=None, **data):
    payload = {"type": event_type, "data": data}
    if message is not None:
        payload["message"] = message
    print("AGENT_EVENT " + json.dumps(payload, separators=(",", ":")), flush=True)


def progress(current, total, unit, stage, message=None):
    emit("progress", message, current=current, total=total, unit=unit, stage=stage,
         percent=round(current / total * 100, 2))


def duration_scale():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=1.0, help="time scale; 1.0 is a few minutes")
    return max(parser.parse_args().duration, 0.01)


def steps(seconds_per_step):
    """Yield 1, 2, 3, ... with a pause before each step."""
    step = 0
    while True:
        time.sleep(seconds_per_step)
        step += 1
        yield step
