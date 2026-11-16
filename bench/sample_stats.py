#!/usr/bin/env python3
"""Sample `docker stats` CPU for the bench containers until SIGTERM.

  sample_stats.py OUT.jsonl CLOCK_CONTAINER

One JSON line per sample: {"host_start_ms", "host_end_ms", "cpu": {container: percent_of_one_core}}.
`docker stats` runs on the host, so the timestamps are host-clock. The k6
output and the watchdog use the Docker VM clock, so the first and last lines
are {"clock": {"host_ms", "vm_ms"}} pairs measured by asking CLOCK_CONTAINER
(the stall watchdog, which runs the whole time) for the VM time; summarize.py
uses them to map VM-clock step windows onto these samples. Accuracy is roughly
the latency of one `docker exec` (tens of ms), far below the multi-second
sample spacing.

A sample takes 1-2 s (docker stats needs two readings to compute CPU), which
is why per-step CPU is an average over a few samples. Standard library only.
"""
import json
import signal
import subprocess
import sys
import time

stop = False


def on_term(*_):
    global stop
    stop = True


def run(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout


def clock_pair(container):
    t0 = time.time() * 1000
    out = run(["docker", "exec", container, "/helper", "time"], 10).strip()
    t1 = time.time() * 1000
    try:
        return {"clock": {"host_ms": (t0 + t1) / 2, "vm_ms": int(out), "rtt_ms": t1 - t0}}
    except ValueError:
        return None


def main(path, clock_container):
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    with open(path, "w") as fh:
        c = clock_pair(clock_container)
        if c:
            fh.write(json.dumps(c) + "\n")
        while not stop:
            names = run(["docker", "ps", "--filter", "name=tollgate-bench-", "--format", "{{.Names}}"]).split()
            if not names:
                time.sleep(1)
                continue
            t0 = time.time() * 1000
            out = run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"] + names, 60)
            t1 = time.time() * 1000
            cpu = {}
            for line in out.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1].endswith("%"):
                    try:
                        cpu[parts[0]] = float(parts[1][:-1])
                    except ValueError:
                        pass
            fh.write(json.dumps({"host_start_ms": t0, "host_end_ms": t1, "cpu": cpu}) + "\n")
            fh.flush()
        c = clock_pair(clock_container)
        if c:
            fh.write(json.dumps(c) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    main(*sys.argv[1:])
