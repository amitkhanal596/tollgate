"""Make the bench modules importable from the tests (no package install needed)."""
import os
import sys

BENCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for sub in ("", "chaos"):
    path = os.path.join(BENCH, sub)
    if path not in sys.path:
        sys.path.insert(0, path)


def k6_line(msg):
    """A k6 --console-output line as the analysis scripts parse them."""
    return f'time="2026-11-16T00:00:00Z" level=info msg="{msg}" source=console\n'
