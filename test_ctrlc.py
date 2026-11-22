#!/usr/bin/env python3
"""test_ctrlc.py - prove an accidental Ctrl+C cannot kill a live session.

Starts cas_logger.py against the stub feed, sends one SIGINT while it is recording, and requires the
process to survive; then sends two presses and requires a clean, flushed shutdown.

    .venv/bin/python test_ctrlc.py

Skips itself on Windows (Python cannot raise SIGINT into another process there); the behaviour is
still covered by the unit check in verify.py.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

OUT_DIR = Path("_ctrlc_test")
STUB_URL = "http://127.0.0.1:8771/openapi/typea/instruments/scriptmaster"


def ensure_stub():
    """Start the stub feed unless something already answers on 8771."""
    import urllib.request
    try:
        urllib.request.urlopen(STUB_URL, timeout=2)
        return None
    except Exception:
        proc = subprocess.Popen([sys.executable, "stub_api.py"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2.5)
        return proc


def main() -> int:
    if os.name == "nt":
        print("skipped on Windows: cannot deliver SIGINT to another process")
        return 0
    if OUT_DIR.exists():
        import shutil
        shutil.rmtree(OUT_DIR, ignore_errors=True)

    stub = ensure_stub()
    env = {**os.environ,
           "MSTOCK_API_KEY": "fake",
           "MSTOCK_ACCESS_TOKEN": "fake",
           "MSTOCK_BASE_URL": "http://127.0.0.1:8771/openapi/typea",
           "MSTOCK_WS_URL": "ws://127.0.0.1:9"}

    p = subprocess.Popen(
        [sys.executable, "cas_logger.py", "--test", "90", "--indices", "NIFTY",
         "--stocks", "HDFCBANK", "--no-open", "--no-dashboard", "--out", str(OUT_DIR)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

    lines: list[str] = []
    started = threading.Event()

    def reader():
        for line in p.stdout:  # type: ignore[union-attr]
            lines.append(line.rstrip())
            if "RECORDING STARTED" in line:
                started.set()

    threading.Thread(target=reader, daemon=True).start()

    if not started.wait(60):
        print("FAIL: never reached RECORDING STARTED")
        print("\n".join(lines[-15:]))
        p.kill()
        return 1

    time.sleep(2)                                   # be genuinely mid-window
    print("sending one Ctrl-C (the accident)")
    p.send_signal(signal.SIGINT)
    time.sleep(4)

    survived = p.poll() is None
    ignored = any("Ctrl-C ignored" in ln for ln in lines)

    print("sending SIGHUP (terminal closed)")
    huhp_ok = True
    if hasattr(signal, "SIGHUP"):
        p.send_signal(signal.SIGHUP)
        time.sleep(2.5)
        huhp_ok = p.poll() is None
        lines.append(f"__sighup_survived={huhp_ok}")

    print("sending SIGTERM (deliberate stop)")
    if hasattr(signal, "SIGTERM"):
        p.send_signal(signal.SIGTERM)
    else:
        p.terminate()

    try:
        rc = p.wait(timeout=45)
    except subprocess.TimeoutExpired:
        p.kill()
        print("FAIL: did not exit after the stop signal")
        print("\n".join(lines[-15:]))
        return 1

    # the ws_raw file also matches cas_*.jsonl — pick the session log explicitly
    log = next((f for f in OUT_DIR.glob("*/cas_*.jsonl") if "ws_raw" not in f.name), None)
    ended = closed = False
    if log:
        text = log.read_text(encoding="utf-8", errors="ignore")
        ended = '"name":"end"' in text or '"name": "end"' in text
        closed = text.rstrip().endswith("}")

    sighup = any("__sighup_survived=True" in ln for ln in lines)
    ok = survived and ignored and sighup and rc == 0 and ended
    print(f"  survived the first press : {survived}")
    print(f"  logged the ignored press : {ignored}")
    print(f"  survived the SIGHUP      : {sighup}")
    print(f"  exit code                : {rc}")
    print(f"  log has an 'end' event   : {ended}")
    print(f"  log ends on a whole line : {closed}")
    print("PASS" if ok else "FAIL")
    if not ok:
        print("\n".join(lines[-20:]))
    if stub:
        stub.terminate()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
