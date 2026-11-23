#!/usr/bin/env python3
"""
cas_logger.py - mStock Closing Auction Session (CAS) data logger
================================================================

PURPOSE
    Record what the mStock Type A API actually exposes during the NSE/BSE
    Closing Auction Session (CAS) so you can answer ONE question with data:

        "Does the feed carry any auction information (indicative price,
         buy/sell imbalance) before the random close at 3:28-3:30 PM, or only
         frozen stock prices + continuously-trading Futures?"

    It does NOT trade, does NOT place orders, and does NOT predict anything.
    It records raw data first. A signal/dashboard comes after you have seen
    real data.

WHAT IT RECORDS (IST, per expiry day)
    15:10:00  connect WebSocket (full mode), subscribe universe
    15:14:50  snapshot  (REST OHLC + latest WS tick of every instrument)
    15:15:10  snapshot  <- frozen 3:15 baseline (cash market paused)
    15:19:50  snapshot  <- just before auction order entry opens
    15:20:00  START recording every WS packet + REST poll; live readout every 3 s
    15:27:15  FINAL READOUT per index (--decide-at): futures move since the 3:15
              baseline, implied spot target, nearest strike that would be ITM at
              that target, its ltp/ask vs value at target. Saved as 'readout'.
              Random close can start at 15:28:00, so place orders BEFORE that.
    15:27:30  STOP  recording      (use --rec-end 15:32:00 to also catch the jump)
    15:31:00  snapshot  <- post-close: actual spot / futures / option prices
    15:35:30  snapshot  <- CAS finished

OUTPUT (./cas_logs/YYYY-MM-DD/)
    cas_YYYY-MM-DD.jsonl          main log: meta, events, snapshots, rest polls,
                                  parsed ws ticks (one JSON object per line)
    cas_YYYY-MM-DD.ws_raw.jsonl   every raw WS frame (base64) so you can
                                  re-parse offline if my packet layout guess
                                  (big-endian, price in paise) is wrong
    scriptmaster_YYYY-MM-DD.csv   cached instrument master

USAGE
    pip install requests websocket-client pyotp
    cp .env.example .env   # fill in credentials (see README.md)

    python cas_logger.py --find "NIFTY 50"          # check index symbol names
    python cas_logger.py --test 120                 # 2-min live test, any time
                                                    # market is open
    python cas_logger.py                            # real run (start ~15:05)
    python cas_logger.py --summarize cas_logs/2026-11-23/cas_2026-11-23.jsonl

RESEARCH REFERENCES (what this script is built on)
    [1] mStock Type A WebSocket (binary frames, modes ltp/quote/full, LOGIN:
        message, quote/index packet layouts):
        https://tradingapi.mstock.com/docs/v1/typeA/market-data/
    [2] mStock Type A REST (ohlc / ltp / scriptmaster; NO /quote endpoint in
        Type A; auth header "token api_key:access_token"):
        https://tradingapi.mstock.com/docs/v1/typeA/market-quote-and-instrument
    [3] mStock auth (connect/login, session/verifytotp, token valid same day):
        https://tradingapi.mstock.com/docs/v1/typeA/User/
    [4] mStock rate limits (Quote APIs 20/sec) and 12h/same-day token expiry:
        https://tradingapi.mstock.com/docs/v1/Introduction/
    [5] NSE CAS: session timings, and the exchange DISSEMINATES indicative
        equilibrium price, indicative tradable qty, cumulative buy/sell qty and
        imbalance qty during CAS (on NSE's own terminal):
        https://www.nseindia.com/static/products-services/closing-auction-session
        SEBI circular: https://www.sebi.gov.in/sebi_data/attachdocs/jan-2026/1768576287344.pdf
    [6] Timeline: 15:15-15:20 reference price / transition, 15:20-15:25 market
        + limit orders, 15:25-15:30 limit only, random close 15:28-15:30.

KNOWN UNKNOWNS (this script exists to settle them)
    * Whether mStock forwards NSE's indicative equilibrium price at all. The
      documented WS packet has NO explicit IEP field. Total Buy/Sell Qty and
      5-level depth MIGHT reflect auction data - the log will show.
    * Price scaling of WS integers (assumed paise, /100). Auto-checked against
      REST at runtime and recorded in the meta record.
    * Byte order (assumed big-endian, like Kite). Raw frames are kept, so you
      can re-parse. Use --little-endian if parsed prices look absurd.
    * Index spot token names in the scriptmaster (resolved by alias match;
      use --find to verify, or set TOKEN_OVERRIDES in a JSON file via
      --overrides).
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import atexit
import shutil
import signal
import math
import statistics
import struct
import sys
import threading
import time
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

VERSION = "1.0"
def resolve_ist():
    """India Standard Time, with a fallback that works on Windows.

    Windows ships no IANA tz database, so ZoneInfo("Asia/Kolkata") raises unless the `tzdata`
    package is installed. IST has no daylight saving, so a fixed +05:30 offset is exactly correct."""
    try:
        return ZoneInfo("Asia/Kolkata")
    except Exception:  # noqa: BLE001 - ZoneInfoNotFoundError, ImportError, missing tzdata
        return timezone(timedelta(hours=5, minutes=30), "IST")


IST = resolve_ist()

DEFAULT_BASE_URL = "https://api.mstock.trade/openapi/typea"
DEFAULT_WS_URL = "wss://ws.mstock.trade"
BASE_URL = os.environ.get("MSTOCK_BASE_URL", DEFAULT_BASE_URL)
WS_URL = os.environ.get("MSTOCK_WS_URL", DEFAULT_WS_URL)


_WRITERS = {"out": None, "raw": None, "done": False, "path": None}


def final_flush(reason: str = "atexit") -> None:
    """Last-resort flush, safe to call from atexit, a crash hook, or the normal wrap-up."""
    if _WRITERS["done"]:
        return
    _WRITERS["done"] = True
    out = _WRITERS["out"]
    try:
        if out is not None and not getattr(out, "closed", False):
            out.write({"type": "event", "t": ts(), "name": "final_flush", "reason": reason})
            out.flush()
            out.close()
    except Exception:  # noqa: BLE001
        pass
    raw = _WRITERS["raw"]
    try:
        if raw is not None and not getattr(raw, "closed", False):
            raw.flush()
            raw.close()
    except Exception:  # noqa: BLE001
        pass


def install_crash_hook(out_dir: Path) -> None:
    """A traceback on stderr scrolls away; write it next to the log so a crash is diagnosable."""
    def hook(exc_type, exc, tb):
        import traceback
        text = "".join(traceback.format_exception(exc_type, exc, tb))
        try:
            (out_dir / f"crash_{now():%Y%m%d_%H%M%S}.txt").write_text(text, encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
        final_flush("crash")
        sys.__excepthook__(exc_type, exc, tb)
    sys.excepthook = hook


def require_disk(path: Path, need_mb: int = 200) -> None:
    """A full disk mid-window would truncate the log silently. Refuse to start instead."""
    free_mb = shutil.disk_usage(path).free / 1_000_000
    if free_mb < need_mb:
        sys.exit(f"only {free_mb:.0f} MB free under {path} — need {need_mb} MB for a session")


class Heartbeat:
    """Writes a heartbeat every 30s so a truncated log is obvious after the fact."""

    def __init__(self, out, interval: float = 30.0):
        self.out, self.interval = out, interval
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="heartbeat")

    def _loop(self):
        while not self.stop_flag.wait(self.interval):
            try:
                self.out.write({"type": "event", "t": ts(), "name": "heartbeat",
                                "recording": not STOP.is_set()})
            except Exception:  # noqa: BLE001
                return

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_flag.set()


def refresh_endpoints() -> None:
    """Re-read the endpoints after .env is loaded.

    These are module-level constants read at import time, and .env is only parsed inside main(),
    so without this an MSTOCK_BASE_URL / MSTOCK_WS_URL in .env is silently ignored — the run goes
    to production while you think it is pointed at a stub."""
    global BASE_URL, WS_URL
    BASE_URL = os.environ.get("MSTOCK_BASE_URL", DEFAULT_BASE_URL)
    WS_URL = os.environ.get("MSTOCK_WS_URL", DEFAULT_WS_URL)

STOP = threading.Event()

# Ctrl+C policy: while the recording window is open, a single press is ignored (an accidental
# Ctrl+C must not cost the session). Pressing again inside SIGINT_CONFIRM_S seconds really stops.
SIGINT_CONFIRM_S = 5.0
_SIGINT = {"armed": False, "last": 0.0, "ignored": 0}
_ALLOW_SINGLE = {"v": False}
_SIGINT_EVENT = {"fn": None}


def _note_event(payload: dict) -> None:
    """Write a diagnostic event from a signal handler; never raise out of a handler."""
    fn = _SIGINT_EVENT.get("fn")
    if not fn:
        return
    try:
        fn(payload)
    except Exception:  # noqa: BLE001
        pass


def sigint_should_stop(armed: bool, last_press: float, now_m: float,
                       window: float = SIGINT_CONFIRM_S) -> bool:
    """True = stop now. False = swallow this press and wait for confirmation.

    Armed means the session is in its recording window; outside it, one press stops as usual."""
    if _ALLOW_SINGLE["v"] or not armed:
        return True
    return (now_m - last_press) < window


def signal_policy(name: str, armed: bool, allow_single: bool = False) -> str:
    """What to do with a terminating signal: 'stop', 'ignore', or 'ignore-once'.

    SIGTERM / SIGQUIT / SIGBREAK are deliberate: stop and flush, no confirmation.
    SIGHUP usually means the terminal or SSH session went away. Killing the run there would cost the
    session for no reason, so while armed it is ignored (the log and dashboard keep working) and it
    stops normally outside the window."""
    if name in ("SIGTERM", "SIGQUIT", "SIGBREAK"):
        return "stop"
    if name == "SIGHUP":
        return "ignore-once" if armed and not allow_single else "stop"
    return "stop"


def _on_signal(signum, frame):  # noqa: ARG001
    name = signal.Signals(signum).name
    action = signal_policy(name, _SIGINT["armed"], _ALLOW_SINGLE["v"])
    if action == "stop":
        STOP.set()
        say(f"{name}: stopping, flushing logs ...")
        return
    say(f"{name} ignored — the terminal went away, the session keeps recording. "
        f"Press Ctrl+C twice or send SIGTERM to stop.")
    _note_event({"type": "event", "t": ts(), "name": "sighup_ignored"})


def _on_sigint(signum, frame):  # noqa: ARG001
    now_m = time.monotonic()
    if sigint_should_stop(_SIGINT["armed"], _SIGINT["last"], now_m):
        STOP.set()
        say("Ctrl-C: stopping, flushing logs ...")
        return
    _SIGINT["last"] = now_m
    _SIGINT["ignored"] += 1
    say(f"Ctrl-C ignored — the session is running. Press again within {SIGINT_CONFIRM_S:g}s to stop.")
    fn = _SIGINT_EVENT.get("fn")
    if fn:
        try:
            fn({"type": "event", "t": ts(), "name": "sigint_ignored",
                "count": _SIGINT["ignored"]})
        except Exception:
            pass
# Index -> indicative close typed in by the operator (NSE shows it on the terminal; mStock's
# API does not carry it). When present it replaces the computed target for that index.
MANUAL: dict[str, float] = {}
MANUAL_LOCK = threading.Lock()


def parse_manual(line: str) -> tuple[str | None, float | None]:
    """'NIFTY 25065' -> ('NIFTY', 25065.0); a bare number applies to the first index given later."""
    parts = line.replace(",", "").split()
    if not parts:
        return None, None
    for i, tok in enumerate(parts):
        try:
            val = float(tok)
        except ValueError:
            continue
        idx = None
        for name in INDEX_CFG:
            if any(name in p.upper().replace("_", "") for p in parts[:i]):
                idx = name
        return idx, val
    return None, None


def watch_stdin(enabled: list[str]) -> None:
    """Read the terminal while recording so a manual indicative price can be typed in live."""
    try:
        for line in sys.stdin:
            idx, val = parse_manual(line)
            if val is None:
                continue
            if idx is None:
                idx = enabled[0] if enabled else "NIFTY"
            with MANUAL_LOCK:
                MANUAL[idx] = val
            say(f"manual indicative for {idx}: {val:.2f}")
    except Exception:
        pass

# ----------------------------------------------------------------------------
# Index / stock configuration
# ----------------------------------------------------------------------------
# spot_alias: normalised names tried against scriptmaster tradingsymbol / name.
# und: underlying name used by futures & options rows.
INDEX_CFG = {
    "NIFTY":      {"spot_ex": "NSE", "deriv_ex": "NFO", "und": "NIFTY",
                   "spot_alias": ["NIFTY50", "NIFTY"]},
    "BANKNIFTY":  {"spot_ex": "NSE", "deriv_ex": "NFO", "und": "BANKNIFTY",
                   "spot_alias": ["NIFTYBANK", "BANKNIFTY"]},
    "FINNIFTY":   {"spot_ex": "NSE", "deriv_ex": "NFO", "und": "FINNIFTY",
                   "spot_alias": ["NIFTYFINSERVICE", "NIFTYFINANCIALSERVICES", "FINNIFTY"]},
    "MIDCPNIFTY": {"spot_ex": "NSE", "deriv_ex": "NFO", "und": "MIDCPNIFTY",
                   "spot_alias": ["NIFTYMIDSELECT", "NIFTYMIDCAPSELECT", "MIDCPNIFTY"]},
    "SENSEX":     {"spot_ex": "BSE", "deriv_ex": "BFO", "und": "SENSEX",
                   "spot_alias": ["SENSEX", "BSESENSEX", "SPBSESENSEX"]},
    "BANKEX":     {"spot_ex": "BSE", "deriv_ex": "BFO", "und": "BANKEX",
                   "spot_alias": ["BANKEX", "BSEBANKEX", "SPBSEBANKEX"]},
}

# Heavyweights from the earlier research chat. Index membership/weights are
# APPROXIMATE and only used to decide what to record, not for any calculation.
DEFAULT_STOCKS = [
    "HDFCBANK", "RELIANCE", "ICICIBANK", "INFY", "BHARTIARTL", "ITC", "LT",
    "TCS", "AXISBANK", "SBIN", "KOTAKBANK", "BAJFINANCE",
    "MAXHEALTH", "INDHOTEL", "PERSISTENT", "BSE", "COFORGE", "FEDERALBNK", "DIXON",
]

# BSE-listed names recorded when SENSEX / BANKEX are selected. This is a RECORDING list chosen
# from the same heavyweights plus the main banks, NOT an official constituent list, and no
# weights are attached (none verified yet), so the weighted cross-check skips these two indices.
SENSEX_STOCKS = ["HDFCBANK", "RELIANCE", "ICICIBANK", "INFY", "BHARTIARTL", "ITC", "LT", "TCS",
                 "AXISBANK", "SBIN", "KOTAKBANK", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
                 "ASIANPAINT", "NTPC", "POWERGRID", "ULTRACEMCO", "TITAN"]
BANKEX_STOCKS = ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK", "INDUSINDBK", "PNB",
                 "BANKBARODA", "FEDERALBNK", "AUBANK", "IDFCFIRSTB", "CANBK"]

# APPROXIMATE index weights taken from the earlier research chat (Gemini conversation).
# UNVERIFIED and only the top constituents (coverage < 100%). Used ONLY for an
# informational cross-check in the readout; refine from official NSE factsheets and
# from Monday's log. Override with --weights weights.json
# (format: {"NIFTY": {"HDFCBANK": 0.115, ...}, "BANKNIFTY": {...}}).
DEFAULT_WEIGHTS = {
    "NIFTY": {"HDFCBANK": 0.115, "RELIANCE": 0.092, "ICICIBANK": 0.080, "INFY": 0.060,
              "BHARTIARTL": 0.042, "ITC": 0.041, "LT": 0.040, "TCS": 0.038,
              "AXISBANK": 0.031, "SBIN": 0.030},
    "BANKNIFTY": {"HDFCBANK": 0.285, "ICICIBANK": 0.240, "SBIN": 0.100,
                  "KOTAKBANK": 0.090, "AXISBANK": 0.090},
    "FINNIFTY": {"HDFCBANK": 0.300, "ICICIBANK": 0.210, "BAJFINANCE": 0.110,
                 "KOTAKBANK": 0.095, "SBIN": 0.085},
    "MIDCPNIFTY": {"MAXHEALTH": 0.085, "INDHOTEL": 0.080, "PERSISTENT": 0.075, "BSE": 0.070,
                   "COFORGE": 0.065, "FEDERALBNK": 0.060, "DIXON": 0.060},
    # SENSEX / BANKEX: approximate free-float weights from public factsheets, UNVERIFIED like the
    # NSE sets above and only used for the informational weighted line.
    "SENSEX": {"HDFCBANK": 0.115, "RELIANCE": 0.100, "ICICIBANK": 0.085, "INFY": 0.070,
               "TCS": 0.050, "BHARTIARTL": 0.042, "ITC": 0.040, "LT": 0.038,
               "AXISBANK": 0.032, "SBIN": 0.030, "KOTAKBANK": 0.028, "BAJFINANCE": 0.028,
               "HINDUNILVR": 0.025, "MARUTI": 0.024, "SUNPHARMA": 0.020, "ASIANPAINT": 0.018,
               "NTPC": 0.017, "POWERGRID": 0.016, "ULTRACEMCO": 0.016, "TITAN": 0.015},
    "BANKEX": {"HDFCBANK": 0.270, "ICICIBANK": 0.240, "SBIN": 0.110, "KOTAKBANK": 0.100,
               "AXISBANK": 0.100, "INDUSINDBK": 0.050, "BANKBARODA": 0.040, "PNB": 0.030,
               "FEDERALBNK": 0.020, "AUBANK": 0.020, "IDFCFIRSTB": 0.010, "CANBK": 0.010},
}

# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------

def now() -> datetime:
    return datetime.now(IST)


def ts() -> str:
    return now().isoformat(timespec="milliseconds")


def norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def parse_hms(s: str) -> tuple[int, int, int]:
    parts = [int(p) for p in s.strip().split(":")]
    while len(parts) < 3:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def at(day: date, hms: str) -> datetime:
    h, m, s = parse_hms(hms)
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=IST)


def say(msg: str) -> None:
    print(f"[{now().strftime('%H:%M:%S')}] {msg}", flush=True)


# --- fault isolation ---------------------------------------------------------
# The 15:15-15:30 window cannot be re-run, so no single exception is allowed to
# end the process, skip a scheduled step, or kill the WS reader thread. Every
# entry point catches, records the traceback into the log, and carries on.
ERRORS: list[dict] = []
ERROR_LIMIT = 40


def note_error(where: str, exc: BaseException, out=None) -> None:
    ERRORS.append({"t": ts(), "where": where, "error": f"{exc.__class__.__name__}: {exc}"})
    if len(ERRORS) > 200:
        del ERRORS[:100]
    if len(ERRORS) <= ERROR_LIMIT:
        say(f"!!! {where} failed: {exc.__class__.__name__}: {exc} (logged, continuing)")
    if out is not None:
        try:
            out.write({"type": "event", "t": ts(), "name": "error", "where": where,
                       "error": f"{exc.__class__.__name__}: {exc}"[:300],
                       "trace": traceback.format_exc(limit=6)[-1200:]})
        except Exception:
            pass


def safe_call(where: str, fn, out=None, *args, **kwargs):
    """Call fn and swallow failures, recording them. Returns None on error."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - deliberate: keep the session alive
        note_error(where, exc, out)
        return None


def needs_late_baseline(kind: str, label: str, base_label: str, have_baseline: bool,
                        event_at: datetime, now_dt: datetime, tol_s: int = 15) -> bool:
    """True when the baseline snapshot is already in the past and has not been taken yet.

    Starting at 15:15 (or later) means the 15:15:10 snapshot is gone; skipping it would leave the
    whole session without a reference point, so the run takes one immediately instead."""
    if kind != "snap" or label != base_label or have_baseline:
        return False
    return event_at < now_dt - timedelta(seconds=tol_s)


def start_dashboard(log_path, host: str = "127.0.0.1", port: int = 8080, open_browser: bool = True):
    """Serve the live dashboard from inside the logger, so one command is enough.

    It tails the same JSONL this process is writing. Returns the URL, or None when the dashboard
    module or a free port is unavailable — the logger never depends on it working.
    """
    try:
        import cas_dashboard as dash
    except Exception as exc:  # noqa: BLE001
        say(f"dashboard unavailable ({exc.__class__.__name__}: {exc}) — logging only")
        return None
    try:
        live = dash.Live(Path(log_path))
        live.poll()
        srv = None
        for candidate in range(port, port + 12):
            try:
                srv = dash.ThreadingHTTPServer((host, candidate), dash.make_handler(live))
                port = candidate
                break
            except OSError:
                continue
        if srv is None:
            say(f"dashboard: no free port in {port}-{port + 11}")
            return None
        threading.Thread(target=srv.serve_forever, daemon=True, name="dashboard").start()
        url = f"http://{host}:{port}/"
        say(f"dashboard ready: {url}")
        if open_browser:
            def _open():
                try:
                    import webbrowser
                    webbrowser.open(url)
                except Exception:
                    pass
            threading.Timer(1.5, _open).start()
        return url
    except Exception as exc:  # noqa: BLE001
        say(f"dashboard failed to start: {exc.__class__.__name__}: {exc} — logging only")
        return None


def load_env(path: str) -> None:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def sleep_until(target: datetime, status=None, every: float = 30.0) -> None:
    last = 0.0
    while not STOP.is_set():
        rem = (target - now()).total_seconds()
        if rem <= 0:
            return
        if status and time.monotonic() - last >= every:
            status()
            last = time.monotonic()
        time.sleep(min(rem, 0.2))


EXPIRY_FORMATS = ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d%b%Y", "%Y-%m-%d %H:%M:%S",
                  "%d-%m-%Y %H:%M:%S", "%d/%m/%Y", "%d-%b-%y", "%Y%m%d")


def parse_expiry(s: str):
    s = (s or "").strip()
    if not s:
        return None
    for f in EXPIRY_FORMATS:
        try:
            return datetime.strptime(s, f).date()
        except ValueError:
            continue
    return None


def to_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


class JsonlWriter:
    """Thread-safe JSONL writer; flushes about once a second."""

    def __init__(self, path: Path):
        self.path = path
        self.f = open(path, "a", encoding="utf-8", buffering=1 << 16)
        self.lock = threading.Lock()
        self.last_flush = time.monotonic()
        self.closed = False

    def write(self, obj: dict) -> None:
        line = json.dumps(obj, separators=(",", ":"), default=str)
        with self.lock:
            self.f.write(line + "\n")
            if time.monotonic() - self.last_flush > 1.0:
                self.f.flush()
                self.last_flush = time.monotonic()

    def flush(self) -> None:
        with self.lock:
            self.f.flush()

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            try:
                self.f.flush()
                os.fsync(self.f.fileno())
            except (OSError, ValueError):
                pass
            finally:
                try:
                    self.f.close()
                except (OSError, ValueError):
                    pass
            self.f.close()


# ----------------------------------------------------------------------------
# Auth (docs [3])
# ----------------------------------------------------------------------------

def make_headers(api_key: str, access_token: str) -> dict:
    return {"X-Mirae-Version": "1", "Authorization": f"token {api_key}:{access_token}"}


def diagnose(log_path: Path, show: int = 25) -> None:
    """Explain a finished session from its own files: which tokens ticked, in what mode.

    A session that records thousands of frames but only a handful of distinct tokens means the
    subscription or the mode handshake is wrong — this prints the evidence instead of a guess."""
    import base64
    from collections import Counter

    log_path = Path(log_path)
    raw_path = log_path.with_name(log_path.stem + ".ws_raw.jsonl")
    if not raw_path.exists():
        raw_path = log_path.with_name(log_path.name.replace(".jsonl", ".ws_raw.jsonl"))

    meta, ticks, snaps, texts, events = {}, Counter(), [], [], Counter()
    with log_path.open(encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            t = r.get("type")
            if t == "meta":
                meta = r
            elif t == "ws_tick":
                ticks[r.get("token")] += 1
            elif t == "snapshot":
                snaps.append(r)
            elif t == "ws_text":
                texts.append(r.get("msg", ""))
            elif t == "event":
                events[r.get("name")] += 1

    print(f"=== {log_path.name} ===")
    uni = meta.get("universe") or meta.get("universe_counts") or {}
    print(f"universe: {uni}")
    by_tok = {}
    for role, items in (meta.get("items") or {}).items() if isinstance(meta.get("items"), dict) else []:
        for it in items:
            by_tok[it.get("token")] = (role, it.get("symbol") or it.get("key"))
    for section in ("index", "future", "stock", "option"):
        for it in (meta.get(section) or []):
            if isinstance(it, dict):
                by_tok[it.get("token")] = (section, it.get("symbol") or it.get("key"))

    print(f"\ndistinct tokens with ticks: {len(ticks)}")
    for tok, n in ticks.most_common(show):
        role, sym = by_tok.get(tok, ("?", "?"))
        print(f"  {tok:>10}  {n:>6} ticks  {role:<7} {sym}")
    missing = [t for t in by_tok if t not in ticks]
    print(f"subscribed but never ticked: {len(missing)}")
    for tok in missing[:10]:
        role, sym = by_tok[tok]
        print(f"  {tok:>10}  0 ticks      {role:<7} {sym}")

    sizes = Counter()
    if raw_path.exists():
        with raw_path.open(encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= 400:
                    break
                try:
                    b = base64.b64decode(json.loads(line).get("b64", ""))
                except Exception:  # noqa: BLE001
                    continue
                sizes[len(b)] += 1
        print(f"\nraw frame sizes (first 400): {dict(sizes.most_common(8))}")
        print("  a frame of 8/16/24 bytes = one or two 8-byte LTP packets; a mixed frame of "
              "n*200 = full mode")

    print(f"\ntext messages from the server: {len(texts)}")
    for m in texts[:6]:
        print("  " + m[:200].replace("\n", " "))
    print(f"\nevents: {dict(events)}")
    print(f"snapshots: {[s.get('label') for s in snaps]}")
    for s in snaps:
        if s.get("label") and "baseline" in str(s.get("label")):
            print(f"  baseline {s.get('label')}: {s.get('spots') or s.get('data', {}).keys()}")


def validate_totp_secret(secret: str) -> str | None:
    """Return a human-readable problem with a TOTP secret, or None if it looks usable.

    Catches the mistakes that otherwise surface as an opaque 'verifytotp failed' at 15:05:
    spaces from copy-paste, lowercase, an access token pasted into the wrong variable."""
    s = (secret or "").strip().replace(" ", "").replace("-", "")
    if not s:
        return "empty"
    if any(c.islower() for c in s):
        return "contains lowercase — base32 secrets are uppercase (it is probably an access token)"
    bad = sorted({c for c in s if c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"})
    if bad:
        return (f"contains {''.join(bad)} — base32 only uses A-Z and 2-7 "
                f"(0, 1, 8 and 9 never appear)")
    if not 16 <= len(s) <= 64:
        return f"length {len(s)} is unusual for a TOTP secret (normally 16 to 32 characters)"
    return None


def login(api_key: str) -> str:
    """Return an access token. Order of preference:
    1. MSTOCK_ACCESS_TOKEN (already generated today)
    2. TOTP flow: optional connect/login (username+password) then session/verifytotp
    """
    token = os.environ.get("MSTOCK_ACCESS_TOKEN", "").strip()
    if token:
        return token

    totp = os.environ.get("MSTOCK_TOTP", "").strip()
    secret = os.environ.get("MSTOCK_TOTP_SECRET", "").strip()
    if secret:
        problem = validate_totp_secret(secret)
        if problem:
            sys.exit(f"MSTOCK_TOTP_SECRET: {problem}")
    if not totp and secret:
        try:
            import pyotp
        except ImportError:
            sys.exit("pyotp is required for MSTOCK_TOTP_SECRET (pip install pyotp)")
        totp = pyotp.TOTP(secret).now()
    if not totp:
        sys.exit("No credentials: set MSTOCK_ACCESS_TOKEN, or MSTOCK_TOTP / MSTOCK_TOTP_SECRET "
                 "(TOTP must be enabled on trade.mstock.com -> Trading APIs).")

    h = {"X-Mirae-Version": "1", "Content-Type": "application/x-www-form-urlencoded"}
    user, pwd = os.environ.get("MSTOCK_USERNAME"), os.environ.get("MSTOCK_PASSWORD")
    if user and pwd:
        r = requests.post(f"{BASE_URL}/connect/login", headers=h,
                          data={"username": user, "password": pwd}, timeout=10)
        j = _json(r)
        if j.get("status") != "success":
            sys.exit(f"connect/login failed: {j.get('message') or r.status_code}")
        say("connect/login OK")

    r = requests.post(f"{BASE_URL}/session/verifytotp", headers=h,
                      data={"api_key": api_key, "totp": totp}, timeout=10)
    j = _json(r)
    if j.get("status") != "success":
        sys.exit(f"verifytotp failed: {j.get('message') or r.status_code}")
    token = (j.get("data") or {}).get("access_token")
    if not token:
        sys.exit("verifytotp succeeded but no access_token in response")
    say("TOTP session OK (token valid until end of day)")
    return token


def _json(r: requests.Response) -> dict:
    try:
        return r.json()
    except ValueError:
        return {"status": "error", "message": f"non-JSON response HTTP {r.status_code}: {r.text[:200]}"}


# ----------------------------------------------------------------------------
# Instrument master -> universe
# ----------------------------------------------------------------------------

def load_scriptmaster(headers: dict, cache_dir: Path, day: date) -> list[dict]:
    path = cache_dir / f"scriptmaster_{day.isoformat()}.csv"
    if not path.exists():
        say("downloading scriptmaster ...")
        r = requests.get(f"{BASE_URL}/instruments/scriptmaster", headers=headers, timeout=60)
        if r.status_code != 200 or r.text.lstrip().startswith("{"):
            sys.exit(f"scriptmaster download failed: HTTP {r.status_code} {r.text[:200]}")
        path.write_text(r.text, encoding="utf-8")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip().lower() for h in (reader.fieldnames or [])]
        rows = [{k: (v or "").strip() for k, v in row.items()} for row in reader]
    need = {"instrument_token", "tradingsymbol", "exchange"}
    if not rows or not need.issubset(rows[0].keys()):
        sys.exit(f"scriptmaster has unexpected columns: {list(rows[0].keys()) if rows else 'EMPTY'}")
    return rows


def und_match(row: dict, und: str, name_exact_exists: bool) -> bool:
    if name_exact_exists:
        return norm(row.get("name")) == und
    return norm(row.get("name")) == und or re.match(rf"^{und}\d", norm(row.get("tradingsymbol"))) is not None


def make_item(row: dict, role: str, index: str | None = None, **extra) -> dict | None:
    try:
        tok = int(row["instrument_token"])
    except (KeyError, ValueError):
        return None
    d = {"token": tok, "exchange": row["exchange"], "symbol": row["tradingsymbol"],
         "key": f'{row["exchange"]}:{row["tradingsymbol"]}', "role": role, "index": index,
         "exchange_token": row.get("exchange_token")}
    d.update(extra)
    return d


class Universe:
    def __init__(self):
        self.by_token: dict[int, dict] = {}
        self.lock = threading.Lock()

    def add(self, item: dict | None) -> bool:
        if not item:
            return False
        with self.lock:
            if item["token"] in self.by_token:
                return False
            self.by_token[item["token"]] = item
            return True

    def keys(self) -> list[str]:
        with self.lock:
            return [i["key"] for i in self.by_token.values()]

    def tokens(self) -> list[int]:
        with self.lock:
            return list(self.by_token.keys())

    def get(self, tok: int) -> dict | None:
        return self.by_token.get(tok)

    def count_by_role(self) -> dict:
        c: dict = defaultdict(int)
        with self.lock:
            for i in self.by_token.values():
                c[i["role"]] += 1
        return dict(c)


class Resolver:
    """Finds index spots, futures, option chains and stocks inside the scriptmaster."""

    def __init__(self, rows: list[dict], today: date, overrides: dict):
        self.rows = rows
        self.today = today
        self.overrides = overrides
        self.by_ex: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            self.by_ex[r["exchange"].upper()].append(r)
        self.notes: list[str] = []
        self.opt_rows: dict[str, list[dict]] = {}
        self.opt_expiry: dict[str, date | None] = {}
        self.unparsed_expiry_samples: list[str] = []
        self.missing: dict[str, list[str]] = {"NSE": [], "BSE": []}

    def spot(self, idx: str) -> dict | None:
        if idx in self.overrides:
            o = self.overrides[idx]
            return {"token": int(o["token"]), "exchange": o.get("exchange", INDEX_CFG[idx]["spot_ex"]),
                    "symbol": o["symbol"], "key": f'{o.get("exchange", INDEX_CFG[idx]["spot_ex"])}:{o["symbol"]}',
                    "role": "index", "index": idx, "exchange_token": None}
        cfg = INDEX_CFG[idx]
        aliases = [norm(a) for a in cfg["spot_alias"]]
        cands = []
        for r in self.by_ex.get(cfg["spot_ex"], []):
            # Only a *real* expiry excludes a row: masters often put "-", "NA" or "" in the
            # expiry column of index rows, and treating that as a derivative hid them entirely.
            if parse_expiry(r.get("expiry", "")) or (to_float(r.get("strike")) or 0) > 0:
                continue
            n1, n2 = norm(r.get("tradingsymbol")), norm(r.get("name"))
            if n1 in aliases or n2 in aliases:
                seg = (r.get("segment", "") + r.get("instrument_type", "")).upper()
                score = (2 if "IND" in seg else 0) + (1 if n1 in aliases else 0) \
                    - (2 if r.get("instrument_type", "").upper() in ("EQ", "BE") else 0)
                cands.append((score, r))
        if not cands:
            # Say WHY, with the evidence: any index-looking row whose name mentions the index.
            key = norm(cfg["und"])
            looks = []
            for r in self.rows:
                blob = norm(r.get("tradingsymbol")) + "/" + norm(r.get("name"))
                itype = (r.get("instrument_type") or "").upper()
                seg = (r.get("segment") or "").upper()
                if key in blob and ("IND" in itype or "IND" in seg or itype in ("", "EQ")):
                    looks.append(f'{r.get("exchange")}:{r.get("tradingsymbol")}'
                                 f'[{itype or seg or "?"}]')
                if len(looks) >= 6:
                    break
            self.notes.append(f"{idx}: spot NOT found in scriptmaster (use --find / --overrides)")
            if looks:
                self.notes.append(f"   {idx}: closest rows: {looks}")
            else:
                self.notes.append(f"   {idx}: no row mentions '{cfg['und']}' at all "
                                  f"— the master may use a different underlying name")
            return None
        cands.sort(key=lambda x: -x[0])
        if len(cands) > 1:
            self.notes.append(f"{idx}: {len(cands)} spot candidates, using {cands[0][1]['tradingsymbol']} "
                              f"(others: {[c[1]['tradingsymbol'] for c in cands[1:4]]})")
        return make_item(cands[0][1], "index", idx)

    def _deriv_rows(self, idx: str) -> list[dict]:
        cfg = INDEX_CFG[idx]
        rows = self.by_ex.get(cfg["deriv_ex"], [])
        exact = any(norm(r.get("name")) == cfg["und"] and r.get("expiry") for r in rows)
        return [r for r in rows if und_match(r, cfg["und"], exact)]

    def _expiry_of(self, r: dict) -> date | None:
        e = parse_expiry(r.get("expiry", ""))
        if e is None and r.get("expiry") and len(self.unparsed_expiry_samples) < 3:
            self.unparsed_expiry_samples.append(r["expiry"])
        return e

    def future(self, idx: str) -> dict | None:
        best, best_exp = None, None
        for r in self._deriv_rows(idx):
            if "FUT" not in (r.get("instrument_type", "") + r.get("tradingsymbol", "")).upper():
                continue
            if r["tradingsymbol"].upper().endswith(("CE", "PE")):
                continue
            e = self._expiry_of(r)
            if e is None or e < self.today:
                continue
            if best_exp is None or e < best_exp:
                best, best_exp = r, e
        if not best:
            self.notes.append(f"{idx}: no current-month future found")
            return None
        return make_item(best, "future", idx, expiry=best_exp.isoformat())

    def prepare_options(self, idx: str) -> None:
        rows = []
        for r in self._deriv_rows(idx):
            sym = r["tradingsymbol"].upper()
            if sym.endswith(("CE", "PE")) and to_float(r.get("strike")) is not None:
                rows.append(r)
        exps = sorted({e for r in rows if (e := self._expiry_of(r)) and e >= self.today})
        self.opt_rows[idx] = rows
        self.opt_expiry[idx] = exps[0] if exps else None
        if not exps:
            self.notes.append(f"{idx}: no option expiry >= today found")

    def options_around(self, idx: str, spot: float, n_each_side: int,
                       expiry: date | None = None) -> list[dict]:
        expiry = expiry or self.opt_expiry.get(idx)
        if expiry is None:
            return []
        chain: dict[float, dict] = defaultdict(dict)
        for r in self.opt_rows.get(idx, []):
            if self._expiry_of(r) == expiry:
                chain[to_float(r["strike"])][r["tradingsymbol"].upper()[-2:]] = r
        strikes = sorted(chain)
        if not strikes:
            return []
        atm = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
        lo, hi = max(0, atm - n_each_side), min(len(strikes), atm + n_each_side + 1)
        items = []
        for s in strikes[lo:hi]:
            for ot in ("CE", "PE"):
                r = chain[s].get(ot)
                if r:
                    items.append(make_item(r, "option", idx, strike=s, otype=ot, expiry=expiry.isoformat()))
        return [i for i in items if i]

    def bse_stock(self, sym: str) -> dict | None:
        """BSE cash symbols look like 'ACC-A' (docs example BSE:ACC-A): match the part before the group."""
        n = norm(sym)
        cands = []
        for r in self.by_ex.get("BSE", []):
            if r.get("expiry") or (to_float(r.get("strike")) or 0) > 0:
                continue
            if r.get("instrument_type", "").upper() == "INDEX":
                continue
            if norm(r["tradingsymbol"].split("-")[0]) == n:
                cands.append(r)
        if not cands:
            self.missing["BSE"].append(sym)
            return None
        cands.sort(key=lambda r: (r.get("instrument_type", "").upper() != "EQ", r["tradingsymbol"]))
        return make_item(cands[0], "stock")

    def stock(self, sym: str) -> dict | None:
        want = {norm(sym), norm(sym) + "EQ"}
        for r in self.by_ex.get("NSE", []):
            if r.get("instrument_type", "").upper() == "EQ" and norm(r["tradingsymbol"]) in want:
                return make_item(r, "stock")
        self.missing["NSE"].append(sym)
        return None


# ----------------------------------------------------------------------------
# WebSocket packets (docs [1])
# ----------------------------------------------------------------------------
PRICE_KEYS = {"ltp", "atp", "open", "high", "low", "close", "chg", "uc", "lc", "yh", "yl"}


def split_frame(buf: bytes, endian: str, prefer: str = "fixed") -> tuple[list[bytes], str]:
    """mStock Type A documents ONE layout: [short count][short size][count x packet of size].

    Earlier versions of this script guessed the Kite layout first ([count] then a per-packet
    length). That agrees with the docs only when count == 1; on a multi-packet frame it can
    consume the buffer "successfully" and return nonsense, so the documented layout is now
    tried first and the Kite shape is only a fallback. `prefer` lets --layout force either.
    """
    if len(buf) < 4:
        return [], "short"
    count = struct.unpack_from(endian + "H", buf, 0)[0]
    size = struct.unpack_from(endian + "H", buf, 2)[0]

    fixed: list[bytes] = []
    if size > 0 and count > 0 and 4 + count * size <= len(buf):
        fixed = [buf[4 + i * size: 4 + (i + 1) * size] for i in range(count)]

    def kite() -> list[bytes]:
        pkts, off = [], 2
        for _ in range(count):
            if off + 2 > len(buf):
                return []
            ln = struct.unpack_from(endian + "H", buf, off)[0]
            off += 2
            if ln <= 0 or off + ln > len(buf):
                return []
            pkts.append(buf[off:off + ln])
            off += ln
        return pkts if off == len(buf) else []

    if prefer == "kite":
        k = kite()
        if k:
            return k, "kite"
        return (fixed, "fixed") if fixed else ([], "fail")
    if fixed:
        return fixed, "fixed"
    k = kite()
    return (k, "kite") if k else ([], "fail")


def parse_packet(pkt: bytes, endian: str) -> dict | None:
    L = len(pkt)
    if L < 8:
        return None
    I = lambda o: struct.unpack_from(endian + "i", pkt, o)[0]  # noqa: E731
    H = lambda o: struct.unpack_from(endian + "h", pkt, o)[0]  # noqa: E731
    d: dict = {"len": L, "token": I(0), "ltp": I(4)}
    if L == 8:
        d["kind"] = "ltp"
    elif L == 48:
        d.update(kind="index", high=I(8), low=I(12), open=I(16), close=I(20), chg=I(24),
                 ex_ts=I(28), uc=I(32), lc=I(36), yh=I(40), yl=I(44))
    elif L >= 64:
        d.update(kind="full" if L >= 184 else "quote", ltq=I(8), atp=I(12), vol=I(16), tbq=I(20),
                 tsq=I(24), open=I(28), high=I(32), low=I(36), close=I(40), lt_ts=I(44), oi=I(48),
                 oi_hi=I(52), oi_lo=I(56), ex_ts=I(60))
        if L >= 184:
            lv = [[I(64 + i * 12), I(68 + i * 12), H(72 + i * 12)] for i in range(10)]
            d["bids"], d["asks"] = lv[:5], lv[5:]
        if L >= 200:
            d.update(uc=I(184), lc=I(188), yh=I(192), yl=I(196))
    else:
        d["kind"] = "unknown"
    return d


# Strike ladders + the smallest projected gap worth acting on, per index.
# The floors are not half a strike: a 35-point NIFTY gap is worth a trade while a 90-point
# BANKNIFTY gap is not, because the premium you pay is a much bigger share of the intrinsic there.
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25,
               "SENSEX": 100, "BANKEX": 100}
MIN_GAP = {"NIFTY": 35, "BANKNIFTY": 120, "FINNIFTY": 60, "MIDCPNIFTY": 35,
           "SENSEX": 100, "BANKEX": 100}


def pick_strike(target: float, step: int, side: str) -> float:
    """Deepest strike that is still ITM at `target`: just BELOW for a CE, just ABOVE for a PE."""
    if side == "CE":
        return math.floor(target / step) * step
    return math.ceil(target / step) * step


def decide(index: str, gap: float, spot_base: float, target: float,
           strike: float | None, ask: float | None) -> dict:
    """One row of the decision table: is the trade there, and what is it worth if we are right?

    edge = intrinsic at the projected target minus what the option costs now. Positive edge means the
    move is not fully priced; near zero means the market already agrees and there is nothing to buy.
    """
    step = STRIKE_STEP.get(index, 50)
    floor_gap = MIN_GAP.get(index, step / 2)
    row = {"index": index, "gap": round(gap, 2), "spot_base": spot_base, "target": round(target, 2),
           "step": step, "side": "CE" if gap > 0 else "PE",
           "min_gap": floor_gap, "tradeable": abs(gap) >= floor_gap}
    row["strike"] = pick_strike(target, step, row["side"])
    if strike is not None:
        row["subscribed_strike"] = strike
    if ask is not None and strike:
        intr = (target - strike) if row["side"] == "CE" else (strike - target)
        row["intrinsic_at_target"] = round(intr, 2)
        row["ask"] = ask
        row["edge"] = round(intr - ask, 2)
    if not row["tradeable"]:
        row["note"] = f"gap under the {floor_gap:g}-pt floor for this index — premium eats the move"
    return row


def tick_mid(d: dict, div: float):
    """Best bid/ask mid in rupees, or None."""
    try:
        b, a = d.get("bids") or [], d.get("asks") or []
        if b and a and b[0][1] > 0 and a[0][1] > 0:
            return (b[0][1] + a[0][1]) / 2 / div
    except (IndexError, TypeError):
        pass
    return None


def tick_micro(d: dict, div: float):
    """Size-weighted level-1 fair value in rupees, or None."""
    try:
        b, a = d.get("bids") or [], d.get("asks") or []
        if not b or not a:
            return None
        bq, bp = b[0][0], b[0][1]
        aq, ap = a[0][0], a[0][1]
        if min(bq, bp, aq, ap) <= 0:
            return None
        return (bp * aq + ap * bq) / (bq + aq) / div
    except (IndexError, TypeError, ZeroDivisionError):
        return None


def parity_forward(uni, latest: dict, idx: str, div: float):
    """Median put-call-parity forward across the subscribed strikes of one index.

    Same-day expiry: forward ~ spot, so the move in this number is the market's estimate of
    where the index will settle. Returns (forward, strikes_used)."""
    by_strike: dict[float, dict] = defaultdict(dict)
    for item in list(uni.by_token.values()):
        if item.get("role") != "option" or item.get("index") != idx:
            continue
        d = latest.get(item["token"])
        if not d:
            continue
        px = tick_mid(d, div) or (d.get("ltp") or 0) / div
        if px:
            by_strike[item["strike"]][item["otype"]] = px
    fwds = [k + v["CE"] - v["PE"] for k, v in by_strike.items() if "CE" in v and "PE" in v]
    if not fwds:
        return None, 0
    return statistics.median(fwds), len(fwds)


def scale(d: dict, div: float, full_depth: bool) -> dict:
    out = {}
    for k, v in d.items():
        if k in PRICE_KEYS and isinstance(v, int):
            out[k] = round(v / div, 4)
            if k == "ltp":
                out["ltp_raw"] = v
        elif k in ("bids", "asks"):
            lv = v if full_depth else v[:1]
            out[k] = [[q, round(p / div, 4), n] for q, p, n in lv]
        else:
            out[k] = v
    return out


class WSFeed:
    def __init__(self, api_key: str, token: str, universe: Universe, out: JsonlWriter,
                 raw: JsonlWriter, endian: str, div: float, full_depth: bool, prefer_layout: str = "fixed"):
        self.api_key, self.token = api_key, token
        self.universe, self.out, self.raw = universe, out, raw
        self.endian, self.div, self.full_depth = endian, div, full_depth
        self.prefer_layout = prefer_layout
        self.recording = threading.Event()
        self.latest: dict[int, dict] = {}      # token -> raw (unscaled) parsed packet
        self.latest_t: dict[int, str] = {}
        self.subscribed: set[int] = set()
        self.n_frames = self.n_pkts = self.n_unmapped = self.n_parse_fail = self.n_text = 0
        self.layouts: dict[str, int] = defaultdict(int)
        self.connected = threading.Event()
        self._last_evt: dict[str, float] = {}
        self._suppressed: dict[str, int] = defaultdict(int)
        self.ws = None
        self.thread = threading.Thread(target=self._run, daemon=True, name="ws")
        self.send_lock = threading.Lock()

    # -- public
    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def subscribe(self, tokens: list[int]) -> None:
        new = [t for t in tokens if t not in self.subscribed]
        if not new:
            return
        self.subscribed.update(new)
        if self.connected.is_set():
            self._send_subscribe(new)

    # -- internals
    def event(self, name: str, **kw) -> None:
        # A dead feed retries every 2s; without this the log fills with identical
        # reconnect lines and the interesting events get buried.
        if name in ("ws_error", "ws_close", "ws_reconnecting"):
            m = time.monotonic()
            if m - self._last_evt.get(name, 0.0) < 5.0:
                self._suppressed[name] += 1
                return
            self._last_evt[name] = m
        self.out.write({"type": "event", "t": ts(), "name": name, **kw})

    def _send(self, payload: str) -> None:
        with self.send_lock:
            self.ws.send(payload)

    def _send_subscribe(self, tokens: list[int]) -> None:
        for i in range(0, len(tokens), 100):
            self._send(json.dumps({"a": "subscribe", "v": tokens[i:i + 100]}))
        self._send(json.dumps({"a": "mode", "v": ["full"]}))

    def _run(self) -> None:
        import websocket  # websocket-client
        while not STOP.is_set():
            try:
                self._loop_once(websocket)
            except Exception as exc:  # noqa: BLE001
                note_error("ws_loop", exc, self.out)
                time.sleep(1)

    def _loop_once(self, websocket) -> None:
        url = (f"{WS_URL}?API_KEY={quote(self.api_key, safe='/@')}"
               f"&ACCESS_TOKEN={quote(self.token, safe='')}")
        self.ws = websocket.WebSocketApp(
            url, on_open=self._on_open, on_message=self._on_message,
            on_error=self._on_error, on_close=self._on_close)
        try:
            self.ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:  # noqa: BLE001
            self.event("ws_exception", error=repr(e))
        self.connected.clear()
        if STOP.is_set():
            return
        self.event("ws_reconnecting")
        time.sleep(2)

    def _on_open(self, ws) -> None:
        self.connected.set()
        self.event("ws_open")
        self._send(f"LOGIN:{self.token}")
        want = sorted(self.subscribed)
        if want:
            self._send_subscribe(want)

    def _on_error(self, ws, err) -> None:
        self.event("ws_error", error=repr(err)[:300])

    def _on_close(self, ws, code, msg) -> None:
        self.connected.clear()
        self.event("ws_close", code=code, msg=str(msg)[:200])

    def _on_message(self, ws, message) -> None:
        try:
            self._handle_message(ws, message)
        except Exception as exc:  # noqa: BLE001
            self.n_parse_fail += 1
            note_error("ws_message", exc, self.out)

    def _handle_message(self, ws, message) -> None:
        t = ts()
        if isinstance(message, str):
            self.n_text += 1
            if self.n_text <= 300:
                self.raw.write({"t": t, "text": message[:2000]})
                self.out.write({"type": "ws_text", "t": t, "msg": message[:500]})
            return
        self.n_frames += 1
        rec = self.recording.is_set()
        if rec:
            self.raw.write({"t": t, "n": len(message), "b64": base64.b64encode(message).decode()})
        pkts, layout = split_frame(message, self.endian, self.prefer_layout)
        self.layouts[layout] += 1
        if not pkts:
            self.n_parse_fail += 1
            return
        for p in pkts:
            d = parse_packet(p, self.endian)
            if not d:
                self.n_parse_fail += 1
                continue
            self.n_pkts += 1
            tok = d["token"]
            self.latest[tok] = d
            self.latest_t[tok] = t
            item = self.universe.get(tok)
            if item is None:
                self.n_unmapped += 1
            if rec:
                role = item["role"] if item else "unmapped"
                s = scale(d, self.div, self.full_depth or role in ("stock", "index"))
                s.update(type="ws_tick", t=t, tok=tok, sym=item["key"] if item else None, role=role,
                         idx=item["index"] if item else None)
                if item and role == "option":
                    s.update(k=item["strike"], ot=item["otype"])
                self.out.write(s)


# ----------------------------------------------------------------------------
# REST poller (docs [2], [4])
# ----------------------------------------------------------------------------

def fetch_ohlc(sess: requests.Session, headers: dict, keys: list[str], chunk: int = 50,
               timeout=(2, 4)) -> tuple[dict, list[str]]:
    data, errors = {}, []
    for i in range(0, len(keys), chunk):
        part = keys[i:i + chunk]
        try:
            r = sess.get(f"{BASE_URL}/instruments/quote/ohlc", headers=headers,
                         params=[("i", k) for k in part], timeout=timeout)
            j = _json(r)
            if j.get("status") == "success" and isinstance(j.get("data"), dict):
                data.update(j["data"])
            else:
                errors.append(f"HTTP {r.status_code}: {j.get('message') or j.get('error_type')}")
        except requests.RequestException as e:
            errors.append(repr(e)[:200])
    return data, errors


class RestPoller(threading.Thread):
    def __init__(self, headers: dict, universe: Universe, out: JsonlWriter, interval: float):
        super().__init__(daemon=True, name="rest")
        self.headers, self.universe, self.out, self.interval = headers, universe, out, interval
        self.enabled = threading.Event()
        self.sess = requests.Session()
        self.n_ok = self.n_err = 0

    def run(self) -> None:
        while not STOP.is_set():
            if not self.enabled.is_set():
                time.sleep(0.1)
                continue
            t0 = time.monotonic()
            t = ts()
            try:
                data, errs = fetch_ohlc(self.sess, self.headers, self.universe.keys())
                dt = int((time.monotonic() - t0) * 1000)
                if errs:
                    self.n_err += 1
                else:
                    self.n_ok += 1
                self.out.write({"type": "rest", "t": t, "dt_ms": dt, "data": data,
                                **({"errors": errs} if errs else {})})
            except Exception as exc:  # noqa: BLE001
                self.n_err += 1
                note_error("rest_poll", exc, self.out)
            STOP.wait(max(0.0, self.interval - (time.monotonic() - t0)))


# ----------------------------------------------------------------------------
# Price divisor check: WS ints vs REST floats
# ----------------------------------------------------------------------------

def infer_divisor(feed: WSFeed, rest: dict, universe: Universe) -> tuple[float | None, list[float]]:
    ratios = []
    for key, v in rest.items():
        try:
            tok = int(v.get("instrument_token"))
        except (TypeError, ValueError):
            continue
        d = feed.latest.get(tok)
        lp = v.get("last_price")
        if d and lp and d.get("ltp"):
            ratios.append(d["ltp"] / lp)
    if len(ratios) < 3:
        return None, ratios
    med = statistics.median(ratios)
    for cand in (1, 10, 100, 1000, 10000, 1e7):
        if abs(med / cand - 1) < 0.05:
            return float(cand), ratios[:5]
    return None, ratios[:5]


# ----------------------------------------------------------------------------
# Summary (offline)
# ----------------------------------------------------------------------------

def summarize(path: str) -> None:
    recs = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    meta = next((r for r in recs if r["type"] == "meta"), {})
    ticks = [r for r in recs if r["type"] == "ws_tick"]
    snaps = {r["label"]: r for r in recs if r["type"] == "snapshot"}
    events = [r for r in recs if r["type"] == "event"]
    print(f"\n=== {path}\nrecords={len(recs)}  ws_ticks={len(ticks)}  events={len(events)}")
    div_ev = [e for e in events if e["name"] == "divisor_check"]
    end_ev = next((e for e in events if e["name"] == "end"), {})
    print("price divisor check:", [(e.get("inferred"), e.get("ratios")) for e in div_ev[-2:]] or "none",
          "| frame layouts:", end_ev.get("layouts"), "| unmapped pkts:", end_ev.get("unmapped_pkts"),
          "| text msgs:", end_ev.get("text_msgs"))
    print("universe:", meta.get("universe"), "| option expiry:", meta.get("opt_expiry"))
    for n in meta.get("notes", []):
        print("NOTE:", n)
    errs = [e for e in events if "error" in e["name"] or e["name"] in ("ws_close", "ws_reconnecting")]
    if errs:
        print("WS problems:", [(e["t"][11:19], e["name"]) for e in errs[:8]])

    # --- Q1: is there any auction information in the feed?
    print("\n--- Q1. Does the feed carry auction information? (recording window)")
    per: dict[str, dict[int, dict]] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for r in ticks:
        d = per[r["role"]][r["tok"]]
        for k in ("ltp", "tbq", "tsq"):
            if k in r:
                d[k].add(r[k])
        if r.get("bids") and any(q for q, _, _ in r["bids"]):
            d["depth"].add(1)
    for role in ("stock", "index", "future", "option"):
        toks = per.get(role, {})
        if not toks:
            print(f"{role:7s}: NO TICKS RECEIVED")
            continue
        n = len(toks)
        ltp_moves = sum(1 for d in toks.values() if len(d["ltp"]) > 1)
        tbq_moves = sum(1 for d in toks.values() if len(d.get("tbq", ())) > 1)
        depth = sum(1 for d in toks.values() if d.get("depth"))
        print(f"{role:7s}: tokens={n:3d}  ltp changed={ltp_moves:3d}  total_buy_qty changed={tbq_moves:3d}  depth populated={depth:3d}")
    st = per.get("stock", {})
    if st:
        frozen = all(len(d["ltp"]) <= 1 for d in st.values())
        qty_live = any(len(d.get("tbq", ())) > 1 for d in st.values())
        print("\nReading it (only meaningful for a REAL run covering 15:20-15:27;")
        print("a --test run during normal market hours always shows moving prices/qty):")
        print(" - stock LTP frozen in window:", frozen, "(expected: cash market is paused 15:15-15:30)")
        print(" - stock total buy/sell qty changing:", qty_live)
        print("   If LTP is frozen but qty/depth keep changing -> the feed probably carries auction order-book")
        print("   data: compare tbq/tsq/bids/asks against NSE's indicative values (zerodha.com/cas).")
        print("   If LTP, qty and depth are all static -> NO usable auction data; only Futures can be used.")

    # --- Q2: did Futures move ahead of the spot jump?
    print("\n--- Q2. Futures vs actual spot move (needs snapshots 15:15:10 and 15:31:00)")
    base = snaps.get("15:15:10") or snaps.get("test_start")
    fin = snaps.get("15:31:00") or snaps.get("test_end")
    if base and fin:
        for idx in INDEX_CFG:
            def px(s, role):
                for k, v in (s.get("rest") or {}).items():
                    m = (s.get("map") or {}).get(k)
                    if m and m.get("index") == idx and m.get("role") == role:
                        return v.get("last_price")
            sb, sf, fb, ff = px(base, "index"), px(fin, "index"), px(base, "future"), px(fin, "future")
            if None in (sb, sf, fb, ff):
                continue
            print(f"{idx:11s} spot {sb:>10.2f} -> {sf:>10.2f} ({sf - sb:+.1f})   "
                  f"future {fb:>10.2f} -> {ff:>10.2f} ({ff - fb:+.1f})")
    else:
        print("snapshots missing")

    fin_ro = [r for r in recs if r["type"] == "readout" and r.get("final")]
    if fin_ro and base and fin:
        print("\n--- Q3. Final readout vs actual spot move (weights are approximate)")
        for row in fin_ro[-1]["rows"]:
            idx = row["index"]
            actual = None
            for k, m in (fin.get("map") or {}).items():
                if m.get("index") == idx and m.get("role") == "index":
                    a, b0 = (fin.get("rest") or {}).get(k), (base.get("rest") or {}).get(k)
                    if a and b0:
                        actual = a["last_price"] - b0["last_price"]
            w = row.get("weighted") or {}
            print(f"{idx:11s} actual {actual if actual is None else format(actual, '+.1f'):>8} | futures-implied {row['fut_move']:+.1f}"
                  f" | wtd ltp {w.get('ltp_pts')} | wtd mid {w.get('mid_pts')} | coverage {w.get('coverage')}")
    print("\nReminder: 16 expiries is too few to prove anything. Collect more days before building a signal.")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="mStock CAS data logger (record only, no trading)")
    p.add_argument("--date", help="YYYY-MM-DD (default today IST)")
    p.add_argument("--out", default="cas_logs")
    p.add_argument("--env", default=".env")
    p.add_argument("--indices", default=",".join(INDEX_CFG))
    p.add_argument("--opt-indices", default="auto",
                   help="comma list, or 'auto' = indices expiring today + SENSEX/BANKEX (+NIFTY if none expire)")
    p.add_argument("--strikes", type=int, default=6, help="strikes each side of ATM (default 6)")
    p.add_argument("--stocks", default=",".join(DEFAULT_STOCKS))
    p.add_argument("--bse-stocks", default="",
                   help="comma list of BSE stocks to record (default: built-in SENSEX/BANKEX lists)")
    p.add_argument("--connect-at", default="15:10:00")
    p.add_argument("--rec-start", default="15:20:00")
    p.add_argument("--rec-end", default="15:27:30")
    p.add_argument("--decide-at", default="15:27:15",
                   help="final readout time, must be BEFORE the random close window opens at 15:28:00")
    p.add_argument("--baseline", default="15:15:10", help="snapshot label used as the frozen 3:15 baseline")
    p.add_argument("--live-every", type=float, default=3.0, help="seconds between live readouts")
    p.add_argument("--no-live", action="store_true", help="disable the live console readout")
    p.add_argument("--weights", help="JSON file overriding/adding index weights (see DEFAULT_WEIGHTS)")
    p.add_argument("--snapshots", default="15:14:50,15:15:10,15:19:50,15:31:00,15:35:30")
    p.add_argument("--rest-interval", type=float, default=2.0)
    p.add_argument("--price-div", default="auto", help="'auto' or a number (default auto, fallback 100)")
    p.add_argument("--little-endian", action="store_true")
    p.add_argument("--layout", default="fixed", choices=["fixed", "kite", "auto"],
                   help="binary frame layout; docs specify 'fixed' ([count][size][packets])")
    p.add_argument("--full-depth", action="store_true", help="keep 5-level depth for options/futures too")
    p.add_argument("--no-rest", action="store_true")
    p.add_argument("--diagnose", metavar="LOG",
                   help="explain a finished session: tokens that ticked, frame sizes, server text")
    p.add_argument("--login-check", action="store_true",
                   help="log in, print the token validity, and exit — no market needed")
    p.add_argument("--no-dashboard", action="store_true", help="do not start the web dashboard")
    p.add_argument("--allow-single-sigint", action="store_true",
                   help="let one Ctrl+C stop the run (default: two presses inside 5s)")
    p.add_argument("--no-open", action="store_true", help="start the dashboard but do not open a browser")
    p.add_argument("--dash-port", type=int, default=8080, help="first port to try for the dashboard")
    p.add_argument("--dash-host", default="127.0.0.1",
                   help="'0.0.0.0' to reach the dashboard from a phone on the same network")
    p.add_argument("--overrides", help="JSON: {\"NIFTY\": {\"token\": 123, \"symbol\": \"Nifty 50\"}}")
    p.add_argument("--test", type=int, metavar="SECONDS",
                   help="ignore the clock: record for SECONDS starting now (market must be open)")
    p.add_argument("--find", metavar="TEXT", help="search scriptmaster for TEXT and exit")
    p.add_argument("--summarize", metavar="FILE", help="summarise a finished log and exit")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.summarize:
        summarize(args.summarize)
        return

    load_env(args.env)
    refresh_endpoints()
    if args.diagnose:
        diagnose(Path(args.diagnose))
        return

    api_key = os.environ.get("MSTOCK_API_KEY", "").strip()
    if not api_key:
        sys.exit("MSTOCK_API_KEY missing (env or .env)")

    day = date.fromisoformat(args.date) if args.date else now().date()
    out_dir = Path(args.out) / day.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, _on_sigint)
    _ALLOW_SINGLE["v"] = bool(args.allow_single_sigint)
    for name in ("SIGTERM", "SIGQUIT", "SIGHUP", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, _on_signal)
            except (OSError, ValueError):      # not supported on this platform/thread
                pass

    if day.weekday() >= 5 and not args.test:
        say(f"WARNING: {day} is a weekend, exchanges are closed.")

    token = login(api_key)
    if args.login_check:
        say(f"login OK — token {token[:4]}…{token[-4:]} ({len(token)} chars)")
        say("credentials work. Nothing else was called.")
        return
    headers = make_headers(api_key, token)
    rows = load_scriptmaster(headers, out_dir, day)
    say(f"scriptmaster: {len(rows)} instruments")

    if args.find:
        q = norm(args.find)
        hits = [r for r in rows if q in norm(r["tradingsymbol"]) or q in norm(r.get("name"))]
        for r in hits[:60]:
            print(f'{r["instrument_token"]:>8} {r["exchange"]:<4} {r["tradingsymbol"]:<28} '
                  f'{r.get("name", "")[:24]:<24} {r.get("instrument_type", ""):<8} {r.get("segment", ""):<8} '
                  f'{r.get("expiry", ""):<12} {r.get("strike", "")}')
        print(f"{len(hits)} match(es){' (first 60 shown)' if len(hits) > 60 else ''}")
        return

    overrides = json.loads(Path(args.overrides).read_text()) if args.overrides else {}
    selected = [i.strip().upper() for i in args.indices.split(",") if i.strip()]
    bad = [i for i in selected if i not in INDEX_CFG]
    if bad:
        sys.exit(f"unknown index {bad}; choose from {list(INDEX_CFG)}")

    # ---- build universe -------------------------------------------------------
    res = Resolver(rows, now().date() if args.test else day, overrides)
    uni = Universe()
    for idx in selected:
        uni.add(res.spot(idx))
        uni.add(res.future(idx))
        res.prepare_options(idx)
    for s in [x.strip() for x in args.stocks.split(",") if x.strip()]:
        uni.add(res.stock(s))

    bse_syms = [x.strip() for x in args.bse_stocks.split(",") if x.strip()]
    if not bse_syms:
        bse_syms = (SENSEX_STOCKS if "SENSEX" in selected else []) + (BANKEX_STOCKS if "BANKEX" in selected else [])
    for s_ in dict.fromkeys(bse_syms):
        uni.add(res.bse_stock(s_))

    for ex_, lst in res.missing.items():
        if lst:
            res.notes.append(f"{ex_} stocks not found in scriptmaster ({len(lst)}): {', '.join(lst)} (check with --find)")

    if args.opt_indices == "auto":
        # indices expiring today + SENSEX/BANKEX always (recording them is cheap); NIFTY too on a non-expiry day
        expiring = [i for i in selected if res.opt_expiry.get(i) == res.today]
        always = [i for i in selected if i in ("SENSEX", "BANKEX")]
        opt_idx = list(dict.fromkeys(expiring + always + ([] if expiring else [i for i in selected if i == "NIFTY"])))
    else:
        opt_idx = [i.strip().upper() for i in args.opt_indices.split(",") if i.strip()]

    sess = requests.Session()
    index_items = {i["index"]: i for i in uni.by_token.values() if i["role"] == "index"}
    fut_items = {i["index"]: i for i in uni.by_token.values() if i["role"] == "future"}
    baseline: dict[str, dict] = {}
    baseline_stock: dict[str, float] = {}
    weights = {k: {norm(a): b for a, b in v.items()} for k, v in DEFAULT_WEIGHTS.items()}
    if args.weights:
        for k, v in json.loads(Path(args.weights).read_text()).items():
            weights[k.upper()] = {norm(a): float(b) for a, b in v.items()}
    def stock_key(item: dict) -> str:
        """'NSE:HDFCBANK' and BSE's 'HDFCBANK-A' must resolve to the same weight key."""
        sym = item["symbol"]
        if item["exchange"] == "BSE":
            sym = sym.split("-")[0]
        return norm(sym).removesuffix("EQ")

    stock_items = {stock_key(i): i for i in uni.by_token.values() if i["role"] == "stock"}

    def refresh_options(spots: dict[str, float]) -> int:
        added, new_tokens = 0, []
        for idx in opt_idx:
            sp = spots.get(idx)
            if sp:
                for it in res.options_around(idx, sp, args.strikes):
                    if uni.add(it):
                        added += 1
                        new_tokens.append(it["token"])
        if new_tokens and feed is not None:
            feed.subscribe(new_tokens)
        return added

    def spots_from(data: dict) -> dict[str, float]:
        out = {}
        for idx, it in index_items.items():
            v = data.get(it["key"])
            if v and v.get("last_price"):
                out[idx] = float(v["last_price"])
        return out

    # ---- writers + meta -------------------------------------------------------
    main_path = out_dir / f"cas_{day.isoformat()}.jsonl"
    require_disk(out_dir)
    out = JsonlWriter(main_path)
    raw = JsonlWriter(out_dir / f"cas_{day.isoformat()}.ws_raw.jsonl")
    _WRITERS.update(out=out, raw=raw, path=main_path, done=False)
    atexit.register(final_flush)
    install_crash_hook(out_dir)
    _SIGINT_EVENT["fn"] = out.write
    heartbeat = Heartbeat(out)
    heartbeat.start()
    feed: WSFeed | None = None

    pre, errs = fetch_ohlc(sess, headers, uni.keys())
    say(f"REST preflight: {len(pre)}/{len(uni.keys())} symbols answered"
        + (f" | errors: {errs[:2]}" if errs else ""))
    spots0 = spots_from(pre)
    missing_spot = [i for i in selected if i not in spots0]
    if missing_spot:
        res.notes.append(f"no REST price for index spot(s) {missing_spot} at startup (options for them are added later)")
    refresh_options(spots0)

    div = 100.0 if args.price_div == "auto" else float(args.price_div)
    endian = "<" if args.little_endian else ">"
    prefer = "fixed" if args.layout == "auto" else args.layout
    feed = WSFeed(api_key, token, uni, out, raw, endian, div, args.full_depth, prefer)
    feed.subscribe(uni.tokens())

    out.write({"type": "meta", "t": ts(), "version": VERSION, "date": day.isoformat(),
               "args": {k: v for k, v in vars(args).items()}, "universe": uni.count_by_role(),
               "opt_indices": opt_idx, "opt_expiry": {k: (v.isoformat() if v else None) for k, v in res.opt_expiry.items()},
               "weights": {"source": "approximate, from earlier research chat, unverified", "values": weights},
               "notes": res.notes, "unparsed_expiry_samples": res.unparsed_expiry_samples,
               "sample_symbols": {r: [i["key"] for i in uni.by_token.values() if i["role"] == r][:3]
                                  for r in ("index", "future", "option", "stock")},
               "preflight_missing": sorted(set(uni.keys()) - set(pre))[:40]})
    say("universe: " + ", ".join(f"{k}={v}" for k, v in uni.count_by_role().items())
        + f" | options for {opt_idx} (expiry {[str(res.opt_expiry.get(i)) for i in opt_idx]})")
    for n in res.notes:
        say("NOTE: " + n)

    poller = RestPoller(headers, uni, out, args.rest_interval)
    poller.start()

    # ---- schedule -------------------------------------------------------------
    base_label = args.baseline
    if args.test:
        t0 = now()
        base_label = "test_start"
        rec_start, rec_end = t0 + timedelta(seconds=8), t0 + timedelta(seconds=8 + args.test)
        snaps = [(t0 + timedelta(seconds=4), "test_start"), (rec_end + timedelta(seconds=2), "test_end")]
        connect_at = t0
        decide_dt = rec_end - timedelta(seconds=2)
    else:
        rec_start, rec_end = at(day, args.rec_start), at(day, args.rec_end)
        snaps = [(at(day, h), h) for h in [s.strip() for s in args.snapshots.split(",") if s.strip()]]
        connect_at = at(day, args.connect_at)
        decide_dt = at(day, args.decide_at)
        if decide_dt >= at(day, "15:28:00"):
            say("WARNING: --decide-at is not before 15:28:00; the random close can start then.")
        if base_label not in [l for _, l in snaps]:
            say(f"WARNING: baseline label {base_label} is not in --snapshots; live readout will have no baseline")
        if now() > rec_end:
            sys.exit(f"recording window ({args.rec_start}-{args.rec_end}) already over today. "
                     f"Use --test 120 to test now, or --date for a different day label.")

    def status() -> None:
        say(f"ws frames={feed.n_frames} pkts={feed.n_pkts} tokens_seen={len(feed.latest)}/{len(uni.tokens())} "
            f"unmapped={feed.n_unmapped} | rest ok={poller.n_ok} err={poller.n_err} "
            f"| {'RECORDING' if feed.recording.is_set() else 'idle'}")

    def px(tok: int):
        d = feed.latest.get(tok)
        return d["ltp"] / feed.div if d and d.get("ltp") else None

    def weighted_est(idx: str, spot_base: float) -> dict | None:
        """Cross-check only. Move of the tracked constituents vs their 3:15 price, weighted and
        re-normalised over the constituents that have data (so it extrapolates the tracked top
        names to the whole index and can overstate), applied to the index spot. The *_raw_pts
        values are NOT re-normalised (they understate when coverage is low): the truth sits between.
        'ltp' uses the last traded price (frozen during CAS), 'mid' uses best bid/ask mid, which
        may or may not reflect auction orders: Monday's log decides if either is useful."""
        W = weights.get(idx)
        if not W:
            return None
        nl = dl = nm = dm = 0.0
        for sym, w in W.items():
            it, base = stock_items.get(sym), baseline_stock.get(sym)
            d = feed.latest.get(it["token"]) if it else None
            if not d or not base or not d.get("ltp"):
                continue
            nl += w * (d["ltp"] / feed.div / base - 1)
            dl += w
            if d.get("bids") and d.get("asks") and d["bids"][0][1] > 0 and d["asks"][0][1] > 0:
                mid = (d["bids"][0][1] + d["asks"][0][1]) / 2 / feed.div
                nm += w * (mid / base - 1)
                dm += w
        if not dl:
            return None
        return {"ltp_pts": round(spot_base * nl / dl, 2),
                "mid_pts": round(spot_base * nm / dm, 2) if dm else None,
                "ltp_raw_pts": round(spot_base * nl, 2), "mid_raw_pts": round(spot_base * nm, 2) if dm else None,
                "coverage": round(dl, 3), "approx_weights": True}

    def live_panel(final: bool = False) -> None:
        try:
            _live_panel(final)
        except Exception as exc:  # noqa: BLE001
            note_error("live_panel", exc, out)

    def _live_panel(final: bool = False) -> None:
        """Descriptive readout: where Futures moved since the frozen 3:15 baseline, the implied spot
        target (assumes the futures-spot basis is unchanged), and the nearest strike that would be ITM
        at that target with its current price vs the value it would have at the target. This is NOT a
        prediction: if the ask is already near the intrinsic value, the market has priced it in."""
        rows, lines = [], []
        for idx in selected:
            b, fi = baseline.get(idx), fut_items.get(idx)
            if not b or not fi or b.get("fut") is None:
                continue
            fnow = px(fi["token"])
            if fnow is None:
                continue
            move = fnow - b["fut"]
            target = b["spot"] + move
            with MANUAL_LOCK:
                manual = MANUAL.get(idx)
            if manual:
                move = manual - b["spot"]
                target = manual
            computed_target = b["spot"] + (fnow - b["fut"])
            row = {"index": idx, "spot_base": b["spot"], "fut_move": round(move, 2), "target": round(target, 2),
                   "target_source": "manual" if manual else "futures",
                   "computed_target": round(computed_target, 2)}
            cur = feed.latest.get(fi["token"])
            md, mp = (tick_mid(cur, feed.div), tick_micro(cur, feed.div)) if cur else (None, None)
            pcp, pcp_n = parity_forward(uni, feed.latest, idx, feed.div)
            calc = {}
            if md and b.get("mid"):
                calc["mid"] = round(md - b["mid"], 2)
            if mp and b.get("micro"):
                calc["micro"] = round(mp - b["micro"], 2)
            if pcp and b.get("pcp"):
                calc["pcp"] = round(pcp - b["pcp"], 2)
                calc["pcp_strikes"] = pcp_n
            if calc:
                row["calc"] = calc
            opts = [o for o in uni.by_token.values() if o["role"] == "option" and o["index"] == idx]
            line = f"{idx:10s} base {b['spot']:>9.1f} | fut {move:+7.1f} -> target {target:>9.1f}"
            if opts and move != 0:
                side = "CE" if move > 0 else "PE"
                cand = [o for o in opts if o["otype"] == side
                        and (o["strike"] <= target if side == "CE" else o["strike"] >= target)]
                if cand:
                    o = max(cand, key=lambda x: x["strike"]) if side == "CE" else min(cand, key=lambda x: x["strike"])
                    d = feed.latest.get(o["token"]) or {}
                    ask = (d["asks"][0][1] / feed.div) if d.get("asks") and d["asks"][0][1] else None
                    ltp = px(o["token"])
                    intr = (target - o["strike"]) if side == "CE" else (o["strike"] - target)
                    row.update(side=side, strike=o["strike"], opt_ltp=ltp, opt_ask=ask, intrinsic_at_target=round(intr, 2))
                    line += f" | {side} {o['strike']:.0f} ltp {ltp} ask {ask} value@target {intr:.1f}"
                else:
                    line += f" | no {side} strike inside the subscribed range"
            if calc:
                extra = " ".join(f"{k} {v:+.1f}" for k, v in calc.items() if k != "pcp_strikes")
                line += f" | calc {extra}"
            we = weighted_est(idx, b["spot"])
            if we:
                row["weighted"] = we
                mid = f"{we['mid_pts']:+.1f}" if we["mid_pts"] is not None else "n/a"
                line += f" | wtd(approx) ltp {we['ltp_pts']:+.1f} mid {mid} cov {we['coverage']}"
            rows.append(row)
            dec = None
            if fi and b.get("spot") is not None:
                try:
                    dec = decide(idx, move, b["spot"], target,
                                 row.get("strike"), row.get("opt_ask"))
                    row["decision"] = dec
                except Exception:
                    dec = None
            lines.append(line)
            if dec:
                tail = (f" -> {dec['side']} {dec['strike']:.0f}"
                        + (f" ask {dec['ask']} value@target {dec['intrinsic_at_target']}"
                           f" edge {dec['edge']:+.2f}" if "edge" in dec else " (not subscribed)")
                        + ("" if dec["tradeable"] else f"  SKIP: {dec['note']}"))
                lines.append(f"             {tail}")
        if not rows:
            return
        head = "FINAL READOUT" if final else "live"
        say(f"--- {head} (act before 15:28:00; futures-implied, NOT a guarantee) ---")
        for l in lines:
            print("   " + l, flush=True)
        if final or feed.recording.is_set():
            out.write({"type": "readout", "t": ts(), "final": final, "rows": rows})

    def live_cb() -> None:
        safe_call("live_readout", live_panel, out, False)

    def snapshot(label: str) -> None:
        try:
            _snapshot(label)
        except Exception as exc:  # noqa: BLE001
            note_error(f"snapshot[{label}]", exc, out)

    def _snapshot(label: str) -> None:
        data, errs = safe_call("snapshot_fetch", fetch_ohlc, out, sess, headers, uni.keys()) or ({}, [])
        sp = safe_call("snapshot_spots", spots_from, out, data) or {}
        if label == base_label:
            for idx, it in index_items.items():
                spot = (data.get(it["key"]) or {}).get("last_price")
                fi = fut_items.get(idx)
                fut = (data.get(fi["key"]) or {}).get("last_price") if fi else None
                if spot:
                    base_tick = feed.latest.get(fi["token"]) if fi else None
                    pcp, pcp_n = parity_forward(uni, feed.latest, idx, feed.div)
                    baseline[idx] = {"spot": float(spot), "fut": float(fut) if fut else None,
                                     "mid": tick_mid(base_tick, feed.div) if base_tick else None,
                                     "micro": tick_micro(base_tick, feed.div) if base_tick else None,
                                     "pcp": pcp, "pcp_n": pcp_n}
            for sym, it in stock_items.items():
                lp = (data.get(it["key"]) or {}).get("last_price")
                if lp:
                    baseline_stock[sym] = float(lp)
            say("baseline locked: " + ", ".join(f"{k} {v['spot']:.1f}" for k, v in baseline.items()))
        added = (safe_call("snapshot_options", refresh_options, out, sp) or 0) if now() < rec_start else 0
        if args.price_div == "auto" and not feed.recording.is_set():
            d, ratios = infer_divisor(feed, data, uni)
            if d:
                feed.div = d
            out.write({"type": "event", "t": ts(), "name": "divisor_check", "inferred": d,
                       "ratios": ratios, "using": feed.div})
        ws_now = {str(tok): {"t": feed.latest_t.get(tok), **scale(d, feed.div, True)}
                  for tok, d in feed.latest.items()}
        out.write({"type": "snapshot", "t": ts(), "label": label, "rest": data,
                   "map": {i["key"]: {"index": i["index"], "role": i["role"]} for i in uni.by_token.values()},
                   "ws_latest": ws_now, **({"errors": errs} if errs else {})})
        out.flush()
        say(f"snapshot '{label}': rest {len(data)}/{len(uni.keys())}, ws tokens {len(feed.latest)}"
            + (f", +{added} option tokens" if added else ""))

    if not args.no_dashboard:
        start_dashboard(out.path, host=args.dash_host, port=args.dash_port,
                        open_browser=not args.no_open)

    say(f"plan: connect {connect_at:%H:%M:%S} | record {rec_start:%H:%M:%S}-{rec_end:%H:%M:%S} | "
        f"snapshots {[l for _, l in snaps]}")
    sleep_until(connect_at, status)
    if STOP.is_set():
        return
    feed.start()
    _SIGINT["armed"] = True          # protected from here to the end of the recording window
    _SIGINT_EVENT["fn"] = out.write
    time.sleep(3)
    say("WebSocket " + ("connected" if feed.connected.is_set() else "NOT connected yet (check .env / token)"))

    def watchdog() -> None:
        """During the recording window, report silence every 30s instead of only at start."""
        while not STOP.is_set():
            if feed.recording.is_set():
                time.sleep(30)
                if STOP.is_set() or not feed.recording.is_set():
                    continue
                if feed.n_pkts == 0:
                    say(f"!!! STILL SILENT: {feed.n_frames} frames, 0 packets parsed "
                        f"({feed.n_text} text msgs). Layouts={dict(feed.layouts)}")
                else:
                    say(f"watchdog: frames={feed.n_frames} pkts={feed.n_pkts} "
                        f"unmapped={feed.n_unmapped} parse_fail={feed.n_parse_fail} "
                        f"rest={poller.n_ok}/{poller.n_err}")
            else:
                time.sleep(2)

    if not args.no_live:
        threading.Thread(target=watchdog, daemon=True, name="watchdog").start()
        try:
            if sys.stdin and sys.stdin.isatty():
                threading.Thread(target=watch_stdin, args=(selected,), daemon=True,
                                 name="manual-indicative").start()
                say("type a manual indicative close any time (e.g. 'NIFTY 25065') to override the estimate")
        except Exception:
            pass

    events = [(t, "snap", l) for t, l in snaps] + [(rec_start, "start", ""), (rec_end, "stop", "")]
    if not args.no_live:
        events.append((decide_dt, "decide", ""))
    events.sort(key=lambda e: e[0])
    for t, kind, label in events:
        if STOP.is_set():
            break
        if t < now() - timedelta(seconds=15) and kind != "stop":
            # The baseline is the whole measurement. If the run starts late (say 15:15), that
            # snapshot is already in the past: take it now, after a short warm-up so ticks exist.
            if needs_late_baseline(kind, label, base_label, bool(baseline), t, now()):
                say(f"late start: the {label} baseline is in the past — taking it now after a 6s warm-up")
                time.sleep(6)
                safe_call(f"late_baseline[{label}]", snapshot, out, label)
                continue
            say(f"skipping {kind} {label or ''} (time already passed)")
            if kind == "start":
                feed.recording.set()
                if not args.no_rest:
                    poller.enabled.set()
            continue
        if feed.recording.is_set() and not args.no_live:
            sleep_until(t, live_cb, args.live_every)
        else:
            sleep_until(t, status)
        if STOP.is_set():
            break
        if kind == "decide":
            safe_call("final_readout", live_panel, out, True)
        elif kind == "snap":
            safe_call(f"snapshot_step[{label}]", snapshot, out, label)
        elif kind == "start":
            feed.recording.set()
            if not args.no_rest:
                poller.enabled.set()
            out.write({"type": "event", "t": ts(), "name": "recording_start"})
            _SIGINT["armed"] = True
            _SIGINT_EVENT["fn"] = out.write
            say(">>> RECORDING STARTED")
            if feed.n_pkts == 0:
                say("!!! ALARM: 0 packets parsed at recording start — the feed is silent or the "
                    "packet layout is wrong. The log WILL be empty for this window.")
                out.write({"type": "event", "t": ts(), "name": "feed_silent",
                           "frames": feed.n_frames, "pkts": feed.n_pkts,
                           "text_msgs": feed.n_text, "layouts": dict(feed.layouts)})
            elif feed.n_unmapped and feed.n_unmapped > feed.n_pkts * 0.5:
                say(f"!!! ALARM: {feed.n_unmapped}/{feed.n_pkts} packets are unmapped tokens — "
                    f"the feed is probably using exchange tokens; check the meta record.")
                out.write({"type": "event", "t": ts(), "name": "tokens_unmapped",
                           "unmapped": feed.n_unmapped, "pkts": feed.n_pkts})
        else:
            feed.recording.clear()
            poller.enabled.clear()
            _SIGINT["armed"] = False
            out.write({"type": "event", "t": ts(), "name": "recording_stop"})
            out.flush()
            raw.flush()
            say("<<< RECORDING STOPPED")

    # ---- wrap up --------------------------------------------------------------
    feed.recording.clear()
    out.write({"type": "event", "t": ts(), "name": "end", "frames": feed.n_frames, "pkts": feed.n_pkts,
               "unmapped_pkts": feed.n_unmapped, "parse_fail": feed.n_parse_fail, "text_msgs": feed.n_text,
               "layouts": dict(feed.layouts), "rest_ok": poller.n_ok, "rest_err": poller.n_err,
               "events_suppressed": dict(feed._suppressed), "errors": len(ERRORS)})
    STOP.set()
    feed.stop()
    heartbeat.stop()
    time.sleep(0.5)
    _WRITERS["done"] = True
    out.close()
    raw.close()
    say(f"done. log: {main_path}")
    say(f"next: python {Path(sys.argv[0]).name} --summarize {main_path}")


if __name__ == "__main__":
    main()
