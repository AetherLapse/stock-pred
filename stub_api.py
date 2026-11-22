#!/usr/bin/env python3
"""Minimal stand-in for mStock's Type A REST API, so cas_logger.py can run its
full schedule offline (--test mode). Serves the scriptmaster CSV and a drifting
OHLC response; nothing else is needed for the logger's read paths.
"""
from __future__ import annotations

import csv
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = 8771
MASTER = "_verify_master.csv"
BASE_PRICES = {
    "NSE:NIFTY 50": 25000.0, "NFO:NIFTY26NOVFUT": 25040.0,
    "BSE:SENSEX": 80500.0, "BFO:SENSEX26NOVFUT": 80600.0,
    "NSE:RELIANCE": 2900.0, "NSE:HDFCBANK": 1500.0,
}
ROWS = []
with open(MASTER, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        ROWS.append(r)
TOK = {f'{r["exchange"]}:{r["tradingsymbol"]}': r for r in ROWS}
drift = {k: v for k, v in BASE_PRICES.items()}
lock = threading.Lock()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path.endswith("/instruments/scriptmaster"):
            with open(MASTER, encoding="utf-8") as f:
                return self._send(200, f.read(), "text/csv")
        if u.path.endswith("/instruments/quote/ohlc"):
            keys = parse_qs(u.query).get("i", [])
            with lock:
                for k in list(drift):
                    drift[k] += random.uniform(-1.2, 1.4)
            data = {}
            for k in keys:
                row = TOK.get(k)
                if not row:
                    continue
                px = drift.get(k, 100.0)
                data[k] = {
                    "instrument_token": row["instrument_token"],
                    "last_price": round(px, 2),
                    "ohlc": {"open": round(px * 0.999, 2), "high": round(px * 1.002, 2),
                             "low": round(px * 0.998, 2), "close": round(px * 0.9995, 2)},
                }
            return self._send(200, json.dumps({"status": "success", "data": data}))
        return self._send(404, json.dumps({"status": "error", "message": "not found"}))


if __name__ == "__main__":
    print(f"stub mStock API on http://127.0.0.1:{PORT}/openapi/typea", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
