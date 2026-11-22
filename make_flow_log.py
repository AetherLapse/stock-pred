#!/usr/bin/env python3
"""Synthetic CAS log with a KNOWN order-flow story, to test cas_flow.py.

Story: HDFCBANK is being accumulated hard, RELIANCE mildly bid, TCS and ITC are being dumped.
The close settles +78 (up), i.e. the flow read is supposed to get the direction right.

Scenarios:
  flow_up    heavy buy imbalance on the heavyweights, close +78
  flow_down  the mirror image, close -78
  flow_noise random imbalances, close +12 (should be a coin-flip, and that is the point)
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
D = datetime(2026, 11, 23, 15, 15, 10, tzinfo=IST)
SPOT = 25000.0

# symbol, weight, starting price, buy_qty rate, sell_qty rate (qty/second during CAS)
BOOK = {
    "flow_up": [("HDFCBANK", 0.115, 1500.0, 900, 120), ("RELIANCE", 0.092, 2900.0, 400, 150),
                ("INFY", 0.060, 1600.0, 160, 140), ("TCS", 0.038, 3900.0, 90, 500),
                ("ITC", 0.041, 430.0, 60, 380)],
    "flow_down": [("HDFCBANK", 0.115, 1500.0, 120, 900), ("RELIANCE", 0.092, 2900.0, 150, 400),
                  ("INFY", 0.060, 1600.0, 140, 160), ("TCS", 0.038, 3900.0, 500, 90),
                  ("ITC", 0.041, 430.0, 380, 60)],
    "flow_noise": [("HDFCBANK", 0.115, 1500.0, 300, 300), ("RELIANCE", 0.092, 2900.0, 250, 260),
                   ("INFY", 0.060, 1600.0, 200, 210), ("TCS", 0.038, 3900.0, 220, 200),
                   ("ITC", 0.041, 430.0, 180, 190)],
}
JUMP = {"flow_up": 78.0, "flow_down": -78.0, "flow_noise": 12.0}


def build(path: Path, scenario: str, seed: int = 11, seconds: int = 720, step: int = 3):
    rnd = random.Random(seed)
    stocks = BOOK[scenario]
    jump = JUMP[scenario]
    recs = []
    mp = {"NSE:NIFTY 50": {"index": "NIFTY", "role": "index"},
          "NFO:NIFTY26NOVFUT": {"index": "NIFTY", "role": "future"}}
    uni = {"index": 1, "future": 1, "stock": len(stocks), "option": 0}
    for i, (sym, _, _, _, _) in enumerate(stocks):
        mp[f"NSE:{sym}"] = {"index": "NIFTY" if False else None, "role": "stock"}
        uni["stock"] = len(stocks)
    recs.append({"type": "meta", "t": D.isoformat(), "version": "1.0", "universe": uni,
                 "notes": [], "weights": {"values": {}}})

    recs.append({"type": "snapshot", "t": D.isoformat(), "label": "15:15:10",
                 "rest": {"NSE:NIFTY 50": {"instrument_token": "1", "last_price": SPOT},
                          "NFO:NIFTY26NOVFUT": {"instrument_token": "2", "last_price": SPOT + 40}},
                 "map": mp, "ws_latest": {}})

    # 15:20 -> 15:27:30: cash LTP frozen, auction book + depth move
    t0 = D + timedelta(minutes=5)
    n_bars = seconds // step
    for b in range(n_bars + 1):
        t = t0 + timedelta(seconds=b * step)
        for i, (sym, _, px0, buy_rate, sell_rate) in enumerate(stocks):
            tok = 300000 + i
            # cumulative auction quantities grow linearly; depth leans the same way
            buy_qty = int(buy_rate * b * step * (1 + rnd.uniform(-0.1, 0.1)))
            sell_qty = int(sell_rate * b * step * (1 + rnd.uniform(-0.1, 0.1)))
            lean = (buy_rate - sell_rate) / max(1, buy_rate + sell_rate)
            depth_scale = 1000 + b * 2
            bid1 = int(depth_scale * (1 + lean))
            ask1 = int(depth_scale * (1 - lean))
            recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok, "sym": f"NSE:{sym}",
                         "role": "stock", "idx": None, "ltp": px0,          # frozen
                         "tbq": buy_qty, "tsq": sell_qty,
                         "bids": [[max(10, bid1), px0 - 0.05, 5]], "asks": [[max(10, ask1), px0 + 0.05, 5]]})
        # futures keep trading: signed volume leans with the flow story
        f = b / n_bars
        fut = SPOT + 40 + jump * f
        vol = 100000 + b * 900
        recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": 2, "sym": "NFO:NIFTY26NOVFUT",
                     "role": "future", "idx": "NIFTY", "ltp": round(fut, 2), "vol": vol,
                     "bids": [[500, round(fut - 0.05, 2), 4]], "asks": [[500, round(fut + 0.05, 2), 4]]})

    recs.append({"type": "snapshot", "t": (t0 + timedelta(seconds=seconds + 240)).isoformat(),
                 "label": "15:31:00",
                 "rest": {"NSE:NIFTY 50": {"instrument_token": "1", "last_price": SPOT + jump},
                          "NFO:NIFTY26NOVFUT": {"instrument_token": "2", "last_price": SPOT + 40 + jump}},
                 "map": mp, "ws_latest": {}})
    path.write_text("\n".join(json.dumps(r) for r in recs) + "\n")
    return path


if __name__ == "__main__":
    for sc in ("flow_up", "flow_down", "flow_noise"):
        print("wrote", build(Path(f"_flow_{sc}.jsonl"), sc))
