#!/usr/bin/env python3
"""Builds a demo CAS log with all six indices, so the dashboard can be previewed
(and the decision maths exercised) without waiting for a live session.

Each index gets: a 15:15:10 baseline, futures ticks drifting by a chosen gap,
constituent stock packets with a buy/sell lean, live option quotes at the strike
the dashboard will pick, and a post-close snapshot for the realised number.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
D = datetime(2026, 11, 23, 15, 15, 10, tzinfo=IST)

# index: (spot, gap points, step, constituents [(name, weight-ish)])
BOOK = {
    "NIFTY":      (25000.0,  78.0, 50,  [("HDFCBANK", 900, 120), ("RELIANCE", 400, 150), ("INFY", 160, 140),
                                         ("TCS", 90, 500), ("ITC", 60, 380)]),
    "BANKNIFTY":  (58000.0, -210.0, 100, [("HDFCBANK", 120, 900), ("ICICIBANK", 100, 700),
                                          ("SBIN", 80, 600), ("KOTAKBANK", 90, 500), ("AXISBANK", 70, 540)]),
    "FINNIFTY":   (27500.0,  55.0, 50,  [("HDFCBANK", 700, 200), ("ICICIBANK", 500, 260),
                                         ("BAJFINANCE", 300, 180), ("KOTAKBANK", 200, 220)]),
    "MIDCPNIFTY": (12500.0, -30.0, 25,  [("MAXHEALTH", 80, 140), ("INDHOTEL", 90, 160),
                                         ("PERSISTENT", 70, 150), ("BSE", 60, 130)]),
    "SENSEX":     (80500.0, 140.0, 100, [("HDFCBANK", 800, 150), ("RELIANCE", 500, 160),
                                         ("ICICIBANK", 420, 200), ("INFY", 300, 240), ("LT", 180, 300)]),
    "BANKEX":     (58000.0,   0.0, 100, [("HDFCBANK", 300, 300), ("ICICIBANK", 290, 300),
                                         ("SBIN", 250, 250), ("KOTAKBANK", 240, 240)]),
}
EXCH = {"NIFTY": ("NSE", "NFO"), "BANKNIFTY": ("NSE", "NFO"), "FINNIFTY": ("NSE", "NFO"),
        "MIDCPNIFTY": ("NSE", "NFO"), "SENSEX": ("BSE", "BFO"), "BANKEX": ("BSE", "BFO")}


def main(path: Path = Path("_dash_demo.jsonl"), seed: int = 5, steps: int = 140, step_s: int = 3):
    rnd = random.Random(seed)
    recs, mp = [], {}
    counter = {"n": 1000}

    def next_tok():
        counter["n"] += 1
        return counter["n"]

    stable: dict[str, int] = {}

    def tok_for(key: str) -> int:
        """One token per instrument, for the whole log — the dashboard keys state by token."""
        if key not in stable:
            stable[key] = next_tok()
        return stable[key]

    snap_base = {"rest": {}, "map": mp}
    opt_plan = {}
    for idx, (spot, gap, step_sz, cons) in BOOK.items():
        spot_ex, deriv_ex = EXCH[idx]
        fut = round(spot * 1.0016, 2)
        mp[f"{spot_ex}:{idx}"] = {"index": idx, "role": "index"}
        mp[f"{deriv_ex}:{idx}FUT"] = {"index": idx, "role": "future"}
        snap_base["rest"][f"{spot_ex}:{idx}"] = {"instrument_token": str(next_tok()), "last_price": spot}
        snap_base["rest"][f"{deriv_ex}:{idx}FUT"] = {"instrument_token": str(next_tok()), "last_price": fut}
        # the strike the dashboard will land on, plus the neighbours
        side = "CE" if gap > 0 else "PE"
        target = spot + gap
        import math
        k = (math.floor(target / step_sz) * step_sz) if side == "CE" else (math.ceil(target / step_sz) * step_sz)
        opt_plan[idx] = {"strikes": [k - step_sz, k, k + step_sz], "side": side, "step": step_sz,
                         "fut0": fut, "gap": gap}

    recs.append({"type": "meta", "t": D.isoformat(), "version": "1.0",
                 "universe": {"index": 6, "future": 6, "stock": 20, "option": 36}, "notes": []})
    recs.append({"type": "snapshot", "t": D.isoformat(), "label": "15:15:10", **snap_base})

    # union of every constituent, emitted ONCE per tick: in the real feed one stock packet
    # exists, and every index whose weight table contains that name reads the same flow
    union: dict[str, tuple[int, int, str]] = {}
    for idx, (spot, gap, step_sz, cons) in BOOK.items():
        spot_ex, _ = EXCH[idx]
        for name, buy_rate, sell_rate in cons:
            if name not in union:
                union[name] = (buy_rate, sell_rate, spot_ex)

    t0 = D + timedelta(minutes=5)
    for b in range(steps + 1):
        t = t0 + timedelta(seconds=b * step_s)
        frac = b / steps
        # one stock tape per timestamp, before the per-index derivative ticks
        for name, (buy_rate, sell_rate, spot_ex) in union.items():
            lean = 1.0 if BOOK["NIFTY"][1] > 0 else -1.0
            buy_qty = int(buy_rate * b * step_s * (1 + lean * 0.6))
            sell_qty = int(sell_rate * b * step_s * (1 - lean * 0.5))
            sym = f"{name}-A" if spot_ex == "BSE" else name
            px = 100.0 + (abs(hash(name)) % 400) / 10.0
            recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok_for(f"STOCK|{name}"),
                         "sym": f"{spot_ex}:{sym}", "role": "stock", "idx": None,
                         "ltp": round(px, 2), "tbq": buy_qty, "tsq": sell_qty,
                         "bids": [[1200, round(px - 0.05, 2), 4]], "asks": [[1200, round(px + 0.05, 2), 4]]})
        for idx, (spot, gap, step_sz, cons) in BOOK.items():
            spot_ex, deriv_ex = EXCH[idx]
            plan = opt_plan[idx]
            fut = plan["fut0"] + gap * frac
            recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok_for(f"{idx}|FUT"),
                         "sym": f"{deriv_ex}:{idx}FUT", "role": "future", "idx": idx,
                         "ltp": round(fut, 2), "vol": 100000 + b * 400,
                         "bids": [[400, round(fut - 0.05, 2), 3]], "asks": [[400, round(fut + 0.05, 2), 3]]})
            # live option quotes, premium ~ intrinsic + time value that decays as the gap grows
            for j, k in enumerate(plan["strikes"]):
                for ot in ("CE", "PE"):
                    intr = max(0.0, (fut - k) if ot == "CE" else (k - fut))
                    time_value = 12.0 * (1.0 - 0.85 * frac)
                    if idx in ("BANKEX", "BANKNIFTY") and ot == plan["side"]:
                        quotes = max(1.0, intr - 3.0)      # stale/illiquid quote below intrinsic
                    else:
                        quotes = intr + time_value
                    recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok_for(f"{idx}|{int(k)}|{ot}"),
                                 "sym": f"{deriv_ex}:{idx}{int(k)}{ot}", "role": "option", "idx": idx,
                                 "k": float(k), "ot": ot, "ltp": round(quotes, 2),
                                 "bids": [[40, round(quotes - 0.5, 2), 2]],
                                 "asks": [[40, round(quotes + 0.5, 2), 2]]})

    post = {"rest": {}, "map": mp}
    for idx, (spot, gap, step_sz, cons) in BOOK.items():
        spot_ex, deriv_ex = EXCH[idx]
        post["rest"][f"{spot_ex}:{idx}"] = {"instrument_token": "1", "last_price": spot + gap}
        post["rest"][f"{deriv_ex}:{idx}FUT"] = {"instrument_token": "2", "last_price": opt_plan[idx]["fut0"] + gap}
    recs.append({"type": "snapshot", "t": (t0 + timedelta(seconds=steps * step_s + 240)).isoformat(),
                 "label": "15:31:00", **post})
    path.write_text("\n".join(json.dumps(r) for r in recs) + "\n")
    return path


if __name__ == "__main__":
    p = main()
    print("wrote", p, "|", sum(1 for _ in p.open()), "records")
