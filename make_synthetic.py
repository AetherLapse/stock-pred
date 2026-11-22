#!/usr/bin/env python3
"""Builds a synthetic CAS log with a KNOWN closing jump, so cas_analysis.py can be scored
against a ground truth instead of vibes.

Scenarios:
  clean     futures track the true implied level exactly, options consistent with it
  noisy     1 tick of futures noise + a stale option leg (illiquid strike)
  laggy     futures lag the move by ~90s (the realistic case: index futures thin out late)
  pcp_edge  futures stay stale but options price the real move (parity beats the future)
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

IST = None
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    pass

D = datetime(2026, 11, 23, 15, 15, 10, tzinfo=IST)
SPOT_BASE = 25000.0
JUMP = 78.0                    # realised CAS jump
FUT_BASE = 25040.0
STRIKES = [24900.0, 25000.0, 25100.0, 25200.0]


def opt_prices(forward):
    """Rough CRR-free approximation: intrinsic + a fixed time value that decays near expiry."""
    out = {}
    for k in STRIKES:
        intr_c = max(0.0, forward - k)
        intr_p = max(0.0, k - forward)
        tv = 28.0
        out[k] = {"CE": round(intr_c + tv, 2), "PE": round(intr_p + tv, 2)}
    return out


def build(path: Path, scenario="clean", seed=7, n_fut=90, spacing=3):
    rnd = random.Random(seed)
    recs = []
    mp = {"NSE:NIFTY 50": {"index": "NIFTY", "role": "index"},
          "NFO:NIFTY26NOVFUT": {"index": "NIFTY", "role": "future"}}
    recs.append({"type": "meta", "t": D.isoformat(), "version": "1.0",
                 "universe": {"index": 1, "future": 1, "option": len(STRIKES) * 2, "stock": 0},
                 "notes": [], "weights": {"values": {}}})
    recs.append({"type": "snapshot", "t": D.isoformat(), "label": "15:15:10",
                 "rest": {"NSE:NIFTY 50": {"instrument_token": "1", "last_price": SPOT_BASE},
                          "NFO:NIFTY26NOVFUT": {"instrument_token": "2", "last_price": FUT_BASE}},
                 "map": mp, "ws_latest": {}})

    total = n_fut * spacing
    for i in range(n_fut + 1):
        t = D + timedelta(seconds=i * spacing)
        frac = i / n_fut
        true_move = JUMP * frac                       # what the cash close will actually be

        if scenario == "laggy":
            fut_frac = 0.0 if frac < 0.85 else (frac - 0.85) / 0.15
        elif scenario == "pcp_edge":
            fut_frac = 0.0 if frac < 0.92 else (frac - 0.92) / 0.08   # future lags, options right
        else:
            fut_frac = frac

        fut = FUT_BASE + JUMP * fut_frac
        if scenario == "noisy":
            fut += rnd.uniform(-4, 4)
        recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": 2, "sym": "NFO:NIFTY26NOVFUT",
                     "role": "future", "idx": "NIFTY", "ltp": round(fut, 2), "tbq": 1200, "tsq": 1100,
                     "bids": [[40, round(fut - 0.05, 2), 3]], "asks": [[40, round(fut + 0.05, 2), 3]]})

        # options: priced off the TRUE forward in pcp_edge, off the future otherwise
        px = opt_prices(FUT_BASE + true_move) if scenario == "pcp_edge" else opt_prices(fut)
        for k in STRIKES:
            for ot in ("CE", "PE"):
                price = px[k][ot]
                if scenario == "noisy" and k == 25200.0:
                    price = opt_prices(FUT_BASE)[k][ot]      # never updates: illiquid
                recs.append({"type": "ws_tick", "t": t.isoformat(),
                             "tok": int(900000 + k + (0 if ot == "CE" else 1)),
                             "sym": f"NFO:NIFTY26NOV{int(k)}{ot}", "role": "option", "idx": "NIFTY",
                             "k": k, "ot": ot, "ltp": price,
                             "bids": [[10, round(price - 0.5, 2), 1]],
                             "asks": [[10, round(price + 0.5, 2), 1]]})

    recs.append({"type": "snapshot", "t": (D + timedelta(seconds=total + 240)).isoformat(),
                 "label": "15:31:00",
                 "rest": {"NSE:NIFTY 50": {"instrument_token": "1", "last_price": SPOT_BASE + JUMP},
                          "NFO:NIFTY26NOVFUT": {"instrument_token": "2",
                                                "last_price": FUT_BASE + JUMP}},
                 "map": mp, "ws_latest": {}})
    path.write_text("\n".join(json.dumps(r) for r in recs) + "\n")
    return path


if __name__ == "__main__":
    for sc in ("clean", "noisy", "laggy", "pcp_edge"):
        p = build(Path(f"_synthetic_{sc}.jsonl"), sc)
        print("wrote", p)
