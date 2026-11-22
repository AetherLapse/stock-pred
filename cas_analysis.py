#!/usr/bin/env python3
"""cas_analysis.py - turn a CAS log into *calculated* estimates of the closing price.

The feed carries no indicative equilibrium price (confirmed against mStock's docs), so the
closing level has to be inferred from what does keep ticking between 15:15 and 15:30:

  fut_ltp     futures last price minus its 3:15 level            (basis unchanged)
  fut_mid     futures best bid/ask mid minus its 3:15 mid         (less jumpy than ltp)
  fut_micro   size-weighted microprice of level 1                (fair value of the top of book)
  pcp_fwd     put-call parity across strikes: F = K + C - P      (options keep trading)
  wtd_imb     weighted stock buy/sell imbalance (informational)  (auction book proxy)

Every estimator is scored against the realised CAS jump (the 15:31 spot snapshot), so after a few
expiries the log itself says which one is worth trusting, per index, instead of us assuming.

Usage
    python cas_analysis.py --log cas_logs/2026-11-23/cas_2026-11-23.jsonl
    python cas_analysis.py --logs cas_logs/*/*.jsonl --json out.json
"""
from __future__ import annotations

import argparse
import glob
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE_LABEL = "15:15:10"
TRUTH_LABELS = ("15:31:00", "test_end")


# ---------------------------------------------------------------- log loading
def load(path):
    recs = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line:
            try:
                recs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return recs


def parse_t(s):
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def series(recs, role):
    """token -> list of (t, tick) for a role, time ordered."""
    out = defaultdict(list)
    for r in recs:
        if r.get("type") != "ws_tick" or r.get("role") != role:
            continue
        t = parse_t(r.get("t"))
        if t is None:
            continue
        out[r["tok"]].append((t, r))
    for k in out:
        out[k].sort(key=lambda x: x[0])
    return out


def last_at(series_list, t):
    """Most recent tick at or before t (ticks arrive newest-last)."""
    found = None
    for ts, tick in series_list:
        if ts <= t:
            found = tick
        else:
            break
    return found


def best_bid_ask(tick):
    if not tick:
        return None, None
    bids = tick.get("bids") or []
    asks = tick.get("asks") or []
    bid = bids[0][1] if bids and len(bids[0]) > 1 else None
    ask = asks[0][1] if asks and len(asks[0]) > 1 else None
    return bid, ask


def mid(tick):
    bid, ask = best_bid_ask(tick)
    if bid and ask and bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


def microprice(tick):
    """Size-weighted fair value of level 1: bid side weighs the ask size."""
    bids = tick.get("bids") or []
    asks = tick.get("asks") or []
    if not bids or not asks or len(bids[0]) < 3 or len(asks[0]) < 3:
        return None
    bid_qty, bid_px = bids[0][0], bids[0][1]
    ask_qty, ask_px = asks[0][0], asks[0][1]
    if not all(isinstance(x, (int, float)) and x > 0 for x in (bid_qty, bid_px, ask_qty, ask_px)):
        return None
    return (bid_px * ask_qty + ask_px * bid_qty) / (bid_qty + ask_qty)


# ---------------------------------------------------------------- snapshots
def snapshot(recs, label):
    for r in recs:
        if r.get("type") == "snapshot" and r.get("label") == label:
            return r
    return None


def spot_of(snap, index):
    if not snap:
        return None
    for key, meta in (snap.get("map") or {}).items():
        if meta.get("index") == index and meta.get("role") == "index":
            v = (snap.get("rest") or {}).get(key)
            if v and v.get("last_price"):
                return float(v["last_price"])
    return None


def fut_of(snap, index):
    if not snap:
        return None
    for key, meta in (snap.get("map") or {}).items():
        if meta.get("index") == index and meta.get("role") == "future":
            v = (snap.get("rest") or {}).get(key)
            if v and v.get("last_price"):
                return float(v["last_price"])
    return None


# ---------------------------------------------------------------- estimators
def pcp_forward(opt_series, t):
    """Median implied forward across strikes from C - P parity (same-day expiry => F ~ S).

    Returns (forward, strikes_used). Needs a CE and a PE quote for the same strike."""
    by_strike = defaultdict(dict)
    for tok, sl in opt_series.items():
        tick = last_at(sl, t)
        if not tick:
            continue
        k = tick.get("k")
        ot = (tick.get("ot") or "").upper()
        m = mid(tick) or tick.get("ltp")
        if k is None or ot not in ("CE", "PE") or not m:
            continue
        by_strike[k][ot] = m
    fwds = [k + v["CE"] - v["PE"] for k, v in by_strike.items() if "CE" in v and "PE" in v]
    if not fwds:
        return None, 0
    return statistics.median(fwds), len(fwds)


def evaluate(path, sample_every_s=60):
    recs = load(path)
    base = snapshot(recs, BASE_LABEL)
    truth_snap = next((snapshot(recs, lbl) for lbl in TRUTH_LABELS if snapshot(recs, lbl)), None)
    if not base or not truth_snap:
        return {"log": str(path), "error": "missing 15:15:10 or post-close snapshot"}

    fut_series = series(recs, "future")
    opt_series = series(recs, "option")
    stock_series = series(recs, "stock")

    results = {"log": str(path), "index": {}}
    for index in sorted({m.get("index") for m in (base.get("map") or {}).values() if m.get("index")}):
        spot_b = spot_of(base, index)
        spot_t = spot_of(truth_snap, index)
        if spot_b is None or spot_t is None:
            continue
        truth = spot_t - spot_b

        # futures series for this index (ticks carry idx)
        fut_ticks = [(t, tk) for tok, sl in fut_series.items() for t, tk in sl if tk.get("idx") == index]
        fut_ticks.sort(key=lambda x: x[0])
        if not fut_ticks:
            results["index"][index] = {"truth": round(truth, 2), "error": "no futures ticks"}
            continue

        # baseline from the earliest tick at/after 15:15 (LTP, mid, micro)
        t_base = min(t for t, _ in fut_ticks)
        first = fut_ticks[0][1]
        base_ltp = first.get("ltp")
        base_mid = mid(first) or base_ltp
        base_micro = microprice(first) or base_mid
        base_fwd, _ = pcp_forward(opt_series, t_base)

        opts = [(t, tk) for tok, sl in opt_series.items() for t, tk in sl if tk.get("idx") == index]
        opts.sort(key=lambda x: x[0])
        if opts and base_fwd is None:
            base_fwd, n = pcp_forward(opt_series, opts[0][0])

        rows = []
        last_t = fut_ticks[-1][0]
        step = max(1, int(sample_every_s * len(fut_ticks) / max(1, (last_t - t_base).total_seconds())))
        for i in range(0, len(fut_ticks), step):
            t, tk = fut_ticks[i]
            ltp, md, mp = tk.get("ltp"), mid(tk), microprice(tk)
            row = {"t": t.strftime("%H:%M:%S")}
            if ltp and base_ltp:
                row["fut_ltp"] = round(ltp - base_ltp, 2)
            if md and base_mid:
                row["fut_mid"] = round(md - base_mid, 2)
            if mp and base_micro:
                row["fut_micro"] = round(mp - base_micro, 2)
            fwd, n = pcp_forward(opt_series, t)
            if fwd and base_fwd:
                row["pcp_fwd"] = round(fwd - base_fwd, 2)
                row["pcp_strikes"] = n
            row["err_fut_ltp"] = round((row.get("fut_ltp") or 0) - truth, 2) if "fut_ltp" in row else None
            rows.append(row)

        # the readout happens at the end of the window: never drop the last tick
        t_end, tk_end = fut_ticks[-1]
        if not rows or rows[-1]["t"] != t_end.strftime("%H:%M:%S"):
            ltp, md, mp = tk_end.get("ltp"), mid(tk_end), microprice(tk_end)
            last = {"t": t_end.strftime("%H:%M:%S")}
            if ltp and base_ltp:
                last["fut_ltp"] = round(ltp - base_ltp, 2)
            if md and base_mid:
                last["fut_mid"] = round(md - base_mid, 2)
            if mp and base_micro:
                last["fut_micro"] = round(mp - base_micro, 2)
            fwd, n = pcp_forward(opt_series, t_end)
            if fwd and base_fwd:
                last["pcp_fwd"] = round(fwd - base_fwd, 2)
                last["pcp_strikes"] = n
            rows.append(last)

        if not rows:
            results["index"][index] = {"truth": round(truth, 2), "error": "no usable samples"}
            continue

        # score: mean absolute error per estimator over the last third of the window
        tail = rows[len(rows) * 2 // 3:]
        scores = {}
        for key in ("fut_ltp", "fut_mid", "fut_micro", "pcp_fwd"):
            vals = [r[key] for r in tail if r.get(key) is not None]
            if vals:
                scores[key] = {
                    "mae": round(statistics.mean(abs(v - truth) for v in vals), 2),
                    "bias": round(statistics.mean(v - truth for v in vals), 2),
                    "samples": len(vals),
                }
        final_row = rows[-1]
        at_readout = {k: round(final_row[k] - truth, 2)
                      for k in ("fut_ltp", "fut_mid", "fut_micro", "pcp_fwd")
                      if final_row.get(k) is not None}
        hit = [r.get("fut_ltp") for r in tail if r.get("fut_ltp") is not None]
        results["index"][index] = {
            "truth": round(truth, 2),
            "spot_base": spot_b,
            "spot_close": spot_t,
            "scores": scores,
            "final_estimate": {k: final_row[k] for k in ("fut_ltp", "fut_mid", "fut_micro", "pcp_fwd")
                               if final_row.get(k) is not None},
            "error_at_readout": at_readout,
            "pcp_strikes": final_row.get("pcp_strikes"),
            "direction_hit": (round(sum(1 for v in hit if (v > 0) == (truth > 0)) / len(hit), 2) if hit else None),
            "rows": rows,
        }
    return results


def calibrate(path):
    """Where the computed target disagreed with the manual indicative close, and by how much.

    The manual number (what the terminal showed) is the truth here; the futures-implied target is our
    estimate. A persistent positive bias means the estimate runs high, and the median is the offset
    worth subtracting."""
    out = {}
    for rec in load(path):
        if rec.get("type") != "readout":
            continue
        for row in rec.get("rows") or []:
            manual = row.get("target") if row.get("target_source") == "manual" else None
            computed = row.get("computed_target")
            if manual is None or computed is None:
                continue
            out.setdefault(row["index"], []).append(round(computed - manual, 2))
    summary = {}
    for idx, errs in out.items():
        if not errs:
            continue
        errs_sorted = sorted(errs)
        summary[idx] = {
            "n": len(errs),
            "bias": round(sum(errs) / len(errs), 2),
            "median": errs_sorted[len(errs_sorted) // 2],
            "mae": round(sum(abs(e) for e in errs) / len(errs), 2),
            "worst": max(errs, key=abs),
        }
    return summary


def report_calibration(summary, label=""):
    if not summary:
        print(f"{label}no manual indicative recorded — nothing to calibrate against")
        return
    print(f"\n--- calibration{(' ' + label) if label else ''}: futures estimate minus manual indicative")
    for idx, s in summary.items():
        print(f"  {idx:11s} bias {s['bias']:+7.2f}  median {s['median']:+7.2f}  MAE {s['mae']:6.2f}  "
              f"worst {s['worst']:+7.2f}  n={s['n']}")
        print(f"              -> correction to apply to the estimate: {-s['median']:+.2f} pts (negative of the median)")


def report(result):
    if "error" in result:
        print(f"{result['log']}: {result['error']}")
        return
    print(f"\n=== {Path(result['log']).name}")
    for index, d in result["index"].items():
        if "error" in d:
            print(f"  {index}: {d['error']}")
            continue
        print(f"  {index}: spot {d['spot_base']:.2f} -> {d['spot_close']:.2f}  "
              f"(realised CAS jump {d['truth']:+.2f})")
        est = d.get("final_estimate") or {}
        err = d.get("error_at_readout") or {}
        if est:
            print("     at readout:  " + "   ".join(f"{k} {est[k]:+7.2f} (err {err.get(k):+6.2f})"
                                                    for k in est))
            if d.get("pcp_strikes"):
                print(f"     pcp used {d['pcp_strikes']} strikes")
        for name, s in (d.get("scores") or {}).items():
            print(f"     {name:10s} MAE {s['mae']:6.2f}  bias {s['bias']:+6.2f}  n={s['samples']}")
        if d.get("direction_hit") is not None:
            print(f"     direction hit rate (fut_ltp): {d['direction_hit'] * 100:.0f}%")


def main():
    ap = argparse.ArgumentParser(description="Score CAS estimators against the realised move")
    ap.add_argument("--log", action="append", default=[], help="one or more jsonl logs")
    ap.add_argument("--logs", help="glob of logs")
    ap.add_argument("--json", help="write the full result (with samples) here")
    ap.add_argument("--calibrate", action="store_true",
                    help="score the futures-implied target against the manual indicative in the log")
    ap.add_argument("--every", type=int, default=60, help="sample spacing hint in seconds")
    a = ap.parse_args()

    paths = list(a.log)
    if a.logs:
        paths += sorted(glob.glob(a.logs))
    if not paths:
        ap.error("give --log or --logs")

    if getattr(a, "calibrate", False):
        for p in paths:
            report_calibration(calibrate(p), Path(p).name)
        return

    all_results = []
    for p in paths:
        r = evaluate(p, a.every)
        report(r)
        all_results.append(r)

    # cross-day summary: pooled MAE per estimator per index
    pooled = defaultdict(lambda: defaultdict(list))
    for r in all_results:
        for index, d in r.get("index", {}).items():
            for name, s in (d.get("scores") or {}).items():
                pooled[index][name].append(s["mae"])
    if pooled:
        print("\n--- pooled across logs (mean MAE, lower is better)")
        for index, ests in pooled.items():
            for name, maes in sorted(ests.items(), key=lambda kv: statistics.mean(kv[1])):
                print(f"  {index:11s} {name:10s} {statistics.mean(maes):6.2f}  (n={len(maes)})")

    if a.json:
        Path(a.json).write_text(json.dumps(all_results, indent=1))
        print(f"\nfull output -> {a.json}")


if __name__ == "__main__":
    main()
