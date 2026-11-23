#!/usr/bin/env python3
"""Closing-window book pressure, 15:20–15:27 IST.

Total buy/sell quantities are outstanding orders, not executed volume. Their
changes include additions and cancellations. Whether this feed exposes auction
activity is unproven. Futures tick-rule volume is a separate heuristic.

Usage: python cas_flow.py --logs "cas_logs/*/*.jsonl" --index NIFTY
"""
from __future__ import annotations

import argparse
import glob
import json
import statistics
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from cas_logger import DEFAULT_WEIGHTS, INDEX_CFG, IST, norm

# Observed up-rate per index from his own sheet, Aug 4 - Nov 19 2026. A flow signal has to beat this,
# not 50%: "always CE" already wins 62.5% of NIFTY expiries. Extend as more sessions are logged.
PRIOR_UP = {"NIFTY": (10, 16)}

WINDOW_START = "15:20:00"
WINDOW_END = "15:27:00"


def load(path):
    recs = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                record = json.loads(line)
                if record.get("type") == "meta":
                    recs = []  # daily files may contain successive recorder runs
                recs.append(record)
            except json.JSONDecodeError:
                continue
    return recs


def t_of(rec):
    try:
        value = datetime.fromisoformat(rec.get("t", ""))
        return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)
    except (TypeError, ValueError):
        return None


def in_window(t, start=None, end=None):
    if t is None:
        return False
    hm = t.strftime("%H:%M:%S")
    return (start or WINDOW_START) <= hm <= (end or WINDOW_END)


def weight_key(label: str) -> str:
    """Weight-table key for a symbol: BSE cash arrives as 'HDFCBANK-A' and NSE as 'HDFCBANK'."""
    sym = label.split(":")[-1]
    if ":" in label and label.split(":")[0].upper() == "BSE":
        sym = sym.rsplit("-", 1)[0]
    return norm(sym).removesuffix("EQ")


def quantity(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0 else None


def book_evidence(index, first, last, asof=None, weights=None):
    """Outstanding book changes, never executed buy/sell volume. One exchange per index.

    Quality thresholds are conservative operational rules, not calibrated probabilities.
    Score includes zero-change valid constituents; missing fields are never filled with zero.
    """
    W = {norm(k): v for k, v in ((weights or DEFAULT_WEIGHTS).get(index) or {}).items()
         if isinstance(v, (int, float)) and math.isfinite(v) and v > 0}
    exchange = INDEX_CFG.get(index, {}).get('spot_ex')
    names = {}
    for tok, f in first.items():
        l = last.get(tok)
        label = f.get('sym') or ''
        key = weight_key(label)
        if not l or f.get('role') != 'stock' or key not in W or label.split(':')[0] != exchange:
            continue
        tf, tl = t_of(f), t_of(l)
        if not tf or not tl or tl <= tf or tf.date() != tl.date():
            continue
        values = [quantity(r.get(k)) for r in (f, l) for k in ('tbq', 'tsq')]
        if any(v is None for v in values):
            continue
        b0, s0, b1, s1 = values
        db, ds = b1-b0, s1-s0
        change = abs(db)+abs(ds)
        late = (tf - tf.replace(hour=15, minute=20, second=0, microsecond=0)).total_seconds()
        age = max(0, (asof-tl).total_seconds()) if asof else 0
        item = {'symbol': label, 'net': db-ds, 'buy_change': db, 'sell_change': ds,
                'imb': (db-ds)/change if change else 0, 'weight': W[key],
                'start': tf.isoformat(), 'end': tl.isoformat(), 'age': age,
                'span': (tl-tf).total_seconds(), 'late': late, 'changed': bool(change)}
        # Duplicate tokens for the same listing never count twice.
        if key not in names or item['end'] > names[key]['end']:
            names[key] = item
    items = list(names.values())
    covered = sum(n['weight'] for n in items)
    expected = sum(W.values())
    score = sum(n['weight']*n['imb'] for n in items)/covered if covered else None
    fresh = [n for n in items if n['age'] <= 45 and n['span'] >= 60 and n['late'] <= 60]
    quality_coverage = sum(n['weight'] for n in fresh)/expected if expected else 0
    if fresh:
        score = sum(n['weight']*n['imb'] for n in fresh)/sum(n['weight'] for n in fresh)
    changed = sum(n['changed'] for n in fresh)
    reasons = []
    if not items: reasons.append('No valid paired stock book quantities in the window')
    if len(fresh) < 3: reasons.append('Need at least 3 timely stock book pairs')
    if quality_coverage < .5: reasons.append('Less than 50% of configured tracked weight has timely book data')
    if items and not changed: reasons.append('Book quantities have not changed')
    eligible = not reasons
    direction = 'NO SIGNAL' if not eligible or score is None else ('BALANCED' if abs(score) < .03 else ('UP PRESSURE' if score > 0 else 'DOWN PRESSURE'))
    return {'score': round(score, 4) if score is not None else None,
            'conviction': None if score is None else ('high' if abs(score)>=.15 else ('low' if abs(score)>=.03 else 'none')),
            'direction': direction, 'eligible': eligible, 'reasons': reasons,
            'coverage': round(covered, 4), 'tracked_coverage': round(covered/expected, 4) if expected else 0,
            'timely_coverage': round(quality_coverage, 4), 'stocks_seen': len(items), 'changed': changed,
            'buy_change': sum(n['buy_change'] for n in items) if items else None,
            'sell_change': sum(n['sell_change'] for n in items) if items else None,
            'net_change': sum(n['net'] for n in items) if items else None,
            'bought': sorted((n for n in items if n['net']>0), key=lambda n: -n['net'])[:5],
            'sold': sorted((n for n in items if n['net']<0), key=lambda n: n['net'])[:5]}


def depth_qty(tick, side):
    levels = tick.get(side) or []
    return sum((lv[0] or 0) for lv in levels if isinstance(lv, list) and len(lv) >= 1)


def signed_volume(ticks):
    """Tick-rule signed volume: volume added on an up-tick is buying, on a down-tick selling."""
    buy = sell = 0
    prev = None
    for tk in ticks:
        vol = tk.get("vol")
        ltp = tk.get("ltp")
        if prev is not None and vol is not None and prev.get("vol") is not None and ltp and prev.get("ltp"):
            dv = vol - prev["vol"]
            if dv > 0:
                if ltp > prev["ltp"]:
                    buy += dv
                elif ltp < prev["ltp"]:
                    sell += dv
                else:
                    # unchanged price: split by which side of the touch the print sits on
                    bids, asks = tk.get("bids") or [], tk.get("asks") or []
                    if asks and ltp >= asks[0][1]:
                        buy += dv
                    elif bids and ltp <= bids[0][1]:
                        sell += dv
        prev = tk
    return buy, sell


def instrument_features(ticks, label, role, index=None):
    """Flow deltas across the window for one instrument."""
    if len(ticks) < 2:
        return None
    first, last = ticks[0], ticks[-1]
    f = {"label": label, "role": role, "index": index, "n_ticks": len(ticks),
         "t_from": first.get("t", "")[11:19], "t_to": last.get("t", "")[11:19]}

    tb0, tb1 = first.get("tbq"), last.get("tbq")
    ts0, ts1 = first.get("tsq"), last.get("tsq")
    if tb0 is not None and tb1 is not None:
        f["d_buy_qty"] = tb1 - tb0
    if ts0 is not None and ts1 is not None:
        f["d_sell_qty"] = ts1 - ts0
    if "d_buy_qty" in f and "d_sell_qty" in f:
        f["d_net"] = f["d_buy_qty"] - f["d_sell_qty"]
        traded = abs(f["d_buy_qty"]) + abs(f["d_sell_qty"])
        if traded > 0:
            # scale-free: how lopsided was the flow into this name during the window
            f["d_imbalance"] = round(f["d_net"] / traded, 4)
        tot = (tb1 or 0) + (ts1 or 0)
        if tot > 0:
            f["imbalance"] = round(((tb1 or 0) - (ts1 or 0)) / tot, 4)

    b0, a0 = depth_qty(first, "bids"), depth_qty(first, "asks")
    b1, a1 = depth_qty(last, "bids"), depth_qty(last, "asks")
    if (b0 + a0) > 0 and (b1 + a1) > 0:
        imb0 = (b0 - a0) / (b0 + a0)
        imb1 = (b1 - a1) / (b1 + a1)
        f["depth_imb_from"] = round(imb0, 4)
        f["depth_imb_to"] = round(imb1, 4)
        f["d_depth_imb"] = round(imb1 - imb0, 4)

    ltp0, ltp1 = first.get("ltp"), last.get("ltp")
    if ltp0 and ltp1:
        f["d_ltp"] = round(ltp1 - ltp0, 2)

    vb, vs = signed_volume(ticks)
    if vb or vs:
        f["signed_buy"] = vb
        f["signed_sell"] = vs
        f["signed_net"] = vb - vs

    # did the auction book move at all? (the whole question)
    f["buy_qty_changed"] = bool(tb0 is not None and tb1 is not None and tb0 != tb1)
    f["depth_changed"] = bool(f.get("d_depth_imb"))
    f["ltp_moved"] = bool(ltp0 and ltp1 and ltp0 != ltp1)
    return f


def analyse(path, index_filter=None):
    recs = load(path)
    meta = next((r for r in recs if r.get("type") == "meta"), {})

    by_tok = defaultdict(list)
    for r in recs:
        if r.get("type") != "ws_tick":
            continue
        t = t_of(r)
        if not in_window(t):
            continue
        by_tok[r.get("tok")].append((t, r))

    feats = []
    for tok, lst in by_tok.items():
        lst.sort(key=lambda x: x[0])
        ticks = [tk for _, tk in lst]
        if not ticks:
            continue
        sample = ticks[0]
        idx = sample.get("idx")
        if index_filter and idx != index_filter and sample.get("role") != "stock":
            continue
        f = instrument_features(ticks, sample.get("sym") or str(tok), sample.get("role"), idx)
        if f:
            feats.append(f)

    # ---- per index rollup: weighted stock flow + futures flow + option activity
    rollups = {}
    # only roll up indices the log actually tracks (futures/options carry idx); otherwise the same
    # heavyweight stock would be reported under every index whose weight table happens to list it
    indices = {f["index"] for f in feats if f["index"] in INDEX_CFG}
    if index_filter:
        indices = {index_filter} if index_filter in INDEX_CFG else set()
    for index in sorted(indices):
        W = {norm(k): v for k, v in (DEFAULT_WEIGHTS.get(index) or {}).items()}
        # cash packets have no index field; attribute them through the weight table
        stocks = [f for f in feats if f["role"] == "stock" and weight_key(f["label"]) in W
                  and f["label"].split(":")[0] == INDEX_CFG[index]["spot_ex"]]
        futures = [f for f in feats if f["index"] == index and f["role"] == "future"]
        options = [f for f in feats if f["index"] == index and f["role"] == "option"]

        wsum = wnet = 0.0
        per_stock = []
        for f in stocks:
            sym = weight_key(f["label"])
            w = W.get(sym)
            net = f.get("d_net")
            if net is None:
                net = f.get("signed_net", 0)
            per_stock.append({"symbol": f["label"], "net": net, "weight": w,
                              "imbalance": f.get("d_imbalance"),
                              "auction_book_moved": f["buy_qty_changed"]})
            imb = f.get("d_imbalance")
            if w and imb is not None:
                wsum += w
                wnet += w * imb
        first = {tok: sorted(lst, key=lambda v: v[0])[0][1] for tok, lst in by_tok.items()}
        last = {tok: sorted(lst, key=lambda v: v[0])[-1][1] for tok, lst in by_tok.items()}
        end = max((t_of(r) for r in last.values() if t_of(r)), default=None)
        if end:
            end = end.replace(hour=15, minute=27, second=0, microsecond=0)
        evidence = book_evidence(index, first, last, end, (meta.get('weights') or {}).get('values'))
        flow_score = evidence['score']

        fut_net = None
        fut_ratio = None
        if futures:
            futures.sort(key=lambda f: abs(f.get("signed_net") or 0), reverse=True)
            fut_net = futures[0].get("signed_net")
            vb, vs = futures[0].get("signed_buy"), futures[0].get("signed_sell")
            if vb is not None and vs is not None and (vb + vs) > 0:
                # same -1..1 contract as the stock imbalance: raw volume is not a score
                fut_ratio = round((vb - vs) / (vb + vs), 4)

        # option vote: net signed volume of calls minus puts
        call_net = sum((f.get("signed_net") or 0) for f in options if f["label"].upper().endswith("CE"))
        put_net = sum((f.get("signed_net") or 0) for f in options if f["label"].upper().endswith("PE"))
        option_vote = (call_net - put_net)

        per_stock.sort(key=lambda s: (s["net"] or 0), reverse=True)
        source = "stocks" if flow_score is not None else ("futures" if fut_ratio is not None else None)
        score = flow_score if flow_score is not None else fut_ratio
        # |score| < 0.05 on a -1..1 ratio means the flow was effectively balanced: abstain
        conviction = None
        if score is not None:
            conviction = "high" if abs(score) >= 0.15 else ("low" if abs(score) >= 0.03 else "none")
        rollups[index] = {
            "flow_score": score,
            "book_evidence": evidence,
            "eligible": evidence["eligible"] and source == "stocks",
            "score_source": source,
            "conviction": conviction,
            "coverage": round(wsum, 3),
            "future_signed_net": fut_net,
            "option_vote": option_vote,
            "bought": [s for s in per_stock if (s["net"] or 0) > 0][:5],
            "sold": [s for s in per_stock if (s["net"] or 0) < 0][-5:],
            "auction_book_moved_on": sum(1 for s in per_stock if s["auction_book_moved"]),
            "stocks_seen": len(per_stock),
        }

    return {"log": str(path), "meta": {"universe": meta.get("universe")}, "instruments": feats,
            "rollup": rollups}


def realised_jump(recs, index):
    """Post-close spot minus the 15:15 spot, from the snapshots."""
    base = next((r for r in recs if r.get("type") == "snapshot" and r.get("label") == "15:15:10"), None)
    fin = next((r for r in recs if r.get("type") == "snapshot" and r.get("label") in ("15:31:00", "test_end")), None)
    if not base or not fin:
        return None

    def px(snap):
        for key, m in (snap.get("map") or {}).items():
            if m.get("index") == index and m.get("role") == "index":
                v = (snap.get("rest") or {}).get(key)
                if v and v.get("last_price"):
                    return float(v["last_price"])
        return None

    a, b = px(base), px(fin)
    return None if (a is None or b is None) else b - a


def report(res):
    print(f"\n=== {Path(res['log']).name}")
    if not res["rollup"]:
        print("  no ticks inside 15:20:00-15:27:00 — nothing to read")
        return
    recs = load(res["log"])
    for index, r in res["rollup"].items():
        jump = realised_jump(recs, index)
        label = r['book_evidence']['direction']
        print(f"  {index}: {label}; raw score {r['flow_score']} [{r.get('score_source')}]"
          f"  (coverage {r['coverage']})"
              f" | futures signed net {r['future_signed_net']} | option vote {r['option_vote']:+d}")
        if not r.get('eligible'):
            print('     ' + '; '.join(r['book_evidence']['reasons']))
        if jump is not None:
            actual = "UP" if jump > 0 else ("DOWN" if jump < 0 else "FLAT")
            if not r.get("eligible"):
                print("     Insufficient timely book evidence — excluded from accuracy scoring")
            elif r["flow_score"] is None:
                print(f"     realised {jump:+.2f} -> {actual}; no flow signal (nothing subscribed)")
            elif jump == 0 or r.get("conviction") == "none":
                print(f"     realised {jump:+.2f} -> {actual}; flow balanced — abstain")
            else:
                pred = "UP" if r["flow_score"] > 0 else "DOWN"
                print(f"     realised {jump:+.2f} -> {actual}; flow ({r['score_source']}) said {pred}"
                      f" {'HIT' if pred == actual else 'MISS'}")
        print(f"     auction book moved on {r['auction_book_moved_on']}/{r['stocks_seen']} stocks")
        if r["bought"]:
            print("     positive net book change: " + ", ".join(f"{s['symbol'].split(':')[-1]} {s['net']:+.0f}" for s in r["bought"]))
        if r["sold"]:
            print("     negative net book change: " + ", ".join(f"{s['symbol'].split(':')[-1]} {s['net']:+.0f}" for s in r["sold"]))


def main():
    ap = argparse.ArgumentParser(description="Order-flow read of the CAS window")
    ap.add_argument("--log", action="append", default=[])
    ap.add_argument("--logs", help="glob")
    ap.add_argument("--index", help="only this index")
    ap.add_argument("--json", help="write full output here")
    a = ap.parse_args()
    paths = list(a.log) + (sorted(glob.glob(a.logs)) if a.logs else [])
    if not paths:
        ap.error("give --log or --logs")

    out, hits, total, ups = [], 0, 0, 0
    per_index = defaultdict(lambda: [0, 0, 0])
    seen = set()
    for p in paths:
        res = analyse(p, a.index)
        report(res)
        out.append(res)
        recs = load(p)
        for index, r in res["rollup"].items():
            jump = realised_jump(recs, index)
            if jump is None or jump == 0 or r["flow_score"] is None or not r.get("eligible"):
                continue
            session_meta = next((x for x in recs if x.get('type') == 'meta'), {})
            if ('args' not in session_meta or (session_meta.get('args') or {}).get('test')
                    or session_meta.get('synthetic')):
                continue
            if r["flow_score"] is None or r.get("conviction") == "none":
                continue
            date = next((t_of(x).date().isoformat() for x in recs if x.get('type') == 'ws_tick' and t_of(x)), None)
            key = (index, date)
            if key in seen: continue
            seen.add(key)
            per_index[index][0] += (r['flow_score'] > 0) == (jump > 0)
            per_index[index][1] += 1
            per_index[index][2] += jump > 0
            total += 1
            ups += jump > 0
            hits += (r["flow_score"] > 0) == (jump > 0)

    for index, (h, n, u) in sorted(per_index.items()):
        print(f"\n{index}: eligible sessions {n}; pressure {h}/{n} ({h/n:.1%}); same-session always UP {u}/{n} ({u/n:.1%})")
    if not total:
        print("\nNo eligible real sessions to score. Predictive accuracy remains unknown.")
    if total:
        print(f"\n--- flow direction hit rate: {hits}/{total} ({hits / total * 100:.0f}%)")
        print(f"    baseline 'always CE' on the same days: {ups}/{total} ({ups / total * 100:.0f}%)"
              f" — a signal only counts if it beats this")
        for idx, (u, n) in PRIOR_UP.items():
            print(f"    prior on {idx}: close above 3:15 on {u}/{n} expiries ({u / n * 100:.1f}%), per the Aug-Nov sheet")
    if a.json:
        Path(a.json).write_text(json.dumps(out, indent=1))
        print(f"full output -> {a.json}")


if __name__ == "__main__":
    main()
