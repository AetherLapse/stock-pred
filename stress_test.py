#!/usr/bin/env python3
"""stress_test.py - 100 generated CAS scenarios through the whole pipeline.

Each scenario builds a log with a different market shape and a different set of defects, then pushes it
through every reader: the dashboard state builder, the flow analyser, the estimator scorer and the
decision maths. Invariants are asserted; nothing is allowed to raise.

    .venv/bin/python stress_test.py            # run all 100
    .venv/bin/python stress_test.py --only 7   # one scenario, verbose

Outputs
    stress_logs/scenarios.jsonl   one record per scenario (params, checks, timing)
    stress_logs/failures.jsonl    only the failures, with traceback
    stress_logs/summary.txt       the table printed at the end
"""
from __future__ import annotations

import argparse
import json
import math
import random
import traceback
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import cas_analysis as A
import cas_dashboard as D
import cas_flow as F
from cas_logger import (DEFAULT_WEIGHTS, INDEX_CFG, MIN_GAP, STRIKE_STEP, decide, pick_strike)

IST = D.datetime(2026, 11, 23, 15, 15, 10).astimezone().tzinfo  # replaced below
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE_T = datetime(2026, 11, 23, 15, 15, 10, tzinfo=IST)
OUT = Path("stress_logs")
OUT.mkdir(exist_ok=True)

INDICES = list(INDEX_CFG)


# --------------------------------------------------------------------------- scenario generator
def make_scenarios(n: int = 100, seed: int = 42):
    rnd = random.Random(seed)
    scenarios = []

    shapes = ["up", "down", "flat", "small_up", "small_down", "huge_up", "huge_down", "at_floor", "just_below_floor"]
    data_defects = ["clean", "no_options", "one_leg_missing", "stale_options", "no_depth", "zero_depth_qty",
                    "frozen_book", "book_moving", "no_stocks", "sparse_ticks", "single_tick_per_token",
                    "garbage_frames", "duplicate_ticks", "out_of_order", "missing_post_snapshot",
                    "truncated_line", "unknown_indices", "paise_scale", "big_numbers", "negative_qty"]
    index_sets = [INDICES, ["NIFTY"], ["BANKEX"], ["SENSEX", "BANKEX"], ["NIFTY", "MIDCPNIFTY"],
                  ["BANKNIFTY", "FINNIFTY"], ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"],
                  ["SENSEX", "BANKEX", "NIFTY"], ["FINNIFTY"], ["MIDCPNIFTY", "SENSEX"]]

    for i in range(n):
        shape = rnd.choice(shapes)
        defect = rnd.choice(data_defects)
        indices = rnd.choice(index_sets)
        scenarios.append({
            "id": i + 1,
            "name": f"{shape}/{defect}/{'+'.join(indices)[:28]}",
            "shapes": shape,
            "indices": indices,
            "defect": defect,
            "steps": rnd.choice([1, 2, 5, 20, 60, 140]),
            "step_s": rnd.choice([1, 3, 10, 30]),
            "stale_options": rnd.random() < 0.3,
            "seed": rnd.randrange(1_000_000),
        })
    return scenarios


# --------------------------------------------------------------------------- log builder
def build_log(sc: dict, path: Path) -> dict:
    """Write a scenario log; returns the ground truth used by the assertions."""
    rnd = random.Random(sc["seed"])
    steps, step_s = sc["steps"], sc["step_s"]
    defect = sc["defect"]
    gap_by_index, truth = {}, {}

    recs = []
    mp = {}
    counter = {"n": 1000}
    stable = {}

    def tok_for(key):
        if key not in stable:
            counter["n"] += 1
            stable[key] = counter["n"]
        return stable[key]

    # gaps: derived from the shape, then nudged per index so no two cards are identical
    for idx in sc["indices"]:
        step = STRIKE_STEP[idx]
        floor = MIN_GAP[idx]
        shape = sc["shapes"]
        base = {
            "up": rnd.uniform(0.5, 3.0) * step,
            "down": -rnd.uniform(0.5, 3.0) * step,
            "flat": 0.0,
            "small_up": rnd.uniform(0.1, 0.8) * floor,
            "small_down": -rnd.uniform(0.1, 0.8) * floor,
            "huge_up": rnd.uniform(5, 12) * step,
            "huge_down": -rnd.uniform(5, 12) * step,
            "at_floor": floor,
            "just_below_floor": floor * 0.98,
        }[shape]
        gap_by_index[idx] = round(base + rnd.uniform(-0.1, 0.1) * step, 2)
        if defect == "paise_scale":
            gap_by_index[idx] = round(gap_by_index[idx] * 100, 2)   # absurd but legal numbers
        if defect == "big_numbers":
            gap_by_index[idx] = round(gap_by_index[idx] * 1000, 2)

    meta = {"type": "meta", "t": BASE_T.isoformat(), "version": "1.0",
            "universe": {"index": len(sc["indices"]), "future": len(sc["indices"]),
                         "stock": 0, "option": 0}, "notes": []}
    recs.append(meta)

    base_rest, post_rest = {}, {}
    for idx in sc["indices"]:
        spot_ex = INDEX_CFG[idx]["spot_ex"]
        deriv_ex = INDEX_CFG[idx]["deriv_ex"]
        spot = {"NIFTY": 25000.0, "BANKNIFTY": 58000.0, "FINNIFTY": 27500.0,
                "MIDCPNIFTY": 12500.0, "SENSEX": 80500.0, "BANKEX": 58000.0}[idx]
        fut0 = round(spot + abs(gap_by_index[idx]) * 0.2 + 12, 2)
        mp[f"{spot_ex}:{idx}"] = {"index": idx, "role": "index"}
        mp[f"{deriv_ex}:{idx}FUT"] = {"index": idx, "role": "future"}
        base_rest[f"{spot_ex}:{idx}"] = {"instrument_token": str(tok_for(f"{idx}|spot")), "last_price": spot}
        base_rest[f"{deriv_ex}:{idx}FUT"] = {"instrument_token": str(tok_for(f"{idx}|fut")),
                                             "last_price": fut0}
        post_rest[f"{spot_ex}:{idx}"] = {"instrument_token": "1",
                                         "last_price": round(spot + gap_by_index[idx], 2)}
        post_rest[f"{deriv_ex}:{idx}FUT"] = {"instrument_token": "2",
                                             "last_price": round(fut0 + gap_by_index[idx], 2)}
        truth[idx] = {"spot": spot, "fut0": fut0, "gap": gap_by_index[idx]}

    recs.append({"type": "snapshot", "t": BASE_T.isoformat(), "label": "15:15:10",
                 "rest": base_rest, "map": mp})

    t0 = BASE_T + timedelta(minutes=5)
    for b in range(steps + 1):
        t = t0 + timedelta(seconds=b * step_s)
        frac = b / steps if steps else 1.0
        for idx in sc["indices"]:
            tinfo = truth[idx]
            gap = tinfo["gap"]
            fut = tinfo["fut0"] + gap * frac
            deriv_ex = INDEX_CFG[idx]["deriv_ex"]
            # sparse tape: drop most ticks but ALWAYS emit the first and last, otherwise the log
            # simply never contains the final state and the reader cannot be at fault for it
            if defect == "sparse_ticks" and b % 7 and b not in (0, steps):
                continue
            recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok_for(f"{idx}|fut"),
                         "sym": f"{deriv_ex}:{idx}FUT", "role": "future", "idx": idx,
                         "ltp": round(fut, 2), "vol": 1000 + b * 10,
                         "bids": [[100, round(fut - 0.05, 2), 2]], "asks": [[100, round(fut + 0.05, 2), 2]]})
            if defect == "duplicate_ticks":
                recs.append(dict(recs[-1], t=(t + timedelta(seconds=0.4)).isoformat()))

            # stocks
            if defect != "no_stocks":
                names = list(DEFAULT_WEIGHTS.get(idx, {}).items())[:3]
                for sym, w in names:
                    lean = 1.0 if gap > 0 else -1.0
                    tbq = int(200 * b * step_s * (1 + lean * 0.5))
                    tsq = int(200 * b * step_s * (1 - lean * 0.5))
                    if defect == "negative_qty":
                        tbq, tsq = -abs(tbq), -abs(tsq)
                    if defect == "frozen_book":
                        tbq, tsq = 5000, 4000
                    if defect == "book_moving":
                        tbq, tsq = int(tbq * (1 + b / 20)), tsq
                    depth_bid = [[500, 99.9, 3]] if defect != "no_depth" else []
                    depth_ask = [[500, 100.1, 3]] if defect != "no_depth" else []
                    if defect == "zero_depth_qty":
                        depth_bid, depth_ask = [[0, 0, 0]], [[0, 0, 0]]
                    sym_bse = f"{sym}-A" if INDEX_CFG[idx]["spot_ex"] == "BSE" else sym
                    recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": tok_for(f"STOCK|{sym}"),
                                 "sym": f"{INDEX_CFG[idx]['spot_ex']}:{sym_bse}", "role": "stock", "idx": None,
                                 "ltp": 100.0, "tbq": tbq, "tsq": tsq,
                                 "bids": depth_bid, "asks": depth_ask})

            # options
            if defect not in ("no_options",):
                step = STRIKE_STEP[idx]
                target = tinfo["spot"] + gap
                side = "CE" if gap > 0 else "PE"
                k = pick_strike(target, step, side)
                for kk in (k - step, k, k + step):
                    legs = ["CE", "PE"]
                    if defect == "one_leg_missing":
                        legs = ["CE"]
                    for ot in legs:
                        intr = max(0.0, (fut - kk) if ot == "CE" else (kk - fut))
                        price = intr + 8.0 * (1 - 0.8 * frac)
                        if defect == "stale_options":
                            price = intr + 3.0
                        recs.append({"type": "ws_tick", "t": t.isoformat(),
                                     "tok": tok_for(f"{idx}|{kk}|{ot}"),
                                     "sym": f"{INDEX_CFG[idx]['deriv_ex']}:{idx}{int(kk)}{ot}",
                                     "role": "option", "idx": idx, "k": float(kk), "ot": ot,
                                     "ltp": round(price, 2),
                                     "bids": [[20, round(price - 0.5, 2), 1]],
                                     "asks": [[20, round(price + 0.5, 2), 1]]})

    if defect == "garbage_frames":
        recs.append({"type": "event", "t": t0.isoformat(), "name": "error",
                     "where": "ws_message", "error": "ValueError: garbage frame", "trace": "..."})
    if defect == "out_of_order":
        rnd.shuffle(recs[2:])

    if defect != "missing_post_snapshot":
        recs.append({"type": "snapshot", "t": (t0 + timedelta(seconds=steps * step_s + 240)).isoformat(),
                     "label": "15:31:00", "rest": post_rest, "map": mp})

    with path.open("w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
        if defect == "truncated_line":
            f.write('{"type":"ws_tick","t":"2026-11-23T15:2')   # half a line, no newline
    return {"truth": truth, "indices": sc["indices"], "defect": defect}


# --------------------------------------------------------------------------- invariants
def check_scenario(sc, log_path, built) -> list[str]:
    problems = []

    # 1) dashboard state must build, serialise and agree with the rules
    live = D.Live(log_path)
    live.poll()
    st = live.state()
    json.dumps(st)                                  # must be JSON-serialisable for the API
    for idx in built["indices"]:
        if idx not in st["indices"]:
            if built["defect"] in ("sparse_ticks",) or sc["steps"] == 0:
                continue
            problems.append(f"{idx}: missing from dashboard state")
            continue
        d = st["indices"][idx]
        gap, side, strike, target = d["gap"], d["side"], d["strike"], d["target"]
        if (gap > 0) != (side == "CE"):
            if gap != 0:
                problems.append(f"{idx}: side {side} contradicts gap {gap}")
        if strike % STRIKE_STEP[idx] != 0:
            problems.append(f"{idx}: strike {strike} not on the {STRIKE_STEP[idx]}-point ladder")
        intr = (target - strike) if side == "CE" else (strike - target)
        if d["tradeable"] and intr < 0:
            problems.append(f"{idx}: tradeable but {side} {strike} is OTM at target {target}")
        expect_tradeable = abs(gap) >= MIN_GAP[idx]
        if d["tradeable"] != expect_tradeable:
            problems.append(f"{idx}: tradeable {d['tradeable']} != floor rule {expect_tradeable} (gap {gap})")
        if d["flow"] is not None and not (-1.001 <= d["flow"] <= 1.001):
            problems.append(f"{idx}: flow {d['flow']} outside [-1,1]")
        bought = {b["symbol"] for b in d["bought"]}
        sold = {s_["symbol"] for s_ in d["sold"]}
        if bought & sold:
            problems.append(f"{idx}: {bought & sold} in both bought and sold")
        if any(b["net"] <= 0 for b in d["bought"]) or any(s_["net"] >= 0 for s_ in d["sold"]):
            problems.append(f"{idx}: bought/sold sign mismatch")
        # truth check: the gap the generator produced must be the gap the reader recovers
        truth_gap = built["truth"][idx]["gap"]
        if abs(d["gap"] - truth_gap) > max(2.0, abs(truth_gap) * 0.02):
            problems.append(f"{idx}: recovered gap {d['gap']} != generated {truth_gap}")
        if abs(d["target"] - (d["spot"] + truth_gap)) > max(2.0, abs(truth_gap) * 0.02):
            problems.append(f"{idx}: target {d['target']} != spot+gap {d['spot'] + truth_gap}")
        realised = (st.get("realised") or {}).get(idx)
        if realised is not None and abs(realised - truth_gap) > max(2.0, abs(truth_gap) * 0.02):
            problems.append(f"{idx}: realised {realised} != generated {truth_gap}")
        allowed = {"STRONG", "WATCH", "SKIP", "NO EDGE", "CONFLICT", "STALE"}
        if d.get("verdict") not in allowed:
            problems.append(f"{idx}: verdict {d.get('verdict')!r} not one of {sorted(allowed)}")
        if d.get("verdict") == "STRONG":
            if not d["tradeable"]:
                problems.append(f"{idx}: STRONG on a below-floor gap")
            if d["edge"] is not None and d["edge"] <= 0:
                problems.append(f"{idx}: STRONG with negative edge {d['edge']}")
            if d["flow"] is not None and d["conviction"] == "high" and (d["flow"] > 0) != (d["gap"] > 0):
                problems.append(f"{idx}: STRONG while flow opposes the gap")
        if d.get("verdict") == "SKIP" and d["tradeable"]:
            problems.append(f"{idx}: SKIP on a tradeable gap")
        if d["edge"] is not None and d["ask"] is not None:
            recomputed = intr - d["ask"]
            if abs(recomputed - d["edge"]) > 0.02:
                problems.append(f"{idx}: edge {d['edge']} != intrinsic-ask {recomputed:.2f}")

    # 2) flow analyser must not raise, and its score must match the dashboard's sign
    res = F.analyse(str(log_path))
    json.dumps(res)
    for idx, roll in res["rollup"].items():
        if roll.get("flow_score") is not None and not (-1.001 <= roll["flow_score"] <= 1.001):
            problems.append(f"{idx}: flow rollup {roll['flow_score']} outside [-1,1]")

    # 3) estimator scorer must complete and produce finite numbers
    an = A.evaluate(str(log_path))
    if "error" not in an:
        for idx, d in an["index"].items():
            for name, s in (d.get("scores") or {}).items():
                if not math.isfinite(s["mae"]) or not math.isfinite(s["bias"]):
                    problems.append(f"{idx}/{name}: non-finite MAE/bias")

    # 4) decision maths must be pure and total for every index and gap sign
    for idx in built["indices"]:
        for gap in (-1000, -MIN_GAP[idx], -1, 0, 1, MIN_GAP[idx], 1000):
            row = decide(idx, gap, 10000.0, 10000.0 + gap, None, None)
            if row["side"] != ("CE" if gap > 0 else "PE"):
                problems.append(f"{idx}: decide() side wrong at gap {gap}")
            if row["strike"] % STRIKE_STEP[idx] != 0:
                problems.append(f"{idx}: decide() strike off-ladder at gap {gap}")

    return problems


# --------------------------------------------------------------------------- runner
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", type=int, help="run one scenario id")
    ap.add_argument("--count", type=int, default=100)
    a = ap.parse_args()

    scenarios = make_scenarios(a.count)
    if a.only:
        scenarios = [s for s in scenarios if s["id"] == a.only]

    scen_log = OUT / "scenarios.jsonl"
    fail_log = OUT / "failures.jsonl"
    scen_log.unlink(missing_ok=True)
    fail_log.unlink(missing_ok=True)

    rows, failures = [], 0
    work = OUT / "logs"
    work.mkdir(exist_ok=True)

    for sc in scenarios:
        path = work / f"scenario_{sc['id']:03d}.jsonl"
        t_start = datetime.now()
        try:
            built = build_log(sc, path)
            problems = check_scenario(sc, path, built)
        except Exception as exc:                     # noqa: BLE001
            problems = [f"EXCEPTION {exc.__class__.__name__}: {exc}"]
            with fail_log.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"id": sc["id"], "name": sc["name"],
                                    "trace": traceback.format_exc()}) + "\n")
        ok = not problems
        failures += 0 if ok else 1
        row = {"id": sc["id"], "name": sc["name"], "shape": sc["shapes"], "defect": sc["defect"],
               "indices": sc["indices"], "steps": sc["steps"], "ok": ok, "problems": problems,
               "ms": round((datetime.now() - t_start).total_seconds() * 1000)}
        rows.append(row)
        with scen_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
        if not ok:
            with fail_log.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"id": sc["id"], "name": sc["name"], "problems": problems}) + "\n")
        status = "PASS" if ok else "FAIL"
        print(f"{sc['id']:3d} {status}  {sc['name'][:70]:70s} {row['ms']:5d}ms"
              + ("" if ok else f"  <- {problems[0][:80]}"))

    total = len(rows)
    by_shape = Counter(r["shape"] for r in rows)
    by_defect = Counter(r["defect"] for r in rows)
    lines = [f"scenarios: {total}", f"passed: {total - failures}", f"failed: {failures}",
             "", "by shape:  " + ", ".join(f"{k}={v}" for k, v in by_shape.items()),
             "", "by defect: " + ", ".join(f"{k}={v}" for k, v in by_defect.items())]
    if failures:
        lines += ["", "failures:"]
        for r in rows:
            if not r["ok"]:
                lines.append(f"  #{r['id']} {r['name']}: {r['problems'][:3]}")
    text = "\n".join(lines)
    (OUT / "summary.txt").write_text(text, encoding="utf-8")
    print("\n" + text)
    raise SystemExit(0 if not failures else 1)


if __name__ == "__main__":
    main()
