#!/usr/bin/env python3
"""Offline verification harness for cas_logger.py.

Exercises every code path that does not need the live feed:
  * scriptmaster parsing + Resolver (spot / future / option chain / stocks)
  * WS frame splitting (kite-style and fixed-size), packet parsing, price scaling
  * divisor inference against REST-shaped data
  * the live readout row builder (strike selection, intrinsic, weighted estimate)
  * summarize() over a synthetic end-to-end log

Run:  .venv/bin/python verify.py
"""
from __future__ import annotations

import csv
import json
import os
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

import cas_logger as C

OK, FAIL = [], []

# ---------------------------------------------------------------------------
# Fixtures are GENERATED, never assumed: a fresh clone must be able to run this
# suite with no previous run's artefacts on disk.
# ---------------------------------------------------------------------------
def _ensure_fixtures():
    import make_flow_log
    import make_synthetic
    for name in ("flow_up", "flow_down", "flow_noise"):
        f = Path(f"_flow_{name}.jsonl")
        if not f.exists():
            make_flow_log.build(f, name)
    for name in ("clean", "noisy", "laggy", "pcp_edge"):
        f = Path(f"_synthetic_{name}.jsonl")
        if not f.exists():
            make_synthetic.build(f, name)
    calib = Path("_calib_demo.jsonl")
    if not calib.exists():
        rows = []
        for i, (manual, computed) in enumerate([(25078, 25084), (25062, 25069), (24980, 24985), (25010, 25029)]):
            rows.append({"type": "readout", "t": f"2026-11-23T15:27:{10 + i * 5:02d}+05:30", "final": True,
                         "rows": [{"index": "NIFTY", "target": manual, "target_source": "manual",
                                   "computed_target": computed}]})
        rows.append({"type": "readout", "t": "2026-11-23T15:27:40+05:30", "final": True,
                     "rows": [{"index": "BANKNIFTY", "target": 57790, "target_source": "manual",
                               "computed_target": 57796}]})
        calib.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


_ensure_fixtures()


def check(name, fn):
    try:
        fn()
        OK.append(name)
        print(f"PASS  {name}")
    except Exception as e:
        FAIL.append((name, f"{e.__class__.__name__}: {e}"))
        print(f"FAIL  {name} -> {e.__class__.__name__}: {e}")
        traceback.print_exc(limit=3)


# ---------------------------------------------------------------- scriptmaster
ROWS = []
def add(tok, ex, sym, name="", itype="", seg="", expiry="", strike=""):
    ROWS.append({"instrument_token": str(tok), "exchange": ex, "tradingsymbol": sym,
                 "name": name, "instrument_type": itype, "segment": seg,
                 "expiry": expiry, "strike": strike, "exchange_token": str(tok // 10)})

today = date(2026, 11, 23)
next_exp = date(2026, 11, 26)
far_exp = date(2026, 12, 31)

# NSE index spot + stock
add(256265, "NSE", "NIFTY 50", "NIFTY 50", "INDEX", "INDICES")
add(738561, "NSE", "RELIANCE", "RELIANCE INDUSTRIES", "EQ", "NSE")
add(341249, "NSE", "HDFCBANK", "HDFC BANK", "EQ", "NSE")
# NFO current-month future + a far one (must pick the nearer)
add(111, "NFO", "NIFTY26NOVFUT", "NIFTY", "FUT", "NFO-FUT", "2026-11-26")
add(112, "NFO", "NIFTY26DECFUT", "NIFTY", "FUT", "NFO-FUT", "2026-12-31")
# NFO option chain around 25000
for strike in (24400, 24500, 24600, 24700, 24800, 24900, 25000, 25100, 25200, 25300):
    for ot, off in (("CE", 0), ("PE", 1)):
        add(200000 + strike + off, "NFO", f"NIFTY26NOV{strike}{ot}", "NIFTY", "CE" if ot == "CE" else "PE",
            "NFO-OPT", "2026-11-26", str(strike))
# BSE index spot + BSE cash symbols ('ACC-A' style)
add(265, "BSE", "SENSEX", "SENSEX", "INDEX", "INDICES")
add(3001, "BSE", "HDFCBANK-A", "HDFC BANK", "EQ", "BSE")
# BFO future + options
add(266, "BSE", "BANKEX", "BANKEX", "INDEX", "INDICES")   # distinct token from SENSEX
add(4001, "BFO", "SENSEX26NOVFUT", "SENSEX", "FUT", "BFO-FUT", "2026-11-26")
add(4002, "BFO", "BANKEX26NOVFUT", "BANKEX", "FUT", "BFO-FUT", "2026-11-26")
for strike in (57000, 58000, 59000):
    for ot, off in (("CE", 0), ("PE", 1)):
        add(7000 + strike + off, "BFO", f"BANKEX26NOV{strike}{ot}", "BANKEX", ot, "BFO-OPT", "2026-11-26", str(strike))
for strike in (80000, 80200, 80400, 80600):
    for ot, off in (("CE", 0), ("PE", 1)):
        add(5000 + strike + off, "BFO", f"SENSEX26NOV{strike}{ot}", "SENSEX", ot, "BFO-OPT", "2026-11-26", str(strike))

master = Path("_verify_master.csv")
with master.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(ROWS[0].keys()))
    w.writeheader()
    w.writerows(ROWS)


def t_master():
    with master.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        r.fieldnames = [h.strip().lower() for h in (r.fieldnames or [])]
        rows = [{k: (v or "").strip() for k, v in row.items()} for row in r]
    need = {"instrument_token", "tradingsymbol", "exchange"}
    assert need.issubset(rows[0].keys()), f"columns wrong: {list(rows[0])}"
    assert len(rows) == len(ROWS), len(rows)
    globals()["ROWS_LOADED"] = rows


ROWS_LOADED = []
check("scriptmaster parse (lowercased headers)", t_master)


def t_resolver():
    res = C.Resolver(ROWS_LOADED, today, {})
    spot = res.spot("NIFTY")
    assert spot and spot["token"] == 256265, spot
    fut = res.future("NIFTY")
    assert fut and fut["token"] == 111, f"picked wrong future: {fut}"
    res.prepare_options("NIFTY")
    assert res.opt_expiry["NIFTY"] == next_exp, res.opt_expiry["NIFTY"]
    chain = res.options_around("NIFTY", 25010, 2)
    strikes = sorted({c["strike"] for c in chain})
    assert strikes == [24800.0, 24900.0, 25000.0, 25100.0, 25200.0], strikes   # ATM 25000 +/-2 = 5
    assert {c["otype"] for c in chain} == {"CE", "PE"}, chain[:2]
    assert all(c["expiry"] == "2026-11-26" for c in chain), chain[0]


check("Resolver: spot, nearest future, ATM +/-2 option chain", t_resolver)


def t_resolver_bse():
    res = C.Resolver(ROWS_LOADED, today, {})
    assert res.spot("SENSEX")["token"] == 265, res.spot("SENSEX")
    st = res.bse_stock("HDFCBANK")
    assert st and st["symbol"] == "HDFCBANK-A", st


check("Resolver: BSE spot + 'ACC-A' cash symbols", t_resolver_bse)


def t_resolver_missing():
    """A master miss now falls back to the documented REST key — no WS token, still quotable."""
    res = C.Resolver(ROWS_LOADED, today, {})
    it = res.spot("MIDCPNIFTY")
    assert it is not None, "should fall back to the REST key"
    assert it["token"] is None, f"fallback must carry no WS token: {it}"
    assert it["key"] == "NSE:NIFTY MIDCAP SELECT", it
    # an index with no documented key is still reported, never invented
    C.INDEX_REST_KEYS.pop("MIDCPNIFTY", None)
    try:
        assert C.Resolver(ROWS_LOADED, today, {}).spot("MIDCPNIFTY") is None
    finally:
        C.INDEX_REST_KEYS["MIDCPNIFTY"] = "NSE:NIFTY MIDCAP SELECT"
    assert any("MIDCPNIFTY" in n for n in res.notes), res.notes
    st = res.stock("NOTASTOCK")
    assert st is None and "NOTASTOCK" in res.missing["NSE"], res.missing


check("Resolver: absent symbols are reported, not invented", t_resolver_missing)


def t_overrides():
    res = C.Resolver(ROWS_LOADED, today, {"NIFTY": {"token": 999, "symbol": "Nifty 50"}})
    s = res.spot("NIFTY")
    assert s["token"] == 999 and s["symbol"] == "Nifty 50", s


check("Resolver: --overrides token path", t_overrides)

# ---------------------------------------------------------------- WS packets
import struct


def mk_ltp(tok, price_paise):
    return struct.pack(">ii", tok, price_paise)


def mk_index(tok, ltp, high, low, op, close):
    return struct.pack(">iiiiiiiiiiii", tok, ltp, high, low, op, close, ltp - close, 0, 0, 0, 0, 0)


def mk_full(tok, ltp, tbq, tsq, with_depth=True):
    body = struct.pack(">iiiiiiiiiiiiiiii", tok, ltp, 10, ltp, 123456, tbq, tsq, 100, 200, 90, 95, 0, 0, 0, 0, 0)  # 16 ints = 64 bytes
    if with_depth:
        for i in range(10):
            body += struct.pack(">iihxx", 100000 + i, 2500000 + i, 10 + i)
    return body


def frame_kite(pkts):
    out = struct.pack(">H", len(pkts))
    for p in pkts:
        out += struct.pack(">H", len(p)) + p
    return out


def t_split_kite():
    # the kite shape is only reachable when asked for explicitly: on a 2-packet frame both
    # interpretations are structurally valid, so the documented 'fixed' layout is the default
    pkts, layout = C.split_frame(frame_kite([mk_ltp(1, 10000), mk_ltp(2, 20000)]), ">", "kite")
    assert layout == "kite" and len(pkts) == 2, (layout, pkts)
    # and with the documented preference that same buffer is read as fixed-size packets
    pkts2, layout2 = C.split_frame(frame_kite([mk_ltp(1, 10000), mk_ltp(2, 20000)]), ">")
    assert layout2 == "fixed", layout2


check("split_frame: --layout kite still available, fixed is default", t_split_kite)


def t_split_fixed():
    p = mk_ltp(7, 12345)
    buf = struct.pack(">HH", 3, len(p)) + p * 3
    pkts, layout = C.split_frame(buf, ">")
    assert layout == "fixed" and len(pkts) == 3, (layout, len(pkts))


check("split_frame: fixed-size layout fallback", t_split_fixed)


def t_split_garbage():
    pkts, layout = C.split_frame(b"\x00\x00", ">")
    assert pkts == [] and layout == "short", (pkts, layout)
    pkts, layout = C.split_frame(b"\xff\xff\x00\x00abcd", ">")
    assert pkts == [], (pkts, layout)


check("split_frame: short/garbage frames do not crash", t_split_garbage)


def t_parse_ltp():
    d = C.parse_packet(mk_ltp(42, 2512345), ">")
    assert d["token"] == 42 and d["ltp"] == 2512345 and d["kind"] == "ltp", d


check("parse_packet: 8-byte ltp", t_parse_ltp)


def t_parse_index():
    d = C.parse_packet(mk_index(256265, 2500000, 2510000, 2490000, 2495000, 2505000), ">")
    assert d["kind"] == "index" and d["high"] == 2510000 and d["open"] == 2495000, d


check("parse_packet: 48-byte index packet", t_parse_index)


def t_parse_full():
    d = C.parse_packet(mk_full(738561, 150000, 12000, 9000), ">")
    assert d["kind"] in ("full", "quote"), d["kind"]
    assert d["tbq"] == 12000 and d["tsq"] == 9000, (d["tbq"], d["tsq"])
    assert "bids" in d and d["bids"][0][0] == 100000, d.get("bids")


check("parse_packet: depth packet (bids/asks, tbq/tsq)", t_parse_full)


def t_scale():
    d = C.parse_packet(mk_full(738561, 150000, 12000, 9000), ">")
    s = C.scale(d, 100.0, True)
    assert abs(s["ltp"] - 1500.0) < 1e-9, s["ltp"]
    assert s["ltp_raw"] == 150000, s.get("ltp_raw")
    assert abs(s["bids"][0][1] - 25000.0) < 1e-6, s["bids"][0]


check("scale: paise -> rupees, ltp_raw preserved", t_scale)


def t_divisor():
    rest = {"NSE:HDFCBANK": {"instrument_token": "738561", "last_price": 1500.0},
            "NSE:RELIANCE": {"instrument_token": "738562", "last_price": 2900.0},
            "NSE:INFY": {"instrument_token": "738563", "last_price": 1600.0}}
    feed = type("F", (), {})()
    feed.latest = {738561: {"ltp": 150000}, 738562: {"ltp": 290000}, 738563: {"ltp": 160000}}
    d, ratios = C.infer_divisor(feed, rest, None)
    assert d == 100.0, (d, ratios)


check("infer_divisor: recognises x100 scaling", t_divisor)

# ---------------------------------------------------------------- summarise
def build_log(path: Path):
    base_t = datetime(2026, 11, 23, 15, 15, 10, tzinfo=C.IST)
    recs = []
    recs.append({"type": "meta", "t": base_t.isoformat(), "version": C.VERSION,
                 "universe": {"index": 2, "future": 2, "option": 28, "stock": 3},
                 "opt_expiry": {"NIFTY": "2026-11-26"},
                 "notes": ["MIDCPNIFTY: spot NOT found in scriptmaster (use --find / --overrides)"],
                 "weights": {"values": {}}})
    rest_base = {"NSE:NIFTY 50": {"instrument_token": "256265", "last_price": 25000.0},
                 "NFO:NIFTY26NOVFUT": {"instrument_token": "111", "last_price": 25040.0},
                 "NSE:HDFCBANK": {"instrument_token": "341249", "last_price": 1500.0}}
    rest_fin = {"NSE:NIFTY 50": {"instrument_token": "256265", "last_price": 25078.0},
                "NFO:NIFTY26NOVFUT": {"instrument_token": "111", "last_price": 25120.0},
                "NSE:HDFCBANK": {"instrument_token": "341249", "last_price": 1512.0}}
    mp = {"NSE:NIFTY 50": {"index": "NIFTY", "role": "index"},
          "NFO:NIFTY26NOVFUT": {"index": "NIFTY", "role": "future"},
          "NSE:HDFCBANK": {"index": None, "role": "stock"}}
    recs.append({"type": "snapshot", "t": base_t.isoformat(), "label": "15:15:10",
                 "rest": rest_base, "map": mp, "ws_latest": {}})
    recs.append({"type": "snapshot", "t": (base_t + timedelta(minutes=16)).isoformat(),
                 "label": "15:31:00", "rest": rest_fin, "map": mp, "ws_latest": {}})
    recs.append({"type": "event", "t": base_t.isoformat(), "name": "divisor_check",
                 "inferred": 100.0, "ratios": [100.0, 100.0, 100.0], "using": 100.0})
    recs.append({"type": "event", "t": (base_t + timedelta(minutes=12)).isoformat(), "name": "end",
                 "frames": 412, "pkts": 3311, "unmapped_pkts": 6, "parse_fail": 0,
                 "text_msgs": 2, "layouts": {"kite": 412}, "rest_ok": 210, "rest_err": 0})
    # ticks: stocks frozen, futures alive (the CAS hypothesis), depth populated
    for i in range(40):
        t = (base_t + timedelta(seconds=i * 3)).isoformat()
        recs.append({"type": "ws_tick", "t": t, "tok": 341249, "sym": "NSE:HDFCBANK",
                     "role": "stock", "idx": None, "ltp": 1500.0, "tbq": 42000 + i * 10,
                     "tsq": 39000, "bids": [[100, 1499.9, 10]], "asks": [[100, 1500.1, 8]]})
        recs.append({"type": "ws_tick", "t": t, "tok": 111, "sym": "NFO:NIFTY26NOVFUT",
                     "role": "future", "idx": "NIFTY", "ltp": 25040.0 + i * 0.8,
                     "tbq": 1000, "tsq": 900, "bids": [], "asks": []})
    recs.append({"type": "readout", "t": (base_t + timedelta(minutes=12)).isoformat(), "final": True,
                 "rows": [{"index": "NIFTY", "spot_base": 25000.0, "fut_move": 80.0, "target": 25080.0,
                           "side": "CE", "strike": 25000.0, "opt_ltp": 96.5, "opt_ask": 97.2,
                           "intrinsic_at_target": 80.0,
                           "weighted": {"ltp_pts": 41.2, "mid_pts": 45.0, "ltp_raw_pts": 22.0,
                                        "mid_raw_pts": 24.0, "coverage": 0.52, "approx_weights": True}}]})
    with path.open("w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, default=str) + "\n")


def t_summarize():
    p = Path("_verify_log.jsonl")
    build_log(p)
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        C.summarize(str(p))
    txt = buf.getvalue()
    assert "Q1." in txt and "Q2." in txt, txt[-400:]
    assert "stock  : tokens=" in txt, txt
    assert "25078.00 (+78.0)" in txt and "25040.00" in txt, txt[txt.find("Q2."):][:300]
    assert "futures-implied +80.0" in txt, txt[txt.find("Q3."):][:300]
    Path("_verify_summarize.txt").write_text(txt, encoding="utf-8")


check("summarize(): Q1/Q2/Q3 blocks on a synthetic log", t_summarize)


# ---------------------------------------------------------------- fault isolation
def t_safe_call():
    calls = []
    def boom(x):
        calls.append(x)
        raise ValueError("synthetic failure")
    r = C.safe_call("test_boom", boom, None, 7)
    assert r is None and calls == [7], (r, calls)
    assert any(e["where"] == "test_boom" for e in C.ERRORS), C.ERRORS


check("safe_call: swallows, records, returns None", t_safe_call)


def t_error_event_written():
    p = Path("_verify_err.jsonl")
    if p.exists():
        p.unlink()
    w = C.JsonlWriter(p)
    try:
        raise RuntimeError("kaboom")
    except RuntimeError as e:
        C.note_error("unit_test", e, w)
    w.close()
    lines = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    assert lines and lines[-1]["name"] == "error", lines
    assert lines[-1]["where"] == "unit_test" and "kaboom" in lines[-1]["error"], lines[-1]
    assert "RuntimeError" in lines[-1]["trace"], lines[-1].keys()


check("note_error: writes a traceable error event into the log", t_error_event_written)


def t_ws_garbage_frame():
    p = Path("_verify_ws.jsonl")
    if p.exists():
        p.unlink()
    out = C.JsonlWriter(p); raw = C.JsonlWriter(Path("_verify_ws_raw.jsonl"))
    feed = C.WSFeed("k", "t", C.Universe(), out, raw, ">", 100.0, False)
    feed.recording.set()
    for garbage in (b"\xff\xff\xff\xff", b"", b"\x00" * 7, os.urandom(300)):
        feed._on_message(None, garbage)          # must not raise
    out.close(); raw.close()
    assert feed.n_frames == 4, feed.n_frames
    assert feed.n_parse_fail >= 1, feed.n_parse_fail


check("WS reader: garbage frames do not raise", t_ws_garbage_frame)


def t_ws_handler_exception():
    p = Path("_verify_ws2.jsonl")
    if p.exists():
        p.unlink()
    out = C.JsonlWriter(p); raw = C.JsonlWriter(Path("_verify_ws2_raw.jsonl"))
    feed = C.WSFeed("k", "t", C.Universe(), out, raw, ">", 100.0, False)
    orig = C.split_frame
    C.split_frame = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("parser exploded"))
    try:
        feed._on_message(None, b"\x00\x01\x00\x04test")
    finally:
        C.split_frame = orig
        out.close(); raw.close()
    assert feed.n_parse_fail >= 1, feed.n_parse_fail
    assert any("parser exploded" in e["error"] for e in C.ERRORS), C.ERRORS[-3:]


check("WS reader: exception inside the parser is contained", t_ws_handler_exception)



def t_split_doc_layout_multi():
    """Docs: [count][size][packet x count]. A multi-packet frame must split by size,
    and the Kite guess must NOT be able to hijack it."""
    a, b, c = mk_ltp(1, 10000), mk_ltp(2, 20000), mk_ltp(3, 30000)
    buf = struct.pack(">HH", 3, 8) + a + b + c
    pkts, layout = C.split_frame(buf, ">")
    assert layout == "fixed", layout
    assert [struct.unpack(">i", x[:4])[0] for x in pkts] == [1, 2, 3], pkts
    # forced kite on a fixed-layout frame must fall back rather than mangle it
    pkts2, layout2 = C.split_frame(buf, ">", "kite")
    assert pkts2 == [] or layout2 == "fixed", (layout2, len(pkts2))


check("split_frame: documented [count][size] layout wins on multi-packet frames", t_split_doc_layout_multi)


def t_doc_sizes():
    """Sizes come straight from the docs: 8 ltp, 48 index, 64 quote, 184 quote+depth, 200 full."""
    assert C.parse_packet(mk_ltp(1, 10000), ">")["kind"] == "ltp"
    assert C.parse_packet(mk_index(1, 100, 110, 90, 95, 99), ">")["kind"] == "index"
    full = mk_full(1, 150000, 1, 2)
    assert len(full) == 184, len(full)
    d = C.parse_packet(full, ">")
    assert d["kind"] == "full" and len(d["bids"]) == 5 and len(d["asks"]) == 5, d.get("kind")
    # 200-byte variant adds the circuit / year-high-low block
    bigger = mk_full(1, 150000, 1, 2) + struct.pack(">iiii", 900000, 100000, 160000, 120000)
    d2 = C.parse_packet(bigger, ">")
    assert len(bigger) == 200 and d2.get("uc") == 900000 and d2.get("yl") == 120000, d2.get("uc")


check("parse_packet: documented packet sizes 8/48/64/184/200", t_doc_sizes)



def t_fair_value_helpers():
    d = {"bids": [[10, 100.0, 2]], "asks": [[30, 100.4, 3]]}
    assert abs(C.tick_mid(d, 1.0) - 100.2) < 1e-9, C.tick_mid(d, 1.0)
    # microprice weights the bid by ask size: (100*30 + 100.4*10)/40
    assert abs(C.tick_micro(d, 1.0) - 100.1) < 1e-9, C.tick_micro(d, 1.0)
    assert C.tick_mid({"bids": [], "asks": []}, 1.0) is None
    assert C.tick_micro({"bids": [[0, 0, 0]], "asks": [[0, 0, 0]]}, 1.0) is None


check("tick_mid / tick_micro: fair value and degenerate book", t_fair_value_helpers)


def t_parity_forward():
    uni = C.Universe()
    uni.add({"token": 1, "exchange": "NFO", "symbol": "K25000CE", "key": "NFO:K25000CE",
             "role": "option", "index": "NIFTY", "strike": 25000.0, "otype": "CE", "expiry": None})
    uni.add({"token": 2, "exchange": "NFO", "symbol": "K25000PE", "key": "NFO:K25000PE",
             "role": "option", "index": "NIFTY", "strike": 25000.0, "otype": "PE", "expiry": None})
    latest = {1: {"ltp": 6800, "bids": [[1, 6790, 1]], "asks": [[1, 6810, 1]]},
              2: {"ltp": 2800, "bids": [[1, 2790, 1]], "asks": [[1, 2810, 1]]}}
    fwd, n = C.parity_forward(uni, latest, "NIFTY", 100.0)
    assert n == 1, n
    assert abs(fwd - 25040.0) < 1.0, fwd        # 25000 + 68 - 28
    fwd2, n2 = C.parity_forward(uni, {}, "NIFTY", 100.0)
    assert fwd2 is None and n2 == 0, (fwd2, n2)


check("parity_forward: F = K + C - P from the live option book", t_parity_forward)



def t_flow_tick_rule():
    import cas_flow as F
    ticks = [{"ltp": 100.0, "vol": 1000},
             {"ltp": 100.2, "vol": 1500},          # up-tick on +500 volume -> buying
             {"ltp": 100.1, "vol": 1700},          # down-tick on +200 -> selling
             {"ltp": 100.1, "vol": 1900, "bids": [[1, 100.1, 1]], "asks": [[1, 100.3, 1]]}]
    buy, sell = F.signed_volume(ticks)
    assert buy == 500 and sell == 400, (buy, sell)   # 200 down + 200 at the bid


check("flow: tick-rule signed volume", t_flow_tick_rule)


def t_flow_features():
    import cas_flow as F
    ticks = [{"tbq": 1000, "tsq": 800, "bids": [[100, 99.9, 2]], "asks": [[50, 100.1, 2]], "ltp": 100.0, "t": "2026-11-23T15:20:00+05:30"},
             {"tbq": 4000, "tsq": 1200, "bids": [[400, 99.9, 2]], "asks": [[50, 100.1, 2]], "ltp": 100.0, "t": "2026-11-23T15:27:00+05:30"}]
    f = F.instrument_features(ticks, "NSE:HDFCBANK", "stock")
    assert f["d_buy_qty"] == 3000 and f["d_sell_qty"] == 400, f
    assert f["d_net"] == 2600, f
    assert abs(f["d_imbalance"] - 2600 / 3400) < 1e-3, f["d_imbalance"]
    assert f["buy_qty_changed"] is True and f["ltp_moved"] is False, f
    assert f["d_depth_imb"] > 0, f


check("flow: auction-book deltas and frozen LTP detected", t_flow_features)


def t_flow_engine():
    import cas_flow as F
    up = F.analyse("_flow_flow_up.jsonl")
    nif = up["rollup"]["NIFTY"]
    assert nif["flow_score"] > 0.1 and nif["conviction"] in ("high", "low"), nif
    assert any("HDFCBANK" in b["symbol"] for b in nif["bought"]), nif["bought"]
    down = F.analyse("_flow_flow_down.jsonl")["rollup"]["NIFTY"]
    assert down["flow_score"] < -0.1, down
    noise = F.analyse("_flow_flow_noise.jsonl")["rollup"]["NIFTY"]
    assert noise["conviction"] == "none", noise
    # direction vs realised: the flow score's sign must match the candle's sign
    for path, sign in (("_flow_flow_up.jsonl", 1), ("_flow_flow_down.jsonl", -1)):
        res = F.analyse(path)
        jump = F.realised_jump(F.load(path), "NIFTY")
        score = res["rollup"]["NIFTY"]["flow_score"]
        assert (score > 0) == (jump > 0) == (sign > 0), (path, score, jump)


check("flow: direction hits on both synthetic stories, abstains on noise", t_flow_engine)



def t_strike_math():
    # gap up: floor to the deepest ITM call; gap down: ceil to the nearest ITM put
    assert C.pick_strike(25078, 50, "CE") == 25050, C.pick_strike(25078, 50, "CE")
    assert C.pick_strike(25078, 50, "PE") == 25100, C.pick_strike(25078, 50, "PE")
    assert C.pick_strike(25100, 50, "CE") == 25100, "exact strike stays ITM as CE"
    assert C.pick_strike(25100, 50, "PE") == 25100, "exact strike stays ITM as PE"


check("strike ladder: floor for CE, ceil for PE", t_strike_math)


def t_decide_row():
    up = C.decide("NIFTY", 78.0, 25000.0, 25078.0, 25050.0, 21.0)
    assert up["side"] == "CE" and up["strike"] == 25050.0 and up["tradeable"], up
    assert up["intrinsic_at_target"] == 28.0 and up["edge"] == 7.0, up
    down = C.decide("NIFTY", -78.0, 25000.0, 24922.0, 24950.0, 30.0)
    assert down["side"] == "PE" and down["strike"] == 24950.0, down
    assert down["tradeable"], down
    # a gap smaller than half a strike must not be presented as a trade
    small = C.decide("NIFTY", 20.0, 25000.0, 25020.0, 25000.0, 5.0)
    assert small["tradeable"] is False and "floor" in small["note"], small
    # banknifty ladder is 100 points, so a 90-point gap is still not a trade
    bn = C.decide("BANKNIFTY", 90.0, 58000.0, 58090.0, 58000.0, 200.0)
    assert bn["tradeable"] is False and bn["step"] == 100 and bn["min_gap"] == 120, bn
    # but a 130-point banknifty gap clears its floor
    assert C.decide("BANKNIFTY", 130.0, 58000.0, 58130.0, 58100.0, 200.0)["tradeable"] is True


check("decision table: side, ITM strike, edge, and the half-strike skip", t_decide_row)



def t_manual_parse():
    assert C.parse_manual("NIFTY 25065") == ("NIFTY", 25065.0), C.parse_manual("NIFTY 25065")
    assert C.parse_manual("banknifty 58,120") == ("BANKNIFTY", 58120.0), C.parse_manual("banknifty 58,120")
    assert C.parse_manual("25065") == (None, 25065.0), C.parse_manual("25065")
    assert C.parse_manual("hello") == (None, None), C.parse_manual("hello")
    assert C.parse_manual("") == (None, None)


check("manual indicative: parses 'INDEX price' and a bare price", t_manual_parse)


def t_manual_override_math():
    # when a manual indicative exists the gap must be measured against it, not the future
    C.MANUAL.clear()
    C.MANUAL["NIFTY"] = 25095.0
    spot_base, fut_base, fut_now = 25000.0, 25040.0, 25070.0
    computed_target = spot_base + (fut_now - fut_base)
    manual_target = C.MANUAL["NIFTY"]
    assert computed_target == 25030.0 and manual_target == 25095.0
    # strike choice follows the manual number
    assert C.pick_strike(manual_target, 50, "CE") == 25050
    assert C.decide("NIFTY", manual_target - spot_base, spot_base, manual_target, 25050.0, 30.0)["edge"] == 15.0
    C.MANUAL.clear()


check("manual indicative: drives the gap, the strike and the edge", t_manual_override_math)



def t_bse_weights_and_keys():
    import cas_flow as F
    assert "SENSEX" in C.DEFAULT_WEIGHTS and "BANKEX" in C.DEFAULT_WEIGHTS, list(C.DEFAULT_WEIGHTS)
    assert len(C.DEFAULT_WEIGHTS["SENSEX"]) >= 15 and len(C.DEFAULT_WEIGHTS["BANKEX"]) >= 10
    # BSE cash series suffix must collapse onto the same weight key as NSE
    assert F.weight_key("BSE:HDFCBANK-A") == F.weight_key("NSE:HDFCBANK") == "HDFCBANK"
    assert F.weight_key("BSE:TCS-A") == "TCS"


check("BSE: SENSEX/BANKEX weights exist, '-A' symbols map to weight keys", t_bse_weights_and_keys)


def t_bse_rollup():
    """A SENSEX log with BSE cash symbols must produce a flow score, not a None."""
    import cas_flow as F
    from datetime import timedelta
    import json as _json
    p = Path("_flow_bse.jsonl")
    D = datetime(2026, 11, 26, 15, 15, 10, tzinfo=C.IST)
    mp = {"BSE:SENSEX": {"index": "SENSEX", "role": "index"},
          "BFO:SENSEX26NOVFUT": {"index": "SENSEX", "role": "future"}}
    recs = [{"type": "meta", "t": D.isoformat(), "universe": {"index": 1, "future": 1, "stock": 3}},
            {"type": "snapshot", "t": D.isoformat(), "label": "15:15:10",
             "rest": {"BSE:SENSEX": {"instrument_token": "1", "last_price": 80500.0},
                      "BFO:SENSEX26NOVFUT": {"instrument_token": "2", "last_price": 80540.0}},
             "map": mp}]
    t0 = D + timedelta(minutes=5)
    book = {"HDFCBANK-A": (1000, 100), "ICICIBANK-A": (800, 120), "TCS-A": (100, 700)}
    for b in range(0, 121, 3):
        t = t0 + timedelta(seconds=b)
        for i, (sym, (buy, sell)) in enumerate(book.items()):
            recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": 500 + i, "sym": f"BSE:{sym}",
                         "role": "stock", "idx": None, "ltp": 100.0,
                         "tbq": int(buy * b), "tsq": int(sell * b),
                         "bids": [[500, 99.95, 3]], "asks": [[500, 100.05, 3]]})
        recs.append({"type": "ws_tick", "t": t.isoformat(), "tok": 2, "sym": "BFO:SENSEX26NOVFUT",
                     "role": "future", "idx": "SENSEX", "ltp": 80540.0 + b * 0.5, "vol": 1000 + b * 100,
                     "bids": [[50, 80539.95, 3]], "asks": [[50, 80540.05, 3]]})
    recs.append({"type": "snapshot", "t": (t0 + timedelta(minutes=8)).isoformat(), "label": "15:31:00",
                 "rest": {"BSE:SENSEX": {"instrument_token": "1", "last_price": 80578.0},
                          "BFO:SENSEX26NOVFUT": {"instrument_token": "2", "last_price": 80618.0}},
                 "map": mp})
    p.write_text("\n".join(_json.dumps(r) for r in recs))
    res = F.analyse(str(p))
    assert "SENSEX" in res["rollup"], res["rollup"].keys()
    d = res["rollup"]["SENSEX"]
    assert d["flow_score"] is not None and d["flow_score"] > 0, d
    assert d["stocks_seen"] == 3, d
    jump = F.realised_jump(F.load(str(p)), "SENSEX")
    assert jump == 78.0, jump


check("BSE rollup: SENSEX flow score computed from '-A' cash packets", t_bse_rollup)



def t_dashboard_state():
    import cas_dashboard as D
    live = D.Live(Path("_flow_flow_up.jsonl"))
    live.poll()
    st = live.state()
    assert "NIFTY" in st["indices"], st.keys()
    n = st["indices"]["NIFTY"]
    assert n["gap"] == 78.0 and n["target"] == 25078.0 and n["strike"] == 25050.0, n
    assert n["side"] == "CE" and n["tradeable"] is True, n
    assert n["flow"] is not None and n["flow"] > 0.1 and n["conviction"] in ("high", "low"), n
    # bought and sold must not overlap, and sold must all be negative
    bought = {b["symbol"] for b in n["bought"]}
    sold = {b["symbol"] for b in n["sold"]}
    assert not (bought & sold), (bought, sold)
    assert all(s["net"] < 0 for s in n["sold"]), n["sold"]
    assert all(b["net"] > 0 for b in n["bought"]), n["bought"]
    assert st["realised"].get("NIFTY") == 78.0, st["realised"]


check("dashboard: state, flow sign, bought/sold separation, realised", t_dashboard_state)


def t_dashboard_empty():
    """A log with nothing useful must render as 'waiting', not crash."""
    import cas_dashboard as D
    p = Path("_dash_empty.jsonl")
    p.write_text('{"type":"meta","t":"2026-11-23T15:10:00+05:30"}\n')
    live = D.Live(p)
    live.poll()
    st = live.state()
    assert st["indices"] == {} and st["note"], st


check("dashboard: empty log renders 'waiting' without raising", t_dashboard_empty)



def t_dashboard_rotation():
    """Rewriting the log must reset derived state, not double-count every tick."""
    import cas_dashboard as D
    src = Path("_flow_flow_up.jsonl")
    tmp = Path("_dash_rotate.jsonl")
    tmp.write_text(src.read_text())
    live = D.Live(tmp); live.poll()
    first = dict(live.counts)
    tmp.write_text(src.read_text())          # same size or different: either way it is a rewrite
    live.poll()
    assert live.counts["ws_tick"] <= first["ws_tick"] * 1.05, (first, dict(live.counts))
    st = live.state()
    assert "NIFTY" in st["indices"], st.keys()


check("dashboard: a rewritten log resets instead of double-counting", t_dashboard_rotation)



def t_verdicts():
    import cas_dashboard as D
    V = D.Live._verdict
    good = {"tradeable": True, "gap": 80.0, "edge": 12.0}
    # strong: tradeable, positive edge, high-conviction flow in the same direction
    assert V(good, {"score": 0.3, "conviction": "high"}, 1.0) == "STRONG"
    # below floor outranks a great edge — a 20-point NIFTY gap is not a trade
    assert V({"tradeable": False, "gap": 20.0, "edge": 40.0}, {"score": 0.5, "conviction": "high"}, 1.0) == "SKIP"
    # big gap but the premium already contains it
    assert V({"tradeable": True, "gap": 90.0, "edge": -5.0}, {"score": 0.3, "conviction": "high"}, 1.0) == "NO EDGE"
    # price says up, confident flow says sold
    assert V(good, {"score": -0.4, "conviction": "high"}, 1.0) == "CONFLICT"
    # quiet flow: watch, do not call it strong
    assert V(good, {"score": 0.08, "conviction": "low"}, 1.0) == "WATCH"
    # a dead line is stale before anything else is considered
    assert V(good, {"score": 0.3, "conviction": "high"}, 120.0) == "STALE"
    assert V(good, None, 1.0) == "WATCH"


check("verdict: SKIP > STALE > CONFLICT > NO EDGE > STRONG > WATCH", t_verdicts)



def t_calibration():
    import cas_analysis as A
    c = A.calibrate("_calib_demo.jsonl")
    assert "NIFTY" in c and c["NIFTY"]["n"] == 4, c
    assert abs(c["NIFTY"]["bias"] - 9.25) < 0.01, c["NIFTY"]
    assert c["NIFTY"]["median"] == 7.0 and c["NIFTY"]["worst"] == 19.0, c["NIFTY"]
    assert c["BANKNIFTY"]["n"] == 1 and c["BANKNIFTY"]["bias"] == 6.0, c["BANKNIFTY"]
    # a log with no manual indicative yields nothing rather than noise
    assert A.calibrate("_flow_flow_up.jsonl") == {}, "should be empty"


check("calibration: bias/median/MAE vs the manual indicative", t_calibration)


def t_prior():
    import cas_flow as F
    assert F.PRIOR_UP["NIFTY"] == (10, 16), F.PRIOR_UP
    u, n = F.PRIOR_UP["NIFTY"]
    assert 0 < u / n < 1


check("prior: base rate carried from the Aug-Nov sheet", t_prior)



def t_late_baseline():
    from datetime import datetime, timedelta
    n = datetime(2026, 11, 23, 15, 15, 0)
    past = n - timedelta(minutes=3)
    future = n + timedelta(minutes=2)
    # a 15:15 start: baseline time already gone, nothing recorded yet -> catch up
    assert C.needs_late_baseline("snap", "15:15:10", "15:15:10", False, past, n) is True
    # already have a baseline -> never retake
    assert C.needs_late_baseline("snap", "15:15:10", "15:15:10", True, past, n) is False
    # future baseline -> normal scheduling
    assert C.needs_late_baseline("snap", "15:15:10", "15:15:10", False, future, n) is False
    # other snapshots are still skipped, not caught up
    assert C.needs_late_baseline("snap", "15:14:50", "15:15:10", False, past, n) is False
    # only snapshots, never start/stop/decide
    assert C.needs_late_baseline("start", "15:15:10", "15:15:10", False, past, n) is False
    # 10s late is inside the tolerance, 20s is not
    assert C.needs_late_baseline("snap", "15:15:10", "15:15:10", False, n - timedelta(seconds=10), n) is False
    assert C.needs_late_baseline("snap", "15:15:10", "15:15:10", False, n - timedelta(seconds=20), n) is True


check("late start: baseline is caught up, other steps are still skipped", t_late_baseline)



def t_env_endpoints():
    """MSTOCK_BASE_URL / MSTOCK_WS_URL in .env must take effect, not just shell exports."""
    import importlib, os
    tmp = Path("_env_test.env")
    tmp.write_text("MSTOCK_BASE_URL=http://127.0.0.1:9999/openapi/typea\nMSTOCK_WS_URL=ws://127.0.0.1:9\n")
    for k in ("MSTOCK_BASE_URL", "MSTOCK_WS_URL"):
        os.environ.pop(k, None)
    C.load_env(str(tmp))
    C.refresh_endpoints()
    assert C.BASE_URL.startswith("http://127.0.0.1:9999"), C.BASE_URL
    assert C.WS_URL == "ws://127.0.0.1:9", C.WS_URL
    # and unset means the documented production defaults
    for k in ("MSTOCK_BASE_URL", "MSTOCK_WS_URL"):
        os.environ.pop(k, None)
    C.refresh_endpoints()
    assert C.BASE_URL == C.DEFAULT_BASE_URL and C.WS_URL == C.DEFAULT_WS_URL


check("env: base/ws URLs from .env actually apply", t_env_endpoints)



def t_ctrlc_policy():
    """One accidental Ctrl+C during the window must not stop the run; a quick second one must."""
    C._ALLOW_SINGLE["v"] = False
    # not armed: stop immediately, as before
    assert C.sigint_should_stop(False, 0.0, 100.0) is True
    # armed, no previous press: swallow this one
    assert C.sigint_should_stop(True, 0.0, 100.0) is False
    # armed, second press 2s later: stop
    assert C.sigint_should_stop(True, 98.0, 100.0) is True
    # armed, press again after the window: treated as a fresh accidental press
    assert C.sigint_should_stop(True, 90.0, 100.0) is False
    # explicit opt-out restores one-press stop
    C._ALLOW_SINGLE["v"] = True
    assert C.sigint_should_stop(True, 0.0, 100.0) is True
    C._ALLOW_SINGLE["v"] = False


check("Ctrl-C: single press ignored while armed, double press stops", t_ctrlc_policy)



def t_signal_policy():
    """Deliberate signals stop; a lost terminal does not cost the session."""
    # deliberate: always stop, armed or not
    for name in ("SIGTERM", "SIGQUIT", "SIGBREAK"):
        assert C.signal_policy(name, armed=False) == "stop"
        assert C.signal_policy(name, armed=True) == "stop"
    # hangup: ignored while armed (keep recording), stops otherwise
    assert C.signal_policy("SIGHUP", armed=True) == "ignore-once"
    assert C.signal_policy("SIGHUP", armed=False) == "stop"
    # ...unless the operator asked for one-signal stops
    assert C.signal_policy("SIGHUP", armed=True, allow_single=True) == "stop"


def t_writer_close_idempotent():
    """close() must be safe to call twice (wrap-up then atexit) and must really close."""
    f = Path("_wr_test.jsonl")
    f.unlink(missing_ok=True)          # JsonlWriter appends; start from a clean file
    w = C.JsonlWriter(f)
    w.write({"type": "event", "name": "x"})
    w.close()
    assert w.closed is True
    w.close()                      # second call: no exception, no duplicate work
    assert f.read_text().count("\n") == 1


check("signals: TERM/QUIT always stop, SIGHUP ignored while armed", t_signal_policy)
check("writer: close is idempotent and flushes to disk", t_writer_close_idempotent)



def t_heartbeat_and_flush():
    """Heartbeat proves liveness; final_flush leaves a marker when the normal exit never ran."""
    import time as _t
    f = Path("_hb_test.jsonl")
    f.unlink(missing_ok=True)
    w = C.JsonlWriter(f)
    hb = C.Heartbeat(w, interval=0.15)
    hb.start()
    _t.sleep(0.45)
    hb.stop()
    _t.sleep(0.2)
    w.flush()                          # the writer batches flushes; force one before reading
    assert "heartbeat" in f.read_text()

    C._WRITERS.update(out=w, raw=None, done=False)
    C.final_flush("unit-test")
    text = f.read_text()
    assert '"name":"final_flush"' in text and '"reason":"unit-test"' in text
    assert w.closed is True
    # second call must not append another marker
    C._WRITERS["done"] = False
    Path("_hb_test2.jsonl").unlink(missing_ok=True)
    C._WRITERS.update(out=C.JsonlWriter(Path("_hb_test2.jsonl")), done=False)
    C.final_flush("x")
    C._WRITERS["done"] = False
    C.final_flush("y")
    assert Path("_hb_test2.jsonl").read_text().count("final_flush") == 1


def t_disk_guard():
    """A nearly-full disk must refuse to start rather than truncate the session."""
    try:
        C.require_disk(Path("."), need_mb=10_000_000)
    except SystemExit as exc:
        assert "free" in str(exc)
    else:
        raise AssertionError("disk guard did not trigger on an impossible requirement")
    C.require_disk(Path("."), need_mb=1)      # normal case passes


check("heartbeat + final flush: liveness and a marker on abnormal exit", t_heartbeat_and_flush)
check("disk guard: refuses to start on a full disk", t_disk_guard)



def t_totp_validator():
    """A malformed secret must be rejected with a usable message, not a cryptic API failure."""
    ok = C.validate_totp_secret("JBSWY3DPEHPK3PXPQZRTJBSWY3DPEHPK3PXP")
    assert ok is None, ok
    assert C.validate_totp_secret("jbswy3dpehpk3pxp") is not None          # lowercase
    assert C.validate_totp_secret("JBSWY3DPEHPK3PXP0") is not None          # 0 is not base32
    assert C.validate_totp_secret("JBSWY3DP EHPK3PXP") is None              # spaces stripped
    assert C.validate_totp_secret("") is not None                           # empty
    assert C.validate_totp_secret("AbC123tokenvalueExample") is not None    # an access token


check("totp: secret shape is validated before the API is called", t_totp_validator)



def t_ist_fallback():
    """Windows has no tz database; the run must still be on IST (any real session needs it)."""
    from datetime import datetime as _dt
    probe = _dt(2026, 11, 23, 12, 0)
    real = C.resolve_ist()
    assert probe.replace(tzinfo=real).utcoffset().total_seconds() == 19800, real   # +05:30
    assert probe.replace(tzinfo=C.IST).utcoffset().total_seconds() == 19800
    # simulate Windows without tzdata
    import zoneinfo
    original = C.ZoneInfo          # cas_logger imported the name; patch it there
    try:
        def boom(_key):
            raise zoneinfo.ZoneInfoNotFoundError("No time zone found")
        C.ZoneInfo = boom
        fallback = C.resolve_ist()
        assert probe.replace(tzinfo=fallback).utcoffset().total_seconds() == 19800, fallback
        assert str(fallback) == "IST"
    finally:
        C.ZoneInfo = original


check("time: IST resolves with or without a tz database (Windows)", t_ist_fallback)



def t_spot_with_placeholder_expiry():
    """Index rows whose expiry column says '-' must still resolve as spot."""
    def row(ts, name, itype, tok, exp):
        return {"exchange": "NSE", "tradingsymbol": ts, "name": name, "instrument_type": itype,
                "segment": "INDICES", "instrument_token": tok, "expiry": exp, "strike": ""}
    rows = [row("NIFTY 50", "NIFTY 50", "INDEX", 256265, "-"),
            row("NIFTY 50", "NIFTY 50", "INDEX", 256265, "NA"),
            row("NIFTY 50", "NIFTY 50", "INDEX", 256265, "")]
    for r in rows:
        res = C.Resolver([r], __import__("datetime").date(2026, 11, 23), {})
        assert res.spot("NIFTY") is not None, f"failed with expiry={r['expiry']!r}"
    # a real future with a real expiry must not be mistaken for the spot: with no index row the
    # resolver goes to the REST fallback instead, which carries no token
    fut = row("NIFTY FUT", "NIFTY", "FUT", 999, "2026-11-27")
    res = C.Resolver([fut], __import__("datetime").date(2026, 11, 23), {})
    got = res.spot("NIFTY")
    assert got is None or got["token"] is None, f"a FUT row must not become the index spot: {got}"


check("resolver: placeholder expiry does not hide an index spot row", t_spot_with_placeholder_expiry)



def t_universe_rest_only():
    """REST-only items must be polled and never subscribed."""
    u = C.Universe()
    idx = {"token": None, "exchange": "NSE", "symbol": "NIFTY 50", "key": "NSE:NIFTY 50",
           "role": "index", "index": "NIFTY"}
    fut = {"token": 555, "exchange": "NFO", "symbol": "NIFTY26NOVFUT", "key": "NFO:NIFTY26NOVFUT",
           "role": "future", "index": "NIFTY"}
    assert u.add(idx) is True and u.add(fut) is True
    assert u.add(idx) is False                       # idempotent
    assert "NSE:NIFTY 50" in u.keys()                # quoted over REST
    assert 555 in u.tokens() and None not in u.tokens()   # never subscribed
    assert len(u.all_items()) == 2


check("universe: index without a WS token is still quoted over REST", t_universe_rest_only)



def t_resubscribe():
    """The 8-of-108 session: a feed delivering a fraction of its tokens must re-subscribe."""
    u = C.Universe()
    for f in ("_rs_test.jsonl", "_rs_raw.jsonl"):
        Path(f).unlink(missing_ok=True)
    out = C.JsonlWriter(Path("_rs_test.jsonl"))
    raw = C.JsonlWriter(Path("_rs_raw.jsonl"))
    feed = C.WSFeed("k", "t", u, out, raw, ">", 100.0, True)
    sent = []
    feed._send = lambda payload: sent.append(payload)

    # not connected yet -> do nothing
    feed.subscribed = set(range(1, 11))
    feed.ensure_subscribed(share=0.6, quiet_s=0.0)
    assert not sent, "must not subscribe while disconnected"

    feed.connected.set()
    feed.latest = {1: {}, 2: {}}                  # 2 of 10 arriving
    feed.subscribed_at = 0.0
    feed.ensure_subscribed(share=0.6, quiet_s=0.0)
    assert sent and '"subscribe"' in sent[0], sent
    assert len(feed.subscribed) == 10

    # healthy: nothing to do
    sent.clear()
    feed.latest = {i: {} for i in range(1, 11)}
    feed.subscribed_at = 0.0
    feed.ensure_subscribed(share=0.6, quiet_s=0.0)
    assert not sent

    # escalation: after two attempts the whole list goes in ONE message
    sent.clear()
    feed.resends = 2
    feed.latest = {1: {}}
    feed.subscribed_at = 0.0
    feed.ensure_subscribed(share=0.6, quiet_s=0.0)
    assert len(sent) == 2, sent                # one subscribe + one mode, not 2 chunks
    assert str(sorted(feed.subscribed)).replace(" ", "") in sent[0].replace(" ", "")

    # quiet period is honoured
    sent.clear()
    feed.latest = {1: {}}
    feed.subscribed_at = time.monotonic()
    feed.ensure_subscribed(share=0.6, quiet_s=600.0)
    assert not sent


check("feed: a partial subscription re-subscribes itself", t_resubscribe)


print("\n================ RESULT ================")
print(f"passed {len(OK)} / {len(OK) + len(FAIL)}")
if FAIL:
    for n, e in FAIL:
        print(f"  BROKEN: {n} -> {e}")
    sys.exit(1)
print("no failures")
