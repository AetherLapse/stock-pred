#!/usr/bin/env python3
"""cas_dashboard.py - live view of a CAS session, in one file, stdlib only.

    python cas_dashboard.py --log cas_logs/2026-11-23/cas_2026-11-23.jsonl
    python cas_dashboard.py --log ... --port 8080 --open

It tails the JSONL that cas_logger.py is appending to, so it can run in a second
terminal during the 15:20-15:27 window, or afterwards as a replay of a finished day.
Nothing is written back: this is a read-only view of the log.

Panels
  per index   gap vs the 3:15 baseline, projected target, strike + side, ask, edge,
              tradeable/skip, flow score and conviction
  flow        top net-bought and net-sold names in the window
  feed        packets, frames, last-tick age, and the alarms the logger raised
"""
from __future__ import annotations

import argparse
import json
import threading
import time
import webbrowser
from collections import defaultdict, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from cas_logger import (DEFAULT_WEIGHTS, INDEX_CFG, MIN_GAP, STRIKE_STEP, decide,
                        norm, pick_strike, tick_micro, tick_mid, now)
from cas_flow import weight_key, book_evidence, t_of

WINDOW_START = "15:20:00"
WINDOW_END = "15:27:00"


def ts_of(rec):
    return t_of(rec)


class Live:
    """Incremental reader over the log: keeps only what the dashboard renders."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.offset = 0
        self.meta = {}
        self.snapshots = {}
        self.latest = {}                    # tok -> tick
        self.last_in_window = {}
        self.first_in_window = {}           # tok -> first tick inside 15:20-15:27:30
        self.last_tick_at = None
        self.events = deque(maxlen=40)
        self.counts = defaultdict(int)
        self.readouts = deque(maxlen=20)
        self.spark = defaultdict(lambda: deque(maxlen=180))   # index -> (hh:mm:ss, ltp)

    # -- ingestion ---------------------------------------------------------
    def poll(self):
        if not self.path.exists():
            return
        with self.lock:
            size = self.path.stat().st_size
            if size < self.offset:          # truncated / rotated: rebuild from scratch
                self._reset()
                self.offset = 0
            if size == self.offset:
                return
            with self.path.open("r", encoding="utf-8") as f:
                f.seek(self.offset)
                while True:
                    pos = f.tell()
                    line = f.readline()
                    if not line or not line.endswith('\n'):
                        self.offset = pos
                        break
                    self.offset = f.tell()
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self._apply(rec)

    def _reset(self):
        """A rewritten log (replay regenerated) must not leave stale derived state behind."""
        self.meta = {}
        self.snapshots = {}
        self.latest = {}
        self.last_in_window = {}
        self.first_in_window = {}
        self.last_tick_at = None
        self.events.clear()
        self.counts.clear()
        self.readouts.clear()
        self.spark.clear()

    def _apply(self, rec):
        kind = rec.get("type")
        if kind == "meta" and self.meta:
            self._reset()
        self.counts[kind] += 1
        if kind == "meta":
            self.meta = rec
        elif kind == "snapshot":
            self.snapshots[rec.get("label")] = rec
        elif kind == "event":
            name = rec.get("name")
            if name in ("feed_silent", "tokens_unmapped", "error", "recording_start", "recording_stop"):
                self.events.append({"t": rec.get("t", "")[11:19], "name": name})
        elif kind == "readout":
            self.readouts.append(rec)
        elif kind == "ws_tick":
            tok = rec.get("tok")
            t = ts_of(rec)
            self.last_tick_at = time.monotonic()
            if not t:
                return
            if tok not in self.latest or t >= ts_of(self.latest[tok]):
                self.latest[tok] = rec
            hm = t.strftime("%H:%M:%S") if t else ""
            if WINDOW_START <= hm <= WINDOW_END:
                if tok not in self.first_in_window or t < ts_of(self.first_in_window[tok]):
                    self.first_in_window[tok] = rec
                if tok not in self.last_in_window or t > ts_of(self.last_in_window[tok]):
                    self.last_in_window[tok] = rec
            idx = rec.get("idx")
            if idx and rec.get("role") == "future" and rec.get("ltp") and hm:
                self.spark[idx].append((hm, rec["ltp"]))

    # -- derived -----------------------------------------------------------
    def _baseline(self):
        return self.snapshots.get("15:15:10") or self.snapshots.get("test_start")

    def _index_spot(self, snap, idx):
        for key, m in (snap.get("map") or {}).items():
            if m.get("index") == idx and m.get("role") == "index":
                v = (snap.get("rest") or {}).get(key)
                if v and v.get("last_price"):
                    return float(v["last_price"])
        return None

    def _spot_axis(self, snap):
        """Map token -> (role, index, strike, otype) from the snapshot's map."""
        out = {}
        # the map is keyed by EXCH:SYMBOL; ticks carry tokens, so rebuild from meta sample if absent
        for key, m in (snap.get("map") or {}).items():
            out[key] = m
        return out

    def state(self):
        with self.lock:
            baseline = self._baseline()
            post = self.snapshots.get("15:31:00") or self.snapshots.get("test_end")
            latest = dict(self.latest)
            first = dict(self.first_in_window)
            window_last = dict(self.last_in_window)
            spark = {k: list(v) for k, v in self.spark.items()}
            events = list(self.events)
            counts = dict(self.counts)
            readouts = list(self.readouts)
            last_age = None if self.last_tick_at is None else round(time.monotonic() - self.last_tick_at, 1)
            try:
                file_age = round(time.time() - self.path.stat().st_mtime, 1)
            except OSError:
                file_age = None
            meta = self.meta

        clock = now()
        newest = max((ts_of(r) for r in latest.values() if ts_of(r)), default=None)
        last_age = round(max(0, (clock-newest).total_seconds()), 1) if newest else None
        args = meta.get('args') or {}
        mode = 'TEST' if args.get('test') else ('REPLAY' if (post or (newest and newest.date() != clock.date())) else 'LIVE')
        reference = newest if mode == 'REPLAY' and newest else clock
        cutoff = reference.replace(hour=15, minute=27, second=0, microsecond=0)
        asof = min(reference, cutoff)
        phase = 'WAITING' if reference.strftime('%H:%M:%S') < WINDOW_START else ('COLLECTING' if reference < cutoff else 'FROZEN')
        selected = args.get('indices') or list(INDEX_CFG)
        if isinstance(selected, str): selected = selected.split(',')
        selected = [i.strip().upper() for i in selected if i.strip().upper() in INDEX_CFG]
        evidence = {i: book_evidence(i, first, window_last, asof, (meta.get('weights') or {}).get('values')) for i in selected}
        indices = {}
        why = []
        if baseline:
            for idx in INDEX_CFG:
                missing = []
                if self._index_spot(baseline, idx) is None:
                    missing.append("no 15:15 spot")
                has_fut_tick = any(tk.get("role") == "future" and tk.get("idx") == idx and tk.get("ltp")
                                   for tk in latest.values())
                if not has_fut_tick:
                    missing.append("no futures ticks")
                has_base_fut = any(m.get("index") == idx and m.get("role") == "future"
                                   and ((baseline.get("rest") or {}).get(k) or {}).get("last_price")
                                   for k, m in (baseline.get("map") or {}).items())
                if not has_base_fut:
                    missing.append("no 15:15 futures price")
                if missing:
                    why.append(f"{idx}: " + ", ".join(missing))
        else:
            why = ["no baseline snapshot in the log yet"]

        if baseline:
            for idx in INDEX_CFG:
                spot = self._index_spot(baseline, idx)
                if spot is None:
                    continue
                fut_tok, fut_now, fut_prev = None, None, None
                for tok, m in (baseline.get("map") or {}).items():
                    if m.get("index") == idx and m.get("role") == "future":
                        fut_tok = tok
                # ticks carry idx, so find the newest futures tick for this index
                fut_ticks = [tk for tk in latest.values()
                             if tk.get("role") == "future" and tk.get("idx") == idx and tk.get("ltp")]
                if fut_ticks:
                    fut_now = max(fut_ticks, key=lambda r: r.get("t", ""))["ltp"]
                base_fut = None
                for key, m in (baseline.get("map") or {}).items():
                    if m.get("index") == idx and m.get("role") == "future":
                        v = (baseline.get("rest") or {}).get(key)
                        if v and v.get("last_price"):
                            base_fut = float(v["last_price"])
                if fut_now is None or base_fut is None:
                    continue
                gap = fut_now - base_fut
                target = spot + gap
                side = "CE" if gap > 0 else "PE"
                strike = pick_strike(target, STRIKE_STEP.get(idx, 50), side)
                # the option we would actually buy: look for that strike in the ticks
                ask = None
                for tk in latest.values():
                    if (tk.get("role") == "option" and tk.get("idx") == idx
                            and tk.get("k") == strike and (tk.get("ot") or "").upper() == side):
                        levels = tk.get("asks") or []
                        quote_t = ts_of(tk)
                        if (levels and len(levels[0]) >= 2 and levels[0][0] and levels[0][0] > 0
                                and levels[0][1] and levels[0][1] > 0 and quote_t
                                and 0 <= (reference - quote_t).total_seconds() <= 45):
                            ask = levels[0][1]
                row = decide(idx, gap, spot, target, strike, ask)
                flow = evidence.get(idx) or book_evidence(idx, first, window_last, asof)
                # Age is measured against the newest tick in the whole log, not wall clock, so a
                # replay of last week's session is not reported as stale. Live, the newest tick is
                # ~now, so the two agree.
                age = None
                newest_t = max((ts_of(r) for r in latest.values() if ts_of(r)), default=None)
                this_t = max((ts_of(r) for r in fut_ticks), default=None)
                if newest_t and this_t:
                    age = round(max(0, (reference - this_t).total_seconds()), 1)
                verdict = self._verdict(row, flow, age)
                if verdict == 'STRONG' and (not flow.get('eligible') or ask is None):
                    verdict = 'WATCH'
                indices[idx] = {
                    "verdict": verdict, "tick_age": age,
                    "spot": spot, "futures": round(fut_now, 2), "gap": round(gap, 2),
                    "target": round(target, 2), "side": side, "strike": strike,
                    "ask": ask, "edge": row.get("edge"), "tradeable": row["tradeable"],
                    "min_gap": row["min_gap"], "note": row.get("note"),
                    "flow": flow["score"], "conviction": flow["conviction"],
                    "bought": flow["bought"], "sold": flow["sold"],
                    "spark": spark.get(idx, [])[-60:],
                }

        realised = {}
        if baseline and post:
            for idx in indices:
                a = self._index_spot(baseline, idx)
                b = self._index_spot(post, idx)
                if a is not None and b is not None:
                    realised[idx] = round(b - a, 2)

        cards = {}
        for idx in selected:
            e = evidence[idx]
            row = dict(indices.get(idx) or {})
            reasons = list(e['reasons'])
            if idx not in indices:
                reasons.extend([w for w in why if w.startswith(idx+':')] or ['Waiting for spot and futures baseline'])
            direction = e['direction']
            if mode == 'LIVE' and phase == 'COLLECTING' and (last_age is None or last_age > 45):
                direction = 'NO SIGNAL'
                reasons.append('Live feed is stale')
            agreement = 'Unavailable'
            if e['eligible'] and row.get('gap') is not None and e['score']:
                agreement = 'AGREES' if (row['gap'] > 0) == (e['score'] > 0) else 'CONFLICT'
            row.update(flow=e['score'], direction=direction, evidence=e, reasons=reasons,
                       agreement=agreement, verdict=row.get('verdict', 'WATCH'))
            cards[idx] = row

        return {
            "now": clock.strftime("%H:%M:%S"),
            "session": {"mode": mode, "phase": phase, "window": "15:20–15:27 IST"},
            "cards": cards,
            "file": str(self.path),
            "records": counts,
            "last_tick_age": last_age,
            "file_age": file_age,
            "events": events[-8:],
            "indices": indices,
            "realised": realised,
            "readouts": readouts[-1:] ,
            "note": ("no baseline snapshot yet (waiting for 15:15:10)" if not baseline
                     else ("no card can be built — " + " | ".join(why) if why and not indices else None)),
            "why": why,
        }

    @staticmethod
    def _verdict(row, flow, age):
        """One word that answers 'should I act on this card?'.

        Order matters: a stale or below-floor card is never tradeable regardless of edge, and a
        negative edge means the premium already contains the move even when the gap is large.
        """
        if age is not None and age > 60:
            return "STALE"
        if not row["tradeable"]:
            return "SKIP"
        edge = row.get("edge")
        flow = flow or {}
        score, conv = flow.get("score"), flow.get("conviction")
        conflict = (score is not None and conv == "high" and (score > 0) != (row["gap"] > 0))
        if conflict:
            return "CONFLICT"
        if edge is not None and edge <= 0:
            return "NO EDGE"
        if conv == "high" and edge is not None:
            return "STRONG"
        return "WATCH"

    def _flow(self, idx, first, latest):
        return book_evidence(idx, first, latest)


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Closing window · CAS</title><style>
:root{color-scheme:dark;font-family:Inter,system-ui,sans-serif;color:#e8eef7;background:#0b1220}*{box-sizing:border-box}body{margin:0}header,.intro,footer{padding:22px max(24px,4vw)}header{border-bottom:1px solid #28364b;display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap}h1{font-size:22px;margin:0}h2{font-size:17px;margin:0}.sub,small{color:#a6b6cc}.status{color:#93c5fd}.intro{padding-bottom:8px}.steps{display:flex;gap:12px;flex-wrap:wrap;margin:18px 0}.step{background:#172238;padding:10px 14px;border-radius:8px}.step strong{display:block;color:#93c5fd}.notice{max-width:1000px;line-height:1.6;color:#bdcbe0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr));gap:18px;padding:20px max(24px,4vw)}article{border:1px solid #2b3b53;background:#111d30;border-radius:14px;padding:22px}.top,.row{display:flex;justify-content:space-between;gap:16px}.badge{font-size:11px;border:1px solid #41516a;border-radius:20px;padding:4px 8px}.direction{font-size:27px;font-weight:750;margin:22px 0 6px}.up{color:#67e8b0}.down{color:#fda4af}.neutral{color:#c7d2fe}.row{padding:9px 0;border-bottom:1px solid #243249;font-size:13px}.row span:first-child{color:#a6b6cc}.row b{text-align:right}progress{width:100%;height:8px;accent-color:#60a5fa;margin:12px 0}.reason{color:#fcd58b;font-size:13px;line-height:1.5}.section{margin-top:20px;color:#9eb5d8;font-size:11px;text-transform:uppercase;letter-spacing:.08em}details{margin-top:16px;font-size:13px}summary{cursor:pointer;color:#93c5fd}table{width:100%;font-size:11px;border-collapse:collapse;margin-top:12px}td,th{text-align:left;padding:7px 2px;border-bottom:1px solid #27344a}footer{color:#a6b6cc;font-size:12px;line-height:1.7}#error{display:none;background:#783c26;padding:16px 4vw;position:sticky;top:0;z-index:2}body.disconnected .grid{opacity:.4}body.disconnected #error{display:block}
</style></head><body><div id="error">Connection lost — displayed readings are stale. Reconnecting…</div><header><div><h1>Closing window</h1><div class="sub">Buy/sell book pressure · recording only</div></div><div><div id="status" class="status">Waiting for recorder</div><small id="clock"></small></div></header><div class="intro"><div class="steps"><div class="step"><strong>15:15</strong>Lock reference prices</div><div class="step"><strong>15:20–15:27</strong>Measure book changes</div><div class="step"><strong>15:27</strong>Freeze pressure reading</div><div class="step"><strong>Before 15:28</strong>Review evidence and conflicts</div></div><div class="notice">Start with the large pressure reading. Check data coverage, then compare with futures. Book quantities are outstanding orders: changes can include cancellations and are <b>not executed buying or selling</b>. Direction remains an unvalidated hypothesis.</div></div><main class="grid" id="grid"></main><footer><b>Benchmark: 62.5% always CE</b> — supplied historical NIFTY sample (10/16), not a measured success rate for this signal. Compare eligible real sessions per index with the same-session always-up baseline. Test runs cannot validate accuracy.<div id="foot"></div></footer><script>
const fmt=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-IN',{maximumFractionDigits:d});
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const row=(a,b)=>`<div class="row"><span>${esc(a)}</span><b>${esc(b)}</b></div>`;
function card(i,d){const e=d.evidence||{},dir=d.direction||'NO SIGNAL',color=dir==='UP PRESSURE'?'up':dir==='DOWN PRESSURE'?'down':'neutral';
const names=[...(e.bought||[]),...(e.sold||[])];
return `<article data-index="${esc(i)}"><div class="top"><h2>${esc(i)}</h2><span class="badge">${esc(d.verdict)}</span></div><div class="direction ${color}">${esc(dir)}</div><small>15:20–15:27 book-pressure hypothesis</small>
${row('Weighted pressure (−1 to +1)',fmt(d.flow,3))}${row('Timely tracked weight',fmt((e.timely_coverage||0)*100,0)+'%')}
<progress max="100" value="${Math.max(0,Math.min(100,(e.timely_coverage||0)*100))}"></progress>
${row('Stocks with valid book pairs',fmt(e.stocks_seen,0))}${row('Buy quantity change',fmt(e.buy_change,0))}${row('Sell quantity change',fmt(e.sell_change,0))}
<div class="reason">${(d.reasons||[]).map(esc).join('<br>')||'Operational coverage checks passed. Predictive accuracy is still unproven.'}</div>
<div class="section">Independent futures check</div>${row('Agreement with pressure',d.agreement)}${row('Move since reference',fmt(d.gap)+' pts')}${row('Implied spot target',fmt(d.target))}${row('Futures quote age',d.tick_age==null?'Unavailable':fmt(d.tick_age,0)+'s')}
<details><summary>Conditional option scenario & stock evidence</summary><p class="sub">Target assumes the futures move carries through to spot. Intrinsic value at that target is not an option fair-value estimate or a buy instruction.</p>${row('Scenario strike',d.strike==null?'Unavailable':d.side+' '+fmt(d.strike,0))}${row('Observed ask',fmt(d.ask))}${row('Intrinsic at target minus ask',fmt(d.edge))}<table><thead><tr><th>Stock</th><th>Δ buy</th><th>Δ sell</th></tr></thead><tbody>${names.map(n=>`<tr><td>${esc(n.symbol)}</td><td>${fmt(n.buy_change,0)}</td><td>${fmt(n.sell_change,0)}</td></tr>`).join('')}</tbody></table><p class="sub">Quality rules: at least three timely stock pairs, at least 50% of configured tracked weight, first observation by 15:21, span ≥60s and endpoint age ≤45s. These thresholds are operational, not statistically calibrated.</p></details></article>`}
let busy=false;
async function tick(){if(busy)return;busy=true;try{const r=await fetch('/api/live',{cache:'no-store',signal:AbortSignal.timeout(5000)});if(!r.ok)throw Error(r.status);const s=await r.json();document.body.classList.remove('disconnected');document.getElementById('status').textContent=`${s.session.mode} · ${s.session.phase} · ${s.session.window}`;document.getElementById('clock').textContent=`${s.now} IST · last recorded tick ${fmt(s.last_tick_age,0)}s ago`;const opened=new Set([...document.querySelectorAll('article:has(details[open])')].map(e=>e.dataset.index));document.getElementById('grid').innerHTML=Object.entries(s.cards).map(([i,d])=>card(i,d)).join('');document.querySelectorAll('article').forEach(e=>{if(opened.has(e.dataset.index))e.querySelector('details').open=true});document.getElementById('foot').textContent=`${s.records.ws_tick||0} ticks · ${s.records.snapshot||0} snapshots · ${(s.events||[]).map(e=>e.t+' '+e.name).join(' · ')}`;}catch(e){document.body.classList.add('disconnected')}finally{busy=false}}
tick();setInterval(tick,1000);
</script></body></html>
"""


def make_handler(live: Live):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            b = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            if self.path.startswith("/api/live"):
                live.poll()
                return self._send(200, json.dumps(live.state()), "application/json")
            if self.path in ("/", "/index.html"):
                return self._send(200, PAGE, "text/html; charset=utf-8")
            self._send(404, "not found", "text/plain")

    return H


def main():
    ap = argparse.ArgumentParser(description="Live dashboard for a CAS log")
    ap.add_argument("--log", required=True, help="cas_<date>.jsonl (live or finished)")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--open", action="store_true", help="open the browser")
    a = ap.parse_args()

    path = Path(a.log).expanduser().resolve()
    live = Live(path)
    live.poll()
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(live))
    url = f"http://{a.host}:{a.port}/"
    print(f"CAS dashboard on {url}\n  log: {path}")
    if a.open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
