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
                        norm, pick_strike, tick_micro, tick_mid)
from cas_flow import weight_key

WINDOW_START = "15:20:00"
WINDOW_END = "15:27:30"


def ts_of(rec):
    try:
        return datetime.fromisoformat(rec.get("t", ""))
    except (TypeError, ValueError):
        return None


class Live:
    """Incremental reader over the log: keeps only what the dashboard renders."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.offset = 0
        self.meta = {}
        self.snapshots = {}
        self.latest = {}                    # tok -> tick
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
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self._apply(rec)
                self.offset = f.tell()

    def _reset(self):
        """A rewritten log (replay regenerated) must not leave stale derived state behind."""
        self.meta = {}
        self.snapshots = {}
        self.latest = {}
        self.first_in_window = {}
        self.last_tick_at = None
        self.events.clear()
        self.counts.clear()
        self.readouts.clear()
        self.spark.clear()

    def _apply(self, rec):
        kind = rec.get("type")
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
            self.latest[tok] = rec
            hm = t.strftime("%H:%M:%S") if t else ""
            if WINDOW_START <= hm <= WINDOW_END and tok not in self.first_in_window:
                self.first_in_window[tok] = rec
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

        indices = {}
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
                        ask = (tk.get("asks") or [[0, None]])[0][1]
                row = decide(idx, gap, spot, target, strike, ask)
                flow = self._flow(idx, first, latest)
                # Age is measured against the newest tick in the whole log, not wall clock, so a
                # replay of last week's session is not reported as stale. Live, the newest tick is
                # ~now, so the two agree.
                age = None
                newest_t = max((ts_of(r) for r in latest.values() if ts_of(r)), default=None)
                this_t = max((ts_of(r) for r in fut_ticks), default=None)
                if newest_t and this_t:
                    age = round((newest_t - this_t).total_seconds(), 1)
                verdict = self._verdict(row, flow, age)
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

        return {
            "now": datetime.now().strftime("%H:%M:%S"),
            "file": str(self.path),
            "records": counts,
            "last_tick_age": last_age,
            "file_age": file_age,
            "events": events[-8:],
            "indices": indices,
            "realised": realised,
            "readouts": readouts[-1:] ,
            "note": "no baseline snapshot yet (waiting for 15:15:10)" if not baseline else None,
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
        if conv == "high":
            return "STRONG"
        return "WATCH"

    def _flow(self, idx, first, latest):
        W = {norm(k): v for k, v in (DEFAULT_WEIGHTS.get(idx) or {}).items()}
        wsum = wnet = 0.0
        names = []
        for tok, f in first.items():
            role = f.get("role")
            if role != "stock":
                continue
            key = weight_key(f.get("sym") or "")
            if key not in W:
                continue
            l = latest.get(tok)
            if not l:
                continue
            db = (l.get("tbq") or 0) - (f.get("tbq") or 0)
            ds = (l.get("tsq") or 0) - (f.get("tsq") or 0)
            traded = abs(db) + abs(ds)
            if traded <= 0:
                continue
            imb = (db - ds) / traded
            wsum += W[key]
            wnet += W[key] * imb
            names.append({"symbol": (f.get("sym") or "").split(":")[-1], "net": db - ds, "imb": imb})
        score = round(wnet / wsum, 3) if wsum else None
        if score is None:
            # no weighted cash imbalance: fall back to the futures tape as a ratio
            for tk in latest.values():
                if tk.get("role") != "future" or tk.get("idx") != idx:
                    continue
                vb, vs = tk.get("signed_buy"), tk.get("signed_sell")
                if vb is not None and vs is not None and (vb + vs) > 0:
                    score = round((vb - vs) / (vb + vs), 3)
                    break
        conviction = None
        if score is not None:
            conviction = "high" if abs(score) >= 0.15 else ("low" if abs(score) >= 0.03 else "none")
        bought = sorted([n for n in names if n["net"] > 0], key=lambda n: n["net"], reverse=True)[:4]
        sold = sorted([n for n in names if n["net"] < 0], key=lambda n: n["net"])[:4]
        return {"score": score, "conviction": conviction, "bought": bought, "sold": sold}


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CAS dashboard</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:#08080c;color:#e8e6e0;font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}
header{display:flex;gap:16px;align-items:baseline;padding:14px 18px;border-bottom:1px solid #1d1d27;
  position:sticky;top:0;background:#08080cf2;backdrop-filter:blur(8px);flex-wrap:wrap}
h1{font:600 15px ui-sans-serif,system-ui;margin:0;letter-spacing:.02em}
.meta{color:#8b8b98;font-size:11px}
.pill{padding:2px 8px;border-radius:999px;border:1px solid #2a2a36;font-size:10px;letter-spacing:.08em;text-transform:uppercase}
.live{border-color:#2c5f3f;color:#7ee2a8}.dead{border-color:#6b2020;color:#ff9b9b}
main{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,400px));gap:12px;padding:14px;justify-content:start}
.card{background:#101018;border:1px solid #23232f;border-radius:10px;padding:12px 14px}
.card h2{font:600 11px ui-sans-serif,system-ui;text-transform:uppercase;letter-spacing:.14em;color:#9aa0ff;margin:0 0 10px;display:flex;align-items:center;gap:8px}
.verdict{font-size:9px;letter-spacing:.1em;padding:2px 7px;border-radius:999px;border:1px solid #2a2a36;color:#c9c9d6}
.v-strong{border-color:#2c5f3f}.v-strong .verdict{border-color:#2c5f3f;color:#7ee2a8}
.v-watch .verdict{border-color:#5c4a1e;color:#ffcc66}
.v-skip{border-color:#33333f}.v-skip .verdict{color:#7d7d8c}
.v-noedge{border-color:#6b5420}.v-noedge .verdict{border-color:#6b5420;color:#ffcc66}
.v-conflict{border-color:#6b2020}.v-conflict .verdict{border-color:#6b2020;color:#ff9b9b}
.v-stale{border-color:#3a3a48;opacity:.75}.v-stale .verdict{color:#ff9b9b}
.row{display:flex;justify-content:space-between;gap:10px;padding:3px 0;border-bottom:1px solid #181820}
.row:last-child{border:0}
.k{color:#7d7d8c}.v{font-variant-numeric:tabular-nums}
.big{font-size:22px;letter-spacing:-.02em}
.up{color:#7ee2a8}.down{color:#ff9b9b}.warn{color:#ffcc66}.muted{color:#7d7d8c}
.bar{height:6px;background:#1b1b25;border-radius:3px;overflow:hidden;margin-top:8px}
.bar span{display:block;height:100%;background:linear-gradient(90deg,#6b6bff,#9aa0ff)}
.skip{border-color:#6b2020}
.spark{width:100%;height:38px;margin-top:8px}
footer{padding:8px 18px 20px;color:#5f5f6e;font-size:11px}
</style></head><body>
<header>
  <h1>CAS dashboard</h1>
  <span class="meta" id="clock">--:--:--</span>
  <span class="pill" id="feed">waiting</span>
  <span class="meta" id="meta"></span>
</header>
<main id="grid"></main>
<footer id="foot"></footer>
<script>
const fmt = (n, d=2) => (n===null||n===undefined) ? '—' : Number(n).toFixed(d);
function card(idx, d){
  const dir = d.gap > 0 ? 'up' : 'down';
  const flowPct = d.flow===null ? 0 : Math.min(100, Math.abs(d.flow)*100);
  const bought = (d.bought||[]).map(b=>`${b.symbol} ${b.net>0?'+':''}${b.net}`).join(', ') || '—';
  const sold = (d.sold||[]).map(b=>`${b.symbol} ${b.net}`).join(', ') || '—';
  const edge = d.edge===null||d.edge===undefined ? '<span class="muted">not subscribed</span>'
      : `<span class="${d.edge>0?'up':'warn'}">${d.edge>0?'+':''}${fmt(d.edge)}</span>`;
  const vClass = {STRONG:'v-strong', WATCH:'v-watch', SKIP:'v-skip', 'NO EDGE':'v-noedge',
                  CONFLICT:'v-conflict', STALE:'v-stale'}[d.verdict] || 'v-watch';
  const ageNote = (d.tick_age===null||d.tick_age===undefined) ? ''
      : `<span class="meta" style="margin-left:auto">${fmt(d.tick_age,0)}s</span>`;
  return `<section class="card ${vClass}">
    <h2>${idx}<span class="verdict">${d.verdict}</span>${ageNote}</h2>
    <div class="row"><span class="k">gap vs 3:15</span><span class="v big ${dir==='up'?'up':'down'}">${d.gap>0?'+':''}${fmt(d.gap)}</span></div>
    <div class="row"><span class="k">spot → target</span><span class="v">${fmt(d.spot)} → ${fmt(d.target)}</span></div>
    <div class="row"><span class="k">trade</span><span class="v">${d.side} <b>${fmt(d.strike,0)}</b></span></div>
    <div class="row"><span class="k">ask now</span><span class="v">${d.ask===null?'—':fmt(d.ask)}</span></div>
    <div class="row"><span class="k">edge at target</span><span class="v">${edge}</span></div>
    <div class="row"><span class="k">flow (${d.conviction||'—'})</span><span class="v">${d.flow===null?'—':(d.flow>0?'+':'')+fmt(d.flow,3)}</span></div>
    <div class="bar"><span style="width:${flowPct}%"></span></div>
    <div class="row"><span class="k">bought</span><span class="v">${bought}</span></div>
    <div class="row"><span class="k">sold</span><span class="v">${sold}</span></div>
    ${d.note?`<div class="row"><span class="k warn">${d.note}</span></div>`:''}
    <canvas class="spark" data-spark='${JSON.stringify(d.spark||[])}'></canvas>
  </section>`;
}
function drawSpark(c){
  const pts = JSON.parse(c.dataset.spark||'[]');
  if(!pts.length) return;
  const w=c.width=c.clientWidth*2, h=c.height=76;
  const ys=pts.map(p=>p[1]); const min=Math.min(...ys), max=Math.max(...ys);
  const x=i=>i*(w/(pts.length-1||1)), y=v=>h-8-((v-min)/((max-min)||1))*(h-16);
  const ctx=c.getContext('2d'); ctx.clearRect(0,0,w,h);
  ctx.strokeStyle='#6b6bff'; ctx.lineWidth=2; ctx.beginPath();
  pts.forEach((p,i)=> i?ctx.lineTo(x(i),y(p[1])):ctx.moveTo(x(i),y(p[1])));
  ctx.stroke();
}
async function tick(){
  try{
    const r = await fetch('/api/live',{cache:'no-store'});
    const s = await r.json();
    document.getElementById('clock').textContent = s.now;
    document.getElementById('meta').textContent =
      `records ${s.records.ws_tick||0} ticks · ${s.records.snapshot||0} snapshots · ${s.file.split('/').slice(-1)[0]}`;
    const f = document.getElementById('feed');
    const age = s.last_tick_age, fage = s.file_age;
    if(age===null||age===undefined){ f.className='pill dead'; f.textContent='no ticks'; }
    else if(fage!==null && fage>120){ f.className='pill'; f.textContent='replay'; }
    else if(age<10){ f.className='pill live'; f.textContent=`live · ${age}s`; }
    else { f.className='pill dead'; f.textContent=`stale · ${age}s`; }
    const grid = document.getElementById('grid');
    const entries = Object.entries(s.indices);
    grid.innerHTML = entries.length ? entries.map(([k,v])=>card(k,v)).join('')
      : `<section class="card"><h2>waiting</h2><div class="row"><span class="k">${s.note||'no data in the log yet'}</span></div></section>`;
    document.querySelectorAll('canvas.spark').forEach(drawSpark);
    const ev = (s.events||[]).map(e=>`${e.t} ${e.name}`).join(' · ');
    const real = Object.entries(s.realised||{}).map(([k,v])=>`${k} ${v>0?'+':''}${v}`).join(' · ');
    document.getElementById('foot').textContent =
      (real?`realised: ${real}   |   `:'') + (ev?`events: ${ev}`:'');
  }catch(e){ /* keep the last good frame */ }
}
tick(); setInterval(tick, 1000);
</script></body></html>"""


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
