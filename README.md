# stock-pred — NSE/BSE closing-auction (CAS) research rig

Records what the mStock Type A API actually exposes during the 3:15–3:30 PM Closing Auction Session,
estimates where the index will settle, scores those estimates against what actually happened, and
shows the whole thing on a live dashboard.

**It places no orders.** Nothing here calls an order endpoint. It is a recorder, an estimator and a
scoreboard.

---

## Why this exists

An index's closing price is set in an auction, and the final print lands in a 3:28–3:30 window. The
retail theory is that you can watch the auction between 3:20 and 3:27, work out which strike is about
to flip in-the-money, and buy it seconds before the close.

Two questions decide whether that is real:

1. **Does the broker feed carry the auction book at all?** NSE publishes indicative price and quantity
   on its own terminals; it does not hand the book to vendors. If mStock's `Total Buy Qty` /
   `Total Sell Qty` and depth are frozen during CAS, the idea dies on the data, not on the maths.
2. **If it does, does the projected move leave any premium on the table?** A strike that flips ITM by
   40 points is worthless if the call already costs 70.

This repo answers both with a log instead of an opinion.

## What it produces

- `cas_logs/<date>/cas_<date>.jsonl` — every tick, snapshot, REST poll and readout for the session
- `cas_logs/<date>/cas_<date>.ws_raw.jsonl` — raw WebSocket frames (base64) for offline re-parsing
- per-index decision rows: gap vs the 3:15 baseline, projected target, strike, ask, **edge**, flow
  score and a one-word verdict
- a live dashboard, plus offline scorers that say whether the estimates were any good

## Quick start

```bash
python3 -m venv .venv
.venv/bin/pip install requests websocket-client pyotp

cp .env.example .env        # fill MSTOCK_API_KEY (+ TOTP, or a same-day access token)

# prove it works without the market (no credentials needed)
.venv/bin/python verify.py            # 37 unit checks
.venv/bin/python stress_test.py       # 100 generated scenarios

# a live session — one command, dashboard included
.venv/bin/python cas_logger.py --rec-end 15:32:00
#   -> dashboard ready: http://127.0.0.1:8080/   (opens a browser unless --no-open)

# before 15:15 the baseline snapshot is taken normally; if you start later, it is
# taken immediately after a 6s warm-up instead of being skipped

# review a finished session later
.venv/bin/python cas_dashboard.py --log cas_logs/<date>/cas_<date>.jsonl --open

# after the close
.venv/bin/python cas_logger.py   --summarize cas_logs/<date>/cas_<date>.jsonl
.venv/bin/python cas_flow.py     --log       cas_logs/<date>/cas_<date>.jsonl
.venv/bin/python cas_analysis.py --log       cas_logs/<date>/cas_<date>.jsonl
.venv/bin/python cas_analysis.py --log       cas_logs/<date>/cas_<date>.jsonl --calibrate
```

## The files

| File | Role |
|---|---|
| `cas_logger.py` | the recorder: auth, universe, WebSocket, REST polling, snapshots, live + final readout |
| `cas_flow.py` | order-flow read of 15:20–15:27: Δbuy/Δsell, Δimbalance, depth lean, signed volume, who bought/sold |
| `cas_analysis.py` | scores price estimators (futures, mid, microprice, put-call parity) against the realised jump; `--calibrate` compares the estimate with a manually entered indicative close |
| `cas_dashboard.py` | single-file web dashboard that tails the log — live or replay; `cas_logger.py` starts it automatically (`--no-dashboard`, `--no-open`, `--dash-port`, `--dash-host`) |
| `verify.py` | unit checks: parsing, resolution, scaling, decisions, calibration |
| `stress_test.py` | 100 generated sessions: 8 market shapes × 20 data defects × 10 index sets |
| `stub_api.py` | fake mStock REST, so the full pipeline runs with no credentials |
| `make_*.py` | synthetic log builders used by the tests and the dashboard demo |
| `MSTOCK_API_REFERENCE.md` | packet layouts, rate limits, caveats, runbook, what testing changed |
| `docs/ORIGINAL_README.md` | the first-day notes this project grew out of |

## Indices covered

NIFTY · BANKNIFTY · FINNIFTY · MIDCPNIFTY (NSE/NFO) · SENSEX · BANKEX (BSE/BFO)

Strike ladders: 50 / 100 / 50 / 25 / 100 / 100 points. Gap floors, below which a card is marked SKIP:
35 / 120 / 60 / 35 / 100 / 100 points.

## How a decision is reached

1. **15:15:10** baseline — spot, futures, best bid/ask mid, microprice, parity forward
2. **15:20:00** recording starts; every packet and REST poll is logged
3. per index: `gap = futures move since baseline`, `target = frozen spot + gap`
4. `strike` = deepest strike still ITM at `target` (floor for CE, ceil for PE)
5. `edge = intrinsic at target − ask now`. **Negative edge means the move is already priced in**
6. verdict, in priority order: `STALE` → `SKIP` (gap under floor) → `CONFLICT` (flow against the gap)
   → `NO EDGE` → `STRONG` / `WATCH`
7. **15:27:15** final readout, written into the log

A manual indicative close can be typed into the logger at any time (`NIFTY 25065`); it overrides the
futures estimate for that index and **both** numbers are recorded, so `--calibrate` can measure the
estimate's bias afterwards.

## Honest limitations

- **No CAS field exists in the API.** There is no indicative equilibrium price and no imbalance.
  Every "projected close" here is estimated from futures, put-call parity and order flow.
- **The auction book may not be forwarded at all.** If `tbq`/`tsq` stay frozen, the run reports
  `auction book moved on 0/N` and the flow read degrades to futures/options pressure.
- **The base rate is not 50%.** NIFTY closed above its 3:15 level on 10 of 16 expiries (Aug–Nov 2026).
  A directional signal must beat that, and 16 samples is far too few to claim an edge.
- **One shot a week.** Calibration needs months, not days.
- **Edge, not direction, is the binding constraint.** In synthetic checks with honest pricing, four of
  six indices showed a negative edge — the market had already priced the flip.
- **No execution modelling.** The system measures a signal; it does not know whether a fill at that
  price was possible in the 45 seconds before the close.

## Safety

Read-only: no order placement, no position management, no credentials in the repo. `.env` is
gitignored; the logger reads the API key from the environment and gets an access token via TOTP or a
same-day token.
