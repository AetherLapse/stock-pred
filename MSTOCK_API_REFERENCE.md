# mStock Trading API — what the docs actually say (Type A, v1.0)

Source: `tradingapi.mstock.com/docs/v1/...`, read 2026-11-22. Everything below is quoted or
paraphrased from the official pages; where the logger's code makes an assumption the docs do not
state, it is marked **ASSUMPTION**.

---

## 1. Connection, auth, headers

| Item | Value |
|---|---|
| Interactive (REST) root | `https://api.mstock.trade` |
| Broadcasting (WebSocket) root | `wss://ws.mstock.trade` |
| API version header | `X-Mirae-Version: 1` |
| Auth header | `Authorization: token <api_key>:<access_token>` |
| Private-key header | `X-PrivateKey: <key>` (listed in Introduction) |
| Body encoding | **form-encoded** parameters, JSON responses |
| Responses | may be **gzipped**; standard HTTP status codes |
| CORS | **not enabled** — cannot be called from a browser |

**Validity**

- API key: 1 year / 1 month / 1 day depending on plan.
- **Access token: 12 hours or end of the same day, whichever comes first.** Renew daily.
- Login (`POST /session/token`) emails/SMSes an OTP; `POST /session/verifytotp` exchanges it, or a
  TOTP, for `access_token`.

**BOD / EOD**: 07:00–08:30 and 19:00–21:00 IST — "system performance may be affected".

## 2. Rate limits

| | Order APIs | Data APIs | Quote APIs | Non-trading |
|---|---|---|---|---|
| per second | 30 | **1** | **20** | — |
| per minute | 250 | 1000 | unlimited | unlimited |
| per hour | 1000 | 5000 | unlimited | unlimited |

The CAS logger polls `quote/ohlc` (Quote APIs) every 2 s with 1–2 request chunks — comfortably
inside 20/s. Historical candles are the "Data APIs" bucket at **1/s** if ever added.

---

## 3. WebSocket market data (Type A)

### Handshake

```
wss://ws.mstock.trade?API_KEY=<api_key>&ACCESS_TOKEN=<access_token>
→ client sends text frame:  LOGIN:<access_token>
```

- Connection is **rejected** if either query parameter is invalid or expired.
- **The session is terminated if `LOGIN:` is not sent promptly after connecting.**
- Requests are JSON: `{"a": "<action>", "v": [ ... ]}`.
- Actions: `subscribe`, `unsubscribe`, `mode`.
- Modes: `ltp`, `quote`, `full` (full = with market depth).

```json
{"a": "subscribe", "v": [55256, 55412]}
{"a": "mode", "v": ["full"]}
```

### Binary frame header

> "The first two bytes [short] represent the number of packets contained in the message. The next two
> bytes [short] represent the total size of the quote packet in bytes. The remaining bytes contain the
> quote packet data."

So the layout is **`[short count][short size][count × packet of `size` bytes]`** — a fixed-size
layout, not a per-packet length prefix. `[short]` = 2 bytes, `[int]` = 4 bytes.

### Quote packet — 64 bytes, +120 depth, +16 circuits

| Field | Type | Bytes |
|---|---|---|
| Token | int | 0–4 |
| LTP | int | 4–8 |
| Last traded qty | int | 8–12 |
| Average traded price | int | 12–16 |
| Volume traded today | int | 16–20 |
| Total buy qty | int | 20–24 |
| Total sell qty | int | 24–28 |
| Open / High / Low / Close | int | 28–32 / 32–36 / 36–40 / 40–44 |
| Last traded timestamp | int | 44–48 |
| Open interest / OI high / OI low | int | 48–52 / 52–56 / 56–60 |
| Exchange timestamp | int | 60–64 |
| **Market depth** | byte[] | **64–184** |
| Upper circuit / Lower circuit | int | 184–188 / 188–192 |
| Year high / Year low | int | 192–196 / 196–200 |

Total for a "full" packet: **200 bytes**; a quote packet without depth/circuits: **64 bytes**.

### Market depth — 120 bytes, 5 bids + 5 asks

Per level: **int qty (4) + int price (4) + short orders (2) + short padding (2) = 12 bytes.**

| Level | Bytes |
|---|---|
| Bid 1 | 64–76 |
| Bid 2 | 76–88 |
| Bid 3 | 88–100 |
| Bid 4 | 100–112 |
| Bid 5 | 112–124 |
| Ask 1 | 124–136 |
| Ask 2 | 136–148 |
| Ask 3 | 148–160 |
| Ask 4 | 160–172 |
| Ask 5 | 172–184 |

**The 2-byte padding is part of the wire format** — the split into 12-byte levels is what the
parser relies on.

### Index packet — 48 bytes (non-tradable index tokens)

| Field | Bytes | Field | Bytes |
|---|---|---|---|
| Token | 0–4 | Exchange timestamp | 28–32 |
| LTP | 4–8 | Upper circuit | 32–36 |
| High | 8–12 | Lower circuit | 36–40 |
| Low | 12–16 | Year high | 40–44 |
| Open | 16–20 | Year low | 44–48 |
| Close | 20–24 | | |
| Price change | 24–28 | | |

> **Doc inconsistency:** the page's own table-of-contents entry says "Index Packet Structure:
> (32 Bytes)" while the section body lists fields through byte 48 and is headed "(48 Bytes)".
> Trust the field table — 48 bytes. Anything reading 32 would drop circuits and year high/low.

### LTP-only packet

Not laid out in the docs, but `mode: ltp` implies token + price. The logger treats **8 bytes** as
`{token, ltp}`. **ASSUMPTION** — verify against `quote` traffic on the first run.

---

## 4. REST: quotes and instruments

All endpoints take `i=<EXCHANGE:SYMBOL>` query params (`i` repeated for multiple instruments).

| Method | Path | Notes |
|---|---|---|
| GET | `/openapi/typea/instruments/quote/ohlc` | `last_price` + `ohlc{open,high,low,close}` |
| GET | `/openapi/typea/instruments/quote/ltp` | last price only |
| GET | `/openapi/typea/instruments/scriptmaster` | full instrument master (CSV) |

Symbol shapes in the docs: `NSE:ACC-EQ`, `BSE:ACC-A`, `NFO:NIFTY2672124150CE`,
`BFO:SENSEX2671677000CE`. **NSE cash carries an `-EQ` suffix and BSE carries a `-<series>`
suffix** — resolve keys from the scriptmaster, never hand-build them.

OHLC response:

```json
{"status":"success","data":{"NSE:ACC-EQ":{"instrument_token":22,"last_price":1373.3,
  "ohlc":{"open":1390,"high":1393.2,"low":1370.2,"close":1383.2}}}}
```

### Newer endpoints worth knowing (Type A)

| Method | Path |
|---|---|
| GET | `/openapi/typea/getoptionchainmaster/{exch}` — expiries + FUTIDX/OPTIDX rows as CSV strings |
| GET | `/openapi/typea/getoptionchainmaster/{exch}/{expiry}/{token}` — chain for one underlying |
| GET | `/openapi/typea/historical-candle` · `/intraday-chart-data` · `/gainers-losers` |

The option-chain master returns `dctExp` (expiry epoch map) plus rows like
`"BANKNIFTY,26009,2,3,4"`, which is a cleaner route to strikes than scanning the scriptmaster.

## 5. Error types

| Error | HTTP | Meaning |
|---|---|---|
| `APIKeyException` | 403 | API key invalid/expired |
| `TokenException` | 401 | access token missing/invalid/expired |
| `InputException` | 400 | bad parameters |
| URL Not Found | 404 | wrong path |
| `MiraeException` | — | upstream broker error |
| `GeneralException` | — | internal |

The logger's guard treats a non-`success` status or a 401/403 as fatal at startup and warns on
anything else.

---

## 6. Type A vs Type B

Both stream **binary** frames; the control messages differ.

| | Type A | Type B |
|---|---|---|
| Request shape | `{"a":"subscribe","v":[tokens]}` | `{"correlationID":…,"action":1,"params":{"mode":3,"tokenList":[{"exchangeType":1,"tokens":["22"]}]}}` |
| Modes | `ltp` / `quote` / `full` | `1` LTP / `2` quote / `3` snap quote |
| Tokens | instrument tokens | token strings **grouped by exchange type** (1 NSECM, 2 NSEFO, 3 BSECM, 4 BSEFO, 13 NSECD) |
| Binary payload | documented as above | same idea, own header |

---

## 7. What the CAS logger assumes, and its status against the docs

| Assumption in code | Docs say | Status |
|---|---|---|
| Frame layout `[count][size][packets]` | exactly this | **confirmed** — now tried first (`--layout fixed`) |
| Quote packet 64 / 184 / 200 bytes | 64 + 120 depth + 16 circuits | **confirmed** |
| Depth level = 12 bytes incl. 2-byte pad | same | **confirmed** |
| Index packet 48 bytes, offsets 8/12/16/20/24/28/32/36/40/44 | same | **confirmed** (TOC's "32 bytes" is wrong) |
| LTP packet = 8 bytes | not documented | **assumption**, unverified |
| Prices are integers in paise (÷100) | `[int]` but no stated scale | **assumption**, auto-checked at runtime against REST |
| Big-endian | not stated | **assumption**, raw frames retained |
| `LOGIN:<access_token>` then subscribe | exactly this | **confirmed** |
| Subscribe 100 tokens per message | not documented | **assumption**, no observed limit |
| Token valid for the run | 12 h / same day | **confirmed** — needs a same-day token |

## 8. Caveats that can bite on the first live run

1. **Generate the access token the same day.** A token made Sunday is dead by Monday 15:15.
2. **No `LOGIN:` = dropped session.** The logger sends it on open; if the URL token is stale the
   server closes instead of erroring, which looks like a silent feed.
3. **REST is form-encoded and not CORS-enabled** — server-side only, never from a browser.
4. **Quote APIs 20/s.** Polling every 2 s is fine; adding option-chain or historical calls in the
   same loop can cross it (historical is 1/s on its own).
5. **BOD/EOD (07:00–08:30, 19:00–21:00 IST)** are degraded windows — irrelevant to a 15:15 run but
   relevant to any post-close re-run.
6. **The index packet has no depth and no total buy/sell qty** — auction-style quantities can only
   appear on the *stock* packets, which is exactly why the logger records stocks.
7. **Nothing in the docs mentions an indicative equilibrium price, imbalance, or any CAS-specific
   field.** If mStock forwards auction data at all, it will surface as movement in
   `tbq`/`tsq`/depth on stock packets while LTP stays frozen — the Q1 block of `--summarize`.
8. **Type B needs tokens grouped by exchange type** — a Type A token list sent to Type B (or vice
   versa) subscribes nothing.

---

## 9. Calculating the close ourselves

Since the feed carries no indicative equilibrium price, the closing level has to be *estimated*.
`cas_analysis.py` computes four estimators from the same log and scores each against the realised
jump, so the data decides which one is worth trusting instead of us assuming.

| Estimator | Formula | Why it might work | Where it fails |
|---|---|---|---|
| `fut_ltp` | `fut_now − fut_15:15` added to the frozen spot | index futures keep trading through CAS; basis is near-constant over 12 minutes | thin late liquidity; a single stale print moves it |
| `fut_mid` | same, using best bid/ask mid | ignores a lone trade away from the touch | wide/spoofed top of book |
| `fut_micro` | size-weighted level-1 fair value | standard short-horizon fair value; reacts to which side is heavier | only as good as level-1 size, which futures hide late |
| `pcp_fwd` | median over strikes of `F = K + C − P`, differenced against its 3:15 value | options keep trading too, so parity gives an *independent* forward — often the only quote that reacts when the future goes quiet | illiquid strikes; needs both legs quoting; noise if the chain thins |

### What the scoring proves

`make_synthetic.py` builds logs with a known closing jump in four shapes: `clean`, `noisy` (a tick of
future noise + one stale strike), `laggy` (the future only converges at the end) and `pcp_edge`
(options price the real move while the future sits still). Measured readout error and window MAE:

- `clean` — every estimator 0.00 error at the readout.
- `noisy` — 1.81 at the readout; the median-across-strikes construction absorbs the stale strike.
- `laggy` — all four identical (options were built off the future in that scenario), MAE 56.
- `pcp_edge` — **`pcp_fwd` MAE 13.00 vs `fut_ltp` 62.83**: parity catches a move the future is
  still ignoring, which is exactly the failure mode a 15:27 deadline punishes.

So the ranking is measurable, not aesthetic: run `cas_analysis.py --logs cas_logs/*/*.jsonl` after a
few expiries and the pooled table says which estimator to place orders from, per index.

### Reading the live line

From the 3:15 baseline the console prints, per index:

```
NIFTY  base 25000.0 | fut   +62.5 -> target 25062.5 | CE 25000 ltp 71.2 ask 71.9 value@target 62.5
       | calc mid +61.8 micro +63.1 pcp +70.4 | wtd(approx) ltp +28.3 mid +30.0 cov 0.52
```

`fut` drives the strike choice; `calc` is the second opinion; `wtd(approx)` is the weighted
constituent move (informational only — weights are approximate and coverage is ~0.5).

### Honest limits

- One expiry day is one sample. The estimator that wins on 23 Nov may lose on 26 Nov; the pooled
  table needs several sessions before it means anything.
- Parity assumes a common expiry and negligible carry over the remaining hours — fine intraday on an
  expiry day, wrong for a monthly contract with a week left.
- If the future and the chain both go quiet, nothing here invents information: the readout will show
  a smaller move than reality and the log will say so (`pcp_strikes` = 0, no `calc` line).

---

## 10. Order flow in 15:20–15:27 (the actual goal)

The question is "who is buying, who is selling, and will the candle close up or down" — so the
signal has to come from **flow**, not from a price estimate. `cas_flow.py` reads the window
(`15:20:00–15:27:30`) out of the log and produces, per instrument and per index:

| Feature | Source field | Meaning |
|---|---|---|
| `d_buy_qty` / `d_sell_qty` | `tbq` / `tsq` | cumulative auction/queue build on each side during the window |
| `d_imbalance` | derived | `(Δbuy − Δsell) / (Δbuy + Δsell)` — scale-free, −1..1 |
| `d_depth_imb` | 5-level `bids`/`asks` | which side the resting book leaned towards, and by how much it changed |
| `signed_net` | `vol` + `ltp` (tick rule) | trade prints: up-tick volume is buying, down-tick selling — works on futures/options, which still trade |
| `option_vote` | option signed volume | calls bought minus puts bought, as a directional vote on the index |
| `buy_qty_changed`, `ltp_moved` | — | **the diagnostic**: did the book move while LTP was frozen? |

Index rollup: weighted mean of `d_imbalance` over the constituents present, weighted by the
approximate index weights, giving one score in −1..1:

- `conviction high` when |score| ≥ 0.15, `low` ≥ 0.03, otherwise **abstain** — a balanced book is
  reported as balanced instead of guessing a direction.
- The per-name table answers the literal question: top 5 net-bought and top 5 net-sold.

### What the synthetic tests prove

| Scenario | Flow score | Realised | Verdict |
|---|---|---|---|
| `flow_up` (heavyweights bid, TCS/ITC dumped) | +0.23 | +78 | HIT |
| `flow_down` (mirror image) | −0.22 | −78 | HIT |
| `flow_noise` (random) | −0.01 | +12 | abstain |

Pooled hit rate 2/2 on the stories, and the noisy day correctly refuses to call it.

### The one unknown that decides this

NSE does not disseminate the CAS order book to brokers; the exchange shows indicative price/qty on
its own terminals. If mStock forwards `tbq`/`tsq` during CAS they *are* the auction book and this
module is exactly the answer. If they are frozen along with LTP, the run will say so plainly —
`auction book moved on 0/N stocks` — and the fallbacks (futures signed volume, option vote,
parity) carry the read instead. Monday's log settles it; no amount of code can.

### Running it during the window

The logger flushes its JSONL about once a second, so the flow read can be taken **live**, not just
after the close:

```bash
# terminal 1 (from ~15:05)
python cas_logger.py --rec-end 15:32:00
# terminal 2, from 15:20 — re-reads the growing log
watch -n 15 "python cas_flow.py --log cas_logs/$(date +%F)/cas_$(date +%F).jsonl"
```

### BSE: SENSEX and BANKEX

Both BSE indices are first-class, not an afterthought:

| | SENSEX | BANKEX |
|---|---|---|
| spot exchange / alias | `BSE`, `SENSEX` | `BSE`, `BANKEX` |
| derivatives | `BFO` futures + options | `BFO` futures + options |
| strike ladder | 100 pts | 100 pts |
| gap floor to act | 100 pts | 100 pts |
| expiry | **weekly, Thursday** | **monthly, last Thursday** |
| weights | 20 constituents ≈ 0.75 coverage | 12 bank names |

Two details that used to break BSE silently:

1. **Cash symbols carry a series suffix** — `HDFCBANK-A` on BSE, `HDFCBANK` on NSE. Both are
   normalised to the same weight key now, otherwise SENSEX/BANKEX flow scores came out empty.
2. **BSE stock recording list** is 20 SENSEX names + 12 BANKEX names, subscribed automatically when
   either index is selected; without them the weighted line has nothing to weight.

Consequence for the 23 Nov run: NSE indices expire that Monday, but **BSE does not** — the next BSE
expiry is Thursday 26 Nov (SENSEX weekly + BANKEX monthly). SENSEX options recorded on Monday are
therefore for the 26 Nov expiry, which is exactly what you want for a forward-looking read.

Coverage note: the score is only as broad as the subscribed stock list. The default 19 NSE names
cover ~0.35 of NIFTY's weight — enough to read direction, not to size it. Widen with
`--stocks` (40–50 heavies) once the mechanism is confirmed.

---

## 11. The end-to-end run (what happens, minute by minute)

| Time | What runs | What you get |
|---|---|---|
| ~15:05 | `python cas_logger.py --rec-end 15:32:00` | login, scriptmaster, universe (index spots, futures, ATM±6 chain, stocks) |
| 15:10 | WebSocket connects, `LOGIN:`, subscribe `full` | live feed; status line reports frames/pkts every 30s |
| 15:14:50 | snapshot | REST prices for everything, divisor check |
| **15:15:10** | **snapshot = BASELINE** | frozen spot + futures + mid + microprice + parity forward |
| 15:19:50 | snapshot | last pre-auction look |
| **15:20:00** | `RECORDING STARTED` | every WS packet + REST poll logged; alarms if the feed is silent |
| 15:20–15:27 | live readout every 3 s | per index: futures move, target, chosen strike, ask, value@target, parity/mid/micro calc, weighted constituent move |
| **15:27:15** | `FINAL READOUT` | the decision table, written to the log as a `readout` record |
| 15:27:30 | `RECORDING STOPPED` | — |
| 15:31:00 | snapshot | the realised close, for scoring |
| 15:35:30 | snapshot | end of CAS |

### The decision table (what the final readout now prints)

```
NIFTY      base   25000.0 | fut   +62.5 -> target   25062.5 | CE 25000 ltp 71.2 ask 71.9 value@target 62.5
             -> CE 25050 ask 21.4 value@target 12.50 edge -8.90
```

- `side` is CE for a positive gap, PE for a negative one.
- `strike` = `floor(target/step)*step` for CE, `ceil(target/step)*step` for PE — deepest strike still
  ITM at the projected level.
- `edge` = intrinsic at the projected target − the ask you pay now. **Negative edge means the move is
  already priced**: the option costs more than it will be worth. That is the real filter, not the gap.
- Floors below which the row says SKIP: NIFTY 35 · BANKNIFTY 120 · FINNIFTY 60 · MIDCPNIFTY 35 ·
  SENSEX/BANKEX 100 points. Half a strike is too generous — the premium eats a gap that size.

### Where the "projected gap" comes from

The screenshot's method assumes NSE's **indicative close** is visible at ~15:26:30 on the broker
terminal. mStock's API does not expose it (§3, §7) — no CAS field exists in the feed. So the gap here
is *computed*:

1. `fut_ltp` — futures move added to the 3:15 spot (primary; drives the strike choice)
2. `pcp_fwd` — put-call parity across strikes (independent second opinion)
3. `fut_micro` / `fut_mid` — fair value of the top of book
4. flow score — which side is lifting (§10), used as confirmation, and to decide whether to trust a
   gap that the futures and the chain disagree on

When parity and futures disagree by more than a strike, the honest read is "sit this one out" — the
market itself has not decided.

### Expiry calendar that matters for this script

- NSE expiries are **Tuesday**; BSE **Thursday** (since 1 Sept 2025).
- One weekly per exchange: **NIFTY weekly (Tue)**, **SENSEX weekly (Thu)**. BANKNIFTY, FINNIFTY,
  MIDCPNIFTY are **monthly only**, last Tuesday.
- If the expiry day is a holiday the contract moves to the previous trading day — which is why
  **23 Nov 2026 (Monday)** is an expiry: 24 Nov is a holiday, and that pulls both the NIFTY weekly and
  the **November monthly** (all NSE indices) onto the same Monday.
- Consequence: on 23 Nov all four NSE indices expire together. Expect the CAS book to be lopsided and
  the option chain to be very liquid — the best day of the month for this test, and the worst day to
  generalise from.

---

## 12. The dashboard

`cas_dashboard.py` is a single-file, stdlib-only web view that **tails the log** the logger is
writing. It works during the run and as a replay of a finished day; it never writes to the log.

```bash
# live, in a second terminal (start the logger first)
python cas_dashboard.py --log cas_logs/$(date +%F)/cas_$(date +%F).jsonl --port 8080 --open

# on a phone on the same network
python cas_dashboard.py --log <log> --host 0.0.0.0 --port 8080

# replay yesterday
python cas_dashboard.py --log cas_logs/2026-11-23/cas_2026-11-23.jsonl
```

What each card shows, per index:

| Row | Meaning |
|---|---|
| `gap vs 3:15` | futures move since the frozen baseline (or the manual indicative, when typed into the logger) |
| `spot → target` | the frozen spot and where that gap projects the close |
| `trade` | side and the deepest strike still ITM at the target |
| `ask now` / `edge at target` | what the option costs and what it would be worth — blank until option ticks arrive |
| `flow` | weighted Δ-imbalance across the constituents, with conviction (high ≥0.15, low ≥0.03) |
| `bought` / `sold` | the names actually being accumulated vs dumped in the window |
| sparkline | futures LTP over the session |

Header: `live · Ns` when ticks are arriving, `stale · Ns` when the feed has gone quiet (or when
replaying a finished file), plus record counts. Footer: realised close once the 15:31 snapshot exists,
and any alarm events the logger raised (`feed_silent`, `tokens_unmapped`, `error`).

The card turns red-bordered and adds `· SKIP` when the gap is under that index's floor — the same rule
the logger uses, so the dashboard and the readout can never disagree.

---

## 13. Stress suite (100 scenarios)

`stress_test.py` generates 100 different sessions and pushes each through every reader — dashboard
state, flow analyser, estimator scorer, decision maths — asserting invariants against the *generated*
truth, not just internal consistency.

    .venv/bin/python stress_test.py            # all 100, ~5 s
    .venv/bin/python stress_test.py --only 42  # one scenario, verbose

Each scenario varies three axes independently:

- **market shape** (8): up · down · flat · small up · small down · huge up · huge down · exactly at the
  gap floor · just below it
- **data defect** (20): clean · no options · one leg missing · stale options · no depth · zero-qty
  depth · frozen auction book · book moving · no stocks · sparse ticks · one tick per token · garbage
  frames · duplicate ticks · out-of-order records · missing post-close snapshot · truncated last line ·
  unknown indices · prices ×100 · absurd magnitudes · negative quantities
- **index set** (10): all six · single index · BSE-only · NSE-only · four-index expiry mix · …

Invariants asserted per scenario:

| Check | What it catches |
|---|---|
| state builds and is JSON-serialisable | a card that would 500 the API |
| side matches gap sign; strike on the ladder; ITM at target | decision maths |
| `tradeable == (|gap| >= that index's floor)` | the skip rule drifting from the logger |
| recovered gap / target / realised == generated truth (±2) | a reader silently using the wrong baseline |
| flow score within [−1, 1] | the raw-quantity fallback bug (found by this suite) |
| bought nets > 0, sold nets < 0, disjoint | the sold-list filter bug |
| edge == intrinsic at target − ask | premium maths |
| estimator MAE/bias finite | scorer blowing up on odd logs |
| no exceptions anywhere, including malformed JSON lines | robustness of the readers |

Artefacts: `stress_logs/scenarios.jsonl` (one row per scenario: params, checks, ms),
`stress_logs/failures.jsonl` (only failures, with traceback — absent when everything passes),
`stress_logs/summary.txt`, and `stress_logs/logs/scenario_NNN.jsonl` (the full session, ~92k records
across the suite).

Passing 100 generated sessions does **not** prove the pipeline is bug-free — it bounds the class of
bugs that can reach the delivery window. The scenarios that matter most are the ones with a defect
*and* a near-threshold gap, because that is where a silent zero looks exactly like a quiet auction.

---

## 14. What the testing loop changed (verdicts, calibration, priors)

Three things came out of the stress suite and the meeting data, not from guesswork:

**1. A verdict per card.** `tradeable` alone was not enough — a big gap with a negative edge is a
losing trade. Each card now carries one word, evaluated in this order:

| Verdict | Condition | Meaning |
|---|---|---|
| `STALE` | newest tick for that index > 60 s behind the log's own clock | the line is dead; ignore everything below |
| `SKIP` | `\|gap\| < floor` for that index | the move can't pay for the premium |
| `CONFLICT` | high-conviction flow points against the gap | price and order flow disagree — stand aside |
| `NO EDGE` | `edge ≤ 0` | the premium already contains the projected move |
| `STRONG` | tradeable, edge > 0, high conviction, flow agreeing | the only state worth acting on |
| `WATCH` | anything else | tradeable but unconvincing |

Age is measured against the **newest tick in the log**, not wall clock, so replaying last week's
session doesn't paint every card stale; live, the newest tick is ~now and they coincide.

**2. Calibration against the terminal.** The logger writes both `target` (what you typed) and
`computed_target` (futures-implied) on every readout, so:

    python cas_analysis.py --log <log> --calibrate

reports bias, median and MAE of our estimate against the indicative close you read. A persistent
positive bias means the estimate runs high and `-median` points is the correction to apply. This is how
the manual step eventually disappears: measure the offset, then subtract it.

**3. The base rate is stated, not implied.** `cas_flow.py` now prints the prior from the Aug–Nov sheet
(NIFTY closed above 3:15 on 10/16 expiries = 62.5%) next to any hit rate, because a 55% signal that
loses to "always CE" is worse than no signal.

---

## 15. Starting it (one command)

```bash
python cas_logger.py --rec-end 15:32:00
# dashboard ready: http://127.0.0.1:8080/
```

The logger starts the dashboard itself, in a thread, tailing the log it is writing. Flags:

| flag | use |
|---|---|
| `--no-dashboard` | logging only |
| `--no-open` | serve, but don't launch a browser |
| `--dash-port 8090` | first port to try (walks upward if busy) |
| `--dash-host 0.0.0.0` | reach it from a phone on the same network |

**Starting at 15:15 is supported but not ideal.** The baseline snapshot is due at 15:15:10; if the
process starts after that, the scheduler no longer skips it — it takes the baseline immediately after a
6-second warm-up (`late start: … taking it now`) so the session still has a reference point. What you
lose is the *pre-freeze* reading: a 15:15 start may capture a spot that has already begun to move, and
the WebSocket needs a few seconds to connect and receive its first ticks. 15:05–15:10 gives a clean
baseline and a warm feed; 15:15 works; after 15:27:30 there is nothing to record.
