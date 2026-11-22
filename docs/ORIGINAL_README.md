# CAS Logger (mStock Type A) - record only

Records what mStock's API exposes during the Closing Auction Session (3:15-3:35 PM).
No orders, no signals. Goal: find out if any auction data (indicative price, buy/sell qty,
depth) reaches you before the random close at 3:28-3:30 PM.

## Setup (once)
    pip install requests websocket-client pyotp
    cp .env.example .env      # fill credentials

## Run order
1. `python cas_logger.py --find "NIFTY 50"`  and `--find SENSEX`
   Check the index names exist. If not, make `overrides.json` and use `--overrides overrides.json`:
   `{"NIFTY": {"token": 123, "symbol": "Nifty 50"}}`
2. `python cas_logger.py --test 120`  (market open, any time)
   Checks login, scriptmaster, WebSocket parsing, price divisor. Then:
   `python cas_logger.py --summarize cas_logs/<date>/cas_<date>.jsonl`
3. Live readout: from 15:20 the console prints, per index, the futures move since the 3:15 baseline,
   the implied target, and the nearest strike that would be ITM at that target (price vs value at target).
   FINAL READOUT is at 15:27:15 (`--decide-at`). It is futures-implied, not a prediction: if the ask is
   already close to the value at target, the move is priced in. Place orders before 15:28:00.
4. Expiry day (e.g. Mon 23 Nov): start about 15:05 and leave it running:
   `python cas_logger.py --rec-end 15:32:00`
   (15:32 also captures the real 3:28-3:30 jump; the default stops at 15:27.)
5. Afterwards: `--summarize` on the log, and share the summary + first 50 lines of the
   `.ws_raw.jsonl` file if anything looks off.

## Output (cas_logs/DATE/)
- `cas_DATE.jsonl`: meta, events, snapshots (15:14:50, 15:15:10, 15:19:50, 15:31:00, 15:35:30),
  REST polls, parsed WS ticks
- `cas_DATE.ws_raw.jsonl`: raw WS frames (base64) for offline re-parsing
- `scriptmaster_DATE.csv`: cached instrument list

## Things that may need adjusting on first run
- Packet byte order / price scale are assumptions (big-endian, paise). The script checks the
  scale against REST automatically; if prices look absurd use `--little-endian`.
- WebSocket subscribe uses `instrument_token`. If the summary shows many "unmapped pkts",
  the feed uses exchange tokens instead; send me the meta record.
- TOTP login order (connect/login before verifytotp) follows the docs; if it fails, paste a
  fresh `MSTOCK_ACCESS_TOKEN` instead.
- SENSEX and BANKEX are recorded too (spot, BFO futures, BFO options, plus BSE-listed stocks from a
  recording list that is NOT an official constituent list). Their options are recorded every day by
  default; they actually expire on Thursdays (26 Nov: Sensex weekly + Bankex monthly). No weights are
  attached to them yet, so the weighted cross-check skips these two.
- BSE symbols look like `HDFCBANK-A`; if the notes say BSE stocks were not found, check with `--find`.

## Weights (approximate)
Index weights in `DEFAULT_WEIGHTS` come from the earlier research chat and are UNVERIFIED and partial
(coverage 0.4-0.6). They only feed an informational line in the readout (`wtd(approx)`), never the
strike choice. `wtd ltp` uses last price (frozen in CAS), `wtd mid` uses best bid/ask mid. The log keeps
normalised and raw (`*_raw_pts`) versions; the `--summarize` Q3 block compares them with the actual move.
Replace with official weights via `--weights weights.json` after Monday.
