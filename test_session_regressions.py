"""Offline regressions for the failed closing-auction recording."""
import json
import contextlib
import io
import os
from pathlib import Path
import subprocess
import struct
import sys
import tempfile
import threading
import unittest
import requests
from http.server import ThreadingHTTPServer
from unittest.mock import patch

import cas_dashboard as D
import cas_logger as C


class QuoteRegression(unittest.TestCase):
    def test_cash_quote_keys_include_exchange_series_without_changing_ws_token(self):
        rows = [
            ('NSE', 'HDFCBANK', 'EQ', 'EQ', 'NSE:HDFCBANK-EQ'),
            ('NSE', 'HDFCBANK-EQ', 'EQ', 'EQ', 'NSE:HDFCBANK-EQ'),
            ('NSE', 'BAJAJ-AUTO', 'EQ', 'EQ', 'NSE:BAJAJ-AUTO-EQ'),
            ('BSE', 'HDFCBANK', 'Equity', 'A', 'BSE:HDFCBANK-A'),
            ('BSE', 'HDFCBANK-A', 'EQ', 'BSE', 'BSE:HDFCBANK-A'),
            ('BSE', 'BAJAJ-AUTO', 'Equity', 'A', 'BSE:BAJAJ-AUTO-A'),
            ('BSE', 'SMALLCAP', 'Equity', 'XT', 'BSE:SMALLCAP-XT')]
        for ex, symbol, kind, segment, expected in rows:
            with self.subTest(symbol=symbol, exchange=ex):
                item = C.make_item({'instrument_token': '1333', 'exchange': ex,
                                    'tradingsymbol': symbol, 'instrument_type': kind,
                                    'segment': segment}, 'stock')
                self.assertEqual(item['key'], expected)
                self.assertEqual(item['symbol'], symbol)
                self.assertEqual(item['token'], 1333)

    def test_rejected_symbols_are_errors_even_in_http_success_response(self):
        payload = {'status': 'success', 'data': {
            'NSE:NIFTY 50': {'last_price': 25349.65},
            'NSE:NIFTY MIDCAP SELECT': {'symbol': 'NSE:NIFTY MIDCAP SELECT',
                                       'message': 'Invalid symbol'},
            'NSE:EMPTY': {'last_price': 0}}}
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        with patch.object(requests.Session, 'get', return_value=response):
            data, errors = C.fetch_ohlc(requests.Session(), {},
                ['NSE:NIFTY 50', 'NSE:NIFTY MIDCAP SELECT', 'NSE:EMPTY', 'NSE:OMITTED'])
        self.assertEqual(data, payload['data'], 'retain rejected rows as raw evidence')
        self.assertTrue(any('MIDCAP SELECT' in error for error in errors))
        self.assertTrue(any('OMITTED' in error for error in errors))
        self.assertTrue(any('EMPTY' in error for error in errors))


class DashboardRegression(unittest.TestCase):
    def test_ticks_without_baseline_prices_explain_missing_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            live = D.Live(Path(tmp) / 'session.jsonl')
            live._apply({'type': 'meta', 'args': {'indices': 'NIFTY'}})
            live._apply({'type': 'snapshot', 'label': '15:15:10', 'rest': {}, 'map': {}})
            live._apply({'type': 'ws_tick', 'tok': 8, 'role': 'stock',
                         'sym': 'NSE:RELIANCE', 'ltp': 2900,
                         't': '2026-11-23T15:21:00+05:30'})
            state = live.state()
            self.assertTrue(state.get('note'), 'dashboard silently discarded recorded data')
            self.assertTrue(any('NIFTY' in reason and 'spot' in reason
                                for reason in state.get('why', [])))


class SubscriptionRegression(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = C.JsonlWriter(Path(self.tmp.name) / 'log.jsonl')
        self.raw = C.JsonlWriter(Path(self.tmp.name) / 'raw.jsonl')
        self.addCleanup(self.out.close)
        self.addCleanup(self.raw.close)
        self.feed = C.WSFeed('fake', 'fake', C.Universe(), self.out, self.raw, '>', 100, True)
        self.messages = []
        self.feed._send = self.messages.append
        self.feed.connected.set()

    def deliver(self, tokens):
        packets = b''.join(struct.pack('>II', tok, 100) for tok in tokens)
        self.feed._handle_message(None, struct.pack('>HH', len(tokens), 8) + packets)

    def test_adding_options_keeps_existing_subscription_in_request(self):
        self.feed.subscribed = {1, 2}
        with patch.object(C.time, 'sleep'):
            self.feed.subscribe([3])
        requests = [json.loads(m)['v'] for m in self.messages if json.loads(m)['a'] == 'subscribe']
        self.assertEqual(requests, [[1, 2, 3]])

    def test_reconnect_does_not_treat_previous_socket_ticks_as_coverage(self):
        self.feed.subscribed = set(range(1, 109))
        self.feed.latest = {i: {'ltp': 100} for i in self.feed.subscribed}
        self.feed.latest_t = {i: '2026-11-23T15:14:00+05:30' for i in self.feed.subscribed}
        self.feed.resends = 4
        with patch.object(C.time, 'sleep'):
            self.feed._on_open(None)
        self.messages.clear()
        self.feed.subscribed_at = 0
        with patch.object(C.time, 'sleep'):
            self.feed.ensure_subscribed(quiet_s=0)
        self.assertTrue(self.messages, 'old coverage/retry budget suppressed recovery on new connection')

    def test_single_token_subscription_is_healthy_after_one_tick(self):
        self.feed.subscribed = {1}
        self.deliver([1])
        with patch.object(C.time, 'sleep'):
            self.feed.ensure_subscribed(quiet_s=0)
        self.assertEqual(self.messages, [])

    def test_one_time_ticks_do_not_hide_later_loss_of_100_tokens(self):
        active = set()
        def broker(payload):
            message = json.loads(payload)
            if message['a'] == 'subscribe':
                active.clear()
                active.update(message['v'])
                # Deliver the first 100 before the final eight replace them.
                self.deliver(sorted(active))
        self.feed._send = broker
        with patch.object(C.time, 'monotonic', return_value=100), patch.object(C.time, 'sleep'):
            self.feed.subscribe(list(range(1, 109)))
            self.assertEqual(len(self.feed.latest), 108)
            self.assertEqual(len(active), 8)
        for clock in (200, 300):
            with patch.object(C.time, 'monotonic', return_value=clock), patch.object(C.time, 'sleep'):
                self.deliver(sorted(active))
                self.feed.ensure_subscribed(quiet_s=0)
        self.assertEqual(active, set(range(1, 109)), 'transient earlier chunks hid later loss')

    def test_last_chunk_wins_server_recovers_all_108_tokens(self):
        # Model the observed broker behavior at the transport boundary: each
        # subscribe replaces the previous list and delivers only those tokens.
        def broker(payload):
            message = json.loads(payload)
            if message['a'] == 'subscribe':
                active.clear()
                active.update(message['v'])

        active = set()
        self.feed._send = broker
        with patch.object(C.time, 'sleep'):
            self.feed.subscribe(list(range(1, 109)))
            self.deliver(sorted(active))
            self.assertEqual(len(self.feed.latest), 8)
            self.feed.ensure_subscribed(quiet_s=0)
            self.deliver(sorted(active))
            self.feed.ensure_subscribed(quiet_s=0)
            self.deliver(sorted(active))
        self.assertEqual(active, set(range(1, 109)))
        self.assertEqual(set(self.feed.latest), set(range(1, 109)))

    def test_option_refresh_does_not_hide_lost_subscription_with_cached_ticks(self):
        active = set()
        def broker(payload):
            message = json.loads(payload)
            if message['a'] == 'subscribe':
                active.clear()
                active.update(message['v'])
        self.feed._send = broker
        with patch.object(C.time, 'sleep'):
            self.feed.subscribe(list(range(1, 101)))
            self.deliver(sorted(active))
            self.feed.subscribe(list(range(101, 109)))
            self.deliver(sorted(active))
            self.assertEqual(len(self.feed.latest), 108)
            self.feed.ensure_subscribed(quiet_s=0)
            self.deliver(sorted(active))
        self.assertEqual(active, set(range(1, 109)), 'historical cache hid missing active tokens')

    def test_persistent_starvation_stops_retrying_and_backs_off(self):
        self.feed.subscribed = set(range(1, 109))
        self.feed.latest = {101: {'ltp': 100}}
        with patch.object(C.time, 'sleep'):
            for _ in range(6):
                self.feed.ensure_subscribed(quiet_s=0)
        subscriptions = [json.loads(m) for m in self.messages if json.loads(m)['a'] == 'subscribe']
        self.assertLessEqual(len(subscriptions), 6)
        self.out.flush()
        events = [json.loads(line) for line in Path(self.tmp.name, 'log.jsonl').read_text().splitlines()]
        self.assertTrue(any(e.get('name') == 'ws_subscription_starved' for e in events))


class DiagnosticRegression(unittest.TestCase):
    def test_diagnose_counts_real_tick_token_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'session.jsonl'
            path.write_text('\n'.join(json.dumps(r) for r in [
                {'type': 'ws_tick', 'tok': 1}, {'type': 'ws_tick', 'tok': 2},
                {'type': 'ws_tick', 'tok': 1}]) + '\n')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                C.diagnose(path)
            self.assertIn('distinct tokens with ticks: 2', output.getvalue())

    def test_historical_log_without_subscription_map_reports_unknown_missing_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'session.jsonl'
            path.write_text(json.dumps({'type': 'meta', 'universe': {'stock': 108}}) + '\n' +
                            json.dumps({'type': 'ws_tick', 'tok': 1, 'sym': 'NSE:RELIANCE',
                                        'role': 'stock'}) + '\n')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                C.diagnose(path)
            self.assertIn('NSE:RELIANCE', output.getvalue())
            self.assertIn('subscribed but never ticked: unknown', output.getvalue())


class SnapshotRegression(unittest.TestCase):
    def test_missing_master_indices_reach_snapshot_and_dashboard(self):
        import stub_api
        server = ThreadingHTTPServer(('127.0.0.1', 0), stub_api.H)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as tmp:
            env = {**os.environ, 'MSTOCK_API_KEY': 'fake', 'MSTOCK_ACCESS_TOKEN': 'fake',
                   'MSTOCK_BASE_URL': f'http://127.0.0.1:{server.server_port}/openapi/typea',
                   'MSTOCK_WS_URL': 'ws://127.0.0.1:9'}
            run = subprocess.run([sys.executable, 'cas_logger.py', '--test', '1',
                                  '--env', str(Path(tmp) / 'no.env'), '--out', tmp,
                                  '--no-dashboard', '--no-live'],
                                 env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            log = next(p for p in Path(tmp).glob('*/cas_*.jsonl') if 'ws_raw' not in p.name)
            records = [json.loads(line) for line in log.read_text().splitlines()]
            snap = next(r for r in records if r.get('label') == 'test_start')
            live = D.Live(log)
            for idx in ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX'):
                self.assertIsNotNone(live._index_spot(snap, idx), idx + ' missing from saved map')
            meta = next(r for r in records if r['type'] == 'meta')
            self.assertEqual(meta['universe']['index'], 6)
            for key in ('NSE:HDFCBANK-EQ', 'NSE:RELIANCE-EQ', 'BSE:HDFCBANK-A'):
                self.assertGreater(snap['rest'].get(key, {}).get('last_price', 0), 0, key)


if __name__ == '__main__':
    unittest.main()
