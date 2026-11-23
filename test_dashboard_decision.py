"""Decision integrity: no future leakage, missing quantities, or false freshness."""
import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import cas_dashboard as D
import cas_flow as F


def tick(tok, time, **fields):
    return dict(type='ws_tick', tok=tok, t='2026-11-23T' + time + '+05:30',
                role='stock', sym='NSE:HDFCBANK-EQ', **fields)


class DecisionTests(unittest.TestCase):
    def test_window_is_frozen_and_missing_quantities_are_not_zero(self):
        live = D.Live(Path('/tmp/nonexistent-decision-log'))
        live._apply(tick(1, '15:20:00', tbq=100, tsq=100))
        live._apply(tick(1, '15:26:59', tbq=200, tsq=100))
        live._apply(tick(1, '15:28:00', tbq=0, tsq=1000))
        self.assertGreater(live.state()['cards']['NIFTY']['flow'], 0)
        other = D.Live(Path('/tmp/nonexistent-decision-log'))
        other._apply(tick(1, '15:20:00', ltp=100))
        other._apply(tick(1, '15:26:59', tbq=200, tsq=100))
        self.assertIsNone(other.state()['cards']['NIFTY']['flow'])

    def test_cash_filter_keeps_cash_for_selected_index(self):
        rows = [tick(1, '15:20:00', tbq=100, tsq=100),
                tick(1, '15:26:59', tbq=200, tsq=100),
                dict(tick(2, '15:20:00', ltp=100), role='future', idx='NIFTY'),
                dict(tick(2, '15:26:59', ltp=101), role='future', idx='NIFTY')]
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'session.jsonl'
            p.write_text('\n'.join(json.dumps(r) for r in rows))
            result = F.analyse(p, 'NIFTY')['rollup']['NIFTY']
            self.assertEqual(result['score_source'], 'stocks')
            self.assertGreater(result['flow_score'], 0)

    def test_nse_index_does_not_use_bse_duplicate(self):
        live = D.Live(Path('/tmp/nonexistent-decision-log'))
        for t, b in [('15:20:00', 100), ('15:26:59', 200)]:
            live._apply(tick(1, t, tbq=b, tsq=100))
        for t, s in [('15:20:00', 100), ('15:26:59', 10000)]:
            live._apply(dict(tick(2, t, tbq=100, tsq=s), sym='BSE:HDFCBANK-A'))
        self.assertGreater(live.state()['cards']['NIFTY']['flow'], 0)
        self.assertLess(live.state()['cards']['SENSEX']['flow'], 0)

    def test_recording_defaults_capture_reference_and_outcome(self):
        import cas_logger as L
        with patch('sys.argv', ['cas_logger.py']):
            args = L.parse_args()
        self.assertEqual((args.rec_start, args.rec_end), ('15:15:00', '15:32:00'))
        self.assertTrue(args.full_depth)

    def test_restart_uses_latest_session_offline_and_live(self):
        rows = []
        for start, end in [(100, 200), (1000, 900)]:
            rows.append({'type': 'meta', 'args': {'indices': 'NIFTY'}})
            rows.extend([tick(1, '15:20:00', tbq=start, tsq=100),
                         tick(1, '15:26:59', tbq=end, tsq=100)])
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'session.jsonl'
            p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            live = D.Live(p); live.poll()
            score = F.analyse(p, 'NIFTY')['rollup']['NIFTY']['book_evidence']['score']
            self.assertLess(score, 0)
            self.assertEqual(score, live.state()['cards']['NIFTY']['flow'])

    def test_stale_heavyweight_cannot_reverse_timely_direction(self):
        first, last = {}, {}
        weights = {'NIFTY': {'HDFCBANK': .4, 'RELIANCE': .2, 'TCS': .2, 'INFY': .2}}
        for tok, symbol in enumerate(weights['NIFTY']):
            first[tok] = dict(tick(tok, '15:20:00', tbq=100, tsq=100), sym='NSE:'+symbol+'-EQ')
            last[tok] = dict(tick(tok, '15:21:00' if tok == 0 else '15:26:59',
                                  tbq=0 if tok == 0 else 110, tsq=200 if tok == 0 else 100), sym='NSE:'+symbol+'-EQ')
        evidence = F.book_evidence('NIFTY', first, last,
                    datetime.fromisoformat('2026-11-23T15:27:00+05:30'), weights)
        self.assertTrue(evidence['eligible'])
        self.assertEqual(evidence['direction'], 'UP PRESSURE')

    def test_partial_json_line_is_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'log.jsonl'
            row = json.dumps(tick(1, '15:20:00', tbq=100, tsq=100))
            p.write_text(row[:30])
            live = D.Live(p); live.poll()
            with p.open('a') as f:
                f.write(row[30:] + '\n')
            live.poll()
            self.assertEqual(live.counts['ws_tick'], 1)

    def test_missing_baseline_still_renders_selected_card(self):
        live = D.Live(Path('/tmp/nonexistent-decision-log'))
        live._apply({'type': 'meta', 'args': {'indices': 'NIFTY,BANKNIFTY'}})
        state = live.state()
        self.assertEqual(set(state['cards']), {'NIFTY', 'BANKNIFTY'})
        self.assertEqual(state['cards']['NIFTY']['direction'], 'NO SIGNAL')

    def test_replaying_old_file_does_not_make_ticks_live(self):
        live = D.Live(Path('/tmp/nonexistent-decision-log'))
        live._apply(tick(1, '15:26:59', tbq=200, tsq=100))
        with patch.object(D, 'now', return_value=datetime.fromisoformat('2026-11-24T15:26:59+05:30')):
            self.assertEqual(live.state()['session']['mode'], 'REPLAY')
            self.assertGreater(live.state()['last_tick_age'], 3600)


if __name__ == '__main__':
    unittest.main()
