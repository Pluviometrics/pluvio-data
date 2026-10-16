import unittest

import mhl_mirror_store as store
from mhl_kiwis import ChallengeError, FetchError, classify, parse_headers


def row(ts, v=0.0):
    return [ts, v]


class TestClassify(unittest.TestCase):
    def test_json_ok(self):
        self.assertEqual(classify(200, {'content-type': 'application/json'}, b'[1]'), [1])

    def test_403_is_challenge(self):
        with self.assertRaises(ChallengeError):
            classify(403, {'content-type': 'text/html'}, b'<html>Just a moment...</html>')

    def test_cf_mitigated_header_is_challenge_even_on_200(self):
        with self.assertRaises(ChallengeError):
            classify(200, {'cf-mitigated': 'challenge'}, b'{}')

    def test_429_is_challenge(self):
        with self.assertRaises(ChallengeError):
            classify(429, {}, b'')

    def test_html_challenge_page_on_200(self):
        with self.assertRaises(ChallengeError):
            classify(200, {'content-type': 'text/html'}, b'<title>Just a moment...</title>')

    def test_500_is_fetch_error(self):
        with self.assertRaises(FetchError):
            classify(500, {}, b'oops')

    def test_bad_json_is_fetch_error(self):
        with self.assertRaises(FetchError):
            classify(200, {'content-type': 'application/json'}, b'not json')

    def test_parse_headers_takes_final_response(self):
        raw = 'HTTP/1.1 100 Continue\r\n\r\nHTTP/2 403\r\ncf-mitigated: challenge\r\n\r\n'
        self.assertEqual(parse_headers(raw), (403, {'cf-mitigated': 'challenge'}))


class TestStore(unittest.TestCase):
    def test_rows_for_year_drops_next_year_boundary(self):
        rows = [row('2025-12-31T23:55:00.000+11:00'), row('2026-01-01T00:00:00.000+11:00')]
        self.assertEqual(store.rows_for_year(rows, 2025), rows[:1])

    def test_merge_dedupes_and_sorts_across_dst(self):
        a = [row('2026-04-05T02:30:00.000+11:00', 1)]
        b = [row('2026-04-05T02:10:00.000+10:00', 2), row('2026-04-05T02:30:00.000+11:00', 3)]
        out = store.merge_rows(a, b)
        self.assertEqual(out, [row('2026-04-05T02:30:00.000+11:00', 3), row('2026-04-05T02:10:00.000+10:00', 2)])

    def test_document_round_trip_keeps_kiwis_shape(self):
        doc = store.year_document('1', [row('2025-01-01T00:00:00.000+11:00')])
        back = store.decode(store.encode(doc))
        self.assertEqual(back[0]['columns'], 'Timestamp,Value')
        self.assertEqual(back[0]['rows'], '1')
        self.assertEqual(back[0]['data'][0][0], '2025-01-01T00:00:00.000+11:00')

    def test_pending_and_fully_mirrored(self):
        e = {'first_year': 2024, 'coverage_to': '2026-10-16T11:00:00.000+11:00', 'years': [], 'complete_years': []}
        self.assertEqual(store.pending_years(e, 2026), [2024, 2025, 2026])
        store.record_year(e, 2024, [row('2024-06-01T00:00:00.000+10:00')], 2026)
        store.record_year(e, 2025, [], 2026)
        self.assertFalse(e['fully_mirrored'])
        self.assertEqual(store.pending_years(e, 2026), [2026])
        store.record_year(e, 2026, [row('2026-10-16T10:55:00.000+11:00')], 2026)
        self.assertTrue(e['fully_mirrored'])
        self.assertEqual(e['complete_years'], [2024, 2025])
        self.assertEqual(e['last_timestamp'], '2026-10-16T10:55:00.000+11:00')
        self.assertEqual(store.pending_years(e, 2026), [])

    def test_decommissioned_series_stops_at_coverage_end(self):
        e = {'first_year': 2010, 'coverage_to': '2012-03-01T00:00:00.000+11:00', 'years': [], 'complete_years': []}
        self.assertEqual(store.pending_years(e, 2026), [2010, 2011, 2012])

    def test_no_coverage_never_fully_mirrored(self):
        self.assertFalse(store.is_fully_mirrored({'first_year': None}, 2026))
        self.assertEqual(store.pending_years({'first_year': None}, 2026), [])


if __name__ == '__main__':
    unittest.main()
