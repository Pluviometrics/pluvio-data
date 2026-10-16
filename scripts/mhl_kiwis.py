"""MHL KiWIS requests for the MHL mirror.

Every request goes through the system curl.exe with its default User-Agent and no
added headers. A 403, a 429 or a Cloudflare challenge raises ChallengeError, which
stops the whole run: the caller must not retry around it.
"""
import json
import os
import subprocess
import tempfile
import time

KIWIS = 'https://wiski.mhl.nsw.gov.au/KiWIS/KiWIS'
BASE_QUERY = 'service=kisters&type=queryServices&format=json'
MIN_GAP_S = 2.0
TIMEOUT_S = 300


class ChallengeError(Exception):
    """MHL refused the request (403/429 or Cloudflare challenge). Stop everything."""

    def __init__(self, url, status, detail):
        super().__init__(f'HTTP {status} {detail} for {url}')
        self.url, self.status, self.detail = url, status, detail


class FetchError(Exception):
    """Any other failure (network, timeout, 5xx, bad JSON). Skippable for this run."""


def classify(status, headers, body):
    """Return the decoded JSON, or raise ChallengeError / FetchError."""
    mitigated = headers.get('cf-mitigated', '')
    ctype = headers.get('content-type', '')
    looks_challenge = b'Just a moment' in body[:2000] or b'challenge-platform' in body[:20000]
    if status in (403, 429) or mitigated or (looks_challenge and 'html' in ctype):
        detail = f'cf-mitigated={mitigated or "-"} content-type={ctype or "-"}'
        raise ChallengeError(None, status, detail)
    if status != 200:
        raise FetchError(f'HTTP {status} content-type={ctype or "-"}')
    try:
        return json.loads(body)
    except ValueError as e:
        raise FetchError(f'HTTP 200 but body is not JSON ({ctype or "-"}): {e}') from None


def parse_headers(raw):
    """Headers of the final response in a curl -D dump, lower-cased keys."""
    blocks = [b for b in raw.replace('\r\n', '\n').split('\n\n') if b.strip()]
    headers, status = {}, 0
    if blocks:
        lines = blocks[-1].split('\n')
        parts = lines[0].split()
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        for line in lines[1:]:
            k, _, v = line.partition(':')
            headers[k.strip().lower()] = v.strip()
    return status, headers


class Kiwis:
    """Paced KiWIS client: at least MIN_GAP_S between the end of one request and the next."""

    def __init__(self, log, gap_s=MIN_GAP_S):
        self.log, self.gap_s, self.last_end = log, gap_s, 0.0
        self.requests = 0

    def get(self, query):
        url = f'{KIWIS}?{BASE_QUERY}&{query}'
        wait = self.gap_s - (time.monotonic() - self.last_end)
        if wait > 0:
            time.sleep(wait)
        fd_h, hpath = tempfile.mkstemp(suffix='.hdr')
        fd_b, bpath = tempfile.mkstemp(suffix='.body')
        os.close(fd_h)
        os.close(fd_b)
        started = time.monotonic()
        try:
            r = subprocess.run(['curl.exe', '-sS', '--max-time', str(TIMEOUT_S), '-D', hpath, '-o', bpath, url],
                               capture_output=True, text=True, encoding='utf-8', errors='replace')
            self.last_end = time.monotonic()
            self.requests += 1
            if r.returncode != 0:
                raise FetchError(f'curl exit {r.returncode}: {r.stderr.strip()}')
            with open(hpath, encoding='latin-1') as f:
                status, headers = parse_headers(f.read())
            with open(bpath, 'rb') as f:
                body = f.read()
        finally:
            for p in (hpath, bpath):
                try:
                    os.remove(p)
                except OSError:
                    pass
        self.log(f'GET {query} -> HTTP {status}, {len(body)} bytes, {self.last_end - started:.1f}s')
        try:
            return classify(status, headers, body)
        except ChallengeError as e:
            e.url = url
            raise

    def coverage(self, ts_id):
        """(from, to) timestamps of the series, or (None, None) when KiWIS lists none."""
        rows = self.get(f'request=getTimeseriesList&ts_id={ts_id}&returnfields=ts_id,coverage')
        for row in rows[1:] if isinstance(rows, list) else []:
            if str(row[0]) == str(ts_id):
                return row[1] or None, row[2] or None
        return None, None

    def values(self, ts_id, start, end):
        """Raw [[timestamp, value], ...] between two naive Sydney-local timestamps."""
        data = self.get(f'request=getTimeseriesValues&ts_id={ts_id}&from={start}&to={end}'
                        '&returnfields=Timestamp,Value')
        if not data:
            return []
        return data[0].get('data') or []
