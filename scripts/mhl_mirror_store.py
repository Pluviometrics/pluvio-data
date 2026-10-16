"""Manifest, year files and R2 uploads for the MHL mirror.

Objects in R2 bucket gauge-data (public at https://gauge-data.pluviometrics.com.au):
  mhl/{ts_id}/{YYYY}.json   one gauge-year, gzip, Content-Encoding: gzip, in the KiWIS
                            getTimeseriesValues JSON shape: [{ts_id, rows, columns, data}]
  mhl/manifest.json         per ts_id: years present, complete years, last timestamp mirrored

The local manifest under outputs/mhl_mirror/ is the source of truth and the progress
record; the R2 copy is published from it. Uploads use `wrangler r2 object put --remote`,
as build_radar_sites.mjs does: CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID when set,
otherwise wrangler's own login.
"""
import gzip
import json
import os
import subprocess
from datetime import datetime

BUCKET = 'gauge-data'
PUBLIC_BASE = 'https://gauge-data.pluviometrics.com.au'
MANIFEST_KEY = 'mhl/manifest.json'
DATASET = 'pluviometrics.mhl_mirror.v1'
CACHE_COMPLETE = 'public, max-age=86400'
CACHE_LIVE = 'no-cache'


def year_key(ts_id, year):
    return f'mhl/{ts_id}/{year}.json'


def epoch(ts):
    return datetime.fromisoformat(ts).timestamp()


def rows_for_year(rows, year):
    """Rows whose local timestamp falls in `year` (KiWIS `to` is inclusive of the boundary)."""
    prefix = f'{year:04d}-'
    return [r for r in rows if isinstance(r, list) and r and str(r[0]).startswith(prefix)]


def merge_rows(*row_lists):
    """One row per timestamp (later lists win), sorted by instant."""
    by_ts = {}
    for rows in row_lists:
        for r in rows:
            by_ts[r[0]] = r
    return sorted(by_ts.values(), key=lambda r: epoch(r[0]))


def year_document(ts_id, rows):
    return [{'ts_id': str(ts_id), 'rows': str(len(rows)), 'columns': 'Timestamp,Value', 'data': rows}]


def encode(doc):
    return gzip.compress(json.dumps(doc, separators=(',', ':')).encode('utf-8'), mtime=0)


def decode(blob):
    return json.loads(gzip.decompress(blob))


def new_manifest():
    return {'dataset': DATASET, 'source': 'https://wiski.mhl.nsw.gov.au/KiWIS/KiWIS',
            'key_pattern': 'mhl/{ts_id}/{YYYY}.json', 'generated_at': None, 'stations': {}}


def load_manifest(path):
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return new_manifest()


def save_manifest(manifest, path):
    manifest['generated_at'] = datetime.now().astimezone().isoformat(timespec='seconds')
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def last_year(entry, current_year):
    """Final calendar year this series can have data for."""
    to = entry.get('coverage_to')
    return min(current_year, int(to[:4])) if to else current_year


def record_year(entry, year, rows, current_year):
    """Note a mirrored year in a manifest entry."""
    years = set(entry.get('years', []))
    years.add(year)
    entry['years'] = sorted(years)
    complete = set(entry.get('complete_years', []))
    if year < current_year:
        complete.add(year)
    entry['complete_years'] = sorted(complete)
    if rows:
        last = rows[-1][0]
        if not entry.get('last_timestamp') or epoch(last) > epoch(entry['last_timestamp']):
            entry['last_timestamp'] = last
    entry['fully_mirrored'] = is_fully_mirrored(entry, current_year)


def is_fully_mirrored(entry, current_year):
    first = entry.get('first_year')
    if first is None:
        return False
    end = last_year(entry, current_year)
    complete, years = set(entry.get('complete_years', [])), set(entry.get('years', []))
    return all(y in complete or (y == current_year and y in years) for y in range(first, end + 1))


def pending_years(entry, current_year):
    """Years still to fetch, oldest first. The current year counts once present."""
    first = entry.get('first_year')
    if first is None:
        return []
    complete, years = set(entry.get('complete_years', [])), set(entry.get('years', []))
    return [y for y in range(first, last_year(entry, current_year) + 1)
            if y not in complete and not (y == current_year and y in years)]


class R2:
    """wrangler r2 object put/get against the gauge-data bucket."""

    def __init__(self, project_root, log):
        self.root, self.log = project_root, log
        self.wrangler = os.path.join(project_root, 'node_modules', 'wrangler', 'bin', 'wrangler.js')

    def _run(self, args):
        r = subprocess.run(['node', self.wrangler, *args], cwd=self.root, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if r.returncode != 0:
            raise RuntimeError(f'wrangler {" ".join(args[:4])} failed (exit {r.returncode}): '
                               f'{(r.stderr or r.stdout).strip()[-800:]}')

    def put(self, key, blob, cache_control, gzip_encoded=True, local_path=None):
        path = local_path
        if path is None:
            path = os.path.join(self.root, 'outputs', 'mhl_mirror', 'upload.tmp')
            with open(path, 'wb') as f:
                f.write(blob)
        args = ['r2', 'object', 'put', f'{BUCKET}/{key}', '--file', path,
                '--content-type', 'application/json', '--cache-control', cache_control, '--remote']
        if gzip_encoded:
            args += ['--content-encoding', 'gzip']
        self._run(args)
        self.log(f'PUT {key} ({len(blob)} bytes, {cache_control})')

    def get(self, key, local_path):
        self._run(['r2', 'object', 'get', f'{BUCKET}/{key}', '--file', local_path, '--remote'])
        with open(local_path, 'rb') as f:
            return f.read()

    def publish_manifest(self, manifest):
        blob = json.dumps(manifest, separators=(',', ':'), sort_keys=True).encode('utf-8')
        self.put(MANIFEST_KEY, gzip.compress(blob, mtime=0), CACHE_LIVE)
