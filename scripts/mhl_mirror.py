"""MHL KiWIS history mirror to R2 bucket gauge-data.

  py scripts\\mhl_mirror.py backfill      resumable full-record backfill, then one incremental pass
  py scripts\\mhl_mirror.py incremental   append new values to each gauge's latest year file
  add --only <ts_id> to limit either mode to one catalogue gauge (testing)

Runs from the scheduled tasks "Pluvio MHL Mirror Backfill" and "Pluvio MHL Mirror Daily"
(see README). Scope: every source=mhl station in pluviometrics_rainfall_stations.json with
cls A-E, keyed on the catalogue ts_id.

One request at a time, at least 2 s apart, through curl.exe with its default User-Agent.
A 403, 429 or Cloudflare challenge stops the run, writes outputs\\mhl_mirror\\HALTED, and
every later run refuses to start until that file is deleted by hand. Nothing retries
around it.

Local state (outputs\\ is gitignored):
  outputs\\mhl_mirror\\manifest.json          progress record; published as mhl/manifest.json
  outputs\\mhl_mirror\\live\\{ts_id}\\{YYYY}.json.gz  copy of each gauge's latest year file
  outputs\\mhl_mirror\\logs\\{mode}_{yyyyMMdd_HHmmss}.log
  outputs\\mhl_mirror\\HALTED                 present only after a stop-on-challenge
  outputs\\mhl_mirror\\run.lock               held while a run is active
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mhl_kiwis import ChallengeError, FetchError, Kiwis  # noqa: E402
import mhl_mirror_store as store  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGUE = os.path.join(ROOT, 'pluviometrics_rainfall_stations.json')
STATE = os.path.join(ROOT, 'outputs', 'mhl_mirror')
MANIFEST = os.path.join(STATE, 'manifest.json')
HALTED = os.path.join(STATE, 'HALTED')
LOCK = os.path.join(STATE, 'run.lock')
CLASSES = set('ABCDE')
PUBLISH_EVERY = 20
MAX_CONSECUTIVE_ERRORS = 5
ONLY = None


class Log:
    def __init__(self, mode):
        os.makedirs(os.path.join(STATE, 'logs'), exist_ok=True)
        self.path = os.path.join(STATE, 'logs', f'{mode}_{datetime.now():%Y%m%d_%H%M%S}.log')

    def __call__(self, text):
        line = f'{datetime.now().astimezone():%Y-%m-%d %H:%M:%S %z} {text}'
        with open(self.path, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
        print(line, flush=True)


def catalogue_stations():
    with open(CATALOGUE, encoding='utf-8') as f:
        stations = json.load(f)['stations']
    out = {}
    for s in stations:
        if s.get('source') == 'mhl' and s.get('cls') in CLASSES and s.get('ts_id'):
            out.setdefault(str(s['ts_id']), s.get('station_name') or '')
    return {ONLY: out[ONLY]} if ONLY else out


def naive(ts):
    """KiWIS from/to: naive Sydney-local, as MHL expects."""
    return ts[:19]


def live_path(ts_id, year):
    return os.path.join(STATE, 'live', ts_id, f'{year}.json.gz')


def write_live(ts_id, year, blob):
    d = os.path.join(STATE, 'live', ts_id)
    os.makedirs(d, exist_ok=True)
    for name in os.listdir(d):  # keep only the latest year locally
        if name != f'{year}.json.gz':
            os.remove(os.path.join(d, name))
    path = live_path(ts_id, year)
    with open(path, 'wb') as f:
        f.write(blob)
    return path


def read_year(r2, ts_id, year):
    path = live_path(ts_id, year)
    if os.path.exists(path):
        with open(path, 'rb') as f:
            blob = f.read()
    else:
        tmp = os.path.join(STATE, 'download.tmp')
        blob = r2.get(store.year_key(ts_id, year), tmp)
    doc = store.decode(blob) if blob[:2] == b'\x1f\x8b' else json.loads(blob)
    return doc[0]['data'] if doc else []


def put_year(r2, ts_id, year, rows, current_year):
    blob = store.encode(store.year_document(ts_id, rows))
    cache = store.CACHE_COMPLETE if year < current_year else store.CACHE_LIVE
    path = write_live(ts_id, year, blob)
    r2.put(store.year_key(ts_id, year), blob, cache, local_path=path)
    return len(blob)


class Runner:
    def __init__(self, mode, log):
        self.mode, self.log = mode, log
        self.kiwis = Kiwis(self.log)
        self.r2 = store.R2(ROOT, self.log)
        self.manifest = store.load_manifest(MANIFEST)
        self.current_year = datetime.now().year
        self.uploads = self.bytes = self.errors = self.consecutive_errors = 0
        self.since_publish = 0

    def save(self, publish=False):
        store.save_manifest(self.manifest, MANIFEST)
        if publish or self.since_publish >= PUBLISH_EVERY:
            self.r2.publish_manifest(self.manifest)
            self.since_publish = 0

    def uploaded(self, size):
        self.uploads += 1
        self.bytes += size
        self.since_publish += 1
        self.consecutive_errors = 0

    def failed(self, what, err):
        self.errors += 1
        self.consecutive_errors += 1
        self.log(f'ERROR {what}: {err}')
        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            raise RuntimeError(f'{self.consecutive_errors} consecutive errors; ending this run')

    def entry(self, ts_id, name):
        e = self.manifest['stations'].setdefault(ts_id, {'station_name': name, 'years': [],
                                                         'complete_years': [], 'last_timestamp': None})
        e['station_name'] = name
        return e

    def backfill(self):
        stations = catalogue_stations()
        self.log(f'backfill: {len(stations)} MHL stations (cls A-E) in catalogue')
        did_work = False
        for i, (ts_id, name) in enumerate(stations.items(), 1):
            e = self.entry(ts_id, name)
            if 'first_year' not in e:
                try:
                    start, end = self.kiwis.coverage(ts_id)
                except FetchError as err:
                    self.failed(f'coverage {ts_id}', err)
                    continue
                did_work = True
                e['coverage_from'], e['coverage_to'] = start, end
                e['first_year'] = int(start[:4]) if start else None
                e['fully_mirrored'] = store.is_fully_mirrored(e, self.current_year)
                self.save()
                if start is None:
                    self.log(f'[{i}/{len(stations)}] {ts_id} {name}: KiWIS lists no coverage; skipped')
                    continue
            todo = store.pending_years(e, self.current_year)
            if not todo:
                continue
            self.log(f'[{i}/{len(stations)}] {ts_id} {name}: {len(todo)} year(s) to fetch '
                     f'({todo[0]}-{todo[-1]})')
            for year in todo:
                did_work = True
                try:
                    raw = self.kiwis.values(ts_id, f'{year}-01-01T00:00:00', f'{year + 1}-01-01T00:00:00')
                    rows = store.merge_rows(store.rows_for_year(raw, year))
                    size = put_year(self.r2, ts_id, year, rows, self.current_year)
                except FetchError as err:
                    self.failed(f'{ts_id} {year}', err)
                    continue
                store.record_year(e, year, rows, self.current_year)
                self.uploaded(size)
                self.log(f'  {ts_id} {year}: {len(rows)} rows, {size} bytes gz')
                self.save()
            if e.get('fully_mirrored'):
                self.log(f'  {ts_id} fully mirrored, last {e.get("last_timestamp")}')
        self.save(publish=True)
        done = sum(1 for e in self.manifest['stations'].values() if e.get('fully_mirrored'))
        self.log(f'backfill pass done: {done}/{len(stations)} stations fully mirrored')
        if did_work:
            self.incremental()

    def incremental(self):
        stations = catalogue_stations()
        self.log('incremental: appending new values to fully mirrored stations')
        now = datetime.now()
        for ts_id, name in stations.items():
            e = self.manifest['stations'].get(ts_id)
            if not e or not e.get('fully_mirrored'):
                continue
            last = e.get('last_timestamp')
            start = naive(last) if last else f'{self.current_year}-01-01T00:00:00'
            try:
                raw = self.kiwis.values(ts_id, start, (now + timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S'))
            except FetchError as err:
                self.failed(f'incremental {ts_id}', err)
                continue
            new_years = sorted({int(str(r[0])[:4]) for r in raw if r})
            if raw:
                newest = max(raw, key=lambda r: store.epoch(r[0]))[0]
                if not e.get('coverage_to') or store.epoch(newest) > store.epoch(e['coverage_to']):
                    e['coverage_to'] = newest
            appended = 0
            try:
                for year in new_years:
                    old = read_year(self.r2, ts_id, year) if year in e.get('years', []) else []
                    rows = store.merge_rows(old, store.rows_for_year(raw, year))
                    if year in e.get('years', []) and len(rows) == len(old):
                        continue
                    size = put_year(self.r2, ts_id, year, rows, self.current_year)
                    store.record_year(e, year, rows, self.current_year)
                    self.uploaded(size)
                    appended += len(rows) - len(old)
            except (FetchError, RuntimeError, ValueError) as err:
                self.failed(f'incremental {ts_id}', err)
                continue
            # Years before the current one are complete once the calendar has moved on.
            for y in e.get('years', []):
                if y < self.current_year and y not in e['complete_years']:
                    e['complete_years'] = sorted(set(e['complete_years']) | {y})
            e['fully_mirrored'] = store.is_fully_mirrored(e, self.current_year)
            self.log(f'  {ts_id} {name}: +{appended} rows, last {e.get("last_timestamp")}')
            self.save()
        self.save(publish=True)


def acquire_lock(log):
    try:
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        with open(LOCK, encoding='utf-8') as f:
            pid = f.read().strip()
        if pid.isdigit() and pid_alive(int(pid)):
            log(f'SKIPPED: another mirror run (pid {pid}) holds {LOCK}')
            return False
        log(f'stale lock from pid {pid or "?"}; taking it over')
        os.remove(LOCK)
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    return True


def pid_alive(pid):
    import ctypes
    handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        return False
    code = ctypes.c_ulong()
    ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
    ctypes.windll.kernel32.CloseHandle(handle)
    return code.value == 259  # STILL_ACTIVE


def halt(log, err, runner):
    banner = '!' * 78
    msg = (f'STOPPED ON MHL CHALLENGE: HTTP {err.status} {err.detail}\n'
           f'URL: {err.url}\n'
           f'Run stopped at {datetime.now().astimezone().isoformat(timespec="seconds")}. Nothing was retried.\n'
           f'Every later run refuses to start until this file is deleted: {HALTED}\n')
    with open(HALTED, 'w', encoding='utf-8') as f:
        f.write(msg)
    log(banner)
    for line in msg.strip().split('\n'):
        log('!!! ' + line)
    log(banner)
    try:
        runner.save(publish=True)
    except Exception as e:  # the stop is what matters; report a failed publish and move on
        log(f'ERROR publishing manifest after stop: {e}')


def main(argv):
    global ONLY
    mode = argv[1] if len(argv) > 1 else ''
    if len(argv) == 4 and argv[2] == '--only':
        ONLY = argv[3]
    if mode not in ('backfill', 'incremental'):
        print(__doc__)
        return 64
    os.makedirs(STATE, exist_ok=True)
    log = Log(mode)
    log(f'manifest: task=mhl_mirror mode={mode} user={os.environ.get("USERNAME")} '
        f'host={os.environ.get("COMPUTERNAME")} root={ROOT} python={sys.version.split()[0]}')
    if os.path.exists(HALTED):
        with open(HALTED, encoding='utf-8') as f:
            log('!!! HALTED marker present; not running. Contents:\n' + f.read())
        return 3
    if not acquire_lock(log):
        return 0
    runner = Runner(mode, log)
    started = time.monotonic()
    try:
        getattr(runner, mode)()
        code = 0
    except ChallengeError as err:
        halt(log, err, runner)
        code = 2
    except Exception as err:
        log(f'FAIL {type(err).__name__}: {err}')
        try:
            runner.save(publish=True)
        except Exception as e:
            log(f'ERROR publishing manifest: {e}')
        code = 1
    finally:
        os.remove(LOCK)
    mins = (time.monotonic() - started) / 60
    log(f'END mode={mode} exit={code} requests={runner.kiwis.requests} uploads={runner.uploads} '
        f'bytes={runner.bytes} errors={runner.errors} minutes={mins:.1f}')
    return code


if __name__ == '__main__':
    sys.exit(main(sys.argv))
