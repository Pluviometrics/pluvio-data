#!/usr/bin/env python3
"""publish_bom_current_readings.py -- BoM 15-minute current rainfall for catalogue stations.

Reads the IDZ65900 snapshots preserved by collect_bom_hcs.py, keys them on the 6-digit BoM
SiteId, joins them to catalogue stations that have no KiWIS series (no ts_id / wdo_ts_id /
mhl_ts_id) but carry a 6-digit BoM number in data_identifier, and writes one browser-sized
file beside the catalogue for data.pluviometrics.com.au.

Inputs (read only):
  pluviometrics_rainfall_stations.json                        station catalogue (this repo)
  source\\bom_hcs_archive\\raw\\IDZ65900\\*.hcs.<sha>.gz            national 15-minute snapshots

Output (overwritten):
  bom_current_readings.json                                   served at data.pluviometrics.com.au

Each snapshot is a rolling ~2 hour window republished every 15 minutes. Snapshots are read
newest first; a snapshot is skipped when the last COVERAGE_HOURS of its window is already
covered by newer snapshots (a slot keeps the value from the newest snapshot that carried it).
Row totals across snapshots are not unique observations. Nothing is written until the join
has succeeded.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]                       # PLUVIO_DATA
CATALOGUE_PATH = ROOT / "pluviometrics_rainfall_stations.json"
SNAPSHOT_DIR = ROOT / "source" / "bom_hcs_archive" / "raw" / "IDZ65900"
OUTPUT_PATH = ROOT / "bom_current_readings.json"

PRODUCT = "IDZ65900"
INTERVAL_MINUTES = 15
WINDOW_DAYS = 7
SNAPSHOT_SPAN_HOURS = 2.0        # each IDZ65900 file carries about the last two hours
COVERAGE_HOURS = 1.5             # skip a snapshot when this much of its tail is already covered
FIELDS = ['IndexNo', 'SensorType', 'SensorDataType', 'SiteIdType', 'SiteId',
          'ObservationTimestamp', 'RealValue', 'Unit', 'SensorParam1',
          'SensorParam2', 'Quality', 'Comment']
NAME_RE = re.compile(r"IDZ65900_(\d{14})\.hcs\.[0-9a-f]{12}\.gz\Z")
SIX_DIGIT_RE = re.compile(r"(?<![0-9])(\d{6})(?![0-9])")


def fail(msg: str) -> None:
    print(f"ABORT: {msg}", file=sys.stderr)
    sys.exit(1)


def snapshot_time(path: Path) -> datetime | None:
    m = NAME_RE.match(path.name)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def read_snapshot(path: Path) -> tuple[dict, list[tuple[str, str, str, str]]]:
    """(header metadata, [(SiteId, ObservationTimestamp, RealValue, Quality)]) with a fast, strict parse."""
    text = gzip.decompress(path.read_bytes()).decode("utf-8-sig")
    metadata: dict[str, str] = {}
    rows = []
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("#"):
            if line.startswith("# HEADER: "):
                key, _, value = line[10:].partition(": ")
                metadata[key] = value
            continue
        parts = line.split(",")
        if len(parts) != len(FIELDS):
            raise ValueError(f"{path.name}: row with {len(parts)} fields")
        # Text fields are double-quoted; none of the four we keep can legitimately contain a comma.
        rows.append(tuple(parts[i].strip('"') for i in (4, 5, 6, 10)))
    fields = [s.strip() for s in metadata.get("Data Fields", "").split(",")]
    if fields != FIELDS:
        raise ValueError(f"{path.name}: unknown HCS field schema")
    if len(rows) != int(metadata.get("Number of Records", "-1")):
        raise ValueError(f"{path.name}: header record count does not match body")
    return metadata, rows


def catalogue_targets(catalogue: dict) -> list[dict]:
    """Catalogue stations with no KiWIS series and at least one 6-digit BoM number."""
    out = []
    for s in catalogue.get("stations", []):
        if s.get("ts_id") or s.get("wdo_ts_id") or s.get("mhl_ts_id"):
            continue
        ids = sorted(set(SIX_DIGIT_RE.findall(str(s.get("data_identifier") or ""))))
        if not ids:
            continue
        try:
            lat, lon = float(s["lat"]), float(s["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        out.append({"station": s, "bom_ids": ids, "lat": lat, "lon": lon})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window-days", type=int, default=WINDOW_DAYS)
    ap.add_argument("--now", default=None, help="ISO UTC instant for the window end (default: now)")
    a = ap.parse_args()

    for p in (CATALOGUE_PATH, SNAPSHOT_DIR):
        if not p.exists():
            fail(f"missing input: {p}")

    now = datetime.fromisoformat(a.now.replace("Z", "+00:00")) if a.now else datetime.now(timezone.utc)
    now = now.astimezone(timezone.utc)
    window_end = now
    window_start = now - timedelta(days=a.window_days)
    step = timedelta(minutes=INTERVAL_MINUTES)

    catalogue = json.loads(CATALOGUE_PATH.read_text(encoding="utf-8"))
    no_series = [s for s in catalogue.get("stations", [])
                 if not (s.get("ts_id") or s.get("wdo_ts_id") or s.get("mhl_ts_id"))]
    targets = catalogue_targets(catalogue)
    wanted_ids = {i for t in targets for i in t["bom_ids"]}

    # Snapshots newest first, skipping any whose useful tail is already covered.
    candidates = []
    for p in SNAPSHOT_DIR.glob("*.gz"):
        t = snapshot_time(p)
        if t is None:
            continue
        if t < window_start or t > window_end + timedelta(hours=1):
            continue
        candidates.append((t, p))
    candidates.sort(reverse=True)
    if not candidates:
        fail(f"no {PRODUCT} snapshots inside the {a.window_days}-day window ending {window_end.isoformat()}")

    covered: list[tuple[datetime, datetime]] = []   # merged coverage intervals

    def is_covered(start: datetime, end: datetime) -> bool:
        return any(cs <= start and end <= ce for cs, ce in covered)

    def add_cover(start: datetime, end: datetime) -> None:
        covered.append((start, end))
        covered.sort()
        merged: list[tuple[datetime, datetime]] = []
        for cs, ce in covered:
            if merged and cs <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], ce))
            else:
                merged.append((cs, ce))
        covered[:] = merged

    slots: dict[str, dict[datetime, float]] = {i: {} for i in wanted_ids}
    sites_seen: set[str] = set()
    used, skipped, bad = [], 0, []
    off_grid = 0
    quality = Counter()
    latest_generation = None
    for t, p in candidates:
        tail_start = t - timedelta(hours=COVERAGE_HOURS)
        if is_covered(tail_start, t):
            skipped += 1
            continue
        try:
            metadata, rows = read_snapshot(p)
        except Exception as exc:              # keep going; an unreadable file is evidence, not fatal
            bad.append({"file": p.name, "error": str(exc)})
            continue
        gen = metadata.get("File Generation Date", "")
        if latest_generation is None or gen > latest_generation:
            latest_generation = gen
        for site, ts, value, q in rows:
            sites_seen.add(site)
            if site not in slots:
                continue
            obs = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            if obs < window_start or obs > window_end:
                continue
            if (obs.minute % INTERVAL_MINUTES) or obs.second:
                off_grid += 1
                continue
            try:
                mm = float(value)
            except ValueError:
                continue
            if mm < 0:
                continue
            quality[q] += 1
            slots[site].setdefault(obs, mm)     # newest snapshot wins: files are processed newest first
        used.append(p.name)
        add_cover(t - timedelta(hours=SNAPSHOT_SPAN_HOURS), t)

    matched = [t for t in targets if any(i in sites_seen for i in t["bom_ids"])]
    stations_out = []
    for t in matched:
        s = t["station"]
        ids = [i for i in t["bom_ids"] if i in sites_seen]
        merged: dict[datetime, float] = {}
        for i in ids:                              # a record with two BoM numbers: first id wins per slot
            for k, v in slots[i].items():
                merged.setdefault(k, v)
        if merged:
            first = min(merged)
            last = max(merged)
            n = int((last - first) / step) + 1
            values = [None] * n
            for k, v in merged.items():
                values[int((k - first) / step)] = v
        else:
            first = last = None
            values = []
        stations_out.append({
            "station_id": s["station_id"],
            "gauge_uid": s.get("gauge_uid"),
            "bom_id": ids[0],
            "bom_ids": ids,
            "station_name": s.get("station_name"),
            "lga": s.get("lga"),
            "lat": t["lat"],
            "lon": t["lon"],
            "ifd_status": s.get("ifd_status"),
            "first_reading": first.isoformat().replace("+00:00", "Z") if first else None,
            "last_reading": last.isoformat().replace("+00:00", "Z") if last else None,
            "reading_count": len(merged),
            "start": first.isoformat().replace("+00:00", "Z") if first else None,
            "values": values,
        })
    stations_out.sort(key=lambda r: r["station_id"])

    doc = {
        "dataset": "bom_current_readings",
        "generated_at": now.isoformat(),
        "source": f"BoM HCS {PRODUCT} 15-minute rainfall (ftp.bom.gov.au/anon/gen/fwo), archived by collect_bom_hcs.py",
        "interval_minutes": INTERVAL_MINUTES,
        "window_days": a.window_days,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "series_encoding": "values[i] is mm for the 15-minute slot ending at start + i*15 min (UTC); null = no reading",
        "snapshots": {
            "used": len(used),
            "skipped_already_covered": skipped,
            "unreadable": bad,
            "latest_generation": latest_generation,
            "newest_used": used[0] if used else None,
            "oldest_used": used[-1] if used else None,
            "sites_seen": len(sites_seen),
            "off_grid_rows_dropped": off_grid,
            "quality_flags": dict(sorted(quality.items())),
        },
        "catalogue": {
            "generated_at": catalogue.get("generated_at"),
            "no_series_records": len(no_series),
            "with_bom_number": len(targets),
            "matched": len(stations_out),
            "matched_lgas": len({r["lga"] for r in stations_out}),
            "matched_with_readings": sum(1 for r in stations_out if r["reading_count"]),
            "unmatched": len(no_series) - len(stations_out),
        },
        "stations": stations_out,
    }
    OUTPUT_PATH.write_text(json.dumps(doc, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    c, sn = doc["catalogue"], doc["snapshots"]
    print(f"snapshots: used {sn['used']}, skipped {sn['skipped_already_covered']}, unreadable {len(bad)}, "
          f"sites seen {sn['sites_seen']}, latest generation {latest_generation}")
    print(f"catalogue: {c['no_series_records']} no-series records, {c['with_bom_number']} with a BoM number, "
          f"matched {c['matched']} in {c['matched_lgas']} LGAs ({c['matched_with_readings']} with readings in window)")
    print(f"wrote {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes, {len(stations_out)} stations)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
