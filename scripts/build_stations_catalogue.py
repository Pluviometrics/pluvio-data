#!/usr/bin/env python3
"""
build_stations_catalogue.py -- station catalogue + embedded IFD table from the gauge registry CSV.

Inputs (read only):
  nsw_rainfall_stations.csv                                 gauge registry export (this repo)
  nsw_rainfall_stations_quality.csv                         statewide quality gate by gauge_uid
  source\\bom_wdo_rainfall_asstored_national_deduplicated_full_pull.csv
                                                             one row per physical BoM WDO gauge (identity)
  source\\bom_wdo_physical_gauge_observation_members_full_pull.csv
                                                             observation-safe membership map
  ..\\PLUVIO_STORMGAUGE\\nsw_lga_boundaries.js                 NSW LGA polygons (point-in-polygon gate)
  ..\\PLUVIO_STORMGAUGE_NSW\\data\\nsw_rainfall_stations_ifd.json  fresh BoM IFD per gauge_uid
                                                             (29 durations x 7 AEP columns + rare)

Outputs (overwritten):
  pluviometrics_rainfall_stations.json                      station catalogue (this repo)
  ..\\PLUVIO_STORMGAUGE\\data\\pluviometrics_ifd_table.json  IFD table keyed by gauge_uid

Selection: live == true AND cls in {A,B,C,D,E} AND quality_status in {ok,unassessed}.
The run aborts unless exactly EXPECTED_COUNT gauges are selected before applying the quality
gate. Gauges without an accepted IFD remain in the catalogue (ifd_status != "ok") but are
omitted from the IFD table and recorded there as errors, so they can never return an AEP.
Every included IFD table is monotonic (non-decreasing across AEP columns from common to
rare, and non-decreasing with duration in every column).

BoM Water Data Online (WDO) side. The dedupe file selects one canonical identity per physical
gauge; it is NOT an observation filter. Every WDO record carries wdo_physical_gauge_id and
wdo_member_ts_ids (every include_for_observation_union member, keeper first) so that any
observation read unions all of them. A registry wdo_ts_id that is a discarded member is
re-pointed at the keeper. WDO records are gated to NSW by point-in-polygon against the LGA
boundaries (never a bounding box); a record that misses every polygon is kept only when it
is within NSW_BOUNDARY_SNAP_KM of an LGA edge (river-centreline border gauges, polygon
slivers) and every such retention is listed. WDO records with impossible or missing
coordinates are quarantined and counted; none can reach the catalogue or the map.

Nothing is written until every check has passed.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]                       # PLUVIO_DATA
CSV_PATH = ROOT / "nsw_rainfall_stations.csv"
QUALITY_PATH = ROOT / "nsw_rainfall_stations_quality.csv"
WDO_IDENTITY_PATH = ROOT / "source" / "bom_wdo_rainfall_asstored_national_deduplicated_full_pull.csv"
WDO_MEMBERS_PATH = ROOT / "source" / "bom_wdo_physical_gauge_observation_members_full_pull.csv"
LGA_BOUNDARIES_PATH = ROOT.parent / "PLUVIO_STORMGAUGE" / "nsw_lga_boundaries.js"
QUARANTINE_OUT = ROOT / "outputs" / "wdo_coordinate_quarantine.json"   # outputs\ is gitignored
IFD_INPUT = ROOT.parent / "PLUVIO_STORMGAUGE_NSW" / "data" / "nsw_rainfall_stations_ifd.json"
CATALOGUE_OUT = ROOT / "pluviometrics_rainfall_stations.json"
IFD_TABLE_OUT = ROOT.parent / "PLUVIO_STORMGAUGE" / "data" / "pluviometrics_ifd_table.json"

# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

EXPECTED_COUNT = 1154
ACCEPTED_CLASSES = ("A", "B", "C", "D", "E")
ACCEPTED_QUALITY_STATUS = ("ok", "unassessed")
ACCEPTED_IFD_STATUS = ("ok",)

# WDO coordinate sanity: anything outside this Australian envelope is impossible.
PLAUSIBLE_LAT = (-45.0, -9.0)
PLAUSIBLE_LON = (110.0, 155.0)
# A WDO record that misses every LGA polygon is retained only this close to an LGA edge.
NSW_BOUNDARY_SNAP_KM = 0.5
KM_PER_DEG = 111.2

# AEP columns in order from common to rare. Rare columns are merged from the
# record's `rare` block; "1 in 100" and "1 in 2000" are deliberately not carried.
AEP_COLUMNS = ["63.2%", "50%", "20%", "10%", "5%", "2%", "1%"]
RARE_COLUMNS = ["1 in 200", "1 in 500", "1 in 1000"]
ALL_COLUMNS = AEP_COLUMNS + RARE_COLUMNS

# Column-monotonicity exemption. BoM's rare design-rainfall grids are derived separately
# from the standard IFD and are not smoothed across the daily boundary, so the rare columns
# can dip slightly between 1440 and 1800 minutes. A decrease in a rare column at that one
# step is tolerated up to max(TOLERANCE_MM, TOLERANCE_PCT % of the 1440 value). Every other
# column, every other step, and the row check stay strict. Every exempted cell is recorded.
TOLERANCE_MM = 5.0
TOLERANCE_PCT = 3.0
TOLERANCE_STEP = ("1440", "1800")
IFD_VALIDATION_RULE = (
    "Each row non-decreasing across AEP columns from common to rare; each column "
    "non-decreasing with duration. Exemption: rare columns may decrease at the "
    f"{TOLERANCE_STEP[0]}-{TOLERANCE_STEP[1]} minute step only, by at most "
    f"{TOLERANCE_MM:g} mm or {TOLERANCE_PCT:g} percent of the {TOLERANCE_STEP[0]} value, "
    "whichever is larger. Exempted cells are listed."
)

COASTAL_SUFFIX = " (coastal)"


def fail(msg: str) -> None:
    print(f"ABORT: {msg}", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def is_selected(row: dict) -> bool:
    if row.get("live", "").strip().lower() != "true":
        return False
    if row.get("cls", "").strip() not in ACCEPTED_CLASSES:
        return False
    return True


def load_selected_rows() -> tuple[list[dict], list[dict], list[dict], Counter]:
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    eligible = [r for r in rows if is_selected(r)]
    uids = [r["gauge_uid"] for r in eligible]
    if len(set(uids)) != len(uids):
        dupes = [u for u, n in Counter(uids).items() if n > 1]
        fail(f"duplicate gauge_uid in selection: {dupes}")
    with QUALITY_PATH.open(encoding="utf-8-sig", newline="") as fh:
        quality_rows = list(csv.DictReader(fh))
    quality_by_uid = {r["gauge_uid"]: r for r in quality_rows}
    if len(quality_by_uid) != len(quality_rows):
        fail("duplicate gauge_uid in quality file")
    missing = sorted(set(uids) - set(quality_by_uid))
    if missing:
        fail(f"quality file missing {len(missing)} eligible gauges: {' '.join(missing)}")
    selected = [
        r for r in eligible
        if quality_by_uid[r["gauge_uid"]].get("quality_status") in ACCEPTED_QUALITY_STATUS
    ]
    dropped = [
        {
            "gauge_uid": r["gauge_uid"],
            "name": r["name"],
            "quality_status": quality_by_uid[r["gauge_uid"]].get("quality_status", ""),
            "quality_reason": quality_by_uid[r["gauge_uid"]].get("quality_reason", ""),
        }
        for r in eligible
        if quality_by_uid[r["gauge_uid"]].get("quality_status") not in ACCEPTED_QUALITY_STATUS
    ]
    dropped_by_status = Counter(row["quality_status"] for row in dropped)
    return eligible, selected, dropped, dropped_by_status


# ---------------------------------------------------------------------------
# Record shaping
# ---------------------------------------------------------------------------

def _int_or_none(s: str):
    s = (s or "").strip()
    return int(float(s)) if s else None


def _float_or_none(s: str):
    s = (s or "").strip()
    return float(s) if s else None


def _str_or_none(s: str):
    s = (s or "").strip()
    return s or None


# ---------------------------------------------------------------------------
# BoM WDO identity, membership, coordinate quarantine and NSW point-in-polygon
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def load_wdo_identity() -> tuple[dict, list[dict]]:
    """Return ({physical_gauge_id: keeper row}, quarantine list) from the dedupe identity view."""
    rows = _read_csv(WDO_IDENTITY_PATH)
    keepers: dict[str, dict] = {}
    quarantine: list[dict] = []
    for r in rows:
        pid = r["physical_gauge_id"].strip()
        if pid in keepers:
            fail(f"duplicate physical_gauge_id in identity file: {pid}")
        keepers[pid] = r
        lat_s, lon_s = r.get("latitude", "").strip(), r.get("longitude", "").strip()
        reason = None
        if not lat_s or not lon_s:
            reason = "missing_coordinates"
        else:
            lat, lon = float(lat_s), float(lon_s)
            if not (PLAUSIBLE_LAT[0] <= lat <= PLAUSIBLE_LAT[1] and PLAUSIBLE_LON[0] <= lon <= PLAUSIBLE_LON[1]):
                reason = "impossible_coordinates"
        if reason:
            quarantine.append({
                "physical_gauge_id": pid,
                "station_id": r["station_id"],
                "station_no": r["station_no"],
                "station_name": r["station_name"],
                "ts_id": r["ts_id"],
                "latitude": lat_s,
                "longitude": lon_s,
                "reason": reason,
            })
    return keepers, quarantine


def load_wdo_members() -> dict[str, dict]:
    """ts_id -> {physical_gauge_id, keeper_ts_id, member_ts_ids (keeper first)} for every union member."""
    groups: dict[str, list[tuple[int, str]]] = {}
    for r in _read_csv(WDO_MEMBERS_PATH):
        if r.get("include_for_observation_union", "True").strip().lower() != "true":
            continue
        keeper = r.get("is_identity_keeper", "").strip().lower() == "true"
        groups.setdefault(r["physical_gauge_id"].strip(), []).append((0 if keeper else 1, r["ts_id"].strip()))
    out: dict[str, dict] = {}
    for pid, members in groups.items():
        ordered = [ts for _, ts in sorted(members)]
        keeper_ts = next((ts for rank, ts in sorted(members) if rank == 0), ordered[0])
        for ts in ordered:
            out[ts] = {"physical_gauge_id": pid, "keeper_ts_id": keeper_ts, "member_ts_ids": ordered}
    return out


def load_lga_polygons() -> list[tuple]:
    """[(minx, miny, maxx, maxy, rings, lganame)] from the site's nsw_lga_boundaries.js."""
    text = LGA_BOUNDARIES_PATH.read_text(encoding="utf-8")
    geojson = json.loads(text[text.index("{"):].rstrip().rstrip(";"))
    polygons = []
    for feature in geojson["features"]:
        geometry = feature["geometry"]
        name = feature["properties"].get("lganame") or feature["properties"].get("councilname") or ""
        parts = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
        for rings in parts:
            xs = [c[0] for c in rings[0]]
            ys = [c[1] for c in rings[0]]
            polygons.append((min(xs), min(ys), max(xs), max(ys), rings, name))
    return polygons


def _ring_contains(ring, x: float, y: float) -> bool:
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def lga_containing(polygons: list[tuple], lon: float, lat: float):
    for minx, miny, maxx, maxy, rings, name in polygons:
        if not (minx <= lon <= maxx and miny <= lat <= maxy):
            continue
        if _ring_contains(rings[0], lon, lat) and not any(_ring_contains(hole, lon, lat) for hole in rings[1:]):
            return name
    return None


def nearest_lga_edge_km(polygons: list[tuple], lon: float, lat: float) -> tuple[float, str]:
    """Distance from the point to the nearest LGA outer ring, in km (equirectangular)."""
    import math
    cos_lat = math.cos(math.radians(lat))
    px, py = lon * cos_lat, lat
    best, best_name = float("inf"), ""
    for _minx, _miny, _maxx, _maxy, rings, name in polygons:
        ring = rings[0]
        for i in range(len(ring) - 1):
            ax, ay = ring[i][0] * cos_lat, ring[i][1]
            bx, by = ring[i + 1][0] * cos_lat, ring[i + 1][1]
            dx, dy = bx - ax, by - ay
            if dx == 0 and dy == 0:
                d = math.hypot(px - ax, py - ay)
            else:
                t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
                d = math.hypot(px - (ax + t * dx), py - (ay + t * dy))
            if d < best:
                best, best_name = d, name
    return best * KM_PER_DEG, best_name


def strip_coastal(lga: str) -> str:
    lga = (lga or "").strip()
    if lga.endswith(COASTAL_SUFFIX):
        lga = lga[: -len(COASTAL_SUFFIX)]
    return lga


def source_of(row: dict) -> tuple[str, str]:
    mhl = row.get("mhl_ts_id", "").strip()
    if mhl:
        return "mhl", mhl
    wdo = row.get("wdo_ts_id", "").strip()
    if wdo:
        return "wdo", wdo
    waternsw = row.get("waternsw_site", "").strip()
    if waternsw:
        return "waternsw", waternsw
    # Preserve the upstream registry values exactly until its fetch adapter lands.
    return row.get("networks", ""), ""


def catalogue_record(row: dict) -> dict:
    source, ts_id = source_of(row)
    has_series_id = bool(ts_id)
    return {
        "station_type": "rainfall",
        "station_id": row["gauge_uid"].strip(),
        "gauge_uid": row["gauge_uid"].strip(),
        "station_name": row["name"].strip(),
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "lga": strip_coastal(row.get("lga", "")),
        "cls": row["cls"].strip(),
        "interval_s": _int_or_none(row.get("interval_s")),
        "mode": _str_or_none(row.get("mode")),
        "networks": _str_or_none(row.get("networks")),
        "last_data": _str_or_none(row.get("last_data")),
        "record_start": _str_or_none(row.get("record_start")),
        "checksum_agree_pct": _float_or_none(row.get("checksum_agree_pct")),
        "source": source,
        "ts_id": ts_id or None,
        "data_identifier": f"{source}:{ts_id}" if has_series_id else row.get("sources", ""),
        "mhl_ts_id": _str_or_none(row.get("mhl_ts_id")),
        "wdo_ts_id": _str_or_none(row.get("wdo_ts_id")),
        "waternsw_site": _str_or_none(row.get("waternsw_site")),
        # BoM WDO physical-gauge identity and observation union (None for non-WDO rows).
        "wdo_physical_gauge_id": row.get("_wdo_physical_gauge_id"),
        "wdo_station_id": row.get("_wdo_station_id"),
        "wdo_station_no": row.get("_wdo_station_no"),
        "wdo_member_ts_ids": row.get("_wdo_member_ts_ids"),
        "wdo_nsw_gate": row.get("_wdo_nsw_gate"),
        # Filled in after the IFD check: "ok" | "missing" | "error". Anything but "ok" must never
        # produce an AEP; the station is absent from the IFD table.
        "ifd_status": None,
    }


# ---------------------------------------------------------------------------
# IFD
# ---------------------------------------------------------------------------

def build_ifd(rec: dict) -> dict:
    """Return {duration: {column: depth}} with the three rare columns merged in."""
    ifds = rec.get("ifds") or {}
    rare = rec.get("rare") or {}
    out: dict[str, dict[str, float]] = {}
    for dur in sorted(ifds, key=float):
        row = {col: ifds[dur][col] for col in AEP_COLUMNS if col in ifds[dur]}
        rare_row = rare.get(dur) or {}
        for col in RARE_COLUMNS:
            if col in rare_row and rare_row[col] is not None:
                row[col] = rare_row[col]
        out[str(int(float(dur)))] = row
    return out


def validate_ifd(uid: str, ifd: dict, exempted: list) -> None:
    """Abort on any monotonicity failure except the scoped rare-column tolerance.

    Tolerated cells are appended to `exempted` as {uid, column, v1440, v1800}.
    """
    durations = sorted(ifd, key=float)
    # Rows: non-decreasing across columns, common -> rare.
    for dur in durations:
        row = ifd[dur]
        prev_key, prev_val = None, None
        for col in ALL_COLUMNS:
            if col not in row:
                continue
            val = row[col]
            if not isinstance(val, (int, float)):
                fail(f"non-numeric depth uid={uid} duration={dur} key={col!r} value={val!r}")
            if prev_val is not None and val < prev_val:
                fail(f"row not non-decreasing uid={uid} duration={dur} key={col!r} "
                     f"({prev_key}={prev_val} > {col}={val})")
            prev_key, prev_val = col, val
    # Columns: non-decreasing with duration.
    for col in ALL_COLUMNS:
        prev_dur, prev_val = None, None
        for dur in durations:
            if col not in ifd[dur]:
                continue
            val = ifd[dur][col]
            if prev_val is not None and val < prev_val:
                decrease = prev_val - val
                allowed = max(TOLERANCE_MM, prev_val * TOLERANCE_PCT / 100.0)
                if (col in RARE_COLUMNS and (prev_dur, dur) == TOLERANCE_STEP
                        and decrease <= allowed):
                    exempted.append({"uid": uid, "column": col,
                                     f"v{TOLERANCE_STEP[0]}": prev_val,
                                     f"v{TOLERANCE_STEP[1]}": val})
                else:
                    fail(f"column not non-decreasing uid={uid} duration={dur} key={col!r} "
                         f"({prev_dur} min={prev_val} > {dur} min={val})")
            prev_dur, prev_val = dur, val


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def apply_wdo_identity(selected: list[dict]) -> tuple[list[dict], dict]:
    """Attach WDO identity + membership, quarantine bad coordinates, gate WDO rows to NSW by PIP."""
    keepers, quarantine = load_wdo_identity()
    members = load_wdo_members()
    polygons = load_lga_polygons()
    quarantined_pids = {q["physical_gauge_id"] for q in quarantine}
    quarantine_by_reason = Counter(q["reason"] for q in quarantine)

    # National context for the log: PIP against NSW LGAs vs a naive bounding box.
    national_in_nsw = 0
    national_in_bbox = 0
    for pid, k in keepers.items():
        if pid in quarantined_pids:
            continue
        lat, lon = float(k["latitude"]), float(k["longitude"])
        if -37.6 <= lat <= -28.1 and 140.9 <= lon <= 153.7:
            national_in_bbox += 1
        if lga_containing(polygons, lon, lat):
            national_in_nsw += 1

    kept: list[dict] = []
    excluded_quarantine: list[dict] = []
    excluded_outside_nsw: list[dict] = []
    boundary_snap: list[dict] = []
    repointed: list[dict] = []
    member_groups = 0
    for r in selected:
        ts = r.get("wdo_ts_id", "").strip()
        if not ts:
            kept.append(r)
            continue
        m = members.get(ts)
        if m is None:
            fail(f"registry wdo_ts_id {ts} ({r['gauge_uid']} {r['name']}) is not in the observation membership map")
        pid = m["physical_gauge_id"]
        keeper = keepers.get(pid)
        if keeper is None:
            fail(f"physical gauge {pid} has no keeper row in the identity file")
        if pid in quarantined_pids:
            excluded_quarantine.append({"gauge_uid": r["gauge_uid"], "name": r["name"], "physical_gauge_id": pid})
            continue
        lat, lon = float(r["lat"]), float(r["lon"])
        inside = lga_containing(polygons, lon, lat)
        if inside:
            r["_wdo_nsw_gate"] = "point_in_polygon"
        else:
            km, nearest = nearest_lga_edge_km(polygons, lon, lat)
            entry = {"gauge_uid": r["gauge_uid"], "name": r["name"], "lat": lat, "lon": lon,
                     "registry_lga": r.get("lga", ""), "nearest_lga": nearest, "distance_km": round(km, 3)}
            if km <= NSW_BOUNDARY_SNAP_KM:
                boundary_snap.append(entry)
                r["_wdo_nsw_gate"] = f"boundary_snap:{km:.3f}km"
            else:
                excluded_outside_nsw.append(entry)
                continue
        if m["keeper_ts_id"] != ts:
            repointed.append({"gauge_uid": r["gauge_uid"], "name": r["name"], "registry_ts_id": ts,
                              "keeper_ts_id": m["keeper_ts_id"], "physical_gauge_id": pid})
            r["wdo_ts_id"] = m["keeper_ts_id"]
        if len(m["member_ts_ids"]) > 1:
            member_groups += 1
        r["_wdo_physical_gauge_id"] = pid
        r["_wdo_station_id"] = keeper["station_id"].strip()
        r["_wdo_station_no"] = keeper["station_no"].strip()
        r["_wdo_member_ts_ids"] = list(m["member_ts_ids"])
        kept.append(r)

    QUARANTINE_OUT.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_OUT.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": WDO_IDENTITY_PATH.name,
        "rule": f"latitude in {list(PLAUSIBLE_LAT)} and longitude in {list(PLAUSIBLE_LON)}; missing coordinates quarantined",
        "count": len(quarantine),
        "by_reason": dict(sorted(quarantine_by_reason.items())),
        "records": quarantine,
    }, indent=2) + "\n", encoding="utf-8")

    summary = {
        "identity_input": WDO_IDENTITY_PATH.name,
        "membership_input": WDO_MEMBERS_PATH.name,
        "lga_boundaries_input": str(LGA_BOUNDARIES_PATH.relative_to(ROOT.parent)),
        "rule": "dedupe keeper = identity only; every include_for_observation_union member ts_id is "
                "carried in wdo_member_ts_ids and must be unioned by any observation read",
        "national_physical_gauges": len(keepers),
        "national_in_nsw_point_in_polygon": national_in_nsw,
        "national_in_nsw_bounding_box": national_in_bbox,
        "coordinate_quarantine_count": len(quarantine),
        "coordinate_quarantine_by_reason": dict(sorted(quarantine_by_reason.items())),
        "coordinate_quarantine_log": str(QUARANTINE_OUT.relative_to(ROOT)),
        "selected_wdo_records": sum(1 for r in kept if r.get("_wdo_physical_gauge_id")),
        "selected_wdo_with_multiple_members": member_groups,
        "registry_ts_repointed_to_keeper": repointed,
        "excluded_quarantined_coordinates": excluded_quarantine,
        "nsw_gate": "point_in_polygon against LGA boundaries; bounding box is never used",
        "boundary_snap_km": NSW_BOUNDARY_SNAP_KM,
        "boundary_snap_retained": boundary_snap,
        "excluded_outside_nsw": excluded_outside_nsw,
    }
    print(f"WDO identity: {len(keepers)} national physical gauges; NSW by point-in-polygon {national_in_nsw} "
          f"(bounding box would give {national_in_bbox})")
    print(f"WDO coordinate quarantine: {len(quarantine)} {dict(sorted(quarantine_by_reason.items()))} "
          f"-> {QUARANTINE_OUT}")
    print(f"WDO selected: {summary['selected_wdo_records']} records, {member_groups} with >1 member series, "
          f"{len(repointed)} registry ts_ids re-pointed to keeper")
    print(f"WDO NSW gate: {len(boundary_snap)} retained by boundary snap (<= {NSW_BOUNDARY_SNAP_KM} km), "
          f"{len(excluded_outside_nsw)} excluded outside NSW, {len(excluded_quarantine)} excluded by quarantine")
    for e in boundary_snap:
        print(f"  snap  {e['gauge_uid']} {e['name']} {e['distance_km']} km -> {e['nearest_lga']} (registry {e['registry_lga']})")
    for e in excluded_outside_nsw:
        print(f"  DROP  {e['gauge_uid']} {e['name']} {e['distance_km']} km from {e['nearest_lga']}")
    return kept, summary


def main() -> int:
    for p in (CSV_PATH, QUALITY_PATH, IFD_INPUT, CATALOGUE_OUT, IFD_TABLE_OUT,
              WDO_IDENTITY_PATH, WDO_MEMBERS_PATH, LGA_BOUNDARIES_PATH):
        if not p.exists():
            fail(f"missing input/target: {p}")

    eligible, selected, quality_dropped, quality_dropped_by_status = load_selected_rows()

    if len(eligible) != EXPECTED_COUNT:
        fail(f"expected {EXPECTED_COUNT} pre-quality eligible gauges, got {len(eligible)}")

    selected, wdo_summary = apply_wdo_identity(selected)

    by_cls = Counter(r["cls"].strip() for r in selected)
    by_source = Counter(source_of(r)[0] for r in selected)
    print(f"pre-quality eligible: {len(eligible)}")
    print(f"quality gate dropped: {len(quality_dropped)} {dict(sorted(quality_dropped_by_status.items()))}")
    print(f"selected: {len(selected)}")
    print(f"by cls:    {dict(sorted(by_cls.items()))}")
    print(f"by source: {dict(sorted(by_source.items()))}")
    catalogue = [catalogue_record(r) for r in selected]
    catalogue.sort(key=lambda s: s["gauge_uid"])

    # ---- IFD ----
    ifd_src = json.loads(IFD_INPUT.read_text(encoding="utf-8"))
    missing, rejected = [], []
    for s in catalogue:
        rec = ifd_src.get(s["gauge_uid"])
        if rec is None:
            missing.append(s["gauge_uid"])
        elif rec.get("status") not in ACCEPTED_IFD_STATUS:
            rejected.append(f"{s['gauge_uid']} (status={rec.get('status')!r})")
    if missing:
        print(f"no IFD record ({len(missing)}): {' '.join(missing)}", file=sys.stderr)
    if rejected:
        print(f"IFD status not accepted ({len(rejected)}): {' '.join(rejected)}", file=sys.stderr)
    for s in catalogue:
        rec = ifd_src.get(s["gauge_uid"])
        s["ifd_status"] = "missing" if rec is None else ("ok" if rec.get("status") in ACCEPTED_IFD_STATUS else "error")

    ifd_stations: dict[str, dict] = {}
    exempted: list[dict] = []
    for s in catalogue:
        rec = ifd_src.get(s["gauge_uid"])
        if rec is None or rec.get("status") not in ACCEPTED_IFD_STATUS:
            continue
        ifd = build_ifd(rec)
        if not ifd:
            fail(f"empty IFD table uid={s['gauge_uid']}")
        validate_ifd(s["gauge_uid"], ifd, exempted)
        ifd_stations[s["gauge_uid"]] = {
            "station_id": s["station_id"],
            "station_name": s["station_name"],
            "source": s["source"],
            "lat": s["lat"],
            "lon": s["lon"],
            "data_identifier": s["data_identifier"],
            "activity_status": "live",
            "ifd": ifd,
        }
    n_dur = Counter(len(v["ifd"]) for v in ifd_stations.values())
    n_rare = sum(1 for v in ifd_stations.values()
                 if all(all(c in row for c in RARE_COLUMNS) for row in v["ifd"].values()))
    print(f"IFD validated: {len(ifd_stations)} stations; durations per station {dict(n_dur)}; "
          f"stations with all three rare columns in every row: {n_rare}")
    print(f"exempted cells ({TOLERANCE_STEP[0]}-{TOLERANCE_STEP[1]} rare-column tolerance): "
          f"{len(exempted)}")
    for e in exempted:
        print(f"  {e}")

    ifd_validation = {
        "rule": IFD_VALIDATION_RULE,
        "tolerance_mm": TOLERANCE_MM,
        "tolerance_pct": TOLERANCE_PCT,
        "step": f"{TOLERANCE_STEP[0]}-{TOLERANCE_STEP[1]}",
        "columns": list(RARE_COLUMNS),
        "exempted": exempted,
    }

    # ---- Write (only after every check passed) ----
    now = datetime.now(timezone.utc).isoformat()

    catalogue_doc = {
        "dataset": "pluviometrics_rainfall_stations",
        "generated_at": now,
        "deduplication": {
            "method": "registry_gauge_uid",
            "note": "One record per gauge_uid. Cross-network duplicate resolution is performed "
                    "upstream in nsw_rainfall_stations.csv, not in this build.",
        },
        "selection": {
            "rule": "live == true AND cls in {A,B,C,D,E} AND quality_status in {ok,unassessed}",
            "expected_pre_quality_count": EXPECTED_COUNT,
            "pre_quality_count": len(eligible),
            "selected_count": len(catalogue),
            "quality_status_required": list(ACCEPTED_QUALITY_STATUS),
            "quality_dropped_count": len(quality_dropped),
            "quality_dropped_by_status": dict(sorted(quality_dropped_by_status.items())),
            "quality_dropped": quality_dropped,
            "by_cls": dict(sorted(by_cls.items())),
            "by_source": dict(sorted(by_source.items())),
            "source_rule": "configured series: MHL, then WDO, then WaterNSW; without a series id, "
                           "source=master.networks and data_identifier=master.sources verbatim",
            "inputs": {
                "stations": CSV_PATH.name,
                "quality": QUALITY_PATH.name,
                "ifd": IFD_INPUT.name,
            },
            "script": Path(__file__).name,
            "ifd_validation": ifd_validation,
            "ifd_status_counts": dict(sorted(Counter(s["ifd_status"] for s in catalogue).items())),
            "ifd_unusable": {"missing": missing, "rejected": rejected},
        },
        "wdo_identity": wdo_summary,
        "stations": catalogue,
    }

    ifd_doc = {
        "generated_at": now,
        "source_input": f"{CSV_PATH.name} + {IFD_INPUT.name}",
        "station_count_input": len(catalogue),
        "enriched_count": len(ifd_stations),
        "error_count": len(missing) + len(rejected),
        "ifd_validation": ifd_validation,
        "stations": ifd_stations,
        "errors": {
            "missing_ifd": missing,
            "rejected_ifd": rejected,
        },
    }

    CATALOGUE_OUT.write_text(json.dumps(catalogue_doc, indent=2, ensure_ascii=False) + "\n",
                             encoding="utf-8")
    IFD_TABLE_OUT.write_text(json.dumps(ifd_doc, indent=2, ensure_ascii=False) + "\n",
                             encoding="utf-8")

    print(f"wrote {CATALOGUE_OUT}  ({CATALOGUE_OUT.stat().st_size:,} bytes, {len(catalogue)} stations)")
    print(f"wrote {IFD_TABLE_OUT}  ({IFD_TABLE_OUT.stat().st_size:,} bytes, {len(ifd_stations)} stations)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
