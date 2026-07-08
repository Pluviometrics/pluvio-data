#!/usr/bin/env node
// build_radar_bom.mjs — Radar v2: 250 m / 5-minute rainfall accumulation for
// the Northern Beaches AOI from the BoM Terrey Hills 64 km radar (IDR714).
//
// BoM's anonymous FTP publishes IDR714.T.<YYYYMMDDHHMM>.png every ~5 minutes
// but retains only ~2 hours, so this script must run at least hourly. Each
// new frame's colour classes are decoded to rain rates (mm/h), converted to
// depth (rate x 5/60), resampled to the SAME 256x256 AOI grid as the Lizard
// pipeline (radar/nb/), and accumulated into per-Sydney-day files under
// radar/nb2/. Windows here are TRUE Sydney midnights (5-min frames don't need
// the 3-hourly snapping the Lizard product does).
//
// Absolute rates are class midpoints from the standard BoM radar legend; the
// Stormgauge client applies mean-field-bias gauge calibration on top, so the
// relative (log-spaced) ladder matters more than the absolute values.
//
// Rules:
//   - Anonymous GET/FTP only; frames fetched once (dedup via frames list).
//   - Missing frames are visible via frames_used vs frames_expected —
//     never zero-filled.
//   - Unknown opaque colours are counted and logged, never guessed.
//
// Usage: node scripts/build_radar_bom.mjs

import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { PNG } = require('pngjs');

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const OUT_DIR = path.join(ROOT, 'radar', 'nb2');
const DAILY_DIR = path.join(OUT_DIR, 'daily');

const FTP_DIR = 'ftp://ftp.bom.gov.au/anon/gen/radar/';
const PRODUCT = 'IDR714';
// Terrey Hills radar site; IDR714 image spans +/-64 km around it.
const RADAR_LAT = -33.7008, RADAR_LON = 151.2094;
const IMG_SIZE = 512, RANGE_KM = 64;
const KM_PER_DEG_LAT = 111.132;
const KM_PER_DEG_LON = 111.320 * Math.cos(RADAR_LAT * Math.PI / 180);

// Identical to the Lizard pipeline so daily files from both are summable.
const AOI_BBOX = { minLon: 151.15, minLat: -33.85, maxLon: 151.40, maxLat: -33.55 };
const GRID_SIZE = 256;
const FRAME_MINUTES = 5;
const SCHEMA_VERSION = 'pluviometrics.radar_accumulation.v2_bom';

// Standard BoM radar intensity legend, light -> extreme (mm/h midpoints).
const BOM_RATE_LUT = new Map([
  ['245,245,255', 0.2], ['180,180,255', 0.5], ['120,120,255', 1.5], ['20,20,255', 2.5],
  ['0,216,195', 4], ['0,150,144', 6], ['0,102,102', 10], ['255,255,0', 15],
  ['255,200,0', 20], ['255,150,0', 35], ['255,100,0', 50], ['255,0,0', 80],
  ['200,0,0', 120], ['120,0,0', 200], ['40,0,0', 360]
]);

// ---------------------------------------------------------------------------
// Sydney time helpers
// ---------------------------------------------------------------------------

function sydneyOffsetMs(utcMs) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Australia/Sydney', timeZoneName: 'longOffset'
  }).formatToParts(new Date(utcMs));
  const name = parts.find(p => p.type === 'timeZoneName')?.value || 'GMT+10:00';
  const m = name.match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/);
  const sign = m[1] === '-' ? -1 : 1;
  return sign * ((Number(m[2]) * 60 + Number(m[3] || 0)) * 60 * 1000);
}

function sydneyDateString(utcMs) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(new Date(utcMs));
}

// True Sydney midnight (no frame snapping — 5-min cadence).
function sydneyMidnightUtcMs(dateStr) {
  const [y, mo, d] = dateStr.split('-').map(Number);
  let guess = Date.UTC(y, mo - 1, d) - 10 * 3600 * 1000;
  return Date.UTC(y, mo - 1, d) - sydneyOffsetMs(guess);
}

function addDays(dateStr, n) {
  const [y, mo, d] = dateStr.split('-').map(Number);
  return new Date(Date.UTC(y, mo - 1, d) + n * 86400000).toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// FTP fetch + frame decode
// ---------------------------------------------------------------------------

function curl(url, binary = false) {
  return execFileSync('curl', ['-s', '--max-time', '60', url],
    { maxBuffer: 32 * 1024 * 1024, encoding: binary ? 'buffer' : 'utf8' });
}

function listFrames() {
  const listing = curl(FTP_DIR);
  const re = new RegExp(`${PRODUCT}\\.T\\.(\\d{12})\\.png`, 'g');
  const stamps = new Set();
  let m;
  while ((m = re.exec(listing))) stamps.add(m[1]);
  return [...stamps].sort();
}

function stampToUtcMs(stamp) {
  return Date.UTC(+stamp.slice(0, 4), +stamp.slice(4, 6) - 1, +stamp.slice(6, 8),
    +stamp.slice(8, 10), +stamp.slice(10, 12));
}

const unknownColours = new Map();

// Decode one frame and resample to the AOI grid as mm of rain in 5 minutes.
function frameToAoiGrid(pngBuffer) {
  const img = PNG.sync.read(pngBuffer);
  if (img.width !== IMG_SIZE || img.height !== IMG_SIZE) {
    throw new Error(`Unexpected radar image size ${img.width}x${img.height}`);
  }
  const out = new Float64Array(GRID_SIZE * GRID_SIZE);
  const lonStep = (AOI_BBOX.maxLon - AOI_BBOX.minLon) / GRID_SIZE;
  const latStep = (AOI_BBOX.maxLat - AOI_BBOX.minLat) / GRID_SIZE;
  const pxPerKm = IMG_SIZE / (2 * RANGE_KM);
  for (let r = 0; r < GRID_SIZE; r++) {
    const lat = AOI_BBOX.maxLat - (r + 0.5) * latStep;
    const dyKm = (lat - RADAR_LAT) * KM_PER_DEG_LAT;
    const py = Math.round(IMG_SIZE / 2 - dyKm * pxPerKm);
    for (let c = 0; c < GRID_SIZE; c++) {
      const lon = AOI_BBOX.minLon + (c + 0.5) * lonStep;
      const dxKm = (lon - RADAR_LON) * KM_PER_DEG_LON;
      const px = Math.round(IMG_SIZE / 2 + dxKm * pxPerKm);
      if (px < 0 || px >= IMG_SIZE || py < 0 || py >= IMG_SIZE) continue;
      const o = (py * IMG_SIZE + px) * 4;
      if (img.data[o + 3] === 0) continue;               // transparent = no echo
      const key = `${img.data[o]},${img.data[o + 1]},${img.data[o + 2]}`;
      const rate = BOM_RATE_LUT.get(key);
      if (rate === undefined) {
        unknownColours.set(key, (unknownColours.get(key) || 0) + 1);
        continue;
      }
      out[r * GRID_SIZE + c] = rate * FRAME_MINUTES / 60;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Day accumulation files
// ---------------------------------------------------------------------------

function writeJson(file, obj) {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, JSON.stringify(obj));
}

function loadDay(date) {
  const file = path.join(DAILY_DIR, `${date}.json`);
  if (existsSync(file)) {
    try { return JSON.parse(readFileSync(file, 'utf8')); } catch { /* rebuild */ }
  }
  const startMs = sydneyMidnightUtcMs(date);
  const endMs = sydneyMidnightUtcMs(addDays(date, 1));
  return {
    schema_version: SCHEMA_VERSION,
    date,
    window_start_utc: new Date(startMs).toISOString(),
    window_end_utc: new Date(endMs).toISOString(),
    frames_expected: Math.round((endMs - startMs) / 60000 / FRAME_MINUTES),
    frames_used: 0,
    frames_last_used_utc: null,
    frames: [],
    values_mm: new Array(GRID_SIZE * GRID_SIZE).fill(0)
  };
}

function main() {
  const stamps = listFrames();
  if (!stamps.length) throw new Error('No IDR714 frames in FTP listing');
  console.log(`FTP listing: ${stamps.length} frames (${stamps[0]} .. ${stamps[stamps.length - 1]})`);

  const touched = new Map();   // date -> day object
  let fetched = 0;

  for (const stamp of stamps) {
    const utcMs = stampToUtcMs(stamp);
    const date = sydneyDateString(utcMs);
    const day = touched.get(date) || loadDay(date);
    touched.set(date, day);
    if (day.frames.includes(stamp)) continue;

    let grid;
    try {
      grid = frameToAoiGrid(curl(`${FTP_DIR}${PRODUCT}.T.${stamp}.png`, true));
    } catch (e) {
      console.error(`frame ${stamp}: ${e.message}`);
      continue;
    }
    for (let i = 0; i < grid.length; i++) {
      day.values_mm[i] = Math.round((day.values_mm[i] + grid[i]) * 1000) / 1000;
    }
    day.frames.push(stamp);
    day.frames.sort();
    day.frames_used = day.frames.length;
    day.frames_first_used_utc = new Date(stampToUtcMs(day.frames[0])).toISOString();
    day.frames_last_used_utc = new Date(stampToUtcMs(day.frames[day.frames.length - 1])).toISOString();
    fetched++;
  }

  for (const [date, day] of touched) {
    writeJson(path.join(DAILY_DIR, `${date}.json`), day);
    console.log(`${date}: frames_used=${day.frames_used}/${day.frames_expected}`);
  }

  // today.json mirrors the current Sydney day for cheap freshness checks.
  const today = sydneyDateString(Date.now());
  const todayDay = touched.get(today) || loadDay(today);
  writeJson(path.join(OUT_DIR, 'today.json'), {
    ...todayDay,
    as_of_utc: new Date().toISOString()
  });

  writeJson(path.join(OUT_DIR, 'metadata.json'), {
    schema_version: SCHEMA_VERSION,
    generated_at: new Date().toISOString(),
    source: 'BoM Terrey Hills 64 km radar (IDR714), colour-class decoded, ~250 m native',
    source_ftp: `${FTP_DIR}${PRODUCT}.T.<YYYYMMDDHHMM>.png`,
    bbox: [AOI_BBOX.minLon, AOI_BBOX.minLat, AOI_BBOX.maxLon, AOI_BBOX.maxLat],
    leaflet_bounds: [[AOI_BBOX.minLat, AOI_BBOX.minLon], [AOI_BBOX.maxLat, AOI_BBOX.maxLon]],
    grid_rows: GRID_SIZE,
    grid_cols: GRID_SIZE,
    row_order: 'north_to_south',
    frame_interval_minutes: FRAME_MINUTES,
    day_boundary_rule: 'True Sydney midnight',
    units: 'mm accumulated over the window (class-midpoint rates x 5 min)',
    warning: 'Colour-class radar QPE. Absolute depths need gauge calibration; the Stormgauge client applies mean-field bias. Gauge records remain authoritative.'
  });

  const dates = readdirSync(DAILY_DIR).filter(f => /^\d{4}-\d{2}-\d{2}\.json$/.test(f))
    .map(f => f.slice(0, 10)).sort();
  writeJson(path.join(OUT_DIR, 'index.json'), { updated_at: new Date().toISOString(), dates });

  if (unknownColours.size) {
    console.warn('Unknown opaque colours (colour: px count):',
      JSON.stringify([...unknownColours.entries()].slice(0, 10)));
  }
  console.log(`Done. ${fetched} new frames ingested; ${dates.length} daily files.`);
}

main();
