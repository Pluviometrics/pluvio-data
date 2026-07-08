#!/usr/bin/env node
// build_radar_accumulation.mjs — Lizard radar rainfall accumulation for the
// Northern Beaches AOI, published as small per-day JSON grids.
//
// For each Sydney calendar day (approximated to the nearest 3-hourly radar
// frame boundary, ±90 min), fetches the AOI-clipped Precipitation Australia
// frames from the Northern Beaches Lizard tenant, sums them per cell, and
// writes:
//
//   radar/nb/metadata.json        grid + methodology (rewritten each run)
//   radar/nb/daily/YYYY-MM-DD.json  one finalised Sydney day per file
//   radar/nb/today.json           running total since the last day boundary
//   radar/nb/index.json           list of available daily files
//
// Grid: native 256x256 frames are downsampled (2x2 mean, nodata-aware) to
// 128x128, row-major from the north-west corner. Cells with zero valid
// samples across the window are null — never zero-filled.
//
// Rules carried over from the Stormgauge radar tooling:
//   - GET only, anonymous; polite delay between requests; 429/5xx backoff.
//   - Frames newer than now-15min are excluded (the feed carries a
//     nowcast/forecast tail that must not be summed as observation).
//   - Missing frames are reported in frames_missing, never silently filled.
//
// Usage:
//   node scripts/build_radar_accumulation.mjs                 # today + finalise yesterday
//   node scripts/build_radar_accumulation.mjs --backfill-days 30

import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from 'node:fs';
import { inflateSync } from 'node:zlib';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const OUT_DIR = path.join(ROOT, 'radar', 'nb');
const DAILY_DIR = path.join(OUT_DIR, 'daily');

const BASE_URL = 'https://northernbeaches.lizard.net/api/v4';
const RASTER_UUID = '1b6c03df-2ad1-4f17-89f6-319ea797b357';
const AOI_BBOX = { minLon: 151.15, minLat: -33.85, maxLon: 151.40, maxLat: -33.55 };
const NODATA_VALUE = -32767;

const NATIVE_SIZE = 256;      // what the API returns for the AOI bbox
// Published at native resolution. Probed 2026-07-09: requesting 512 returns
// 99.2% duplicated 2x2 blocks, so 256 is the source's real information ceiling.
const GRID_SIZE = 256;
const FRAME_INTERVAL_MS = 3 * 3600 * 1000;
const OBSERVATION_LAG_MS = 15 * 60 * 1000;   // ignore frames newer than now-15min
const FETCH_DELAY_MS = 250;
const FETCH_RETRIES = 3;
const SCHEMA_VERSION = 'pluviometrics.radar_accumulation.v1';

// ---------------------------------------------------------------------------
// GeoTIFF parsing — vendored from Stormgauge
// src/modules/radar/radarCumulativeRainfall.js (single-band Float32, DEFLATE)
// ---------------------------------------------------------------------------

const TIFF_TYPE_SIZE = { 1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 11: 4, 12: 8 };

function parseIfd(dv, ifdOffset, little) {
  const num = dv.getUint16(ifdOffset, little);
  const tags = {};
  for (let i = 0; i < num; i++) {
    const e = ifdOffset + 2 + i * 12;
    const tag = dv.getUint16(e, little);
    const type = dv.getUint16(e + 2, little);
    const count = dv.getUint32(e + 4, little);
    const sz = (TIFF_TYPE_SIZE[type] || 0) * count;
    const valOffset = sz > 4 ? dv.getUint32(e + 8, little) : (e + 8);
    tags[tag] = { type, count, valOffset };
  }
  return tags;
}

const readU16 = (dv, tag, little) => dv.getUint16(tag.valOffset, little);

function readU32Array(dv, tag, little) {
  const out = new Uint32Array(tag.count);
  for (let i = 0; i < tag.count; i++) out[i] = dv.getUint32(tag.valOffset + i * 4, little);
  return out;
}

function parseLizardGeoTiff(buffer) {
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const order = String.fromCharCode(buf[0], buf[1]);
  const little = order === 'II';
  if (!little && order !== 'MM') throw new Error(`Unrecognised TIFF byte order: ${order}`);
  if (dv.getUint16(2, little) !== 42) throw new Error('Unsupported TIFF version (expected classic TIFF)');

  const tags = parseIfd(dv, dv.getUint32(4, little), little);
  const width = readU16(dv, tags[256], little);
  const height = readU16(dv, tags[257], little);
  const bps = readU16(dv, tags[258], little);
  const compression = readU16(dv, tags[259], little);
  const samplesPerPixel = readU16(dv, tags[277], little);
  const rowsPerStrip = readU16(dv, tags[278], little);
  const sampleFormat = readU16(dv, tags[339], little);
  const predictor = tags[317] ? readU16(dv, tags[317], little) : 1;

  if (bps !== 32 || sampleFormat !== 3) throw new Error(`Unsupported sample format bps=${bps} fmt=${sampleFormat}`);
  if (samplesPerPixel !== 1) throw new Error(`Unsupported samples per pixel: ${samplesPerPixel}`);
  if (compression !== 8) throw new Error(`Unsupported compression: ${compression} (expected DEFLATE)`);
  if (predictor !== 1) throw new Error(`Unsupported TIFF predictor: ${predictor}`);

  const stripOffsets = readU32Array(dv, tags[273], little);
  const stripByteCounts = readU32Array(dv, tags[279], little);

  const values = new Float32Array(width * height);
  for (let s = 0; s < stripOffsets.length; s++) {
    const raw = inflateSync(buf.subarray(stripOffsets[s], stripOffsets[s] + stripByteCounts[s]));
    const f32 = new Float32Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength));
    const rowStart = s * rowsPerStrip;
    const rowsInStrip = Math.min(rowsPerStrip, height - rowStart);
    for (let r = 0; r < rowsInStrip; r++) {
      values.set(f32.subarray(r * width, (r + 1) * width), (rowStart + r) * width);
    }
  }
  return { width, height, values };
}

// ---------------------------------------------------------------------------
// Sydney day boundaries on the 3-hourly frame grid
// ---------------------------------------------------------------------------

function sydneyOffsetMs(utcMs) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Australia/Sydney', timeZoneName: 'longOffset'
  }).formatToParts(new Date(utcMs));
  const name = parts.find(p => p.type === 'timeZoneName')?.value || 'GMT+10:00';
  const m = name.match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/);
  if (!m) throw new Error(`Cannot parse Sydney offset from "${name}"`);
  const sign = m[1] === '-' ? -1 : 1;
  return sign * ((Number(m[2]) * 60 + Number(m[3] || 0)) * 60 * 1000);
}

function sydneyDateString(utcMs) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Australia/Sydney', year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(new Date(utcMs));
}

// UTC ms of Sydney midnight for a YYYY-MM-DD Sydney date, snapped to the
// nearest 3-hourly frame time (frames land at 00/03/../21Z).
function dayBoundaryUtcMs(dateStr) {
  const [y, mo, d] = dateStr.split('-').map(Number);
  let guess = Date.UTC(y, mo - 1, d) - 10 * 3600 * 1000;
  guess = Date.UTC(y, mo - 1, d) - sydneyOffsetMs(guess); // refine for DST
  return Math.round(guess / FRAME_INTERVAL_MS) * FRAME_INTERVAL_MS;
}

function addDays(dateStr, n) {
  const [y, mo, d] = dateStr.split('-').map(Number);
  const t = Date.UTC(y, mo - 1, d) + n * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchFrame(tsIso) {
  const params = new URLSearchParams({
    bbox: `${AOI_BBOX.minLon},${AOI_BBOX.minLat},${AOI_BBOX.maxLon},${AOI_BBOX.maxLat}`,
    start: tsIso, stop: tsIso,
    format: 'geotiff', projection: 'EPSG:4326'
  });
  const url = `${BASE_URL}/rasters/${RASTER_UUID}/data/?${params}`;
  for (let attempt = 1; attempt <= FETCH_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { headers: { 'User-Agent': 'pluviometrics-radar-accumulation/1.0' } });
      if (resp.status === 429 || resp.status >= 500) {
        await sleep(1500 * attempt);
        continue;
      }
      if (!resp.ok) return null; // 404 etc — frame missing
      const bytes = Buffer.from(await resp.arrayBuffer());
      if (!bytes.length) return null;
      return parseLizardGeoTiff(bytes);
    } catch (e) {
      if (attempt === FETCH_RETRIES) {
        console.error(`  frame ${tsIso}: ${e.message}`);
        return null;
      }
      await sleep(1500 * attempt);
    }
  }
  return null;
}

// Frame -> publication grid as Float64Array with NaN for invalid pixels.
// Passes native-resolution frames through; block-averages (nodata-aware) if
// the frame is an exact multiple of the grid size.
function toGrid(frame) {
  if (frame.width !== frame.height || frame.width % GRID_SIZE !== 0) {
    throw new Error(`Unexpected frame size ${frame.width}x${frame.height} (grid ${GRID_SIZE})`);
  }
  const k = frame.width / GRID_SIZE;
  const out = new Float64Array(GRID_SIZE * GRID_SIZE).fill(NaN);
  for (let r = 0; r < GRID_SIZE; r++) {
    for (let c = 0; c < GRID_SIZE; c++) {
      let sum = 0, n = 0;
      for (let dr = 0; dr < k; dr++) {
        for (let dc = 0; dc < k; dc++) {
          const v = frame.values[(r * k + dr) * frame.width + (c * k + dc)];
          if (v !== NODATA_VALUE && Number.isFinite(v) && v >= 0) { sum += v; n++; }
        }
      }
      if (n > 0) out[r * GRID_SIZE + c] = sum / n;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Window accumulation
// ---------------------------------------------------------------------------

function frameTimesInWindow(startMs, endMs) {
  const times = [];
  for (let t = startMs + FRAME_INTERVAL_MS; t <= endMs; t += FRAME_INTERVAL_MS) times.push(t);
  return times;
}

async function accumulateWindow(startMs, endMs, label) {
  const clampMs = Date.now() - OBSERVATION_LAG_MS;
  const expected = frameTimesInWindow(startMs, endMs);
  const usable = expected.filter(t => t <= clampMs);

  const sums = new Float64Array(GRID_SIZE * GRID_SIZE);
  const counts = new Uint16Array(GRID_SIZE * GRID_SIZE);
  const used = [];
  const missing = [];

  for (const t of usable) {
    const tsIso = new Date(t).toISOString().replace('.000Z', 'Z');
    const frame = await fetchFrame(tsIso);
    await sleep(FETCH_DELAY_MS);
    if (!frame) { missing.push(tsIso); continue; }
    const grid = toGrid(frame);
    for (let i = 0; i < grid.length; i++) {
      if (!Number.isNaN(grid[i])) { sums[i] += grid[i]; counts[i]++; }
    }
    used.push(tsIso);
  }

  const values = new Array(GRID_SIZE * GRID_SIZE);
  for (let i = 0; i < values.length; i++) {
    values[i] = counts[i] > 0 ? Math.round(sums[i] * 100) / 100 : null;
  }
  console.log(`${label}: frames expected=${expected.length} usable=${usable.length} used=${used.length} missing=${missing.length}`);
  return {
    window_start_utc: new Date(startMs).toISOString(),
    window_end_utc: new Date(endMs).toISOString(),
    frames_expected: expected.length,
    frames_used: used.length,
    frames_last_used_utc: used.length ? used[used.length - 1] : null,
    frames_missing: missing,
    values_mm: values
  };
}


// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

function writeJson(file, obj) {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, JSON.stringify(obj));
}

function writeMetadata() {
  writeJson(path.join(OUT_DIR, 'metadata.json'), {
    schema_version: SCHEMA_VERSION,
    generated_at: new Date().toISOString(),
    source: 'Lizard "Precipitation Australia" raster, Northern Beaches tenant',
    source_api: `${BASE_URL}/rasters/${RASTER_UUID}/data/`,
    bbox: [AOI_BBOX.minLon, AOI_BBOX.minLat, AOI_BBOX.maxLon, AOI_BBOX.maxLat],
    leaflet_bounds: [[AOI_BBOX.minLat, AOI_BBOX.minLon], [AOI_BBOX.maxLat, AOI_BBOX.maxLon]],
    grid_rows: GRID_SIZE,
    grid_cols: GRID_SIZE,
    row_order: 'north_to_south',
    frame_interval_hours: 3,
    day_boundary_rule: 'Sydney midnight snapped to the nearest 3-hourly UTC frame time (max deviation ~90 min)',
    units: 'mm accumulated over the window; null = no valid radar sample',
    warning: 'Uncalibrated radar-derived rainfall. Visual/screening aid only — gauge records remain authoritative. Not an AEP or design-rainfall product.'
  });
}

function rebuildIndex() {
  const dates = existsSync(DAILY_DIR)
    ? readdirSync(DAILY_DIR).filter(f => /^\d{4}-\d{2}-\d{2}\.json$/.test(f)).map(f => f.slice(0, 10)).sort()
    : [];
  writeJson(path.join(OUT_DIR, 'index.json'), { updated_at: new Date().toISOString(), dates });
  return dates.length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const argIdx = process.argv.indexOf('--backfill-days');
  const backfillDays = argIdx > -1 ? Math.max(0, Math.min(365, Number(process.argv[argIdx + 1]) || 0)) : 0;

  const today = sydneyDateString(Date.now());

  // Full days: backfill window plus yesterday (finalised every run — a daily
  // file written mid-day by an earlier run would otherwise stay partial).
  const fullDays = new Set([addDays(today, -1)]);
  for (let n = 1; n <= backfillDays; n++) fullDays.add(addDays(today, -n));

  for (const date of [...fullDays].sort()) {
    const startMs = dayBoundaryUtcMs(date);
    const endMs = dayBoundaryUtcMs(addDays(date, 1));
    const file = path.join(DAILY_DIR, `${date}.json`);
    // Skip only if an existing file used every expected frame AND matches the
    // current grid resolution (resolution changes force a rebuild).
    if (existsSync(file)) {
      try {
        const prev = JSON.parse(readFileSync(file, 'utf8'));
        if (prev.frames_used === prev.frames_expected &&
            prev.values_mm?.length === GRID_SIZE * GRID_SIZE) {
          console.log(`${date}: complete, skipping`);
          continue;
        }
      } catch { /* rebuild on unreadable file */ }
    }
    const result = await accumulateWindow(startMs, endMs, date);
        writeJson(file, { schema_version: SCHEMA_VERSION, date, ...result });
  }

  // Today: running total since the last day boundary.
  const todayStart = dayBoundaryUtcMs(today);
  const todayEnd = dayBoundaryUtcMs(addDays(today, 1));
  const todayResult = await accumulateWindow(todayStart, todayEnd, `today (${today})`);
  writeJson(path.join(OUT_DIR, 'today.json'), {
    schema_version: SCHEMA_VERSION,
    date: today,
    as_of_utc: new Date().toISOString(),
    ...todayResult
  });

  writeMetadata();
  const nDates = rebuildIndex();
  console.log(`Done. ${nDates} daily files indexed; today.json as of ${new Date().toISOString()}.`);
}

main().catch(e => { console.error(e); process.exit(1); });
