"use strict";

// Transport: desktop runs in the Tauri webview (IPC bridge); browser mode is
// served by the built-in web server — same-origin HTTP for commands and a
// WebSocket for the stats feed. Each browser gets its own ZMQ session on the
// server (sid), so concurrent users never share a connection; the stats feed
// is shared server-side and fanned out to every connection.
const IN_TAURI = typeof window.__TAURI__ !== "undefined";
const SID = IN_TAURI
  ? ""
  : (window.crypto && crypto.randomUUID
      ? crypto.randomUUID()
      : "s" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10));
const invoke = IN_TAURI
  ? window.__TAURI__.core.invoke
  : async (cmd, args) => {
      const r = await fetch("/api/" + cmd + "?sid=" + SID, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(args || {}),
      });
      const text = await r.text();
      if (!r.ok) throw text || ("HTTP " + r.status);
      return JSON.parse(text);
    };
const $ = (id) => document.getElementById(id);

const COLORS = [
  "#4fc3f7", "#ff8a65", "#aed581", "#ba68c8", "#ffd54f",
  "#4db6ac", "#f06292", "#7986cb", "#90a4ae", "#e6c229",
];

// TimeUnit enum -> milliseconds per unit (for display only)
const UNIT_MS = { 0: 1000, 1: 1, 2: 0.001, 3: 1e-6, 4: 60000, 5: 3600000, 6: 86400000 };
const RES_NAME = { 0: "SECOND", 1: "MILLISECOND", 2: "MICROSECOND", 3: "NANOSECOND", 4: "MINUTE", 5: "HOUR", 6: "DAY" };

const MERGE_CAP = 200_000;    // hard cap on merged points per query (plot)
// raw (table) cap: ~23 days of dense 1-second data fits whole; larger
// ranges are decimated with a notice (a full year is ~31M rows ≈ GBs of
// client memory)
const TABLE_CAP = 2_000_000;

// bump on every frontend change — shown in the status line so a stale
// binary (old bundled frontend) is visible at a glance
const BUILD = "ui21";

const S = {
  connected: false,
  host: "127.0.0.1",
  port: 9497,
  dbs: [],
  db: null,          // {name, resolution, sizeBytes, files}
  data: null,        // {timestamps: [], cols: {i: [...]}}
  dataRaw: false,    // data fetched without plot downsampling (table mode)
  rowsReturned: 0,
  rowsPlotted: 0,
  selCols: new Set(),
  colsOpen: true,     // column picker expanded (persisted across re-renders)
  colsCount: 0,       // column checkboxes currently in the DOM (skip rebuilds)
  view: null,        // {t0, t1} in DB time units
  liveTimer: null,
  // rows per range_query request. The server streams its replies (no size
  // rejection), so this is purely a client-memory knob: each reply is
  // materialized whole in the viewer (wire buffer + prost tree, ~300B/row),
  // so 1M rows caps a dense chunk at ~300MB. Multi-year ranges are still
  // split — one request per year would peak at GBs client-side.
    chunk: 1_000_000,
    viewMode: "plot",  // plot | table
    scaleMode: "auto", // auto | log | linear (auto log-scales large value spans)
    monitor: { rtt: [], lastQuery: null },
    monTimer: null,
    stats: null,       // latest SysStats_PUB snapshot (server push)
    statsStale: false,
    statsLastRx: 0,
    sparks: { cpu: [], mem: [], net: [], disk: [] }, // sparkline history (capped)
    rowSizes: null,    // {sampled, min, max, avg, formulaMax} from the last query
    dbRowSizes: {},   // per-DB row-size stats: {db: rowSizes} — feeds the slot badge
    querying: false,  // a chunked query is in flight (guards against concurrent runs)
    cfgMeta: null,    // .CFG file shown as a metadata table in the table area
    tableTotal: -1,    // row count of the last table render (scroll reset on change)
    tableSlice: "all", // all | head | tail — sample the table to the first/last N rows
    tableN: 100,       // N rows for HEAD/TAIL
    fileDetailJson: null, // last-rendered file list — skips 10s re-renders when unchanged
};

const canvas = $("chart");
const ctx = canvas.getContext("2d");
let framePending = false;
let hover = null;    // {px, py}
let drag = null;     // {px, t0, t1}

// ---------- helpers ----------

const unitMs = () => UNIT_MS[S.db.resolution] ?? 1000;
const tsToMs = (ts) => ts * unitMs();

function fmtTs(ts) {
  const ms = tsToMs(ts);
  if (!isFinite(ms)) return String(ts);
  return new Date(ms).toISOString().replace("T", " ").slice(0, 19) + "Z";
}

function fmtBytes(b) {
  if (b == null) return "?";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return b.toFixed(b < 10 && i > 0 ? 1 : 0) + " " + u[i];
}

function unitNow() {
  return Math.floor(Date.now() / unitMs());
}

function fmtKbps(k) {
  if (k >= 1024) return (k / 1024).toFixed(1) + " MB/s";
  return k.toFixed(0) + " KB/s";
}

// thousands separators for data values ("378,348" not "378348");
// up to 6 fraction digits, none shown when the value is integral
function fmtCell(v) {
  return v.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

// "2.7 / 63 GB" — one shared unit for the pair instead of repeating it
function fmtBytesPair(a, b) {
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { a /= 1024; b /= 1024; i++; }
  const f = (x) => (i > 0 && x < 100 ? x.toFixed(1) : String(Math.round(x)));
  return `${f(a)} / ${f(b)} ${u[i]}`;
}

// files are now {name, actualBytes, apparentBytes} objects (was bare strings)
const fileName = (f) => (typeof f === "string" ? f : f.name);

// year -> [first, last] timestamp in DB time units (not ms, not seconds)
function yearBounds(y) {
  const startS = Date.UTC(y, 0, 1) / 1000;
  const endS = Date.UTC(y + 1, 0, 1) / 1000;
  const unitsPerSec = 1000 / unitMs();
  return [Math.floor(startS * unitsPerSec), Math.ceil(endS * unitsPerSec) - 1];
}

function setConn(mode) {
  const el = $("conn-status");
  el.className = "status " + mode;
  el.textContent = mode === "on" ? "connected" : mode === "busy" ? "working" : "offline";
}

function statusMsg(m) { $("query-status").textContent = m; }

function requestRender() {
  if (framePending) return;
  framePending = true;
  requestAnimationFrame(() => { framePending = false; render(); });
}

// ---------- connection & DB browser ----------

// server lists every dir in BaseDir/DataStore as a "DB" (incl. nested
// dirs like DataStore/logs); only real DBs have a <name>.CFG file
function filterDbs(dbs) {
  return (dbs || []).filter((d) =>
    (d.files || []).some((f) => { const n = fileName(f); return n === d.name + ".CFG" || n.endsWith(".CFG"); })
  );
}

// Every connect starts a fresh session: drop the previous server's data,
// view window, column selection, monitor history and table state so nothing
// from an old session survives a reconnect (to the same or another machine).
function resetSession() {
  S.connected = false;
  S.dbs = [];
  S.db = null;
  S.data = null;
  S.dataRaw = false;
  S.rowSizes = null;
  S.dbRowSizes = {};
  S.querying = false;
  S.cfgMeta = null;
  S.view = null;
  S.rowsReturned = 0;
  S.rowsPlotted = 0;
  S.selCols.clear();
  S.tableTotal = -1;
  tableScroll = 0;
  S.monitor.rtt.length = 0;
  S.monitor.lastQuery = null;
  S.scaleMode = "auto";
  $("btn-scale").textContent = "SCALE: AUTO";
  S.viewMode = "plot";
  $("btn-view").textContent = "VIEW: PLOT";
  if (S.liveTimer) { clearInterval(S.liveTimer); S.liveTimer = null; }
  $("live").checked = false;
  $("controls").hidden = true;
  $("t0").value = ""; $("t1").value = "";
  $("t0-dt").textContent = ""; $("t1-dt").textContent = "";
  $("t1").classList.remove("bad");
  $("server-info").textContent = "";
  $("db-list").innerHTML = '<div class="dim" style="padding:4px 10px">not connected</div>';
  $("db-bar").innerHTML = "";
  $("file-detail-wrap").innerHTML = "";
  $("cols-bar").innerHTML = "";
  $("stats").innerHTML = "";
  $("inspector").innerHTML = "click a point on the chart to inspect the raw row";
  $("data-table").innerHTML = "";
  $("table-wrap").scrollTop = 0;
  clearStats(); // also drops the previous server's feed history
  renderMonitor();
  requestRender();
}

async function connect() {
  S.host = $("host").value.trim();
  S.port = parseInt($("port").value, 10) || 9497;
  resetSession();
  setConn("busy");
  try {
    const info = await invoke("connect", { host: S.host, port: S.port });
    S.connected = true;
    S.dbs = filterDbs(info.dbs);
    $("server-info").textContent = `${info.serverAddr} · ${info.threadCount} workers`;
    renderDbs();
    setConn("on");
    startInfoRefresh(); // db sizes/files refresh in the background every 10s
    const cm = info.connectMs || {};
    statusMsg(`connected · build ${BUILD} · ping ${(cm.ping ?? 0).toFixed(0)}ms · info ${(cm.info ?? 0).toFixed(0)}ms`);
    if (S.dbs.length) selectDb(S.dbs[0].name);
    else $("db-bar").innerHTML = "connected — no databases found";
  } catch (e) {
    S.connected = false;
    setConn("off");
    stopMonitor();
    stopInfoRefresh();
    clearStats();
    $("db-bar").innerHTML = `<span style="color:#e57373">connect failed: ${e}</span>`;
  }
}

// slot vs measured row size, shown in the db list + db bar (not the stats
// bar): XStore pads every serialized row out to its slot, which is aligned
// to a power of two (8B = word size), so the proper slot for the data is the
// largest row rounded UP to that alignment (rowSizes.formulaMax — a 55B row
// needs 56B, not the next power of two 64B). slot == proper: green tick;
// slot > proper: red X; slot < proper: red X (data would not even fit). The
// (N% waste) is ALWAYS shown once rows are known: (slot - largest row) /
// slot — the pad paid on every row, even when the slot fits (a 55B row in a
// 64B slot is 14% waste). Unknown until a query for that DB has sampled real
// rows.
function slotBadge(d) {
  if (!d || !d.rowLength) return "";
  const rs = S.dbRowSizes[d.name];
  if (!rs || !rs.sampled) return `slot ${d.rowLength}B`;
  const row = rs.min === rs.max ? `${rs.min}B` : `${rs.min}–${rs.max}B`;
  const proper = rs.formulaMax || 0;
  if (proper && d.rowLength < proper) {
    return `slot ${d.rowLength}B · row ${row} <span class="bad-x">✗ too small</span>`;
  }
  const waste = Math.max(0, Math.round((1 - rs.max / d.rowLength) * 100));
  const mark = proper && d.rowLength === proper
    ? '<span class="ok">✓</span>'
    : '<span class="bad-x">✗</span>';
  return `slot ${d.rowLength}B · row ${row} ${mark} (${waste}% waste)`;
}

function renderDbs() {
  const el = $("db-list");
  el.innerHTML = "";
  for (const d of S.dbs) {
    const item = document.createElement("div");
    item.className = "db-item" + (S.db && S.db.name === d.name ? " sel" : "");
    // two deliberate lines: summary, then the slot badge on its own nowrap
    // line — one run-on line wrapped mid-phrase ("slot 64B · row" / "55B ✓")
    item.innerHTML =
      `<div class="name">${d.name}</div>` +
      `<div class="meta">${RES_NAME[d.resolution] ?? d.resolution} · ${fmtBytes(d.sizeBytes)} disk` +
      ` · ${(d.files || []).length} files</div>` +
      (d.rowLength ? `<div class="badge">${slotBadge(d)}</div>` : "");
    item.onclick = () => selectDb(d.name);
    el.appendChild(item);
  }
}

function renderDbBar() {
  if (!S.db) { $("db-bar").innerHTML = ""; return; }
  const files = S.db.files || [];
  const years = files.map((f) => parseInt(fileName(f), 10)).filter((y) => y > 1900 && y < 2200);
  const span = years.length ? `${Math.min(...years)}–${Math.max(...years)}` : null;
  $("db-bar").innerHTML =
    `<b>${S.db.name}</b> · ${RES_NAME[S.db.resolution] ?? S.db.resolution} · ${fmtBytes(S.db.sizeBytes)} disk` +
    (S.db.rowLength ? ` · <span class="nowrap">${slotBadge(S.db)}</span>` : "") +
    ` · ${files.length} files` + (span ? ` (${span})` : "");
}

// per-file actual (allocated, st_blocks×512) vs logical (st_size, the file
// size the filesystem reports); logical > actual = sparse /
// preallocated-but-not-fully-written. Data files (<year>.XSTORE) query the
// first day of their year on click; the .CFG shows its metadata as a table.
function renderFileDetail() {
  const wrap = $("file-detail-wrap");
  if (!S.db || !(S.db.files || []).length) { wrap.innerHTML = ""; S.fileDetailJson = null; return; }
  // the 10s background refresh must NOT rebuild this panel while the user is
  // clicking its rows: replacing the DOM mid-click drops the click (a mousedown
  // on a row that gets swapped out before mouseup fires no click event), which
  // left the table stuck on the previous view. Only rebuild when the file list
  // actually changed.
  const sorted0 = [...S.db.files].sort((a, b) => fileName(a).localeCompare(fileName(b)));
  const json = JSON.stringify(sorted0.map((f) => typeof f === "string" ? f : [f.name, f.actualBytes, f.apparentBytes]));
  if (json === S.fileDetailJson && $("file-detail")) return;
  S.fileDetailJson = json;
  // keep the user's open/closed choice instead of snapping it back.
  // Default: COLLAPSED — the list only expands when the user clicks the summary
  const prev = $("file-detail");
  const keepOpen = prev ? prev.open : false;
  let html = `<details id="file-detail"><summary>file sizes</summary><table>` +
    "<tr><th>file</th><th>actual</th><th>logical</th></tr>";
  for (const f of sorted0) {
    const name = fileName(f);
    const year = parseInt(name, 10);
    const isData = /^\d{4}\.XSTORE$/i.test(name) && year > 1900 && year < 2200;
    const isCfg = /\.CFG$/i.test(name);
    if (typeof f === "string") {
      html += `<tr><td>${name}</td><td>—</td><td>—</td></tr>`;
      continue;
    }
    const sparse = f.apparentBytes > f.actualBytes;
    const attrs = isData ? ` data-year="${year}"` : isCfg ? ` data-cfg="${name}"` : "";
    html += `<tr${attrs}><td>${name}${sparse ? ` <span class="sparse">sparse</span>` : ""}</td>` +
      `<td>${fmtBytes(f.actualBytes)}</td><td>${fmtBytes(f.apparentBytes)}</td></tr>`;
  }
  html += "</table></details>";
  wrap.innerHTML = html;
  $("file-detail").open = keepOpen;
  wrap.querySelectorAll("tr[data-year]").forEach((tr) => {
    tr.classList.add("clickable");
    tr.onclick = () => queryFirstDayOfYear(+tr.dataset.year);
  });
  wrap.querySelectorAll("tr[data-cfg]").forEach((tr) => {
    tr.classList.add("clickable");
    tr.onclick = () => showCfgMeta(tr.dataset.cfg);
  });
}

// clicking a <year>.XSTORE row queries the first day of that year (from the
// year's beginning timestamp) and plots/tables it
function queryFirstDayOfYear(year) {
  if (!S.db || !S.connected) return;
  S.cfgMeta = null; // data file click exits any .CFG metadata view
  const [y0] = yearBounds(year);
  const day = Math.max(1, Math.floor(86400000 / unitMs()));
  $("t0").value = y0;
  $("t1").value = y0 + day - 1;
  updateDtHints();
  statusMsg(`querying first day of ${year} (${day.toLocaleString()} units)`);
  query();
}

// clicking the .CFG row switches to table view and shows the file's
// underlying metadata: a .CFG is a serialized ConfigStruct — dbName,
// timeResolution, rowLength — nothing more (that's why the badge in the db
// bar already carries all of it)
function showCfgMeta(name) {
  S.cfgMeta = name;
  S.viewMode = "table";
  $("btn-view").textContent = "VIEW: TABLE";
  requestRender();
}

function renderCfgTable(name) {
  const db = S.db;
  const f = db && (db.files || []).find((x) => fileName(x) === name);
  const rows = [
    ["file", name],
    ["metadata size", f ? `${fmtBytes(f.actualBytes)} actual / ${fmtBytes(f.apparentBytes)} logical` : "—"],
    ["dbName", db ? db.name : "—"],
    ["timeResolution", db ? (RES_NAME[db.resolution] ?? db.resolution) : "—"],
    ["rowLength (slot)", db ? slotBadge(db) : "—"],
  ];
  let html = "<thead><tr><th>key</th><th>value</th></tr></thead><tbody>";
  for (const [k, v] of rows) html += `<tr><td>${k}</td><td>${v}</td></tr>`;
  html += `<tr><td colspan="2" class="dim">click a data file row or press Query to view the data</td></tr>`;
  html += "</tbody>";
  $("data-table").innerHTML = html;
}

function selectDb(name) {
  S.db = S.dbs.find((d) => d.name === name);
  S.data = null;
  S.rowSizes = null;
  S.cfgMeta = null;
  S.fileDetailJson = null; // force the file panel to re-render for the new DB
  S.view = null;
  S.selCols.clear();
  $("controls").hidden = false;
  renderDbBar(); // slot badge lives here (and in the db list), not the stats bar
  renderFileDetail();
  renderDbs();
  renderQuickRanges();
  renderColsBar();
  requestRender();

  // default range: From = start of the first available year, To = From + 1
  // day (the first day of data). Prefill only; no auto-query — a full-span
  // chunked query over every year would saturate the server on connect.
  // The year chips / ALL cover the wider ranges.
  const files = S.db.files || [];
  const years = files.map((f) => parseInt(fileName(f), 10)).filter((y) => y > 1900 && y < 2200);
  if (years.length) {
    const [t0] = yearBounds(Math.min(...years));
    const day = Math.max(1, Math.floor(86400000 / unitMs()));
    $("t0").value = t0;
    $("t1").value = t0 + day - 1; // inclusive end: exactly the first full day
  }
  updateDtHints();
  statusMsg(`${S.db.name} selected — default range: From + 1 day${years.length ? ` (first day of ${Math.min(...years)})` : ""} · query, or pick a year/range`);
  probeRowSize(); // fills the badge's "row NNNB ✓" part — needs real rows
}

// The slot badge's row-size half is only knowable from actual data
// (serialized RowData length), so a freshly selected DB shows just
// "slot NNNB" until rows have been sampled. Fire one small first-day query
// on selection to measure it — WITHOUT touching the From/To inputs, the
// view or the chart: the user still decides when to run the real query.
// A hole-only first day falls back to the first week; if that comes up
// empty too, the 10s refresh keeps retrying until rows are sampled.
async function probeRowSize(days = 1) {
  if (!S.db || !S.connected) return;
  const files = S.db.files || [];
  const years = files.map((f) => parseInt(fileName(f), 10)).filter((y) => y > 1900 && y < 2200);
  if (!years.length) return;
  const [y0] = yearBounds(Math.min(...years));
  const units = Math.max(1, Math.floor((days * 86400000) / unitMs()));
  const db = S.db.name;
  try {
    const res = await invoke("range_query", { db, start: y0, end: y0 + units - 1, raw: false });
    // the user may have switched DBs while the probe was in flight
    if (S.db && S.db.name === db && res.rowSizes) {
      S.dbRowSizes[db] = res.rowSizes;
      renderDbs();
      renderDbBar();
    } else if (days < 7) {
      probeRowSize(7); // first day empty — retry the first week
    }
  } catch (e) { /* transient — the 10s refresh retries */ }
}

function renderQuickRanges() {
  const el = $("quick-ranges");
  el.innerHTML = "";
  const years = (S.db.files || []).map((f) => parseInt(fileName(f), 10)).filter((y) => y > 1900 && y < 2200).sort((a, b) => a - b);
  for (const y of years) {
    const b = document.createElement("button");
    b.textContent = String(y);
    b.onclick = () => {
      const [t0, t1] = yearBounds(y);
      $("t0").value = t0;
      $("t1").value = t1;
      updateDtHints();
      query();
    };
    el.appendChild(b);
  }
  const all = document.createElement("button");
  all.textContent = "ALL";
  all.onclick = () => {
    if (!years.length) return;
    const [t0] = yearBounds(Math.min(...years));
    const [, t1] = yearBounds(Math.max(...years));
    $("t0").value = t0;
    $("t1").value = t1;
    updateDtHints();
    query();
  };
  el.appendChild(all);
}

// ---------- query ----------

// Split [t0, t1] into <=S.chunk-row queries and merge. The server streams
// its responses, so it never rejects a span for size (the old 2GB cap is
// gone — see docs/xstore-bigrange-oom-handoff.md, RESOLVED); chunking exists
// only to cap the viewer's own memory per in-flight reply. raw = table mode:
// the client-side (convert.rs) plot downsampling is skipped, so every row
// the server returned lands in the merged arrays.
async function queryChunked(t0, t1, raw, onChunk) {
  const out = { timestamps: [], cols: {}, rowsReturned: 0, dataRows: 0, chunks: 0, status: "", rpcMs: 0, rowSizes: null, decimated: false };
  let start = t0;
  while (start <= t1) {
    const end = Math.min(t1, start + S.chunk - 1);
    out.chunks++;
    statusMsg(`querying ${S.db.name} [${start.toLocaleString()} .. ${end.toLocaleString()}] chunk ${out.chunks} (${S.chunk.toLocaleString()}/chunk)`);
    let res;
    try {
      res = await invoke("range_query", { db: S.db.name, start, end, raw });
    } catch (e) {
      // transient socket blip — the Rust client auto-reconnects, so retry
      // the same chunk once before giving up on the whole query
      statusMsg(`chunk failed (${String(e).slice(0, 60)}) — retrying once`);
      res = await invoke("range_query", { db: S.db.name, start, end, raw });
    }
    out.rpcMs += res.rpcMs || 0;
    mergeRange(out, res);
    if (onChunk) onChunk(out); // progressive: draw the data landed so far
    start = end + 1;
  }
  // merge per-chunk row-size samples into one set of stats
  if (out.rowSizes) {
    out.rowSizes.avg = out.rowSizes.sum / out.rowSizes.sampled;
    delete out.rowSizes.sum;
  }
  out.decimated = capMerged(out, raw ? TABLE_CAP : MERGE_CAP);
  return out;
}

function mergeRange(out, res) {
  const ts = res.timestamps || [];
  const colKeys = Object.keys(res.cols || {});
  for (const c of colKeys) if (!out.cols[c]) out.cols[c] = [];
  const allCols = Object.keys(out.cols);
  for (let i = 0; i < ts.length; i++) {
    out.timestamps.push(ts[i]);
    // a chunk may expose fewer columns than the union seen so far; keep
    // every column array exactly aligned with out.timestamps
    for (const c of allCols) out.cols[c].push((res.cols[c] && res.cols[c][i]) ?? null);
  }
  out.rowsReturned += res.rowsReturned || 0;
  out.dataRows += res.dataRows || 0;
  if (res.status) out.status = res.status;
  if (res.rowSizes) {
    out.rowSizes = out.rowSizes || { sampled: 0, min: Infinity, max: 0, sum: 0 };
    out.rowSizes.min = Math.min(out.rowSizes.min, res.rowSizes.min);
    out.rowSizes.max = Math.max(out.rowSizes.max, res.rowSizes.max);
    out.rowSizes.sum += res.rowSizes.avg * res.rowSizes.sampled;
    out.rowSizes.sampled += res.rowSizes.sampled;
    out.rowSizes.formulaMax = Math.max(out.rowSizes.formulaMax || 0, res.rowSizes.formulaMax || 0);
  }
}

// Decimate the merged arrays to `cap` points (stride sampling, keeps the
// first row of each stride); returns true if anything was dropped.
function capMerged(out, cap) {
  const n = out.timestamps.length;
  if (n <= cap) return false;
  const step = Math.ceil(n / cap);
  out.timestamps = out.timestamps.filter((_, i) => i % step === 0);
  for (const c of Object.keys(out.cols)) out.cols[c] = out.cols[c].filter((_, i) => i % step === 0);
  return true;
}

async function query() {
  if (!S.db || !S.connected) return;
  if (S.querying) { statusMsg("query already in progress — wait for it to finish"); return; }
  S.querying = true;
  const t0 = Math.floor(+$("t0").value);
  const t1 = Math.floor(+$("t1").value);
  if (!(t0 >= 0) || !(t1 >= 0) || t1 < t0) { S.querying = false; statusMsg("invalid range"); return; }
  S.cfgMeta = null; // real data replaces any .CFG metadata table
  setConn("busy");
  const tStart = performance.now();
  // table mode pulls the range un-decimated so every row is browsable
  const raw = S.viewMode === "table";
  // Progressive: frame the view on the query range and render each chunk as
  // it lands — the user watches data flow in instead of waiting for the
  // whole range (an aggressive zoom-out can span many chunks).
  if (!S.selCols.size) S.selCols.add(0);
  S.view = { t0, t1 };
  syncRangeInputs();
  S.data = { timestamps: [], cols: {} };
  S.dataRaw = raw;
  S.rowSizes = null;
  S.rowsReturned = 0;
  S.rowsPlotted = 0;
  S.tableTotal = -1; // new data -> table restarts at the top
  renderColsBar();
  renderStats();
  requestRender();
  try {
    const res = await queryChunked(t0, t1, raw, (out) => {
      // mergeRange pushes into out's arrays — point S.data at them (same
      // arrays from the second chunk on) and draw one frame per chunk
      S.data = { timestamps: out.timestamps, cols: out.cols };
      S.rowsPlotted = out.timestamps.length;
      renderColsBar();
      requestRender();
    });
    S.data = { timestamps: res.timestamps, cols: res.cols }; // capMerged may replace the arrays
    S.rowsReturned = res.rowsReturned;
    S.rowsPlotted = res.timestamps.length;
    S.rowSizes = res.rowSizes || null;
    if (res.rowSizes) {
      // measured row size lands in the db list + db bar badge (slot fit)
      S.dbRowSizes[S.db.name] = res.rowSizes;
      renderDbs();
      renderDbBar();
    }
    const ms = performance.now() - tStart;
    S.monitor.lastQuery = { rows: S.rowsReturned, ms };
    statusMsg(
      `${S.rowsReturned.toLocaleString()} rows · ${res.dataRows.toLocaleString()} data · ${S.rowsPlotted.toLocaleString()} plotted · ` +
      `${Object.keys(S.data.cols).length} cols · total ${ms.toFixed(0)}ms · server ${res.rpcMs.toFixed(0)}ms · ${res.chunks} chunk(s)` +
      (raw ? (res.decimated ? ` · table decimated to ${TABLE_CAP.toLocaleString()} rows` : " · raw") : "") +
      (res.status ? ` · ${res.status}` : "")
    );
    if (S.data.timestamps.length && !$("live").checked) {
      // fit to the data that actually came back (sparse ranges are narrower
      // than the query); live mode keeps its anchored window
      S.view = { t0: S.data.timestamps[0], t1: S.data.timestamps[S.data.timestamps.length - 1] };
      syncRangeInputs();
    }
    setConn("on");
    renderColsBar();
    renderStats();
    requestRender();
  } catch (e) {
    setConn("off");
    statusMsg("error: " + e);
  } finally {
    S.querying = false;
  }
}

// CLEAR: drop the fetched data back to the idle state. The connection, the
// selected DB and the From/To range are kept so the same range can be
// re-queried immediately.
function clearData() {
  S.data = null;
  S.dataRaw = false;
  S.cfgMeta = null;
  S.view = null;
  S.selCols.clear();
  S.rowSizes = null;
  S.rowsReturned = 0;
  S.rowsPlotted = 0;
  S.tableTotal = -1;
  S.monitor.lastQuery = null;
  tableScroll = 0;
  $("table-wrap").scrollTop = 0;
  $("inspector").innerHTML = "click a point on the chart to inspect the raw row";
  renderColsBar();
  renderStats();
  renderMonitor();
  requestRender();
  statusMsg("cleared");
}

function syncRangeInputs() {
  if (!S.view) return;
  $("t0").value = Math.floor(S.view.t0);
  $("t1").value = Math.ceil(S.view.t1);
  updateDtHints();
}

// Column picker: a header (collapse toggle + All/None) over a scrollable list
// of per-column checkboxes. The list scrolls instead of growing without bound
// when a DB has many columns, and the whole picker can be collapsed to free
// vertical space.
function renderColsBar() {
  const el = $("cols-bar");
  const n = S.data ? Object.keys(S.data.cols).length : 0;
  if (!n) { el.innerHTML = ""; S.colsCount = 0; return; }
  const list = el.querySelector(".cols-list");
  if (!list || S.colsCount !== n) {
    buildColsBar(el, n);
    S.colsCount = n;
  } else {
    // Col set unchanged (a checkbox / All / None / collapse toggle): refresh
    // in place so the picker's scroll position and open/collapsed state
    // survive. Rebuilding the DOM here would reset both.
    const boxes = list.querySelectorAll("input[type=checkbox]");
    for (let i = 0; i < boxes.length; i++) boxes[i].checked = S.selCols.has(i);
    list.hidden = !S.colsOpen;
    const t = el.querySelector(".cols-toggle");
    if (t) t.textContent = `${S.colsOpen ? "▾" : "▸"} columns (${n})`;
  }
}

function buildColsBar(el, n) {
  el.innerHTML = "";
  const head = document.createElement("div");
  head.className = "cols-head";
  const toggle = document.createElement("button");
  toggle.className = "cols-toggle";
  toggle.title = "show / hide the column picker";
  toggle.textContent = `${S.colsOpen ? "▾" : "▸"} columns (${n})`;
  toggle.onclick = () => { S.colsOpen = !S.colsOpen; renderColsBar(); };
  head.appendChild(toggle);
  const mk = (label, fn) => {
    const b = document.createElement("button");
    b.textContent = label;
    b.onclick = () => { fn(); renderColsBar(); renderStats(); requestRender(); };
    head.appendChild(b);
  };
  mk("All", () => { for (let i = 0; i < n; i++) S.selCols.add(i); });
  mk("None", () => S.selCols.clear());
  el.appendChild(head);
  const list = document.createElement("div");
  list.className = "cols-list";
  list.hidden = !S.colsOpen;
  for (let i = 0; i < n; i++) {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = S.selCols.has(i);
    cb.onchange = () => {
      if (cb.checked) S.selCols.add(i);
      else S.selCols.delete(i);
      renderColsBar();   // in-place refresh (keeps scroll + open state)
      renderStats();
      requestRender();
    };
    const sw = document.createElement("span");
    sw.className = "swatch";
    sw.style.background = COLORS[i % COLORS.length];
    label.appendChild(cb);
    label.appendChild(sw);
    label.appendChild(document.createTextNode(`col ${i}`));
    list.appendChild(label);
  }
  el.appendChild(list);
}

// ---------- live tail ----------

function toggleLive() {
  if ($("live").checked) {
    S.liveTimer = setInterval(liveTick, 2000);
    statusMsg("live tail: polling last window every 2s");
  } else {
    clearInterval(S.liveTimer);
    S.liveTimer = null;
  }
}

async function liveTick() {
  if (!S.db || !S.connected || !S.view) return;
  const span = S.view.t1 - S.view.t0;
  const now = unitNow();
  if (now > S.view.t1) {
    // live tail tracks the present — slide the view forward
    S.view = { t0: now - span, t1: now };
    syncRangeInputs();
  }
  // poll window is always <= 100k rows. Never anchor start at the view and
  // end at "now": while browsing an old year that is a multi-year span
  // (hundreds of millions of rows) re-queried every tick
  const win = Math.min(Math.max(1, Math.floor(span * 0.1)), 100_000);
  const end = Math.floor(S.view.t1);
  const start = Math.max(0, end - win + 1);
  if (end < start) return;
  try {
    const res = await invoke("range_query", { db: S.db.name, start, end, raw: S.viewMode === "table" });
    mergeData(res);
    if (res.rowSizes) {
      S.dbRowSizes[S.db.name] = res.rowSizes;
      renderDbs();
      renderDbBar();
    }
    renderStats();
    requestRender();
  } catch (e) {
    statusMsg("live error: " + e);
  }
}

function mergeData(res) {
  if (!S.data) S.data = { timestamps: [], cols: {} };
  const ts = S.data.timestamps;
  const cols = S.data.cols;
  const colKeys = Object.keys(res.cols || {});
  let last = ts.length ? ts[ts.length - 1] : -1;
  let added = 0;
  for (let i = 0; i < (res.timestamps || []).length; i++) {
    const t = res.timestamps[i];
    if (t <= last) continue;
    ts.push(t);
    for (const c of colKeys) {
      if (!cols[c]) cols[c] = [];
      // keep arrays aligned: backfill gap with null if t jumped
      const arr = cols[c];
      while (arr.length < ts.length - 1) arr.push(null);
      arr.push(res.cols[c][i] ?? null);
    }
    last = t;
    added++;
  }
  if (!Object.keys(cols).length && colKeys.length) {
    for (const c of colKeys) cols[c] = res.cols[c].slice();
  }
  // cap memory: keep newest half
  if (ts.length > 200000) {
    const cut = ts.length - 100000;
    ts.splice(0, cut);
    for (const c of Object.keys(cols)) cols[c].splice(0, cut);
  }
  S.rowsReturned += res.rowsReturned;
  return added;
}

// ---------- chart ----------

const PAD = { l: 64, r: 14, t: 12, b: 26 };

function pickTimeStep(rangeMs) {
  const steps = [
    1e3, 5e3, 15e3, 30e3, 6e4, 3e5, 9e5, 18e5, 36e5, 72e5, 144e5, 288e5,
    36e6 * 1, 72e6, 168e6, 336e6, 6048e5, 2592e6, 7776e6, 15552e6, 31536e6,
  ];
  for (const s of steps) if (rangeMs / s <= 8) return s;
  return steps[steps.length - 1];
}

function render() {
  // the HEAD/TAIL N control only makes sense over a data table
  $("table-slice-ctl").hidden = !(S.viewMode === "table" && !S.cfgMeta);
  if (S.viewMode === "table") {
    canvas.style.display = "none";
    $("table-wrap").hidden = false;
    // table mode: I-beam + text selection so cells can be copied
    $("table-wrap").classList.add("selectable");
    if (S.cfgMeta) { renderCfgTable(S.cfgMeta); return; }
    if (S.data && S.view && S.data.timestamps.length) renderTable();
    else $("data-table").innerHTML = `<tr><td class="dim">no data — run a query</td></tr>`; // centered via table CSS
    return;
  }
  $("table-wrap").hidden = true;
  $("table-wrap").classList.remove("selectable");
  canvas.style.display = "block";

  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (w === 0 || h === 0) return;
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const hasData = S.data && S.data.timestamps.length && S.view;
  if (!hasData) {
    ctx.fillStyle = "#5a6a7a";
    ctx.font = "14px monospace";
    ctx.fillText("no data — connect and run a query", 20, h / 2);
    return;
  }

  const { t0, t1 } = S.view;
  const pw = w - PAD.l - PAD.r;
  const ph = h - PAD.t - PAD.b;
  const x = (t) => PAD.l + ((t - t0) / (t1 - t0)) * pw;
  const ts = S.data.timestamps;
  // visible window as index range — per-column loops only walk points inside
  // the view, not the whole (up to 200k) array. Full-array scans here made
  // the UI sluggish/unresponsive: every mousemove/drag frame re-ran them
  // once per selected column.
  const i0 = lowerBound(ts, t0);
  const i1 = upperBound(ts, t1);

  // time grid + ticks (computed in ms space, mapped back to units)
  const rangeMs = (t1 - t0) * unitMs();
  const stepMs = pickTimeStep(rangeMs);
  const stepU = stepMs / unitMs();
  const firstTick = Math.ceil(t0 / stepU) * stepU;
  ctx.font = "11px monospace";
  for (let tk = firstTick; tk <= t1; tk += stepU) {
    const px = x(tk);
    ctx.strokeStyle = "#1c2530";
    ctx.beginPath();
    ctx.moveTo(px, PAD.t);
    ctx.lineTo(px, h - PAD.b);
    ctx.stroke();
    const d = new Date(tk * unitMs());
    const label = stepMs >= 86400000
      ? d.toISOString().slice(0, 10)
      : d.toISOString().slice(5, 16);
    ctx.fillStyle = "#6b7a8c";
    ctx.textAlign = "center";
    ctx.fillText(label, px, h - PAD.b + 16);
  }

  // ONE shared y-domain across all selected series (visible window only):
  // every plotted column uses the same scale, so toggling columns rescales
  // the axis to the union of their value spans. Per-series autoscale made
  // each column fill the full height and the axis never seemed to change.
  let dmn = Infinity, dmx = -Infinity;
  for (const col of S.selCols) {
    const arr = S.data.cols[col];
    if (!arr) continue;
    for (let i = i0; i < i1; i++) {
      const v = arr[i];
      if (typeof v !== "number") continue;
      if (v < dmn) dmn = v;
      if (v > dmx) dmx = v;
    }
  }
  const dom = isFinite(dmn) ? makeDomain(dmn, dmx) : null;
  if (!dom) {
    ctx.fillStyle = "#5a6a7a";
    ctx.font = "14px monospace";
    // no rows in the view window: either the selected columns have no values
    // here, or the view panned outside the fetched range (drag end / zoom
    // auto re-queries the window — say so while that is in flight)
    let msg = "no values in view for selected columns";
    if (viewOutsideData()) {
      const d0 = S.data.timestamps[0], d1 = S.data.timestamps[S.data.timestamps.length - 1];
      msg = `outside fetched range ${fmtTs(d0)} → ${fmtTs(d1)}`
        + (S.querying ? " — loading…" : " — release to load, or press Query");
    }
    ctx.fillText(msg, 20, h / 2);
    return;
  }
  const y = dom.log
    ? (v) => PAD.t + (1 - (Math.log10(v) - Math.log10(dom.mn)) / (Math.log10(dom.mx) - Math.log10(dom.mn))) * ph
    : (v) => PAD.t + (1 - (v - dom.mn) / (dom.mx - dom.mn)) * ph;

  // series (shared scale; gaps on null; log skips non-positive values)
  const drawn = [];
  for (const col of S.selCols) {
    const arr = S.data.cols[col];
    if (!arr) continue;

    ctx.strokeStyle = COLORS[col % COLORS.length];
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    let started = false;
    for (let i = i0; i < i1; i++) {
      const t = ts[i];
      if (t < t0 || t > t1) { started = false; continue; }
      const v = arr[i];
      if (typeof v !== "number") { started = false; continue; }
      if (dom.log && v <= 0) { started = false; continue; }
      const px = x(t), py = y(v);
      if (!started) { ctx.moveTo(px, py); started = true; }
      else ctx.lineTo(px, py);
    }
    ctx.stroke();
    drawn.push({ col, dom, y });
  }

  // y-axis for the primary (first selected) series
  if (drawn.length) {
    const { dom, y } = drawn[0];
    const ticks = dom.log ? logTicks(dom.mn, dom.mx) : linTicks(dom.mn, dom.mx);
    ctx.font = "10px monospace";
    ctx.textAlign = "right";
    for (const v of ticks) {
      const py = y(v);
      if (py < PAD.t - 1 || py > h - PAD.b + 1) continue;
      ctx.strokeStyle = "#2a3646";
      ctx.beginPath();
      ctx.moveTo(PAD.l - 3, py);
      ctx.lineTo(PAD.l, py);
      ctx.stroke();
      ctx.fillStyle = dom.log ? "#8fb8d8" : "#6b7a8c";
      ctx.fillText(fmtNum(v), PAD.l - 6, py + 3);
    }
  }

  // hover crosshair + tooltip
  if (hover && !drag) {
    const ht = t0 + ((hover.px - PAD.l) / pw) * (t1 - t0);
    let bi = nearestIdx(ts, ht);
    if (bi >= 0) {
      const px = x(ts[bi]);
      ctx.strokeStyle = "#3a4a5e";
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.moveTo(px, PAD.t);
      ctx.lineTo(px, h - PAD.b);
      ctx.stroke();
      ctx.setLineDash([]);

      const lines = [`t = ${fmtTs(ts[bi])}`];
      for (const d of drawn) {
        const v = S.data.cols[d.col][bi];
        if (typeof v === "number") {
          lines.push(`col ${d.col}: ${fmtCell(v)}`);
        }
      }
      const tw = ctx.measureText(lines[0]).width + 24;
      const bx = Math.min(px + 12, w - tw - 8);
      const by = Math.min(hover.py, h - PAD.t - lines.length * 16 - 10);
      ctx.fillStyle = "rgba(13,18,24,0.92)";
      ctx.strokeStyle = "#2a3646";
      ctx.fillRect(bx, by, tw, lines.length * 16 + 8);
      ctx.strokeRect(bx, by, tw, lines.length * 16 + 8);
      ctx.textAlign = "left";
      ctx.fillStyle = "#d5dee8";
      lines.forEach((l, i) => ctx.fillText(l, bx + 10, by + 16 + i * 16));
    }
  }

  // axis frame
  ctx.strokeStyle = "#232d3a";
  ctx.strokeRect(PAD.l, PAD.t, pw, ph);
}

function fmtNum(v) {
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toPrecision(3) + "B";
  if (a >= 1e6) return (v / 1e6).toPrecision(3) + "M";
  if (a >= 1e3) return (v / 1e3).toPrecision(3) + "k";
  if (a >= 1) return v.toPrecision(3);
  if (a === 0) return "0";
  return v.toExponential(1);
}

function linTicks(mn, mx) {
  const out = [];
  for (let i = 0; i <= 4; i++) out.push(mn + ((mx - mn) * i) / 4);
  return out;
}

function logTicks(mn, mx) {
  const lo = Math.ceil(Math.log10(mn));
  const hi = Math.floor(Math.log10(mx));
  if (hi - lo >= 1) {
    const out = [];
    for (let k = lo; k <= hi; k++) out.push(Math.pow(10, k));
    return out;
  }
  // span under one decade: even log-space steps
  const lmn = Math.log10(mn), lmx = Math.log10(mx);
  const out = [];
  for (let i = 0; i <= 4; i++) out.push(Math.pow(10, lmn + ((lmx - lmn) * i) / 4));
  return out;
}

// shared y domain from an already-computed [mn, mx]
function makeDomain(mn, mx) {
  const wantLog = S.scaleMode === "log" || (S.scaleMode === "auto" && mn > 0 && mx / mn > 100);
  if (wantLog && mn > 0) {
    // pad in LOG space. The old code padded linearly, so the lower bound
    // (mn - 8% of span) went to zero/negative for wide spans and the 1e-9
    // floor clamp stretched the axis far below the data: data 10..1k drew
    // an axis 1e-9..1e3 with the signal squashed into the top band.
    let lmn = Math.log10(mn), lmx = Math.log10(mx);
    if (lmx - lmn < 0.3) lmx = lmn + 0.3; // degenerate span: floor at 0.3 decades
    const pad = (lmx - lmn) * 0.08;
    return { mn: Math.pow(10, lmn - pad), mx: Math.pow(10, lmx + pad), log: true };
  }
  if (mn === mx) { mn -= 1; mx += 1; }
  const padV = (mx - mn) * 0.08;
  return { mn: mn - padV, mx: mx + padV, log: false };
}

// ---------- table view (windowed) ----------
// Only the rows around the viewport are in the DOM; spacer rows keep the
// full scroll height, so scrolling "loads" the next slice while the rows
// far behind the viewport are dropped. Stays fluid at 200k rows.
// The rendered pitch (row height) is MEASURED from the live table — padding
// + line-height + the 1px collapsed border — because spacers sized from a
// wrong pitch leave a blank band under the last row at the end of the
// scroll. headH is the sticky thead's height; row i sits at headH + i*pitch.
const TM = { pitch: 25, headH: 25 };
const TABLE_BUF = 6; // extra rows kept above/below the viewport
let tableScroll = 0; // row index at the top of the scroll position

function renderTable() {
  const ts = S.data.timestamps;
  const { t0, t1 } = S.view;
  const i0 = lowerBound(ts, t0);
  const i1 = upperBound(ts, t1);
  const total = i1 - i0;
  // HEAD/TAIL: window the table to the first/last N rows of the query range
  // instead of showing all (a 50M-row range is unscrollable; the sample is
  // for eyeballing, the plot stays the full-range view). Rows outside the
  // slice are NOT rendered — no spacer for them — so the sample always
  // starts at the top of the scroll area.
  let base = i0;          // data index of the slice's first row
  let effTotal = total;   // rows in the slice
  if (total && S.tableSlice !== "all") {
    const n = Math.min(S.tableN, total);
    if (S.tableSlice === "head") { base = i0; effTotal = n; }
    else { base = i1 - n; effTotal = n; }
  }
  $("table-slice-note").textContent =
    total && S.tableSlice !== "all"
      ? `${effTotal.toLocaleString()} of ${total.toLocaleString()} rows (${S.tableSlice})`
      : total ? `${total.toLocaleString()} rows` : "";
  const cols = [...S.selCols].sort((a, b) => a - b);
  const wrap = $("table-wrap");
  const nCols = 2 + cols.length;

  if (effTotal !== S.tableTotal) {
    // new data, new view window, or slice change: restart from the top
    S.tableTotal = effTotal;
    tableScroll = 0;
    wrap.scrollTop = 0;
  }
  tableScroll = Math.max(0, Math.min(tableScroll, effTotal));

  let html = "<thead><tr><th>time (unix)</th><th>time (UTC)</th>";
  for (const c of cols) html += `<th>col ${c}</th>`;
  html += "</tr></thead><tbody>";

  if (!total) {
    html += `<tr><td colspan="${nCols}" class="dim">no data — run a query</td></tr></tbody>`;
    $("data-table").innerHTML = html;
    return;
  }

  const p = TM.pitch, hh = TM.headH;
  const viewRows = Math.max(10, Math.ceil((wrap.clientHeight - hh) / p));
  // Rows ABOVE the viewport start collapse into one spacer; the rows BELOW
  // the viewport are real rows (the buffer), and only the tail beyond that
  // is a spacer. Making the buffer rows into a bottom spacer is what left
  // the blank band under the last row when scrolled to the end.
  const start = Math.max(0, Math.min(tableScroll - TABLE_BUF, effTotal));
  const end = Math.max(0, Math.min(tableScroll + viewRows + TABLE_BUF, effTotal));
  const shown = end - start;
  if (start * p) html += spacerRow(nCols, start * p);
  for (let r = 0; r < shown; r++) {
    const i = base + start + r;
    html += `<tr><td>${Math.round(ts[i]).toLocaleString()}</td><td>${fmtTs(ts[i])}</td>`;
    for (const c of cols) {
      const v = S.data.cols[c] ? S.data.cols[c][i] : null;
      html += `<td>${typeof v === "number" ? fmtCell(v) : (v == null ? "" : v)}</td>`;
    }
    html += "</tr>";
  }
  const bottom = (effTotal - end) * p;
  if (bottom) html += spacerRow(nCols, bottom);
  html += "</tbody>";
  const table = $("data-table");
  table.innerHTML = html;
  // first render used the cached metrics; if the measured ones differ,
  // re-render once with the correct pitch (converges: second pass measures
  // the same values)
  if (measureTable(table)) renderTable();
}

// pitch = distance between two consecutive data rows (offsetTop difference
// is frame-independent); headH = thead's own height (offsetHeight). Both
// fall back to the cached values when not measurable.
function measureTable(table) {
  const rows = table.tBodies[0].rows;
  let changed = false;
  for (let i = 0; i + 1 < rows.length; i++) {
    if (rows[i].className !== "tspacer" && rows[i + 1].className !== "tspacer") {
      const p = rows[i + 1].offsetTop - rows[i].offsetTop;
      if (p > 0 && p !== TM.pitch) { TM.pitch = p; changed = true; }
      break;
    }
  }
  if (table.tHead) {
    const hh = table.tHead.offsetHeight;
    if (hh > 0 && hh !== TM.headH) { TM.headH = hh; changed = true; }
  }
  return changed;
}

function spacerRow(nCols, h) {
  return `<tr class="tspacer"><td colspan="${nCols}" style="height:${h}px"></td></tr>`;
}

function nearestIdx(ts, target) {
  if (!ts.length) return -1;
  let lo = 0, hi = ts.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (ts[mid] < target) lo = mid;
    else hi = mid;
  }
  return Math.abs(ts[lo] - target) <= Math.abs(ts[hi] - target) ? lo : hi;
}

// timestamps are sorted — first index with ts[i] >= target / ts[i] > target
function lowerBound(ts, target) {
  let lo = 0, hi = ts.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (ts[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
function upperBound(ts, target) {
  let lo = 0, hi = ts.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (ts[mid] <= target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// The fetched rows only cover one contiguous window (the last query's range).
// Pan/zoom moves S.view; once the view no longer overlaps that window there is
// nothing client-side to draw and the plot goes blank — but the server still
// holds the data (aggregates over the new range prove it). Returning true here
// is the signal to re-query the panned-to window so the plot refills.
function viewOutsideData() {
  if (!S.data || !S.data.timestamps.length || !S.view) return false;
  const d0 = S.data.timestamps[0];
  const d1 = S.data.timestamps[S.data.timestamps.length - 1];
  return S.view.t1 < d0 || S.view.t0 > d1;
}

// ---------- interaction ----------

canvas.addEventListener("mousemove", (e) => {
  const r = canvas.getBoundingClientRect();
  const px = e.clientX - r.left;
  const py = e.clientY - r.top;
  if (drag) {
    const pw = canvas.clientWidth - PAD.l - PAD.r;
    const dt = ((drag.px - px) / pw) * (drag.t1 - drag.t0);
    S.view = { t0: drag.t0 + dt, t1: drag.t1 + dt };
    syncRangeInputs();
    requestRender();
    return;
  }
  hover = { px, py };
  requestRender();
});
canvas.addEventListener("mouseleave", () => { hover = null; requestRender(); });
canvas.addEventListener("mousedown", (e) => {
  const r = canvas.getBoundingClientRect();
  drag = { px: e.clientX - r.left, t0: S.view.t0, t1: S.view.t1 };
});
window.addEventListener("mouseup", async (e) => {
  if (!drag) return;
  const moved = Math.abs((e.clientX - canvas.getBoundingClientRect().left) - drag.px);
  const was = drag;
  drag = null;
  if (moved < 4) await inspectAt(was);
  requestRender();
  // panned outside the fetched window: load the new range so the plot refills
  // instead of sitting blank (aggregates over it already work server-side)
  if (moved >= 4 && viewOutsideData()) await query();
});
canvas.addEventListener("wheel", (e) => {
  if (!S.view) return;
  e.preventDefault();
  const r = canvas.getBoundingClientRect();
  const pw = canvas.clientWidth - PAD.l - PAD.r;
  const px = e.clientX - r.left;
  const { t0, t1 } = S.view;
  const ta = t0 + ((px - PAD.l) / pw) * (t1 - t0);
  const f = Math.exp(-e.deltaY * 0.0015);
  let nt0 = ta - (ta - t0) * f;
  let nt1 = ta + (t1 - ta) * f;
  const span = nt1 - nt0;
  if (span < 1) { const c = (nt0 + nt1) / 2; nt0 = c - 0.5; nt1 = c + 0.5; }
  if (span > 1e9) { nt0 = 0; nt1 = 1e9; }
  S.view = { t0: nt0, t1: nt1 };
  syncRangeInputs();
  requestRender();
  // zooming out past the fetched window: load the new range (the S.querying
  // guard coalesces rapid wheel events into one in-flight query)
  if (viewOutsideData()) query();
}, { passive: false });
canvas.addEventListener("dblclick", () => {
  if (!S.data || !S.data.timestamps.length) return;
  S.view = { t0: S.data.timestamps[0], t1: S.data.timestamps[S.data.timestamps.length - 1] };
  syncRangeInputs();
  requestRender();
});

async function inspectAt(was) {
  if (!S.db || !S.data || !S.data.timestamps.length) return;
  const pw = canvas.clientWidth - PAD.l - PAD.r;
  const ht = was.t0 + ((was.px - PAD.l) / pw) * (was.t1 - was.t0);
  const i = nearestIdx(S.data.timestamps, ht);
  const ts = Math.round(S.data.timestamps[i]);
  try {
    const res = await invoke("unary_query", { db: S.db.name, ts });
    const el = $("inspector");
    const cols = res.cols || {};
    const keys = Object.keys(cols).sort((a, b) => +a - +b);
    if (!keys.length) {
      el.innerHTML = `<div class="dim">${fmtTs(ts)}: no data (hole)</div>`;
      return;
    }
    let html = `<div style="color:#d5dee8;margin-bottom:4px">${fmtTs(ts)}</div><table>`;
    for (const k of keys) {
      html += `<tr><td>col ${k}</td><td>${JSON.stringify(cols[k])}</td></tr>`;
    }
    html += `</table>`;
    el.innerHTML = html;
  } catch (e) {
    $("inspector").innerHTML = `<span style="color:#e57373">${e}</span>`;
  }
}

// ---------- stats bar ----------

function renderStats() {
  const el = $("stats");
  el.innerHTML = "";
  if (!S.db || !S.view || !S.data) return;
  const col = S.selCols.values().next().value;
  if (col == null) { el.textContent = ""; return; }
  const mk = (op) => {
    const b = document.createElement("button");
    b.className = "agg"; // fixed-width box (see #stats .agg in style.css)
    b.textContent = op.toUpperCase();
    b.onclick = async () => {
      // aggregate the From/To inputs, not the chart window: S.view is the
      // fit-to-data plot view, which silently shrinks the range once a query
      // has returned (and differs entirely in table mode)
      const t0 = +$("t0").value, t1 = +$("t1").value;
      if (!S.db || Number.isNaN(t0) || Number.isNaN(t1) || t1 <= t0) {
        b.textContent = op.toUpperCase() + " ?";
        b.title = "Set a valid From < To range in the inputs first";
        return;
      }
      b.textContent = op.toUpperCase() + " …";
      b.title = "";
      try {
        const r = await invoke("aggregate", {
          op,
          db: S.db.name,
          start: Math.floor(t0),
          end: Math.floor(t1),
          col,
        });
        const txt = `${op.toUpperCase()} ${typeof r.value === "number" ? fmtCell(r.value) : (r.value ?? "∅")}`;
        b.textContent = txt;
        // box is a fixed width — the full value, the aggregated range, and
        // any status always live in the tooltip, so long results never
        // enlarge the box
        b.title = `${txt}\nrange: ${fmtTs(t0)} → ${fmtTs(t1)}`
          + (r.status && r.status !== "SUCCESS" ? `\nstatus: ${r.status}` : "");
      } catch (e) {
        b.textContent = op.toUpperCase() + " err";
        b.title = String(e);
      }
    };
    return b;
  };
  for (const op of ["min", "max", "avg", "sum"]) el.appendChild(mk(op));
}

// ---------- server monitor ----------

function startMonitor() {
  stopMonitor();
  renderMonitor();
  S.monTimer = setInterval(monitorTick, 2000);
}

function stopMonitor() {
  if (S.monTimer) { clearInterval(S.monTimer); S.monTimer = null; }
  S.monitor.rtt.length = 0;
  renderMonitor();
}

async function monitorTick() {
  if (!S.connected) return;
  try {
    const r = await invoke("ping");
    const a = S.monitor.rtt;
    a.push(r.ms);
    if (a.length > 30) a.shift();
    flash("mon-rtt"); // brief green shade = a round trip just came back
  } catch (e) {
    // socket dropped server-side; next user query reconnects on demand
  }
  renderMonitor();
}

function flash(id) {
  const el = $(id);
  el.classList.remove("flash");
  void el.offsetWidth; // reflow so the animation restarts on rapid pings
  el.classList.add("flash");
}

function renderMonitor() {
  const a = S.monitor.rtt;
  $("mon-rtt").textContent = a.length
    ? `${Math.min(...a).toFixed(0)} / ${((a.reduce((x, y) => x + y, 0) / a.length)).toFixed(0)} / ${Math.max(...a).toFixed(0)} ms`
    : "—";
  const lq = S.monitor.lastQuery;
  $("mon-query").textContent = lq && lq.ms > 0
    ? `${lq.rows.toLocaleString()} rows · ${(lq.rows / lq.ms * 1000).toLocaleString()} rows/s`
    : "—";
}

$("btn-ping").onclick = () => { monitorTick(); };

// host resource stats: the server pushes one SysStats_PUB every ~2s over a
// PUB socket (main port - 1) -> Tauri event. No polling.
const LEVEL_COLOR = { ok: "#aed581", warn: "#ffd54f", crit: "#ef5350" };

// utilization thresholds per resource (see note under the monitor panel):
//   cpu:  >= 85% red, >= 60% amber
//   mem:  >= 90% of total red, >= 70% amber
//   net:  max(rx,tx) >= 100 MB/s red, >= 10 MB/s amber
//   disk: free <= 5% of data volume red, <= 20% amber
function statLevel(kind, v) {
  switch (kind) {
    case "cpu": return v.cpu >= 85 ? "crit" : v.cpu >= 60 ? "warn" : "ok";
    case "mem": {
      const r = v.memTotal ? v.memUsed / v.memTotal : 0;
      return r >= 0.9 ? "crit" : r >= 0.7 ? "warn" : "ok";
    }
    case "net": {
      const k = Math.max(v.netRx, v.netTx);
      return k >= 100 * 1024 ? "crit" : k >= 10 * 1024 ? "warn" : "ok";
    }
    case "disk": {
      const r = v.diskTotal ? v.diskFree / v.diskTotal : 1;
      return r <= 0.05 ? "crit" : r <= 0.2 ? "warn" : "ok";
    }
  }
  return "ok";
}

function onStats(v) {
  // a stale/old-server frame must never repaint the monitor once this
  // session is over (the Rust side unsubscribes before a new connect, this
  // closes the last gap)
  if (!S.connected) return;
  const prev = S.stats;
  S.stats = v;
  S.statsLastRx = Date.now();
  // stale = snapshotUnixMs NOT advancing between consecutive messages
  // (server collector wedged). Deliberately not a wall-clock comparison:
  // clock skew between app and server would false-positive.
  S.statsStale = prev ? v.snapMs <= prev.snapMs : false;
  putStat("mon-cpu", "cpu", `${v.cpu.toFixed(1)}%`);
  putStat("mon-mem", "mem", fmtBytesPair(v.memUsed, v.memTotal));
  putStat("mon-net", "net", `rx ${fmtKbps(v.netRx)} · tx ${fmtKbps(v.netTx)}`);
  putStat("mon-disk", "disk", `${v.diskRdIops}/${v.diskWrIops} iops · free ${fmtBytesPair(v.diskFree, v.diskTotal)}`);
  // sparkline history (2s cadence, capped -> flat memory)
  const s = S.sparks;
  s.cpu.push(v.cpu);
  s.mem.push(v.memTotal ? (v.memUsed / v.memTotal) * 100 : 0);
  s.net.push(Math.max(v.netRx, v.netTx));
  s.disk.push(v.diskRdIops + v.diskWrIops);
  for (const k of ["cpu", "mem", "net", "disk"]) {
    if (s[k].length > SPARK_CAP) s[k].shift();
    drawSpark(k);
  }
}

// small activity-over-time plot under each resource row; min/max-normalized
// to its own history window (last 3 min), colored by the current level
const SPARK_CAP = 90;
function drawSpark(kind) {
  const el = $("spark-" + kind);
  const a = S.sparks[kind];
  const dpr = window.devicePixelRatio || 1;
  const w = el.clientWidth, h = el.clientHeight;
  if (!w || !h) return;
  if (el.width !== Math.round(w * dpr)) { el.width = Math.round(w * dpr); el.height = Math.round(h * dpr); }
  const c = el.getContext("2d");
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, w, h);
  if (a.length < 2) return;
  let mn = Infinity, mx = -Infinity;
  for (const v of a) { if (v < mn) mn = v; if (v > mx) mx = v; }
  if (mn === mx) { mn -= 1; mx += 1; }
  const pad = (mx - mn) * 0.1;
  mn -= pad; mx += pad;
  c.strokeStyle = S.stats ? LEVEL_COLOR[statLevel(kind, S.stats)] : "#6b7a8c";
  c.lineWidth = 1;
  c.beginPath();
  for (let i = 0; i < a.length; i++) {
    const px = (i / (a.length - 1)) * w;
    const py = h - ((a[i] - mn) / (mx - mn)) * h;
    if (i) c.lineTo(px, py);
    else c.moveTo(px, py);
  }
  c.stroke();
}

function putStat(id, kind, text) {
  const el = $(id);
  el.textContent = S.statsStale ? text + " ·stale" : text;
  el.style.color = LEVEL_COLOR[statLevel(kind, S.stats)];
}

function clearStats() {
  S.stats = null;
  S.statsStale = false;
  S.statsLastRx = 0;
  for (const k of ["cpu", "mem", "net", "disk"]) S.sparks[k].length = 0;
  for (const id of ["mon-cpu", "mon-mem", "mon-net", "mon-disk"]) {
    const el = $(id);
    el.textContent = "—";
    el.style.color = "";
  }
  for (const k of ["cpu", "mem", "net", "disk"]) drawSpark(k);
}

// feed-silent watchdog: conflate keeps memory flat, but if NO message
// arrives for >6s the socket is half-dead — say so
setInterval(() => {
  if (!S.connected || !S.stats) return;
  if (Date.now() - S.statsLastRx > 6000) {
    for (const id of ["mon-cpu", "mon-mem", "mon-net", "mon-disk"]) {
      const el = $(id);
      el.textContent = "no feed";
      el.style.color = "#55606e";
    }
  }
}, 5000);

// Host resource stats (SysStats PUB, ~2s cadence): Tauri event in desktop
// mode, WebSocket in browser mode (auto-reconnects on drop).
function connectStatsFeed() {
  if (IN_TAURI) {
    window.__TAURI__.event.listen("sys-stats", (e) => onStats(e.payload));
    return;
  }
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(proto + "://" + location.host + "/api/stats");
  ws.onmessage = (m) => onStats(JSON.parse(m.data));
  ws.onclose = () => setTimeout(connectStatsFeed, 2000);
}
connectStatsFeed();

// db sizes/file lists only come from InfoQuery (the 2s PUB feed is
// cpu/memory/network/disk only), so refresh them in the background — this
// replaces the manual "refresh server info" button
let infoTimer = null;
function refreshInfo() {
  if (!S.connected) return;
  invoke("info")
    .then((info) => {
      S.dbs = filterDbs(info.dbs);
      const name = S.db && S.db.name;
      S.db = S.dbs.find((d) => d.name === name) || S.db;
      renderDbs();
      renderDbBar();
      renderFileDetail(); // new year files appear without re-selecting the db
      if (S.db) {
        // new year files -> a new year chip (e.g. 1999 appears before 2000)
        renderQuickRanges();
        // the badge's row half is only knowable from data — keep probing
        // until a query has sampled real rows for the selected DB
        const rs = S.dbRowSizes[S.db.name];
        if (!rs || !rs.sampled) probeRowSize();
      }
      renderMonitor();
    })
    .catch(() => { /* transient; retried on the next tick */ });
}
function startInfoRefresh() {
  if (infoTimer) return;
  infoTimer = setInterval(refreshInfo, 10000);
}
function stopInfoRefresh() {
  if (infoTimer) { clearInterval(infoTimer); infoTimer = null; }
}

// ---------- wire up ----------

statusMsg(`build ${BUILD} — connect to start`);

$("btn-connect").onclick = connect;
// Desktop has the About page as a native-menu window; web mode has no menu,
// so this button is the only entry point there — an in-page modal in the
// same tab (no new tab, no redirect).
$("btn-about").onclick = () => {
  if (IN_TAURI) invoke("open_about", {});
  else openAbout();
};
function openAbout() { $("about-modal").hidden = false; }
function closeAbout() { $("about-modal").hidden = true; }
$("about-close").onclick = closeAbout;
$("about-modal").addEventListener("click", (e) => {
  if (e.target === $("about-modal")) closeAbout();
});
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeAbout();
});
$("btn-query").onclick = query;
$("btn-clear").onclick = clearData;
$("live").onchange = toggleLive;
$("btn-scale").onclick = () => {
  S.scaleMode = S.scaleMode === "auto" ? "log" : S.scaleMode === "log" ? "linear" : "auto";
  $("btn-scale").textContent = "SCALE: " + S.scaleMode.toUpperCase();
  requestRender();
};
$("btn-view").onclick = async () => {
  const toTable = S.viewMode === "plot";
  S.viewMode = toTable ? "table" : "plot";
  $("btn-view").textContent = "VIEW: " + S.viewMode.toUpperCase();
  if (!toTable) S.cfgMeta = null; // leaving table view drops the .CFG metadata table
  if (toTable && S.data && !S.dataRaw) {
    // current data is plot-decimated (20k points/chunk) — re-pull the
    // current range un-decimated so the table shows every row
    statusMsg("loading full table data…");
    await query();
  }
  requestRender();
};
$("t0").addEventListener("change", () => {
  const t0 = +$("t0").value, t1 = +$("t1").value;
  if (t0 >= 0 && t1 > t0) { S.view = { t0, t1 }; requestRender(); }
});
$("t1").addEventListener("change", () => {
  const t0 = +$("t0").value, t1 = +$("t1").value;
  if (t0 >= 0 && t1 > t0) { S.view = { t0, t1 }; requestRender(); }
});
window.addEventListener("resize", requestRender);

// update human-readable date hints next to the raw unix-timestamp inputs;
// also live-validates the range (To must be >= From)
function updateDtHints() {
  const t0 = $("t0").value, t1 = $("t1").value;
  $("t0-dt").textContent = t0 !== "" && S.db ? fmtTs(+t0) : "";
  $("t1").classList.remove("bad");
  if (!S.db) { $("t1-dt").textContent = ""; return; }
  if (t1 === "") { $("t1-dt").textContent = ""; return; }
  if (t0 !== "" && +t1 < +t0) {
    $("t1-dt").textContent = "must be ≥ From";
    $("t1").classList.add("bad");
  } else {
    $("t1-dt").textContent = fmtTs(+t1);
  }
}
// From/To take raw integer timestamps only — strip anything but digits as
// the user types (blocks -, +, e, ., decimals, paste junk)
function intInput(el) {
  const pos = el.selectionStart ?? el.value.length;
  const clean = el.value.replace(/\D/g, "");
  if (clean !== el.value) {
    const kept = el.value.slice(0, pos).replace(/\D/g, "").length;
    el.value = clean;
    el.setSelectionRange(kept, kept);
  }
  updateDtHints();
}
$("t0").addEventListener("input", (e) => intInput(e.target));
$("t1").addEventListener("input", (e) => intInput(e.target));

// HEAD/TAIL N table sampling — changing the mode or N forces a window reset
$("table-slice").addEventListener("change", () => {
  S.tableSlice = $("table-slice").value;
  S.tableTotal = -1;
  tableScroll = 0;
  $("table-wrap").scrollTop = 0;
  requestRender();
});
$("table-n").addEventListener("change", () => {
  S.tableN = Math.max(1, +$("table-n").value || 1);
  $("table-n").value = S.tableN;
  S.tableTotal = -1;
  tableScroll = 0;
  $("table-wrap").scrollTop = 0;
  requestRender();
});
$("table-n").addEventListener("input", (e) => intInput(e.target));

// table view: re-render only the slice around the viewport on scroll
let tableScrollPending = false;
$("table-wrap").addEventListener("scroll", () => {
  if (S.viewMode !== "table" || tableScrollPending) return;
  tableScrollPending = true;
  requestAnimationFrame(() => {
    tableScrollPending = false;
    if (S.viewMode !== "table") return;
    tableScroll = Math.max(0, Math.floor(($("table-wrap").scrollTop - TM.headH) / TM.pitch));
    renderTable();
  });
});
