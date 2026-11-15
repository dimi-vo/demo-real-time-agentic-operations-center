"use strict";

/*  Ops wall — variant-E render code, driven by the live /ws snapshot (D-054).
 *
 *  The wall's state does all the counting; this page only draws what the snapshot says.
 *  Each service has a Latency and a Traffic metric; each metric is a line chart above a lane.
 *  The line chart (D-061): the raw service_metrics value, one point per second, on a log y
 *  scale fitted to what's visible, with a purple tick at each injected incident's start.
 *  The lane:
 *  - upper half: each injected incident, with its verdict (answered, pending, missed)
 *  - lower half: one block per anomaly run, from the moment the detector flagged it to the
 *    moment the agent's first decision was written (open on the right while it waits); the
 *    snapshot names its owning incident (none ⇒ false alarm), its closing action and the
 *    counts of all its decisions, all shown in its tooltip (no icons on the chart)
 *  - the summary numbers and the tile status come ready-made as well
 *  All positions are axis-relative seconds (0 = left edge, axisSeconds = now). The DOM is
 *  made once and updated in place by every snapshot (D-062): a row per service, a bar per
 *  incident and per block, kept by key; a ResizeObserver redraws a chart whose width changes.
 */

// ---------- WebSocket ----------
let latest = null;
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const conn = document.getElementById("conn");
  ws.onopen = () => { conn.textContent = "● LIVE"; conn.className = "conn"; };
  ws.onclose = () => {
    conn.textContent = "● reconnecting…"; conn.className = "conn offline";
    setTimeout(connect, 1500);
  };
  ws.onmessage = (e) => {
    try { latest = JSON.parse(e.data); render(latest); settleRefresh(); }
    catch (err) { console.error("bad snapshot", err); }
  };
}
connect();

// ---------- Refresh data ----------
// POST /refresh clears the wall and re-reads the topics (D-053). The button reads "Refreshing…"
// from the click until a snapshot of the new generation arrives, and never for less than
// REFRESH_MIN_MS, so it doesn't flicker. Clicks while a refresh runs are ignored; the server
// serialises restarts as well.
const REFRESH_MIN_MS = 1000;
const refreshBtn = document.getElementById("refresh");
let refresh = null;  // while refreshing: { since: ms, generation: number | null }
function endRefresh(failed) {
  refresh = null;
  refreshBtn.disabled = false;
  refreshBtn.textContent = failed ? "Refresh failed, try again" : "Refresh data";
  refreshBtn.classList.toggle("failed", !!failed);
  if (failed) setTimeout(() => {
    if (!refresh) { refreshBtn.textContent = "Refresh data"; refreshBtn.classList.remove("failed"); }
  }, 4000);
}
function settleRefresh() {
  if (!refresh || refresh.generation === null || !latest) return;
  if ((latest.generation || 0) >= refresh.generation && Date.now() - refresh.since >= REFRESH_MIN_MS) endRefresh(false);
}
refreshBtn.addEventListener("click", async () => {
  if (refresh) return;
  refresh = { since: Date.now(), generation: null };
  refreshBtn.disabled = true;
  refreshBtn.classList.remove("failed");
  refreshBtn.textContent = "Refreshing…";
  try {
    const r = await fetch("/refresh", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    refresh.generation = (await r.json()).generation;
    settleRefresh();  // the first snapshot of the new generation may already be here
  } catch (err) {
    console.error("refresh failed", err);
    endRefresh(true);
  }
});

// ---------- Helpers ----------
const NS = "http://www.w3.org/2000/svg";
function attrs(n, a) {
  for (const k in a) n.setAttribute(k, a[k]);
  return n;
}
function s(tag, a = {}, parent) {
  const n = attrs(document.createElementNS(NS, tag), a);
  if (parent) parent.appendChild(n);
  return n;
}
// Writes that skip an unchanged value, so a still part of the page stays untouched (a text
// selection in it survives, nothing is laid out again).
const setText = (el, text) => { if (el.textContent !== text) el.textContent = text; };
const setClass = (el, cls) => { if (el.className !== cls) el.className = cls; };
const setHTML = (el, html) => { if (el._html !== html) { el._html = html; el.innerHTML = html; } };

// The page is updated in place, never rebuilt (D-062): the node under the pointer or holding
// the focus stays the same node from one snapshot to the next. sync() makes parent's children
// one node per item, in the items' order: reused by key, created for a new key, removed with
// its key. Keys must be unique among the items; parent holds nothing else.
function sync(parent, items, keyOf, create, update) {
  const prev = parent._keyed || new Map(), next = new Map();
  for (const item of items) {
    const k = keyOf(item);
    next.set(k, prev.get(k) || create(item));
  }
  for (const [k, n] of prev) if (!next.has(k)) n.remove();
  let at = parent.firstChild;
  for (const item of items) {
    const n = next.get(keyOf(item));
    if (n === at) at = at.nextSibling; else parent.insertBefore(n, at);
    update(n, item);
  }
  parent._keyed = next;
}
const dur = sec => {
  sec = Math.max(0, Math.round(sec));
  const m = Math.floor(sec / 60), r = sec % 60;
  return m ? (r ? `${m}m ${r}s` : `${m}m`) : `${r}s`;
};
let AXIS = 600;
const ago = sec => sec >= AXIS ? "now" : `−${dur(AXIS - sec)}`;
const range = (a, b) => `${ago(a)} to ${ago(b)}, ${dur(Math.min(b, AXIS) - a)}`;
const human = t => ({
  BAD_DEPLOY: "Bad deploy",
  TRAFFIC_SURGE: "Traffic surge",
  UPSTREAM_DROP: "Upstream drop",
}[t] || t.toLowerCase().replace(/_/g, " ").replace(/^./, c => c.toUpperCase()));
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const andList = xs => xs.length > 1 ? `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}` : xs.join("");
// Agent output (target, reason) ends up in a tooltip's innerHTML.
const esc = v => String(v).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const clip = (text, n) => text.length > n ? `${text.slice(0, n - 1).trimEnd()}…` : text;

function formatRate(rps) {
  if (rps == null) return "—";
  return rps >= 1000 ? `${(rps / 1000).toFixed(1)}k rps` : `${Math.round(rps)} rps`;
}
function formatPct(pct) {
  if (pct == null) return "—";
  return `${pct.toFixed(1)}%`;
}

const ACTION = {
  ROLLBACK: "Rollback",
  SCALE_OUT: "Scale out",
  INVESTIGATE_UPSTREAM: "Investigate upstream",
  OBSERVE: "Observe",
  INVALID: "Invalid decision",
};

function bar(x, y, w, h, r, openRight) {
  w = Math.max(w, 2); r = Math.min(r, w / 2, h / 2);
  const rr = openRight ? 0 : r;
  return `M ${x + r} ${y} H ${x + w - rr} ${rr ? `A ${rr} ${rr} 0 0 1 ${x + w} ${y + rr}` : ""} V ${y + h - rr} ${rr ? `A ${rr} ${rr} 0 0 1 ${x + w - rr} ${y + h}` : ""} H ${x + r} A ${r} ${r} 0 0 1 ${x} ${y + h - r} V ${y + r} A ${r} ${r} 0 0 1 ${x + r} ${y} Z`;
}

// ---------- Model ----------
// A chart's model is a filter of the snapshot: one service's incidents and blocks for one
// metric ("latency" or "traffic"). Nothing is matched or counted here; the state did that.
function metricModel(svc, kind) {
  const byId = new Map((svc.groundTruth || []).map(g => [g.incidentId, g]));
  return {
    kind,
    truth: (svc.groundTruth || []).filter(g => g.metric === kind),
    blocks: (svc.blocks || []).filter(b => b.metric === kind).sort((a, b) => a.start - b.start),
    incidentOf: b => (b.incidentId == null ? null : byId.get(b.incidentId) || null),
    key: `${svc.name}/${kind}`,
    records: svc.metrics || [],  // service_metrics samples on the axis, oldest first
  };
}
const isQuiet = m => !m.truth.length && !m.blocks.length;

// ---------- Words ----------
const TRIGGER = { LATENCY_HIGH: "Latency high", TRAFFIC_HIGH: "Traffic high", TRAFFIC_LOW: "Traffic low" };
const triggerLabel = b => TRIGGER[b.trigger]
  || (b.metric === "latency" ? "Latency high" : b.direction === "down" ? "Traffic low" : "Traffic high");
const actionLabel = a => ACTION[a] || esc(human(a || "unknown"));
const actionWord = a => (a === "INVALID" ? "invalid" : actionLabel(a).toLowerCase());

function targetText(action, target) {
  if (target == null || target === "") return "";
  if (action === "ROLLBACK") return `to v${esc(String(target).replace(/^v/i, ""))}`;
  if (action === "SCALE_OUT") return `to ${esc(target)} replicas`;
  return `target ${esc(target)}`;
}

// "5 decisions: 5 rollback" / "3 decisions: 2 rollback, 1 observe". A lone decision is the
// answer, already named above it.
function countsText(b) {
  const n = b.decisionCount || 0;
  if (!n) return "";
  if (n === 1) return "1 decision.";
  const parts = Object.entries(b.decisions || {})
    .sort((x, y) => y[1] - x[1])
    .map(([a, k]) => `${k} ${actionWord(a)}`);
  return `${plural(n, "decision", "decisions")}${parts.length ? `: ${parts.join(", ")}` : ""}.`;
}

// An incident's story from its first block: "Detected 16s after onset. Answered 8s later."
function storyOf(t) {
  const detected = t.timeToDetect > 0 ? `Detected ${dur(t.timeToDetect)} after onset.` : "Detected at onset.";
  if (t.timeToAnswer != null) return `${detected} Answered ${dur(t.timeToAnswer)} later.`;
  if (t.verdict === "answered") return `${detected} Answered.`;
  if (t.verdict === "missed") return `${detected} Nobody answered.`;
  return `${detected} Waiting for the agent.`;
}

function incidentTip(t) {
  const head = `<strong>Injected: ${human(t.type)}</strong>${range(t.start, t.end)}`;
  if (t.detectedAt != null) return `${head}<em>${storyOf(t)}</em>`;
  return `${head}<em>${t.verdict === "missed" ? "Nobody answered." : "Not detected yet."}</em>`;
}

function blockTip(m, b) {
  const t = m.incidentOf(b);
  let lag = "";
  if (b.incidentId == null) lag = "No injected incident at this time.";
  else if (t) {
    const what = human(t.type).toLowerCase();
    lag = b.timeToDetect > 0 ? `${dur(b.timeToDetect)} after the ${what} started.` : `At the start of the ${what}.`;
  }
  const target = targetText(b.action, b.target);
  const answer = b.open ? "Waiting for the agent."
    : `Answered ${dur(b.timeToAnswer)} after detection: ${actionWord(b.action)}${target ? ` ${target}` : ""}.`;
  const counts = b.decisionCount > 1 ? countsText(b) : "";
  return `<strong>Detected: ${triggerLabel(b)}</strong>${range(b.start, b.end)}` +
    `<em>${[lag, answer].filter(Boolean).join(" ")}</em>` +
    (counts ? `<em>${counts}</em>` : "") +
    (b.reason && !b.open ? `<span class="reason">${esc(clip(String(b.reason), 180))}</span>` : "");
}

// ---------- Line chart ----------
// One point per service_metrics record, a plain line on the lane's X() axis (D-061). The y
// scale is logarithmic: the incidents are ratios (x5, x0.1, x4), and on a log scale a ratio
// has the same height wherever the service's normal level sits. It runs from a round number
// just below the smallest visible value above 0 to one just above the largest, at least x10
// apart so a quiet chart doesn't blow plain noise up. A second with 0 requests sits on the
// bottom edge. The range is printed in a band above the plot, so it never covers the data.
const LINE_H = 56, PLOT_TOP = 15;  // a 15 px label band over a 41 px plot
const UNIT = { latency: "ms", traffic: "rps" };
const num = v => `${+v.toPrecision(3)}`;
const valueText = (kind, v) => kind === "latency" ? `${v >= 100 ? Math.round(v) : v.toFixed(1)} ms` : `${v} rps`;

// The nearest multiple of a fifth of the value's power of ten at or above / at or below it:
// 152 → 160, 37 → 38, 248 → 260; 33 → 32, 170 → 160.
function ceilNice(v) {
  if (!(v > 0)) return 1;
  const step = Math.pow(10, Math.floor(Math.log10(v))) / 5;
  return Math.ceil(v / step - 1e-9) * step;
}
function floorNice(v) {
  const step = Math.pow(10, Math.floor(Math.log10(v))) / 5;
  return Math.floor(v / step + 1e-9) * step;
}

// y as a fraction of the plot height (0 = bottom, 1 = top), and the band's label.
function logScale(kind, values) {
  const positive = values.filter(v => v > 0);
  const hi = ceilNice(positive.length ? Math.max(...positive) : 1);
  let lo = floorNice(positive.length ? Math.min(...positive) : hi / 10);
  if (hi / lo < 10) lo = floorNice(hi / 10);
  const span = Math.log(hi / lo);
  return {
    frac: v => Math.max(0, Math.log(Math.max(v, lo) / lo) / span),
    label: `${num(lo)}–${num(hi)} ${UNIT[kind]} · log`,
  };
}

// The pointer over a line chart. The data moves under a still pointer every second, so each
// redraw puts the crosshair and the tooltip on the second now under it.
let lineHover = null;  // { key, x, y }
function hideLineTip() {
  if (tip.dataset.owner === "line") tip.classList.remove("on");
  document.querySelectorAll(".line .crosshair").forEach(g => g.setAttribute("visibility", "hidden"));
}
document.addEventListener("pointermove", e => {
  const hit = e.target.closest?.(".line .hit");
  if (hit && hit._show) { lineHover = { key: hit._key, x: e.clientX, y: e.clientY }; hit._show(e.clientX, e.clientY); }
  else if (lineHover) { lineHover = null; hideLineTip(); }
});
document.documentElement.addEventListener("pointerleave", () => { lineHover = null; hideLineTip(); });

// A chart's <svg> and its layers (<g>s), made on its first draw and kept. A redraw empties the
// layers that only show things and updates in place the nodes the pointer or the focus can be
// on: the line chart's hit area, the lane's bars.
function chartSvg(host, cls, layers) {
  if (!host._svg) {
    host._svg = s("svg", { class: cls }, host);
    host._layers = Object.fromEntries(layers.map(name => [name, s("g", {}, host._svg)]));
  }
  return host._svg;
}
const sizeSvg = (svg, w, h) => attrs(svg, { width: w, height: h, viewBox: `0 0 ${w} ${h}` });
// Measured on the first draw, then kept current by the ResizeObserver.
function hostWidth(host) {
  if (host._w == null) host._w = Math.floor(host.clientWidth);
  return Math.max(160, host._w);
}

function drawLine(host) {
  const m = host._m;
  if (!m) return;
  const w = hostWidth(host);
  const H = LINE_H, plotH = H - PLOT_TOP - 1;
  const X = t => (Math.max(0, Math.min(t, AXIS)) / AXIS) * w;
  const recs = m.records.filter(r => r.t >= 0 && r.t <= AXIS);
  const scale = logScale(m.kind, recs.map(r => r[m.kind]));
  const Y = v => PLOT_TOP + plotH * (1 - scale.frac(v));

  const svg = chartSvg(host, "chart line", ["plot", "hover"]);
  const { plot, hover } = host._layers;
  sizeSvg(svg, w, H);
  plot.replaceChildren();
  for (let i = 1; i < 10; i++) {
    const x = Math.round(X(i * AXIS / 10)) + 0.5;
    s("line", { class: "grid", x1: x, x2: x, y1: PLOT_TOP, y2: H }, plot);
  }
  s("line", { class: "ceiling", x1: 0, x2: w, y1: PLOT_TOP + 0.5, y2: PLOT_TOP + 0.5 }, plot);
  s("line", { class: "zero", x1: 0, x2: w, y1: H - 0.5, y2: H - 0.5 }, plot);
  s("line", { class: "now", x1: w - 0.5, x2: w - 0.5, y1: PLOT_TOP, y2: H }, plot);
  s("text", { class: "scale", x: 0, y: PLOT_TOP - 5 }, plot).textContent = recs.length ? scale.label : "No records yet";

  // The start of each injected incident on this metric (INCIDENT_METRIC in the state).
  for (const t of m.truth) {
    const x = Math.round(X(t.start)) + 0.5;
    s("line", { class: "onset", x1: x, x2: x, y1: PLOT_TOP, y2: H }, plot);
  }

  // A missing second (the simulator stopped, a refresh) breaks the line instead of
  // bridging it.
  let d = "", prev = null;
  for (const r of recs) {
    d += `${prev == null || r.t - prev > 1.5 ? "M" : "L"} ${X(r.t).toFixed(1)} ${Y(r[m.kind]).toFixed(1)} `;
    prev = r.t;
  }
  if (d) s("path", { class: "series", d }, plot);

  // Hover: a hit area over the whole chart; the crosshair snaps to the nearest second.
  if (!hover.firstChild) {
    const cross = s("g", { class: "crosshair", visibility: "hidden" }, hover);
    s("line", { class: "cross", y1: PLOT_TOP, y2: H }, cross);
    s("circle", { class: "cross-dot", r: 3.5 }, cross);
    s("rect", { class: "hit", x: 0, y: 0, height: H }, hover);
  }
  const [cross, hit] = hover.children, [cl, cd] = cross.children;
  hit.setAttribute("width", w);
  hit._key = m.key;
  hit._show = (clientX, clientY) => {
    if (!recs.length) return;
    const t = ((clientX - svg.getBoundingClientRect().left) / w) * AXIS;
    let i = 0, j = recs.length - 1;
    while (i < j) { const mid = (i + j) >> 1; if (recs[mid].t < t) i = mid + 1; else j = mid; }
    const r = i > 0 && t - recs[i - 1].t < recs[i].t - t ? recs[i - 1] : recs[i];
    const x = X(r.t);
    attrs(cl, { x1: x, x2: x });
    attrs(cd, { cx: x, cy: Y(r[m.kind]) });
    cross.setAttribute("visibility", "visible");
    const during = m.truth.find(g => r.t >= g.start && r.t <= g.end);
    tipNode = null;
    setHTML(tip, `<strong>${valueText(m.kind, r[m.kind])}</strong>${ago(r.t)}` +
      (during ? `<em>During the ${esc(human(during.type).toLowerCase())}.</em>` : ""));
    tip.dataset.owner = "line";
    tip.classList.add("on");
    placeTip(clientX, clientY);
  };
  if (lineHover && lineHover.key === m.key) hit._show(lineHover.x, lineHover.y);
}

// ---------- Lane ----------
const laneHeight = m => (isQuiet(m) ? 34 : 78);

function drawChart(host) {
  const m = host._m;
  if (!m) return;
  const w = hostWidth(host);
  const quiet = isQuiet(m);
  const H = laneHeight(m), zero = H / 2, lane = quiet ? H / 2 : 32;
  const X = t => (Math.max(0, Math.min(t, AXIS)) / AXIS) * w;

  const svg = chartSvg(host, "chart", ["back", "truths", "blocks", "front"]);
  const { back, truths, blocks, front } = host._layers;
  sizeSvg(svg, w, H);
  back.replaceChildren();
  front.replaceChildren();

  for (let i = 1; i < 10; i++) {
    const x = Math.round(X(i * AXIS / 10)) + 0.5;
    s("line", { class: "grid", x1: x, x2: x, y1: 0, y2: H }, back);
  }
  s("line", { class: "zero", x1: 0, x2: w, y1: zero + 0.5, y2: zero + 0.5 }, back);
  s("line", { class: "now", x1: w - 0.5, x2: w - 0.5, y1: 0, y2: H }, back);
  if (quiet) s("text", { class: "quiet-label", x: 8, y: zero - 6 }, back).textContent = "No incidents or detections";

  // Upper half: the injected incidents, a bar per incident id. Pending stays plain purple;
  // only missed is dashed.
  sync(truths, m.truth, t => t.incidentId, () => s("path", { tabindex: 0 }), (node, t) => {
    const x0 = X(t.start), x1 = X(t.end);
    attrs(node, {
      class: t.verdict === "missed" ? "missed" : "truth",
      d: bar(x0, zero - lane, x1 - x0, lane - 1, 3, t.end >= AXIS || t.open),
      "data-tip": incidentTip(t),
    });
  });
  for (const t of m.truth) {
    const x0 = X(t.start), x1 = X(t.end);
    if (t.verdict !== "missed" || x1 - x0 <= 52) continue;
    s("text", { class: "bar-label t", x: x0 + 7, y: zero - lane / 2 + 4 }, front).textContent = "Missed";
  }

  // Lower half: one bar per anomaly run (by its key), from detection to the agent's answer.
  sync(blocks, m.blocks, b => b.key, () => s("path", { tabindex: 0 }), (node, b) => {
    const x0 = X(b.start), x1 = X(b.end);
    attrs(node, {
      class: b.incidentId == null ? "false-alarm" : "detect",
      d: bar(x0, zero + 1, x1 - x0, lane - 1, 3, b.open),
      "data-tip": blockTip(m, b),
    });
  });
  for (const b of m.blocks) {
    const x0 = X(b.start), x1 = X(b.end);
    if (b.incidentId != null || x1 - x0 <= 76) continue;
    s("text", { class: "bar-label d", x: x0 + 7, y: zero + lane / 2 + 5 }, front).textContent = "No incident";
  }

  // Time to detect: from the incident's start to its first block's start.
  for (const t of m.truth) {
    if (t.verdict === "missed" || t.detectedAt == null || !(t.timeToDetect > 0)) continue;
    const x0 = X(t.start), x1 = X(t.detectedAt), y = zero + 10;
    s("path", { class: "lag", d: `M ${x0} ${y - 4} V ${y + 4} M ${x0} ${y} H ${x1 - 3} M ${x1 - 3} ${y - 4} V ${y + 4}` }, front);
    const label = s("text", { class: "lag-label", y: y + 15 }, front);
    label.textContent = `+${dur(t.timeToDetect)}`;
    if (x1 - x0 >= 40) attrs(label, { x: (x0 + x1) / 2, "text-anchor": "middle" });
    else attrs(label, { x: x0 - 5, "text-anchor": "end" });
  }
}

// ---------- Rows ----------
function railNote(models) {
  const truth = models.flatMap(m => m.truth).sort((a, b) => a.start - b.start);
  const parts = [];
  const first = truth.find(t => t.verdict !== "missed" && t.detectedAt != null);
  if (first) parts.push(storyOf(first));
  for (const t of truth) {
    if (t.verdict === "missed") parts.push(`${human(t.type)} at ${ago(t.start)} went unanswered.`);
  }
  const fas = models.reduce((n, m) => n + m.blocks.filter(b => b.incidentId == null).length, 0);
  if (fas) parts.push(`${plural(fas, "detection", "detections")} with no matching incident.`);
  return parts.join(" ");
}

// A service's row: made once, then updated by every snapshot. Each chart host gets its
// height before anything in the row is drawn or measured: a host at 0 px would shrink the
// page for one layout, and the browser would clamp the scroll position to that.
function createRow() {
  const row = document.createElement("article");
  row.innerHTML = `
    <div class="rail">
      <div class="name-line"><h2></h2><span class="tier"></span></div>
      <div class="status"><span class="dot"></span><span></span></div>
      <p class="note"></p>
      <dl class="stats">
        <div><dt>Version</dt><dd></dd></div>
        <div><dt>Replicas</dt><dd></dd></div>
        <div><dt>Rate</dt><dd></dd></div>
        <div><dt>Errors</dt><dd></dd></div>
      </dl>
    </div>
    <div class="metrics"></div>`;
  const status = row.querySelector(".status");
  const [version, replicas, rate, errors] = row.querySelectorAll(".stats dd");
  row._ui = {
    name: row.querySelector("h2"), tier: row.querySelector(".tier"),
    status, statusText: status.lastElementChild, note: row.querySelector(".note"),
    version, replicas, rate, errors, charts: [],
  };
  const metrics = row.querySelector(".metrics");
  for (const kind of ["latency", "traffic"]) {
    const wrap = document.createElement("div");
    wrap.className = "metric";
    wrap.innerHTML = `<div class="m-label">${kind === "latency" ? "Latency" : "Traffic"}</div>
      <div class="chart-host"><div class="line-host"></div><div class="lane-host"></div></div>`;
    const line = wrap.querySelector(".line-host"), lane = wrap.querySelector(".lane-host");
    line.style.height = `${LINE_H}px`;
    line._draw = () => drawLine(line);
    lane._draw = () => drawChart(lane);
    ro.observe(line);
    ro.observe(lane);
    row._ui.charts.push({ kind, line, lane });
    metrics.appendChild(wrap);
  }
  return row;
}

function updateRow(row, svc) {
  const ui = row._ui, status = svc.status || "healthy";
  const models = ui.charts.map(c => metricModel(svc, c.kind));
  const note = railNote(models);
  setClass(row, `row ${status}`);
  setText(ui.name, svc.name);
  setText(ui.tier, svc.tier || "");
  setClass(ui.status, `status ${status}`);
  setText(ui.statusText, svc.statusText || "");
  setText(ui.note, note);
  ui.note.hidden = !note;
  setText(ui.version, `v${svc.version || "—"}`);
  setText(ui.replicas, svc.replicas != null ? String(svc.replicas) : "—");
  setText(ui.rate, formatRate(svc.rateRps));
  setText(ui.errors, formatPct(svc.errorRatePct));
  setClass(ui.errors, svc.errorRatePct != null && svc.errorRatePct >= 1 ? "hot" : "");
  ui.charts.forEach((c, i) => {
    c.line._m = c.lane._m = models[i];
    c.lane.style.height = `${laneHeight(models[i])}px`;
  });
  for (const c of ui.charts) { c.line._draw(); c.lane._draw(); }
}

// ---------- Summary / axis / legend ----------
// "3 incidents injected. 2 answered: detected a median of 16s after they started, answered 8s
// after detection. 1 missed and 1 false alarm." Every number comes from the snapshot.
function renderSummary(S = {}) {
  const b = (n, cls) => `<b${cls ? ` class="${cls}"` : ""}>${n}</b>`;
  const injected = S.injected || 0, answered = S.answered || 0;
  const ttd = S.medianTimeToDetect, tta = S.medianTimeToAnswer;
  let html = `${b(injected)} ${injected === 1 ? "incident" : "incidents"} injected.`;
  if (injected) {
    html += ` ${b(answered)} answered`;
    if (answered) {
      const one = answered === 1, clauses = [];
      if (ttd != null) clauses.push(`detected ${one ? "" : "a median of "}${b(dur(ttd))} after ${one ? "it" : "they"} started`);
      if (tta != null) clauses.push(`answered ${one || ttd != null ? "" : "a median of "}${b(dur(tta))} after detection`);
      if (clauses.length) html += `: ${clauses.join(", ")}`;
    }
    html += ".";
  }
  const tail = [];
  if (S.pending) tail.push(`${b(S.pending)} still open`);
  if (S.missed) tail.push(`${b(S.missed, "miss")} missed`);
  if (S.falseAlarms) tail.push(`${b(S.falseAlarms)} ${S.falseAlarms === 1 ? "false alarm" : "false alarms"}`);
  if (tail.length) html += ` ${andList(tail)}.`;
  setHTML(document.getElementById("summary"), html);
}

function renderAxis() {
  const axis = document.getElementById("axis");
  if (axis.dataset.rendered === "1") return;
  for (let i = 0; i <= 5; i++) {
    const sec = i * AXIS / 5;
    const span = document.createElement("span");
    span.style.left = `${(sec / AXIS) * 100}%`;
    span.textContent = i === 5 ? "now" : `−${Math.round((AXIS - sec) / 60)}m`;
    axis.appendChild(span);
  }
  axis.dataset.rendered = "1";
}

function renderLegend() {
  const lg = document.getElementById("legend");
  if (lg.dataset.rendered === "1") return;
  const swatch = (style) => `<svg width="18" height="10"><rect width="18" height="10" rx="2" style="${style}"/></svg>`;
  lg.innerHTML = `
    <span class="item"><svg width="22" height="12"><path d="M 1 9 L 5 6 L 8 8 L 12 3 L 15 7 L 18 5 L 21 6" stroke="var(--metric)" stroke-width="1.25" fill="none" stroke-linejoin="round"/></svg>Value per second</span>
    <span class="item"><svg width="8" height="14"><line x1="4" x2="4" y1="0" y2="14" stroke="var(--truth)" stroke-width="1.5"/></svg>Incident starts</span>
    <span class="item">${swatch("fill:var(--truth)")}Injected (up)</span>
    <span class="item">${swatch("fill:var(--detect)")}Detected → answered (down)</span>
    <span class="item"><svg width="22" height="12"><path d="M 1 2 V 10 M 1 6 H 21 M 21 2 V 10" stroke="var(--text)" stroke-width="1.25" fill="none"/></svg>Time to detect</span>`;
  lg.dataset.rendered = "1";
}

// ---------- Tooltip ----------
// A bar's tooltip follows the bar node, which outlives the snapshots: each one refreshes the
// words (an answer arrived, the times moved) or hides the tooltip once the bar has left.
const tip = document.getElementById("tip");
let tipNode = null;  // the bar the tooltip is on; null when hidden or on a line chart
function placeTip(x, y) {
  const r = tip.getBoundingClientRect();
  let left = x + 14, top = y + 16;
  if (left + r.width > innerWidth - 8) left = x - r.width - 14;
  if (top + r.height > innerHeight - 8) top = y - r.height - 12;
  tip.style.left = `${Math.max(8, left)}px`;
  tip.style.top = `${Math.max(8, top)}px`;
}
function showTip(node, x, y) {
  delete tip.dataset.owner;
  tipNode = node;
  setHTML(tip, node.getAttribute("data-tip"));
  tip.classList.add("on");
  placeTip(x, y);
}
function hideTip() {
  tipNode = null;
  tip.classList.remove("on");
}
function refreshTip() {
  if (!tipNode) return;
  if (tipNode.isConnected) setHTML(tip, tipNode.getAttribute("data-tip"));
  else hideTip();
}
const tipTarget = e => e.target.closest?.("[data-tip]");
document.addEventListener("pointerover", e => { const n = tipTarget(e); if (n) showTip(n, e.clientX, e.clientY); });
document.addEventListener("pointermove", e => { if (tipNode && tipTarget(e)) placeTip(e.clientX, e.clientY); });
document.addEventListener("pointerout", e => { if (tipTarget(e)) hideTip(); });
document.addEventListener("focusin", e => {
  const n = tipTarget(e); if (!n) return;
  const r = n.getBoundingClientRect();
  showTip(n, r.left + r.width / 2, r.bottom);
});
document.addEventListener("focusout", hideTip);

// ---------- Render ----------
// One ResizeObserver for every chart host: a new width redraws that chart.
const ro = new ResizeObserver(entries => {
  for (const e of entries) {
    const h = e.target, w = Math.floor(e.contentRect.width);
    if (!h.isConnected) { ro.unobserve(h); continue; }
    if (h._w !== w) { h._w = w; h._draw(); }
  }
});

function render(D) {
  AXIS = D.axisSeconds || 600;
  renderAxis();
  renderLegend();
  sync(document.getElementById("rows"), D.services || [], svc => svc.name, createRow, updateRow);
  renderSummary(D.summary || {});
  setText(document.getElementById("updated"),
    new Date((D.updatedAt || Date.now() / 1000) * 1000)
      .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
  refreshTip();
}
