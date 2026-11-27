"use strict";

/*  Ops wall — render code, driven by the live /ws snapshot (D-054).
 *
 *  The wall's state does all the counting; this page only draws what the snapshot says.
 *  The frame is the target design's (D-068): a header, then one full-width row per service
 *  in the snapshot's order. A row is its main panel (name and status chip on the left, a
 *  detail slot on the right, then its body) and its Issues panel (svc.issues: injected,
 *  detected of injected with the missed line, the median times to detect and to remediate; the
 *  security row words them its own way), which moves below the row on a narrow screen. svc.row
 *  says which kind of row (ROW_KINDS, under "Rows", holds what differs between them):
 *  - "releases" (web-gateway, the design's deploy row) draws the laptop, the line and the
 *    Releases list on the left and its Errors chart (D-067) on the right, with "Running v…"
 *    in the header (see "Releases row" below);
 *  - "windows" (payments, the design's aggregate row) draws the service, the Σ box of its
 *    10 s window and the Aggregated records on the left and its Latency and Traffic charts on
 *    the right, with "Window 10s" in the header (see "Windows row" below);
 *  - "tickets" (checkout) draws the ticket sources, the list and the timeline of tickets
 *    and agent runs instead (see "Ticket row" below);
 *  - "security" ("Code security incidents", D-070, last: no service) draws the laptop with
 *    "git" under it, the line and the Scan findings list on the left, and a timeline of its own,
 *    findings and agent runs with an external lane, in one SVG (see "Security row" below).
 *  Every row's time axis is the last 2 minutes (D.chartSeconds), the design's "−2m … now".
 *  A chart is the design's: a title with the current value (the latest complete second),
 *  then the plot, back to front (D-066):
 *  - a translucent box per injected incident on that metric, the full plot height, from its
 *    start to its end (to the right edge while open); a missed one is dashed
 *  - the design's dashed grid lines and their labels
 *  - a line at every deploy and rollback (svc.deploys), good deploys included (D-067): a
 *    deploy is the design's deploy marker, accent and dashed with its version at the top; a
 *    rollback is grey, labelled "rollback"
 *  - the raw service_metrics line (D-061) over its area: one point per second on a log y
 *    scale fitted to what's visible; on the errors chart, the share of each second's
 *    requests that failed on a fixed linear 0-100 % scale, with lab 2.2's threshold as a
 *    dashed line, and a gap for a second without requests
 *  - under the plot, a marker strip with one set of marks per detection block: a dot where
 *    the detector flagged it (hollow when no incident owns it: a false alarm), a line from it
 *    to the agent's first decision (to "now" while it waits), and an amber diamond at that
 *    decision, one row below the dot so the two never cover each other
 *  The rows' status and Issues numbers come ready-made as well; what each mark means is in
 *  its tooltip.
 *  All positions are axis-relative seconds (0 = left edge, chartSeconds = now). The DOM is
 *  made once and updated in place by every snapshot (D-062): a row per service, a box per
 *  incident and a mark group per block, kept by key; a ResizeObserver redraws a chart whose
 *  width changes.
 */

// ---------- WebSocket ----------
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const conn = document.getElementById("conn"), label = conn.querySelector(".conn-label");
  ws.onopen = () => { label.textContent = "Live"; conn.className = "conn live"; };
  ws.onclose = () => {
    label.textContent = "Reconnecting…"; conn.className = "conn offline";
    setTimeout(connect, 1500);
  };
  ws.onmessage = (e) => {
    try { render(JSON.parse(e.data)); }
    catch (err) { console.error("bad snapshot", err); }
  };
}
connect();

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
const setClass = (el, cls) => { if (el.getAttribute("class") !== cls) el.setAttribute("class", cls); };
const setAttr = (el, k, v) => { if (el.getAttribute(k) !== v) el.setAttribute(k, v); };
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
let AXIS = 120;  // D.chartSeconds: every row's time axis, the last 2 minutes
let UPDATED = 0;  // the snapshot's updatedAt, epoch seconds
const ago = sec => sec >= AXIS ? "now" : `−${dur(AXIS - sec)}`;
const range = (a, b) => `${ago(a)} to ${ago(b)}, ${dur(Math.min(b, AXIS) - a)}`;
const human = t => ({
  BAD_DEPLOY: "Bad deploy",
  BAD_DEPLOY_ERRORS: "Bad deploy (errors)",
  UPSTREAM_DROP: "Upstream drop",
}[t] || t.toLowerCase().replace(/_/g, " ").replace(/^./, c => c.toUpperCase()));
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
// Agent output (target, reason) ends up in a tooltip's innerHTML.
const esc = v => String(v).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const clip = (text, n) => text.length > n ? `${text.slice(0, n - 1).trimEnd()}…` : text;

let ACTION = {};  // the snapshot's actionWords: every action in words, as the responder's bars read it

// ---------- Model ----------
// A chart's model is a filter of the snapshot: one service's incidents and blocks for one
// metric ("latency", "traffic" or "errors"). Nothing is matched or counted here; the state
// did that, the error share included.
const VALUE = { latency: r => r.latency, traffic: r => r.traffic, errors: r => r.errorPct };
function metricModel(svc, kind) {
  const byId = new Map((svc.groundTruth || []).map(g => [g.incidentId, g]));
  return {
    kind,
    truth: (svc.groundTruth || []).filter(g => g.metric === kind),
    blocks: (svc.blocks || []).filter(b => b.metric === kind).sort((a, b) => a.start - b.start),
    incidentOf: b => (b.incidentId == null ? null : byId.get(b.incidentId) || null),
    key: `${svc.name}/${kind}`,
    records: svc.metrics || [],  // service_metrics samples on the axis, oldest first
    value: VALUE[kind],          // a record's value on this chart; null leaves a gap
    deploys: svc.deploys || [],  // version changes on the axis, drawn on every chart of the row
  };
}

// ---------- Words ----------
// A chart's title, as the design writes it ("p95" becomes "mean": the samples are each second's mean).
const METRIC = { latency: "Latency, mean", traffic: "Traffic", errors: "Error rate, failed requests over total requests" };
const TRIGGER = {
  LATENCY_HIGH: "Latency high", TRAFFIC_HIGH: "Traffic high", TRAFFIC_LOW: "Traffic low",
  ERROR_RATE_HIGH: "Error rate high",
};
const triggerLabel = b => TRIGGER[b.trigger]
  || (b.metric === "latency" ? "Latency high" : b.metric === "errors" ? "Error rate high"
    : b.direction === "down" ? "Traffic low" : "Traffic high");
const actionLabel = a => esc(ACTION[a] || human(a || "unknown"));
const actionWord = a => actionLabel(a).toLowerCase();

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

// A deploy mark's words: "Deploy: 3.4.3 → 3.4.4", and how long ago.
function deployTip(d) {
  const what = d.kind === "ROLLBACK" ? "Rollback" : "Deploy";
  return `<strong>${what}: ${esc(d.previousVersion ?? "?")} → ${esc(d.version ?? "?")}</strong>${ago(d.t)}`;
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

// ---------- Scale ----------
// The latency and traffic scales are logarithmic: the incidents are ratios (x5, x0.1, x4),
// and on a log scale a ratio has the same height wherever the service's normal level sits.
// Each runs from a round number just below the smallest visible value above 0 to one just
// above the largest, at least x10 apart so a quiet chart doesn't blow plain noise up. A
// second with 0 requests sits on the bottom edge (D-061). The errors scale is fixed and
// linear, 0 to 100 %, so the threshold line never moves (D-067).
const UNIT = { latency: "ms", traffic: "rps" };
const num = v => `${+v.toPrecision(3)}`;
let THRESHOLD = 25;  // errorThresholdPct from the snapshot: lab 2.2's rule
function valueText(kind, r) {
  if (kind === "errors") {
    return r.errorPct == null ? "No requests" : `${r.errorPct.toFixed(1)} % of ${plural(r.traffic, "request", "requests")}`;
  }
  const v = r[kind];
  return kind === "latency" ? `${v >= 100 ? Math.round(v) : v.toFixed(1)} ms` : `${v} rps`;
}

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

// The value in a chart's title: the latest second with a value, in the design's words.
function nowText(kind, recs, value) {
  for (let i = recs.length - 1; i >= 0; i--) {
    const v = value(recs[i]);
    if (v == null) continue;
    return kind === "errors" ? `${v.toFixed(1)}%` : kind === "latency" ? `${Math.round(v)} ms` : `${v} req/s`;
  }
  return "—";
}

// The errors scale labels its top and middle grid lines on the plot, as the design does, and
// the threshold; a log scale names its range in the chart's title line instead (the design's
// latency and traffic plots carry no labels, and a label on the plot would cover its marks).
function errorScale() {
  return { frac: v => Math.max(0, Math.min(1, v / 100)), labels: [[100, "100%"], [50, "50%"]], range: "" };
}

// y as a fraction of the plot height (0 = bottom, 1 = top), and the range's label.
function logScale(kind, values) {
  const positive = values.filter(v => v > 0);
  const hi = ceilNice(positive.length ? Math.max(...positive) : 1);
  let lo = floorNice(positive.length ? Math.min(...positive) : hi / 10);
  if (hi / lo < 10) lo = floorNice(hi / 10);
  const span = Math.log(hi / lo);
  return {
    frac: v => Math.max(0, Math.log(Math.max(v, lo) / lo) / span),
    labels: [],
    range: `${num(lo)}–${num(hi)} ${UNIT[kind]} · log`,
  };
}

// ---------- Chart ----------
// The title line (HTML), then the plot at the design's height, then the marker strip:
// detection dots on one row, answer diamonds on the next (an answer lands about a second
// after its detection, two pixels apart). The design's grid lines: top and middle on the
// errors chart, the middle on latency, none on traffic.
const PLOT_H = { errors: 200, latency: 112, traffic: 64 };
const GRID = { errors: [1, 0.5], latency: [0.5], traffic: [] };
const STRIP_H = 26;
const chartHeight = kind => (PLOT_H[kind] || 112) + STRIP_H;
const DOT_R = 3.5, DIAMOND_R = 4.5;
const LABEL_CHAR = 6.6;  // about the width of one Geist Mono character at 11 px
const diamond = (x, y, r) => `M ${x} ${y - r} L ${x + r} ${y} L ${x} ${y + r} L ${x - r} ${y} Z`;

// The pointer over a plot. The data moves under a still pointer every second, so each
// redraw puts the crosshair and the tooltip on the second now under it.
let lineHover = null;  // { key, x, y }
function hideLineTip() {
  if (tip.dataset.owner === "line") tip.classList.remove("on");
  document.querySelectorAll(".chart .crosshair").forEach(g => g.setAttribute("visibility", "hidden"));
}
document.addEventListener("pointermove", e => {
  const hit = e.target.closest?.(".chart .hit");
  if (hit && hit._show) { lineHover = { key: hit._key, x: e.clientX, y: e.clientY }; hit._show(e.clientX, e.clientY); }
  else if (lineHover) { lineHover = null; hideLineTip(); }
});
document.documentElement.addEventListener("pointerleave", () => { lineHover = null; hideLineTip(); });

// Measured on the first draw, then kept current by the ResizeObserver.
function hostWidth(host) {
  if (host._w == null) host._w = Math.floor(host.clientWidth);
  return Math.max(160, host._w);
}

// A chart's <svg> and its layers (<g>s) are made on its first draw and kept. A redraw empties
// the layer that only shows things and updates in place the nodes the pointer or the focus
// can be on: the hit area, the boxes, the mark groups.
function chartLayers(host) {
  if (!host._svg) {
    host._svg = s("svg", { class: "chart" }, host);
    host._layers = Object.fromEntries(["boxes", "plot", "deploys", "labels", "hover", "marks"].map(n => [n, s("g", {}, host._svg)]));
  }
  return host._layers;
}

function drawChart(host) {
  const m = host._m;
  if (!m) return;
  const w = hostWidth(host);
  const X = t => (Math.max(0, Math.min(t, AXIS)) / AXIS) * w;
  const recs = m.records.filter(r => r.t >= 0 && r.t <= AXIS);
  const scale = m.kind === "errors" ? errorScale() : logScale(m.kind, recs.map(m.value));
  const PLOT_TOP = 0, plotH = PLOT_H[m.kind] || 112, PLOT_BOTTOM = PLOT_TOP + plotH, CHART_H = PLOT_BOTTOM + STRIP_H;
  const DET_Y = PLOT_BOTTOM + 9, ANS_Y = PLOT_BOTTOM + 20;
  const Y = v => PLOT_TOP + plotH * (1 - scale.frac(v));
  if (host._now) setText(host._now, nowText(m.kind, recs, m.value));
  if (host._range) setText(host._range, recs.length ? scale.range : "");

  const { boxes, deploys, plot, labels, hover, marks } = chartLayers(host);
  attrs(host._svg, { width: w, height: CHART_H, viewBox: `0 0 ${w} ${CHART_H}` });

  // The injected incidents on this metric (INCIDENT_METRIC in the state), by incident id.
  sync(boxes, m.truth, t => t.incidentId, () => s("rect", { tabindex: 0, rx: 2 }), (node, t) => {
    const x0 = X(t.start), x1 = X(t.end);
    attrs(node, {
      class: t.verdict === "missed" ? "box missed" : "box",
      x: x0, y: PLOT_TOP, width: Math.max(2, x1 - x0), height: plotH,
      "data-tip": incidentTip(t),
    });
  });

  // The grid lines, the baseline, the threshold (errors chart), the area and the line. A
  // missing second (the simulator stopped), or one without requests on the errors chart,
  // breaks the line and its area instead of bridging them.
  plot.replaceChildren();
  for (const f of GRID[m.kind] || []) {
    const y = PLOT_TOP + Math.round(plotH * (1 - f)) + 0.5;
    s("line", { class: "grid", x1: 0, x2: w, y1: y, y2: y }, plot);
  }
  s("line", { class: "baseline", x1: 0, x2: w, y1: PLOT_BOTTOM + 0.5, y2: PLOT_BOTTOM + 0.5 }, plot);
  labels.replaceChildren();  // the scale's labels, over the deploy lines
  if (m.kind === "errors") {
    const y = Y(THRESHOLD);
    s("line", { class: "threshold", x1: 0, x2: w, y1: y, y2: y }, plot);
    s("text", { class: "scale threshold-label", x: 0, y: y + 4, "dominant-baseline": "hanging" }, labels)
      .textContent = `${num(THRESHOLD)}%`;
  }
  const segments = [];
  let prev = null;
  for (const r of recs) {
    const v = m.value(r);
    if (v == null) { prev = null; continue; }
    if (prev == null || r.t - prev > 1.5) segments.push([]);
    segments[segments.length - 1].push([X(r.t), Y(v)]);
    prev = r.t;
  }
  const xy = ([x, y]) => `${x.toFixed(1)} ${y.toFixed(1)}`;
  const line = segments.map(seg => `M ${seg.map(xy).join(" L ")}`).join(" ");
  if (line && m.kind !== "traffic") {
    const area = segments.map(seg => `M ${seg[0][0].toFixed(1)} ${PLOT_BOTTOM} L ${seg.map(xy).join(" L ")} ` +
      `L ${seg[seg.length - 1][0].toFixed(1)} ${PLOT_BOTTOM} Z`).join(" ");
    s("path", { class: "area", d: area }, plot);
  }
  if (line) s("path", { class: `series ${m.kind}`, d: line }, plot);
  // The scale's labels, the design's way: at the left, just under their grid line.
  for (const [v, text] of recs.length ? scale.labels : []) {
    s("text", { class: "scale", x: 0, y: Y(v) + 4, "dominant-baseline": "hanging" }, labels).textContent = text;
  }
  if (!recs.length) {
    s("text", { class: "scale", x: 0, y: PLOT_TOP + 4, "dominant-baseline": "hanging" }, labels).textContent = "No records yet";
  }

  // The version changes (D-067): a line across the plot at each deploy and rollback, a
  // keyboard stop with its own tooltip; the pointer gets the same words on the line's. A
  // deploy's version sits at the top just left of its line, as the design's markers do; a
  // rollback's label just right of it, so the two of a bad deploy, seconds apart, stay clear
  // of each other. Only a row's top chart labels them, as in the design; the lines below
  // carry the same tooltips. The versions are placed first, then the rollbacks, newest first: a label
  // that would cover one already placed moves to the other side of its line, or hides if it
  // fits there neither (its tooltip still says it).
  const deployLabel = d => (d.kind === "ROLLBACK" ? "rollback" : `v ${d.version ?? "?"}`);
  sync(deploys, m.deploys, d => d.key, () => {
    const g = s("g", { tabindex: 0 });
    s("line", { class: "deploy-line" }, g);
    s("text", { class: "deploy-label", "dominant-baseline": "hanging" }, g);
    return g;
  }, (g, d) => {
    const x = X(d.t);
    setClass(g, d.kind === "ROLLBACK" ? "deploy rollback" : "deploy");
    g.setAttribute("data-tip", deployTip(d));
    attrs(g.firstChild, { x1: x, x2: x, y1: PLOT_TOP, y2: PLOT_BOTTOM });
    attrs(g.lastChild, { y: PLOT_TOP + 4 });
    setText(g.lastChild, deployLabel(d));
  });
  const topLabel = recs.length ? scale.labels.find(([v]) => Y(v) <= PLOT_TOP + 1) : null;
  const minX = topLabel ? topLabel[1].length * LABEL_CHAR + 8 : 0;  // the scale's label has the top left
  const taken = [], newest = m.deployLabels === false ? [] : m.deploys.map((d, i) => i).reverse();
  if (m.deployLabels === false) for (const g of deploys.children) g.lastChild.setAttribute("visibility", "hidden");
  const order = [...newest.filter(i => m.deploys[i].kind !== "ROLLBACK"), ...newest.filter(i => m.deploys[i].kind === "ROLLBACK")];
  for (const i of order) {
    const d = m.deploys[i], x = X(d.t), width = deployLabel(d).length * LABEL_CHAR;
    const sides = d.kind === "ROLLBACK" ? ["right", "left"] : ["left", "right"];
    const fits = side => {
      const [x0, x1] = side === "left" ? [x - 6 - width, x - 6] : [x + 6, x + 6 + width];
      return x0 >= minX && x1 <= w && !taken.some(([a, b]) => x0 < b + 8 && a < x1 + 8) && [x0, x1];
    };
    const side = sides.find(fits), label = deploys.children[i].lastChild;
    label.setAttribute("visibility", side ? "inherit" : "hidden");
    if (!side) continue;
    taken.push(fits(side));
    attrs(label, { x: side === "left" ? x - 6 : x + 6, "text-anchor": side === "left" ? "end" : "start" });
  }

  // Hover: a hit area over the label band and the plot; the crosshair snaps to the nearest
  // second, and inside an incident's box the tooltip tells that incident as well.
  if (!hover.firstChild) {
    const cross = s("g", { class: "crosshair", visibility: "hidden" }, hover);
    s("line", { class: "cross", y1: PLOT_TOP, y2: PLOT_BOTTOM }, cross);
    s("circle", { class: "cross-dot", r: 3.5 }, cross);
    s("rect", { class: "hit", x: 0, y: 0, height: PLOT_BOTTOM }, hover);
  }
  const [cross, hit] = hover.children, [cl, cd] = cross.children;
  attrs(cl, { y1: PLOT_TOP, y2: PLOT_BOTTOM });
  attrs(hit, { width: w, height: PLOT_BOTTOM });
  hit._key = m.key;
  hit._show = (clientX, clientY) => {
    if (!recs.length) return;
    const t = ((clientX - host._svg.getBoundingClientRect().left) / w) * AXIS;
    let i = 0, j = recs.length - 1;
    while (i < j) { const mid = (i + j) >> 1; if (recs[mid].t < t) i = mid + 1; else j = mid; }
    const r = i > 0 && t - recs[i - 1].t < recs[i].t - t ? recs[i - 1] : recs[i];
    const x = X(r.t), v = m.value(r);
    attrs(cl, { x1: x, x2: x });
    // "inherit", not "visible": a visible child would still show once the group is hidden
    attrs(cd, { cx: x, cy: Y(v ?? 0), visibility: v == null ? "hidden" : "inherit" });
    cross.setAttribute("visibility", "visible");
    const during = m.truth.find(g => r.t >= g.start && r.t <= g.end);
    const deploy = m.deploys.find(dp => Math.abs(dp.t - r.t) <= 1.5);
    tipNode = null;
    setHTML(tip, `<strong>${valueText(m.kind, r)}</strong>${ago(r.t)}` +
      (deploy ? `<span class="rule"></span>${deployTip(deploy)}` : "") +
      (during ? `<span class="rule"></span>${incidentTip(during)}` : ""));
    tip.dataset.owner = "line";
    tip.classList.add("on");
    placeTip(clientX, clientY);
  };
  if (lineHover && lineHover.key === m.key) hit._show(lineHover.x, lineHover.y);

  // The marker strip: one group per block (by its key), from detection to the agent's answer.
  sync(marks, m.blocks, b => b.key, () => {
    const g = s("g", { tabindex: 0 });
    s("rect", { class: "mark-hit" }, g);
    s("path", { class: "link" }, g);
    s("circle", { class: "det", r: DOT_R }, g);
    s("path", { class: "ans" }, g);
    return g;
  }, (g, b) => {
    const [hitArea, link, det, ans] = g.children;
    const x0 = X(b.start), x1 = b.open ? w : Math.max(x0, X(b.end));
    setClass(g, b.incidentId == null ? "mark false-alarm" : "mark");
    g.setAttribute("data-tip", blockTip(m, b));
    attrs(hitArea, { x: x0 - 7, y: PLOT_BOTTOM + 1, width: x1 - x0 + 14, height: CHART_H - PLOT_BOTTOM - 1 });
    attrs(det, { cx: x0, cy: DET_Y });
    if (b.open) {
      attrs(link, { d: `M ${x0} ${DET_Y} H ${x1}` });
      ans.setAttribute("visibility", "hidden");
    } else {
      attrs(link, { d: `M ${x0} ${DET_Y} H ${x1} V ${ANS_Y}` });
      attrs(ans, { d: diamond(x1, ANS_Y, DIAMOND_R), visibility: "visible" });
    }
  });
}

// ---------- Rows ----------
// Every row's time axis, the design's: the last 2 minutes (AXIS seconds) in five steps, i from 0
// (the left edge) to 4 ("now"). The security row's timeline draws the same labels in its SVG.
function axisLabel(i) {
  const sec = AXIS * (4 - i) / 4;
  return i === 4 ? "now" : sec >= 120 && sec % 60 === 0 ? `−${sec / 60}m` : `−${sec}s`;
}
function axisHTML() {
  return `<div class="axis" aria-hidden="true">${[0, 1, 2, 3, 4].map(i => `<span>${axisLabel(i)}</span>`).join("")}</div>`;
}

// A service's row: made once, then updated by every snapshot. Each chart host gets its
// height before anything in the row is drawn or measured: a host at 0 px would shrink the
// page for one layout, and the browser would clamp the scroll position to that.
function createRow() {
  const row = document.createElement("div");
  row.innerHTML = `
    <section class="panel frame row-main">
      <header class="row-head">
        <div class="who">
          <h2></h2>
          <span class="status"><span class="dot"></span><span class="status-text"></span></span>
        </div>
        <div class="detail"></div>
      </header>
      <div class="row-body">
        <div class="charts"></div>
        ${axisHTML()}
      </div>
    </section>
    <aside class="panel frame issues">
      <div class="issues-head"><h3>Issues</h3><span>Last 10 min</span></div>
      <div class="stats">
        <div class="stat">
          <span class="stat-label"></span>
          <span class="stat-value" data-k="injected"></span>
        </div>
        <div class="stat">
          <span class="stat-label"></span>
          <span class="stat-line"><span class="stat-value" data-k="detected"></span><span class="stat-of"></span></span>
          <span class="stat-missed"></span>
        </div>
        <div class="stat">
          <span class="stat-label"></span>
          <span class="stat-value" data-k="medianTimeToDetect"></span>
        </div>
        <div class="stat remediate">
          <span class="stat-label"></span>
          <span class="stat-value" data-k="medianTimeToRemediate"></span>
        </div>
      </div>
    </aside>`;
  const status = row.querySelector(".status");
  row._ui = {
    main: row.querySelector(".row-main"), issues: row.querySelector(".issues"),
    name: row.querySelector("h2"), status, statusText: status.lastElementChild,
    stat: Object.fromEntries([...row.querySelectorAll(".stat-value")].map(n => [n.dataset.k, n])),
    labels: [...row.querySelectorAll(".stat-label")], of: row.querySelector(".stat-of"),
    missed: row.querySelector(".stat-missed"), remediate: row.querySelector(".stat.remediate"),
    charts: [], chartsKey: null,
  };
  return row;
}

// The row's chart hosts, one per kind the snapshot names (svc.charts, D-067): made on the
// first update, before anything is drawn, and again only if the list changes. A row with
// no charts (checkout, which has no metrics: D-068; the security row) has no time axis either.
function ensureCharts(row, kinds) {
  const ui = row._ui, key = kinds.join(",");
  if (ui.chartsKey === key) return;
  ui.chartsKey = key;
  for (const c of ui.charts) ro.unobserve(c.host);
  row.querySelector(".row-body").hidden = kinds.length === 0;
  const charts = row.querySelector(".charts");
  charts.replaceChildren();
  ui.charts = kinds.map(kind => {
    const box = document.createElement("div");
    box.className = `chart-box ${kind}`;
    box.innerHTML = `<div class="chart-head"><span class="chart-title">${esc(METRIC[kind] || kind)}` +
      `<span class="chart-range"></span></span><span class="chart-now"></span></div><div class="chart-host"></div>`;
    const host = box.lastChild;
    host.style.height = `${chartHeight(kind)}px`;
    host._now = box.querySelector(".chart-now");
    host._range = box.querySelector(".chart-range");
    host._draw = () => drawChart(host);
    ro.observe(host);
    charts.appendChild(box);
    return { kind, host };
  });
}

// What differs between the kinds of row (svc.row, see the top of this file), in one place: the
// row's class, whether it draws the snapshot's charts (a row without them has no time axis
// either), what makes and updates its body (made on its first update), and its Issues panel's
// words, the four labels top to bottom and the tooltip on the time to remediate. A kind the page
// doesn't know draws the frame and its charts.
const INCIDENT_WORDS = {
  labels: ["Injected", "Detected", "Median time to detect", "Median time to remediate"],
  remediate: "Measured from the incident's start to the agent's answer",
};
const ROW_KINDS = {
  releases: { cls: "releases-row", charts: true, update: updateReleaseRow, issues: INCIDENT_WORDS },
  windows: { cls: "windows-row", charts: true, update: updateWindowsRow, issues: INCIDENT_WORDS },
  tickets: {
    cls: "ticket-row", charts: false, update: updateTicketRow,
    issues: { ...INCIDENT_WORDS, remediate: "Measured from the ticket's creation to the responder's answer" },
  },
  security: {  // critical findings and decisions, not incidents: spec decision 26's words
    cls: "security-row", charts: false, update: updateSecurityRow,
    issues: {
      labels: ["Critical findings", "Right decisions", "Median time to decide", "Median time to patch"],
      remediate: "Measured from the finding's scan to the patch agent's finish, patched findings only",
    },
  },
};
const OTHER_ROW = { cls: "", charts: true, update: () => {}, issues: INCIDENT_WORDS };

function updateRow(row, svc) {
  const ui = row._ui, status = svc.status || "healthy", kind = ROW_KINDS[svc.row] || OTHER_ROW;
  ensureCharts(row, kind.charts ? svc.charts || ["latency", "traffic"] : []);
  setClass(row, kind.cls ? `svc-row ${kind.cls} ${status}` : `svc-row ${status}`);
  setText(ui.name, svc.name);
  setAttr(ui.main, "aria-label", svc.name);
  setAttr(ui.issues, "aria-label", `${svc.name} issues in the last 10 minutes`);
  setClass(ui.status, `status ${status}`);
  setText(ui.statusText, svc.statusText || "");
  renderIssues(ui, svc.issues || {}, kind.issues);
  kind.update(row, svc);
  ui.charts.forEach((c, i) => {
    c.host._m = { ...metricModel(svc, c.kind), deployLabels: i === 0 };  // the design labels the top chart's markers
    c.host._draw();
  });
}

// A design row's body: its left column (sources, flow, list), and the row's charts with their
// axis moved into the right column. Its header's detail: a label and a mono value.
function flowBody(row, cls, left, label) {
  const body = document.createElement("div");
  body.className = `flow-body ${cls}`;
  body.innerHTML = `<div class="flow-left">${left}</div><div class="chart-col"></div>`;
  row.querySelector(".row-main").appendChild(body);
  body.querySelector(".chart-col").appendChild(row.querySelector(".row-body"));
  const detail = row.querySelector(".detail");
  detail.innerHTML = `<span>${label} <span class="detail-value"></span></span>`;
  return { body, value: detail.querySelector(".detail-value"), q: sel => body.querySelector(sel) };
}

// ---------- Arrivals ----------
// How a list row takes in what the snapshot brings (D-068): an item whose key the page hasn't
// seen arrives, unless it is the first snapshot or the item is older than the row allows (a
// backread). An arrival fades its list row in, once, and may ring its source box; each
// animation replays by class.
const FRESH_SECONDS = 10;  // a new id this old or younger animates; an older one (a backread) doesn't
const FADE_MS = 4200;      // the design's fade on a new list row

// The items of this snapshot's list that arrive, by key and age; holder.seen (null before the
// first snapshot) becomes the list's keys.
function freshItems(holder, list, keyOf, timeOf, maxAge = FRESH_SECONDS) {
  const fresh = holder.seen ? list.filter(x => !holder.seen.has(keyOf(x)) && UPDATED - timeOf(x) <= maxAge) : [];
  holder.seen = new Set(list.map(keyOf));
  return fresh;
}

// A new list row's fade, once: the row's class says " fresh" while n._fresh holds.
function fadeIn(n, ms = FADE_MS) {
  n._fresh = true;
  setTimeout(() => { n._fresh = false; n.classList.remove("fresh"); }, ms);
}

// A source box rings, lit ("busy") for a second.
function ring(box) {
  box.classList.add("busy");
  replay(box, "ring");
  clearTimeout(box._busy);
  box._busy = setTimeout(() => box.classList.remove("busy"), 1000);
}

// Replay a CSS animation: the class flips between its -a and -b twins, which run the same
// keyframes, so the animation starts again (the design's way).
function replay(el, base) {
  const next = el.classList.contains(`${base}-a`) ? `${base}-b` : `${base}-a`;
  el.classList.remove(`${base}-a`, `${base}-b`);
  el.classList.add(next);
}

// ---------- Releases row ----------
// A row whose snapshot says row: "releases" (web-gateway, the design's deploy row, D-068):
// the laptop, a line with the ambient flow on to the Releases list, and the row's charts on
// the right; "Running v…" in the header. The state picked the releases and counted them:
// the newest deploys of the last 10 minutes, each with its date and, once the agent rolled it
// back, how long it ran. A release key the page hasn't seen (not on the first snapshot, and
// at most FRESH_SECONDS old) lights the laptop, sends a packet down the line and pushes its
// row in with the design's fade; a rollback replays the fade on the row it changes. Under
// prefers-reduced-motion nothing moves (CSS).
const LAPTOP = '<rect x="4" y="5" width="16" height="11" rx="1.5"></rect><path d="M2 19h20"></path>';
const ARROW = '<svg width="7" height="9" viewBox="0 0 7 9" class="flow-arrow"><path d="M1 1l4.5 3.5L1 8"></path></svg>';
const flowDots = (cls, delays) => delays.map(d => `<span class="${cls}" style="animation-delay: ${d}"></span>`).join("");

function createReleaseBody(row) {
  const { body, value, q } = flowBody(row, "rl-body", `
      <div class="src-box rl-laptop">
        <svg role="img" aria-label="Developer laptop" width="18" height="18" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${LAPTOP}</svg>
      </div>
      <div class="flow-line rl-line" aria-hidden="true">
        ${flowDots("flow", ["0s", ".4s", ".8s", "1.2s"])}
        <span class="rl-packet"></span>
        ${ARROW}
      </div>
      <div class="list">
        <div class="list-head"><span class="list-title">Releases</span><span class="list-count"></span></div>
        <div class="list-view rl-view" aria-live="polite"><div class="rl-rows"></div></div>
      </div>`, "Running");
  row._rl = { body, running: value, laptop: q(".rl-laptop"), packet: q(".rl-packet"), rows: q(".rl-rows"),
              count: q(".list-count"), seen: null };  // the release keys on the previous snapshot; null before the first
}

function updateReleaseRow(row, svc) {
  if (!row._rl) createReleaseBody(row);
  const rl = row._rl, releases = svc.releases || {}, list = releases.newest || [];
  setText(rl.running, svc.version ? `v ${svc.version}` : "—");
  setText(rl.count, `${releases.count || 0} in 10 min`);

  // The keys this snapshot brings that the page hasn't seen: they animate (not on the first).
  const fresh = freshItems(rl, list, r => r.key, r => r.deployedAt);

  sync(rl.rows, list, r => r.key, r => {
    const n = document.createElement("div");
    n.innerHTML = '<span class="rl-version"></span><span class="rl-at"></span><span class="rl-back"></span>';
    if (fresh.includes(r)) fadeIn(n);  // as the packet lands
    return n;
  }, (n, r) => {
    const back = r.rolledBackAfter != null;
    setClass(n, `rl-row${back ? " rolled-back" : ""}${n._fresh ? " fresh" : ""}`);
    if (back && n._back === false && !n._fresh) replay(n, "rback");  // rolled back since the last snapshot
    n._back = back;
    const [version, at, rolled] = n.children;
    setText(version, `v ${r.version ?? "?"}`);
    setText(at, `deployed at ${ticketTime(r.deployedAt)}`);
    setText(rolled, back ? `rolled back after ${r.rolledBackAfterText}` : "");
  });
  if (fresh.length) {
    replay(rl.rows, "rpush");
    ring(rl.laptop);
    replay(rl.packet, "travel");
  }
}

// ---------- Windows row ----------
// A row whose snapshot says row: "windows" (payments, the design's aggregate row, D-068). The
// design's cluster of five services is one node here, the service itself (there are no
// sub-services in our data), its ambient flow running into the Σ box of the window Flink
// aggregates (svc.windowSeconds, 10 s; the design's 5 s is not ours), then a line on to
// "Aggregated records": the newest service_health windows, each one line of the record's own
// JSON, cut at the panel's width, its tooltip the whole record. A status that flags the window
// and the value it judges (svc.windows.newest[].flags; the value comes first, so it shows on a
// line cut short) are accent and bold, and tint the line. A window the page hasn't
// seen (not on the first snapshot) flushes the box and starts its fill again, sends a packet
// out and pushes its line in with the design's fade: the arrival is the trigger, not the
// clock, since Flink writes a window a few seconds after it closed. The fill takes the
// window's length, and on the first snapshot starts where the newest window's detection
// left it. Under prefers-reduced-motion nothing moves (CSS).
const CHIP = '<rect x="4" y="4" width="16" height="16" rx="3"></rect><path d="M9 9h6v6H9z"></path>';
const WINDOW_FRESH_SECONDS = 30;  // a new window this long after its end or sooner animates
const LINE_KEYS = ["avg_latency_ms", "traffic_count", "version", "window_end", "latency_status", "traffic_status"];
const TIME_KEYS = new Set(["window_start", "window_end", "detected_at"]);
const clockOf = sec => { const d = new Date(sec * 1000); return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`; };
// A record's value as the line prints it: times as the local clock, numbers to 0.1, strings quoted.
function recordValue(key, v) {
  if (v == null) return { text: "null", cls: "k" };
  if (TIME_KEYS.has(key) && typeof v === "number") return { text: `"${clockOf(v)}"`, cls: "s" };
  if (typeof v === "number") return { text: Number.isInteger(v) ? String(v) : v.toFixed(1), cls: "v" };
  return { text: `"${esc(v)}"`, cls: "s" };
}
function recordLine(w) {
  const flags = new Set(w.flags || []), rec = w.record || {};
  const parts = LINE_KEYS.filter(k => k in rec).map(k => {
    const { text, cls } = recordValue(k, rec[k]);
    return `<span class="k">"${k}":</span><span class="${flags.has(k) ? "flag" : cls}">${text}</span>`;
  });
  return `<span class="k">{</span>${parts.join('<span class="k">,</span>')}<span class="k">}</span>`;
}
function recordTip(w) {
  const rec = w.record || {};
  const lines = Object.keys(rec).map(k => `${esc(k)}: ${recordValue(k, rec[k]).text}`);
  return `<strong>Window to ${clockOf(w.windowEnd)}</strong><span class="record">${lines.join("<br>")}</span>`;
}

function createWindowsBody(row, svc) {
  const name = esc(svc.name);
  const { body, value, q } = flowBody(row, "ag-body", `
      <div class="ag-cluster">
        <svg width="250" height="280" viewBox="0 0 250 280" aria-hidden="true"><path d="M40 140 H122" class="tk-path"></path></svg>
        <div class="src-box ag-node" title="${name}">
          <svg role="img" aria-label="${name}" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            stroke-width="1.6" stroke-linejoin="round">${CHIP}</svg>
        </div>
        <span class="ag-label">${name}</span>
        ${flowDots("flowc path-ag", ["0s", ".47s", ".93s"])}
        <div class="ag-sigma"><div class="ag-fill"></div><span class="ag-sum"><span>Σ</span><span class="ag-secs"></span></span></div>
      </div>
      <div class="flow-line ag-line" aria-hidden="true"><span class="ag-packet"></span>${ARROW}</div>
      <div class="list">
        <div class="list-head"><span class="list-title">Aggregated records</span><span class="list-count"></span></div>
        <div class="list-view ag-view" aria-live="polite"><div class="ag-rows"></div></div>
      </div>`, "Window");
  row._ag = { body, window: value, sigma: q(".ag-sigma"), fill: q(".ag-fill"), secs: q(".ag-secs"), packet: q(".ag-packet"),
              rows: q(".ag-rows"), count: q(".list-count"), seen: null };  // the window keys on the previous snapshot
}

function updateWindowsRow(row, svc) {
  if (!row._ag) createWindowsBody(row, svc);
  const ag = row._ag, windows = svc.windows || {}, list = windows.newest || [], secs = svc.windowSeconds || 10;
  setText(ag.window, `${secs}s`);
  setText(ag.secs, `${secs}s`);
  setAttr(ag.sigma, "title", `Aggregates every ${secs} seconds`);
  ag.sigma.style.setProperty("--window", `${secs}s`);
  setText(ag.count, `${windows.flagged || 0} flagged in 10 min`);

  // The keys this snapshot brings that the page hasn't seen: they animate (not on the first).
  const first = ag.seen === null;
  const fresh = freshItems(ag, list, w => w.key, w => w.windowEnd, WINDOW_FRESH_SECONDS);

  sync(ag.rows, list, w => w.key, w => {
    const n = document.createElement("div");
    n.innerHTML = '<div class="ag-json"></div>';
    if (fresh.includes(w)) fadeIn(n, 3800);  // as the packet lands, which is sooner here
    return n;
  }, (n, w) => {
    const flagged = (w.flags || []).length > 0;
    setClass(n, `ag-row${flagged ? " flagged" : ""}${n._fresh ? " fresh" : ""}`);
    setHTML(n.firstChild, recordLine(w));
    n.setAttribute("data-tip", recordTip(w));
  });

  if (first && list.length) {  // the fill picks up where the newest window's arrival left it
    const since = UPDATED - (list[0].record?.detected_at ?? list[0].windowEnd);
    ag.fill.style.animationDelay = `${-Math.max(0, Math.min(secs, since)).toFixed(1)}s`;
    replay(ag.fill, "fill");
  }
  if (fresh.length) {
    ag.fill.style.animationDelay = "0s";
    replay(ag.fill, "fill");
    replay(ag.sigma, "flush");
    ag.packet.dataset.flag = (fresh[0].flags || []).length ? "yes" : "no";
    replay(ag.packet, "out");
    replay(ag.rows, "jpush");
  }
}

// ---------- Ticket row ----------
// A row whose snapshot says row: "tickets" (checkout, D-068). Three columns, as in the target
// design: the two sources, whose curved paths merge and run on to the list; the list of the
// newest tickets; and "Tickets and agent runs", the last 2 minutes (D.chartSeconds) with
// a mark per ticket and one lane per agent. The state did the matching and the counting: the
// runs come with their positions, labels and words, the sources and the lanes with their names,
// and each lane with its state. A ticket id the page hasn't seen before lights its source, sends
// a dot down the path and into the list, and fades its row in; the ones on the first snapshot
// don't. Under prefers-reduced-motion nothing moves (CSS).
// Where the snapshot's two sources (svc.sources) are drawn, in its order: the design's ticket
// icon and upper path for the first, its bell and lower path for the second.
const SOURCE_LAYOUT = [
  { top: 70, labelTop: 72, path: "M40 90 H70 C105 90 110 140 145 140 H170",
    icon: '<path d="M4 7a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v2a2 2 0 0 0 0 4v2a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-2a2 2 0 0 0 0-4z"></path><path d="M14 6v2M14 11v2M14 16v1"></path>' },
  { top: 170, labelTop: 196, path: "M40 190 H70 C105 190 110 140 145 140 H170",
    icon: '<path d="M6 16v-5a6 6 0 0 1 12 0v5l1.5 2h-15z"></path><path d="M10 20.5a2 2 0 0 0 4 0"></path>' },
];
const pad2 = n => String(n).padStart(2, "0");
const ticketTime = sec => {  // "24.11.2026 09:26", local time
  const d = new Date(sec * 1000);
  return `${pad2(d.getDate())}.${pad2(d.getMonth() + 1)}.${d.getFullYear()} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
};
const prioClass = p => (p === "P1" || p === "P2" ? p.toLowerCase() : "p3");
// The ticket row's timeline (see updateTimeline): where each lane of the snapshot (svc.lanes, by
// key) is drawn, its band's top in the plot; a mark per ticket, a P1 a full-height line with its
// id, a P2 or P3 a short tick at the bottom; and a bar's tooltip.
const TICKET_TIMELINE = {
  lanes: [{ key: "classifier", top: 30 }, { key: "responder", top: 86 }],
  mark: m => [prioClass(m.priority), m.priority === "P1" ? `P1 ${m.id}` : ""],
  tip: runTip,
};

// A timeline of marks and agent runs over the last 2 minutes (the ticket row's; the security
// row draws its own, see "Security row"): its title, the plot with a band and a chip per lane
// (lanes: where each is drawn), and the axis. The right of its title line (.tk-agents) is the
// row's to fill.
function timelineHTML(title, label, lanes) {
  return `
    <div class="tk-timeline">
      <div class="tk-tl-head"><span class="tk-tl-title">${title}</span><span class="tk-agents"></span></div>
      <div class="tk-plot" role="group" aria-label="${label}">
        <div class="tk-floor"></div>
        ${lanes.map(l => `<div class="tk-band" style="top: ${l.top}px"></div>`).join("")}
        <div class="tk-marks"></div>
        <div class="tk-runs"></div>
        ${lanes.map(l => `
          <div class="tk-chip ${l.key}" style="top: ${l.top + 9}px">
            <span class="tk-square"></span><span class="tk-lane-name"></span><span class="tk-lane-state"></span>
          </div>`).join("")}
      </div>
      ${axisHTML()}
    </div>`;
}

// A timeline's parts, for its row to update: what draws it (timeline: TICKET_TIMELINE) and its
// lanes, the plot, the marks, the bars, the chips by lane.
function timelineParts(body, timeline) {
  const q = sel => body.querySelector(sel), lanes = timeline.lanes;
  return { timeline, lanes, plot: q(".tk-plot"), marks: q(".tk-marks"), runs: q(".tk-runs"), agents: q(".tk-agents"),
           chips: Object.fromEntries(lanes.map(l => [l.key, q(`.tk-chip.${l.key}`)])) };
}

// A position on a timeline's axis (seconds from its left edge) as a share of its width.
const pct = t => `${(Math.max(0, Math.min(t, AXIS)) / AXIS * 100).toFixed(3)}%`;

// A timeline from the snapshot (the ticket row's): a mark per item (svc.marks, by id), a bar per
// run in its agent's lane (svc.runs, by key: D-062), the lanes' chips, then the labels placed.
// What differs is the row's (tl.timeline): where each lane is drawn, a mark's class and label,
// and a bar's tooltip, given its lane's name; and, if it has them, what a bar holds before its
// cap and label (bar), adds to its class (barClass) and draws in it (drawBar).
function updateTimeline(tl, svc) {
  const { lanes, mark, bar = "", barClass, drawBar, tip } = tl.timeline;
  sync(tl.marks, svc.marks || [], m => m.id, () => {
    const n = document.createElement("div");
    n.appendChild(document.createElement("span"));
    return n;
  }, (n, m) => {
    const [cls, label] = mark(m);
    setClass(n, `tk-mark ${cls}`);
    setAttr(n, "style", `left: ${pct(m.t)}`);
    setText(n.firstChild, label);
  });
  const top = Object.fromEntries(lanes.map(l => [l.key, l.top + 9]));
  const laneName = Object.fromEntries((svc.lanes || []).map(l => [l.key, l.name]));
  sync(tl.runs, svc.runs || [], r => r.key, () => {
    const n = document.createElement("div");
    n.tabIndex = 0;
    n.innerHTML = `${bar}<span class="tk-cap"></span><span class="tk-run-label"></span>`;
    return n;
  }, (n, r) => {
    setClass(n, `tk-run ${r.lane}${r.open ? " open" : ""}${barClass ? barClass(r) : ""}`);
    setAttr(n, "style", `left: ${pct(r.start)}; width: ${pct(r.end - r.start)}; top: ${top[r.lane] ?? top[lanes[0].key]}px`);
    if (drawBar) drawBar(n, r);
    setText(n.lastChild, r.label || "");
    n.setAttribute("data-tip", tip(r, laneName[r.lane] || r.lane));
  });
  updateChips(tl, svc.lanes);
  placeLabels(tl);
}

// The lanes' chips: each lane's name and state, as the snapshot says them (svc.lanes).
function updateChips(tl, lanes) {
  for (const lane of lanes || []) {
    const chip = tl.chips[lane.key];
    if (!chip) continue;
    setClass(chip, `tk-chip ${lane.key}${lane.working ? " working" : ""}`);
    setText(chip.querySelector(".tk-lane-name"), lane.name || "");
    setText(chip.lastElementChild, lane.state || "");
  }
}

function createTicketBody(row, svcSources) {
  const SOURCES = SOURCE_LAYOUT.map((at, k) => ({ ...at, key: svcSources[k]?.key ?? "", name: esc(svcSources[k]?.name ?? "") }));
  const body = document.createElement("div");
  body.className = "flow-body tk-body";
  body.innerHTML = `
    <div class="flow-left">
      <div class="tk-sources">
        <svg width="170" height="280" viewBox="0 0 170 280" aria-hidden="true">
          ${SOURCES.map(x => `<path d="${x.path}" class="tk-path"></path>`).join("")}
          <circle cx="145" cy="140" r="3.5" class="tk-node"></circle>
        </svg>
        ${SOURCES.map((x, k) => `
          <span class="tk-src-label" style="top: ${x.labelTop}px">${x.name}</span>
          ${flowDots(`flowc path-${k}`, k ? [".23s", ".7s", "1.17s"] : ["0s", ".47s", ".93s"])}
          <span class="tk-packet path-${k}" data-src="${x.key}"></span>
          <div class="src-box tk-src" data-src="${x.key}" title="${x.name}" style="top: ${x.top}px">
            <svg role="img" aria-label="${x.name}" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
              stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${x.icon}</svg>
          </div>`).join("")}
      </div>
      <div class="flow-line" aria-hidden="true">
        ${flowDots("flow", [".1s", ".5s", ".9s", "1.3s"])}
        <span class="tk-merge"></span>
        ${ARROW}
      </div>
      <div class="list">
        <div class="list-head"><span class="list-title">Tickets</span><span class="list-count"></span></div>
        <div class="list-view tk-view" aria-live="polite"><div class="tk-rows"></div></div>
      </div>
    </div>
    ${timelineHTML("Tickets and agent runs", "Tickets over the last 2 minutes, with the runs of the two agents P1 tickets start",
      TICKET_TIMELINE.lanes)}`;
  row.querySelector(".row-main").appendChild(body);
  const q = sel => body.querySelector(sel);
  row._tk = {
    ...timelineParts(body, TICKET_TIMELINE), body, rows: q(".tk-rows"), received: q(".list-count"),
    sources: Object.fromEntries(SOURCES.map(x => [x.key, q(`.tk-src[data-src="${x.key}"]`)])),
    packets: Object.fromEntries(SOURCES.map(x => [x.key, q(`.tk-packet[data-src="${x.key}"]`)])),
    merge: q(".tk-merge"), seen: null,  // the ids on the previous snapshot; null before the first
  };
  const detail = row.querySelector(".detail");
  detail.innerHTML = '<span>Open P1 <span class="open-p1"></span></span>';
  row._tk.openP1 = detail.querySelector(".open-p1");
}

function updateTicketRow(row, svc) {
  if (!row._tk) createTicketBody(row, svc.sources || []);
  const tk = row._tk, list = (svc.tickets && svc.tickets.newest) || [];

  // Header: open P1 tickets, in the accent above 0.
  const openP1 = svc.openP1 || 0;
  setText(tk.openP1, String(openP1));
  setClass(tk.openP1, openP1 ? "open-p1 some" : "open-p1");

  // The ids this snapshot brings that the page hasn't seen: they animate (not on the first).
  const fresh = freshItems(tk, list, t => t.id, t => t.eventTime);

  // The list: the newest tickets, by id.
  setText(tk.received, `${(svc.tickets && svc.tickets.received) || 0} received`);
  sync(tk.rows, list, t => t.id, t => {
    const r = document.createElement("div");
    r.innerHTML = '<span class="tk-badge"></span><div class="tk-text"><span class="tk-title"></span>' +
      '<span class="tk-sub"><span class="tk-id"></span><span class="tk-from"></span></span></div>';
    if (fresh.includes(t)) fadeIn(r);
    return r;
  }, (r, t) => {
    setClass(r, `tk-row ${prioClass(t.priority)}${r._fresh ? " fresh" : ""}`);
    const [badge, text] = r.children, [title, sub] = text.children;
    setText(badge, t.priority || "");
    setText(title, t.subject || "");
    setAttr(title, "title", t.subject || "");
    setText(sub.firstChild, t.id);
    setText(sub.lastChild, ` from ${t.sourceName || "?"} at ${ticketTime(t.eventTime)}`);
  });
  if (fresh.length) replay(tk.rows, "push");
  for (const t of fresh.slice().reverse()) {  // oldest first, so the newest colours last
    const box = tk.sources[t.source], packet = tk.packets[t.source], cls = prioClass(t.priority);
    if (box) { box.dataset.prio = cls; ring(box); }
    if (packet) { packet.dataset.prio = cls; replay(packet, "pkt"); }
    tk.merge.dataset.prio = cls;
    replay(tk.merge, "merge");
  }

  // The words above the timeline (the state said which lane works), then the timeline.
  setText(tk.agents, svc.agentsText || "");
  setClass(tk.agents, svc.agentsWorking ? "tk-agents working" : "tk-agents");
  updateTimeline(tk, svc);
}

// Two tickets a second apart put their labels on top of each other, and a lane's chip covers
// a label near the left edge. The newest label wins: an older one that would cover it, the
// chip or the plot's edge moves to the right of its bar if there is room, or else hides (its
// tooltip still says it). A P1 mark's label hides the same way.
const overlaps = (a, b) => a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
function placeLabels(tk) {
  const plot = tk.plot.getBoundingClientRect();
  const fits = (r, taken) => r.left >= plot.left && r.right <= plot.right && !taken.some(t => overlaps(r, t));
  for (const l of tk.lanes) {
    const bars = [...tk.runs.children].filter(b => b.classList.contains(l.key));
    const barBoxes = bars.map(b => b.getBoundingClientRect());
    const taken = [tk.chips[l.key].getBoundingClientRect()];
    for (let i = bars.length - 1; i >= 0; i--) {  // newest first
      const bar = bars[i], label = bar.lastChild;
      if (!label.textContent) continue;
      let box = label.getBoundingClientRect();
      if (!fits(box, taken)) {
        bar.classList.add("label-right");
        box = label.getBoundingClientRect();
        if (!fits(box, [...taken, ...barBoxes.filter((_, j) => j !== i)])) {
          bar.classList.replace("label-right", "label-hidden");
          continue;
        }
      }
      taken.push(box);
    }
  }
  const taken = [];
  for (const mark of [...tk.marks.children].reverse()) {
    const label = mark.firstChild;
    if (!label.textContent) continue;
    const box = label.getBoundingClientRect();
    if (fits(box, taken)) taken.push(box);
    else mark.classList.add("label-hidden");
  }
}

// A run's tooltip: the ticket, the agent's verdict or action, and its reason.
function runTip(r, agent) {
  const what = r.lane === "classifier" ? "Verdict" : "Action";
  const answer = r.outcome != null ? `${what}: ${esc(r.outcome)}.`
    : r.open ? `Waiting for the ${agent.toLowerCase()}.` : `The ${agent.toLowerCase()} gave no answer.`;
  return `<strong>${esc(r.subject || r.ticketId)}</strong>${esc(r.priority || "")} ${esc(r.ticketId)} · ${agent} · ${esc(r.label)}` +
    `<em>${answer}</em>` +
    (r.reason ? `<span class="reason">${esc(clip(String(r.reason), 180))}</span>` : "");
}

// ---------- Security row ----------
// The row whose snapshot says row: "security" (D-070): "Code security incidents", after the
// services, since it is none. On the left, the deploy row's chrome: the laptop with "git" under
// it (the scanner reads River Shop's repositories), a line with the ambient flow on to "Scan
// findings", the newest findings with their repository and CVSS score, a critical one in the
// accent. A finding id the page hasn't seen (not on the first snapshot, at most FRESH_SECONDS old)
// sends a packet along the line, in the accent when critical, rings the laptop and fades its row
// in. The Issues panel takes spec decision 26's words (ROW_KINDS): critical findings, right
// decisions with the missed line, the median times to decide and to patch.
// On the right, "Findings and agent runs", the design in `inspiration/Findings timeline
// redesign.html` (issue 13), drawn by drawSecurityTimeline as one SVG. A gutter on the left holds
// the lane names and our agent's chip, so no text sits in the plot. Top to bottom: a pill per
// critical finding, two lines (its CVE id, and our agent's label word for word under it, in the
// colour the state names, r.tone), a diamond for it on the finding track, our agent's lane, a gap for the call down and the reply up, the patch agent's
// lane, a tick per other finding, and the axis. Our bar is solid accent, with the wall's blinking
// cap while open; the part where our agent waited on the patch agent is hatched, and under it the
// external bar, with an accent arrow down at the call, a lavender one up at the reply and a soft
// band between them. The patch's times come with the decision (spec decision 24), so all of that
// appears when our agent answers, not while the patch runs. A run that overlaps another in its lane
// is half height on its track for its whole length, and its external run takes the same track
// (r.track, r.split: the state's). Hovering any part of a finding, or focusing its diamond, shows
// its one tooltip and dims every other finding. The diamond is its one tab stop, so a finding whose
// pill has dropped can still be reached. Under prefers-reduced-motion nothing moves (CSS).

// The timeline's bands, top to bottom, in px: the pills (two lines each), the finding track, our
// agent's lane, the gap, the external lane, the strip of other findings, the axis line and its
// labels. The design's, tightened from its 294 px to 260.
const SC_Y = { pill: 4, pillH: 38, event: 62, aTop: 80, aBot: 116, bTop: 168, bBot: 204,
               otherTop: 216, otherBot: 226, axis: 234, label: 252, H: 260 };
const SC_GUTTER = 118;  // the lane names' column, left of the plot
const SC_RIGHT = 8;     // between the plot and the panel's edge
const SC_BAR = { pad: 8, h: 20, half: 9 };  // a bar's inset in its lane, its height, and a split bar's
const PILL_GAP = 6;     // between two pills
const SC_FONT = { id: '500 12px "Geist Mono", ui-monospace, monospace', status: '400 12px "Geist", system-ui, sans-serif',
                  chip: '500 11px "Geist Mono", ui-monospace, monospace' };
const textWidth = (() => {
  const ctx = document.createElement("canvas").getContext("2d");
  return (text, font) => { ctx.font = font; return ctx.measureText(text).width; };
})();
const r1 = v => Math.round(v * 10) / 10;
// setAttr for several attributes at once, numbers to 0.1 px.
function put(el, a) {
  for (const k in a) setAttr(el, k, typeof a[k] === "number" ? String(r1(a[k])) : a[k]);
  return el;
}
const show = (el, on) => setAttr(el, "visibility", on ? "visible" : "hidden");
const fidOf = el => (el && el.closest ? el.closest("[data-fid]")?.getAttribute("data-fid") ?? null : null);

function createSecurityBody(row) {
  const body = document.createElement("div");
  body.className = "flow-body sc-body";
  body.innerHTML = `
    <div class="flow-left">
      <div class="sc-source">
        <div class="src-box sc-laptop">
          <svg role="img" aria-label="River Shop's repositories, scanned for CVEs" width="18" height="18" viewBox="0 0 24 24"
            fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${LAPTOP}</svg>
        </div>
        <span class="sc-git">git</span>
      </div>
      <div class="flow-line" aria-hidden="true">
        ${flowDots("flow", ["0s", ".4s", ".8s", "1.2s"])}
        <span class="sc-packet"></span>
        ${ARROW}
      </div>
      <div class="list">
        <div class="list-head"><span class="list-title">Scan findings</span><span class="list-count"></span></div>
        <div class="list-view sc-view" aria-live="polite"><div class="sc-rows"></div></div>
      </div>
    </div>
    <div class="sc-timeline">
      <div class="sc-head">
        <span class="sc-title">Findings and agent runs</span>
        <span class="sc-legend" aria-hidden="true">
          <span class="sc-key"><svg width="18" height="10" viewBox="0 0 18 10"><rect width="18" height="10" rx="2"
            class="sc-key-run"></rect></svg>Working</span>
          <span class="sc-key"><svg width="18" height="10" viewBox="0 0 18 10"><defs><pattern id="sc-key-hatch" width="5"
            height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="5" height="5"
            class="sc-hatch-ground"></rect><line x1="0" y1="0" x2="0" y2="5" class="sc-hatch-line"></line></pattern></defs><rect
            x=".5" y=".5" width="17" height="9" rx="2" class="sc-key-wait" fill="url(#sc-key-hatch)"></rect></svg>Waiting on patch agent</span>
        </span>
      </div>
      <div class="sc-chart" role="group" aria-label="${"Findings over the last 2 minutes, with the runs of the security "
        + "agent on the critical ones and of the external patch agent it calls"}"></div>
    </div>`;
  row.querySelector(".row-main").appendChild(body);
  const q = sel => body.querySelector(sel);
  const chart = q(".sc-chart");
  chart._draw = () => drawSecurityTimeline(chart);
  ro.observe(chart);
  row._sc = {
    body, chart, rows: q(".sc-rows"), scanned: q(".list-count"),
    laptop: q(".sc-laptop"), packet: q(".sc-packet"), seen: null,  // the ids on the previous snapshot; null before the first
  };
  const detail = row.querySelector(".detail");
  detail.innerHTML = '<span>Open criticals <span class="open-crit"></span></span>';
  row._sc.open = detail.querySelector(".open-crit");
}

function updateSecurityRow(row, svc) {
  if (!row._sc) createSecurityBody(row);
  const sc = row._sc, findings = svc.findings || {}, list = findings.newest || [];

  // Header: the critical findings our agent is working on, in the accent above 0.
  const open = svc.openCriticals || 0;
  setText(sc.open, String(open));
  setClass(sc.open, open ? "open-crit some" : "open-crit");

  // The ids this snapshot brings that the page hasn't seen: they animate (not on the first).
  const fresh = freshItems(sc, list, f => f.id, f => f.eventTime);

  // The list: the newest findings, by id.
  setText(sc.scanned, `${findings.scanned || 0} scanned`);
  sync(sc.rows, list, f => f.id, f => {
    const r = document.createElement("div");
    r.innerHTML = '<div class="sc-text"><span class="sc-repo"></span><span class="sc-sub"></span></div><span class="sc-score"></span>';
    if (fresh.includes(f)) fadeIn(r);  // as the packet lands
    return r;
  }, (r, f) => {
    setClass(r, `sc-row${f.critical ? " critical" : ""}${r._fresh ? " fresh" : ""}`);
    const [text, score] = r.children, [repo, sub] = text.children;
    setText(repo, f.repository || "?");
    setText(sub, `${f.cveId || "?"} · ${f.id}`);
    setText(score, f.cvssScore == null ? "" : `CVSS ${Number(f.cvssScore).toFixed(1)}`);
  });
  if (fresh.length) {
    const crit = fresh.some(f => f.critical) ? "yes" : "no";  // a critical one among them takes the accent
    replay(sc.rows, "rpush");
    sc.laptop.dataset.crit = crit;
    ring(sc.laptop);
    sc.packet.dataset.crit = crit;
    replay(sc.packet, "travel");
  }

  sc.chart._svc = svc;
  drawSecurityTimeline(sc.chart);
}

// The timeline's <svg>, made on its first draw and kept: the defs (the clip to the plot, the hatch),
// the plot's ground, the gutter, then the layers kept by key, the ticks and the findings clipped to
// the plot and the pills over them. The pointer and the focus say which finding is hot.
function securityChart(host) {
  if (host._p) return host._p;
  const svg = s("svg", { class: "sc-svg" }, host);
  const defs = s("defs", {}, svg);
  const clip = s("rect", {}, s("clipPath", { id: "sc-clip" }, defs));
  const hatch = s("pattern", { id: "sc-hatch", width: 5, height: 5, patternUnits: "userSpaceOnUse",
                               patternTransform: "rotate(45)" }, defs);
  s("rect", { width: 5, height: 5, class: "sc-hatch-ground" }, hatch);
  s("line", { x1: 0, y1: 0, x2: 0, y2: 5, class: "sc-hatch-line" }, hatch);
  const ground = s("g", {}, svg);
  const p = {
    svg, clip,
    lanes: [s("rect", { class: "sc-lane", rx: 6 }, ground), s("rect", { class: "sc-lane", rx: 6 }, ground)],
    track: s("line", { class: "sc-track" }, ground),
    grid: [1, 2, 3].map(() => s("line", { class: "sc-grid" }, ground)),
    axis: s("line", { class: "sc-track" }, ground),
    labels: [0, 1, 2, 3, 4].map(i => s("text", { class: i === 4 ? "sc-ax now" : i % 2 ? "sc-ax minor" : "sc-ax",
                                                 "text-anchor": i === 0 ? "start" : i === 4 ? "end" : "middle" }, ground)),
  };
  // The gutter: fixed text, never inside the plot.
  const Y = SC_Y, aMid = (Y.aTop + Y.aBot) / 2, bMid = (Y.bTop + Y.bBot) / 2, gap = (Y.aBot + Y.bTop) / 2;
  const gutter = s("g", { class: "sc-gutter" }, svg);
  s("text", { class: "sc-gl", x: 0, y: Y.event + 4 }, gutter).textContent = "Findings";
  s("rect", { class: "sc-sq ours", x: 0, y: aMid - 13, width: 9, height: 9, rx: 2 }, gutter);
  p.nameA = s("text", { class: "sc-gn", x: 15, y: aMid - 4 }, gutter);
  p.chip = s("g", { class: "sc-chip" }, gutter);
  p.chipBox = s("rect", { class: "sc-chip-box", x: 15, y: aMid + 1, height: 16, rx: 8 }, p.chip);
  p.chipDot = s("circle", { class: "sc-chip-dot", cx: 24, cy: aMid + 9, r: 3 }, p.chip);
  p.chipText = s("text", { class: "sc-chip-text", y: aMid + 13 }, p.chip);
  s("text", { class: "sc-gs", x: 0, y: gap + 4 }, gutter).textContent = "↓ call · ↑ reply";
  s("rect", { class: "sc-sq ext", x: 0.5, y: bMid - 12.5, width: 8, height: 8, rx: 2 }, gutter);
  p.nameB = s("text", { class: "sc-gn", x: 15, y: bMid - 4 }, gutter);
  p.detailB = s("text", { class: "sc-gs", x: 15, y: bMid + 12 }, gutter);
  s("text", { class: "sc-gs", x: 0, y: Y.otherBot - 1 }, gutter).textContent = "Other findings";
  p.ticks = s("g", { "clip-path": "url(#sc-clip)" }, svg);
  p.findings = s("g", { "clip-path": "url(#sc-clip)" }, svg);
  p.pills = s("g", {}, svg);
  // Which finding is hot: the one under the pointer, else the one whose pill has the focus. The
  // listeners are on the host: Chrome makes an SVG element with focus listeners a tab stop.
  p.pointer = p.focus = null;
  host.addEventListener("pointerover", e => { p.pointer = fidOf(e.target); applyHot(p); });
  host.addEventListener("pointerleave", () => { p.pointer = null; applyHot(p); });
  host.addEventListener("focusin", e => { p.focus = fidOf(e.target); applyHot(p); });
  host.addEventListener("focusout", () => { p.focus = null; applyHot(p); });
  host._p = p;
  return p;
}

// While one finding is hot, every other finding's marks dim (CSS).
function applyHot(p) {
  const hot = p.pointer ?? p.focus;
  const marks = [...p.findings.children, ...p.pills.children];
  const live = hot != null && marks.some(n => n.getAttribute("data-fid") === hot);
  setClass(p.svg, live ? "sc-svg has-hot" : "sc-svg");
  for (const n of marks) n.classList.toggle("hot", live && n.getAttribute("data-fid") === hot);
}

// A finding as the timeline draws it, from the snapshot: its critical mark (svc.marks), our agent's
// run and the patch agent's (svc.runs, by findingId, the lanes top to bottom in svc.lanes), and its
// list row, if it is among the newest, for a tooltip without a run. In the order they happened.
function securityItems(svc) {
  const external = (svc.lanes || [])[1]?.key;
  const listed = new Map(((svc.findings || {}).newest || []).map(f => [f.id, f]));
  const items = new Map();
  const item = id => {
    if (!items.has(id)) items.set(id, { id, mark: null, ours: null, ext: null, listed: listed.get(id) });
    return items.get(id);
  };
  for (const m of svc.marks || []) if (m.critical) item(m.id).mark = m;
  for (const r of svc.runs || []) item(r.findingId)[r.lane === external ? "ext" : "ours"] = r;
  const at = it => (it.mark ? it.mark.t : (it.ours || it.ext).start);
  return [...items.values()].sort((a, b) => at(a) - at(b) || (a.id < b.id ? -1 : 1));
}

// A bar's top and height in a lane whose bars start at `top`: the full height, or, split, the top
// or the bottom half on its track (r.split and r.track, issue 13).
function barY(top, r) {
  if (!r || !r.split) return [top, SC_BAR.h];
  return [r.track ? top + SC_BAR.h - SC_BAR.half : top, SC_BAR.half];
}

// A finding's one tooltip, as HTML for #tip and as text for its diamond's aria-label: the CVE in its
// repository, the finding id, the CVSS and our agent's label; what our agent's run came to
// (r.outcomeText), how long it waited on the patch agent and its reason; after a patch call, how
// long the patch agent took, when, and its reply, which carries the commit.
function findingTip(it) {
  const r = it.ours, e = it.ext, listed = it.listed || {};
  const cve = r?.cveId || it.mark?.cveId || listed.cveId || it.id;
  const repo = r?.repository || listed.repository, score = r?.cvssScore ?? listed.cvssScore;
  const parts = [["strong", repo ? `${cve} in ${repo}` : cve],
                 ["", `${it.id}${score != null ? `, CVSS ${Number(score).toFixed(1)}` : ""}${r ? ` · ${r.label}` : ""}`]];
  if (r?.outcomeText) parts.push(["em", r.outcomeText]);
  if (r && r.waitFrom != null && r.waitTo != null) {
    parts.push(["em", `Waiting on patch agent for ${dur(r.waitTo - r.waitFrom)}, the hatched part.`]);
  }
  if (r?.reason) parts.push(["reason", clip(String(r.reason), 180)]);
  if (e) {
    const when = e.startedAt != null && e.finishedAt != null
      ? `, started ${clockOf(e.startedAt)}, finished ${clockOf(e.finishedAt)}` : "";
    parts.push(["em", `The patch agent answered in ${e.durationText || dur(e.duration || 0)}${when}.`]);
    if (e.reply) parts.push(["reason", clip(String(e.reply), 240)]);
  }
  const html = parts.map(([tag, text]) => tag === "strong" ? `<strong>${esc(text)}</strong>`
    : tag === "em" ? `<em>${esc(text)}</em>` : tag === "reason" ? `<span class="reason">${esc(text)}</span>` : esc(text));
  const text = parts.map(([, t]) => t.replace(/ · /g, ", ")).map(t => (/[.!?…]$/.test(t) ? t : `${t}.`));
  return { html: html.join(""), text: text.join(" ") };
}

function drawSecurityTimeline(host) {
  const svc = host._svc;
  if (!svc) return;
  const p = securityChart(host), Y = SC_Y;
  const W = hostWidth(host), pl = SC_GUTTER, pr = Math.max(pl + 40, W - SC_RIGHT), pw = pr - pl;
  const X = t => pl + (Math.max(0, Math.min(t, AXIS)) / AXIS) * pw;
  put(p.svg, { width: W, height: Y.H, viewBox: `0 0 ${W} ${Y.H}` });
  put(p.clip, { x: pl, y: 0, width: pw, height: Y.H });

  // The plot's ground: the two lanes, the finding track, the dashed grid, the axis and its labels.
  put(p.lanes[0], { x: pl, y: Y.aTop, width: pw, height: Y.aBot - Y.aTop });
  put(p.lanes[1], { x: pl, y: Y.bTop, width: pw, height: Y.bBot - Y.bTop });
  put(p.track, { x1: pl, x2: pr, y1: Y.event, y2: Y.event });
  p.grid.forEach((n, k) => { const x = X(AXIS * (k + 1) / 4); put(n, { x1: x, x2: x, y1: Y.aTop, y2: Y.axis }); });
  put(p.axis, { x1: pl, x2: pr, y1: Y.axis, y2: Y.axis });
  p.labels.forEach((n, i) => { put(n, { x: X(AXIS * i / 4), y: Y.label }); setText(n, axisLabel(i)); });

  // The gutter's names, and our agent's chip under its name, as the snapshot says them (svc.lanes).
  const [ours = {}, external = {}] = svc.lanes || [];
  setText(p.nameA, ours.name || "");
  setText(p.nameB, external.name || "");
  setText(p.detailB, external.detail || "");
  const state = ours.state || "", working = !!ours.working;
  setClass(p.chip, working ? "sc-chip working" : "sc-chip");
  put(p.chipBox, { width: Math.ceil(textWidth(state, SC_FONT.chip)) + (working ? 24 : 14) });
  show(p.chipDot, working);
  put(p.chipText, { x: working ? 32 : 22 });
  setText(p.chipText, state);

  // A tick per other finding, in the strip under the external lane.
  sync(p.ticks, (svc.marks || []).filter(m => !m.critical), m => m.id, () => s("line", { class: "sc-tick" }), (n, m) => {
    const x = X(m.t);
    put(n, { x1: x, x2: x, y1: Y.otherTop, y2: Y.otherBot });
  });

  // A group per finding, by id, with every mark of it in the plot; then a pill per critical
  // finding in the window that fits, the newest kept.
  const items = securityItems(svc);
  for (const it of items) it.tip = findingTip(it);
  sync(p.findings, items, it => it.id, createFinding, (g, it) => updateFinding(g, it, X));
  sync(p.pills, placePills(items.filter(it => it.mark), X, pl, pr), q => q.it.id, createPill, updatePill);
  applyHot(p);
}

// A finding's group: its marks back to front, the arrows' hit strips, then its diamond, the
// finding's one tab stop, with its focus ring and a hit area round it.
function createFinding() {
  const g = s("g", { class: "sc-f" });
  const part = (tag, cls, parent = g) => s(tag, { class: cls }, parent);
  const mark = s("g", { class: "sc-dmark", role: "img" }, g);
  g._p = {
    band: part("rect", "sc-band"), link: part("line", "sc-link"), ext: part("rect", "sc-ext"),
    run: part("rect", "sc-run"), hatch: part("rect", "sc-hatch"), after: part("rect", "sc-run"),
    cap: part("rect", "sc-cap"), call: part("path", "sc-call"), reply: part("path", "sc-reply"),
    callHit: part("rect", "sc-hit call"), replyHit: part("rect", "sc-hit reply"),
    mark, markHit: part("rect", "sc-hit dia", mark), ring: part("path", "sc-dring", mark),
    diamond: part("path", "sc-diamond", mark),
  };
  g.appendChild(mark);  // last, over the arrows' strips
  g._p.hatch.setAttribute("fill", "url(#sc-hatch)");
  return g;
}

// A bar from x0 to x1, inset all round by `inset`, or hidden when that leaves it no width.
function segment(el, x0, x1, y, h, inset = 0) {
  const width = x1 - x0 - 2 * inset;
  show(el, width > 0.5);
  if (width > 0.5) put(el, { x: x0 + inset, y: y + inset, width, height: h - 2 * inset, rx: 2 });
}

function updateFinding(g, it, X) {
  const P = g._p, Y = SC_Y, r = it.ours, e = it.ext;
  setAttr(g, "data-fid", it.id);
  setAttr(g, "data-tip", it.tip.html);

  // The diamond on the finding track: the finding's tab stop, saying what its tooltip says.
  const mx = it.mark ? X(it.mark.t) : null;
  show(P.mark, mx != null);
  if (mx != null) {
    setAttr(P.mark, "tabindex", "0");
    setAttr(P.mark, "data-tip", it.tip.html);
    setAttr(P.mark, "aria-label", it.tip.text);
    put(P.diamond, { d: diamond(mx, Y.event, 5) });
    put(P.ring, { d: diamond(mx, Y.event, 10) });
    put(P.markHit, { x: mx - 8, y: Y.event - 8, width: 16, height: 16 });
  } else if (P.mark.hasAttribute("tabindex")) {
    P.mark.removeAttribute("tabindex");
  }

  // Our bar: solid, hatched between waitFrom and waitTo, the blinking cap at its open end.
  const [oy, oh] = barY(Y.aTop + SC_BAR.pad, r);
  const x0 = r ? X(r.start) : 0, x1 = r ? Math.max(X(r.end), x0 + 2) : 0;
  const waited = r && r.waitFrom != null && r.waitTo != null && r.waitTo > r.waitFrom;
  const w0 = waited ? X(r.waitFrom) : x1, w1 = waited ? Math.max(X(r.waitTo), w0 + 1) : x1;
  segment(P.run, x0, r ? w0 : 0, oy, oh);
  segment(P.hatch, w0, waited ? w1 : w0, oy, oh, 0.5);
  segment(P.after, w1, waited ? x1 : w1, oy, oh);
  show(P.cap, !!(r && r.open));
  if (r && r.open) put(P.cap, { x: x1 - 3, y: oy, width: 3, height: oh });
  show(P.link, !!(r && mx != null));
  if (r && mx != null) put(P.link, { x1: mx, y1: Y.event + 5, x2: x0, y2: oy });

  // The external bar, the band over the gap, the call down at its start and the reply up at its
  // end, from our bar's bottom to its top (an arrow on the plot's edge is a call or a reply before
  // the window, and is left out).
  const [ey, eh] = barY(Y.bTop + SC_BAR.pad, e);  // the state gave it our run's track
  const ex0 = e ? X(e.start) : 0, ex1 = e ? Math.max(X(e.end), ex0 + 2) : 0, top = oy + oh;
  segment(P.ext, ex0, ex1, ey, eh);
  segment(P.band, ex0, ex1, top, ey - top);
  const call = !!(e && e.start > 0), reply = !!(e && e.end < AXIS);
  show(P.call, call);
  show(P.callHit, call);
  if (call) {
    put(P.call, { d: `M ${r1(ex0)} ${top} V ${ey - 6} M ${r1(ex0 - 4.5)} ${ey - 7} L ${r1(ex0)} ${ey} L ${r1(ex0 + 4.5)} ${ey - 7} Z` });
    put(P.callHit, { x: ex0 - 6, y: top, width: 12, height: ey - top });
  }
  show(P.reply, reply);
  show(P.replyHit, reply);
  if (reply) {
    put(P.reply, { d: `M ${r1(ex1)} ${ey} V ${top + 6} M ${r1(ex1 - 4.5)} ${top + 7} L ${r1(ex1)} ${top} L ${r1(ex1 + 4.5)} ${top + 7} Z` });
    put(P.replyHit, { x: ex1 - 6, y: top, width: 12, height: ey - top });
  }
}

// The design's sticky callouts: a pill centres on its diamond, stops at the plot's edges and is
// pushed by its neighbours, 6 px apart. One that no longer fits at the left drops out, so the
// oldest goes first. A pill is as wide as its wider line, the CVE id or the label under it.
function placePills(items, X, pl, pr) {
  const pills = items.map(it => {
    const cve = it.mark.cveId || it.id, status = it.ours ? it.ours.label || "" : "";
    const w = Math.ceil(10 + Math.max(textWidth(cve, SC_FONT.id), status ? textWidth(status, SC_FONT.status) : 0) + 10);
    return { it, cve, status, w, mx: X(it.mark.t), left: 0 };
  }).sort((a, b) => a.mx - b.mx);
  for (const q of pills) q.left = Math.max(pl, Math.min(q.mx - q.w / 2, pr - q.w));
  for (let i = 1; i < pills.length; i++) pills[i].left = Math.max(pills[i].left, pills[i - 1].left + pills[i - 1].w + PILL_GAP);
  for (let i = pills.length - 1; i >= 0; i--) {
    const limit = i === pills.length - 1 ? pr : pills[i + 1].left - PILL_GAP;
    pills[i].left = Math.min(pills[i].left, limit - pills[i].w);
  }
  return pills.filter(q => q.left >= pl - 1);
}

function createPill() {
  const g = s("g", { class: "sc-pill" });
  g._p = {
    box: s("rect", { class: "sc-pill-box", rx: 7 }, g),
    id: s("text", { class: "sc-pid" }, g), status: s("text", { class: "sc-pst" }, g),
    caret: s("path", { class: "sc-caret" }, g), edge: s("path", { class: "sc-caret-edge" }, g),
    tether: s("line", { class: "sc-tether" }, g),
  };
  return g;
}

// A pill sits at its left edge by a transform, which glides (CSS): the CVE id on top, the label
// under it in its tone's colour (the state's r.tone, none while open), or the id alone, centred.
// Its caret slides along its bottom edge over the diamond, and a short line runs from the caret to
// the diamond. The pill takes the pointer, not the focus: the diamond is the tab stop.
function updatePill(g, q) {
  const P = g._p, Y = SC_Y, top = Y.pill, bottom = top + Y.pillH, L = r1(q.left), tone = q.it.ours?.tone;
  setAttr(g, "data-fid", q.it.id);
  setAttr(g, "data-tip", q.it.tip.html);
  if (g.style.transform !== `translate(${L}px, 0px)`) g.style.transform = `translate(${L}px, 0px)`;
  put(P.box, { x: 0, y: top, width: q.w, height: Y.pillH });
  put(P.id, { x: 10, y: top + (q.status ? 16 : 23) });
  setText(P.id, q.cve);
  put(P.status, { x: 10, y: top + 31 });
  setText(P.status, q.status);
  setClass(P.status, tone ? `sc-pst ${tone}` : "sc-pst");
  const cx = r1(Math.max(L + 12, Math.min(q.mx, L + q.w - 12)) - L);
  put(P.caret, { d: `M ${cx - 5} ${bottom - 1} L ${cx} ${bottom + 6} L ${cx + 5} ${bottom - 1} Z` });
  put(P.edge, { d: `M ${cx - 5} ${bottom} L ${cx} ${bottom + 6} L ${cx + 5} ${bottom}` });
  put(P.tether, { x1: cx, y1: bottom + 6, x2: q.mx - L, y2: Y.event - 6 });
}

// ---------- Issues ----------
// A row's Issues panel (D-068): the numbers the state counted (svc.issues), whatever they
// were counted from, under its kind's words (ROW_KINDS). Detected reads "x of y" over a missed
// line, amber when any was missed. The durations come in words ("6s", "1m 05s", "—" with
// nothing to show).
function renderIssues(ui, I, words) {
  ui.labels.forEach((n, i) => setText(n, words.labels[i]));
  setAttr(ui.remediate, "title", words.remediate);
  const injected = I.injected ?? 0, missed = I.missed ?? 0;
  setText(ui.stat.injected, String(injected));
  setText(ui.stat.detected, String(I.detected ?? 0));
  setText(ui.of, `of ${injected}`);
  setText(ui.missed, missed ? `${missed} missed` : "None missed");
  setClass(ui.missed, missed ? "stat-missed some" : "stat-missed");
  setText(ui.stat.medianTimeToDetect, I.medianTimeToDetectText || "—");
  setText(ui.stat.medianTimeToRemediate, I.medianTimeToRemediateText || "—");
}

// ---------- Tooltip ----------
// A box's or a mark's tooltip follows its node, which outlives the snapshots: each one
// refreshes the words (an answer arrived, the times moved) or hides the tooltip once the
// node has left.
const tip = document.getElementById("tip");
let tipNode = null;  // the node the tooltip is on; null when hidden or on a plot
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

// The header's clock, hh:mm:ss on the 24-hour clock whatever the browser's locale.
function clockText(sec) {
  const d = new Date(sec * 1000), p = n => String(n).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function render(D) {
  AXIS = D.chartSeconds || 120;
  UPDATED = D.updatedAt || Date.now() / 1000;
  THRESHOLD = D.errorThresholdPct ?? THRESHOLD;
  ACTION = D.actionWords || ACTION;
  sync(document.getElementById("rows"), D.services || [], svc => svc.name, createRow, updateRow);
  setText(document.getElementById("updated"), clockText(D.updatedAt || Date.now() / 1000));
  refreshTip();
}
