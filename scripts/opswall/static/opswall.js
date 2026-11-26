"use strict";

/*  Ops wall — render code, driven by the live /ws snapshot (D-054).
 *
 *  The wall's state does all the counting; this page only draws what the snapshot says.
 *  The frame is the target design's (D-068): a header, then one full-width row per service
 *  in the snapshot's order. A row is its main panel (name and status chip on the left, a
 *  detail slot on the right, then its body) and its Issues panel (svc.issues: injected,
 *  detected of injected with the missed line, the median times to detect and to remediate),
 *  which moves below the row on a narrow screen. svc.row says which kind of row:
 *  - "releases" (web-gateway, the design's deploy row) draws the laptop, the line and the
 *    Releases list on the left and its Errors chart (D-067) on the right, with "Running v…"
 *    in the header (see "Releases row" below);
 *  - "windows" (payments, the design's aggregate row) draws the service, the Σ box of its
 *    10 s window and the Aggregated records on the left and its Latency and Traffic charts on
 *    the right, with "Window 10s" in the header (see "Windows row" below);
 *  - "tickets" (checkout) draws the ticket sources, the list and the timeline of tickets
 *    and agent runs instead (see "Ticket row" below);
 *  - "security" ("Code security incidents", D-070, last: no service) draws the laptop with
 *    "git" under it, the line and the Scan findings list on the left, and the ticket row's
 *    timeline of findings and agent runs, with an external lane (see "Security row" below).
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
// Every row's time axis, the design's: the last 2 minutes (AXIS seconds) in five steps.
function axisHTML() {
  const label = i => {
    const sec = AXIS * (4 - i) / 4;
    return i === 4 ? "now" : sec >= 120 && sec % 60 === 0 ? `−${sec / 60}m` : `−${sec}s`;
  };
  return `<div class="axis" aria-hidden="true">${[0, 1, 2, 3, 4].map(i => `<span>${label(i)}</span>`).join("")}</div>`;
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
          <span class="stat-label">Injected</span>
          <span class="stat-value" data-k="injected"></span>
        </div>
        <div class="stat">
          <span class="stat-label">Detected</span>
          <span class="stat-line"><span class="stat-value" data-k="detected"></span><span class="stat-of"></span></span>
          <span class="stat-missed"></span>
        </div>
        <div class="stat">
          <span class="stat-label">Median time to detect</span>
          <span class="stat-value" data-k="medianTimeToDetect"></span>
        </div>
        <div class="stat remediate">
          <span class="stat-label">Median time to remediate</span>
          <span class="stat-value" data-k="medianTimeToRemediate"></span>
        </div>
      </div>
    </aside>`;
  const status = row.querySelector(".status");
  row._ui = {
    main: row.querySelector(".row-main"), issues: row.querySelector(".issues"),
    name: row.querySelector("h2"), status, statusText: status.lastElementChild,
    stat: Object.fromEntries([...row.querySelectorAll(".stat-value")].map(n => [n.dataset.k, n])),
    of: row.querySelector(".stat-of"), missed: row.querySelector(".stat-missed"),
    remediate: row.querySelector(".stat.remediate"),
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

function updateRow(row, svc) {
  const ui = row._ui, status = svc.status || "healthy", tickets = svc.row === "tickets";
  const releases = svc.row === "releases", windows = svc.row === "windows", security = svc.row === "security";
  ensureCharts(row, tickets || security ? [] : svc.charts || ["latency", "traffic"]);
  setClass(row, `svc-row${tickets ? " ticket-row" : releases ? " releases-row" : windows ? " windows-row"
    : security ? " security-row" : ""} ${status}`);
  setText(ui.name, svc.name);
  setAttr(ui.main, "aria-label", svc.name);
  setAttr(ui.issues, "aria-label", `${svc.name} issues in the last 10 minutes`);
  setClass(ui.status, `status ${status}`);
  setText(ui.statusText, svc.statusText || "");
  renderIssues(ui, svc.issues || {});
  setAttr(ui.remediate, "title", tickets ? "Measured from the ticket's creation to the responder's answer"
    : security ? "Measured from the finding's scan to the patch agent's finish, patched findings only"
    : "Measured from the incident's start to the agent's answer");
  if (tickets) updateTicketRow(row, svc);
  if (releases) updateReleaseRow(row, svc);
  if (windows) updateWindowsRow(row, svc);
  if (security) updateSecurityRow(row, svc);
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
  const first = rl.seen === null;
  const fresh = first ? [] : list.filter(r => !rl.seen.has(r.key) && UPDATED - r.deployedAt <= FRESH_SECONDS);
  rl.seen = new Set(list.map(r => r.key));

  sync(rl.rows, list, r => r.key, r => {
    const n = document.createElement("div");
    n.innerHTML = '<span class="rl-version"></span><span class="rl-at"></span><span class="rl-back"></span>';
    if (fresh.includes(r)) {  // the design's fade, once, as the packet lands
      n._fresh = true;
      setTimeout(() => { n._fresh = false; n.classList.remove("fresh"); }, 4200);
    }
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
    rl.laptop.classList.add("busy");
    replay(rl.laptop, "ring");
    clearTimeout(rl.laptop._busy);
    rl.laptop._busy = setTimeout(() => rl.laptop.classList.remove("busy"), 1000);
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
  const fresh = first ? [] : list.filter(w => !ag.seen.has(w.key) && UPDATED - w.windowEnd <= WINDOW_FRESH_SECONDS);
  ag.seen = new Set(list.map(w => w.key));

  sync(ag.rows, list, w => w.key, w => {
    const n = document.createElement("div");
    n.innerHTML = '<div class="ag-json"></div>';
    if (fresh.includes(w)) {  // the design's fade, once, as the packet lands
      n._fresh = true;
      setTimeout(() => { n._fresh = false; n.classList.remove("fresh"); }, 3800);
    }
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
// Where each lane of the snapshot (svc.lanes, by key) is drawn: its band's top in the plot.
const LANES = [
  { key: "classifier", top: 30 },
  { key: "responder", top: 86 },
];
const FRESH_SECONDS = 10;  // a new id this old or younger animates; an older one (a backread) doesn't
const pad2 = n => String(n).padStart(2, "0");
const ticketTime = sec => {  // "24.11.2026 09:26", local time
  const d = new Date(sec * 1000);
  return `${pad2(d.getDate())}.${pad2(d.getMonth() + 1)}.${d.getFullYear()} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
};
const prioClass = p => (p === "P1" || p === "P2" ? p.toLowerCase() : "p3");
// Replay a CSS animation: the class flips between its -a and -b twins, which run the same
// keyframes, so the animation starts again (the design's way).
function replay(el, base) {
  const next = el.classList.contains(`${base}-a`) ? `${base}-b` : `${base}-a`;
  el.classList.remove(`${base}-a`, `${base}-b`);
  el.classList.add(next);
}

// A timeline of marks and agent runs over the last 2 minutes (the ticket row's, and the
// security row's): its title, the plot with a band and a chip per lane (lanes: where each
// is drawn), and the axis. The right of its title line (.tk-agents) is the row's to fill.
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

// A timeline's parts, for its row to update: the plot, the marks, the bars, the chips by lane.
function timelineParts(body, lanes) {
  const q = sel => body.querySelector(sel);
  return { lanes, plot: q(".tk-plot"), marks: q(".tk-marks"), runs: q(".tk-runs"), agents: q(".tk-agents"),
           chips: Object.fromEntries(lanes.map(l => [l.key, q(`.tk-chip.${l.key}`)])) };
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
    ${timelineHTML("Tickets and agent runs", "Tickets over the last 2 minutes, with the runs of the two agents P1 tickets start", LANES)}`;
  row.querySelector(".row-main").appendChild(body);
  const q = sel => body.querySelector(sel);
  row._tk = {
    ...timelineParts(body, LANES), body, rows: q(".tk-rows"), received: q(".list-count"),
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
  const tk = row._tk, T = AXIS, list = (svc.tickets && svc.tickets.newest) || [];
  const pct = t => `${(Math.max(0, Math.min(t, T)) / T * 100).toFixed(3)}%`;

  // Header: open P1 tickets, in the accent above 0.
  const openP1 = svc.openP1 || 0;
  setText(tk.openP1, String(openP1));
  setClass(tk.openP1, openP1 ? "open-p1 some" : "open-p1");

  // The ids this snapshot brings that the page hasn't seen: they animate (not on the first).
  const fresh = tk.seen ? list.filter(t => !tk.seen.has(t.id) && UPDATED - t.eventTime <= FRESH_SECONDS) : [];
  tk.seen = new Set(list.map(t => t.id));

  // The list: the newest tickets, by id.
  setText(tk.received, `${(svc.tickets && svc.tickets.received) || 0} received`);
  sync(tk.rows, list, t => t.id, t => {
    const r = document.createElement("div");
    r.innerHTML = '<span class="tk-badge"></span><div class="tk-text"><span class="tk-title"></span>' +
      '<span class="tk-sub"><span class="tk-id"></span><span class="tk-from"></span></span></div>';
    if (fresh.includes(t)) {  // the design's fade, once
      r._fresh = true;
      setTimeout(() => { r._fresh = false; r.classList.remove("fresh"); }, 4200);
    }
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
    if (box) {
      box.dataset.prio = cls;
      box.classList.add("busy");
      replay(box, "ring");
      clearTimeout(box._busy);
      box._busy = setTimeout(() => box.classList.remove("busy"), 1000);
    }
    if (packet) { packet.dataset.prio = cls; replay(packet, "pkt"); }
    tk.merge.dataset.prio = cls;
    replay(tk.merge, "merge");
  }

  // The timeline: a mark per ticket (a P1 a full-height line with its id, a P2 or P3 a short
  // tick at the bottom), then the runs, each a bar in its agent's lane, by key.
  sync(tk.marks, svc.marks || [], m => m.id, () => {
    const n = document.createElement("div");
    n.appendChild(document.createElement("span"));
    return n;
  }, (n, m) => {
    setClass(n, `tk-mark ${prioClass(m.priority)}`);
    setAttr(n, "style", `left: ${pct(m.t)}`);
    setText(n.firstChild, m.priority === "P1" ? `P1 ${m.id}` : "");
  });
  const top = Object.fromEntries(LANES.map(l => [l.key, l.top + 9]));
  const laneName = Object.fromEntries((svc.lanes || []).map(l => [l.key, l.name]));
  sync(tk.runs, svc.runs || [], r => r.key, () => {
    const bar = document.createElement("div");
    bar.tabIndex = 0;
    bar.innerHTML = '<span class="tk-cap"></span><span class="tk-run-label"></span>';
    return bar;
  }, (bar, r) => {
    setClass(bar, `tk-run ${r.lane}${r.open ? " open" : ""}`);
    setAttr(bar, "style", `left: ${pct(r.start)}; width: ${pct(r.end - r.start)}; top: ${top[r.lane] ?? top.classifier}px`);
    setText(bar.lastChild, r.label || "");
    bar.setAttribute("data-tip", runTip(r, laneName[r.lane] || r.lane));
  });

  // The lanes' chips, and the words above the chart: the state said which lane works.
  updateChips(tk, svc.lanes);
  setText(tk.agents, svc.agentsText || "");
  setClass(tk.agents, svc.agentsWorking ? "tk-agents working" : "tk-agents");
  placeLabels(tk);
}

// Two tickets a second apart put their labels on top of each other, and a lane's chip covers
// a label near the left edge. The newest label wins: an older one that would cover it, the
// chip or the plot's edge moves to the right of its bar if there is room, or else hides (its
// tooltip still says it). A P1 mark's label hides the same way, and so does a critical
// finding's on the security row.
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
// accent. On the right, the ticket row's timeline, "Findings and agent runs": a mark per finding
// (a critical one a full-height line with its CVE id, the rest short ticks), the security agent's
// bars in the top lane, in the accent like the classifier's, and the patch agent's below it, in a
// colour no ticket lane uses and dashed all round: that agent is external, on the participant's
// laptop. The part of our bar the patch covers is hatched (our agent waited on the patch agent
// then). The state matched and counted everything: the bars come with their positions, words and
// the hatch's span, and both the hatch and the external bar come with the decision, so they are
// drawn when our agent's answer lands, not while the patch runs (spec decision 24). A finding id
// the page hasn't seen (not on the first snapshot, at most FRESH_SECONDS old) sends a packet
// along the line, in the accent when critical, rings the laptop and fades its row in. Under
// prefers-reduced-motion nothing moves (CSS).
const SECURITY_LANES = [
  { key: "security", top: 30 },
  { key: "external", top: 86 },
];

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
    ${timelineHTML("Findings and agent runs", "Findings over the last 2 minutes, with the runs of the security agent "
      + "on the critical ones and of the external patch agent it calls", SECURITY_LANES)}`;
  row.querySelector(".row-main").appendChild(body);
  const q = sel => body.querySelector(sel);
  row._sc = {
    ...timelineParts(body, SECURITY_LANES), body, rows: q(".sc-rows"), scanned: q(".list-count"),
    laptop: q(".sc-laptop"), packet: q(".sc-packet"), seen: null,  // the ids on the previous snapshot; null before the first
  };
  const detail = row.querySelector(".detail");
  detail.innerHTML = '<span>Open criticals <span class="open-crit"></span></span>';
  row._sc.open = detail.querySelector(".open-crit");
}

function updateSecurityRow(row, svc) {
  if (!row._sc) createSecurityBody(row);
  const sc = row._sc, T = AXIS, findings = svc.findings || {}, list = findings.newest || [];
  const pct = t => `${(Math.max(0, Math.min(t, T)) / T * 100).toFixed(3)}%`;

  // Header: the critical findings our agent is working on, in the accent above 0.
  const open = svc.openCriticals || 0;
  setText(sc.open, String(open));
  setClass(sc.open, open ? "open-crit some" : "open-crit");

  // The ids this snapshot brings that the page hasn't seen: they animate (not on the first).
  const fresh = sc.seen ? list.filter(f => !sc.seen.has(f.id) && UPDATED - f.eventTime <= FRESH_SECONDS) : [];
  sc.seen = new Set(list.map(f => f.id));

  // The list: the newest findings, by id.
  setText(sc.scanned, `${findings.scanned || 0} scanned`);
  sync(sc.rows, list, f => f.id, f => {
    const r = document.createElement("div");
    r.innerHTML = '<div class="sc-text"><span class="sc-repo"></span><span class="sc-sub"></span></div><span class="sc-score"></span>';
    if (fresh.includes(f)) {  // the design's fade, once, as the packet lands
      r._fresh = true;
      setTimeout(() => { r._fresh = false; r.classList.remove("fresh"); }, 4200);
    }
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
    sc.laptop.classList.add("busy");
    replay(sc.laptop, "ring");
    clearTimeout(sc.laptop._busy);
    sc.laptop._busy = setTimeout(() => sc.laptop.classList.remove("busy"), 1000);
    sc.packet.dataset.crit = crit;
    replay(sc.packet, "travel");
  }

  // The timeline: a mark per finding, then the bars, each in its lane, by key.
  sync(sc.marks, svc.marks || [], m => m.id, () => {
    const n = document.createElement("div");
    n.appendChild(document.createElement("span"));
    return n;
  }, (n, m) => {
    setClass(n, `tk-mark ${m.critical ? "critical" : "finding"}`);
    setAttr(n, "style", `left: ${pct(m.t)}`);
    setText(n.firstChild, m.critical ? m.cveId || m.id : "");
  });
  const top = Object.fromEntries(SECURITY_LANES.map(l => [l.key, l.top + 9]));
  const laneName = Object.fromEntries((svc.lanes || []).map(l => [l.key, l.name]));
  sync(sc.runs, svc.runs || [], r => r.key, () => {
    const bar = document.createElement("div");
    bar.tabIndex = 0;
    bar.innerHTML = '<span class="sc-wait"></span><span class="tk-cap"></span><span class="tk-run-label"></span>';
    return bar;
  }, (bar, r) => {
    setClass(bar, `tk-run ${r.lane}${r.open ? " open" : ""}${r.unreachable ? " unreachable" : ""}`);
    setAttr(bar, "style", `left: ${pct(r.start)}; width: ${pct(r.end - r.start)}; top: ${top[r.lane] ?? top.security}px`);
    // The hatch: the wait's span, as a share of the bar.
    const wait = bar.firstChild, span = r.end - r.start;
    const hatched = r.waitFrom != null && r.waitTo != null && r.waitTo > r.waitFrom && span > 0;
    wait.hidden = !hatched;
    if (hatched) {
      const share = t => `${((t - r.start) / span * 100).toFixed(3)}%`;
      setAttr(wait, "style", `left: ${share(r.waitFrom)}; right: calc(100% - ${share(r.waitTo)})`);
    }
    setText(bar.lastChild, r.label || "");
    bar.setAttribute("data-tip", findingRunTip(r, laneName[r.lane] || r.lane));
  });
  updateChips(sc, svc.lanes);
  placeLabels(sc);
}

// A finding's bar's tooltip. Ours: the finding, the security agent's action and its reason, the
// commit, and how long it waited on the patch agent (the hatch). The external one: the patch
// agent's reply, when the patch started and finished, and how long it took.
function findingRunTip(r, lane) {
  const score = r.cvssScore != null ? `, CVSS ${Number(r.cvssScore).toFixed(1)}` : "";
  const head = `<strong>${esc(r.cveId || r.findingId)} in ${esc(r.repository || "?")}</strong>` +
    `${esc(r.findingId)}${score} · ${esc(lane)} · ${esc(r.label)}`;
  if (r.lane === "external") {
    const when = r.startedAt != null && r.finishedAt != null
      ? `<em>Started ${clockOf(r.startedAt)}, finished ${clockOf(r.finishedAt)}: ${esc(r.durationText || dur(r.duration || 0))}.</em>`
      : "";
    return head + when + (r.reply ? `<span class="reason">${esc(clip(String(r.reply), 240))}</span>` : "");
  }
  const answer = r.action === "INVALID" ? "No PATCH or DISMISS came back."
    : r.outcome != null ? `Action: ${esc(r.outcome)}.`
    : r.open ? "Waiting for the security agent." : "The security agent gave no answer.";
  const commit = r.commit ? ` Commit ${esc(r.commit)}.` : r.unreachable ? " No commit came back." : "";
  const wait = r.waitFrom != null && r.waitTo != null
    ? `<em>Waiting on patch agent for ${dur(r.waitTo - r.waitFrom)}, the hatched part.</em>` : "";
  return `${head}<em>${answer}${commit}</em>${wait}` +
    (r.reason ? `<span class="reason">${esc(clip(String(r.reason), 180))}</span>` : "");
}

// ---------- Issues ----------
// A row's Issues panel (D-068): the numbers the state counted (svc.issues), whatever they
// were counted from. Detected reads "x of y" over a missed line, amber when any was missed.
// The durations come in words ("6s", "1m 05s", "—" with nothing to show).
function renderIssues(ui, I) {
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
