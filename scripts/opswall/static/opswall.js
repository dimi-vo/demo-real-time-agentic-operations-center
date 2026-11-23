"use strict";

/*  Ops wall — render code, driven by the live /ws snapshot (D-054).
 *
 *  The wall's state does all the counting; this page only draws what the snapshot says.
 *  One card per service; in it, the charts the snapshot names (svc.charts) and one time axis:
 *  Latency and Traffic for checkout and payments, Errors for web-gateway (D-067).
 *  A chart, back to front (D-066):
 *  - a translucent purple box per injected incident on that metric, the full plot height,
 *    from its start to its end (to the right edge while open); a missed one is dashed
 *  - on the errors chart, a thin mark at every deploy and rollback (svc.deploys): solid for a
 *    deploy, dashed for a rollback, good deploys included (D-067)
 *  - the raw service_metrics line (D-061): one point per second on a log y scale fitted to
 *    what's visible; on the errors chart, the share of each second's requests that failed on
 *    a fixed linear 0-100 % scale, with lab 2.2's threshold as a dashed line, and a gap for a
 *    second without requests
 *  - under the plot, a marker strip with one set of marks per detection block: a cyan dot
 *    where the detector flagged it (hollow when no incident owns it: a false alarm), a cyan
 *    line from it to the agent's first decision (to "now" while it waits), and an amber
 *    diamond at that decision, one row below the dot so the two never cover each other
 *  The summary table and the cards' status come ready-made as well.
 *  All positions are axis-relative seconds (0 = left edge, axisSeconds = now). The DOM is
 *  made once and updated in place by every snapshot (D-062): a card per service, a box per
 *  incident and a mark group per block, kept by key; a ResizeObserver redraws a chart whose
 *  width changes.
 */

// ---------- WebSocket ----------
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const conn = document.getElementById("conn"), label = conn.querySelector(".conn-label");
  ws.onopen = () => { label.textContent = "LIVE"; conn.className = "conn live"; };
  ws.onclose = () => {
    label.textContent = "reconnecting…"; conn.className = "conn offline";
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
  BAD_DEPLOY_ERRORS: "Bad deploy (errors)",
  TRAFFIC_SURGE: "Traffic surge",
  UPSTREAM_DROP: "Upstream drop",
}[t] || t.toLowerCase().replace(/_/g, " ").replace(/^./, c => c.toUpperCase()));
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
// Agent output (target, reason) ends up in a tooltip's innerHTML.
const esc = v => String(v).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const clip = (text, n) => text.length > n ? `${text.slice(0, n - 1).trimEnd()}…` : text;

const ACTION = {
  ROLLBACK: "Rollback",
  SCALE_OUT: "Scale out",
  INVESTIGATE_UPSTREAM: "Investigate upstream",
  OBSERVE: "Observe",
  INVALID: "Invalid decision",
};

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
    deploys: svc.deploys || [],  // version changes on the axis, drawn on every chart of the card
  };
}

// ---------- Words ----------
const METRIC = { latency: "Latency", traffic: "Traffic", errors: "Errors" };
const TRIGGER = {
  LATENCY_HIGH: "Latency high", TRAFFIC_HIGH: "Traffic high", TRAFFIC_LOW: "Traffic low",
  ERROR_RATE_HIGH: "Error rate high",
};
const triggerLabel = b => TRIGGER[b.trigger]
  || (b.metric === "latency" ? "Latency high" : b.metric === "errors" ? "Error rate high"
    : b.direction === "down" ? "Traffic low" : "Traffic high");
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

function errorScale() {
  return { frac: v => Math.max(0, Math.min(1, v / 100)), label: `0–100 % · threshold ${num(THRESHOLD)} %` };
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
    label: `${num(lo)}–${num(hi)} ${UNIT[kind]} · log`,
  };
}

// ---------- Chart ----------
// A label band, the plot, then the marker strip: detection dots on one row, answer diamonds
// on the next (an answer lands about a second after its detection, two pixels apart).
const PLOT_TOP = 18, PLOT_BOTTOM = 68;
const DET_Y = PLOT_BOTTOM + 9, ANS_Y = PLOT_BOTTOM + 20, CHART_H = PLOT_BOTTOM + 26;
const DOT_R = 3.5, DIAMOND_R = 4.5;
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
    host._layers = Object.fromEntries(["boxes", "deploys", "plot", "hover", "marks"].map(n => [n, s("g", {}, host._svg)]));
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
  const plotH = PLOT_BOTTOM - PLOT_TOP;
  const Y = v => PLOT_TOP + plotH * (1 - scale.frac(v));

  const { boxes, deploys, plot, hover, marks } = chartLayers(host);
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

  // The version changes (D-067): a thin line across the plot at each deploy and rollback,
  // a keyboard stop with its own tooltip; the pointer gets the same words on the line's.
  sync(deploys, m.deploys, d => d.key, () => {
    const g = s("g", { tabindex: 0 });
    s("line", { class: "deploy-line" }, g);
    return g;
  }, (g, d) => {
    const x = X(d.t);
    setClass(g, d.kind === "ROLLBACK" ? "deploy rollback" : "deploy");
    g.setAttribute("data-tip", deployTip(d));
    attrs(g.firstChild, { x1: x, x2: x, y1: PLOT_TOP, y2: PLOT_BOTTOM });
  });

  // The label band, the baseline, the threshold (errors chart) and the line. A missing second
  // (the simulator stopped), or one without requests on the errors chart, breaks the line
  // instead of bridging it.
  plot.replaceChildren();
  const label = s("text", { class: "scale", x: 0, y: PLOT_TOP - 6 }, plot);
  s("tspan", { class: "metric-name" }, label).textContent = METRIC[m.kind];
  s("tspan", {}, label).textContent = ` · ${recs.length ? scale.label : "No records yet"}`;
  s("line", { class: "baseline", x1: 0, x2: w, y1: PLOT_BOTTOM + 0.5, y2: PLOT_BOTTOM + 0.5 }, plot);
  if (m.kind === "errors") {
    const y = Y(THRESHOLD);
    s("line", { class: "threshold", x1: 0, x2: w, y1: y, y2: y }, plot);
  }
  let d = "", prev = null;
  for (const r of recs) {
    const v = m.value(r);
    if (v == null) { prev = null; continue; }
    d += `${prev == null || r.t - prev > 1.5 ? "M" : "L"} ${X(r.t).toFixed(1)} ${Y(v).toFixed(1)} `;
    prev = r.t;
  }
  if (d) s("path", { class: "series", d }, plot);

  // Hover: a hit area over the label band and the plot; the crosshair snaps to the nearest
  // second, and inside an incident's box the tooltip tells that incident as well.
  if (!hover.firstChild) {
    const cross = s("g", { class: "crosshair", visibility: "hidden" }, hover);
    s("line", { class: "cross", y1: PLOT_TOP, y2: PLOT_BOTTOM }, cross);
    s("circle", { class: "cross-dot", r: 3.5 }, cross);
    s("rect", { class: "hit", x: 0, y: 0, height: PLOT_BOTTOM }, hover);
  }
  const [cross, hit] = hover.children, [cl, cd] = cross.children;
  hit.setAttribute("width", w);
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

// ---------- Cards ----------
// A service's card: made once, then updated by every snapshot. Each chart host gets its
// height before anything in the card is drawn or measured: a host at 0 px would shrink the
// page for one layout, and the browser would clamp the scroll position to that.
function createCard() {
  const card = document.createElement("article");
  card.innerHTML = `
    <header class="card-head">
      <div class="who">
        <h2></h2>
        <span class="status"><span class="dot"></span><span class="status-text"></span></span>
      </div>
    </header>
    <div class="charts"></div>
    <div class="axis" aria-hidden="true"></div>`;
  const status = card.querySelector(".status");
  card._ui = {
    name: card.querySelector("h2"), status, statusText: status.lastElementChild,
    charts: [], chartsKey: null,
  };
  const axis = card.querySelector(".axis");
  for (let i = 0; i <= 5; i++) {
    const sec = i * AXIS / 5;
    const span = document.createElement("span");
    span.style.left = `${(sec / AXIS) * 100}%`;
    span.textContent = i === 5 ? "now" : `−${Math.round((AXIS - sec) / 60)}m`;
    axis.appendChild(span);
  }
  return card;
}

// The card's chart hosts, one per kind the snapshot names (svc.charts, D-067): made on the
// first update, before anything is drawn, and again only if the list changes.
function ensureCharts(card, kinds) {
  const ui = card._ui, key = kinds.join(",");
  if (ui.chartsKey === key) return;
  ui.chartsKey = key;
  for (const c of ui.charts) ro.unobserve(c.host);
  const charts = card.querySelector(".charts");
  charts.replaceChildren();
  ui.charts = kinds.map(kind => {
    const host = document.createElement("div");
    host.className = "chart-host";
    host.style.height = `${CHART_H}px`;
    host._draw = () => drawChart(host);
    ro.observe(host);
    charts.appendChild(host);
    return { kind, host };
  });
}

function updateCard(card, svc) {
  const ui = card._ui, status = svc.status || "healthy";
  ensureCharts(card, svc.charts || ["latency", "traffic"]);
  setClass(card, `card ${status}`);
  setText(ui.name, svc.name);
  setClass(ui.status, `status ${status}`);
  setText(ui.statusText, svc.statusText || "");
  for (const c of ui.charts) { c.host._m = metricModel(svc, c.kind); c.host._draw(); }
}

// ---------- Summary ----------
// One cell per summary field, whole numbers; a field the snapshot leaves empty (no median
// before the first answer) reads 0.
const summaryCells = [...document.querySelectorAll("#summary td[data-k]")];
function renderSummary(S = {}) {
  for (const td of summaryCells) {
    const v = Math.round(S[td.dataset.k] ?? 0);
    setText(td, String(v));
    if (td.dataset.k === "missed") setClass(td, v > 0 ? "miss" : "");
  }
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

function render(D) {
  AXIS = D.axisSeconds || 600;
  THRESHOLD = D.errorThresholdPct ?? THRESHOLD;
  sync(document.getElementById("cards"), D.services || [], svc => svc.name, createCard, updateCard);
  renderSummary(D.summary || {});
  setText(document.getElementById("updated"),
    new Date((D.updatedAt || Date.now() / 1000) * 1000)
      .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
  refreshTip();
}
