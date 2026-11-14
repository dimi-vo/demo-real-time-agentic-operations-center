"use strict";

/*  Ops wall — variant-E render code, driven by the live /ws snapshot (D-054).
 *
 *  The wall's state does all the counting; this page only draws what the snapshot says:
 *  - upper half: each injected incident, with its verdict (answered, pending, missed)
 *  - lower half: one block per anomaly run, from the moment the detector flagged it to the
 *    moment the agent's first decision was written (open on the right while it waits); the
 *    snapshot names its owning incident (none ⇒ false alarm), its closing action and the
 *    counts of all its decisions, all shown in its tooltip (no icons on the chart)
 *  - the summary numbers and the tile status come ready-made as well
 *  All positions are axis-relative seconds (0 = left edge, axisSeconds = now). The DOM is
 *  rebuilt on every snapshot (six rows); a ResizeObserver redraws the charts.
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
function s(tag, attrs = {}, parent) {
  const n = document.createElementNS(NS, tag);
  for (const k in attrs) n.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(n);
  return n;
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

// ---------- Chart ----------
function drawChart(host, m) {
  const w = Math.max(160, Math.floor(host.clientWidth));
  const quiet = isQuiet(m);
  const H = quiet ? 34 : 78, zero = H / 2, lane = quiet ? H / 2 : 32;
  const X = t => (Math.max(0, Math.min(t, AXIS)) / AXIS) * w;

  host.innerHTML = "";
  const svg = s("svg", { class: "chart", width: w, height: H, viewBox: `0 0 ${w} ${H}` }, host);

  for (let i = 1; i < 10; i++) {
    const x = Math.round(X(i * AXIS / 10)) + 0.5;
    s("line", { class: "grid", x1: x, x2: x, y1: 0, y2: H }, svg);
  }

  s("line", { class: "zero", x1: 0, x2: w, y1: zero + 0.5, y2: zero + 0.5 }, svg);
  s("line", { class: "now", x1: w - 0.5, x2: w - 0.5, y1: 0, y2: H }, svg);

  if (quiet) {
    const t = s("text", { class: "quiet-label", x: 8, y: zero - 6 }, svg);
    t.textContent = "No incidents or detections";
    return;
  }

  // Upper half: the injected incidents. Pending stays plain purple; only missed is dashed.
  for (const t of m.truth) {
    const missed = t.verdict === "missed";
    const x0 = X(t.start), x1 = X(t.end);
    s("path", {
      class: missed ? "missed" : "truth",
      d: bar(x0, zero - lane, x1 - x0, lane - 1, 3, t.end >= AXIS || t.open),
      tabindex: 0, "data-tip": incidentTip(t),
    }, svg);
    if (missed && x1 - x0 > 52) {
      const l = s("text", { class: "bar-label t", x: x0 + 7, y: zero - lane / 2 + 4 }, svg);
      l.textContent = "Missed";
    }
  }

  // Lower half: one block per anomaly run, from detection to the agent's answer.
  for (const b of m.blocks) {
    const fa = b.incidentId == null;
    const x0 = X(b.start), x1 = X(b.end);
    s("path", {
      class: fa ? "false-alarm" : "detect",
      d: bar(x0, zero + 1, x1 - x0, lane - 1, 3, b.open),
      tabindex: 0, "data-tip": blockTip(m, b),
    }, svg);
    if (fa && x1 - x0 > 76) {
      const l = s("text", { class: "bar-label d", x: x0 + 7, y: zero + lane / 2 + 5 }, svg);
      l.textContent = "No incident";
    }
  }

  // Time to detect: from the incident's start to its first block's start.
  for (const t of m.truth) {
    if (t.verdict === "missed" || t.detectedAt == null || !(t.timeToDetect > 0)) continue;
    const x0 = X(t.start), x1 = X(t.detectedAt), y = zero + 10;
    s("path", { class: "lag", d: `M ${x0} ${y - 4} V ${y + 4} M ${x0} ${y} H ${x1 - 3} M ${x1 - 3} ${y - 4} V ${y + 4}` }, svg);
    const label = s("text", { class: "lag-label", y: y + 15 }, svg);
    label.textContent = `+${dur(t.timeToDetect)}`;
    if (x1 - x0 >= 40) { label.setAttribute("x", (x0 + x1) / 2); label.setAttribute("text-anchor", "middle"); }
    else { label.setAttribute("x", x0 - 5); label.setAttribute("text-anchor", "end"); }
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

function serviceRow(svc, hosts) {
  const models = [metricModel(svc, "latency"), metricModel(svc, "traffic")];
  const row = document.createElement("article");
  row.className = `row ${svc.status || "healthy"}`;
  const rateStr = formatRate(svc.rateRps);
  const errStr = formatPct(svc.errorRatePct);
  const errHot = svc.errorRatePct != null && svc.errorRatePct >= 1 ? "hot" : "";
  const note = railNote(models);
  row.innerHTML = `
    <div class="rail">
      <div class="name-line"><h2>${svc.name}</h2><span class="tier">${svc.tier || ""}</span></div>
      <div class="status ${svc.status || "healthy"}"><span class="dot"></span>${svc.statusText || ""}</div>
      ${note ? `<p class="note">${note}</p>` : ""}
      <dl class="stats">
        <div><dt>Version</dt><dd>v${svc.version || "—"}</dd></div>
        <div><dt>Replicas</dt><dd>${svc.replicas != null ? svc.replicas : "—"}</dd></div>
        <div><dt>Rate</dt><dd>${rateStr}</dd></div>
        <div><dt>Errors</dt><dd class="${errHot}">${errStr}</dd></div>
      </dl>
    </div>
    <div class="metrics"></div>`;
  const metrics = row.querySelector(".metrics");
  for (const m of models) {
    const wrap = document.createElement("div");
    wrap.className = "metric" + (isQuiet(m) ? " quiet" : "");
    wrap.innerHTML = `<div class="m-label">${m.kind === "latency" ? "Latency" : "Traffic"}</div><div class="chart-host"></div>`;
    const host = wrap.querySelector(".chart-host");
    host._model = m;
    hosts.push(host);
    metrics.appendChild(wrap);
  }
  return row;
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
  document.getElementById("summary").innerHTML = html;
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
    <span class="item">${swatch("fill:var(--truth)")}Injected (up)</span>
    <span class="item">${swatch("fill:var(--detect)")}Detected → answered (down)</span>
    <span class="item"><svg width="22" height="12"><path d="M 1 2 V 10 M 1 6 H 21 M 21 2 V 10" stroke="var(--text)" stroke-width="1.25" fill="none"/></svg>Time to detect</span>`;
  lg.dataset.rendered = "1";
}

// ---------- Tooltip ----------
const tip = document.getElementById("tip");
function placeTip(x, y) {
  const r = tip.getBoundingClientRect();
  let left = x + 14, top = y + 16;
  if (left + r.width > innerWidth - 8) left = x - r.width - 14;
  if (top + r.height > innerHeight - 8) top = y - r.height - 12;
  tip.style.left = `${Math.max(8, left)}px`;
  tip.style.top = `${Math.max(8, top)}px`;
}
function showTip(node, x, y) {
  tip.innerHTML = node.getAttribute("data-tip");
  tip.classList.add("on");
  placeTip(x, y);
}
const tipTarget = e => e.target.closest?.("[data-tip]");
document.addEventListener("pointerover", e => { const n = tipTarget(e); if (n) showTip(n, e.clientX, e.clientY); });
document.addEventListener("pointermove", e => { if (tip.classList.contains("on") && tipTarget(e)) placeTip(e.clientX, e.clientY); });
document.addEventListener("pointerout", e => { if (tipTarget(e)) tip.classList.remove("on"); });
document.addEventListener("focusin", e => {
  const n = tipTarget(e); if (!n) return;
  const r = n.getBoundingClientRect();
  showTip(n, r.left + r.width / 2, r.bottom);
});
document.addEventListener("focusout", () => tip.classList.remove("on"));

// ---------- Render ----------
let ro = null;
function render(D) {
  AXIS = D.axisSeconds || 600;
  renderAxis();
  renderLegend();
  const rowsEl = document.getElementById("rows");
  rowsEl.innerHTML = "";
  const hosts = [];
  for (const svc of D.services || []) rowsEl.appendChild(serviceRow(svc, hosts));
  renderSummary(D.summary || {});
  document.getElementById("updated").textContent =
    new Date((D.updatedAt || Date.now() / 1000) * 1000)
      .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  if (ro) ro.disconnect();
  ro = new ResizeObserver(entries => {
    for (const e of entries) {
      const h = e.target, w = Math.floor(e.contentRect.width);
      if (h._w !== w) { h._w = w; drawChart(h, h._model); }
    }
  });
  hosts.forEach(h => ro.observe(h));
}
