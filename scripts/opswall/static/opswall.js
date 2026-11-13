"use strict";

/*  Ops wall — variant-E render code, wired to the live /ws snapshot.
 *
 *  Adapted from the mockup the user picked in .scratch/mockups/variant-e-ops-wall-dark.html:
 *  - the FAKE block is gone; every render is driven by a JSON snapshot from the server
 *  - rate / errorRate are formatted from the numeric fields the state produces
 *  - incident type labels match the simulator's enum (TRAFFIC_SURGE, UPSTREAM_DROP)
 *  - each service's warm-up band is computed from its WARMING_UP health rows, which the
 *    state now carries implicitly in the `warmup` tuple it emits (first / last axis-relative
 *    second a WARMING_UP row sat in); absent ⇒ no band
 *  - DOM is rebuilt on every snapshot (cheap; six rows), ResizeObserver still redraws charts
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
    try { latest = JSON.parse(e.data); render(latest); }
    catch (err) { console.error("bad snapshot", err); }
  };
}
connect();

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
const median = xs => {
  if (!xs.length) return null;
  const a = [...xs].sort((x, y) => x - y), m = a.length >> 1;
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
};

function formatRate(rps) {
  if (rps == null) return "—";
  return rps >= 1000 ? `${(rps / 1000).toFixed(1)}k rps` : `${Math.round(rps)} rps`;
}
function formatPct(pct) {
  if (pct == null) return "—";
  return `${pct.toFixed(1)}%`;
}

const CLOSERS = new Set(["ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM"]);
const PIN = {
  ROLLBACK:  { label: "Rollback", d: "M -6 -6 L -6 6 M -6 -6 L 6 -6 L 2.5 -2.5 L 6 1 L -6 1 Z", fill: "var(--crit)" },
  SCALE_OUT: { label: "Scale out", d: "M 0 -7 L 7 5 L -7 5 Z", fill: "var(--warn)" },
  INVESTIGATE_UPSTREAM: { label: "Investigate upstream", d: "M 2 -6 L -6 0 L 2 6 L 2 3 L 7 3 L 7 -3 L 2 -3 Z", fill: "var(--serious)" },
  OBSERVE:   { label: "Observe", note: "Keeps watching. Doesn't close the detection." },
  INVALID:   { label: "Invalid decision", note: "The agent's answer was rejected." },
};
function pinShape(action, parent) {
  const p = PIN[action] || PIN.OBSERVE;
  if (action === "OBSERVE" || !PIN[action]) {
    return s("circle", { r: 4.5, fill: "var(--ink-0)", stroke: "var(--observe)", "stroke-width": 1.75 }, parent);
  }
  if (action === "INVALID") {
    const g = s("g", {}, parent);
    s("circle", { r: 6, fill: "var(--ink-0)" }, g);
    s("path", { d: "M -3.5 -3.5 L 3.5 3.5 M -3.5 3.5 L 3.5 -3.5", stroke: "var(--text-3)", "stroke-width": 2, "stroke-linecap": "round" }, g);
    return g;
  }
  if (action === "ROLLBACK") {
    const g = s("g", {}, parent);
    s("path", { d: "M -5 -7 L 6 -7 L 2.5 -3.5 L 6 0 L -5 0 Z", fill: p.fill, stroke: "var(--ink-0)", "stroke-width": 1.25, "stroke-linejoin": "round" }, g);
    s("line", { x1: -5, y1: -7, x2: -5, y2: 7, stroke: p.fill, "stroke-width": 2, "stroke-linecap": "round" }, g);
    return g;
  }
  return s("path", {
    d: p.d, fill: p.fill, stroke: "var(--ink-0)", "stroke-width": 1.25, "stroke-linejoin": "round",
  }, parent);
}

function bar(x, y, w, h, r, openRight) {
  w = Math.max(w, 2); r = Math.min(r, w / 2, h / 2);
  const rr = openRight ? 0 : r;
  return `M ${x + r} ${y} H ${x + w - rr} ${rr ? `A ${rr} ${rr} 0 0 1 ${x + w} ${y + rr}` : ""} V ${y + h - rr} ${rr ? `A ${rr} ${rr} 0 0 1 ${x + w - rr} ${y + h}` : ""} H ${x + r} A ${r} ${r} 0 0 1 ${x} ${y + h - r} V ${y + r} A ${r} ${r} 0 0 1 ${x + r} ${y} Z`;
}

// ---------- Model ----------
function analyse(truth, detect, decisions) {
  const used = new Set(), pairs = [];
  for (const t of truth) {
    const d = detect.find(d => !used.has(d) && d.start < t.end && d.end > t.start);
    if (d) { used.add(d); pairs.push({ t, d, lag: d.start - t.start }); }
  }
  for (const p of pairs) {
    const c = decisions.find(x => CLOSERS.has(x.action) && x.at >= p.d.start && x.at <= p.d.end + 1);
    if (c) { p.closer = c; p.act = c.at - p.d.start; }
  }
  return {
    pairs,
    missed: truth.filter(t => !pairs.some(p => p.t === t)),
    falseAlarms: detect.filter(d => !used.has(d)),
  };
}

function metricModel(svc, kind, defaultWarmup) {
  const lat = kind === "latency";
  const truth = (svc.groundTruth || []).filter(g => (g.type === "BAD_DEPLOY") === lat);
  const detect = (lat ? svc.detectedLatency : svc.detectedTraffic) || [];
  const decisions = (svc.decisions || []).filter(d => (d.trigger === "LATENCY_HIGH") === lat);
  const warmup = svc.warmup || defaultWarmup;
  return { kind, truth, detect, decisions, warmup, ...analyse(truth, detect, decisions) };
}

function detectLabel(m, d) {
  if (m.kind === "latency") return "Latency high";
  return d.direction === "up" ? "Traffic high" : "Traffic low";
}

// ---------- Chart ----------
function drawChart(host, m) {
  const w = Math.max(160, Math.floor(host.clientWidth));
  const quiet = !m.truth.length && !m.detect.length && !m.decisions.length;
  const H = quiet ? 34 : 78, zero = H / 2, lane = quiet ? H / 2 : 32;
  const X = t => (Math.max(0, Math.min(t, AXIS)) / AXIS) * w;

  host.innerHTML = "";
  const svg = s("svg", { class: "chart", width: w, height: H, viewBox: `0 0 ${w} ${H}` }, host);

  for (let i = 1; i < 10; i++) {
    const x = Math.round(X(i * AXIS / 10)) + 0.5;
    s("line", { class: "grid", x1: x, x2: x, y1: 0, y2: H }, svg);
  }

  const [ws, we] = m.warmup;
  if (we > ws) {
    const r = s("rect", { class: "warmup", x: X(ws), y: zero, width: X(we) - X(ws), height: lane }, svg);
    r.setAttribute("data-tip", `<strong>Detector warming up</strong>${range(ws, we)}. Nothing can be flagged here.`);
    if (X(we) - X(ws) > 84) {
      const t = s("text", { class: "warm-label", x: X(ws) + 6, y: zero + lane - 5 }, svg);
      t.textContent = "Warming up";
    }
  }

  s("line", { class: "zero", x1: 0, x2: w, y1: zero + 0.5, y2: zero + 0.5 }, svg);
  s("line", { class: "now", x1: w - 0.5, x2: w - 0.5, y1: 0, y2: H }, svg);

  if (quiet) {
    const t = s("text", { class: "quiet-label", x: 8, y: zero - 6 }, svg);
    t.textContent = "No incidents or detections";
    return;
  }

  for (const t of m.truth) {
    const missed = m.missed.includes(t);
    const x0 = X(t.start), x1 = X(t.end);
    const p = s("path", {
      class: missed ? "missed" : "truth",
      d: bar(x0, zero - lane, x1 - x0, lane - 1, 3, t.end >= AXIS || t.open),
    }, svg);
    p.setAttribute("tabindex", "0");
    p.setAttribute("data-tip", `<strong>Injected: ${human(t.type)}</strong>${range(t.start, t.end)}` +
      (missed ? `<em>Not detected${t.start < we && t.start >= ws ? ". It started during warm-up." : "."}</em>` : ""));
    if (missed && x1 - x0 > 52) {
      const l = s("text", { class: "bar-label t", x: x0 + 7, y: zero - lane / 2 + 4 }, svg);
      l.textContent = "Missed";
    }
  }

  for (const d of m.detect) {
    const fa = m.falseAlarms.includes(d);
    const pair = m.pairs.find(p => p.d === d);
    const x0 = X(d.start), x1 = X(d.end);
    const p = s("path", {
      class: fa ? "false-alarm" : "detect",
      d: bar(x0, zero + 1, x1 - x0, lane - 1, 3, d.end >= AXIS || d.open),
    }, svg);
    p.setAttribute("tabindex", "0");
    p.setAttribute("data-tip", `<strong>Detected: ${detectLabel(m, d)}</strong>${range(d.start, d.end)}` +
      (fa ? "<em>No injected incident at this time.</em>"
          : `<em>${pair.lag > 0 ? `${dur(pair.lag)} after the incident started` : "At incident start"}${pair.closer ? `. Closed by ${PIN[pair.closer.action].label.toLowerCase()} ${dur(pair.act)} later.` : ". Still open."}</em>`));
    if (!fa) {
      for (let t = d.start + 10; t < Math.min(d.end, AXIS); t += 10) {
        const x = Math.round(X(t)) + 0.5;
        s("line", { class: "notch", x1: x, x2: x, y1: zero + lane - 7, y2: zero + lane - 2 }, svg);
      }
    } else if (x1 - x0 > 76) {
      const l = s("text", { class: "bar-label d", x: x0 + 7, y: zero + lane / 2 + 5 }, svg);
      l.textContent = "No incident";
    }
  }

  for (const p of m.pairs) {
    if (p.lag <= 0) continue;
    const x0 = X(p.t.start), x1 = X(p.d.start), y = zero + 10;
    s("path", { class: "lag", d: `M ${x0} ${y - 4} V ${y + 4} M ${x0} ${y} H ${x1 - 3} M ${x1 - 3} ${y - 4} V ${y + 4}` }, svg);
    const label = s("text", { class: "lag-label", y: y + 15 }, svg);
    label.textContent = `+${dur(p.lag)}`;
    if (x1 - x0 >= 40) { label.setAttribute("x", (x0 + x1) / 2); label.setAttribute("text-anchor", "middle"); }
    else { label.setAttribute("x", x0 - 5); label.setAttribute("text-anchor", "end"); }
  }

  for (const dec of m.decisions) {
    const g = s("g", { transform: `translate(${X(dec.at)} ${zero})`, tabindex: 0 }, svg);
    pinShape(dec.action, g);
    const pair = m.pairs.find(p => p.closer === dec);
    const info = PIN[dec.action] || PIN.OBSERVE;
    g.setAttribute("data-tip", `<strong>${info.label}</strong>At ${ago(dec.at)}` +
      (pair ? `, ${dur(pair.act)} after detection` : "") + "." +
      (info.note ? `<em>${info.note}</em>` : ""));
  }
}

// ---------- Rows ----------
function railNote(svc, models) {
  const pairs = models.flatMap(m => m.pairs).sort((a, b) => a.t.start - b.t.start);
  const missed = models.flatMap(m => m.missed.map(t => ({ t, m })));
  const fas = models.flatMap(m => m.falseAlarms);
  const parts = [];
  if (pairs.length) {
    const p = pairs[0];
    parts.push(`Detected ${dur(p.lag)} after onset.` + (p.closer ? ` Acted ${dur(p.act)} later.` : " No action yet."));
  }
  for (const { t, m } of missed) {
    const inWarm = t.start >= m.warmup[0] && t.start < m.warmup[1];
    parts.push(`${human(t.type)} at ${ago(t.start)} went undetected${inWarm ? " during warm-up" : ""}.`);
  }
  if (fas.length) parts.push(`${fas.length} detection${fas.length > 1 ? "s" : ""} with no matching incident.`);
  return parts.join(" ");
}

function serviceRow(svc, defaultWarmup, hosts) {
  const models = [metricModel(svc, "latency", defaultWarmup), metricModel(svc, "traffic", defaultWarmup)];
  const row = document.createElement("article");
  row.className = `row ${svc.status || "healthy"}`;
  const rateStr = formatRate(svc.rateRps);
  const errStr = formatPct(svc.errorRatePct);
  const errHot = svc.errorRatePct != null && svc.errorRatePct >= 1 ? "hot" : "";
  const note = railNote(svc, models);
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
    const quiet = !m.truth.length && !m.detect.length && !m.decisions.length;
    const wrap = document.createElement("div");
    wrap.className = "metric" + (quiet ? " quiet" : "");
    wrap.innerHTML = `<div class="m-label">${m.kind === "latency" ? "Latency" : "Traffic"}</div><div class="chart-host"></div>`;
    const host = wrap.querySelector(".chart-host");
    host._model = m;
    hosts.push(host);
    metrics.appendChild(wrap);
  }
  return row;
}

// ---------- Summary / axis / legend ----------
function renderSummary(allModels) {
  const pairs = allModels.flatMap(m => m.pairs);
  const injected = allModels.reduce((n, m) => n + m.truth.length, 0);
  const missed = allModels.reduce((n, m) => n + m.missed.length, 0);
  const fas = allModels.reduce((n, m) => n + m.falseAlarms.length, 0);
  const lag = median(pairs.map(p => Math.max(0, p.lag)));
  const act = median(pairs.filter(p => p.closer).map(p => p.act));
  const plural = (n, a, b) => `${n} ${n === 1 ? a : b}`;
  let html = `<b>${plural(injected, "incident", "incidents")}</b> injected. `;
  html += `<b>${pairs.length}</b> detected`;
  if (lag != null) html += `, a median of <b>${dur(lag)}</b> after they started`;
  html += ". ";
  if (act != null) html += `The agent acted a median of <b>${dur(act)}</b> after detection. `;
  const tail = [];
  if (missed) tail.push(`<b class="miss">${missed}</b> missed`);
  if (fas) tail.push(`<b>${plural(fas, "false alarm", "false alarms")}</b>`);
  if (tail.length) html += tail.join(" and ") + ".";
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
    <span class="item">${swatch("fill:var(--detect)")}Detected (down)</span>
    <span class="item">${swatch("fill:url(#hatch);stroke:var(--line)")}Detector warm-up</span>
    <span class="item"><svg width="22" height="12"><path d="M 1 2 V 10 M 1 6 H 21 M 21 2 V 10" stroke="var(--text)" stroke-width="1.25" fill="none"/></svg>Time to detect</span>
    <span class="sep"></span>`;
  for (const a of ["OBSERVE", "ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM", "INVALID"]) {
    const item = document.createElement("span");
    item.className = "item";
    const svg = s("svg", { width: 16, height: 16, viewBox: "-8 -8 16 16" });
    pinShape(a, svg);
    item.appendChild(svg);
    item.append(PIN[a].label);
    lg.appendChild(item);
  }
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
  const defaultWarmup = [0, D.warmupSeconds || 0];
  renderAxis();
  renderLegend();
  const rowsEl = document.getElementById("rows");
  rowsEl.innerHTML = "";
  const hosts = [];
  for (const svc of D.services || []) rowsEl.appendChild(serviceRow(svc, defaultWarmup, hosts));
  renderSummary(hosts.map(h => h._model));
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
