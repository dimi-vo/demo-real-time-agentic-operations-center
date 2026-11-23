"""Data x detector tuning harness (issue 05, D-059).

Data: the service_metrics model (one record per service per second; a 10-s window is the SUM
of 10 Poisson traffic counts and the plain AVG of 10 per-second mean latencies), built by
sampler.window(). Schedule: issue 06 (per-service, overlap, 20 s gap per service,
per-type rests, first three one of each type, 50/25/25, quiet period
(minTrainingSize + 1) windows + one window, rounded up). Detector: the replica's AR(p) with
any refit interval (a running-sums fast path, checked against scripts.tuning.arima.detect by
`selftest`). Scored with scripts.evaluation.detection.score, as `uv run evaluate` does.

Usage (from the repo root; NAME is a key of CONFIGS or an expression like "C(conf=99.95, dur=30)",
RESTS is BAD_DEPLOY/TRAFFIC_SURGE/UPSTREAM_DROP in seconds, e.g. 390/300/300):
  uv run python -m scripts.tuning.research.tune selftest
  uv run python -m scripts.tuning.research.tune search SEEDS VSEEDS MINUTES NAME... [--mix]
  uv run python -m scripts.tuning.research.tune validate NAME RESTS SEEDS MINUTES
  uv run python -m scripts.tuning.research.tune compete NAME RESTS SEEDS MINUTES
  uv run python -m scripts.tuning.research.tune quiet NAME...
"""

from __future__ import annotations

import dataclasses
import itertools
import math
import random
import sys
import zlib
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from scripts.evaluation.detection import DETECTED, LATE, NO_DATA, HealthRow, Incident, score
from scripts.tuning.arima import DetectorConfig, _solve
from scripts.tuning.research import sampler

W = 10.0
START = 1_800_000_000.0
TYPES = ("BAD_DEPLOY", "TRAFFIC_SURGE", "UPSTREAM_DROP")
SHORT = {"BAD_DEPLOY": "BD", "TRAFFIC_SURGE": "SURGE", "UPSTREAM_DROP": "DROP"}
TRIGGER = {"BAD_DEPLOY": "LATENCY_HIGH", "TRAFFIC_SURGE": "TRAFFIC_HIGH", "UPSTREAM_DROP": "TRAFFIC_LOW"}
WEIGHTS = {"BAD_DEPLOY": 0.5, "TRAFFIC_SURGE": 0.25, "UPSTREAM_DROP": 0.25}
SERVICES = ("web-gateway", "checkout", "payments")
# D-067: checkout gets bad deploys and surges, payments bad deploys and drops; web-gateway
# deploys on its own clock and is detected by an error-rate threshold, so it is not scored
# here (D-059/D-060 had web-gateway BAD_DEPLOY + TRAFFIC_SURGE, the others BAD_DEPLOY +
# UPSTREAM_DROP).
TYPES_OF = {
    "web-gateway": (),
    "checkout": ("BAD_DEPLOY", "TRAFFIC_SURGE"),
    "payments": ("BAD_DEPLOY", "UPSTREAM_DROP"),
}
ARIMA_SERVICES = tuple(s for s in SERVICES if TYPES_OF[s])
MEDIAN = {"web-gateway": 35.0, "checkout": 140.0, "payments": 220.0}
GRID = (0, 60, 120, 180, 240, 300, 420, 600, 900, 1200)


@dataclass(frozen=True)
class C:
    means: tuple[float, float, float] = (50.0, 30.0, 30.0)  # web-gateway, checkout, payments
    surge: float = 5.0
    drop: float = 0.1
    bd: float = 4.0
    dur: int = 20
    conf: float = 99.9
    p: int = 1
    min_t: int = 12
    max_t: int = 360
    upd: int = 1
    gap: int = 20
    sched_min_t: int | None = None  # the simulator's quiet period follows OUR minTrainingSize, not an attendee's

    def det(self) -> DetectorConfig:
        return DetectorConfig(
            min_training=self.min_t, max_training=self.max_t, confidence=self.conf, p=self.p, update_interval=self.upd
        )

    def quiet(self) -> float:
        m = self.min_t if self.sched_min_t is None else self.sched_min_t
        return math.ceil(((m + 1) * W + W) / W) * W

    def effect(self, t: str) -> tuple[float, float]:
        return {"BAD_DEPLOY": (1.0, self.bd), "TRAFFIC_SURGE": (self.surge, 1.0), "UPSTREAM_DROP": (self.drop, 1.0)}[t]

    def label(self) -> str:
        base = C()
        parts = [f"{f.name}={getattr(self, f.name)}" for f in dataclasses.fields(self)
                 if getattr(self, f.name) != getattr(base, f.name)]  # fmt: skip
        return " ".join(parts) or "start"


# --- detector: AR(p), refit every `upd` rows, running sums ------------------------------


def fast_detect(series: list[float], cfg: DetectorConfig) -> list[tuple | None]:
    """Same output as scripts.tuning.arima.detect: (forecast, lower, upper) or None."""
    p, k = cfg.p, cfg.p + 1
    whole = all(float(v).is_integer() for v in series)
    z = cfg.z
    s = [[0.0] * k for _ in range(k)]
    b = [0.0] * k
    yy = 0.0
    n = 0
    lo = hi = p  # targets i in [lo, hi) are in the sums

    def xrow(i):
        return [1.0] + [series[i - j] for j in range(1, p + 1)]

    def upd(i, sign):
        nonlocal yy, n
        x, y = xrow(i), series[i]
        for a in range(k):
            b[a] += sign * x[a] * y
            for c in range(k):
                s[a][c] += sign * x[a] * x[c]
        yy += sign * y * y
        n += sign

    out: list[tuple | None] = []
    params = None
    since = cfg.update_interval
    for t in range(len(series)):
        h0 = max(0, t - cfg.max_training)
        while hi < max(t, p):
            upd(hi, 1)
            hi += 1
        while lo < h0 + p:
            upd(lo, -1)
            lo += 1
        if t - h0 < cfg.min_training + cfg.warmup_extra:
            out.append(None)
            continue
        if params is None or since >= cfg.update_interval:
            since = 0
            if n <= p + 1:
                params = None
            else:
                beta = _solve([row[:] for row in s], b[:])
                if beta is None:
                    hist = series[h0:t]
                    params = ([sum(hist) / len(hist)] + [0.0] * p, 0.0)
                else:
                    sse = yy - 2 * sum(beta[a] * b[a] for a in range(k))
                    sse += sum(beta[a] * s[a][c] * beta[c] for a in range(k) for c in range(k))
                    params = (beta, math.sqrt(max(sse, 0.0) / max(1, n - cfg.sigma_ddof)))
        since += 1
        if params is None:
            out.append(None)
            continue
        beta, sigma = params
        f = beta[0] + sum(beta[j] * series[t - j] for j in range(1, p + 1))
        lo_b, up_b = f - z * sigma, f + z * sigma
        if whole:
            f, lo_b, up_b = float(round(f)), float(round(lo_b)), float(round(up_b))
        out.append((f, lo_b, up_b))
    return out


def detect_rows(svc: str, win: list[tuple[float, int, float]], cfg: DetectorConfig) -> list[HealthRow]:
    lat = fast_detect([x[2] for x in win], cfg)
    trf = fast_detect([float(x[1]) for x in win], cfg)
    rows = []
    for (ws, cnt, la), lp, tp in zip(win, lat, trf, strict=True):
        if lp is None or tp is None:
            rows.append(HealthRow(svc, ws, ws + W, "WARMING_UP", "WARMING_UP", avg_latency_ms=la, traffic_count=cnt))
            continue
        ls = "HIGH" if la > lp[2] else "NORMAL"
        ts = "HIGH" if cnt > tp[2] else "LOW" if cnt < tp[1] else "NORMAL"
        rows.append(HealthRow(svc, ws, ws + W, ls, ts, avg_latency_ms=la, traffic_count=cnt))
    return rows


# --- schedule (issue 06) -----------------------------------------------------------------


def schedule(seed: int, c: C, rests: dict[str, float], minutes: float) -> list[tuple[str, str, float, float]]:
    rng = random.Random(seed)
    q = START + c.quiet()
    free_at = dict.fromkeys(SERVICES, q)
    last: dict[tuple[str, str], float] = {}
    seen: set[str] = set()
    out = []
    b, end = q, START + minutes * 60
    while b < end:
        order = list(SERVICES)
        rng.shuffle(order)
        for s in order:
            if b < free_at[s]:
                continue
            allowed = [t for t in TYPES_OF[s] if b - last.get((s, t), -1e18) >= rests[t]]
            if len(seen) < 3:
                allowed = [t for t in allowed if t not in seen]
            if not allowed:
                continue
            t = rng.choices(allowed, weights=[WEIGHTS[x] for x in allowed], k=1)[0]
            out.append((t, s, b, b + c.dur))
            free_at[s] = b + c.dur + c.gap
            last[(s, t)] = b + c.dur
            seen.add(t)
        b += W
    return out


def windows(svc: str, c: C, n: int, incs: list[tuple[int, str]], rng) -> list[tuple[float, int, float]]:
    rps = dict(zip(SERVICES, c.means, strict=True))[svc]
    eff = {}
    for w0, t in incs:
        for w in range(w0, w0 + c.dur // 10):
            eff[w] = c.effect(t)
    out = []
    for w in range(n):
        lm, latm = eff.get(w, (1.0, 1.0))
        cnt, lat = sampler.window(MEDIAN[svc], rps, lm, latm, rng)
        out.append((START + w * W, cnt, lat))
    return out


def rng_for(*key) -> random.Random:
    return random.Random(zlib.crc32(repr(key).encode()))


# --- one run, summaries --------------------------------------------------------------------


def run_demo(args):
    c, rests, seed, minutes = args
    incs = schedule(seed, c, rests, minutes)
    n = int(minutes * 6)
    det = c.det()
    rows = []
    data = (c.means, c.surge, c.drop, c.bd, c.dur)  # the data seed ignores the detector
    for s in ARIMA_SERVICES:
        mine = [(int((st - START) // W), t) for t, sv, st, _ in incs if sv == s]
        rows += detect_rows(s, windows(s, c, n, mine, rng_for("d", data, seed, s)), det)
    incidents = [Incident(f"I{i}", t, s, st, en, TRIGGER[t], "") for i, (t, s, st, en) in enumerate(incs)]
    rep = score(rows, incidents, START, START + minutes * 60 - 100)
    res = [(r.incident.type, r.status, r.first_window) for r in rep.incidents if r.status != NO_DATA]
    fps = [f.tag for sv in rep.services.values() for f in sv.false_positives]
    fresh = sum(len(sv.fresh_start_noise) for sv in rep.services.values())
    hours = sum(sv.scored_hours for sv in rep.services.values())
    return res, fps, hours, len(incs), minutes, fresh


def summarize(outs) -> dict:
    agg = defaultdict(lambda: [0, 0])
    fw = defaultdict(int)
    late = 0
    for res, *_ in outs:
        for t, st, w in res:
            agg[t][0] += st == DETECTED
            agg[t][1] += 1
            late += st == LATE
            fw[w if w is not None else "miss"] += 1
    fps = [f for o in outs for f in o[1]]
    hours = sum(o[2] for o in outs)
    tot = max(1, sum(agg[t][1] for t in TYPES))
    return {
        "rec": {t: 100 * agg[t][0] / max(1, agg[t][1]) for t in TYPES},
        "n": {t: agg[t][1] for t in TYPES},
        "miss": {t: agg[t][1] - agg[t][0] for t in TYPES},
        "mix": {t: 100 * agg[t][1] / tot for t in TYPES},
        "inc_h": sum(o[3] for o in outs) / sum(o[4] for o in outs) * 60,
        "echo_h": sum(1 for f in fps if f == "echo") / hours,
        "noise_h": sum(1 for f in fps if f != "echo") / hours,
        "fresh": sum(o[5] for o in outs) / (3 * len(outs)),
        "late": late,
        "fw": {k: 100 * v / tot for k, v in fw.items()},
    }


def evaluate(pool, c, rests, seeds, minutes, seed0=0) -> dict:
    args = [(c, rests, s, minutes) for s in range(seed0, seed0 + seeds)]
    return summarize(list(pool.map(run_demo, args, chunksize=2)))


def search(pool, c, seeds, minutes, target=99.5) -> dict | None:
    """Smallest grid rest per type with recall >= target (other types resting 1200 s), then
    raise any type that falls below the target when all three run at their minimum."""
    best = {}
    for t in TYPES:
        best[t] = None
        for r in GRID:
            rests = dict.fromkeys(TYPES, 1200)
            rests[t] = r
            if evaluate(pool, c, rests, seeds, minutes)["rec"][t] >= target:
                best[t] = r
                break
        if best[t] is None:
            return None
    for _ in range(6):
        s = evaluate(pool, c, best, seeds, minutes)
        low = [t for t in TYPES if s["rec"][t] < target]
        if not low:
            return best
        for t in low:
            best[t] = GRID[min(GRID.index(best[t]) + 1, len(GRID) - 1)]
    return best


def step_up(rests):
    return {t: GRID[min(GRID.index(r) + 1, len(GRID) - 1)] for t, r in rests.items()}


def sched_rate(args) -> tuple[float, dict]:
    """Steady-state incidents/h and mix from the schedule alone (no data, no detector)."""
    c, rests, seeds, minutes = args
    n = 0
    mix = defaultdict(int)
    for seed in range(seeds):
        incs = [x for x in schedule(1000 + seed, c, rests, minutes) if x[2] >= START + 1800]
        n += len(incs)
        for t, *_ in incs:
            mix[t] += 1
    hours = seeds * (minutes - 30) / 60
    return n / hours, {t: 100 * mix[t] / max(1, n) for t in TYPES}


def rests_for_rate(pool, c, floor: dict, target=60.0, step=30, span=600):
    """Rests >= floor (per type) giving ~target incidents/h, mix closest to 50/25/25."""
    cands = [dict(zip(TYPES, combo, strict=True)) for combo in itertools.product(
        *[range(floor[t], floor[t] + span + 1, step) for t in TYPES])]  # fmt: skip
    rates = list(pool.map(sched_rate, [(c, r, 3, 600) for r in cands], chunksize=16))
    ok = [(r, rate, mix) for r, (rate, mix) in zip(cands, rates, strict=True) if abs(rate - target) <= 1.5]
    if not ok:
        return None
    goal = {"BAD_DEPLOY": 50, "TRAFFIC_SURGE": 25, "UPSTREAM_DROP": 25}
    return min(ok, key=lambda x: (sum(abs(x[2][t] - goal[t]) for t in TYPES), sum(x[0].values())))


def rests_bd_only(c, floor: dict, target=60.0, step=30):
    """Traffic rests stay at the floor (more traffic incidents = wider traffic bands = fewer
    echoes; latency can't echo); only the BAD_DEPLOY rest grows until ~target/h."""
    r = dict(floor)
    rate, _ = sched_rate((c, r, 3, 600))
    if rate < target - 1.5:
        return None
    while rate > target + 0.5 and r["BAD_DEPLOY"] < 3600:
        r["BAD_DEPLOY"] += step
        rate, _ = sched_rate((c, r, 3, 600))
    return r, rate


def fmt(label, rests, s):
    rs = "/".join(f"{rests[t]:g}" for t in TYPES)
    rec = " ".join(f"{SHORT[t]} {s['rec'][t]:.2f} ({s['miss'][t]}/{s['n'][t]})" for t in TYPES)
    mix = "/".join(f"{s['mix'][t]:.0f}" for t in TYPES)
    fw = " ".join(f"w{k}:{v:.1f}" for k, v in sorted(s["fw"].items(), key=lambda kv: str(kv[0])))
    return (
        f"{label} rests {rs}: {s['inc_h']:.1f} inc/h | mix {mix}% | {rec} | late {s['late']} | "
        f"FP/svc-h noise {s['noise_h']:.3f} echo {s['echo_h']:.3f} | fresh-start/svc {s['fresh']:.3f} | {fw}"
    )


# --- quiet data noise (no incidents) ----------------------------------------------------------


def run_quiet(args):
    c, s, seed, hours = args
    win = windows(s, c, int(hours * 360), [], rng_for("q", c.means, s, seed))
    rows = detect_rows(s, win, c.det())
    scored = [r for r in rows[30:] if not r.warming_up]
    lat = sum(r.latency_status == "HIGH" for r in scored)
    hi = sum(r.traffic_status == "HIGH" for r in scored)
    lo = sum(r.traffic_status == "LOW" for r in scored)
    return lat, hi, lo, len(scored)


def quiet(pool, c, seeds=30, hours=3) -> dict:
    out = {}
    for s in SERVICES:
        o = list(pool.map(run_quiet, [(c, s, k, hours) for k in range(seeds)]))
        h = sum(x[3] for x in o) / 360
        out[s] = tuple(sum(x[i] for x in o) / h for i in range(3))
    return out


# --- named configs ---------------------------------------------------------------------------

CONFIGS = {
    "start": C(),
    "c99.5": C(conf=99.5),
    "c99.95": C(conf=99.95),
    "c99.99": C(conf=99.99),
    "d30": C(dur=30),
    "d30c99.99": C(dur=30, conf=99.99),
    "m25": C(means=(25.0, 15.0, 15.0)),
    "m100": C(means=(100.0, 60.0, 60.0)),
    "small": C(surge=3.0, drop=0.2, bd=3.0),
    "smaller": C(surge=2.5, drop=0.3, bd=2.5),
    "big": C(surge=8.0, drop=0.05, bd=6.0),
    "max180": C(max_t=180),
    "max720": C(max_t=720),
    "min6": C(min_t=6),
    "min24": C(min_t=24),
    "ar2": C(p=2),
    "upd5": C(upd=5),
}


def named(name: str) -> C:
    return CONFIGS[name] if name in CONFIGS else eval(name)  # a developer's own expression: "C(conf=99.95, dur=30)"


def selftest():
    from scripts.tuning.arima import detect

    rng = random.Random(3)
    series_set = [
        [float(rng.randint(20, 40)) for _ in range(300)] + [5.0] * 4 + [float(rng.randint(20, 40)) for _ in range(200)],
        [rng.gauss(100, 8) + (300 if 200 <= i < 202 else 0) for i in range(600)],
    ]
    for series in series_set:
        for p, u, mn, mx in ((1, 1, 12, 360), (2, 1, 12, 360), (1, 5, 12, 360), (2, 5, 6, 50), (3, 1, 12, 100)):
            cfg = DetectorConfig(min_training=mn, max_training=mx, p=p, update_interval=u)
            a, b = fast_detect(series, cfg), detect(series, cfg)
            for x, y in zip(a, b, strict=True):
                assert (x is None) == y.warming_up, (p, u)
                if x is not None:
                    assert abs(x[0] - y.forecast) < 1e-6 and abs(x[2] - y.upper) < 1e-6, (p, u, x, y)
    print("fast_detect matches arima.detect")


if __name__ == "__main__":
    part = sys.argv[1]
    if part == "selftest":
        selftest()
        sys.exit()
    with ProcessPoolExecutor() as pool:
        if part == "search":
            seeds, vseeds, minutes = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
            for name in [a for a in sys.argv[5:] if not a.startswith("--")]:
                c = named(name)
                mn = search(pool, c, seeds, minutes)
                if mn is None:
                    print(f"{name:12} [{c.label()}]: no rests found", flush=True)
                    continue
                m = step_up(mn)
                rate, mix = sched_rate((c, m, 3, 600))
                s = evaluate(pool, c, m, vseeds, minutes, seed0=10_000)
                mns = "/".join(str(mn[t]) for t in TYPES)
                print(fmt(f"{name:12} [{c.label()}] min {mns} margin(steady {rate:.0f}/h)", m, s), flush=True)
                pick = rests_bd_only(c, m)
                if pick is None:
                    print(f"{name:12}   60/h: margin rate {rate:.0f}/h is below 60/h", flush=True)
                    continue
                r60, rate60 = pick
                s60 = evaluate(pool, c, r60, vseeds, minutes, seed0=20_000)
                print(fmt(f"{name:12}   60/h BD rest (steady {rate60:.1f}/h)", r60, s60), flush=True)
                if "--mix" in sys.argv:
                    pick = rests_for_rate(pool, c, m)
                    if pick is not None:
                        r60, rate60, _ = pick
                        s60 = evaluate(pool, c, r60, vseeds, minutes, seed0=20_000)
                        print(fmt(f"{name:12}   60/h mix     (steady {rate60:.1f}/h)", r60, s60), flush=True)
        elif part == "validate":
            # validate NAME RESTS SEEDS MINUTES: one setup at given rests, fresh seeds
            c, rests = named(sys.argv[2]), dict(zip(TYPES, map(int, sys.argv[3].split("/")), strict=True))
            seeds, minutes = int(sys.argv[4]), float(sys.argv[5])
            rate, mix = sched_rate((c, rests, 3, 600))
            s = evaluate(pool, c, rests, seeds, minutes, seed0=30_000)
            print(fmt(f"{sys.argv[2]:12} [{c.label()}] steady {rate:.1f}/h", rests, s), flush=True)
        elif part == "compete":
            # compete NAME RESTS SEEDS MINUTES: our data and schedule, different detectors
            c, rests = named(sys.argv[2]), dict(zip(TYPES, map(int, sys.argv[3].split("/")), strict=True))
            seeds, minutes = int(sys.argv[4]), float(sys.argv[5])
            r = dataclasses.replace
            base = r(c, sched_min_t=c.min_t)
            variants = {
                "ours": base,
                "conf 99 (looser)": r(base, conf=99.0),
                "conf 99.5": r(base, conf=99.5),
                "conf 99.9 (the D-029 reference)": r(base, conf=99.9),
                "conf 99.99 (stricter)": r(base, conf=99.99),
                "Confluent defaults as AR(1): min 128, max 512, conf 99, refit 5":
                    r(base, min_t=128, max_t=512, conf=99.0, upd=5),
                "min 6": r(base, min_t=6),
                "max 60": r(base, max_t=60),
                "AR(2)": r(base, p=2),
                "AR(3)": r(base, p=3),
            }  # fmt: skip
            for label, v in variants.items():
                s = evaluate(pool, v, rests, seeds, minutes, seed0=40_000)
                q = quiet(pool, v, seeds=10, hours=3)
                qn = sum(sum(x) for x in q.values()) / 3
                print(fmt(f"{label:62}", rests, s) + f" | quiet-data FP/svc-h {qn:.2f}", flush=True)
        elif part == "quiet":
            for name in sys.argv[2:]:
                c = named(name)
                q = quiet(pool, c)
                cells = " | ".join(f"{s} lat {v[0]:.2f} hi {v[1]:.2f} lo {v[2]:.2f}" for s, v in q.items())
                print(f"{name:12} [{c.label()}] quiet-data FP/svc-h: {cells}", flush=True)
