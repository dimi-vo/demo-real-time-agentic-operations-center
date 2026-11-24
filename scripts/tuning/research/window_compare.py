"""Compare 10 s vs 5 s windows with the repo's offline harness (sweep.py's generator + AR(1)
replica + evaluate's scorer). Patches the window constants per worker (D-064's option (b)).

Written for the engine before D-068 (surges on checkout). Kept as the record of its study; it
may not run as is.

Usage (from the repo root): uv run python -m scripts.tuning.research.window_compare
Prints, per variant in VARIANTS: the first flagged window per incident type, false positives
(echoes) per service-hour, and the forecast error and band half-width on normal windows.
"""

from __future__ import annotations

import dataclasses
import statistics as st
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

SEEDS, HOURS = 6, 3.0


def patch(window_s: float) -> None:
    import scripts.evaluation.detection as det
    import scripts.tuning.sweep as sw

    sw.WINDOW_MS = int(window_s * 1000)
    det.WINDOW_SECONDS = float(window_s)
    det.DETECT_WITHIN_WINDOWS = 10**6  # bucket by first window ourselves


def one(args):
    window_s, cfg_kw, seed = args
    patch(window_s)
    import scripts.evaluation.detection as det
    import scripts.tuning.sweep as sw

    cfg = dataclasses.replace(sw.CURRENT, **cfg_kw)
    rows, incidents, start, end, _ = sw.simulate(sw.DataConfig(aligned=True), seed, HOURS)
    scored = sw.apply_detector(rows, cfg)
    rep = det.score(scored, incidents, start, end - 100)
    out = {"inc": [], "fp": [], "hours": 0.0, "err": defaultdict(list), "band": defaultdict(list)}
    for r in rep.incidents:
        if r.status != det.NO_DATA:
            out["inc"].append((r.incident.type, r.first_window))
    for s in rep.services.values():
        out["hours"] += s.scored_hours
        out["fp"] += [fp.tag for fp in s.false_positives]
    spans = defaultdict(list)
    for i in incidents:
        spans[i.service_name].append((i.started_at, i.ended_at + 60))
    for r in scored:
        if r.latency_status in (None, "WARMING_UP") or r.window_start < start + 300:
            continue
        if any(r.window_end > a and r.window_start < b for a, b in spans[r.service_name]):
            continue  # normal windows only: not in an incident nor the minute after
        svc = "web-gateway" if r.service_name == "web-gateway" else "checkout/payments"
        out["err"][("traffic", svc)].append(abs(r.traffic_count - r.expected_traffic_count) / r.expected_traffic_count)
        out["err"][("latency", svc)].append(abs(r.avg_latency_ms - r.expected_latency_ms) / r.expected_latency_ms)
        out["band"][("traffic", svc)].append(
            (r.traffic_upper_bound - r.expected_traffic_count) / r.expected_traffic_count
        )
        out["band"][("latency", svc)].append((r.latency_upper_bound - r.expected_latency_ms) / r.expected_latency_ms)
    return out


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


VARIANTS = {
    "10 s windows, max 360 (today: 1 h history)": (10, {}),
    "5 s windows, max 360 (30 min history)": (5, {}),
    "5 s windows, max 720 (1 h history)": (5, {"max_training": 720}),
}

if __name__ == "__main__":
    with ProcessPoolExecutor() as pool:
        for label, (w, kw) in VARIANTS.items():
            outs = list(pool.map(one, [(w, kw, s) for s in range(SEEDS)]))
            print(f"\n=== {label} ===")
            by = defaultdict(list)
            for o in outs:
                for t, k in o["inc"]:
                    by[t].append(k)
            for t, ks in sorted(by.items()):
                hist = defaultdict(int)
                for k in ks:
                    hist[k if k is not None else "missed"] += 1
                print(f"  {t:14} n={len(ks):3}  first flagged window: {dict(sorted(hist.items(), key=str))}")
            hours = sum(o["hours"] for o in outs)
            fps = [t for o in outs for t in o["fp"]]
            print(
                f"  false positives: {len(fps)} in {hours:.1f} service-hours = {len(fps) / hours:.2f}/svc-h "
                f"({sum(t == 'echo' for t in fps)} echoes)"
            )
            err, band = defaultdict(list), defaultdict(list)
            for o in outs:
                for k, v in o["err"].items():
                    err[k] += v
                for k, v in o["band"].items():
                    band[k] += v
            for k in sorted(err):
                print(
                    f"  {k[0]:7} {k[1]:17} forecast error median {100 * st.median(err[k]):5.1f}%  "
                    f"p95 {100 * pct(err[k], 0.95):5.1f}% | band half-width median {100 * st.median(band[k]):5.1f}%  "
                    f"p95 {100 * pct(band[k], 0.95):5.1f}%  max {100 * max(band[k]):5.1f}%"
                )
    print(f"\n{SEEDS} seeds x {HOURS:g} h, aligned start, current generator (D-059), AR(1) replica, 99.95%.")
