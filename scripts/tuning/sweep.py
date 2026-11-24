"""Sweep generator settings x detector settings offline and score each combination.

Usage: uv run python -m scripts.tuning.sweep [--hours 3] [--seeds 4]

For every variant it runs the real generator (`opsgen.Simulation`, with rates and
multipliers overridden) for `hours` of simulated time per seed, aggregates its per-second
metrics samples into the same 10-second windows Flink uses (SUM of traffic_count, AVG of
latency_ms, version as of the window's last tick; D-058), runs the calibrated AR(1)
replica, and scores the result with
`scripts.evaluation.detection.score`, the same code `uv run evaluate` uses. Each seed is
a fresh start, so fresh-start noise is measured too.

The replica matches Confluent within a few percent on series without big jumps, and is
conservative (bounds up to ~1.5x wider) for a while after a bad deploy or traffic spike on
the same series (see `scripts.tuning.calibrate`, D-031). Confirm the winner once live.
"""

from __future__ import annotations

import argparse
import dataclasses
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field

from scripts.evaluation.detection import DETECTED, NO_DATA, HealthRow, Incident, score
from scripts.tuning.arima import DetectorConfig, detect

WINDOW_MS = 10_000


def _defaults() -> dict[str, float]:
    import opsgen.distributions as dist
    import opsgen.engine as eng

    return {
        "drop": eng.DROP_LAMBDA_MULT,
        "out_p": dist.SLOW_OUTLIER_PROB,
        "out_m": dist.SLOW_OUTLIER_MULT,
    }


_DEFAULTS = _defaults()
START_BASE = 1_800_000_000.0  # a 10-second boundary


def _arima_services() -> tuple[str, ...]:
    import opsgen.engine as eng

    return eng.SHARED_SERVICES


# The services ARIMA scores: the shared scheduler's, payments alone since D-068 (labs 2.3-2.5
# filter to it). web-gateway is detected by an error-rate threshold instead (D-067), so its rows
# are left out of scoring.
ARIMA_SERVICES = _arima_services()


@dataclass(frozen=True)
class DataConfig:
    rates: tuple[tuple[str, float], ...] = ()  # service -> traffic mean (per second) overrides
    drop_mult: float | None = None
    outlier_prob: float | None = None
    outlier_mult: float | None = None
    aligned: bool = False  # start on a 10-second boundary (else a seed-dependent offset, like a live start)

    def label(self) -> str:
        parts = [f"{k}={v:g}" for k, v in self.rates]
        if self.aligned:
            parts.append("aligned start")
        for name in ("drop_mult", "outlier_prob", "outlier_mult"):
            v = getattr(self, name)
            if v is not None:
                parts.append(f"{name}={v:g}")
        return " ".join(parts) or "current generator"


@dataclass
class Result:
    label: str
    traffic_per_s: float = 0.0
    by_type: dict[str, list[int]] = field(default_factory=lambda: defaultdict(lambda: [0, 0]))
    fp: int = 0
    fp_echo: int = 0
    fp_by_service: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    hours_by_service: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    fresh_worst: int = 0
    fresh_total: int = 0
    fresh_starts: int = 0
    fresh_over_one: int = 0


def simulate(data: DataConfig, seed: int, hours: float) -> tuple[list[HealthRow], list[Incident], float, float, float]:
    """Run the generator and return window rows (without detector output), the incidents
    ARIMA is meant to catch (the shared scheduler's types, D-067), the run's start/end, and
    the average traffic (traffic_count per second, all metric services)."""
    import opsgen.distributions as dist
    import opsgen.engine as eng
    import opsgen.simulation as sim_mod
    from opsgen.engine import Config
    from opsgen.services import METRIC_SERVICES

    # Worker processes are reused across tasks: set every knob, defaults included.
    rates = dict(data.rates)
    sim_mod.METRIC_SERVICES = tuple(dataclasses.replace(s, rps=rates.get(s.name, s.rps)) for s in METRIC_SERVICES)
    eng.DROP_LAMBDA_MULT = _DEFAULTS["drop"] if data.drop_mult is None else data.drop_mult
    dist.SLOW_OUTLIER_PROB = _DEFAULTS["out_p"] if data.outlier_prob is None else data.outlier_prob
    dist.SLOW_OUTLIER_MULT = _DEFAULTS["out_m"] if data.outlier_mult is None else data.outlier_mult

    start = START_BASE + (0.0 if data.aligned else 0.5 + (seed * 3.7) % 9.0)
    sim = sim_mod.Simulation(seed, start=start, config=Config())
    # Per window: SUM(traffic_count), the sum of latency_ms and the number of samples, so
    # the window latency is the unweighted AVG(latency_ms) of its samples, as in Flink.
    agg: dict[tuple[str, int], list[float]] = defaultdict(lambda: [0, 0.0, 0])  # traffic, latency sum, samples
    ctx: dict[tuple[str, int], tuple[str, float]] = {}
    incidents: list[Incident] = []
    total = 0
    ticks = int(hours * 3600)
    for k in range(ticks):
        now = start + k
        metrics, _deps, events, _tickets = sim.tick(now)
        for m in metrics:
            total += m["traffic_count"]
            a = agg[(m["service_name"], m["event_time"] // WINDOW_MS)]
            a[0] += m["traffic_count"]
            a[1] += m["latency_ms"]
            a[2] += 1
        w = sim.tick_ms(now) // WINDOW_MS
        for name, d in sim.deployments.items():
            ctx[(name, w)] = (d.version, d.deployed_at)
        for ev in events:
            # Only the incidents ARIMA is meant to catch: web-gateway's BAD_DEPLOY_ERRORS (its
            # deploy clock) moves error_count alone, which an error-rate threshold flags (D-067).
            if ev.kind == "incident_end" and ev.incident is not None and ev.incident.type in eng.INCIDENT_TYPES:
                inc = ev.incident
                incidents.append(
                    Incident(inc.incident_id, inc.type, inc.service_name, inc.degradation_start, inc.degradation_end,
                             inc.expected_trigger, inc.expected_action)
                )  # fmt: skip
    rows = []
    for (name, w), (traffic, lat_sum, samples) in sorted(agg.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        version, deployed_at = ctx.get((name, w), (None, None))
        end = (w + 1) * WINDOW_MS / 1000
        rows.append(
            HealthRow(
                service_name=name, window_start=w * WINDOW_MS / 1000, window_end=end, latency_status=None,
                traffic_status=None, avg_latency_ms=lat_sum / samples, traffic_count=int(traffic),
                version=version, seconds_since_deploy=None if deployed_at is None else int(end - deployed_at),
            )
        )  # fmt: skip
    return rows, incidents, start, start + ticks, total / ticks


def apply_detector(rows: list[HealthRow], cfg: DetectorConfig) -> list[HealthRow]:
    by: dict[str, list[HealthRow]] = defaultdict(list)
    for r in rows:
        by[r.service_name].append(r)
    out = []
    for srows in by.values():
        srows.sort(key=lambda r: r.window_end)
        lat = detect([r.avg_latency_ms for r in srows], cfg)
        trf = detect([float(r.traffic_count) for r in srows], cfg)
        for r, lp, tp in zip(srows, lat, trf, strict=True):
            if lp.warming_up or tp.warming_up:
                out.append(dataclasses.replace(r, latency_status="WARMING_UP", traffic_status="WARMING_UP"))
                continue
            ls = "HIGH" if lp.actual > lp.upper else "NORMAL"
            ts = "HIGH" if tp.actual > tp.upper else "LOW" if tp.actual < tp.lower else "NORMAL"
            out.append(
                dataclasses.replace(
                    r, latency_status=ls, traffic_status=ts, expected_latency_ms=lp.forecast,
                    latency_upper_bound=lp.upper, expected_traffic_count=tp.forecast,
                    traffic_lower_bound=tp.lower, traffic_upper_bound=tp.upper,
                )
            )  # fmt: skip
    return out


def run_one(args: tuple[DataConfig, DetectorConfig, int, float]) -> dict:
    data, cfg, seed, hours = args
    rows, incidents, start, end, rate = simulate(data, seed, hours)
    rows = [r for r in rows if r.service_name in ARIMA_SERVICES]
    incidents = [i for i in incidents if i.service_name in ARIMA_SERVICES]
    rep = score(apply_detector(rows, cfg), incidents, start, end - 100)
    out = {"rate": rate, "types": [], "fp": [], "hours": {}, "fresh": {}}
    for r in rep.incidents:
        if r.status == NO_DATA:
            continue
        out["types"].append((r.incident.type, r.status == DETECTED))
    for s in rep.services.values():
        out["hours"][s.service_name] = s.scored_hours
        out["fresh"][s.service_name] = len(s.fresh_start_noise)
        out["fp"] += [(s.service_name, fp.tag) for fp in s.false_positives]
    return out


def evaluate_variant(data: DataConfig, cfg: DetectorConfig, seeds: int, hours: float, label: str, pool) -> Result:
    res = Result(label)
    outs = list(pool.map(run_one, [(data, cfg, seed, hours) for seed in range(seeds)]))
    for o in outs:
        res.traffic_per_s += o["rate"] / len(outs)
        for t, ok in o["types"]:
            res.by_type[t][1] += 1
            res.by_type[t][0] += int(ok)
        for svc, tag in o["fp"]:
            res.fp += 1
            res.fp_echo += tag == "echo"
            res.fp_by_service[svc] += 1
        for svc, h in o["hours"].items():
            res.hours_by_service[svc] += h
        res.fresh_worst = max(res.fresh_worst, max(o["fresh"].values()))
        res.fresh_total += sum(o["fresh"].values())
        res.fresh_starts += len(o["fresh"])
        res.fresh_over_one += sum(v > 1 for v in o["fresh"].values())
    return res


def print_results(results: list[Result]) -> None:
    def frac(pair):
        ok, n = pair
        return f"{ok}/{n}" if n else "-"

    print(
        f"{'variant':58} {'req/s':>5} | {'BAD_DEPLOY':>10} {'DROP':>7} | "
        f"{'FP/svc-h avg':>12} {'worst':>6} {'echo':>4} | fresh-start noise: worst"
    )
    for r in results:
        hours = sum(r.hours_by_service.values())
        worst = max((r.fp_by_service[s] / h for s, h in r.hours_by_service.items() if h), default=0.0)
        print(
            f"{r.label[:58]:58} {r.traffic_per_s:5.1f} | {frac(r.by_type['BAD_DEPLOY']):>10} "
            f"{frac(r.by_type['UPSTREAM_DROP']):>7} | {r.fp / hours if hours else 0:12.2f} {worst:6.2f} "
            f"{r.fp_echo:4d} | {r.fresh_worst} (avg {r.fresh_total / max(1, r.fresh_starts):.2f}, "
            f">1: {r.fresh_over_one}/{r.fresh_starts})"
        )


CURRENT = DetectorConfig(min_training=12, max_training=360, confidence=99.95, p=1, update_interval=1)  # D-059

# Earlier rate profiles, cut to the services that produce metrics: checkout produces tickets
# since D-068.
_D031 = (("web-gateway", 10), ("payments", 6))
_D030 = (("web-gateway", 8), ("payments", 2))
_ROUGH = (("web-gateway", 25), ("payments", 15))
DATA_VARIANTS = {
    "D-030 rates (payments 2/s), aligned start": DataConfig(rates=_D030, aligned=True),
    "D-031 rates (payments 6/s), aligned start": DataConfig(rates=_D031, aligned=True),
    "half the D-059 rates (payments 15/s), aligned start": DataConfig(rates=_ROUGH, aligned=True),
    "current generator (D-059, payments 30/s), live-like start": DataConfig(),
    "current generator (D-059, payments 30/s), aligned start": DataConfig(aligned=True),
}


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--hours", type=float, default=3.0, help="simulated hours per seed")
    p.add_argument("--seeds", type=int, default=4)
    p.add_argument("--detectors", action="store_true", help="also sweep detector settings on the current data")
    a = p.parse_args(argv)
    results = []
    with ProcessPoolExecutor() as pool:
        for label, data in DATA_VARIANTS.items():
            results.append(evaluate_variant(data, CURRENT, a.seeds, a.hours, label, pool))
            print_results(results[-1:])
        if a.detectors:
            for label, cfg in {
                "current data, confidence 99": dataclasses.replace(CURRENT, confidence=99.0),
                "current data, confidence 99.5": dataclasses.replace(CURRENT, confidence=99.5),
                "current data, confidence 99.9 (D-029)": dataclasses.replace(CURRENT, confidence=99.9),
                "current data, maxTraining 180": dataclasses.replace(CURRENT, max_training=180),
                "current data, AR(2)": dataclasses.replace(CURRENT, p=2, update_interval=1),
            }.items():
                results.append(evaluate_variant(DataConfig(), cfg, a.seeds, a.hours, label, pool))
                print_results(results[-1:])
    print()
    print_results(results)
    print(
        f"\n{a.seeds} seeds x {a.hours:g} h each; detection = within 3 windows. Detector: AR(1) replica, "
        f"min {CURRENT.min_training}, max {CURRENT.max_training}, {CURRENT.confidence}% unless stated."
    )


if __name__ == "__main__":
    main()
