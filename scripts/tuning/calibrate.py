"""Compare the replica with what Confluent Cloud computed, on the same input series.

Usage: uv run python -m scripts.tuning.calibrate --rows DUMP.jsonl [--min-training 12] ...

For every service and metric it feeds the recorded actual values (avg_latency_ms,
request_count) through the replica and compares the forecasts, bound half-widths and
statuses with the ones Confluent stored in service_health.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from statistics import median

from scripts.common.topic_reader import epoch_seconds
from scripts.tuning.arima import DetectorConfig, detect

METRICS = (
    ("latency", "avg_latency_ms", "expected_latency_ms", None, "latency_upper_bound", "latency_status"),
    (
        "traffic",
        "request_count",
        "expected_request_count",
        "traffic_lower_bound",
        "traffic_upper_bound",
        "traffic_status",
    ),
)


def series_by_service(path: str) -> dict[str, list[dict]]:
    by: dict[str, dict[float, dict]] = defaultdict(dict)
    for line in open(path):
        if line.strip():
            r = json.loads(line)
            by[r["service_name"]][epoch_seconds(r["window_end"])] = r
    return {s: [v[k] for k in sorted(v)] for s, v in by.items()}


def status_of(kind: str, actual: float, lower: float | None, upper: float | None) -> str:
    if upper is None:
        return "WARMING_UP"
    if actual > upper:
        return "HIGH"
    if kind == "traffic" and actual < lower:
        return "LOW"
    return "NORMAL"


def compare(path: str, cfg: DetectorConfig) -> dict:
    stats = defaultdict(list)
    agree = defaultdict(lambda: [0, 0])
    disagreements = []
    for svc, rows in sorted(series_by_service(path).items()):
        for kind, col, exp_col, _lo_col, up_col, st_col in METRICS:
            actual = [float(r[col]) for r in rows]
            for r, pt in zip(rows, detect(actual, cfg), strict=True):
                cc_warm = r[exp_col] is None
                if cc_warm or pt.warming_up:
                    agree[kind][1] += 1
                    agree[kind][0] += int(cc_warm == pt.warming_up)
                    continue
                cc_half = r[up_col] - r[exp_col]
                my_half = pt.upper - pt.forecast
                scale = max(abs(r[exp_col]), 1e-9)
                stats[(kind, "forecast_rel_err")].append(abs(pt.forecast - r[exp_col]) / scale)
                if cc_half > 0:
                    stats[(kind, "half_width_ratio")].append(my_half / cc_half)
                cc_status = r[st_col].strip()
                my_status = status_of(kind, pt.actual, pt.lower, pt.upper)
                agree[kind][1] += 1
                agree[kind][0] += int(cc_status == my_status)
                if cc_status != my_status:
                    disagreements.append((svc, kind, r["window_end"], cc_status, my_status))
    return {"stats": stats, "agree": agree, "disagreements": disagreements}


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--rows", required=True)
    p.add_argument("--min-training", type=int, default=12)
    p.add_argument("--max-training", type=int, default=360)
    p.add_argument("--confidence", type=float, default=99.9)
    p.add_argument("--update-interval", type=int, default=5)
    p.add_argument("--warmup-extra", type=int, default=1)
    p.add_argument("--ddof", type=int, default=0)
    p.add_argument("--show", type=int, default=10, help="status disagreements to list")
    a = p.parse_args(argv)
    cfg = DetectorConfig(a.min_training, a.max_training, a.confidence, 1, a.update_interval, a.warmup_extra, a.ddof)
    res = compare(a.rows, cfg)
    for kind in ("latency", "traffic"):
        fe = res["stats"][(kind, "forecast_rel_err")]
        hw = res["stats"][(kind, "half_width_ratio")]
        ok, n = res["agree"][kind]
        print(
            f"{kind:8} forecast error: median {median(fe):.2%}, p90 {sorted(fe)[int(len(fe) * 0.9)]:.2%} | "
            f"half-width replica/Confluent: median {median(hw):.3f}, p10 {sorted(hw)[len(hw) // 10]:.3f}, "
            f"p90 {sorted(hw)[int(len(hw) * 0.9)]:.3f} | status agreement {ok}/{n} ({ok / n:.2%})"
        )
    for d in res["disagreements"][: a.show]:
        print("   disagree:", *d)


if __name__ == "__main__":
    main()
