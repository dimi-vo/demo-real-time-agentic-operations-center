"""Live vs replica: does ML_DETECT_ANOMALIES's band widen after an incident, and how fast
does it recover? Uses a recorded service_health dump (offline files only).

Usage (from the repo root):
  uv run evaluate --dump health.jsonl ...      # once, while the pipeline runs
  uv run python -m scripts.tuning.research.live_recovery health.jsonl [MIN_TRAINING] [CONFIDENCE]
MIN_TRAINING and CONFIDENCE must be the dump's live values (default: the labs' 12 and 99.95).
Incidents are found from the data (runs of windows far from the rolling median), so no
incidents dump is needed.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from statistics import median

from scripts.common.topic_reader import epoch_seconds
from scripts.tuning.arima import DetectorConfig, detect

path = sys.argv[1]
min_tr = int(sys.argv[2]) if len(sys.argv) > 2 else 12
conf = float(sys.argv[3]) if len(sys.argv) > 3 else DetectorConfig.confidence
cfg = DetectorConfig(min_training=min_tr, max_training=360, confidence=conf, p=1, update_interval=1)

by = defaultdict(dict)
for line in open(path):
    if line.strip():
        r = json.loads(line)
        by[r["service_name"]][epoch_seconds(r["window_end"])] = r
series = {s: [v[k] for k in sorted(v)] for s, v in by.items()}

METRICS = {
    "latency": ("avg_latency_ms", "expected_latency_ms", "latency_upper_bound"),
    "traffic": ("traffic_count", "expected_traffic_count", "traffic_upper_bound"),
}

BUCKETS = [(1, 1), (2, 2), (3, 5), (6, 12), (13, 30), (31, 60), (61, 120), (121, 240), (241, 400)]


def incidents(vals, kind):
    """Runs of windows far from the rolling median: (start_idx, end_idx_exclusive, direction)."""
    out = []
    flags = []
    for i, v in enumerate(vals):
        ref = median(vals[max(0, i - 60) : i] or [v]) if i >= 10 else None
        if ref is None:
            flags.append(0)
            continue
        if kind == "latency":
            flags.append(1 if v > 2.2 * ref else 0)
        else:
            flags.append(1 if v > 1.8 * ref else -1 if v < 0.45 * ref else 0)
    i = 0
    while i < len(flags):
        if flags[i]:
            j = i
            while j < len(flags) and flags[j] == flags[i]:
                j += 1
            if j - i >= 2:
                out.append((i, j, flags[i]))
            i = j
        else:
            i += 1
    return out


agg = defaultdict(lambda: defaultdict(list))  # (kind, dir) -> bucket -> [(live_ratio, rep_ratio)]
for svc, rows in sorted(series.items()):
    for kind, (col, ecol, ucol) in METRICS.items():
        vals = [float(r[col]) for r in rows]
        rep = detect(vals, cfg)
        live_hw = [None if r[ecol] is None else r[ucol] - r[ecol] for r in rows]
        rep_hw = [None if p.warming_up else p.upper - p.forecast for p in rep]
        incs = incidents(vals, kind)
        for s, e, d in incs:
            pre_l = [h for h in live_hw[max(0, s - 30) : s] if h]
            pre_r = [h for h in rep_hw[max(0, s - 30) : s] if h]
            if len(pre_l) < 10 or len(pre_r) < 10:
                continue
            bl, br = median(pre_l), median(pre_r)
            # stop at the next incident on this series
            nxt = min([s2 for s2, _, _ in incs if s2 > s] + [len(vals)])
            for lo, hi in BUCKETS:
                ls = [live_hw[e - 1 + k] / bl for k in range(lo, hi + 1) if e - 1 + k < nxt and live_hw[e - 1 + k]]
                rs = [rep_hw[e - 1 + k] / br for k in range(lo, hi + 1) if e - 1 + k < nxt and rep_hw[e - 1 + k]]
                if ls and rs:
                    agg[(kind, d)][(lo, hi)].append((median(ls), median(rs)))
            post = min(e, len(vals) - 1)
            print(
                f"{svc:12} {kind:7} dir={d:+d} windows {s}-{e} ({e - s}) "
                f"pre half-width live {bl:.1f} replica {br:.1f}; "
                f"first post window live x{(live_hw[post] or 0) / bl:.2f} replica x{(rep_hw[post] or 0) / br:.2f}"
            )

print("\nhalf-width after the incident's last window, relative to the 30 windows before it (median over incidents)")
for key, b in sorted(agg.items()):
    print(key)
    for (lo, hi), pairs in b.items():
        print(
            f"  windows +{lo:>3}..+{hi:<3} n={len(pairs):2}  live x{median(p[0] for p in pairs):.2f}  "
            f"replica x{median(p[1] for p in pairs):.2f}"
        )
