"""Print live vs replica status, forecast and bounds around the end of each surge/drop/bad deploy
in a recorded service_health dump: the window before, the incident's windows and four after.

Written for the engine before D-068 (surges on checkout). Kept as the record of its study; it
may not run as is.

Usage (from the repo root):
  uv run evaluate --dump health.jsonl ...      # once, while the pipeline runs
  uv run python -m scripts.tuning.research.live_surge_end health.jsonl [MIN_TRAINING] [CONFIDENCE]
MIN_TRAINING and CONFIDENCE must be the dump's live values (default: the labs' 12 and 99.95).
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
for svc, d in sorted(by.items()):
    rows = [d[k] for k in sorted(d)]
    cnt = [float(r["traffic_count"]) for r in rows]
    lat = [float(r["avg_latency_ms"]) for r in rows]
    rc, rl = detect(cnt, cfg), detect(lat, cfg)
    i = 10
    while i < len(rows):
        ref_c = median(cnt[max(0, i - 60) : i])
        ref_l = median(lat[max(0, i - 60) : i])
        kind = (
            "SURGE"
            if cnt[i] > 1.8 * ref_c
            else "DROP"
            if cnt[i] < 0.45 * ref_c
            else "BD"
            if lat[i] > 2.2 * ref_l
            else None
        )
        if kind is None:
            i += 1
            continue
        j = i
        while j < len(rows) and (
            cnt[j] > 1.5 * ref_c
            if kind == "SURGE"
            else cnt[j] < 0.6 * ref_c
            if kind == "DROP"
            else lat[j] > 1.8 * ref_l
        ):
            j += 1
        if j - i >= 2:
            print(f"\n{svc} {kind} windows {i}..{j - 1} ({rows[i]['window_end']})")
            for k in range(i - 1, min(len(rows), j + 4)):
                r = rows[k]
                tag = "IN " if i <= k < j else "   "
                print(
                    f"  {tag}{k:4} cnt {cnt[k]:5.0f} live {r['traffic_status']:10} f={r['expected_traffic_count']} "
                    f"[{r['traffic_lower_bound']},{r['traffic_upper_bound']}] | "
                    f"replica f={rc[k].forecast} [{rc[k].lower},{rc[k].upper}]"
                    f" || lat {lat[k]:6.1f} live {r['latency_status']:10} "
                    f"rep up={rl[k].upper and round(rl[k].upper, 1)}"
                )
        i = max(j, i + 1)
