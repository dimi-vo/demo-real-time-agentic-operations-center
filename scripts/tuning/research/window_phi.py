"""The AR(1) coefficient the replica fits, per window size (last 360 windows, as the detector
sees them). A coefficient near 1 is what makes the first normal window after a spike look
like an anomaly the other way (an echo; D-064).

Usage (from the repo root): uv run python -m scripts.tuning.research.window_phi
"""

import statistics as st
from collections import defaultdict

from scripts.tuning.research.window_compare import patch


def main() -> None:
    for w in (10, 5):
        patch(w)
        import scripts.tuning.sweep as sw

        phis = defaultdict(list)
        for seed in range(6):
            rows, *_ = sw.simulate(sw.DataConfig(aligned=True), seed, 3.0)
            by = defaultdict(list)
            for r in sorted(rows, key=lambda r: r.window_end):
                by[r.service_name].append(r)
            for svc, rs in by.items():
                for metric, ys in (
                    ("traffic", [r.traffic_count for r in rs]),
                    ("latency", [r.avg_latency_ms for r in rs]),
                ):
                    for end in range(400, len(ys), 50):
                        x, y = ys[end - 361 : end - 1], ys[end - 360 : end]
                        mx, my = st.fmean(x), st.fmean(y)
                        phis[(svc, metric)].append(
                            sum((a - mx) * (b - my) for a, b in zip(x, y, strict=True)) / sum((a - mx) ** 2 for a in x)
                        )
        print(f"{w:2} s windows: " + "  ".join(f"{s}/{m} {st.median(v):.2f}" for (s, m), v in sorted(phis.items())))


if __name__ == "__main__":
    main()
