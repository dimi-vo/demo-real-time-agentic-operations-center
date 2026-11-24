"""Where do the false alarms come from? Breaks down fresh-start noise and echoes for one setup
of `tune.py`, by service, signal, the last incident before it and the seconds since.

Written for the engine before D-068 (surges on checkout). Kept as the record of its study; it
may not run as is.

Usage (from the repo root): uv run python -m scripts.tuning.research.tune_diag NAME RESTS SEEDS MINUTES
  e.g. uv run python -m scripts.tuning.research.tune_diag c99.95 390/300/300 50 120
"""

import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

from scripts.evaluation.detection import Incident, score
from scripts.tuning.research import tune as T


def one(args):
    c, rests, seed, minutes = args
    incs = T.schedule(seed, c, rests, minutes)
    n = int(minutes * 6)
    rows = []
    data = T.dataclasses.replace(c, conf=99.9, p=1, min_t=12, max_t=360, upd=1)
    for s in T.SERVICES:
        mine = [(int((st - T.START) // T.W), t) for t, sv, st, _ in incs if sv == s]
        rows += T.detect_rows(s, T.windows(s, c, n, mine, T.rng_for("d", data, seed, s)), c.det())
    incidents = [Incident(f"I{i}", t, s, st, en, T.TRIGGER[t], "") for i, (t, s, st, en) in enumerate(incs)]
    rep = score(rows, incidents, T.START, T.START + minutes * 60 - 100)
    out = []
    for sv in rep.services.values():
        for kind, fps in (("fresh", sv.fresh_start_noise), ("run", sv.false_positives)):
            for f in fps:
                ws = f.row.window_start
                prev = [x for x in incidents if x.service_name == sv.service_name and x.ended_at <= ws]
                last = max(prev, key=lambda x: x.ended_at) if prev else None
                since = None if last is None else int(ws - last.ended_at)
                out.append(
                    (kind, sv.service_name, f.signal, f.tag, last.type if last else "-", since, int(ws - T.START))
                )
    return out


if __name__ == "__main__":
    c = T.named(sys.argv[1])
    rests = dict(zip(T.TYPES, map(int, sys.argv[2].split("/")), strict=True))
    seeds, minutes = int(sys.argv[3]), float(sys.argv[4])
    with ProcessPoolExecutor() as pool:
        allfp = [x for o in pool.map(one, [(c, rests, s, minutes) for s in range(seeds)]) for x in o]
    for kind in ("fresh", "run"):
        sel = [x for x in allfp if x[0] == kind]
        print(f"== {kind}: {len(sel)} over {seeds} seeds")
        print("  by service/signal/last-incident:", Counter((x[1], x[2], x[4]) for x in sel).most_common(12))
        print(
            "  by seconds since last incident end:",
            sorted(Counter(x[5] for x in sel).items(), key=lambda kv: (kv[0] is None, kv[0] or 0))[:20],
        )
        if kind == "fresh":
            print("  by seconds since start:", sorted(Counter(x[6] for x in sel).items()))
