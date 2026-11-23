"""What are the 5 s window echoes, and do 10 s incidents (2 windows of 5 s) remove them?
Each false positive is listed by service, the incident before it, its signal and how long
after that incident's end its window closed.

Usage (from the repo root): uv run python -m scripts.tuning.research.window_echoes
"""

from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor

from scripts.tuning.research.window_compare import SEEDS, patch


def one(args):
    window_s, duration, seed = args
    patch(window_s)
    import opsgen.engine as eng
    import scripts.evaluation.detection as det
    import scripts.tuning.sweep as sw

    orig = eng.Config.__init__
    if not getattr(eng.Config, "_patched", False):  # sweep.simulate builds Config() itself

        def init(self, *a, **k):
            orig(self, *a, **k)
            self.max_duration = eng.Config._dur

        eng.Config.__init__ = init
        eng.Config._patched = True
    eng.Config._dur = duration
    rows, incidents, start, end, _ = sw.simulate(sw.DataConfig(aligned=True), seed, 3.0)
    scored = sw.apply_detector(rows, sw.CURRENT)
    rep = det.score(scored, incidents, start, end - 100)
    ends = defaultdict(list)
    for i in incidents:
        ends[i.service_name].append(i)
    out, firsts, hours = Counter(), Counter(), 0.0
    for r in rep.incidents:
        if r.status != det.NO_DATA:
            firsts[(r.incident.type, r.first_window)] += 1
    for s in rep.services.values():
        hours += s.scored_hours
        for fp in s.false_positives:
            prev = max(
                (i for i in ends[s.service_name] if i.ended_at <= fp.row.window_end),
                key=lambda i: i.ended_at,
                default=None,
            )
            after = round(fp.row.window_end - prev.ended_at) if prev else None
            out[(s.service_name, prev.type if prev else None, fp.signal, after)] += 1
    return out, firsts, hours


if __name__ == "__main__":
    with ProcessPoolExecutor() as pool:
        for w, dur in [(5, 20.0), (5, 10.0), (10, 20.0)]:
            res = list(pool.map(one, [(w, dur, s) for s in range(SEEDS)]))
            tot, firsts, hours = Counter(), Counter(), sum(h for _, _, h in res)
            for c, f, _ in res:
                tot.update(c)
                firsts.update(f)
            n = sum(tot.values())
            print(f"\n=== {w} s windows, {dur:g} s incidents: {n} FPs = {n / hours:.2f}/svc-h ===")
            print("  first flagged window:", dict(sorted(firsts.items(), key=str)))
            for k, n in tot.most_common(8):
                print(f"  {n:4}  service={k[0]:12} after {k[1]} ended -> {k[2]:13} window_end = end + {k[3]} s")
