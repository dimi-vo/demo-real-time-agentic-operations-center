"""Re-check ARIMA when web-gateway leaves the shared scheduler and checkout gets its surges (D-067).

Written for the engine before D-068 (surges on checkout). Kept as the record of its study; it
may not run as is.

Usage (from the repo root):
    uv run python -m scripts.tuning.research.shared_scheduler [SEEDS] [HOURS]    # default 500 x 3 h

Before D-067 the shared scheduler gave web-gateway BAD_DEPLOY and TRAFFIC_SURGE, and checkout
and payments BAD_DEPLOY and UPSTREAM_DROP. D-067 takes web-gateway out (it gets its own deploy
clock and an error-rate threshold, not ARIMA), so the surges need a new home. First proposed:
checkout and payments both get all three types ("after"). That loses the drops: a traffic
series with x5 surges gets a band so wide (it is symmetric) that its lower bound goes below
zero, and a x0.1 drop never falls under it. Adopted: surges on checkout only, drops on payments
only ("split"), so each traffic series spikes in one direction, as before.

How it runs: the real generator through `close_loop.simulate` (`sweep.simulate`, aligned start,
D-059 data, rests 390/300/300, the 35 s cap), the AR(1) replica at `sweep.CURRENT`, scored with
evaluate's code (`close_loop.score_run`). Each scenario sets `opsgen.engine.CANDIDATES` in the
worker before its run:
- before: the candidates before D-067, scored on all three services (D-065's row C, as a harness
  check) and on checkout and payments only;
- after (rejected): all three types on checkout and payments, scored on both (ARIMA doesn't
  run on web-gateway);
- split (D-067): surges on checkout only, drops on payments only, bad deploys on both, scored
  on both.
Each with no fix and with every incident fixed at T+11: D-066 measured answers about 1 s after the
detection, which lands when the first window closes at T+10 (D-064). Same seeds everywhere.

Columns: as `close_loop.py`. inc/h counts the scored services' incidents only.
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import opsgen.engine as eng
import scripts.tuning.research.close_loop as cl

ALL = ("web-gateway", "checkout", "payments")
TWO = ("checkout", "payments")
BEFORE = {eng.BAD_DEPLOY: ALL, eng.TRAFFIC_SURGE: ("web-gateway",), eng.UPSTREAM_DROP: TWO}
AFTER = {t: TWO for t in eng.INCIDENT_TYPES}
# What D-067 adopted once AFTER lost the drops: surges and drops on different services.
SPLIT = {eng.BAD_DEPLOY: TWO, eng.TRAFFIC_SURGE: ("checkout",), eng.UPSTREAM_DROP: ("payments",)}
CANDIDATE_SETS = {"before": BEFORE, "after": AFTER, "split": SPLIT}

# web-gateway's deploy clock came with D-067 (issue 03). The "before" candidates still give
# web-gateway shared incidents, which the clock would deploy into, so it is off for them. For
# the others it only moves web-gateway's errors and versions, which ARIMA doesn't read, and
# sweep.simulate leaves its BAD_DEPLOY_ERRORS out of the scored incidents.
_CLOCK_DUE = eng.DeployClock.due


def _no_deploy(self, now):
    return None


@dataclass(frozen=True)
class Run:
    label: str
    candidates: str  # a key of CANDIDATE_SETS
    scored: tuple[str, ...]
    fix_at: float | None = None


RUNS = {
    "B3": Run("before, all three scored, no fix (D-065 row C)", "before", ALL),
    "B2": Run("before, checkout + payments scored, no fix", "before", TWO),
    "A2": Run("after (rejected), checkout + payments, no fix", "after", TWO),
    "B2f": Run("before, checkout + payments scored, fixed at T+11", "before", TWO, 11),
    "A2f": Run("after (rejected), checkout + payments, fixed at T+11", "after", TWO, 11),
    "S2": Run("split (D-067): surges on checkout, drops on payments, no fix", "split", TWO),
    "S2f": Run("split (D-067): surges on checkout, drops on payments, fixed at T+11", "split", TWO, 11),
}


def one(args: tuple[str, int, float]) -> dict:
    key, seed, hours = args
    run = RUNS[key]
    eng.CANDIDATES = dict(CANDIDATE_SETS[run.candidates])
    eng.DeployClock.due = _no_deploy if run.candidates == "before" else _CLOCK_DUE
    rows, incidents, start, end, _ = cl.simulate(cl.Scenario(run.label, 35, run.fix_at), seed, hours)
    rows = [r for r in rows if r.service_name in run.scored]
    incidents = [i for i in incidents if i.service_name in run.scored]
    return cl.score_run(rows, incidents, start, end, cl.sw.CURRENT.confidence)


def main(argv: list[str]) -> None:
    seeds = int(argv[0]) if argv else 500
    hours = float(argv[1]) if len(argv) > 1 else 3.0
    t0 = time.time()
    results = {}
    with ProcessPoolExecutor() as pool:
        for key, run in RUNS.items():
            t = time.time()
            outs = list(pool.map(one, [(key, seed, hours) for seed in range(seeds)], chunksize=8))
            s = results[key] = cl.summarize(outs, hours)
            rb = "  ".join(f"{t[:7]} {ok}/{n}" for t, (ok, n) in s["recall_by_type"].items())
            print(
                f"\n{key}: {run.label} [{time.time() - t:.0f} s]\n"
                f"  {s['n']} scored incidents, {s['svc_h']:.0f} scored service-hours; lengths {s['lengths']}\n"
                f"  recall by type: {rb}; late {s['late']}, missed {s['missed']}\n"
                f"  first flagged window: {dict(sorted(s['first_window'].items(), key=str))}\n"
                f"  opposite flags in the span's return-to-normal window (not counted as FPs by evaluate): "
                f"{s['absorbed']}"
            )
    print(
        "\n| | run | inc/h | mix BD/SURGE/DROP % | recall % | w1 % | echo /svc-h (n, opp) | "
        "other FP /svc-h (n) | fresh /svc | calls/inc |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for key, s in results.items():
        mix = "/".join(f"{s['mix'][t]:.0f}" for t in eng.INCIDENT_TYPES)
        print(
            f"| {key} | {RUNS[key].label} | {s['inc_h']:.1f} | {mix} | {s['recall']:.2f} | {s['w1']:.2f} | "
            f"{s['echo_h']:.3f} ({s['echo_n']}, opp {s['echo_opp']:.0f}%) | {s['other_h']:.3f} ({s['other_n']}) | "
            f"{s['fresh']:.2f} | {s['calls']:.2f} |"
        )
    print(
        f"\n{seeds} seeds x {hours:g} h per run (same seeds), aligned start, D-059 data, rests 390/300/300, "
        f"35 s cap, AR(1) replica {cl.sw.CURRENT.min_training}/{cl.sw.CURRENT.max_training}/"
        f"{cl.sw.CURRENT.confidence}. Total {time.time() - t0:.0f} s."
    )


if __name__ == "__main__":
    main(sys.argv[1:])
