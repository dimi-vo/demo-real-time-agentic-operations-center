"""Close the loop: what a longer incident cap and an early fix do to the reference detector.

Usage (from the repo root):
    uv run python -m scripts.tuning.research.close_loop [SEEDS] [HOURS]    # default 1000 x 3 h

The change considered: an incident lasts at most a cap (35 s, so it ends mid-window, at T+35
where T is its start boundary), and when an agent answers correctly the generator ends it early:
metrics return to baseline from the fix tick on, and the ground truth's `ended_at` is the fix
tick. The gap and the per-type rests stay measured from the SCHEDULED end (start + cap), so the
schedule is the same whatever the agents do.

How it runs: the real generator through `sweep.simulate` (aligned start, D-059 data and rests),
the AR(1) replica at `sweep.CURRENT` (or a stated confidence), scored with
`scripts.evaluation.detection.score` (evaluate's code). The fix is modelled by `FixEngine`, an
`opsgen.engine.Engine` subclass patched into `opsgen.simulation` per worker (opsgen itself is
untouched):
- at an incident's start, if the scenario fixes it, `degradation_end` becomes start + fix, so
  the effect stops on the fix tick, a BAD_DEPLOY's ROLLBACK fires there, and the end event (and
  so the scored incident's `ended_at`) carries the fix tick;
- when it ends, the service's gap and the type's rest are dated from start + cap, not from now.
Scenario G draws which incidents are fixed from its own rng, seeded from the run's seed, so the
schedule rng is untouched. Every scenario uses seeds 0..SEEDS-1; the schedule depends only on the
cap and the rests, so C, E, F and G share one schedule per seed (checked at the start).

Columns: inc/h is ended incidents per run-hour (the 150 s quiet start included); recall is
detected within 3 windows (evaluate's rule); w1 is first flagged in window 1; echo and other are
evaluate's false positives per service-hour, split by its `echo` tag (a flag in the minute after
an incident ended), with opp the share of echoes that are the opposite traffic flag; fresh is
fresh-start false alarms per service; calls/inc is flagged span windows per incident (one agent
call each), then the share of incidents with window 1, 2, 3, 4 of their span flagged.
A' and B' rerun D-059's 20 s and 30 s rows (confidence 99.9, their rests) as a harness check.
H is a diagnostic: C's schedule with a fix on a window boundary, against E and F's mid-window fix.
"""

from __future__ import annotations

import dataclasses
import math
import random
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import opsgen.engine as eng
import opsgen.simulation as sim_mod
import scripts.evaluation.detection as det
import scripts.tuning.sweep as sw

_ORIG_CONFIG = eng.Config
_ORIG_ENGINE = sim_mod.Engine


@dataclass(frozen=True)
class Scenario:
    label: str
    cap: float  # incident length without a fix; gap and rests are dated from start + cap
    fix_at: float | None = None  # seconds after the start; None = never fixed
    fix_share: float = 1.0  # share of incidents fixed (drawn from the fix rng)
    confidence: float = sw.CURRENT.confidence
    rests: tuple[float, float, float] | None = None  # BAD_DEPLOY/TRAFFIC_SURGE/UPSTREAM_DROP; None = RESTS


SCENARIOS = {
    "A": Scenario("20 s, no fix (today)", 20),
    "B": Scenario("30 s, no fix", 30),
    "C": Scenario("35 s, no fix", 35),
    "D": Scenario("40 s, no fix", 40),
    "E": Scenario("35 s cap, all fixed at T+15", 35, 15),
    "F": Scenario("35 s cap, all fixed at T+18", 35, 18),
    "G": Scenario("35 s cap, 80% fixed at T+18", 35, 18, 0.8),
    # Diagnostic: a fix on a window boundary, C's schedule; isolates the part-spike last window.
    "H": Scenario("diagnostic: 35 s cap, all fixed at T+20", 35, 20),
    # D-059's own setups for its 20 s and 30 s rows (confidence 99.9, their ~60/h rests).
    "A'": Scenario("D-059 check: 20 s, 99.9, rests 420/240/300", 20, confidence=99.9, rests=(420, 240, 300)),
    "B'": Scenario("D-059 check: 30 s, 99.9, rests 330/300/300", 30, confidence=99.9, rests=(330, 300, 300)),
}


class FixEngine(eng.Engine):
    """`Engine` with an early fix and scheduling from start + cap (see the module docstring).
    The run's parameters are class attributes, set by `patch` before each run (one run at a
    time per worker process)."""

    fix_at: float | None = None
    fix_share: float = 1.0
    fix_seed: int = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        self.fix_rng = random.Random(f"close-loop-fix-{self.fix_seed}")

    def _cap(self) -> float:
        return float(math.ceil(self.config.max_duration))

    def _start_incident(self, itype, svc, now, deployments, events) -> None:
        super()._start_incident(itype, svc, now, deployments, events)
        if self.fix_at is not None and self.fix_rng.random() < self.fix_share:
            self.active[svc].degradation_end = now + self.fix_at

    def _end_incidents(self, now, deployments, events) -> None:
        ending = [inc for inc in self.active.values() if now >= inc.degradation_end]
        super()._end_incidents(now, deployments, events)
        # The schedule ignores the fix: gap and rest from start + cap. Since D-065 the stock
        # engine does this too (`Incident.scheduled_end`); kept so the run is as recorded.
        for inc in ending:
            scheduled_end = inc.degradation_start + self._cap()
            self.last_incident_end[inc.service_name] = scheduled_end
            self.last_type_end[(inc.service_name, inc.type)] = scheduled_end


def patch(sc: Scenario | None, seed: int) -> None:
    """Point sweep.simulate at this scenario's Config and engine (None = the stock ones)."""
    if sc is None:
        eng.Config, sim_mod.Engine = _ORIG_CONFIG, _ORIG_ENGINE
        return
    rests = dict(eng.RESTS) if sc.rests is None else dict(zip(eng.INCIDENT_TYPES, sc.rests, strict=True))
    eng.Config = lambda: _ORIG_CONFIG(max_duration=sc.cap, rests=dict(rests))
    FixEngine.fix_at, FixEngine.fix_share, FixEngine.fix_seed = sc.fix_at, sc.fix_share, seed
    sim_mod.Engine = FixEngine


def simulate(sc: Scenario | None, seed: int, hours: float):
    patch(sc, seed)
    return sw.simulate(sw.DataConfig(aligned=True), seed, hours)


def one(args: tuple[str, int, float]) -> dict:
    key, seed, hours = args
    sc = SCENARIOS[key]
    rows, incidents, start, end, _ = simulate(sc, seed, hours)
    return score_run(rows, incidents, start, end, sc.confidence)


def score_run(rows, incidents, start: float, end: float, confidence: float) -> dict:
    """One run's window rows through the replica at `confidence`, scored with evaluate's code;
    the per-run counts `summarize` adds up. Also used by `shared_scheduler.py` (D-067)."""
    scored = sw.apply_detector(rows, dataclasses.replace(sw.CURRENT, confidence=confidence))
    rep = det.score(scored, incidents, start, end - 100)
    by_svc: dict[str, list[det.HealthRow]] = defaultdict(list)
    for r in scored:
        by_svc[r.service_name].append(r)
    incs_by_svc: dict[str, list[det.Incident]] = defaultdict(list)
    for i in incidents:
        incs_by_svc[i.service_name].append(i)

    out = {
        "n_ended": len(incidents), "inc": [], "fp": [], "hours": 0.0, "fresh": [], "lengths": Counter(), "absorbed": 0,
    }  # fmt: skip
    for i in incidents:
        out["lengths"][round(i.ended_at - i.started_at)] += 1
    for r in rep.incidents:
        if r.status == det.NO_DATA:
            continue
        inc = r.incident
        flagged = sorted(inc.window_number(row) for row in by_svc[inc.service_name] if inc.in_span(row) and row.signals)
        out["inc"].append((inc.type, r.status, r.first_window, tuple(flagged)))
        # An opposite flag in the span window holding the return to normal: evaluate counts it
        # neither as a false positive nor as a wrong direction (a mid-window end makes that
        # window part spike, part baseline), so count it here.
        opp = det.OPPOSITE.get(inc.expected_trigger)
        out["absorbed"] += sum(1 for _, sig, at_end in r.other_signals if sig == opp and at_end)
    for s in rep.services.values():
        out["hours"] += s.scored_hours
        out["fresh"].append(len(s.fresh_start_noise))
        for fp in s.false_positives:
            opposite = False
            if fp.tag == "echo":
                prev = max(
                    (i for i in incs_by_svc[s.service_name]
                     if i.effective_end <= fp.row.window_start < i.effective_end + det.ECHO_SECONDS),
                    key=lambda i: i.ended_at,
                )  # fmt: skip
                opposite = det.OPPOSITE.get(prev.expected_trigger) == fp.signal
            out["fp"].append((fp.tag, opposite))
    return out


def summarize(outs: list[dict], hours: float) -> dict:
    incs = [x for o in outs for x in o["inc"]]
    n = len(incs)
    types = Counter(t for t, *_ in incs)
    det_by_type = Counter(t for t, st, *_ in incs if st == det.DETECTED)
    fps = [x for o in outs for x in o["fp"]]
    svc_h = sum(o["hours"] for o in outs)
    echoes = [opp for tag, opp in fps if tag == "echo"]
    fresh = [x for o in outs for x in o["fresh"]]
    flagged_k = Counter(k for *_, ks in incs for k in set(ks))
    lengths = Counter()
    for o in outs:
        lengths.update(o["lengths"])
    return {
        "inc_h": sum(o["n_ended"] for o in outs) / (len(outs) * hours),
        "mix": {t: 100 * types[t] / n for t in eng.INCIDENT_TYPES},
        "n": n,
        "recall": 100 * sum(det_by_type.values()) / n,
        "recall_by_type": {t: (det_by_type[t], types[t]) for t in eng.INCIDENT_TYPES},
        "late": sum(st == det.LATE for _, st, *_ in incs),
        "missed": sum(st == det.MISSED for _, st, *_ in incs),
        "w1": 100 * sum(fw == 1 for _, _, fw, _ in incs) / n,
        "first_window": Counter(fw for _, _, fw, _ in incs),
        "echo_h": len(echoes) / svc_h,
        "echo_n": len(echoes),
        "echo_opp": 100 * sum(echoes) / max(1, len(echoes)),
        "other_h": (len(fps) - len(echoes)) / svc_h,
        "other_n": len(fps) - len(echoes),
        "fresh": sum(fresh) / len(fresh),
        "calls": sum(len(ks) for *_, ks in incs) / n,
        "flag_k": {k: 100 * flagged_k[k] / n for k in (1, 2, 3, 4)},
        "svc_h": svc_h,
        "lengths": dict(sorted(lengths.items())),
        "absorbed": sum(o["absorbed"] for o in outs),
    }


def selfcheck(hours: float) -> list[str]:
    """FixEngine without a fix reproduces the stock engine (35 s since D-065, so scenario C);
    the 35 s scenarios share a schedule."""
    msgs = []
    for seed in (0, 1):
        stock = simulate(None, seed, hours)
        c = simulate(SCENARIOS["C"], seed, hours)
        same = stock[0] == c[0] and stock[1] == c[1]
        msgs.append(f"seed {seed}: scenario C == stock engine (rows and incidents): {same}")
        sched = {}
        for key in "CEFG":
            _, incs, _, end, _ = simulate(SCENARIOS[key], seed, hours)
            # sweep lists incidents as they end, so sort; one still running at the end isn't listed
            sched[key] = sorted((i.started_at, i.type, i.service_name) for i in incs if i.started_at < end - 40)
            lens = Counter(round(i.ended_at - i.started_at) for i in incs)
            msgs.append(f"seed {seed}: {key} incident lengths {dict(sorted(lens.items()))}")
        msgs.append(f"seed {seed}: C/E/F/G identical schedules: {len({tuple(v) for v in sched.values()}) == 1}")
    patch(None, 0)
    return msgs


def fmt_row(key: str, s: dict) -> str:
    mix = "/".join(f"{s['mix'][t]:.0f}" for t in eng.INCIDENT_TYPES)
    fk = "/".join(f"{s['flag_k'][k]:.0f}" for k in (1, 2, 3, 4))
    return (
        f"| {key} | {SCENARIOS[key].label} | {s['inc_h']:.1f} | {mix} | {s['recall']:.2f} | {s['w1']:.2f} | "
        f"{s['echo_h']:.3f} ({s['echo_n']}, opp {s['echo_opp']:.0f}%) | {s['other_h']:.3f} ({s['other_n']}) | "
        f"{s['fresh']:.2f} | {s['calls']:.2f} ({fk}) |"
    )


def main(argv: list[str]) -> None:
    seeds = int(argv[0]) if argv else 1000
    hours = float(argv[1]) if len(argv) > 1 else 3.0
    t0 = time.time()
    for m in selfcheck(hours):
        print(m)
    results = {}
    with ProcessPoolExecutor() as pool:
        for key in SCENARIOS:
            t = time.time()
            outs = list(pool.map(one, [(key, seed, hours) for seed in range(seeds)], chunksize=8))
            s = results[key] = summarize(outs, hours)
            rb = "  ".join(f"{t[:7]} {ok}/{n}" for t, (ok, n) in s["recall_by_type"].items())
            print(
                f"\n{key}: {SCENARIOS[key].label} [{time.time() - t:.0f} s]\n"
                f"  {s['n']} scored incidents, {s['svc_h']:.0f} scored service-hours; lengths {s['lengths']}\n"
                f"  recall by type: {rb}; late {s['late']}, missed {s['missed']}\n"
                f"  first flagged window: {dict(sorted(s['first_window'].items(), key=str))}\n"
                f"  span window k flagged (% of incidents): { {k: round(v, 3) for k, v in s['flag_k'].items()} }\n"
                f"  opposite flags in the span's return-to-normal window (not counted as FPs by evaluate): "
                f"{s['absorbed']}"
            )
    print(
        "\n| | scenario | inc/h | mix BD/SURGE/DROP % | recall % | w1 % | echo /svc-h (n, opp) | "
        "other FP /svc-h (n) | fresh /svc | calls/inc (w1/w2/w3/w4 flagged %) |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for key, s in results.items():
        print(fmt_row(key, s))
    print(
        f"\n{seeds} seeds x {hours:g} h per scenario (same seeds), aligned start, current generator (D-059 data, "
        f"rests 390/300/300 unless stated), AR(1) replica {sw.CURRENT.min_training}/{sw.CURRENT.max_training}/"
        f"{sw.CURRENT.confidence} refit every row unless stated. Total {time.time() - t0:.0f} s."
    )


if __name__ == "__main__":
    main(sys.argv[1:])
