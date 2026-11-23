"""Drivers that turn a `Simulation` into output: a real-time loop for `--dry-run`
(and, in Phase 2, live Kafka) and a fast synthetic-clock loop for `--simulate`.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from opsgen.engine import DEPLOY_CLOCK_SERVICE, INCIDENT_TYPES, WINDOW_SECONDS, Config, next_window_boundary
from opsgen.groundtruth import end_record, start_record
from opsgen.services import service_names
from opsgen.simulation import Simulation
from opsgen.sinks import format_event
from opsgen.state import save_state, state_path

if TYPE_CHECKING:  # the feed needs Kafka; --dry-run and --simulate never import it
    from opsgen.decisions import DecisionFeed


def _mmss(seconds: int) -> str:
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def aligned_start(now: float) -> float:
    """The next 10-second boundary at or after `now` (D-031). Starting there means the
    first 10-second window Flink aggregates is a full one: a half-empty first window would
    look like a traffic drop and widen the detectors' bounds while it stays in training.
    The F1 race did the same with its 20-second laps."""
    return next_window_boundary(now)


def wait_until(start: float) -> None:
    delay = start - time.time()
    if delay > 0:
        sys.stderr.write(f"Starting on the next 10-second boundary (in {delay:.0f} s)...\n")
        sys.stderr.flush()
        time.sleep(delay)


def tick_once(
    sim: Simulation, sink, now: float, decisions: DecisionFeed | None = None, st_path: Path | None = None
) -> None:
    """One second of live output: tick with the decisions read since the last tick (so a
    resolving one ends its incident on this tick, D-065), emit the records and events,
    and save the state (every 5 s, and on any event) when `st_path` is given."""
    metrics, deps, events = sim.tick(now, decisions.drain() if decisions is not None else ())
    if deps:
        sink.emit_deployments(deps)
    sink.emit_metrics(metrics)
    for ev in events:
        sink.emit_event(ev)
        if ev.incident is not None:
            # Two records per incident: one at start (ended_at null) and one
            # at end (both timestamps), tied by incident_id.
            if ev.kind == "incident_start":
                sink.emit_incident(start_record(ev.incident, ev.now))
            elif ev.kind == "incident_end":
                sink.emit_incident(end_record(ev.incident, ev.now))
    if st_path is not None and (events or round(now - sim.start) % 5 == 0):
        save_state(st_path, sim.state_snapshot())
    sink.flush()


def run_realtime(sim: Simulation, sink, root: Path | None = None, decisions: DecisionFeed | None = None) -> None:
    """Tick once per wall-clock second, emitting through `sink`, until Ctrl-C.

    When `root` is given, the generator's state (versions, incident counter) is saved.
    `--dry-run` passes root=None, so a dry run writes nothing and stays reproducible.
    The first tick is at `sim.start`; callers pass an `aligned_start` and this waits for it.
    Live mode passes the `decisions` feed: each tick gets what it read since the last one,
    and it is stopped on the way out.

    The schedule itself is not saved: on a restart, the incidents in flight when it stopped
    (up to one per service) never get their end record. Accepted (D-060): the wall keeps them
    open for 11 minutes, and `evaluate` scores only incidents that ended.
    """
    wait_until(sim.start)
    sink.emit_deployments(sim.bootstrap())
    sink.flush()
    st_path = state_path(root) if root is not None else None
    start_wall = sim.start
    k = 0
    try:
        while True:
            tick_once(sim, sink, start_wall + k, decisions, st_path)
            k += 1
            delay = (start_wall + k) - time.time()
            if delay > 0:
                time.sleep(delay)
    except KeyboardInterrupt:
        if st_path is not None:
            save_state(st_path, sim.state_snapshot())
        sys.stderr.write("\nStopped.\n")
    finally:
        if decisions is not None:
            decisions.stop()


def schedule_settings(config: Config) -> str:
    """The schedule's settings on one line, for the start banner (D-060)."""
    if config.no_incidents:
        return "no incidents"
    rests = " ".join(f"{t}={config.rests[t]:g}s" for t in INCIDENT_TYPES)
    gaps = config.deploy_clock_gaps
    clock = f"{DEPLOY_CLOCK_SERVICE} deploys: first={config.deploy_clock_first:g}s every={min(gaps):g}-{max(gaps):g}s"
    return f"quiet={config.quiet_period:g}s max={config.max_duration:g}s gap={config.gap:g}s rests: {rests}; {clock}"


def run_simulate(sim: Simulation, minutes: float, out=None) -> None:
    """Run a synthetic clock for `minutes` without sleeping, printing one summary line
    per service per 10-second window plus the deploy/incident log (brief section 4.5).
    A line sums up the window's metrics records the way lab 2.2 does: total traffic,
    average latency, and the share of failed requests (D-058)."""
    out = out if out is not None else sys.stdout
    names = service_names()
    total = round(minutes * 60)

    def fresh() -> dict[str, dict]:
        return {n: {"traffic": 0, "errors": 0, "lat": 0.0, "records": 0} for n in names}

    agg = fresh()
    out.write(f"# ops-sim --simulate {minutes:g}min  ({schedule_settings(sim.config)})\n")
    for k in range(total):
        now = sim.start + k
        metrics, _deps, events = sim.tick(now)
        for ev in events:
            out.write(format_event(ev, clock=_mmss(round(ev.now - sim.start))) + "\n")
        for r in metrics:
            a = agg[r["service_name"]]
            a["traffic"] += r["traffic_count"]
            a["errors"] += r["error_count"]
            a["lat"] += r["latency_ms"]
            a["records"] += 1
        if k % WINDOW_SECONDS == WINDOW_SECONDS - 1:
            label = _mmss(k + 1)
            for name in names:
                a = agg[name]
                err = 100 * a["errors"] / a["traffic"] if a["traffic"] else 0.0
                lat = a["lat"] / a["records"]
                ver = sim.deployments[name].version
                out.write(
                    f"[t={label}] {name:<12} traffic={a['traffic']:>4} err={err:4.1f}% lat={lat:6.1f}ms ver={ver}\n"
                )
            agg = fresh()
    out.flush()
