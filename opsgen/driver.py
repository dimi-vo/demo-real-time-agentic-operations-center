"""Drivers that turn a `Simulation` into output: a real-time loop for `--dry-run`
(and, in Phase 2, live Kafka) and a fast synthetic-clock loop for `--simulate`.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from opsgen.groundtruth import append_incident, incidents_path
from opsgen.instances import version_hash
from opsgen.services import service_names
from opsgen.simulation import Simulation
from opsgen.sinks import format_event
from opsgen.state import save_state, state_path


def _mmss(seconds: int) -> str:
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def run_realtime(sim: Simulation, sink, root: Path | None = None, write_groundtruth: bool = False) -> None:
    """Tick once per wall-clock second, emitting through `sink`, until Ctrl-C.

    When `root` is given, the generator's deployment state is saved and (if
    `write_groundtruth`) each finished incident is appended to incidents.jsonl.
    `--dry-run` passes root=None, so a dry run writes nothing and stays reproducible.
    """
    sink.emit_deployments(sim.bootstrap())
    sink.flush()
    gt_path = incidents_path(root) if root is not None and write_groundtruth else None
    st_path = state_path(root) if root is not None else None
    start_wall = sim.start
    k = 0
    try:
        while True:
            now = start_wall + k
            requests, deps, events = sim.tick(now)
            if deps:
                sink.emit_deployments(deps)
            if requests:
                sink.emit_requests(requests)
            for ev in events:
                sink.emit_event(ev)
                if ev.kind == "incident_end" and gt_path is not None and ev.incident is not None:
                    append_incident(gt_path, ev.incident)
            if st_path is not None and (events or k % 5 == 0):
                save_state(st_path, sim.state_snapshot())
            sink.flush()
            k += 1
            delay = (start_wall + k) - time.time()
            if delay > 0:
                time.sleep(delay)
    except KeyboardInterrupt:
        if st_path is not None:
            save_state(st_path, sim.state_snapshot())
        sys.stderr.write("\nStopped.\n")


def run_simulate(sim: Simulation, minutes: float, out=None) -> None:
    """Run a synthetic clock for `minutes` without sleeping, printing one summary line
    per service per 10-second window plus the deploy/incident log (brief section 4.5)."""
    out = out if out is not None else sys.stdout
    names = service_names()
    total = round(minutes * 60)

    def fresh() -> dict[str, dict]:
        return {n: {"count": 0, "errors": 0, "lat": 0.0} for n in names}

    agg = fresh()
    out.write(
        f"# ops-sim --simulate {minutes:g}min  "
        f"(quiet={sim.config.quiet_period:g}s spacing={sim.config.spacing_min:g}-{sim.config.spacing_max:g}s "
        f"duration={sim.config.duration_min:g}-{sim.config.duration_max:g}s cooldown={sim.config.cooldown:g}s)\n"
    )
    for k in range(total):
        now = sim.start + k
        requests, _deps, events = sim.tick(now)
        for ev in events:
            out.write(format_event(ev, clock=_mmss(round(ev.now - sim.start))) + "\n")
        for r in requests:
            a = agg[r["service_name"]]
            a["count"] += 1
            a["lat"] += r["latency_ms"]
            if r["status_code"] >= 500:
                a["errors"] += 1
        if k % 10 == 9:
            label = _mmss(k + 1)
            for name in names:
                a = agg[name]
                if a["count"] == 0:
                    out.write(f"[t={label}] {name:<12} reqs=   0  (NO TRAFFIC)\n")
                    continue
                err = 100 * a["errors"] / a["count"]
                lat = a["lat"] / a["count"]
                ver = sim.deployments[name].version
                out.write(
                    f"[t={label}] {name:<12} reqs={a['count']:>4} err={err:4.1f}% "
                    f"lat={lat:6.1f}ms ver={ver:<7} inst={version_hash(name, ver)}\n"
                )
            agg = fresh()
    out.flush()
