"""Drivers that turn a `Simulation` into output: a real-time loop for `--dry-run`
(and, in Phase 2, live Kafka) and a fast synthetic-clock loop for `--simulate`.
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

from opsgen import findings, tickets
from opsgen.engine import DEPLOY_CLOCK_SERVICE, INCIDENT_TYPES, WINDOW_SECONDS, Config, next_window_boundary
from opsgen.groundtruth import end_record, start_record
from opsgen.services import TICKET_SERVICES, metric_service_names
from opsgen.simulation import REEMIT_INTERVAL, Simulation
from opsgen.sinks import format_event, format_finding, format_record, format_ticket
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
    sim: Simulation,
    sink,
    now: float,
    decisions: DecisionFeed | None = None,
    st_path: Path | None = None,
    show_records: str | None = None,
) -> None:
    """One second of live output: tick with the decisions read since the last tick (so a
    resolving one ends its incident on this tick, D-065), emit the records, events, tickets
    and findings, and save the state (every 5 s, and on any event, ticket or finding) when
    `st_path` is given.

    `show_records` names a service (`--show-records`). For a metric service, on the first
    tick and then every REEMIT_INTERVAL (5 s, with the deployment heartbeat), its
    service_metrics record goes to stderr as sent. For checkout, every ticket and its
    answer-key record go to stderr as they are produced (D-068)."""
    metrics, deps, events, new_tickets, new_findings = sim.tick(now, decisions.drain() if decisions is not None else ())
    if deps:
        sink.emit_deployments(deps)
    sink.emit_metrics(metrics)
    if show_records is not None and round(now - sim.start) % REEMIT_INTERVAL == 0:
        for r in metrics:
            if r["service_name"] == show_records:
                sys.stderr.write(format_record("service_metrics", r, now) + "\n")
                sys.stderr.flush()
    for ev in events:
        sink.emit_event(ev)
        if ev.incident is not None:
            # Two records per incident: one at start (ended_at null) and one
            # at end (both timestamps), tied by incident_id.
            if ev.kind == "incident_start":
                sink.emit_incident(start_record(ev.incident, ev.now))
            elif ev.kind == "incident_end":
                sink.emit_incident(end_record(ev.incident, ev.now))
    if new_tickets:
        sink.emit_tickets(new_tickets)
        if show_records in TICKET_SERVICES:
            for t in new_tickets:
                sys.stderr.write(format_record("support_tickets", t.ticket, now) + "\n")
                key = t.answer_key["ticket_id"]
                sys.stderr.write(format_record("ticket_answer_key", t.answer_key, now, key=key) + "\n")
            sys.stderr.flush()
    if new_findings:
        sink.emit_findings(new_findings)
    # A tick with a ticket or a finding saves too, so a restart never reuses its id (D-068,
    # D-070).
    if st_path is not None and (events or new_tickets or new_findings or round(now - sim.start) % 5 == 0):
        save_state(st_path, sim.state_snapshot())
    sink.flush()


def run_realtime(
    sim: Simulation,
    sink,
    root: Path | None = None,
    decisions: DecisionFeed | None = None,
    show_records: str | None = None,
) -> None:
    """Tick once per wall-clock second, emitting through `sink`, until Ctrl-C.

    When `root` is given, the generator's state (versions, incident counter) is saved.
    `--dry-run` passes root=None, so a dry run writes nothing and stays reproducible.
    The first tick is at `sim.start`; callers pass an `aligned_start` and this waits for it.
    Live mode passes the `decisions` feed: each tick gets what it read since the last one,
    and it is stopped on the way out. `show_records` is passed to `tick_once`.

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
            tick_once(sim, sink, start_wall + k, decisions, st_path, show_records)
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
    """The schedule's settings on one line, for the start banner (D-060): payments'
    incidents, web-gateway's deploy clock, checkout's ticket clock (D-068) and the scan clock
    (D-070)."""
    if config.no_incidents:
        return "no incidents, no tickets, no findings"
    rests = " ".join(f"{t}={config.rests[t]:g}s" for t in INCIDENT_TYPES)
    gaps = config.deploy_clock_gaps
    clock = f"{DEPLOY_CLOCK_SERVICE} deploys: first={config.deploy_clock_first:g}s every={min(gaps):g}-{max(gaps):g}s"
    prios = " ".join(f"{p}={w:.0%}" for p, w in zip(tickets.PRIORITIES, tickets.PRIORITY_WEIGHTS, strict=True))
    ticket_clock = f"{tickets.SERVICE} tickets: 1/{1 / tickets.RATE:g}s {prios} P1-HIGH={tickets.P1_HIGH_SHARE:.0%}"
    scan_clock = (
        f"findings: 1/{1 / findings.RATE:g}s critical={findings.CRITICAL_SHARE:.0%} PATCH={findings.PATCH_SHARE:.0%}"
    )
    return (
        f"quiet={config.quiet_period:g}s max={config.max_duration:g}s gap={config.gap:g}s rests: {rests}; "
        f"{clock}; {ticket_clock}; {scan_clock}"
    )


def _ticket_summary(drawn: list[tickets.NewTicket], minutes: float) -> list[str]:
    """The `--simulate` closing lines: tickets by priority and true severity, and the HIGH
    ones by expected action (D-068)."""
    by = Counter((t.ticket["priority"], t.answer_key["true_severity"]) for t in drawn)
    actions = Counter(t.answer_key["expected_action"] for t in drawn if t.answer_key["true_severity"] == tickets.HIGH)
    per_minute = len(drawn) / minutes if minutes else 0.0
    return [
        f"# tickets: {len(drawn)} in {minutes:g} min, {per_minute:.1f} a minute",
        *(f"#   {p}: {by[p, tickets.HIGH]} HIGH, {by[p, tickets.LOW]} LOW" for p in tickets.PRIORITIES),
        "#   HIGH by expected action: " + ", ".join(f"{a} {actions[a]}" for a in tickets.ACTIONS),
    ]


def _finding_summary(drawn: list[findings.NewFinding], minutes: float) -> list[str]:
    """The `--simulate` closing lines for the scan clock: findings a minute, and the critical
    ones by expected action (D-070)."""
    actions = Counter(f.answer_key["expected_action"] for f in drawn)
    critical = actions[findings.PATCH] + actions[findings.DISMISS]
    per_minute = len(drawn) / minutes if minutes else 0.0
    return [
        f"# findings: {len(drawn)} in {minutes:g} min, {per_minute:.1f} a minute",
        f"#   critical: {critical} (PATCH {actions[findings.PATCH]}, DISMISS {actions[findings.DISMISS]}), "
        f"not critical: {actions[None]}",
    ]


def run_simulate(sim: Simulation, minutes: float, out=None) -> None:
    """Run a synthetic clock for `minutes` without sleeping, printing one summary line
    per metric service per 10-second window plus the deploy/incident log (brief section 4.5).
    A line sums up the window's metrics records the way the labs do: total traffic and
    average latency (lab 2.3), and the share of failed requests (lab 2.2) (D-058, D-067).
    Each of checkout's tickets gets its console line, and the run ends with the tickets
    counted by priority and true severity (D-068); each CVE scan finding too, counted by
    expected action (D-070)."""
    out = out if out is not None else sys.stdout
    names = metric_service_names()
    total = round(minutes * 60)

    def fresh() -> dict[str, dict]:
        return {n: {"traffic": 0, "errors": 0, "lat": 0.0, "records": 0} for n in names}

    agg = fresh()
    drawn: list[tickets.NewTicket] = []
    scanned: list[findings.NewFinding] = []
    out.write(f"# ops-sim --simulate {minutes:g}min  ({schedule_settings(sim.config)})\n")
    for k in range(total):
        now = sim.start + k
        metrics, _deps, events, new_tickets, new_findings = sim.tick(now)
        for ev in events:
            out.write(format_event(ev, clock=_mmss(round(ev.now - sim.start))) + "\n")
        for t in new_tickets:
            out.write(format_ticket(t, clock=_mmss(k)) + "\n")
        for f in new_findings:
            out.write(format_finding(f, clock=_mmss(k)) + "\n")
        drawn += new_tickets
        scanned += new_findings
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
    if sim.ticket_clock.on:
        out.write("".join(line + "\n" for line in _ticket_summary(drawn, minutes)))
    else:
        out.write("# tickets: off (--no-incidents)\n")
    if sim.scan_clock.on:
        out.write("".join(line + "\n" for line in _finding_summary(scanned, minutes)))
    else:
        out.write("# findings: off (--no-incidents)\n")
    out.flush()
