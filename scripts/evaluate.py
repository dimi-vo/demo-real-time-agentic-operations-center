"""`uv run evaluate` — developer tool (not part of the walkthrough) that compares the
pipeline's output against the generator's incident ground truth.

Detection half (Phase 3): reads `service_health` over Kafka (no Flink statement, no
cost), compares it with the generator's ground truth in `service_incidents` (D-046),
and prints:

- pipeline checks per service: rows, duplicate or missing windows, warm-up rows;
- per incident: whether and when it was detected (window number after onset), with
  which status and direction, and anything else that fired;
- per service: false positives per hour outside incidents, and the fresh-start noise
  in the first 5 minutes.

Decision half (Phase 4): if `remediation_decisions` exists, it also reads that and prints:

- per agent: how many decisions, how many carry a valid action, routing errors;
- per incident: the recommended action against the expected one, with the target;
- routing and coverage: mutual exclusivity, and agent calls against anomalous rows.

Offline: `--dump FILE` saves the rows it read as JSONL; `--rows FILE` evaluates such a
file without connecting (so a run can be shared and re-scored without credentials).
`--dump-decisions` / `--decision-rows` do the same for `remediation_decisions`, and
`--dump-incidents` / `--incidents` for `service_incidents`. `--rows` needs
`--incidents`: offline, there is no other ground truth.
Definitions are in `scripts/evaluation/detection.py` (D-021) and
`scripts/evaluation/decisions.py` (D-032).
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.table import Table

from scripts.common.paths import get_project_root
from scripts.common.topic_reader import epoch_seconds, jsonable
from scripts.evaluation.decisions import (
    VALID_ACTION_TARGET,
    DecisionReport,
    DecisionRow,
    score_decisions,
)
from scripts.evaluation.detection import (
    DETECT_WITHIN_WINDOWS,
    DETECTED,
    FRESH_START_SECONDS,
    LATE,
    MISSED,
    NO_DATA,
    UNLOGGED_TAIL_SECONDS,
    DetectionReport,
    HealthRow,
    Incident,
    default_range,
    incidents_from_records,
    rescale_confidence,
    score,
)

SERVICE_HEALTH = "service_health"
REMEDIATION_DECISIONS = "remediation_decisions"
SERVICE_INCIDENTS = "service_incidents"
FP_TARGET_PER_HOUR = 1.0
FRESH_START_TARGET = 1

console = Console()


def _t(epoch: float | None) -> str:
    if epoch is None:
        return "-"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%H:%M:%S")


def _dt(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _parse_args(argv):
    p = argparse.ArgumentParser(prog="evaluate", description=__doc__.split("\n\n")[0])
    p.add_argument("--minutes", type=float, default=60.0, help="length of the scored range (default: 60)")
    p.add_argument("--from", dest="range_from", help="range start, ISO-8601 UTC (overrides --minutes)")
    p.add_argument("--to", dest="range_to", help="range end, ISO-8601 UTC (default: 100 s before the newest row)")
    p.add_argument("--topic", default=SERVICE_HEALTH, help="table to score (default: service_health)")
    p.add_argument("--creds", help="credential card (default: the active card)")
    p.add_argument("--incidents", help="score against this JSONL dump of service_incidents instead of reading Kafka")
    p.add_argument("--dump-incidents", help="also write the incident records read to this JSONL file")
    p.add_argument("--dump", help="also write the rows read to this JSONL file")
    p.add_argument("--rows", help="evaluate this JSONL dump instead of reading Kafka")
    p.add_argument(
        "--decisions-topic",
        default=REMEDIATION_DECISIONS,
        help=f"table of decisions (default: {REMEDIATION_DECISIONS})",
    )
    p.add_argument("--decision-rows", help="score this JSONL dump of decisions instead of reading Kafka")
    p.add_argument("--dump-decisions", help="also write the decisions read to this JSONL file")
    p.add_argument("--no-decisions", action="store_true", help="skip the decision half")
    p.add_argument("--details", action="store_true", help="list every false positive and fresh-start anomaly")
    p.add_argument(
        "--rescale",
        metavar="FROM:TO",
        help="offline tuning aid: re-derive statuses as if confidencePercentage were TO instead of FROM",
    )
    return p.parse_args(argv)


def _read_jsonl(path: str) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def _dump(records: list[dict], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    console.print(f"Wrote {len(records)} rows to {path}")


def _read_table(topic: str, creds, root: Path) -> list[dict] | None:
    """Read a table over Kafka. None when it doesn't exist yet."""
    from scripts.common.credentials import load_card
    from scripts.common.topic_reader import TopicNotFound, read_topic

    _, card = load_card(creds, root)
    with console.status(f"Reading {topic} …"):
        try:
            records = read_topic(card, topic)
        except TopicNotFound:
            return None
    return [jsonable(r) for r in records]


def _load_rows(args, root: Path) -> list[dict]:
    if args.rows:
        return _read_jsonl(args.rows)
    records = _read_table(args.topic, args.creds, root)
    if records is None:
        raise SystemExit(f"`{args.topic}` doesn't exist yet. Create it with sql/2.4_service_health.sql.")
    if args.dump:
        _dump(records, args.dump)
    return records


def _load_incidents(args, root: Path) -> tuple[list[Incident], str]:
    """The ended incidents to score against, and where they came from (D-046)."""
    if args.incidents:
        return incidents_from_records(_read_jsonl(args.incidents)), args.incidents
    if args.rows:  # offline health dump, no credentials in play
        raise SystemExit(
            "--rows scores offline, so it needs --incidents FILE as well: a dump of "
            f"`{SERVICE_INCIDENTS}` made with --dump-incidents."
        )
    records = _read_table(SERVICE_INCIDENTS, args.creds, root)
    if records is None:
        console.print(f"[yellow]`{SERVICE_INCIDENTS}` doesn't exist yet, so there is no ground truth.[/yellow]")
        return [], SERVICE_INCIDENTS
    if args.dump_incidents:
        _dump(records, args.dump_incidents)
    return incidents_from_records(records), SERVICE_INCIDENTS


def _load_decisions(args, root: Path) -> list[dict] | None:
    """The decision rows, or None when the decision half doesn't apply."""
    if args.no_decisions:
        return None
    if args.decision_rows:
        return _read_jsonl(args.decision_rows)
    if args.rows:  # offline health dump, no credentials in play
        return None
    records = _read_table(args.decisions_topic, args.creds, root)
    if records is not None and args.dump_decisions:
        _dump(records, args.dump_decisions)
    return records


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    root = get_project_root()
    records = _load_rows(args, root)
    if not records:
        raise SystemExit(f"`{args.topic}` has no rows yet.")
    rows = [HealthRow.from_record(r) for r in records]
    if args.rescale:
        base, target = (float(x) for x in args.rescale.split(":"))
        rows = rescale_confidence(rows, base, target)
        console.print(f"[yellow]Statuses re-derived offline for confidence {target} (ran at {base}).[/yellow]")

    range_from, range_to = default_range(rows, args.minutes)
    if args.range_to:
        range_to = epoch_seconds(args.range_to)
        range_from = range_to - args.minutes * 60.0
    if args.range_from:
        range_from = epoch_seconds(args.range_from)

    incidents, gt_source = _load_incidents(args, root)
    report = score(rows, incidents, range_from, range_to)
    _render(report, args.rows or args.topic, gt_source, len(incidents), args.details)

    records = _load_decisions(args, root)
    if records is None:
        console.print(
            f"\n[dim]No `{args.decisions_topic}` yet — the decision half is skipped. "
            f"Create it with sql/4.3_remediation_decisions.sql and the two INSERT statements.[/dim]"
        )
        return
    decisions = [DecisionRow.from_record(r) for r in records]
    _render_decisions(
        score_decisions(decisions, rows, incidents, range_from, range_to),
        args.decision_rows or args.decisions_topic,
        len(decisions),
        args.details,
    )


def _render(rep: DetectionReport, source: str, gt_source: str, n_incidents: int, details: bool) -> None:
    console.print(
        f"\n[bold]{source}[/bold]: {rep.total_rows} rows, {len(rep.services)} services. "
        f"Scored range: {_dt(rep.range_from)} → {_t(rep.range_to)} "
        f"([dim]ends {UNLOGGED_TAIL_SECONDS:.0f} s before the newest row, "
        f"so no incident still in progress is scored[/dim])"
    )
    console.print(f"Ground truth: {gt_source} ({n_incidents} ended incidents)\n")

    t = Table(title="Pipeline checks", title_justify="left")
    cols = ("service", "rows", "first window", "last window", "warm-up rows", "warm-up later", "duplicates", "gaps")
    for col in cols:
        t.add_column(col, justify="right" if col != "service" else "left")
    for s in rep.services.values():
        t.add_row(
            s.service_name, str(s.rows), _t(s.first_window_end), _t(s.last_window_end),
            f"{s.warmup_rows} (until {_t(s.last_warmup_end)})" if s.warmup_rows else "0",
            _flag(s.warmup_after_fresh_start), _flag(s.duplicates), f"{s.gaps} ({s.missing_windows})",
        )  # fmt: skip
    console.print(t)
    console.print(
        "[dim]warm-up later = WARMING_UP rows after a service's first 5 minutes (expect 0). "
        "Gaps are expected only where ops-sim was stopped.[/dim]\n"
    )

    t = Table(title="Incidents (detection)", title_justify="left")
    for col in ("id", "type", "service", "started", "dur", "expected", "result", "window", "evidence", "also"):
        t.add_column(col)
    for r in rep.incidents:
        inc = r.incident
        result = {
            DETECTED: "[green]detected[/green]",
            LATE: "[yellow]late[/yellow]",
            MISSED: "[red]missed[/red]",
            NO_DATA: "[dim]no rows[/dim]",
        }[r.status]
        if r.wrong_direction:
            result += " [red](wrong direction)[/red]"
        evidence = r.detected_row.describe(inc.expected_trigger) if r.detected_row else ""
        also = ", ".join(f"{sig}@{k}{' end' if end else ''}" for k, sig, end in r.other_signals)
        t.add_row(
            inc.incident_id, inc.type, inc.service_name, _t(inc.started_at), f"{inc.ended_at - inc.started_at:.0f}s",
            inc.expected_trigger, result, str(r.first_window or "-"), evidence, also,
        )  # fmt: skip
    console.print(t)
    console.print(
        f"[dim]window = first window carrying the expected trigger, counting the window that contains "
        f"the onset as 1 (target: ≤ {DETECT_WITHIN_WINDOWS}). also = other signals in the "
        f"incident's windows, as SIGNAL@window ('end' = the window holding the return to normal). "
        f"gaps = gaps (missing windows).[/dim]"
    )
    for typ, (ok, total) in sorted(rep.by_type().items()):
        console.print(f"  {typ}: {ok}/{total} detected within {DETECT_WITHIN_WINDOWS} windows")
    console.print()

    t = Table(title="False positives (outside incidents) and fresh-start noise", title_justify="left")
    cols = ("service", "scored h", "false pos.", "per hour", "of which echo", "after deploy", "fresh start", "restart")
    for col in cols:
        t.add_column(col, justify="right" if col != "service" else "left")
    worst_fp, worst_fresh = 0.0, 0
    for s in rep.services.values():
        per_h = s.fp_per_hour
        worst_fp = max(worst_fp, per_h or 0.0)
        worst_fresh = max(worst_fresh, len(s.fresh_start_noise))
        t.add_row(
            s.service_name, f"{s.scored_hours:.2f}", str(len(s.false_positives)),
            "-" if per_h is None else _target(per_h, FP_TARGET_PER_HOUR, "{:.1f}"),
            str(sum(fp.tag == "echo" for fp in s.false_positives)),
            str(sum(fp.tag == "after deploy" for fp in s.false_positives)),
            _target(len(s.fresh_start_noise), FRESH_START_TARGET, "{}"),
            str(len(s.restart_artifacts)),
        )  # fmt: skip
    console.print(t)
    console.print(
        f"[dim]echo = within a minute after an incident on that service ended; after deploy = the "
        f"service deployed less than a minute earlier. Fresh start = the first "
        f"{FRESH_START_SECONDS / 60:.0f} minutes after the service's first window in the topic (only "
        f"meaningful if the topic starts at a fresh `reset` + `ops-sim`). restart = signals next to a gap in "
        f"the data (ops-sim stopped and restarted); not counted as false positives.[/dim]"
    )

    if details:
        for s in rep.services.values():
            groups = (
                ("false positive", s.false_positives),
                ("fresh start", s.fresh_start_noise),
                ("restart", s.restart_artifacts),
            )
            for label, fps in groups:
                for fp in fps:
                    tag = f" ({fp.tag})" if fp.tag and fp.tag != "restart" else ""
                    console.print(
                        f"  {label:14} {s.service_name:12} {_t(fp.row.window_end)} {fp.signal:12} "
                        f"{fp.row.describe(fp.signal)} v{fp.row.version}{tag}"
                    )

    scored = [r for r in rep.incidents if r.status != NO_DATA]
    in_time = sum(r.in_time for r in scored)
    console.print("\n[bold]Acceptance (brief section 11)[/bold]")
    console.print(f"  Detected within {DETECT_WITHIN_WINDOWS} windows: {in_time}/{len(scored)} incidents")
    console.print(f"  False positives: worst service {worst_fp:.1f}/hour (target ≤ {FP_TARGET_PER_HOUR:.0f})")
    console.print(f"  Fresh-start noise: worst service {worst_fresh} in 5 min (target ≤ {FRESH_START_TARGET})")


def _render_decisions(rep: DecisionReport, source: str, total_read: int, details: bool) -> None:
    console.print(
        f"\n[bold]{source}[/bold]: {rep.total_decisions} decisions in the scored range "
        f"({total_read} read in total)\n"
    )
    if not rep.total_decisions:
        console.print("[yellow]No decisions in range. Are both INSERT statements running?[/yellow]")
        return

    t = Table(title="Agents", title_justify="left")
    for col in ("agent", "decisions", "valid action", "routing errors", "actions"):
        t.add_column(col, justify="right" if col != "agent" else "left")
    for a in sorted(rep.agents.values(), key=lambda a: a.agent_name):
        t.add_row(
            a.agent_name or "[red](none)[/red]", str(a.decisions),
            _target_low(a.valid_share or 0.0, VALID_ACTION_TARGET, "{:.0%}"), _flag(a.routing_errors),
            ", ".join(f"{k} {v}" for k, v in a.actions.most_common()),
        )  # fmt: skip
    console.print(t)
    console.print(
        "[dim]valid action = the parsed action is one the agent is allowed to take "
        "(INVALID covers a failed call or an answer outside the set). routing errors = "
        "the trigger belongs to the other agent.[/dim]\n"
    )

    t = Table(title="Incidents (decisions)", title_justify="left")
    for col in ("id", "type", "service", "expected", "recommended", "target", "calls", "later windows"):
        t.add_column(col)
    scored = [i for i in rep.incidents if i.decisions]
    for item in scored:
        inc = item.incident
        d = item.headline
        rec = f"[green]{d.action}[/green]" if item.matched else f"[red]{d.action}[/red]"
        if not item.matched and item.any_matched:
            rec += " [yellow](later)[/yellow]"
        target = {True: "[green]ok[/green]", False: "[red]wrong[/red]", None: "-"}[item.target_ok]
        if d.target:
            target += f" {d.target}"
        later = ", ".join(f"{k} {v}" for k, v in item.actions.most_common() if k != d.action)
        t.add_row(
            inc.incident_id, inc.type, inc.service_name, inc.expected_action, rec, target,
            str(len(item.decisions)), later,
        )  # fmt: skip
    console.print(t)
    console.print(
        "[dim]recommended = the decision on the first window of the incident, the one the "
        "on-call team would act on. (later) = a later window did recommend the expected "
        "action. target = the version for ROLLBACK or the replica count for SCALE_OUT, "
        "against what the SQL computed.[/dim]"
    )
    no_decision = [i for i in rep.incidents if not i.decisions]
    if no_decision:
        console.print(
            f"[dim]{len(no_decision)} incidents in range produced no decision "
            f"({', '.join(i.incident.incident_id for i in no_decision)}): "
            f"not detected, or the agents were not running.[/dim]"
        )

    mismatches = [i for i in scored if not i.matched]
    if mismatches:
        console.print("\n[bold]Mismatches[/bold] (expected vs recommended, with the agent's own words)")
        for item in mismatches:
            d = item.headline
            console.print(
                f"  [red]{item.incident.incident_id}[/red] {item.incident.service_name} "
                f"{item.incident.type}: expected {item.incident.expected_action}, got {d.action} "
                f"— {rep.context(d)}"
            )
            console.print(f"    reason: {d.reason or '-'}")
            if details:
                console.print(f"    raw: {(d.raw_response or '').strip()}")
        if not details:
            console.print("  [dim]--details prints each raw_response in full.[/dim]")

    if rep.bad_targets:
        console.print(f"\n[bold]Targets[/bold]: [red]{len(rep.bad_targets)} wrong[/red]")
        for d in rep.bad_targets[: None if details else 10]:
            console.print(
                f"  {d.service_name:12} {_t(d.window_end)} {d.action:20} target={d.target!r} "
                f"— {rep.context(d)}"
            )

    cov = rep.coverage
    console.print("\n[bold]Routing and coverage[/bold]")
    console.print(
        f"  Mutually exclusive: {_ok(not rep.both_agents)} "
        f"({len(rep.both_agents)} windows went to both agents)"
    )
    console.print(
        f"  Agent calls track anomalies: {cov.decided}/{cov.expected} anomalous rows have a decision, "
        f"out of {cov.total_rows} rows"
    )
    console.print(
        f"    [dim]counted from {_t(cov.counted_from)}, the first decision: the INSERTs read "
        f"service_health from the latest offset, so anomalies from before the agents were "
        f"created have no decision by design.[/dim]"
    )
    if cov.in_flight:
        console.print(f"    [dim]{cov.in_flight} anomalous rows are too recent to have a decision yet.[/dim]")
    if cov.missing:
        console.print(f"  [red]Missing decisions: {len(cov.missing)}[/red]")
        for svc, we, agent in cov.missing if details else cov.missing[:10]:
            console.print(f"    {svc:12} {_t(we)} {agent}")
    if cov.unexpected:
        console.print(f"  [red]Decisions on rows that were not anomalous: {len(cov.unexpected)}[/red]")
    if cov.duplicates:
        console.print(f"  [yellow]Duplicate decisions (same window and agent): {len(cov.duplicates)}[/yellow]")

    if rep.outside:
        actions = Counter(d.action for d in rep.outside)
        console.print(
            f"\n  Outside any incident: {len(rep.outside)} decision{'s' if len(rep.outside) != 1 else ''} "
            f"({', '.join(f'{k} {v}' for k, v in actions.most_common())}) — these follow the "
            f"detector's false positives; OBSERVE is the appropriate answer."
        )

    console.print("\n[bold]Acceptance (brief section 11)[/bold]")
    console.print(
        f"  Valid actions: {_target_low(rep.valid_share or 0.0, VALID_ACTION_TARGET, '{:.0%}')} "
        f"(target ≥ {VALID_ACTION_TARGET:.0%})"
    )
    console.print(f"  Action matches ground truth: {sum(i.matched for i in scored)}/{len(scored)} incidents")
    console.print(f"  Routing mutually exclusive: {_ok(not rep.both_agents)}")


def _ok(passed: bool) -> str:
    return "[green]yes[/green]" if passed else "[red]no[/red]"


def _target_low(value: float, target: float, fmt: str) -> str:
    """Colour a value that should be at or above `target`."""
    text = fmt.format(value)
    return f"[green]{text}[/green]" if value >= target else f"[red]{text}[/red]"


def _flag(n: int) -> str:
    return f"[red]{n}[/red]" if n else "0"


def _target(value: float, target: float, fmt: str) -> str:
    text = fmt.format(value)
    return f"[red]{text}[/red]" if value > target else f"[green]{text}[/green]"


if __name__ == "__main__":
    main()
