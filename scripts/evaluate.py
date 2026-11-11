"""`uv run evaluate` — developer tool (not part of the walkthrough) that compares the
pipeline's output against the generator's incident ground truth.

Detection half (Phase 3): reads `service_health` over Kafka (no Flink statement, no
cost), compares it with `runs/pre-provisioned/incidents.jsonl`, and prints:

- pipeline checks per service: rows, duplicate or missing windows, warm-up rows;
- per incident: whether and when it was detected (window number after onset), with
  which status and direction, and anything else that fired;
- per service: false positives per hour outside incidents, and the fresh-start noise
  in the first 5 minutes.

The decision half (reading `remediation_decisions`) lands in Phase 4.

Offline: `--dump FILE` saves the rows it read as JSONL; `--rows FILE` evaluates such a
file without connecting (so a run can be shared and re-scored without credentials).
Definitions are in `scripts/evaluation/detection.py` (D-021).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.table import Table

from scripts.common.paths import get_project_root
from scripts.common.topic_reader import epoch_seconds, jsonable
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
    default_range,
    load_incidents,
    rescale_confidence,
    score,
)

SERVICE_HEALTH = "service_health"
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
    p.add_argument("--incidents", help="ground-truth file (default: runs/pre-provisioned/incidents.jsonl)")
    p.add_argument("--dump", help="also write the rows read to this JSONL file")
    p.add_argument("--rows", help="evaluate this JSONL dump instead of reading Kafka")
    p.add_argument("--details", action="store_true", help="list every false positive and fresh-start anomaly")
    p.add_argument(
        "--rescale",
        metavar="FROM:TO",
        help="offline tuning aid: re-derive statuses as if confidencePercentage were TO instead of FROM",
    )
    return p.parse_args(argv)


def _load_rows(args, root: Path) -> list[dict]:
    if args.rows:
        return [json.loads(line) for line in Path(args.rows).read_text().splitlines() if line.strip()]
    from scripts.common.credentials import load_card
    from scripts.common.topic_reader import TopicNotFound, read_topic

    _, card = load_card(args.creds, root)
    with console.status(f"Reading {args.topic} …"):
        try:
            records = read_topic(card, args.topic)
        except TopicNotFound:
            raise SystemExit(f"`{args.topic}` doesn't exist yet. Create it with sql/2.4_service_health.sql.") from None
    records = [jsonable(r) for r in records]
    if args.dump:
        Path(args.dump).parent.mkdir(parents=True, exist_ok=True)
        with Path(args.dump).open("w", encoding="utf-8") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")
        console.print(f"Wrote {len(records)} rows to {args.dump}")
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

    gt_path = Path(args.incidents) if args.incidents else root / "runs" / "pre-provisioned" / "incidents.jsonl"
    incidents = load_incidents(gt_path)
    report = score(rows, incidents, range_from, range_to)
    _render(report, args.rows or args.topic, gt_path, len(incidents), args.details)


def _render(rep: DetectionReport, source: str, gt_path: Path, n_incidents: int, details: bool) -> None:
    console.print(
        f"\n[bold]{source}[/bold]: {rep.total_rows} rows, {len(rep.services)} services. "
        f"Scored range: {_dt(rep.range_from)} → {_t(rep.range_to)} "
        f"([dim]ends {UNLOGGED_TAIL_SECONDS:.0f} s before the newest row, "
        f"so no incident still in progress is scored[/dim])"
    )
    console.print(f"Ground truth: {gt_path} ({n_incidents} incidents in the file)\n")

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
        if r.version_mismatches:
            result += f" [red]({r.version_mismatches} version mismatches)[/red]"
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


def _flag(n: int) -> str:
    return f"[red]{n}[/red]" if n else "0"


def _target(value: float, target: float, fmt: str) -> str:
    text = fmt.format(value)
    return f"[red]{text}[/red]" if value > target else f"[green]{text}[/green]"


if __name__ == "__main__":
    main()
