"""Output sinks and human formatting for the generator.

`DryRunSink` writes the data events as JSON lines on stdout (each tagged with
`_stream` or `_event`, so they're greppable and testable) and a concise human line
per schedule event on stderr. The Kafka sink lives in `opsgen/kafka_io.py` (Phase 2).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from opsgen.engine import BAD_DEPLOY, Event, Incident


def _hms(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%H:%M:%S")


def _how_it_ended(inc: Incident) -> str:
    """`resolved by release_agent ROLLBACK after 17 s`, or `expired after 35 s` (D-065)."""
    lasted = f"after {inc.degradation_end - inc.degradation_start:.0f} s"
    return f"resolved by {inc.resolved_by} {lasted}" if inc.resolved_by else f"expired {lasted}"


def format_event(ev: Event, clock: str | None = None) -> str:
    """One human-readable line describing a schedule event. `clock` is an optional
    pre-formatted time label; if omitted, the event's wall-clock time is used."""
    when = clock if clock is not None else _hms(ev.now)
    if ev.kind == "bad_deploy":
        return f"[{when}] BAD DEPLOY      {ev.service_name:<12} {ev.old_version} -> {ev.new_version}"
    if ev.kind == "incident_start":
        inc = ev.incident
        return (
            f"[{when}] INCIDENT START  {inc.type:<14} {ev.service_name:<12} "
            f"{inc.expected_trigger} -> expect {inc.expected_action}"
        )
    if ev.kind == "incident_end":
        inc = ev.incident
        tail = []
        if inc and inc.type == BAD_DEPLOY:
            tail.append(f"rollback {ev.old_version} -> {ev.new_version}")
        if inc:
            tail.append(_how_it_ended(inc))
        itype = inc.type if inc else "?"
        return f"[{when}] INCIDENT END    {itype:<14} {ev.service_name:<12} {', '.join(tail)}".rstrip()
    return f"[{when}] {ev.kind} {ev.service_name}"


def event_json(ev: Event) -> dict:
    out = {"_event": ev.kind, "service_name": ev.service_name, "event_time": int(ev.now * 1000)}
    if ev.old_version is not None:
        out["old_version"] = ev.old_version
    if ev.new_version is not None:
        out["new_version"] = ev.new_version
    if ev.incident is not None:
        out["incident_id"] = ev.incident.incident_id
        out["incident_type"] = ev.incident.type
        out["expected_trigger"] = ev.incident.expected_trigger
        out["expected_action"] = ev.incident.expected_action
    return out


class DryRunSink:
    """Prints data events as JSON lines to stdout and schedule events to stderr."""

    def __init__(self, out=None, log=None) -> None:
        self.out = out if out is not None else sys.stdout
        self.log = log if log is not None else sys.stderr

    def emit_metrics(self, records: list[dict]) -> None:
        for r in records:
            self.out.write(json.dumps({"_stream": "service_metrics", **r}) + "\n")

    def emit_deployments(self, records: list[dict]) -> None:
        for r in records:
            self.out.write(json.dumps({"_stream": "service_deployments", **r}) + "\n")

    def emit_incident(self, record: dict) -> None:
        self.out.write(json.dumps({"_stream": "service_incidents", **record}) + "\n")

    def emit_event(self, ev: Event) -> None:
        self.out.write(json.dumps(event_json(ev)) + "\n")
        self.log.write(format_event(ev) + "\n")
        self.log.flush()

    def flush(self) -> None:
        self.out.flush()
