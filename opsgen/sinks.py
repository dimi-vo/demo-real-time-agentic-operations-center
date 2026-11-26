"""Output sinks and human formatting for the generator.

`DryRunSink` writes the data events as JSON lines on stdout (each tagged with
`_stream` or `_event`, so they're greppable and testable) and a concise human line
per schedule event, per support ticket and per CVE scan finding on stderr. The Kafka sink lives in
`opsgen/kafka_io.py` (Phase 2).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from opsgen.engine import BAD_DEPLOY_TYPES, Event, Incident
from opsgen.findings import NewFinding
from opsgen.tickets import SOURCES_BY_CODE, NewTicket


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
    if ev.kind == "deploy":  # a good deploy (D-067)
        return f"[{when}] DEPLOY          {ev.service_name:<12} {ev.old_version} -> {ev.new_version}"
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
        if inc and inc.type in BAD_DEPLOY_TYPES:
            tail.append(f"rollback {ev.old_version} -> {ev.new_version}")
        if inc:
            tail.append(_how_it_ended(inc))
        itype = inc.type if inc else "?"
        return f"[{when}] INCIDENT END    {itype:<14} {ev.service_name:<12} {', '.join(tail)}".rstrip()
    return f"[{when}] {ev.kind} {ev.service_name}"


def format_ticket(t: NewTicket, clock: str | None = None) -> str:
    """One human-readable line per ticket (D-068): its priority, id, source and subject, then
    its answer key in brackets, set apart so nobody takes it for part of the ticket.

        [10:42:05] TICKET          P1  ZD-48215    Zendesk     "Checkout times out ..."  [truth: HIGH, SCALE_OUT]
    """
    ticket, key = t
    when = clock if clock is not None else _hms(ticket["event_time"] / 1000)
    truth = ", ".join(v for v in (key["true_severity"], key["expected_action"]) if v)
    return (
        f"[{when}] TICKET          {ticket['priority']}  {ticket['ticket_id']:<10}  "
        f'{SOURCES_BY_CODE[ticket["source"]].name:<10}  "{ticket["subject"]}"  [truth: {truth}]'
    )


def format_finding(f: NewFinding, clock: str | None = None) -> str:
    """One human-readable line per CVE scan finding (D-070): its id, score, repository, CVE and
    package bump, then its answer key in brackets, set apart like a ticket's:

        [10:42:05] FINDING         SCAN-000001   9.4  checkout           CVE-2026-41234  ...
            ... urllib3 1.26.4 -> 1.26.18  [truth: PATCH]

    on one line. A finding that isn't critical reads `[truth: not critical]`.
    """
    finding, key = f
    when = clock if clock is not None else _hms(finding["event_time"] / 1000)
    truth = key["expected_action"] or "not critical"
    return (
        f"[{when}] FINDING         {finding['finding_id']}  {finding['cvss_score']:>4.1f}  "
        f"{finding['repository']:<17}  {finding['cve_id']}  "
        f"{finding['package']} {finding['installed_version']} -> {finding['fixed_version']}  [truth: {truth}]"
    )


def format_record(topic: str, record: dict, now: float, key: str | None = None) -> str:
    """One record as ops-sim sends it, for `--show-records`: the topic, the Kafka key (the
    raw service name unless `key` says otherwise: an answer-key record's is its ticket id)
    and the dict the Avro serializer gets, unchanged.

        [10:42:05] service_metrics key=web-gateway {"event_time": 1800000005999, ...}
        [10:42:05] ticket_answer_key key=ZD-48215 {"event_time": 1800000005000, ...}
    """
    key = record["service_name"] if key is None else key
    return f"[{_hms(now)}] {topic} key={key} {json.dumps(record)}"


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

    def emit_tickets(self, tickets: list[NewTicket]) -> None:
        """Each ticket's two records, tagged with their topics, and its console line."""
        for t in tickets:
            self.out.write(json.dumps({"_stream": "support_tickets", **t.ticket}) + "\n")
            self.out.write(json.dumps({"_stream": "ticket_answer_key", **t.answer_key}) + "\n")
            self.log.write(format_ticket(t) + "\n")
        self.log.flush()

    def emit_findings(self, findings: list[NewFinding]) -> None:
        """Each finding's two records, tagged with their topics, and its console line (D-070)."""
        for f in findings:
            self.out.write(json.dumps({"_stream": "cve_scans", **f.finding}) + "\n")
            self.out.write(json.dumps({"_stream": "cve_answer_key", **f.answer_key}) + "\n")
            self.log.write(format_finding(f) + "\n")
        self.log.flush()

    def emit_event(self, ev: Event) -> None:
        self.out.write(json.dumps(event_json(ev)) + "\n")
        self.log.write(format_event(ev) + "\n")
        self.log.flush()

    def flush(self) -> None:
        self.out.flush()
