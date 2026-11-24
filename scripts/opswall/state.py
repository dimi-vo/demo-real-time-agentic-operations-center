"""Thread-safe in-memory snapshot of the dashboard.

One ``OpsWallState`` is shared between the Kafka consumer threads, which call
``apply_*()``, and the FastAPI websocket loop, which calls
``snapshot(now)`` and pushes it to every browser at ~4 Hz. All access is guarded
by one lock; the write methods are tiny so contention is negligible.

The appliers keep the raw facts: one health row and one error-rate row per window, one
decision per window and agent, one incident per id, one ``service_metrics`` sample per
second, and each version change.
``snapshot(now)`` derives everything the page shows from them, so the page only draws
and the counting is tested here (D-054). Deriving
at snapshot time also lets a late row or decision reshape what is drawn (records can land
out of order, and the consumer reads ``read_uncommitted``, so a row an aborted Flink
transaction wrote comes again when the statement replays, D-063).

- A **run** is consecutive flagged windows of one service for one trigger (latency
  high, traffic high, traffic low from ``service_health``; error rate high from
  ``service_error_rate``, D-067), counting only windows whose ``window_end`` is on the
  chart. One window without that flag ends the run.
- Its **block** opens at the ``detected_at`` of the run's first window and closes at
  the Kafka write time of the first decision for any window of the run, whatever the
  action. Still unanswered 30 s after the run's last ``detected_at``, it is removed.
- A block **belongs** to an incident on the same metric when the run's first window
  overlaps the incident, counting up to 30 s after it ended. Otherwise it is a false
  alarm. An incident can own several blocks.
- An incident is **answered** once one of its blocks closed, **missed** when it ended
  more than 30 s ago with none closed, and **pending** until then. Time to detect and
  time to answer are measured on its first block.
- The tile: a block open → ``anomaly``; a fix within the last 120 s → ``remediated``;
  otherwise ``healthy``.
- The **Issues panel** of a row (``issues``, D-068), over the incidents that started in the
  last 10 minutes: injected; detected (an owned block, and not missed); missed; the median
  time to detect and the median time to remediate (start → the first answer), both over
  answered incidents. A ticket service has its own, counted from tickets (below); the page
  draws ``issues`` whatever it was counted from. There is no global summary.
- The **samples** are the raw ``service_metrics`` records, one per metric service per
  second (D-058, D-068), kept for the axis plus a margin (``trim()``). The snapshot
  carries the ones on the chart; the page draws them as the latency and traffic lines
  (D-061), and, for a row with the errors chart, the share of each second's requests
  that failed (null for a second without requests, D-067).
- The **charts** of a row: web-gateway's is the errors chart alone (lab 2.2's threshold,
  D-067), payments' are latency and traffic, and checkout, which produces tickets and no
  metrics, has none (D-068). A row with the errors chart
  also gets a **deploy mark** per DEPLOY and ROLLBACK of its service, good deploys
  included, keyed by ``deployed_at`` so the 5-s heartbeat adds nothing.
- The **row kind** (``row``): ``"tickets"`` for the ticket services of ``opsgen.services``
  (checkout), ``"metrics"`` for the others, so the page holds no service names (D-068).
- A **ticket** is one record per ``ticket_id``, filled in from five topics in any order: its
  fields (``support_tickets``), when lab 5.1 routed it (the ``p1_tickets`` record's Kafka
  timestamp), the classifier's verdict and reason and when it gave them (``ticket_triage``),
  its true severity and expected action (``ticket_answer_key``), and the responder's action
  and reason and when it gave them (a ``remediation_decisions`` record with a ``ticket_id``).
  Such a decision goes to its ticket and nowhere else: it never closes a metric block,
  counts as a false alarm or sets a metric row's status.
- A ticket row's **runs** (the last 2 minutes): the classifier's, from the ``p1_tickets``
  record to the verdict ("High in 4s"), and for a HIGH verdict the responder's, from the
  verdict to the decision ("Scale out in 6s"). A run with no answer is open ("Working 3s")
  until 30 s after it started, then ends there ("No answer"). The row is "Agent resolving
  P1" while a run is open; ``openP1`` counts the open runs' tickets, ``agentsWorking`` their
  lanes. Its **Issues panel** counts the really-HIGH P1 tickets created in the last 10
  minutes: detected (a HIGH verdict), missed (LOW, INVALID, or no verdict 30 s after the
  ticket), ticket -> verdict, ticket -> action. Its list is the newest 5 tickets, with the
  count of the last 10 minutes; its marks are the tickets of the last 2 minutes.

State stores absolute epoch seconds everywhere. The snapshot turns positions into the
axis-relative seconds the page draws with: ``t - (now - axis_seconds)``, clipped to
``[0, axis_seconds]``. Durations stay plain seconds.
"""

from __future__ import annotations

import bisect
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opsgen.services import SERVICES, TICKET_SERVICES

AXIS_SECONDS = 600
WINDOW_SECONDS = 10.0
ANSWER_WAIT_SECONDS = 30.0      # an unanswered block goes this long after its run's last detection
OWNERSHIP_GRACE_SECONDS = 30.0  # a run starting this long after its incident ended still belongs to it
MISSED_AFTER_SECONDS = 30.0     # an incident nobody answered is missed this long after it ended
FIX_RECENT_SECONDS = 120.0      # the tile shows a fix this long
FIX_ACTIONS = ("ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM")
# Keep this long past the axis: a run straddling the left edge stays whole, and a sample
# stays until well after it scrolled off.
RETAIN_MARGIN_SECONDS = 60.0


@dataclass(frozen=True)
class Trigger:
    name: str        # as the agents write it in remediation_decisions.trigger
    metric: str      # the chart it is drawn on
    direction: str
    source: str      # the ServiceState rows it reads: "health" or "error_rates"
    column: str      # the row's status field it reads
    flag: str        # the status value that flags it
    words: str       # how the tile says it


TRIGGERS = (
    Trigger("LATENCY_HIGH", "latency", "up", "health", "latency_status", "HIGH", "latency high"),
    Trigger("TRAFFIC_HIGH", "traffic", "up", "health", "traffic_status", "HIGH", "traffic high"),
    Trigger("TRAFFIC_LOW", "traffic", "down", "health", "traffic_status", "LOW", "traffic low"),
    Trigger("ERROR_RATE_HIGH", "errors", "up", "error_rates", "error_status", "HIGH", "error rate high"),
)
INCIDENT_METRIC = {  # no TRAFFIC_SURGE: surges are retired (D-068)
    "BAD_DEPLOY": "latency",
    "UPSTREAM_DROP": "traffic",
    "BAD_DEPLOY_ERRORS": "errors",
}
# The rows whose only chart is the errors chart: the services lab 2.2 reads (D-067).
ERRORS_CHART_SERVICES = ("web-gateway",)
# Lab 2.2's rule (sql/2.2_error_rate.sql), drawn as a line on the errors chart; a test keeps
# the two in step.
ERROR_RATE_THRESHOLD_PCT = 25.0
DEPLOY_CHANGES = ("DEPLOY", "ROLLBACK")


@dataclass
class HealthRow:
    window_end: float  # epoch seconds
    latency_status: str
    traffic_status: str
    detected_at: float | None = None  # when the detector produced the verdict; None on older rows


@dataclass
class ErrorRateRow:
    """One ``service_error_rate`` row: one web-gateway window of lab 2.2 (D-067)."""
    window_end: float  # epoch seconds
    error_status: str
    detected_at: float | None = None


@dataclass
class MetricSample:
    """One ``service_metrics`` record: one second of one service (D-058)."""
    event_time: float  # epoch seconds; the end of the second it covers
    traffic_count: int
    latency_ms: float
    error_count: int = 0


@dataclass
class DeployMark:
    """A version change of one service, from ``service_deployments`` (D-067)."""
    at: float                     # deployed_at, epoch seconds
    kind: str                     # DEPLOY or ROLLBACK
    version: str | None           # the version it went to
    previous_version: str | None  # the version it left


@dataclass
class IncidentEvent:
    event_time: float  # epoch seconds
    incident_id: str
    incident_type: str
    started_at: float
    ended_at: float | None
    expected_trigger: str
    expected_action: str


@dataclass
class DecisionMark:
    at: float            # epoch seconds — Kafka record timestamp, the write time
    window_end: float    # the health window the agent reasoned about
    action: str
    trigger: str
    target: str | None
    reason: str | None
    agent: str           # release_agent or capacity_agent


@dataclass
class Ticket:
    """One support ticket, filled in from whichever of its records have arrived (D-068)."""
    ticket_id: str
    event_time: float | None = None  # when it was created
    source: str | None = None        # ZENDESK or SERVICENOW
    service_name: str | None = None
    priority: str | None = None      # P1, P2 or P3
    subject: str | None = None
    p1_at: float | None = None          # the p1_tickets record's Kafka timestamp (lab 5.1)
    verdict: str | None = None          # ticket_triage.severity: HIGH, LOW or INVALID (lab 5.2)
    verdict_reason: str | None = None
    classified_at: float | None = None  # the ticket_triage record's Kafka timestamp
    true_severity: str | None = None    # ticket_answer_key: HIGH or LOW
    expected_action: str | None = None
    action: str | None = None           # the ticket_responder decision (lab 5.3)
    action_reason: str | None = None
    answered_at: float | None = None    # the decision's Kafka timestamp


@dataclass
class ServiceState:
    name: str
    previous_version: str | None = None
    last_change: str | None = None
    deployed_at: float | None = None
    health: list[HealthRow] = field(default_factory=list)
    error_rates: list[ErrorRateRow] = field(default_factory=list)
    deploys: list[DeployMark] = field(default_factory=list)  # by at
    incidents: list[IncidentEvent] = field(default_factory=list)
    decisions: list[DecisionMark] = field(default_factory=list)
    samples: list[MetricSample] = field(default_factory=list)  # by event_time


@dataclass
class _Block:
    """One run as the snapshot draws it."""
    trigger: Trigger
    start: float                  # detected_at of the run's first window
    first_window_end: float
    decisions: list[DecisionMark]  # every decision for the run's windows, by write time
    incident: IncidentEvent | None = None

    @property
    def closer(self) -> DecisionMark | None:
        return self.decisions[0] if self.decisions else None

    @property
    def end(self) -> float | None:
        """The write time of the first decision, never before the block opened."""
        return max(self.start, self.closer.at) if self.closer else None


class OpsWallState:
    """Thread-safe collector + snapshotter for the dashboard."""

    def __init__(self, axis_seconds: int = AXIS_SECONDS) -> None:
        self.axis_seconds = axis_seconds
        self._lock = threading.RLock()
        self._services = _seeded_services()
        self._tickets: dict[str, Ticket] = {}  # by ticket_id
        self._connection_errors: dict[str, dict] = {}  # topic -> {code, detail, ts}

    # ---- writes ---------------------------------------------------------------

    def apply_deployment(self, record: dict) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        with self._lock:
            s = self._services[name]
            s.previous_version = record.get("previous_version") or s.previous_version
            s.last_change = record.get("last_change") or s.last_change
            if record.get("deployed_at") is not None:
                s.deployed_at = _to_seconds(record["deployed_at"])
            # A version change is a mark, once: the heartbeat repeats the record every 5 s.
            at = _to_seconds(record.get("deployed_at"))
            change = record.get("last_change")
            if change in DEPLOY_CHANGES and at is not None and all(_ms(d.at) != _ms(at) for d in s.deploys):
                s.deploys.append(DeployMark(at, change, record.get("version"), record.get("previous_version")))
                s.deploys.sort(key=lambda d: d.at)

    def apply_health(self, record: dict) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        w = _to_seconds(record.get("window_end"))
        if w is None:
            return
        row = HealthRow(
            window_end=w,
            latency_status=record.get("latency_status") or "NORMAL",
            traffic_status=record.get("traffic_status") or "NORMAL",
            detected_at=_to_seconds(record.get("detected_at")),
        )
        with self._lock:
            s = self._services[name]
            # One row per window (the consumer may replay; an INSERT run twice writes every
            # window twice). The newer record wins, but the window keeps the first moment
            # it was detected.
            for i, prior in enumerate(s.health):
                if prior.window_end == w:
                    row.detected_at = _earliest(prior.detected_at, row.detected_at)
                    s.health[i] = row
                    return
            s.health.append(row)

    def apply_error_rate(self, record: dict) -> None:
        """One ``service_error_rate`` row (lab 2.2, D-067): kept like a health row, one per
        window, the newer status winning and the first detection kept."""
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        w = _to_seconds(record.get("window_end"))
        if w is None:
            return
        row = ErrorRateRow(
            window_end=w,
            error_status=record.get("error_status") or "NORMAL",
            detected_at=_to_seconds(record.get("detected_at")),
        )
        with self._lock:
            rows = self._services[name].error_rates
            for i, prior in enumerate(rows):
                if prior.window_end == w:
                    row.detected_at = _earliest(prior.detected_at, row.detected_at)
                    rows[i] = row
                    return
            rows.append(row)

    def apply_decision(self, record: dict, kafka_ts: float | None = None) -> None:
        if record.get("ticket_id"):  # the responder's: it never touches a metric block (D-068)
            with self._lock:
                self._apply_ticket_decision(record, kafka_ts)
            return
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        we = _to_seconds(record.get("window_end"))
        at = kafka_ts if kafka_ts is not None else we
        if at is None:
            return
        agent = record.get("agent_name", "")
        mark = _decision(record, we, at, agent)
        with self._lock:
            s = self._services[name]
            # One decision per window and agent. The newer record wins, but the answer keeps
            # the moment it was first written.
            for i, d in enumerate(s.decisions):
                if d.window_end == mark.window_end and d.agent == agent:
                    mark.at = min(d.at, mark.at)
                    s.decisions[i] = mark
                    return
            s.decisions.append(mark)

    def apply_incident(self, record: dict) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        iid = record.get("incident_id")
        if not iid:
            return
        with self._lock:
            s = self._services[name]
            ev = IncidentEvent(
                event_time=_to_seconds(record.get("event_time")) or 0.0,
                incident_id=iid,
                incident_type=record.get("incident_type") or "UNKNOWN",
                started_at=_to_seconds(record.get("started_at")) or 0.0,
                ended_at=_to_seconds(record.get("ended_at")),
                expected_trigger=record.get("expected_trigger") or "",
                expected_action=record.get("expected_action") or "",
            )
            for i, prior in enumerate(s.incidents):
                if prior.incident_id == iid:
                    # End record arriving after start; keep the later ended_at.
                    s.incidents[i] = ev if ev.ended_at is not None else prior
                    return
            s.incidents.append(ev)

    def apply_metric(self, record: dict) -> None:
        """One ``service_metrics`` sample. Samples stay in ``event_time`` order (a late one is
        put in its place; the same second twice keeps the newer)."""
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        t = _to_seconds(record.get("event_time"))
        traffic, latency = record.get("traffic_count"), record.get("latency_ms")
        if t is None or traffic is None or latency is None:
            return
        sample = MetricSample(t, traffic, latency, record.get("error_count") or 0)
        with self._lock:
            samples = self._services[name].samples
            if not samples or t > samples[-1].event_time:
                samples.append(sample)
            else:
                i = bisect.bisect_left(samples, t, key=lambda m: m.event_time)
                if i < len(samples) and samples[i].event_time == t:
                    samples[i] = sample
                else:
                    samples.insert(i, sample)

    # A ticket (D-068) fills in from five topics, in whatever order their records arrive. As
    # for decisions, a record written twice keeps its first Kafka timestamp and the newer
    # content (D-063). A record without a timestamp falls back to the ticket's event_time.

    def apply_ticket(self, record: dict) -> None:
        """One ``support_tickets`` record: the ticket's fields."""
        with self._lock:
            ticket = self._ticket(record)
            if ticket is not None:
                _fill_ticket(ticket, record)

    def apply_p1_ticket(self, record: dict, kafka_ts: float | None = None) -> None:
        """One ``p1_tickets`` record (lab 5.1): the moment the classifier's run starts."""
        with self._lock:
            ticket = self._ticket(record)
            if ticket is not None:
                _fill_ticket(ticket, record)
                ticket.p1_at = _earliest(ticket.p1_at, _written(kafka_ts, ticket))

    def apply_triage(self, record: dict, kafka_ts: float | None = None) -> None:
        """One ``ticket_triage`` record (lab 5.2): the classifier's verdict and reason."""
        with self._lock:
            ticket = self._ticket(record)
            if ticket is not None:
                _fill_ticket(ticket, record)
                ticket.verdict = record.get("severity") or "INVALID"
                ticket.verdict_reason = record.get("reason")
                ticket.classified_at = _earliest(ticket.classified_at, _written(kafka_ts, ticket))

    def apply_answer_key(self, record: dict) -> None:
        """One ``ticket_answer_key`` record: what the ticket really is, for the Issues panel."""
        with self._lock:
            ticket = self._ticket(record)
            if ticket is not None:
                if ticket.event_time is None:
                    ticket.event_time = _to_seconds(record.get("event_time"))
                ticket.true_severity = record.get("true_severity")
                ticket.expected_action = record.get("expected_action")

    def _apply_ticket_decision(self, record: dict, kafka_ts: float | None) -> None:
        """A ``remediation_decisions`` record with a ticket_id (lab 5.3): the responder's
        answer, kept on its ticket and nowhere else. Call under the lock."""
        ticket = self._ticket(record)
        if ticket.event_time is None:  # window_end carries the ticket's event_time
            ticket.event_time = _to_seconds(record.get("window_end"))
        ticket.action = record.get("action") or "INVALID"
        ticket.action_reason = record.get("reason")
        ticket.answered_at = _earliest(ticket.answered_at, _written(kafka_ts, ticket))

    def _ticket(self, record: dict) -> Ticket | None:
        """The ticket a record is about, made on its first record. Call under the lock."""
        tid = record.get("ticket_id")
        if not tid:
            return None
        return self._tickets.setdefault(tid, Ticket(tid))

    def record_error(self, topic: str, code: str, detail: str) -> None:
        with self._lock:
            self._connection_errors[topic] = {"code": code, "detail": detail, "ts": time.time()}

    def clear_error(self, topic: str) -> None:
        with self._lock:
            self._connection_errors.pop(topic, None)

    # ---- housekeeping ---------------------------------------------------------

    def trim(self, now: float) -> None:
        """Drop entries whose anchor time is older than ``now - axis_seconds - margin``: an
        incident by its end, or by its start while it is open (an incident whose end record
        never came leaves the wall here). The feed calls this every 10 s."""
        cutoff = now - self.axis_seconds - RETAIN_MARGIN_SECONDS
        with self._lock:
            for s in self._services.values():
                s.health = [h for h in s.health if h.window_end >= cutoff]
                s.error_rates = [r for r in s.error_rates if r.window_end >= cutoff]
                s.deploys = [d for d in s.deploys if d.at >= cutoff]
                s.samples = [m for m in s.samples if m.event_time >= cutoff]
                s.decisions = [d for d in s.decisions if d.at >= cutoff]
                s.incidents = [i for i in s.incidents
                               if (i.ended_at or i.started_at) >= cutoff]
            # A ticket goes once the last thing that happened to it is that old.
            self._tickets = {tid: t for tid, t in self._tickets.items() if _ticket_last(t) >= cutoff}

    # ---- snapshot -------------------------------------------------------------

    def snapshot(self, now: float | None = None) -> dict:
        now = now if now is not None else time.time()
        left = now - self.axis_seconds

        def rel(t: float) -> float:
            return max(0.0, min(self.axis_seconds, t - left))

        with self._lock:
            services = []
            for s in self._services.values():
                svc = _snapshot_service(s, now, left, rel)
                if s.name in TICKET_SERVICES:  # its status, Issues numbers, list and timeline
                    svc.update(_ticket_row([t for t in self._tickets.values()
                                            if t.service_name in (None, s.name)], now))
                services.append(svc)
            return {
                "axisSeconds": self.axis_seconds,
                "errorThresholdPct": ERROR_RATE_THRESHOLD_PCT,
                "updatedAt": now,
                "connectionErrors": dict(self._connection_errors),
                "services": services,
            }


# ---- snapshot helpers ---------------------------------------------------------

def _snapshot_service(s: ServiceState, now: float, left: float, rel) -> dict:
    rows = {source: sorted((r for r in getattr(s, source) if r.window_end >= left), key=lambda r: r.window_end)
            for source in ("health", "error_rates")}
    incidents = sorted((i for i in s.incidents if _incident_end(i, now) >= left),
                       key=lambda i: i.started_at)
    blocks = _blocks(rows, s.decisions, now)
    charts = _charts(s.name)
    for b in blocks:
        b.incident = _owner(b, incidents, now)

    status, status_text = _status(blocks, now)
    return {
        "name": s.name,
        # Which kind of row the page builds (D-068); a ticket row's own fields come from
        # _ticket_row, which also replaces its status and its issues.
        "row": "tickets" if s.name in TICKET_SERVICES else "metrics",
        "previousVersion": s.previous_version,
        "lastChange": s.last_change,
        "secondsSinceDeploy": (now - s.deployed_at) if s.deployed_at else None,
        "status": status,
        "statusText": status_text,
        "issues": _incident_issues(incidents, blocks, left, now),  # the page draws it whatever it counts
        "groundTruth": [_incident_json(i, [b for b in blocks if b.incident is i], now, rel)
                        for i in incidents],
        "blocks": [_block_json(b, now, rel) for b in blocks],
        "charts": charts,
        "metrics": _samples_json(s.samples, left, now, errors="errors" in charts),
        "deploys": [_deploy_json(d, rel) for d in s.deploys if d.at >= left] if "errors" in charts else [],
    }


def _charts(name: str) -> list[str]:
    """The charts a row shows, top to bottom (D-067). A ticket service has no metrics, so
    none (D-068)."""
    if name in TICKET_SERVICES:
        return []
    return ["errors"] if name in ERRORS_CHART_SERVICES else ["latency", "traffic"]


def _samples_json(samples: list[MetricSample], left: float, now: float, errors: bool = False) -> list[dict]:
    """The samples on the chart, oldest first: ``t`` is the axis position of ``event_time``.
    A sample stamped after ``now`` (it covers the second still running) waits for the next
    snapshot. Latency is rounded to 0.1 ms, which is all the page shows. With ``errors``,
    ``errorPct`` is the share of the second's requests that failed, in %, to 0.1; a second
    without requests has none (the page leaves a gap)."""
    start = bisect.bisect_left(samples, left, key=lambda m: m.event_time)
    out = []
    for m in samples[start:]:
        if m.event_time > now:
            continue
        sample = {"t": round(m.event_time - left, 3), "traffic": m.traffic_count, "latency": round(m.latency_ms, 1)}
        if errors:
            sample["errorPct"] = round(100 * m.error_count / m.traffic_count, 1) if m.traffic_count else None
        out.append(sample)
    return out


def _deploy_json(d: DeployMark, rel) -> dict:
    return {"key": f"{d.kind}@{_ms(d.at)}", "t": rel(d.at), "kind": d.kind,
            "version": d.version, "previousVersion": d.previous_version}


def _blocks(rows: dict[str, list], decisions: list[DecisionMark], now: float) -> list[_Block]:
    """The blocks still on the wall, by start: answered ones, and unanswered ones until
    ``ANSWER_WAIT_SECONDS`` after their run's last detection. ``rows`` holds each source's
    rows on the chart, by window (``Trigger.source``)."""
    blocks = []
    for trigger in TRIGGERS:
        for run in _runs(rows[trigger.source], trigger):
            if run[0].detected_at is None:
                continue  # a row from before detected_at existed: no block
            windows = {_ms(r.window_end) for r in run}
            answers = sorted((d for d in decisions
                              if d.trigger == trigger.name and _ms(d.window_end) in windows),
                             key=lambda d: d.at)
            last_detected = max(r.detected_at for r in run if r.detected_at is not None)
            if not answers and now - last_detected > ANSWER_WAIT_SECONDS:
                continue
            blocks.append(_Block(trigger, run[0].detected_at, run[0].window_end, answers))
    return sorted(blocks, key=lambda b: b.start)


def _runs(rows: list, trigger: Trigger) -> list[list]:
    """Consecutive rows flagged for ``trigger``; any other row ends a run."""
    runs: list[list] = []
    current: list = []
    for row in rows:
        if getattr(row, trigger.column) == trigger.flag:
            current.append(row)
        elif current:
            runs.append(current)
            current = []
    if current:
        runs.append(current)
    return runs


def _owner(block: _Block, incidents: list[IncidentEvent], now: float) -> IncidentEvent | None:
    """The incident on the block's metric whose span, extended by the grace period,
    overlaps the run's first window."""
    window_start = block.first_window_end - WINDOW_SECONDS
    for inc in incidents:
        if INCIDENT_METRIC.get(inc.incident_type) != block.trigger.metric:
            continue
        if window_start < _incident_end(inc, now) + OWNERSHIP_GRACE_SECONDS \
                and block.first_window_end > inc.started_at:
            return inc
    return None


def _verdict(inc: IncidentEvent, owned: list[_Block], now: float) -> str:
    if any(b.closer for b in owned):
        return "answered"
    if inc.ended_at is not None and now - inc.ended_at > MISSED_AFTER_SECONDS:
        return "missed"
    return "pending"


def _incident_json(inc: IncidentEvent, owned: list[_Block], now: float, rel) -> dict:
    first = owned[0] if owned else None
    verdict = _verdict(inc, owned, now)
    return {
        "incidentId": inc.incident_id,
        "type": inc.incident_type,
        "metric": INCIDENT_METRIC.get(inc.incident_type),
        "start": rel(inc.started_at),
        "end": rel(_incident_end(inc, now)),
        "open": inc.ended_at is None,
        "verdict": verdict,
        "detectedAt": rel(first.start) if first else None,
        "timeToDetect": first.start - inc.started_at if first else None,
        "timeToAnswer": first.end - first.start if first and first.closer else None,
    }


def _issues_json(injected: int, detected: int, missed: int,
                 to_detect: list[float], to_remediate: list[float]) -> dict:
    """A service's Issues panel (D-068): the counts, and the medians in seconds (None
    with nothing to take one over)."""
    return {
        "injected": injected,
        "detected": detected,
        "missed": missed,
        "medianTimeToDetect": _median(to_detect),
        "medianTimeToRemediate": _median(to_remediate),
    }


def _incident_issues(incidents: list[IncidentEvent], blocks: list[_Block], left: float, now: float) -> dict:
    """The Issues panel of a metric service: the incidents that started in the last 10 minutes
    (one cut by the chart's left edge started earlier), and their blocks."""
    recent = [inc for inc in incidents if inc.started_at >= left]
    detected = missed = 0
    to_detect, to_remediate = [], []
    for inc in recent:
        owned = [b for b in blocks if b.incident is inc]
        verdict = _verdict(inc, owned, now)
        if verdict == "missed":
            missed += 1  # even while a block of its run is still drawn: nobody answered
        elif owned:
            detected += 1
        if verdict == "answered":
            to_detect.append(owned[0].start - inc.started_at)
            to_remediate.append(min(b.end for b in owned if b.closer) - inc.started_at)
    return _issues_json(len(recent), detected, missed, to_detect, to_remediate)


# ---- a ticket row (D-068) ---------------------------------------------------------

LIST_LENGTH = 5           # the newest tickets the list shows
RECEIVED_SECONDS = 600.0  # "N received" counts the tickets created this long ago or since
TIMELINE_SECONDS = 120    # "Tickets and agent runs" covers the last 2 minutes


def _ticket_row(tickets: list[Ticket], now: float) -> dict:
    """What a ticket service's row shows beyond the shared fields: the list, the timeline,
    and its status and Issues numbers in place of the metric ones. Timeline positions are
    seconds from its left edge, ``now - TIMELINE_SECONDS``."""
    left = now - TIMELINE_SECONDS
    listed = sorted((t for t in tickets if t.event_time is not None and t.priority is not None),
                    key=lambda t: t.event_time, reverse=True)
    runs = sorted((r for t in tickets for r in _agent_runs(t, now) if r.end >= left), key=lambda r: r.start)
    working = [r for r in runs if r.open]
    return {
        "status": "working" if working else "healthy",
        "statusText": "Agent resolving P1" if working else "Healthy",
        "openP1": len({r.ticket.ticket_id for r in working}),
        "agentsWorking": len({r.lane for r in working}),
        "timelineSeconds": TIMELINE_SECONDS,
        "runs": [_run_json(r, left) for r in runs],
        "marks": [{"id": t.ticket_id, "priority": t.priority, "t": t.event_time - left}
                  for t in reversed(listed) if left <= t.event_time <= now],
        "issues": _ticket_issues(tickets, now),
        "tickets": {
            "received": sum(1 for t in listed if now - RECEIVED_SECONDS <= t.event_time <= now),
            "newest": [{"id": t.ticket_id, "source": t.source, "priority": t.priority, "subject": t.subject,
                        "eventTime": t.event_time} for t in listed[:LIST_LENGTH]],
        },
    }


def _ticket_issues(tickets: list[Ticket], now: float) -> dict:
    """A ticket service's Issues panel (spec decision 36), over the tickets created in the
    last 10 minutes: injected, the P1s whose answer key says HIGH; detected, those the
    classifier called HIGH; missed, those it called LOW or INVALID, or hasn't called 30 s
    after the ticket; the median ticket -> HIGH verdict, and ticket -> the responder's
    action, whatever it is. A LOW ticket called HIGH, and a wrong action, are evaluate's."""
    injected = [t for t in tickets if t.priority == "P1" and t.true_severity == "HIGH"
                and t.event_time is not None and now - RECEIVED_SECONDS <= t.event_time <= now]
    detected = [t for t in injected if t.verdict == "HIGH"]
    missed = [t for t in injected if t.verdict in ("LOW", "INVALID")
              or (t.verdict is None and now - t.event_time > RUN_WAIT_SECONDS)]
    return _issues_json(
        len(injected), len(detected), len(missed),
        [t.classified_at - t.event_time for t in detected if t.classified_at is not None],
        [t.answered_at - t.event_time for t in injected if t.answered_at is not None],
    )


@dataclass
class _Run:
    """One agent's work on one ticket, as the timeline draws it (spec decision 34)."""
    lane: str             # "classifier" or "responder"
    ticket: Ticket
    start: float
    end: float            # the answer; now while open; RUN_WAIT_SECONDS after the start with no answer
    open: bool
    label: str
    outcome: str | None   # the verdict or the action, in words; None with no answer
    reason: str | None


RUN_WAIT_SECONDS = 30.0  # a run nobody answered ends this long after it started


def _agent_runs(ticket: Ticket, now: float) -> list[_Run]:
    """The classifier's run, from the p1_tickets record to the verdict, and for a HIGH
    verdict the responder's, from the verdict to the decision."""
    runs = []
    if ticket.p1_at is not None:
        runs.append(_run("classifier", ticket, ticket.p1_at, ticket.classified_at,
                         _words(ticket.verdict, VERDICT_WORDS), ticket.verdict_reason, now))
    if ticket.verdict == "HIGH" and ticket.classified_at is not None:
        runs.append(_run("responder", ticket, ticket.classified_at, ticket.answered_at,
                         _words(ticket.action, ACTION_WORDS), ticket.action_reason, now))
    return runs


def _run(lane: str, ticket: Ticket, start: float, answered: float | None, outcome: str | None,
         reason: str | None, now: float) -> _Run:
    """An answered run reads its outcome and how long it took ("High in 4s"); an unanswered
    one is open and reads "Working 3s", until it gives up and reads "No answer"."""
    if answered is not None:
        end = max(start, answered)
        return _Run(lane, ticket, start, end, False, f"{outcome} in {_dur_words(end - start)}", outcome, reason)
    if now - start <= RUN_WAIT_SECONDS:
        return _Run(lane, ticket, start, now, True, f"Working {_dur_words(now - start)}", None, None)
    return _Run(lane, ticket, start, start + RUN_WAIT_SECONDS, False, "No answer", None, None)


VERDICT_WORDS = {"HIGH": "High", "LOW": "Low", "INVALID": "Invalid"}
ACTION_WORDS = {"ROLLBACK": "Roll back", "SCALE_OUT": "Scale out",
                "INVESTIGATE_UPSTREAM": "Investigate upstream", "INVALID": "Invalid"}


def _words(code: str | None, words: dict[str, str]) -> str | None:
    if code is None:
        return None
    return words.get(code) or code.replace("_", " ").capitalize()


def _run_json(run: _Run, left: float) -> dict:
    def pos(t: float) -> float:
        return max(0.0, min(TIMELINE_SECONDS, t - left))

    return {
        "key": f"{run.lane}@{run.ticket.ticket_id}",  # who the bar is from one snapshot to the next (D-062)
        "lane": run.lane,
        "ticketId": run.ticket.ticket_id,
        "priority": run.ticket.priority,
        "subject": run.ticket.subject,
        "start": pos(run.start),
        "end": pos(run.end),
        "open": run.open,
        "label": run.label,
        "outcome": run.outcome,
        "reason": run.reason,
    }


def _dur_words(sec: float) -> str:
    """A duration as the design writes it: "6s", "1m 05s"."""
    sec = max(0, round(sec))
    return f"{sec}s" if sec < 60 else f"{sec // 60}m {sec % 60:02d}s"


def _ticket_last(ticket: Ticket) -> float:
    """The latest moment known about a ticket, the anchor ``trim()`` drops it by."""
    times = (ticket.event_time, ticket.p1_at, ticket.classified_at, ticket.answered_at)
    return max((t for t in times if t is not None), default=0.0)


def _written(kafka_ts: float | None, ticket: Ticket) -> float | None:
    """When a ticket's record was written: its Kafka timestamp, else the ticket's creation."""
    return kafka_ts if kafka_ts is not None else ticket.event_time


def _fill_ticket(ticket: Ticket, record: dict) -> None:
    """The ticket's own fields from any record that carries them (support_tickets, and
    p1_tickets and ticket_triage, which copy them); a field already known stays."""
    if ticket.event_time is None:
        ticket.event_time = _to_seconds(record.get("event_time"))
    for name in ("source", "service_name", "priority", "subject"):
        if getattr(ticket, name) is None and record.get(name):
            setattr(ticket, name, record[name])


def _block_json(b: _Block, now: float, rel) -> dict:
    closer = b.closer
    return {
        # Who the block is from one snapshot to the next, so the page keeps its node (D-062).
        "key": f"{b.trigger.name}@{_ms(b.first_window_end)}",
        "start": rel(b.start),
        "end": rel(b.end if closer else now),
        "open": closer is None,
        "metric": b.trigger.metric,
        "direction": b.trigger.direction,
        "trigger": b.trigger.name,
        "incidentId": b.incident.incident_id if b.incident else None,
        "action": closer.action if closer else None,
        "target": closer.target if closer else None,
        "reason": closer.reason if closer else None,
        "decisions": dict(Counter(d.action for d in b.decisions)),
        "decisionCount": len(b.decisions),
        "timeToDetect": b.start - b.incident.started_at if b.incident else None,
        "timeToAnswer": b.end - b.start if closer else None,
    }


def _status(blocks: list[_Block], now: float) -> tuple[str, str]:
    waiting = [t.words for t in TRIGGERS if any(b.trigger is t and not b.closer for b in blocks)]
    if waiting:
        return "anomaly", f"Anomaly detected: {' and '.join(waiting)}, waiting for the agent"
    fixes = [d for b in blocks for d in b.decisions
             if d.action in FIX_ACTIONS and now - d.at <= FIX_RECENT_SECONDS]
    if fixes:
        d = max(fixes, key=lambda d: d.at)
        label = {
            "ROLLBACK": f"Rolled back to v{d.target}" if d.target else "Rolled back",
            "SCALE_OUT": f"Scaled out to {d.target} replicas" if d.target else "Scaled out",
            "INVESTIGATE_UPSTREAM": "Traced to upstream",
        }[d.action]
        return "remediated", label
    return "healthy", "Healthy"


# ---- helpers ------------------------------------------------------------------

def _seeded_services() -> dict[str, ServiceState]:
    """Every service, so the wall shows its rows before the first record arrives."""
    return {name: ServiceState(name=name) for name in SERVICES}


def _to_seconds(value) -> float | None:
    """Epoch seconds from a record's timestamp, which the Avro decoder hands over as a
    `datetime`: naive for Flink's TIMESTAMP(3) (registered as local-timestamp-millis, read
    as UTC wall-clock, D-048), aware for a TIMESTAMP_LTZ. Anything else is no time."""
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _decision(record: dict, window_end: float | None, at: float, agent: str) -> DecisionMark:
    return DecisionMark(
        at=at,
        window_end=window_end or at,
        action=record.get("action") or "INVALID",
        trigger=record.get("trigger") or "",
        target=record.get("target"),
        reason=record.get("reason"),
        agent=agent,
    )


def _incident_end(inc: IncidentEvent, now: float) -> float:
    return inc.ended_at if inc.ended_at is not None else now


def _earliest(a: float | None, b: float | None) -> float | None:
    return min(a, b) if a is not None and b is not None else (a if b is None else b)


def _ms(t: float) -> int:
    """A window time as a match key: whole milliseconds, so float noise never splits a
    window."""
    return round(t * 1000)


def _median(values) -> float | None:
    xs = [v for v in values if v is not None]
    return statistics.median(xs) if xs else None
