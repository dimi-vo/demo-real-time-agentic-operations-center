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
  ``service_error_rate``, D-067), counting only windows whose ``window_end`` is in the
  history (the last 10 minutes). One window without that flag ends the run.
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
  time to detect and the median time to remediate (start → the first block's answer, time to
  detect plus time to answer; while that block waits unanswered, start → the first answer),
  both over answered incidents. A ticket service has its own, counted from tickets (below);
  the page draws ``issues`` whatever it was counted from, the medians in the words it shows
  ("6s", "1m 05s", "—" with none). There is no global summary.
- The **samples** are the raw ``service_metrics`` records, one per metric service per
  second (D-058, D-068), kept for the history plus a margin (``trim()``). The snapshot
  carries the ones on the chart; the page draws them as the latency and traffic lines
  (D-061), and, for a row with the errors chart, the share of each second's requests
  that failed (null for a second without requests, D-067).
- The **charts** of a row: web-gateway's is the errors chart alone (lab 2.2's threshold,
  D-067), payments' are latency and traffic, and checkout, which produces tickets and no
  metrics, has none (D-068). A metric row also gets a **deploy mark** per DEPLOY and
  ROLLBACK of its service, good deploys included, keyed by ``deployed_at`` so the 5-s
  heartbeat adds nothing.
- The **chart** of every row shows the last 2 minutes (``CHART_SECONDS``, the design's, D-068);
  what has left it still counts in the Issues panel and the status, worked out over the
  history.
- A **releases** row (web-gateway, the design's deploy row, D-068) also carries its running
  version (the latest ``service_deployments`` record's, heartbeat included) and its Releases
  list: the newest 5 DEPLOYs of the last 10 minutes, each with how long it ran before the
  ROLLBACK that left its version, and their count.
- A **windows** row (payments, the design's aggregate row, D-068) also carries its window (10 s,
  Flink's) and its "Aggregated records": the newest 7 ``service_health`` windows, each with its
  whole record (one per window, the newer record winning, the first detection kept) and the
  status columns that flag it, and the count of flagged windows of the last 10 minutes.
- The **row kind** (``row``): ``"tickets"`` for the ticket services of ``opsgen.services``
  (checkout), ``"releases"`` for a row with the errors chart (web-gateway), ``"windows"`` for
  the others (payments), so the page holds no service names (D-068). Each kind carries only
  what the page draws for it: a metric row its charts, samples, deploys, blocks and
  incidents, a releases row its version and Releases list as well, a windows row its window
  and newest windows; a ticket row its sources, list and timeline. The words the page shows
  come with the snapshot too: ``actionWords`` for every action, a ticket source's name, a
  lane's state.
- A **ticket** is one record per ``ticket_id``, filled in from five topics in any order: its
  fields (``support_tickets``), when lab 5.1 routed it (the ``p1_tickets`` record's Kafka
  timestamp), the classifier's verdict and reason and when it gave them (``ticket_triage``),
  its true severity and expected action (``ticket_answer_key``), and the responder's action
  and reason and when it gave them (a ``remediation_decisions`` record with a ``ticket_id``).
  Such a decision goes to its ticket and nowhere else: it never closes a metric block,
  counts as a false alarm or sets a metric row's status.
- A ticket row's **runs** (the chart's last 2 minutes): the classifier's, from the ``p1_tickets``
  record to the verdict ("High in 4s"), and for a HIGH verdict the responder's, from the
  verdict to the decision ("Scale out in 6s"). A run with no answer is open ("Working 3s")
  until 30 s after it started, then ends there ("No answer"). The row is "Agent resolving
  P1" while a run is open; ``openP1`` counts the open runs' tickets, ``agentsWorking`` their
  lanes, each lane says whether it is Working or Idle, and ``agentsText`` sums them up ("Both
  agents idle", "1 of 2 agents working"). Its **Issues panel** counts the really-HIGH P1
  tickets created in the last 10 minutes: detected (a HIGH verdict), missed (LOW, INVALID, or
  no verdict 30 s after the ticket), ticket -> verdict, ticket -> action. Its list is the
  newest 5 tickets, each with its source's name, and the count of the last 10 minutes; its
  marks are the tickets of the last 2 minutes.
- The **"Code security incidents" row** (``row: "security"``, D-070) comes after the services'
  rows. It is no service, so it is built by its own function, not from ``opsgen.services``. A
  **finding** is one record per ``finding_id``, filled in from four topics in any order, as a
  ticket is: its fields (``cve_scans``), when lab 6.1 routed it (the ``critical_cves`` record's
  Kafka timestamp), what it should get (``cve_answer_key``), and the security agent's decision
  with the patch agent's commit, start and finish and the reply they came in (a
  ``patch_decisions`` record, lab 6.4, and its Kafka timestamp). ``patch_decisions`` never goes
  through ``apply_decision``: nothing in ``remediation_decisions``' routing changes.
- Its **runs**: the security agent's, from the ``critical_cves`` record (the finding's
  ``event_time`` if only the decision has come) to the decision: "Patched in 12s", "Dismissed
  in 8s", "Patch agent unreachable" (``FAILED``) or "Invalid answer" (``INVALID``, which is
  also what a stopped patch agent gives: Flink fails at the agent card before the model
  answers, issue 02). Open ("Working 3s") until 60 s after it started, then "No answer". The
  external lane's, from the patch's start to its finish, once the decision has brought both
  (spec decision 24: the external bar appears when our agent answers, not while the patch
  runs); our run carries the part of its bar that span covers, the hatch. The row is "Agent on
  critical CVE" while a run of ours is open, and ``openCriticals`` counts their findings. Its
  **Issues panel** scores the critical findings reported in the last 10 minutes that have an
  answer key: right (the action expected), missed (another action, ``INVALID``, or no decision
  60 s after lab 6.1's record), the median finding -> decision, and finding -> the patch's
  finish for the ``PATCHED`` ones. Its list is the newest 5 findings and the count of the last
  10 minutes; its marks are the findings of the last 2 minutes, critical or not.

State stores absolute epoch seconds everywhere. The snapshot turns positions into the
axis-relative seconds the page draws with: ``t - (now - CHART_SECONDS)``, clipped to
``[0, CHART_SECONDS]``. Durations stay plain seconds; the Releases list's dates stay absolute.
"""

from __future__ import annotations

import bisect
import re
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opsgen.services import SERVICES, TICKET_SERVICES
from opsgen.tickets import HIGH, LOW, SOURCES, SOURCES_BY_CODE

HISTORY_SECONDS = 600  # what the state keeps and the Issues panel counts over: the last 10 minutes
CHART_SECONDS = 120    # every row's chart, checkout's timeline included: the last 2 minutes (D-068)
WINDOW_SECONDS = 10.0
ANSWER_WAIT_SECONDS = 30.0      # an unanswered block goes this long after its run's last detection
OWNERSHIP_GRACE_SECONDS = 30.0  # a run starting this long after its incident ended still belongs to it
MISSED_AFTER_SECONDS = 30.0     # an incident nobody answered is missed this long after it ended
FIX_RECENT_SECONDS = 120.0      # the tile shows a fix this long
FIX_ACTIONS = ("ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM")
# Keep this long past the history: a run straddling its start stays whole, and a sample stays
# until well after it left.
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
# The rows whose only chart is the errors chart: the services lab 2.2 reads (D-067). Each is a
# "releases" row: the design's deploy row, with its running version and Releases list (D-068).
ERRORS_CHART_SERVICES = ("web-gateway",)
RELEASES_LISTED = 5  # the newest deploys a Releases list shows
WINDOWS_LISTED = 7   # the newest service_health windows a windows row's "Aggregated records" shows
# The service_health column each status column judges: a flagged window's line marks both.
JUDGED = {"latency_status": "avg_latency_ms", "traffic_status": "traffic_count"}
# Lab 2.2's rule (sql/2.2_error_rate.sql), drawn as a line on the errors chart; a test keeps
# the two in step.
ERROR_RATE_THRESHOLD_PCT = 25.0
DEPLOY_CHANGES = ("DEPLOY", "ROLLBACK")
INVALID = "INVALID"  # what the labs' INSERTs write for an answer they can't parse
CRITICAL_CVSS = 9.0  # lab 6.1's rule (sql/6.1_critical_cves.sql), CVSS's Critical band; a test keeps them in step


@dataclass
class HealthRow:
    window_end: float  # epoch seconds
    latency_status: str
    traffic_status: str
    detected_at: float | None = None  # when the detector produced the verdict; None on older rows
    record: dict = field(default_factory=dict)  # every column, times in epoch seconds: the record line


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
class Finding:
    """One CVE scan finding, filled in from whichever of its records have arrived (D-070)."""
    finding_id: str
    event_time: float | None = None  # when the scanner reported it
    repository: str | None = None
    cve_id: str | None = None
    package: str | None = None
    installed_version: str | None = None
    fixed_version: str | None = None
    cvss_score: float | None = None
    finding: str | None = None             # the scanner's sentence
    critical_at: float | None = None       # the critical_cves record's Kafka timestamp (lab 6.1)
    expected_action: str | None = None     # cve_answer_key: PATCH or DISMISS; None when not critical
    action: str | None = None              # the security agent's (lab 6.4): PATCH, DISMISS or INVALID
    reason: str | None = None
    patch_status: str | None = None        # PATCHED, FAILED or N/A, worked out by lab 6.4
    commit_sha: str | None = None
    patch_started_at: float | None = None  # the patch's start and finish, from the patch agent's reply
    patch_finished_at: float | None = None
    reply: str | None = None               # the answer's Patch line: the patch agent's reply, as copied
    decided_at: float | None = None        # the patch_decisions record's Kafka timestamp

    @property
    def critical(self) -> bool:
        return self.cvss_score is not None and self.cvss_score >= CRITICAL_CVSS


@dataclass
class ServiceState:
    name: str
    version: str | None = None  # the running version, from the latest service_deployments record
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

    def __init__(self, history_seconds: int = HISTORY_SECONDS) -> None:
        self.history_seconds = history_seconds
        self._lock = threading.RLock()
        self._services = _seeded_services()
        self._tickets: dict[str, Ticket] = {}  # by ticket_id
        self._findings: dict[str, Finding] = {}  # by finding_id (D-070)
        self._connection_errors: dict[str, dict] = {}  # topic -> {code, detail, ts}

    # ---- writes ---------------------------------------------------------------

    def apply_deployment(self, record: dict) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        with self._lock:
            s = self._services[name]
            s.version = record.get("version") or s.version
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
            record={k: _to_seconds(v) if isinstance(v, datetime) else v for k, v in record.items()},
        )
        with self._lock:
            s = self._services[name]
            # One row per window (the consumer may replay; an INSERT run twice writes every
            # window twice). The newer record wins, but the window keeps the first moment
            # it was detected.
            for i, prior in enumerate(s.health):
                if prior.window_end == w:
                    row.detected_at = _earliest(prior.detected_at, row.detected_at)
                    if "detected_at" in row.record:
                        row.record["detected_at"] = row.detected_at
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
                ticket.verdict = record.get("severity") or INVALID
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
        ticket.action = record.get("action") or INVALID
        ticket.action_reason = record.get("reason")
        ticket.answered_at = _earliest(ticket.answered_at, _written(kafka_ts, ticket))

    def _ticket(self, record: dict) -> Ticket | None:
        """The ticket a record is about, made on its first record. Call under the lock."""
        tid = record.get("ticket_id")
        if not tid:
            return None
        return self._tickets.setdefault(tid, Ticket(tid))

    # A finding (D-070) fills in from four topics the same way: its fields from the first record
    # that carries them, a record written twice keeping its first Kafka timestamp and the newer
    # content, and a record without a timestamp falling back to the finding's event_time.

    def apply_finding(self, record: dict) -> None:
        """One ``cve_scans`` record: the finding's fields."""
        with self._lock:
            finding = self._finding(record)
            if finding is not None:
                _fill_finding(finding, record)

    def apply_critical_cve(self, record: dict, kafka_ts: float | None = None) -> None:
        """One ``critical_cves`` record (lab 6.1): the moment the security agent's run starts."""
        with self._lock:
            finding = self._finding(record)
            if finding is not None:
                _fill_finding(finding, record)
                finding.critical_at = _earliest(finding.critical_at, _written(kafka_ts, finding))

    def apply_cve_answer_key(self, record: dict) -> None:
        """One ``cve_answer_key`` record: what the finding should get, for the Issues panel."""
        with self._lock:
            finding = self._finding(record)
            if finding is not None:
                if finding.event_time is None:
                    finding.event_time = _to_seconds(record.get("event_time"))
                finding.expected_action = record.get("expected_action")

    def apply_patch_decision(self, record: dict, kafka_ts: float | None = None) -> None:
        """One ``patch_decisions`` record (lab 6.4): the security agent's decision and the
        patch agent's answer, kept on its finding. It is no ``remediation_decisions`` record and
        never goes near ``apply_decision``."""
        with self._lock:
            finding = self._finding(record)
            if finding is not None:
                _fill_finding(finding, record)
                finding.action = record.get("action") or INVALID
                finding.reason = record.get("reason")
                finding.patch_status = record.get("patch_status")
                finding.commit_sha = record.get("commit_sha")
                finding.patch_started_at = _to_seconds(record.get("patch_started_at"))
                finding.patch_finished_at = _to_seconds(record.get("patch_finished_at"))
                finding.reply = _patch_line(record.get("raw_response"))
                finding.decided_at = _earliest(finding.decided_at, _written(kafka_ts, finding))

    def _finding(self, record: dict) -> Finding | None:
        """The finding a record is about, made on its first record. Call under the lock."""
        fid = record.get("finding_id")
        if not fid:
            return None
        return self._findings.setdefault(fid, Finding(fid))

    def record_error(self, topic: str, code: str, detail: str) -> None:
        with self._lock:
            self._connection_errors[topic] = {"code": code, "detail": detail, "ts": time.time()}

    def clear_error(self, topic: str) -> None:
        with self._lock:
            self._connection_errors.pop(topic, None)

    # ---- housekeeping ---------------------------------------------------------

    def trim(self, now: float) -> None:
        """Drop entries whose anchor time is older than ``now - history_seconds - margin``: an
        incident by its end, or by its start while it is open (an incident whose end record
        never came leaves the wall here). The feed calls this every 10 s."""
        cutoff = now - self.history_seconds - RETAIN_MARGIN_SECONDS
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
            self._findings = {fid: f for fid, f in self._findings.items() if _finding_last(f) >= cutoff}

    # ---- snapshot -------------------------------------------------------------

    def snapshot(self, now: float | None = None) -> dict:
        now = now if now is not None else time.time()
        left = now - self.history_seconds
        chart_left = now - CHART_SECONDS

        def rel(t: float) -> float:
            return max(0.0, min(CHART_SECONDS, t - chart_left))

        with self._lock:
            services = []
            for s in self._services.values():
                if s.name in TICKET_SERVICES:
                    tickets = [t for t in self._tickets.values() if t.service_name in (None, s.name)]
                    services.append(_ticket_row(s.name, tickets, now))
                else:
                    services.append(_metric_row(s, now, left, chart_left, rel))
            services.append(_security_row(list(self._findings.values()), now))  # no service: beside them (D-070)
            return {
                "chartSeconds": CHART_SECONDS,  # every row's time axis
                "errorThresholdPct": ERROR_RATE_THRESHOLD_PCT,
                "actionWords": dict(ACTION_WORDS),  # how the page names an action, in every tooltip
                "updatedAt": now,
                "connectionErrors": dict(self._connection_errors),
                "services": services,
            }


# ---- snapshot helpers ---------------------------------------------------------

def _metric_row(s: ServiceState, now: float, left: float, chart_left: float, rel) -> dict:
    """A metric service's row: its charts and what they draw, its status and Issues numbers. The
    blocks, the incidents and the status are worked out over the history (``left``), the chart
    shows what reaches its last 2 minutes (``chart_left``). A "releases" row adds its running
    version and its Releases list, a "windows" row its window and its newest windows."""
    rows = {source: sorted((r for r in getattr(s, source) if r.window_end >= left), key=lambda r: r.window_end)
            for source in ("health", "error_rates")}
    incidents = sorted((i for i in s.incidents if _incident_end(i, now) >= left),
                       key=lambda i: i.started_at)
    blocks = _blocks(rows, s.decisions, now)
    charts = _charts(s.name)
    for b in blocks:
        b.incident = _owner(b, incidents, now)

    status, status_text = _status(blocks, now)
    kind = "releases" if s.name in ERRORS_CHART_SERVICES else "windows"
    row = {
        "name": s.name,
        "row": kind,  # which kind of row the page builds (D-068)
        "status": status,
        "statusText": status_text,
        "issues": _incident_issues(incidents, blocks, left, now),  # the page draws it whatever it counts
        "groundTruth": [_incident_json(i, [b for b in blocks if b.incident is i], now, rel)
                        for i in incidents if _incident_end(i, now) >= chart_left],
        "blocks": [_block_json(b, now, rel) for b in blocks if (b.end if b.closer else now) >= chart_left],
        "charts": charts,
        "metrics": _samples_json(s.samples, chart_left, now, errors="errors" in charts),
        "deploys": [_deploy_json(d, rel) for d in s.deploys if d.at >= chart_left],
    }
    if kind == "releases":
        row["version"] = s.version
        row["releases"] = _releases_json(s.deploys, left, now)
    else:
        row["windowSeconds"] = int(WINDOW_SECONDS)
        row["windows"] = _windows_json(s.health, left, now)
    return row


def _charts(name: str) -> list[str]:
    """The charts a metric row shows, top to bottom (D-067)."""
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


def _releases_json(deploys: list[DeployMark], left: float, now: float) -> dict:
    """A Releases list (D-068): the newest DEPLOYs of the last 10 minutes, newest first, and
    their count. A deploy's rollback is the first ROLLBACK after it that leaves its version
    (versions are never reused); the list says how long the deploy ran before it, in seconds
    and in the words the page shows."""
    recent = [d for d in deploys if d.kind == "DEPLOY" and left <= d.at <= now]

    def rolled_back_after(d: DeployMark) -> float | None:
        back = next((r for r in deploys if r.kind == "ROLLBACK" and r.at >= d.at
                     and r.previous_version == d.version), None)
        return back.at - d.at if back else None

    newest = []
    for d in reversed(recent[-RELEASES_LISTED:]):
        after = rolled_back_after(d)
        newest.append({"key": f"{d.kind}@{_ms(d.at)}", "version": d.version, "deployedAt": d.at,
                       "rolledBackAfter": after, "rolledBackAfterText": None if after is None else _dur_words(after)})
    return {"count": len(recent), "newest": newest}


def _windows_json(health: list[HealthRow], left: float, now: float) -> dict:
    """A windows row's "Aggregated records" (D-068): the newest service_health windows, newest
    first, each with its whole record and the columns that flag it: each status column that
    flags it (a latency HIGH, a traffic HIGH or LOW) after the column it judges, which the line
    shows first; and the count of flagged windows of the last 10 minutes."""
    def flags(h: HealthRow) -> list[str]:
        statuses = dict.fromkeys(t.column for t in TRIGGERS if t.source == "health" and getattr(h, t.column) == t.flag)
        return [c for status in statuses for c in (JUDGED[status], status)]

    rows = sorted((h for h in health if h.window_end <= now), key=lambda h: h.window_end)
    return {
        "flagged": sum(1 for h in rows if h.window_end >= left and flags(h)),
        "newest": [{"key": f"W@{_ms(h.window_end)}", "windowEnd": h.window_end, "record": h.record, "flags": flags(h)}
                   for h in reversed(rows[-WINDOWS_LISTED:])],
    }


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
    with nothing to take one over) and in the words the page shows."""
    to_detect_median, to_remediate_median = _median(to_detect), _median(to_remediate)
    return {
        "injected": injected,
        "detected": detected,
        "missed": missed,
        "medianTimeToDetect": to_detect_median,
        "medianTimeToRemediate": to_remediate_median,
        "medianTimeToDetectText": _dur_text(to_detect_median),
        "medianTimeToRemediateText": _dur_text(to_remediate_median),
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
            # Spec decision 29: timeToDetect + timeToAnswer, the first block's start to its answer.
            # While the first block is still drawn unanswered that sum is undefined, so it runs
            # to the first answer of a later block instead.
            first = owned[0]
            answered = first if first.closer else min((b for b in owned if b.closer), key=lambda b: b.end)
            to_detect.append(first.start - inc.started_at)
            to_remediate.append(answered.end - inc.started_at)
    return _issues_json(len(recent), detected, missed, to_detect, to_remediate)


# ---- a ticket row (D-068) ---------------------------------------------------------

LIST_LENGTH = 5           # the newest tickets the list shows
RECEIVED_SECONDS = 600.0  # "N received" counts the tickets created this long ago or since
LANES = (("classifier", "Classifier"), ("responder", "Responder"))  # the timeline's, top to bottom


def _ticket_row(name: str, tickets: list[Ticket], now: float) -> dict:
    """A ticket service's row: its status and Issues numbers, its two sources, the list, and the
    timeline with each lane's state. Timeline positions are seconds from its left edge,
    ``now - CHART_SECONDS``."""
    left = now - CHART_SECONDS
    listed = sorted((t for t in tickets if t.event_time is not None and t.priority is not None),
                    key=lambda t: t.event_time, reverse=True)
    runs = sorted((r for t in tickets for r in _agent_runs(t, now) if r.end >= left), key=lambda r: r.start)
    working = [r for r in runs if r.open]
    busy = {r.lane for r in working}
    return {
        "name": name,
        "row": "tickets",  # which kind of row the page builds (D-068)
        "status": "working" if working else "healthy",
        "statusText": "Agent resolving P1" if working else "Healthy",
        "issues": _ticket_issues(tickets, now),
        "sources": [{"key": s.code, "name": s.name} for s in SOURCES],
        "tickets": {
            "received": sum(1 for t in listed if now - RECEIVED_SECONDS <= t.event_time <= now),
            "newest": [{"id": t.ticket_id, "source": t.source, "sourceName": _source_name(t.source),
                        "priority": t.priority, "subject": t.subject, "eventTime": t.event_time}
                       for t in listed[:LIST_LENGTH]],
        },
        "marks": [{"id": t.ticket_id, "priority": t.priority, "t": t.event_time - left}
                  for t in reversed(listed) if left <= t.event_time <= now],
        "runs": [_run_json(r, left) for r in runs],
        "lanes": [{"key": key, "name": lane, "working": key in busy, "state": "Working" if key in busy else "Idle"}
                  for key, lane in LANES],
        "agentsText": f"{len(busy)} of {len(LANES)} agents working" if busy else "Both agents idle",
        "agentsWorking": len(busy),
        "openP1": len({r.ticket.ticket_id for r in working}),
    }


def _source_name(code: str | None) -> str | None:
    """A ticket source as a person reads it: "Zendesk" for ZENDESK."""
    return SOURCES_BY_CODE[code].name if code in SOURCES_BY_CODE else code


def _ticket_issues(tickets: list[Ticket], now: float) -> dict:
    """A ticket service's Issues panel (spec decision 36), over the tickets created in the
    last 10 minutes: injected, the P1s whose answer key says HIGH; detected, those the
    classifier called HIGH; missed, those it called LOW or INVALID, or hasn't called 30 s
    after the ticket; the median ticket -> HIGH verdict, and ticket -> the responder's
    action, whatever it is. A LOW ticket called HIGH, and a wrong action, are evaluate's."""
    injected = [t for t in tickets if t.priority == "P1" and t.true_severity == HIGH
                and t.event_time is not None and now - RECEIVED_SECONDS <= t.event_time <= now]
    detected = [t for t in injected if t.verdict == HIGH]
    missed = [t for t in injected if t.verdict in (LOW, INVALID)
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
    if ticket.verdict == HIGH and ticket.classified_at is not None:
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


VERDICT_WORDS = {HIGH: "High", LOW: "Low", INVALID: "Invalid"}
# Every action in words: the responder's bars, and the snapshot's actionWords for the page's
# tooltips, so an action reads the same everywhere.
ACTION_WORDS = {"ROLLBACK": "Roll back", "SCALE_OUT": "Scale out", "INVESTIGATE_UPSTREAM": "Investigate upstream",
                "OBSERVE": "Observe", INVALID: "Invalid"}


def _words(code: str | None, words: dict[str, str]) -> str | None:
    if code is None:
        return None
    return words.get(code) or code.replace("_", " ").capitalize()


def _run_json(run: _Run, left: float) -> dict:
    def pos(t: float) -> float:
        return max(0.0, min(CHART_SECONDS, t - left))

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
    """A duration as the design writes it: "6s", "1m 05s", to the nearest second (a half up)."""
    sec = int(max(0.0, sec) + 0.5)
    return f"{sec}s" if sec < 60 else f"{sec // 60}m {sec % 60:02d}s"


def _dur_text(sec: float | None) -> str:
    """An Issues panel's duration: "6s", or "—" with nothing to show."""
    return "—" if sec is None else _dur_words(sec)


def _ticket_last(ticket: Ticket) -> float:
    """The latest moment known about a ticket, the anchor ``trim()`` drops it by."""
    times = (ticket.event_time, ticket.p1_at, ticket.classified_at, ticket.answered_at)
    return max((t for t in times if t is not None), default=0.0)


def _written(kafka_ts: float | None, item: Ticket | Finding) -> float | None:
    """When a ticket's or a finding's record was written: its Kafka timestamp, else the ticket's
    creation or the finding's report."""
    return kafka_ts if kafka_ts is not None else item.event_time


def _fill_ticket(ticket: Ticket, record: dict) -> None:
    """The ticket's own fields from any record that carries them (support_tickets, and
    p1_tickets and ticket_triage, which copy them); a field already known stays."""
    if ticket.event_time is None:
        ticket.event_time = _to_seconds(record.get("event_time"))
    for name in ("source", "service_name", "priority", "subject"):
        if getattr(ticket, name) is None and record.get(name):
            setattr(ticket, name, record[name])


# ---- the "Code security incidents" row (D-070) -----------------------------------------

SECURITY_ROW = "security"  # the row kind; the services' rows are "releases", "tickets" and "windows"
SECURITY_NAME = "Code security incidents"
SECURITY_WAIT_SECONDS = 60.0  # our agent's run gives up this long after it started; checkout's 30 s is too short
SCANNED_SECONDS = 600.0       # "N scanned" and the Issues panel count the findings reported this long ago or since
FINDINGS_LISTED = 5           # the newest findings the list shows
# The timeline's lanes, top to bottom. The patch agent is always "external" on the wall (spec Terms).
SECURITY_LANES = (("security", "Security agent"), ("external", "External · LangGraph · A2A"))
SECURITY_ACTION_WORDS = {"PATCH": "Patch", "DISMISS": "Dismiss", INVALID: "Invalid"}
PATCHED, FAILED = "PATCHED", "FAILED"  # lab 6.4's patch_status, besides N/A
# The answer's Patch line, as lab 6.4 finds it (a markdown bold label allowed): what the patch
# agent replied, which the security agent copies there.
PATCH_LINE = re.compile(r"\*{0,2}Patch:\*{0,2}[ \t]*([^\n]+)")


@dataclass
class _FindingRun:
    """One agent's work on one critical finding, as the timeline draws it (spec decision 24)."""
    lane: str          # "security" (ours) or "external" (the patch agent)
    finding: Finding
    start: float
    end: float         # the answer; now while open; SECURITY_WAIT_SECONDS after the start with no answer
    open: bool
    label: str


def _security_row(findings: list[Finding], now: float) -> dict:
    """The "Code security incidents" row: its status and Issues numbers, the list of findings,
    and the timeline with its two lanes. Timeline positions are seconds from its left edge,
    ``now - CHART_SECONDS``."""
    left = now - CHART_SECONDS
    listed = sorted((f for f in findings if f.event_time is not None and f.cvss_score is not None),
                    key=lambda f: f.event_time, reverse=True)
    runs = sorted((r for f in findings for r in _finding_runs(f, now) if r.end >= left), key=lambda r: r.start)
    working = [r for r in runs if r.open]
    busy = {r.lane for r in working}
    return {
        "name": SECURITY_NAME,
        "row": SECURITY_ROW,  # which kind of row the page builds
        "status": "working" if working else "healthy",
        "statusText": "Agent on critical CVE" if working else "Healthy",
        "issues": _finding_issues(findings, now),
        "findings": {
            "scanned": sum(1 for f in listed if now - SCANNED_SECONDS <= f.event_time <= now),
            "newest": [{"id": f.finding_id, "repository": f.repository, "cveId": f.cve_id, "cvssScore": f.cvss_score,
                        "critical": f.critical, "eventTime": f.event_time} for f in listed[:FINDINGS_LISTED]],
        },
        "marks": [{"id": f.finding_id, "cveId": f.cve_id, "critical": f.critical, "t": f.event_time - left}
                  for f in reversed(listed) if left <= f.event_time <= now],
        "runs": [_finding_run_json(r, left) for r in runs],
        # The wall hears of the external call only when our agent answers (spec decision 24), so
        # it never says the external agent is working: its lane has no state.
        "lanes": [{"key": "security", "name": SECURITY_LANES[0][1], "working": "security" in busy,
                   "state": "Working" if "security" in busy else "Idle"},
                  {"key": "external", "name": SECURITY_LANES[1][1], "working": False, "state": None}],
        "openCriticals": len({r.finding.finding_id for r in working}),
    }


def _finding_runs(finding: Finding, now: float) -> list[_FindingRun]:
    """Our agent's run, from lab 6.1's record (the finding's event_time when only the decision
    has come) to the decision, and the patch agent's, from the patch's start to its finish once
    the decision has brought both."""
    runs = []
    start = finding.critical_at
    if start is None and finding.decided_at is not None:
        start = finding.event_time
    if start is not None:
        if finding.decided_at is not None:
            end = max(start, finding.decided_at)
            runs.append(_FindingRun("security", finding, start, end, False, _security_label(finding, end - start)))
        elif now - start <= SECURITY_WAIT_SECONDS:
            runs.append(_FindingRun("security", finding, start, now, True, f"Working {_dur_words(now - start)}"))
        else:
            runs.append(_FindingRun("security", finding, start, start + SECURITY_WAIT_SECONDS, False, "No answer"))
    began, ended = finding.patch_started_at, finding.patch_finished_at
    if began is not None and ended is not None:
        took = _dur_words(max(began, ended) - began)
        label = f"{finding.commit_sha} in {took}" if finding.commit_sha else took
        runs.append(_FindingRun("external", finding, began, max(began, ended), False, label))
    return runs


def _security_label(finding: Finding, took: float) -> str:
    """What our closed bar reads: the outcome, and how long it took when it is an answer."""
    if finding.patch_status == FAILED:
        return "Patch agent unreachable"
    if finding.action == "PATCH":
        return f"Patched in {_dur_words(took)}"
    if finding.action == "DISMISS":
        return f"Dismissed in {_dur_words(took)}"
    return "Invalid answer"


def _wait(run: _FindingRun) -> tuple[float, float] | None:
    """The part of our bar the patch covers, hatched while our agent waited on the patch agent.
    The patch's times are the laptop's clock and our bar's are Kafka's, so the span is clipped
    to the bar."""
    f = run.finding
    if f.patch_started_at is None or f.patch_finished_at is None:
        return None
    began, ended = max(run.start, f.patch_started_at), min(run.end, f.patch_finished_at)
    return (began, ended) if began < ended else None


def _finding_run_json(run: _FindingRun, left: float) -> dict:
    def pos(t: float) -> float:
        return max(0.0, min(CHART_SECONDS, t - left))

    f = run.finding
    out = {
        "key": f"{run.lane}@{f.finding_id}",  # who the bar is from one snapshot to the next (D-062)
        "lane": run.lane,
        "findingId": f.finding_id,
        "repository": f.repository,
        "cveId": f.cve_id,
        "cvssScore": f.cvss_score,
        "start": pos(run.start),
        "end": pos(run.end),
        "open": run.open,
        "label": run.label,
    }
    if run.lane == "security":  # the tooltip: the action, the reason and the commit
        wait = _wait(run)
        out.update({
            "action": f.action,
            "outcome": _words(f.action, SECURITY_ACTION_WORDS),
            "reason": f.reason,
            "commit": f.commit_sha,
            "patchStatus": f.patch_status,
            "unreachable": f.patch_status == FAILED,  # the page draws these words in red
            "waitFrom": pos(wait[0]) if wait else None,
            "waitTo": pos(wait[1]) if wait else None,
        })
    else:  # the tooltip: the patch agent's reply, its times and how long it took
        out.update({
            "commit": f.commit_sha,
            "reply": f.reply,
            "startedAt": f.patch_started_at,
            "finishedAt": f.patch_finished_at,
            "duration": run.end - run.start,
            "durationText": _dur_words(run.end - run.start),
        })
    return out


def _finding_issues(findings: list[Finding], now: float) -> dict:
    """The security row's Issues panel (spec decision 26), over the critical findings reported
    in the last 10 minutes that have an answer key: injected, all of them; detected, those whose
    action is the one expected; missed, those given another action or INVALID, or no decision
    60 s after lab 6.1's record (after the finding, if lab 6.1 never wrote one); the median
    finding -> decision, whatever the action, and finding -> the patch's finish, PATCHED only."""
    scored = [f for f in findings if f.critical and f.expected_action is not None
              and f.event_time is not None and now - SCANNED_SECONDS <= f.event_time <= now]
    right = [f for f in scored if f.action == f.expected_action]
    missed = [f for f in scored if (f.action is not None and f.action != f.expected_action)
              or (f.action is None and now - (f.critical_at or f.event_time) > SECURITY_WAIT_SECONDS)]
    return _issues_json(
        len(scored), len(right), len(missed),
        [f.decided_at - f.event_time for f in scored if f.decided_at is not None],
        [f.patch_finished_at - f.event_time for f in scored
         if f.patch_status == PATCHED and f.patch_finished_at is not None],
    )


def _finding_last(finding: Finding) -> float:
    """The latest moment known about a finding, the anchor ``trim()`` drops it by."""
    times = (finding.event_time, finding.critical_at, finding.decided_at, finding.patch_finished_at)
    return max((t for t in times if t is not None), default=0.0)


def _fill_finding(finding: Finding, record: dict) -> None:
    """The finding's own fields from any record that carries them (cve_scans, and critical_cves
    and patch_decisions, which copy them); a field already known stays."""
    if finding.event_time is None:
        finding.event_time = _to_seconds(record.get("event_time"))
    for name in ("repository", "cve_id", "package", "installed_version", "fixed_version", "cvss_score", "finding"):
        if getattr(finding, name) is None and record.get(name) is not None:
            setattr(finding, name, record[name])


def _patch_line(answer) -> str | None:
    """The answer's Patch line without its label, or None."""
    m = PATCH_LINE.search(answer) if isinstance(answer, str) else None
    return (m.group(1).strip() or None) if m else None


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
        action=record.get("action") or INVALID,
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
