"""Score `remediation_decisions` against the generator's ground truth (`service_incidents`)
and against the `service_health` rows the agents were supposed to see.

Pure functions over plain data, so they're unit-tested offline. The definitions (D-032)
are:

- **Valid action.** The parsed `action` is one of the agent's allowed actions. The SQL
  maps a failed agent call, or an answer outside the set, to `INVALID` (D-007), so
  anything but `INVALID` is valid. Brief section 11 wants at least 95% valid.
- **Routing.** `LATENCY_HIGH` and `ERROR_RATE_HIGH` (D-067) belong to the release agent,
  `TRAFFIC_HIGH` and `TRAFFIC_LOW` to the capacity agent, and `HIGH_SEVERITY_TICKET` to the
  ticket responder (D-068), whose actions are `ROLLBACK`, `SCALE_OUT` and
  `INVESTIGATE_UPSTREAM`. A decision whose agent doesn't match its trigger is a routing error.
  **Mutual exclusivity**: no (service, window) may appear under both metric agents.
- **Ticket decisions** (D-068) are the rows with a `ticket_id`: the responder's answer about a
  ticket, not a window. `score_decisions` leaves them out, since they have no health row and
  would count as unexpected and move `counted_from`; `tickets.py` scores them.
- **Coverage.** Every anomalous `service_health` row should produce exactly one decision,
  and no other row should produce any: agent calls track anomalies, not windows. A row is
  *expected to call* the capacity agent when `traffic_status` is HIGH or LOW, and the
  release agent when `latency_status` is HIGH and traffic is normal, or when a
  `service_error_rate` row's `error_status` is HIGH.
  Coverage is counted **from the first decision in range, not from the start of the
  range**: the INSERT statements read `service_health` from the latest offset, so
  anomalies from before they were created have no decision by design and are not
  missing. The first decision is the best available proxy for when the agents started,
  which means an anomaly the agents missed at the very start of their life is not
  counted either. Missing decisions at the very end of the range are usually in flight.
- **Per incident.** The decisions on that service's windows inside the incident span.
  The **headline** decision is the one on the earliest such window: the first
  recommendation the on-call team would see. It matches when its action equals the
  incident's `expected_action`.
- **Target.** One free-text column (D-034). Only a `ROLLBACK` carries one, checked against
  the joined `service_health` (or `service_error_rate`) row, the input the agent answered
  (D-045): it should name that row's `previous_version`. Any other action should carry none:
  the capacity agent's Target line is always N/A since D-068, which took `SCALE_OUT` and its
  replica count away from it.
- **Context.** The decision rows hold no metrics, so everything else is read from the
  `service_health` (or `service_error_rate`) row with the same (service_name, window_end).
- **Outside incidents.** Decisions on rows outside every incident span of their service.
  These follow the detector's false positives; `OBSERVE` is the appropriate answer there.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from scripts.common.topic_reader import KAFKA_TIMESTAMP, epoch_seconds
from scripts.evaluation.detection import (
    ERROR_RATE_HIGH,
    LATENCY_HIGH,
    TRAFFIC_HIGH,
    TRAFFIC_LOW,
    HealthRow,
    Incident,
    error_rate_text,
)

RELEASE_AGENT = "release_agent"
CAPACITY_AGENT = "capacity_agent"
TICKET_RESPONDER = "ticket_responder"  # D-068
HIGH_SEVERITY_TICKET = "HIGH_SEVERITY_TICKET"

ROLLBACK = "ROLLBACK"
SCALE_OUT = "SCALE_OUT"
INVESTIGATE_UPSTREAM = "INVESTIGATE_UPSTREAM"
OBSERVE = "OBSERVE"
INVALID = "INVALID"

# One shared vocabulary (D-034, D-043); each agent is offered its own subset in its
# prompt. Every incident type has exactly one correct action; OBSERVE answers a false alarm.
# No metric agent offers SCALE_OUT since D-068 (surges are retired); it stays in the
# vocabulary for the ticket responder.
ACTIONS = (ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, OBSERVE)
ALLOWED_ACTIONS = {
    RELEASE_AGENT: (ROLLBACK, OBSERVE),
    CAPACITY_AGENT: (INVESTIGATE_UPSTREAM, OBSERVE),
    TICKET_RESPONDER: (ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM),  # D-068
}
AGENT_FOR_TRIGGER = {
    LATENCY_HIGH: RELEASE_AGENT,
    ERROR_RATE_HIGH: RELEASE_AGENT,  # D-067
    TRAFFIC_HIGH: CAPACITY_AGENT,
    TRAFFIC_LOW: CAPACITY_AGENT,
    HIGH_SEVERITY_TICKET: TICKET_RESPONDER,  # D-068
}

VALID_ACTION_TARGET = 0.95
# A decision is written a few seconds after its window closes (the model call). Rows in
# this tail of the range may legitimately have no decision yet.
IN_FLIGHT_SECONDS = 60.0

Key = tuple[str, float]


def _num(v: float | int | None) -> str:
    return "?" if v is None else f"{v:.0f}"


@dataclass(frozen=True)
class DecisionRow:
    """One row of `remediation_decisions`: what happened, who answered, what to do."""

    window_end: float
    service_name: str
    agent_name: str
    trigger: str
    action: str
    target: str | None = None
    reason: str | None = None
    raw_response: str | None = None
    ticket_id: str | None = None  # a ticket_responder decision's ticket (D-068); null for the metric agents
    written_at: float | None = None  # the record's Kafka timestamp, when it was read with one

    @classmethod
    def from_record(cls, rec: dict) -> DecisionRow:
        target = rec.get("target")
        return cls(
            window_end=epoch_seconds(rec["window_end"]),
            service_name=rec["service_name"],
            agent_name=rec.get("agent_name") or "",
            trigger=rec.get("trigger") or "",
            action=(rec.get("action") or INVALID).strip().upper(),
            target=target.strip() if isinstance(target, str) else target,
            reason=rec.get("reason"),
            raw_response=rec.get("raw_response"),
            ticket_id=rec.get("ticket_id"),
            written_at=epoch_seconds(rec.get(KAFKA_TIMESTAMP)),
        )

    @property
    def is_ticket(self) -> bool:
        """A ticket_responder decision (D-068): scored by `tickets.py`, never here."""
        return self.ticket_id is not None

    @property
    def key(self) -> Key:
        return (self.service_name, self.window_end)

    @property
    def valid(self) -> bool:
        return self.action in ALLOWED_ACTIONS.get(self.agent_name, ())

    @property
    def routed_correctly(self) -> bool:
        return AGENT_FOR_TRIGGER.get(self.trigger) == self.agent_name


def expected_agent(row: HealthRow) -> str | None:
    """Which agent a `service_health` row should call, per the routing rules."""
    if row.traffic_status in ("HIGH", "LOW"):
        return CAPACITY_AGENT
    if row.latency_status == "HIGH" or row.error_status == "HIGH":
        return RELEASE_AGENT
    return None


def describe(d: DecisionRow, health: HealthRow | None) -> str:
    """The numbers behind a decision, read from its `service_health` row."""
    if health is None:
        return "no matching service_health or service_error_rate row in range"
    if d.trigger in (LATENCY_HIGH, ERROR_RATE_HIGH):
        body = (
            error_rate_text(health)
            if d.trigger == ERROR_RATE_HIGH
            else f"{_num(health.avg_latency_ms)}ms vs {_num(health.expected_latency_ms)}ms expected"
        )
        if health.seconds_since_deploy is not None:
            body += f", {health.last_change or 'no change'} {health.seconds_since_deploy}s ago"
        return body
    body = f"{_num(health.traffic_count)} req vs {_num(health.expected_traffic_count)} expected"
    if health.ready_replicas is not None:
        body += f", {health.ready_replicas} replicas"
    return body


def check_target(d: DecisionRow, health: HealthRow | None) -> bool | None:
    """True when the target is right, False when wrong or where none belongs, None when
    there is nothing to check. Judged against the health row the agent answered (D-045)."""
    if d.action == ROLLBACK:
        if health is None or health.previous_version is None:
            return None if d.target else False
        return d.target == health.previous_version
    return False if d.target else None


@dataclass
class IncidentDecisions:
    incident: Incident
    decisions: list[DecisionRow] = field(default_factory=list)
    target_ok: bool | None = None  # of the headline decision

    @property
    def headline(self) -> DecisionRow | None:
        """The decision on the earliest window of the span."""
        return self.decisions[0] if self.decisions else None

    @property
    def matched(self) -> bool:
        d = self.headline
        return d is not None and d.action == self.incident.expected_action

    @property
    def any_matched(self) -> bool:
        return any(d.action == self.incident.expected_action for d in self.decisions)

    @property
    def actions(self) -> Counter:
        return Counter(d.action for d in self.decisions)


@dataclass
class AgentSummary:
    agent_name: str
    decisions: int = 0
    valid: int = 0
    routing_errors: int = 0
    actions: Counter = field(default_factory=Counter)

    @property
    def valid_share(self) -> float | None:
        return self.valid / self.decisions if self.decisions else None


@dataclass
class Coverage:
    """Anomalous `service_health` rows against the decisions they should have produced."""

    counted_from: float | None = None  # when the agents first answered; see the module docstring
    expected: int = 0
    decided: int = 0
    missing: list[tuple[str, float, str]] = field(default_factory=list)  # service, window_end, agent
    in_flight: int = 0
    unexpected: list[DecisionRow] = field(default_factory=list)
    duplicates: list[tuple[str, float, str]] = field(default_factory=list)
    total_rows: int = 0  # all health rows in range, to contrast calls with windows


@dataclass
class DecisionReport:
    range_from: float
    range_to: float
    total_decisions: int
    agents: dict[str, AgentSummary]
    incidents: list[IncidentDecisions]
    outside: list[DecisionRow]
    coverage: Coverage
    both_agents: list[Key]  # (service, window_end) seen under both agents
    bad_targets: list[DecisionRow]  # wrong version, wrong count, or a target that shouldn't be there
    health: dict[Key, HealthRow] = field(default_factory=dict)

    @property
    def valid_share(self) -> float | None:
        valid = sum(a.valid for a in self.agents.values())
        return valid / self.total_decisions if self.total_decisions else None

    def context(self, d: DecisionRow) -> str:
        return describe(d, self.health.get(d.key))


def score_decisions(
    decisions: list[DecisionRow],
    health_rows: list[HealthRow],
    incidents: list[Incident],
    range_from: float,
    range_to: float,
) -> DecisionReport:
    """Score the decisions in `[range_from, range_to)` against the incidents and the
    health rows that should have triggered them."""
    scoped = sorted(
        (d for d in decisions if not d.is_ticket and range_from <= d.window_end < range_to),
        key=lambda d: (d.window_end, d.service_name, d.agent_name),
    )
    health = {(r.service_name, r.window_end): r for r in health_rows}

    agents: dict[str, AgentSummary] = {}
    for d in scoped:
        a = agents.setdefault(d.agent_name, AgentSummary(d.agent_name))
        a.decisions += 1
        a.valid += int(d.valid)
        a.routing_errors += int(not d.routed_correctly)
        a.actions[d.action] += 1

    by_window: dict[Key, list[DecisionRow]] = {}
    for d in scoped:
        by_window.setdefault(d.key, []).append(d)
    both = sorted(k for k, ds in by_window.items() if len({d.agent_name for d in ds}) > 1)

    per_incident = []
    claimed: set[int] = set()
    for inc in incidents:
        item = IncidentDecisions(inc)
        for d in scoped:
            if d.service_name == inc.service_name and inc.in_span_at(d.window_end):
                item.decisions.append(d)
                claimed.add(id(d))
        if item.headline is not None:
            item.target_ok = check_target(item.headline, health.get(item.headline.key))
        per_incident.append(item)
    outside = [d for d in scoped if id(d) not in claimed]

    bad_targets = [d for d in scoped if check_target(d, health.get(d.key)) is False]

    coverage = _coverage(scoped, health_rows, by_window, range_to)
    return DecisionReport(
        range_from=range_from,
        range_to=range_to,
        total_decisions=len(scoped),
        agents=agents,
        incidents=per_incident,
        outside=outside,
        coverage=coverage,
        both_agents=both,
        bad_targets=bad_targets,
        health=health,
    )


def _coverage(
    scoped: list[DecisionRow],
    health_rows: list[HealthRow],
    by_window: dict[Key, list[DecisionRow]],
    range_to: float,
) -> Coverage:
    cov = Coverage()
    if not scoped:
        return cov
    # The agents read service_health from the latest offset, so they can only answer for
    # windows from their own creation onward.
    cov.counted_from = min(d.window_end for d in scoped)
    seen: set[tuple[str, float, str]] = set()
    for key, ds in by_window.items():
        for d in ds:
            triple = (*key, d.agent_name)
            if triple in seen:
                cov.duplicates.append(triple)
            seen.add(triple)

    wanted: set[tuple[str, float, str]] = set()
    for row in health_rows:
        if not (cov.counted_from <= row.window_end < range_to):
            continue
        cov.total_rows += 1
        agent = expected_agent(row)
        if agent is None:
            continue
        cov.expected += 1
        triple = (row.service_name, row.window_end, agent)
        wanted.add(triple)
        if triple in seen:
            cov.decided += 1
        elif row.window_end >= range_to - IN_FLIGHT_SECONDS:
            cov.in_flight += 1
        else:
            cov.missing.append(triple)
    if wanted:
        cov.unexpected = [d for d in scoped if (d.service_name, d.window_end, d.agent_name) not in wanted]
    cov.missing.sort()
    return cov
