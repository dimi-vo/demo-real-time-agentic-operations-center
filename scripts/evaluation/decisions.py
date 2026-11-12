"""Score `remediation_decisions` against the generator's ground truth (`incidents.jsonl`)
and against the `service_health` rows the agents were supposed to see.

Pure functions over plain data, so they're unit-tested offline. The definitions (D-032)
are:

- **Valid action.** The parsed `action` is one of the agent's allowed actions. The SQL
  maps a failed agent call, or an answer outside the set, to `INVALID` (D-007), so
  anything but `INVALID` is valid. Brief section 11 wants at least 95% valid.
- **Routing.** `LATENCY_HIGH` belongs to the release agent, `TRAFFIC_HIGH` and
  `TRAFFIC_LOW` to the capacity agent. A decision whose agent doesn't match its trigger
  is a routing error. **Mutual exclusivity**: no (service, window) may appear under both
  agents.
- **Coverage.** Every anomalous `service_health` row should produce exactly one decision,
  and no other row should produce any: agent calls track anomalies, not windows. A row is
  *expected to call* the capacity agent when `traffic_status` is HIGH or LOW, and the
  release agent when `latency_status` is HIGH and traffic is normal.
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
- **Target.** One free-text column (D-034). A `ROLLBACK` should name the incident's
  `good_version`; a `SCALE_OUT` should carry the replica count the SQL computed
  (recomputed here from the joined health row); any other action should carry none.
- **Context.** The decision rows hold no metrics, so everything else is read from the
  `service_health` row with the same (service_name, window_end).
- **Outside incidents.** Decisions on rows outside every incident span of their service.
  These follow the detector's false positives; `OBSERVE` is the appropriate answer there.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field

from scripts.common.topic_reader import epoch_seconds
from scripts.evaluation.detection import (
    LATENCY_HIGH,
    TRAFFIC_HIGH,
    TRAFFIC_LOW,
    HealthRow,
    Incident,
)

RELEASE_AGENT = "release_agent"
CAPACITY_AGENT = "capacity_agent"

ROLLBACK = "ROLLBACK"
SCALE_OUT = "SCALE_OUT"
INVESTIGATE_UPSTREAM = "INVESTIGATE_UPSTREAM"
ESCALATE = "ESCALATE"
OBSERVE = "OBSERVE"
INVALID = "INVALID"

# One shared vocabulary (D-034); each agent is offered its own subset in its prompt.
ACTIONS = (ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, ESCALATE, OBSERVE)
ALLOWED_ACTIONS = {
    RELEASE_AGENT: (ROLLBACK, ESCALATE, OBSERVE),
    CAPACITY_AGENT: (SCALE_OUT, INVESTIGATE_UPSTREAM, ESCALATE, OBSERVE),
}
AGENT_FOR_TRIGGER = {
    LATENCY_HIGH: RELEASE_AGENT,
    TRAFFIC_HIGH: CAPACITY_AGENT,
    TRAFFIC_LOW: CAPACITY_AGENT,
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
        )

    @property
    def key(self) -> Key:
        return (self.service_name, self.window_end)

    @property
    def valid(self) -> bool:
        return self.action in ALLOWED_ACTIONS.get(self.agent_name, ())

    @property
    def routed_correctly(self) -> bool:
        return AGENT_FOR_TRIGGER.get(self.trigger) == self.agent_name


def suggested_replicas(
    ready: int | None, maximum: int | None, count: int | None, expected: float | None
) -> int | None:
    """The capacity INSERT's formula (brief section 5.5), in Python."""
    if ready is None or maximum is None:
        return None
    base = ready + 1
    if count is not None and expected:
        base = max(base, math.ceil(ready * count / expected))
    return min(maximum, base)


def expected_agent(row: HealthRow) -> str | None:
    """Which agent a `service_health` row should call, per the routing rules."""
    if row.traffic_status in ("HIGH", "LOW"):
        return CAPACITY_AGENT
    if row.latency_status == "HIGH":
        return RELEASE_AGENT
    return None


def describe(d: DecisionRow, health: HealthRow | None) -> str:
    """The numbers behind a decision, read from its `service_health` row."""
    if health is None:
        return "no matching service_health row in range"
    if d.trigger == LATENCY_HIGH:
        body = f"{_num(health.avg_latency_ms)}ms vs {_num(health.expected_latency_ms)}ms expected"
        if health.seconds_since_deploy is not None:
            body += f", {health.last_change or 'no change'} {health.seconds_since_deploy}s ago"
        return body
    body = f"{_num(health.request_count)} req vs {_num(health.expected_request_count)} expected"
    if health.ready_replicas is not None:
        body += f", {health.ready_replicas}/{_num(health.max_replicas)} replicas"
    return body


def check_target(d: DecisionRow, incident: Incident | None, health: HealthRow | None) -> bool | None:
    """True when the target is right, False when wrong or where none belongs, None when
    there is nothing to check."""
    if d.action == ROLLBACK:
        if incident is None or incident.good_version is None:
            return None if d.target else False
        return d.target == incident.good_version
    if d.action == SCALE_OUT:
        if health is None:
            return None
        want = suggested_replicas(
            health.ready_replicas, health.max_replicas, health.request_count, health.expected_request_count
        )
        return want is not None and d.target == str(want)
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
        (d for d in decisions if range_from <= d.window_end < range_to),
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
    incident_of: dict[int, Incident] = {}
    claimed: set[int] = set()
    for inc in incidents:
        item = IncidentDecisions(inc)
        for d in scoped:
            if d.service_name == inc.service_name and inc.in_span_at(d.window_end):
                item.decisions.append(d)
                claimed.add(id(d))
                incident_of[id(d)] = inc
        if item.headline is not None:
            item.target_ok = check_target(item.headline, inc, health.get(item.headline.key))
        per_incident.append(item)
    outside = [d for d in scoped if id(d) not in claimed]

    bad_targets = [
        d for d in scoped if check_target(d, incident_of.get(id(d)), health.get(d.key)) is False
    ]

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
