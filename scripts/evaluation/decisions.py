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
- **Coverage.** Every anomalous `service_health` row in range should produce exactly one
  decision, and no other row should produce any: agent calls track anomalies, not
  windows. A row is *expected to call* the capacity agent when `traffic_status` is HIGH
  or LOW, and the release agent when `latency_status` is HIGH and traffic is normal.
  Missing decisions at the very end of the range are usually just in flight.
- **Per incident.** The decisions on that service's windows inside the incident span.
  The **headline** decision is the one on the earliest such window: the first
  recommendation the on-call team would see. It matches when its action equals the
  incident's `expected_action`. Targets are checked too: a `ROLLBACK` should name the
  incident's `good_version`, a `SCALE_OUT` should carry the replica count the SQL
  computed (recomputed here from the stored context).
- **Outside incidents.** Decisions on rows outside every incident span of their service.
  These follow the detector's false positives; `WATCH` is the appropriate answer there.
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
WATCH = "WATCH"
ESCALATE = "ESCALATE"
SCALE_OUT = "SCALE_OUT"
INVESTIGATE_UPSTREAM = "INVESTIGATE_UPSTREAM"
INVALID = "INVALID"

ALLOWED_ACTIONS = {
    RELEASE_AGENT: (ROLLBACK, WATCH, ESCALATE),
    CAPACITY_AGENT: (SCALE_OUT, INVESTIGATE_UPSTREAM, ESCALATE, WATCH),
}
AGENT_FOR_TRIGGER = {
    LATENCY_HIGH: RELEASE_AGENT,
    TRAFFIC_HIGH: CAPACITY_AGENT,
    TRAFFIC_LOW: CAPACITY_AGENT,
}
CONFIDENCES = ("HIGH", "MEDIUM", "LOW")

VALID_ACTION_TARGET = 0.95
# A decision is written a few seconds after its window closes (the model call). Rows in
# this tail of the range may legitimately have no decision yet.
IN_FLIGHT_SECONDS = 60.0


def _num(v: float | int | None) -> str:
    return "?" if v is None else f"{v:.0f}"


@dataclass(frozen=True)
class DecisionRow:
    """One row of `remediation_decisions`."""

    window_end: float
    service_name: str
    agent_name: str
    trigger: str
    action: str
    target_version: str | None = None
    target_replicas: int | None = None
    confidence: str | None = None
    summary: str | None = None
    reasoning: str | None = None
    raw_response: str | None = None
    version: str | None = None
    seconds_since_deploy: int | None = None
    avg_latency_ms: float | None = None
    expected_latency_ms: float | None = None
    request_count: int | None = None
    expected_request_count: float | None = None
    error_rate: float | None = None
    ready_replicas: int | None = None
    max_replicas: int | None = None

    @classmethod
    def from_record(cls, rec: dict) -> DecisionRow:
        def num(key, cast):
            v = rec.get(key)
            return None if v is None else cast(v)

        return cls(
            window_end=epoch_seconds(rec["window_end"]),
            service_name=rec["service_name"],
            agent_name=rec.get("agent_name") or "",
            trigger=rec.get("trigger") or "",
            action=(rec.get("action") or INVALID).strip().upper(),
            target_version=rec.get("target_version"),
            target_replicas=num("target_replicas", int),
            confidence=rec.get("confidence"),
            summary=rec.get("summary"),
            reasoning=rec.get("reasoning"),
            raw_response=rec.get("raw_response"),
            version=rec.get("version"),
            seconds_since_deploy=num("seconds_since_deploy", int),
            avg_latency_ms=num("avg_latency_ms", float),
            expected_latency_ms=num("expected_latency_ms", float),
            request_count=num("request_count", int),
            expected_request_count=num("expected_request_count", float),
            error_rate=num("error_rate", float),
            ready_replicas=num("ready_replicas", int),
            max_replicas=num("max_replicas", int),
        )

    @property
    def key(self) -> tuple[str, float]:
        return (self.service_name, self.window_end)

    @property
    def valid(self) -> bool:
        return self.action in ALLOWED_ACTIONS.get(self.agent_name, ())

    @property
    def routed_correctly(self) -> bool:
        return AGENT_FOR_TRIGGER.get(self.trigger) == self.agent_name

    @property
    def suggested_replicas(self) -> int | None:
        """What the SQL handed the agent, recomputed from the stored context."""
        return suggested_replicas(
            self.ready_replicas, self.max_replicas, self.request_count, self.expected_request_count
        )

    def describe(self) -> str:
        """The numbers behind the decision, for the mismatch list."""
        if self.trigger == LATENCY_HIGH:
            body = f"{_num(self.avg_latency_ms)}ms vs {_num(self.expected_latency_ms)}ms expected"
            if self.seconds_since_deploy is not None:
                body += f", deployed {self.seconds_since_deploy}s ago"
            return body
        body = f"{_num(self.request_count)} req vs {_num(self.expected_request_count)} expected"
        if self.ready_replicas is not None:
            body += f", {self.ready_replicas}/{_num(self.max_replicas)} replicas"
        return body


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


@dataclass
class IncidentDecisions:
    incident: Incident
    decisions: list[DecisionRow] = field(default_factory=list)

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
    def target_ok(self) -> bool | None:
        """True/False when the headline action carries a target, None when it needs none."""
        d = self.headline
        if d is None:
            return None
        if d.action == ROLLBACK:
            return d.target_version is not None and d.target_version == self.incident.good_version
        if d.action == SCALE_OUT:
            return d.target_replicas is not None and d.target_replicas == d.suggested_replicas
        return None

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
    confidences: Counter = field(default_factory=Counter)

    @property
    def valid_share(self) -> float | None:
        return self.valid / self.decisions if self.decisions else None


@dataclass
class Coverage:
    """Anomalous `service_health` rows against the decisions they should have produced."""

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
    both_agents: list[tuple[str, float]]  # (service, window_end) seen under both agents

    @property
    def valid_share(self) -> float | None:
        valid = sum(a.valid for a in self.agents.values())
        return valid / self.total_decisions if self.total_decisions else None

    @property
    def invalid(self) -> list[DecisionRow]:
        return [d for d in self.outside if not d.valid]


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

    agents: dict[str, AgentSummary] = {}
    for d in scoped:
        a = agents.setdefault(d.agent_name, AgentSummary(d.agent_name))
        a.decisions += 1
        a.valid += int(d.valid)
        a.routing_errors += int(not d.routed_correctly)
        a.actions[d.action] += 1
        a.confidences[d.confidence or "-"] += 1

    by_window: dict[tuple[str, float], list[DecisionRow]] = {}
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
        per_incident.append(item)
    outside = [d for d in scoped if id(d) not in claimed]

    coverage = _coverage(scoped, health_rows, by_window, range_from, range_to)
    return DecisionReport(
        range_from=range_from,
        range_to=range_to,
        total_decisions=len(scoped),
        agents=agents,
        incidents=per_incident,
        outside=outside,
        coverage=coverage,
        both_agents=both,
    )


def _coverage(
    scoped: list[DecisionRow],
    health_rows: list[HealthRow],
    by_window: dict[tuple[str, float], list[DecisionRow]],
    range_from: float,
    range_to: float,
) -> Coverage:
    cov = Coverage()
    seen: set[tuple[str, float, str]] = set()
    for key, ds in by_window.items():
        for d in ds:
            triple = (*key, d.agent_name)
            if triple in seen:
                cov.duplicates.append(triple)
            seen.add(triple)

    wanted: set[tuple[str, float, str]] = set()
    for row in health_rows:
        if not (range_from <= row.window_end < range_to):
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
