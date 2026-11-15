"""Thread-safe in-memory snapshot of the dashboard.

One ``OpsWallState`` is shared between the Kafka consumer threads, which call
``apply_*()``, and the FastAPI websocket loop, which calls
``snapshot(now)`` and pushes it to every browser at ~4 Hz. All access is guarded
by one lock; the write methods are tiny so contention is negligible.

The appliers keep the raw facts: one health row per window, one decision per window
and agent, one incident per id, one ``service_metrics`` sample per second.
``snapshot(now)`` derives everything the page shows from them, so the page only draws
and the counting is tested here (D-054). Deriving
at snapshot time also lets a late row or decision reshape what is drawn (the consumer
reads ``read_committed``, and transactional commits can land out of order).

- A **run** is consecutive flagged windows of one service for one trigger (latency
  high, traffic high, traffic low), counting only windows whose ``window_end`` is on
  the chart. One window without that flag ends the run.
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
- The **samples** are the raw ``service_metrics`` records, one per service per second
  (D-058), kept for the axis plus a margin (``trim()``). The snapshot
  carries the ones on the chart; the page draws them as the latency and traffic lines
  (D-061). The rail's Rate (requests/s) and Errors (% of requests) come from the last 10 s
  of them.

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

from opsgen.services import SERVICES

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
RATE_WINDOW_SECONDS = 10.0  # the rail's Rate and Errors: the samples of the last 10 s


@dataclass(frozen=True)
class Trigger:
    name: str        # as the agents write it in remediation_decisions.trigger
    metric: str      # the chart it is drawn on
    direction: str
    column: str      # the HealthRow status it reads
    flag: str        # the status value that flags it
    words: str       # how the tile says it


TRIGGERS = (
    Trigger("LATENCY_HIGH", "latency", "up", "latency_status", "HIGH", "latency high"),
    Trigger("TRAFFIC_HIGH", "traffic", "up", "traffic_status", "HIGH", "traffic high"),
    Trigger("TRAFFIC_LOW", "traffic", "down", "traffic_status", "LOW", "traffic low"),
)
INCIDENT_METRIC = {"BAD_DEPLOY": "latency", "TRAFFIC_SURGE": "traffic", "UPSTREAM_DROP": "traffic"}


@dataclass
class HealthRow:
    window_end: float  # epoch seconds
    latency_status: str
    traffic_status: str
    detected_at: float | None = None  # when the detector produced the verdict; None on older rows


@dataclass
class MetricSample:
    """One ``service_metrics`` record: one second of one service (D-058)."""
    event_time: float  # epoch seconds; the end of the second it covers
    traffic_count: int
    latency_ms: float
    error_count: int


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
class ServiceState:
    name: str
    tier: str
    version: str | None = None
    previous_version: str | None = None
    ready_replicas: int | None = None
    last_change: str | None = None
    deployed_at: float | None = None
    health: list[HealthRow] = field(default_factory=list)
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
        self._connection_errors: dict[str, dict] = {}  # topic -> {code, detail, ts}
        # Bumped by clear(); every snapshot carries it so the page can tell data read after
        # a "Refresh data" from the snapshots that came before it (D-053).
        self.generation = 0

    # ---- writes ---------------------------------------------------------------

    def apply_deployment(self, record: dict) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        with self._lock:
            s = self._services[name]
            s.version = record.get("version") or s.version
            s.previous_version = record.get("previous_version") or s.previous_version
            s.ready_replicas = record.get("ready_replicas") or s.ready_replicas
            s.last_change = record.get("last_change") or s.last_change
            if record.get("deployed_at") is not None:
                s.deployed_at = _to_seconds(record["deployed_at"])

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

    def apply_decision(self, record: dict, kafka_ts: float | None = None) -> None:
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

    def record_error(self, topic: str, code: str, detail: str) -> None:
        with self._lock:
            self._connection_errors[topic] = {"code": code, "detail": detail, "ts": time.time()}

    def clear_error(self, topic: str) -> None:
        with self._lock:
            self._connection_errors.pop(topic, None)

    def clear(self) -> None:
        """Back to the state a new wall starts in: three pre-seeded services (tier and replicas,
        nothing else), no connection errors, and the next ``generation``. The owner of the feed
        calls this while no consumer is running, then starts new ones (D-053)."""
        with self._lock:
            self._services = _seeded_services()
            self._connection_errors = {}
            self.generation += 1

    # ---- housekeeping ---------------------------------------------------------

    def trim(self, now: float) -> None:
        """Drop entries whose anchor time is older than ``now - axis_seconds - margin``: an
        incident by its end, or by its start while it is open (an incident whose end record
        never came leaves the wall here). The feed calls this every 10 s."""
        cutoff = now - self.axis_seconds - RETAIN_MARGIN_SECONDS
        with self._lock:
            for s in self._services.values():
                s.health = [h for h in s.health if h.window_end >= cutoff]
                s.samples = [m for m in s.samples if m.event_time >= cutoff]
                s.decisions = [d for d in s.decisions if d.at >= cutoff]
                s.incidents = [i for i in s.incidents
                               if (i.ended_at or i.started_at) >= cutoff]

    # ---- snapshot -------------------------------------------------------------

    def snapshot(self, now: float | None = None) -> dict:
        now = now if now is not None else time.time()
        left = now - self.axis_seconds

        def rel(t: float) -> float:
            return max(0.0, min(self.axis_seconds, t - left))

        with self._lock:
            services = [_snapshot_service(s, now, left, rel) for s in self._services.values()]
            return {
                "generation": self.generation,
                "axisSeconds": self.axis_seconds,
                "updatedAt": now,
                "connectionErrors": dict(self._connection_errors),
                "summary": _summary([inc for svc in services for inc in svc["groundTruth"]],
                                    [b for svc in services for b in svc["blocks"]]),
                "services": services,
            }


# ---- snapshot helpers ---------------------------------------------------------

def _snapshot_service(s: ServiceState, now: float, left: float, rel) -> dict:
    health = sorted((h for h in s.health if h.window_end >= left), key=lambda h: h.window_end)
    incidents = sorted((i for i in s.incidents if _incident_end(i, now) >= left),
                       key=lambda i: i.started_at)
    blocks = _blocks(health, s.decisions, now)
    for b in blocks:
        b.incident = _owner(b, incidents, now)

    status, status_text = _status(blocks, now)
    rate, error_pct = _rate_and_errors(s.samples, now)
    return {
        "name": s.name,
        "tier": s.tier,
        "version": s.version,
        "previousVersion": s.previous_version,
        "replicas": s.ready_replicas,
        "lastChange": s.last_change,
        "secondsSinceDeploy": (now - s.deployed_at) if s.deployed_at else None,
        "rateRps": rate,
        "errorRatePct": error_pct,
        "status": status,
        "statusText": status_text,
        "groundTruth": [_incident_json(i, [b for b in blocks if b.incident is i], now, rel)
                        for i in incidents],
        "blocks": [_block_json(b, now, rel) for b in blocks],
        "metrics": _samples_json(s.samples, left, now),
    }


def _samples_json(samples: list[MetricSample], left: float, now: float) -> list[dict]:
    """The samples on the chart, oldest first: ``t`` is the axis position of ``event_time``.
    A sample stamped after ``now`` (it covers the second still running) waits for the next
    snapshot. Latency is rounded to 0.1 ms, which is all the page shows."""
    start = bisect.bisect_left(samples, left, key=lambda m: m.event_time)
    return [{"t": round(m.event_time - left, 3), "traffic": m.traffic_count,
             "latency": round(m.latency_ms, 1), "errors": m.error_count}
            for m in samples[start:] if m.event_time <= now]


def _rate_and_errors(samples: list[MetricSample], now: float) -> tuple[float | None, float | None]:
    """Requests per second and the share of failed requests (%), over the samples of the
    last ``RATE_WINDOW_SECONDS``. A sample is one second, so the rate is the mean
    ``traffic_count``. No samples → no rate; no requests → no error share."""
    start = bisect.bisect_right(samples, now - RATE_WINDOW_SECONDS, key=lambda m: m.event_time)
    recent = samples[start:]
    if not recent:
        return None, None
    requests = sum(m.traffic_count for m in recent)
    errors = sum(m.error_count for m in recent)
    return requests / len(recent), (100.0 * errors / requests if requests else None)


def _blocks(health: list[HealthRow], decisions: list[DecisionMark], now: float) -> list[_Block]:
    """The blocks still on the wall, by start: answered ones, and unanswered ones until
    ``ANSWER_WAIT_SECONDS`` after their run's last detection."""
    blocks = []
    for trigger in TRIGGERS:
        for run in _runs(health, trigger):
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


def _runs(health: list[HealthRow], trigger: Trigger) -> list[list[HealthRow]]:
    """Consecutive rows flagged for ``trigger``; any other row ends a run."""
    runs: list[list[HealthRow]] = []
    current: list[HealthRow] = []
    for row in health:
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


def _incident_json(inc: IncidentEvent, owned: list[_Block], now: float, rel) -> dict:
    first = owned[0] if owned else None
    if any(b.closer for b in owned):
        verdict = "answered"
    elif inc.ended_at is not None and now - inc.ended_at > MISSED_AFTER_SECONDS:
        verdict = "missed"
    else:
        verdict = "pending"
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


def _block_json(b: _Block, now: float, rel) -> dict:
    closer = b.closer
    return {
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


def _summary(incidents: list[dict], blocks: list[dict]) -> dict:
    verdicts = Counter(i["verdict"] for i in incidents)
    answered = [i for i in incidents if i["verdict"] == "answered"]
    return {
        "injected": len(incidents),
        "answered": verdicts["answered"],
        "missed": verdicts["missed"],
        "pending": verdicts["pending"],
        "falseAlarms": sum(1 for b in blocks if b["incidentId"] is None),
        "medianTimeToDetect": _median(i["timeToDetect"] for i in answered),
        "medianTimeToAnswer": _median(i["timeToAnswer"] for i in answered),
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
    """The pre-seeded tiles: every service with its tier and replica count, so the wall looks
    lived-in before the first record arrives."""
    return {
        s.name: ServiceState(name=s.name, tier=s.tier, ready_replicas=s.ready_replicas)
        for s in SERVICES
    }


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
