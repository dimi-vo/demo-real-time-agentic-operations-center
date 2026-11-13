"""Thread-safe in-memory snapshot of the dashboard.

One ``OpsWallState`` is shared between the Kafka consumer threads (or the mock
driver) which call ``apply_*()`` and the FastAPI websocket loop which calls
``snapshot(now)`` and pushes it to every browser at ~4 Hz. All access is guarded
by one lock; the write methods are tiny so contention is negligible.

State stores absolute epoch seconds everywhere. ``snapshot(now)`` is where
absolute times become the axis-relative seconds the browser's variant-e render
code wants: a span at ``[a, b]`` becomes ``start = a - (now - axis_seconds)`` and
``end = b - (now - axis_seconds)``, clipped to ``[0, axis_seconds]``.

Spans are consolidated at snapshot time, not written in by the applier: that way
a late health row or a late decision can still reshape a span retroactively (the
consumer reads ``read_committed``, but transactional commits can land out of
chronological order). The applier keeps the raw per-window facts; the snapshot
collapses them.

Status per service is derived from the latest health row + the last actionable
decision: WARMING_UP → ``warming``, latency/traffic flag → ``anomaly``, normal
after a recent actionable decision → ``remediated``, otherwise ``healthy``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opsgen.services import SERVICES

AXIS_SECONDS = 600
WARMUP_SECONDS = 130
REMEDIATION_RECENT_SECONDS = 120.0
CLOSING_ACTIONS = ("ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM")
HEALTH_RETAIN_MARGIN = 60.0  # keep 60 s past the axis so a span straddling it stays whole


@dataclass
class HealthRow:
    window_end: float  # epoch seconds
    latency_status: str
    traffic_status: str
    request_count: int | None = None
    error_count: int | None = None


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
    at: float            # epoch seconds — Kafka record timestamp
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


class OpsWallState:
    """Thread-safe collector + snapshotter for the dashboard."""

    def __init__(self, axis_seconds: int = AXIS_SECONDS, warmup_seconds: int = WARMUP_SECONDS) -> None:
        self.axis_seconds = axis_seconds
        self.warmup_seconds = warmup_seconds
        self._lock = threading.RLock()
        self._services: dict[str, ServiceState] = {
            s.name: ServiceState(name=s.name, tier=s.tier, ready_replicas=s.ready_replicas)
            for s in SERVICES
        }
        self._connection_errors: dict[str, dict] = {}  # topic -> {code, detail, ts}

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
        with self._lock:
            s = self._services[name]
            # Dedup by window_end (the consumer may replay).
            for i, row in enumerate(s.health):
                if row.window_end == w:
                    s.health[i] = HealthRow(
                        window_end=w,
                        latency_status=record.get("latency_status") or row.latency_status,
                        traffic_status=record.get("traffic_status") or row.traffic_status,
                        request_count=record.get("request_count"),
                        error_count=record.get("error_count"),
                    )
                    return
            s.health.append(HealthRow(
                window_end=w,
                latency_status=record.get("latency_status") or "NORMAL",
                traffic_status=record.get("traffic_status") or "NORMAL",
                request_count=record.get("request_count"),
                error_count=record.get("error_count"),
            ))

    def apply_decision(self, record: dict, kafka_ts: float | None = None) -> None:
        name = record.get("service_name")
        if not name or name not in self._services:
            return
        we = _to_seconds(record.get("window_end"))
        at = kafka_ts if kafka_ts is not None else we
        if at is None:
            return
        with self._lock:
            s = self._services[name]
            # Dedup by (window_end, agent).
            agent = record.get("agent_name", "")
            for i, d in enumerate(s.decisions):
                if d.window_end == we and d.agent == agent:
                    s.decisions[i] = _decision(record, we, at, agent)
                    return
            s.decisions.append(_decision(record, we, at, agent))

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

    def record_error(self, topic: str, code: str, detail: str) -> None:
        with self._lock:
            self._connection_errors[topic] = {"code": code, "detail": detail, "ts": time.time()}

    def clear_error(self, topic: str) -> None:
        with self._lock:
            self._connection_errors.pop(topic, None)

    # ---- housekeeping ---------------------------------------------------------

    def trim(self, now: float) -> None:
        """Drop entries whose anchor time is older than ``now - axis_seconds - margin``."""
        cutoff = now - self.axis_seconds - HEALTH_RETAIN_MARGIN
        with self._lock:
            for s in self._services.values():
                s.health = [h for h in s.health if h.window_end >= cutoff]
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
            services = [self._snapshot_service(s, now, left, rel) for s in self._services.values()]
            return {
                "axisSeconds": self.axis_seconds,
                "warmupSeconds": self.warmup_seconds,
                "updatedAt": now,
                "connectionErrors": dict(self._connection_errors),
                "services": services,
            }

    def _snapshot_service(self, s: ServiceState, now: float, left: float, rel) -> dict:
        health = sorted((h for h in s.health if h.window_end >= left), key=lambda h: h.window_end)
        decisions = sorted(s.decisions, key=lambda d: d.at)
        actionable = [d for d in decisions if d.action in CLOSING_ACTIONS]

        lat_spans = _consolidate(health, lambda h: h.latency_status == "HIGH",
                                  actionable, "LATENCY_HIGH", now)
        trf_up = _consolidate(health, lambda h: h.traffic_status == "HIGH",
                               actionable, "TRAFFIC_HIGH", now)
        trf_dn = _consolidate(health, lambda h: h.traffic_status == "LOW",
                               actionable, "TRAFFIC_LOW", now)

        ground_truth = [{
            "type": i.incident_type,
            "start": rel(i.started_at),
            "end": rel(i.ended_at if i.ended_at is not None else now),
            "open": i.ended_at is None,
            "incidentId": i.incident_id,
        } for i in sorted(s.incidents, key=lambda i: i.started_at)]

        detected_latency = [{"start": rel(a), "end": rel(b), "open": b >= now - 1}
                            for a, b in lat_spans]
        detected_traffic = (
            [{"start": rel(a), "end": rel(b), "direction": "up", "open": b >= now - 1}
             for a, b in trf_up]
            + [{"start": rel(a), "end": rel(b), "direction": "down", "open": b >= now - 1}
               for a, b in trf_dn]
        )

        dec_json = [{
            "action": d.action,
            "at": rel(d.at),
            "trigger": d.trigger,
            "target": d.target,
            "agent": d.agent,
            "reason": d.reason,
        } for d in decisions if d.at >= left]

        status, status_text = _status(s, health, actionable, now)
        rate = (health[-1].request_count / 10.0) if health and health[-1].request_count is not None else None
        err = None
        if health and health[-1].request_count and health[-1].error_count is not None:
            err = 100.0 * health[-1].error_count / max(1, health[-1].request_count)
        seconds_since_deploy = (now - s.deployed_at) if s.deployed_at else None
        warmup = _warmup_span(health, rel)
        snap = {
            "name": s.name,
            "tier": s.tier,
            "version": s.version,
            "previousVersion": s.previous_version,
            "replicas": s.ready_replicas,
            "lastChange": s.last_change,
            "secondsSinceDeploy": seconds_since_deploy,
            "rateRps": rate,
            "errorRatePct": err,
            "status": status,
            "statusText": status_text,
            "groundTruth": ground_truth,
            "detectedLatency": detected_latency,
            "detectedTraffic": detected_traffic,
            "decisions": dec_json,
        }
        if warmup is not None:
            snap["warmup"] = warmup
        return snap


# ---- helpers ------------------------------------------------------------------

def _to_seconds(value) -> float | None:
    """Epoch seconds from what a record carries: a `datetime` (what the Avro decoder makes
    of Flink's TIMESTAMP(3), registered as local-timestamp-millis; naive values are UTC
    wall-clock here, D-048), epoch ms (int) or seconds (float, as the mock passes)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    if isinstance(value, (int, float)):
        v = float(value)
        return v / 1000.0 if v > 1e12 else v
    return None


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


def _consolidate(health, match, actionable, trigger, now) -> list[tuple[float, float]]:
    """Group contiguous anomalous windows into spans; close at an actionable decision."""
    spans: list[list[float]] = []
    current: list[float] | None = None
    for row in health:
        if match(row):
            if current is None:
                current = [row.window_end - 10.0, row.window_end]
            else:
                current[1] = row.window_end
        else:
            if current is not None:
                spans.append(current)
                current = None
    if current is not None:
        spans.append(current)

    # Close any span if an actionable decision for this trigger lands inside it.
    closed: list[tuple[float, float]] = []
    for span in spans:
        a, b = span
        closer = next((d for d in actionable if d.trigger == trigger and a <= d.at <= b + 30), None)
        if closer:
            b = closer.at
        elif b >= now - 10:
            b = min(b, now)  # still open at the right edge
        closed.append((a, b))
    return closed


def _warmup_span(health, rel) -> list[float] | None:
    """Earliest and latest axis-relative seconds where a WARMING_UP row sits in the
    visible health, as a [start, end] pair the frontend's warm-up band expects.
    Returns None if nothing in the visible window was warming up — once the
    warm-up has scrolled off the left of the axis it should not be drawn."""
    warming = [h for h in health
               if h.latency_status == "WARMING_UP" or h.traffic_status == "WARMING_UP"]
    if not warming:
        return None
    start = rel(min(h.window_end - 10.0 for h in warming))
    end = rel(max(h.window_end for h in warming))
    return [start, end]


def _status(s: ServiceState, health, actionable, now) -> tuple[str, str]:
    if not health:
        return "warming", "Detector warming up"
    latest = health[-1]
    if latest.latency_status == "WARMING_UP" or latest.traffic_status == "WARMING_UP":
        return "warming", "Detector warming up"
    if latest.latency_status == "HIGH":
        return "anomaly", "Latency above the expected band"
    if latest.traffic_status == "HIGH":
        return "anomaly", "Traffic above the expected band"
    if latest.traffic_status == "LOW":
        return "anomaly", "Traffic below the expected band"
    # Normal — check for a very recent actionable decision.
    recent = [d for d in actionable if now - d.at <= REMEDIATION_RECENT_SECONDS]
    if recent:
        d = recent[-1]
        label = {
            "ROLLBACK": f"Rolled back to v{d.target}" if d.target else "Rolled back",
            "SCALE_OUT": f"Scaled out to {d.target} replicas" if d.target else "Scaled out",
            "INVESTIGATE_UPSTREAM": "Traced to upstream",
        }.get(d.action, "Remediated")
        return "remediated", label
    return "healthy", "Healthy"
