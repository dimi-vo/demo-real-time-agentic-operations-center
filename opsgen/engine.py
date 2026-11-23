"""The schedule: incidents, one per-service schedule each.

Ticked once per simulated second. `Engine.tick(now, deployments)` ends and starts
incidents, mutates the `DeploymentState`s on deploys and rollbacks, and returns the
`Event`s that fired this tick. `Engine.resolve(decision, now)`, called before the tick,
ends an incident early on an agent's correct answer. All scheduling randomness comes from
the single rng, drawn in a fixed order, so `--seed` reproduces the whole schedule.

Rules (D-060, which amends D-042; D-065 and D-067 amend both):
- The schedule serves checkout (bad deploys and surges) and payments (bad deploys and drops),
  D-067. web-gateway is a candidate of none: it deploys on its own clock. A traffic series
  never gets both a surge and a drop: its band would widen too far to catch the drop.
- Every service runs its own schedule: incidents on different services may overlap, and a
  service has at most one at a time. Each service has its own detector model (D-009), so
  overlap costs no accuracy.
- Quiet period: no incident in the first `quiet_period` seconds after start (the
  detector's warm-up).
- Every incident starts on a 10-second window boundary, so its first window is all spike,
  and lasts up to `max_duration` (35 s, D-065). The cap ends it mid-window on purpose: the
  return to baseline splits over two windows and stays inside the detector's band.
- A service can start its next incident at the first boundary at least `gap` after its
  last one's scheduled end (start + `max_duration`), with a type whose rest is over. Rests
  are per service and type, measured from the scheduled end of that type's last incident on
  that service (`rests`, D-059). Counting from the scheduled end, not the actual one, keeps
  the schedule the same however incidents end. The rests set the rate (about 37 an hour,
  about 19 on each of the two services, D-067) and keep the same metric's spikes on a service far
  enough apart for the detector.
- At each boundary, while some service can start one: pick the type by weight among the
  types allowed now, then a random service allowed for it. Until all three types have
  happened, only types not seen yet are allowed, so the first three are one of each.
- An incident is **resolved** when an agent answers it correctly: its expected action under
  its expected trigger, for a window that ends after it started. It then ends on the tick
  that reads the decision, mid-window (D-065). Otherwise it **expires** at its cap.
- BAD_DEPLOY fires a DEPLOY and starts degrading on the same tick; it ends, resolved or
  expired, with a ROLLBACK to the previous version, and metrics return to baseline at that
  moment. These are the only version changes: there are no other deploys (D-057).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from opsgen.deployments import DeploymentState
from opsgen.distributions import ERROR_RATE

# Incident types, their weights, candidate services, trigger and expected action. Every
# type has exactly one expected action, whatever the service (D-043).
BAD_DEPLOY = "BAD_DEPLOY"
TRAFFIC_SURGE = "TRAFFIC_SURGE"
UPSTREAM_DROP = "UPSTREAM_DROP"

INCIDENT_TYPES = (BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP)
INCIDENT_WEIGHTS = {BAD_DEPLOY: 0.50, TRAFFIC_SURGE: 0.25, UPSTREAM_DROP: 0.25}
# D-067: the shared scheduler serves checkout and payments only; web-gateway is a candidate of
# none (it gets its own deploy clock). Surges and drops stay on different services: a traffic
# series that has both gets a band so wide (it is symmetric) that a drop no longer falls below
# it. Offline, with both types on both services, 1.6 % of drops were caught.
SHARED_SERVICES = ("checkout", "payments")
CANDIDATES = {
    BAD_DEPLOY: SHARED_SERVICES,
    TRAFFIC_SURGE: ("checkout",),
    UPSTREAM_DROP: ("payments",),
}
TRIGGER = {BAD_DEPLOY: "LATENCY_HIGH", TRAFFIC_SURGE: "TRAFFIC_HIGH", UPSTREAM_DROP: "TRAFFIC_LOW"}
EXPECTED_ACTION = {BAD_DEPLOY: "ROLLBACK", TRAFFIC_SURGE: "SCALE_OUT", UPSTREAM_DROP: "INVESTIGATE_UPSTREAM"}

# Metric effects during degradation. They change a service_metrics record's values, never
# the number of records (D-058). Each type moves exactly one metric (D-059): a surge leaves
# latency alone. The sizes barely matter to the detector, whose band grows with the spike
# windows it has trained on; they are big enough to read at a glance on the wall's charts.
BAD_DEPLOY_LATENCY_MULT = 4.0
BAD_DEPLOY_ERROR_RATE = 0.12  # share of failed requests, for error_count (display only)
SURGE_LAMBDA_MULT = 5.0
DROP_LAMBDA_MULT = 0.1

WINDOW_SECONDS = 10  # Flink's tumbling window; incidents start on its boundaries


def next_window_boundary(t: float) -> float:
    """The first 10-second window boundary at or after `t` (epoch seconds). Flink's
    tumbling windows are epoch-aligned, so this is where a window starts."""
    return float(math.ceil(t / WINDOW_SECONDS) * WINDOW_SECONDS)


def _first_tick_of_window(now: float) -> bool:
    """Whether the tick at `now` is the first of its 10-second window. Ticks are one second
    apart; on an aligned start (what ops-sim does) this is the tick on the boundary itself."""
    return now - math.floor(now / WINDOW_SECONDS) * WINDOW_SECONDS < 1


# The time a service waits, from the scheduled end of an incident of a type, before it can
# have that type again, the same on every service (D-059). The detector trains on its spike
# windows: too many of them on one metric and its band outgrows the next spike. These rests
# keep every type caught and land on about 19 incidents an hour per service: 56 on three
# services (D-065), 37 on D-067's two. The long bad-deploy
# rest brings the rate down without making traffic spikes rarer, which would add after-spike
# false alarms.
RESTS = {BAD_DEPLOY: 390.0, TRAFFIC_SURGE: 300.0, UPSTREAM_DROP: 300.0}


@dataclass
class Config:
    """All timings in seconds. They live here only: ops-sim has no timing flags (D-060)."""

    # The detector's warm-up: (minTrainingSize + 1) windows plus one window of margin, so the
    # first incident is in a window the detector scores. 150 s with lab 2.3's minTrainingSize
    # 12; a test keeps the two in step.
    quiet_period: float = 150.0
    # D-065 (was a fixed 20 s, D-059): an incident lasts until an agent resolves it, or this
    # cap. 35 s ends mid-window on purpose: the return to baseline splits over two windows, in
    # two steps small enough to stay inside the detector's band. A multiple of 10 ends on a
    # boundary and brings the after-spike echoes back (30 s and 40 s: 80x and 670x those of
    # the old 20 s).
    max_duration: float = 35.0
    gap: float = 20.0  # between two incidents on one service, whatever their types
    rests: dict[str, float] = field(default_factory=lambda: dict(RESTS))  # per type
    no_incidents: bool = False


@dataclass
class Effect:
    """What an incident does to a service's metrics this tick: multipliers for the traffic
    mean and the latency median, and the share of failed requests. Neutral by default."""

    lam_mult: float = 1.0
    latency_mult: float = 1.0
    error_rate: float = ERROR_RATE


NEUTRAL = Effect()


@dataclass
class Incident:
    incident_id: str
    type: str
    service_name: str
    degradation_start: float
    degradation_end: float  # when its effect stops; the scheduled end unless it ends early
    expected_trigger: str
    expected_action: str
    # start + max_duration. The gap and the rests count from here, however the incident
    # ends, so the schedule doesn't depend on when it ended (D-065).
    scheduled_end: float
    resolved_by: str | None = None  # "<agent_name> <action>" once an agent resolved it

    def effect(self) -> Effect:
        if self.type == BAD_DEPLOY:
            return Effect(latency_mult=BAD_DEPLOY_LATENCY_MULT, error_rate=BAD_DEPLOY_ERROR_RATE)
        if self.type == TRAFFIC_SURGE:
            return Effect(lam_mult=SURGE_LAMBDA_MULT)  # traffic only: latency untouched (D-059)
        return Effect(lam_mult=DROP_LAMBDA_MULT)


@dataclass(frozen=True)
class Decision:
    """An agent's decision, as ops-sim reads it from `remediation_decisions` (D-065): plain
    values, no Kafka or Avro types. `window_end` is in epoch seconds. No `target`: a resolved
    bad deploy rolls back to its previous version whatever the agent named, and `SCALE_OUT`
    leaves the replicas alone (`evaluate` scores targets, D-045)."""

    service_name: str
    window_end: float
    agent_name: str
    trigger: str
    action: str


@dataclass
class Event:
    """Something that happened this tick, for the console/log and the ground-truth file."""

    kind: str  # "bad_deploy" | "incident_start" | "incident_end"
    now: float
    service_name: str
    old_version: str | None = None
    new_version: str | None = None
    incident: Incident | None = None


@dataclass
class Engine:
    rng: object  # random.Random
    start: float
    config: Config = field(default_factory=Config)

    active: dict[str, Incident] = field(default_factory=dict, init=False)  # by service
    last_incident_end: dict[str, float] = field(default_factory=dict, init=False)  # by service
    last_type_end: dict[tuple[str, str], float] = field(default_factory=dict, init=False)  # by (service, type)
    seen: set[str] = field(default_factory=set, init=False)  # the types that have started
    incident_counter: int = field(default=0, init=False)  # last incident number used
    quiet_until: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self.quiet_until = next_window_boundary(self.start + self.config.quiet_period)

    # --- effects ---------------------------------------------------------------

    def effect_for(self, service_name: str, now: float) -> Effect:
        inc = self.active.get(service_name)
        if inc is not None and inc.degradation_start <= now < inc.degradation_end:
            return inc.effect()
        return NEUTRAL

    # --- resolve ---------------------------------------------------------------

    def resolve(self, decision: Decision, now: float) -> bool:
        """Resolve the active incident on the decision's service if the decision is its
        correct answer: the incident's expected action under its expected trigger, for a
        window that ends after the incident started (so an answer about an earlier incident
        can't end this one). The incident then ends on the tick at `now`, as it would at
        its cap. The first match wins: an incident already resolved ignores the decision.
        Returns whether it resolved one."""
        inc = self.active.get(decision.service_name)
        if (
            inc is None
            or inc.resolved_by is not None
            or decision.action != inc.expected_action
            or decision.trigger != inc.expected_trigger
            or decision.window_end <= inc.degradation_start
        ):
            return False
        inc.degradation_end = now
        inc.resolved_by = f"{decision.agent_name} {decision.action}"
        return True

    # --- tick ------------------------------------------------------------------

    def tick(self, now: float, deployments: dict[str, DeploymentState]) -> list[Event]:
        events: list[Event] = []
        self._end_incidents(now, deployments, events)
        self._start_incidents(now, deployments, events)
        return events

    def _end_incidents(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        # End: rollback a bad deploy; everything else just returns to baseline.
        for svc, inc in list(self.active.items()):
            if now < inc.degradation_end:
                continue
            if inc.type == BAD_DEPLOY:
                old, new = deployments[svc].rollback(now)
                events.append(Event("incident_end", now, svc, old, new, inc))
            else:
                events.append(Event("incident_end", now, svc, incident=inc))
            self.last_incident_end[svc] = inc.scheduled_end
            self.last_type_end[(svc, inc.type)] = inc.scheduled_end
            del self.active[svc]

    def _start_incidents(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        if self.config.no_incidents or now < self.quiet_until or not _first_tick_of_window(now):
            return
        # Every service that can start one does, so overlap across services is the norm.
        while (chosen := self._choose_incident(now)) is not None:
            self._start_incident(*chosen, now, deployments, events)

    def _start_incident(
        self, itype: str, svc: str, now: float, deployments: dict[str, DeploymentState], events: list[Event]
    ) -> None:
        # Whole seconds, so degradation_end falls on a tick: the incident really ends (and a
        # BAD_DEPLOY's ROLLBACK fires) exactly at the ended_at in its end record.
        scheduled_end = now + float(math.ceil(self.config.max_duration))
        self.incident_counter += 1
        inc = Incident(
            incident_id=f"INC{self.incident_counter:04d}",
            type=itype,
            service_name=svc,
            degradation_start=now,
            degradation_end=scheduled_end,
            expected_trigger=TRIGGER[itype],
            expected_action=EXPECTED_ACTION[itype],
            scheduled_end=scheduled_end,
        )
        # A BAD_DEPLOY's deploy and its degradation start on the same tick (D-042).
        if itype == BAD_DEPLOY:
            old, new = deployments[svc].deploy(now)
            events.append(Event("bad_deploy", now, svc, old, new, inc))
        events.append(Event("incident_start", now, svc, incident=inc))
        self.active[svc] = inc
        self.seen.add(itype)

    # --- choice helpers --------------------------------------------------------

    def _eligible(self, itype: str, now: float) -> list[str]:
        """The services an `itype` incident can start on now: no incident of their own,
        `gap` since their last one ended, and `itype`'s rest over on that service."""
        out = []
        for svc in CANDIDATES[itype]:
            if svc in self.active:
                continue
            if now - self.last_incident_end.get(svc, -math.inf) < self.config.gap:
                continue
            if now - self.last_type_end.get((svc, itype), -math.inf) < self.config.rests[itype]:
                continue
            out.append(svc)
        return out

    def _choose_incident(self, now: float) -> tuple[str, str] | None:
        """The next (type, service) to start now, or None. Until all three types have
        started, only the ones not seen yet are allowed (order random)."""
        types = [t for t in INCIDENT_TYPES if t not in self.seen] or INCIDENT_TYPES
        available = {t: elig for t in types if (elig := self._eligible(t, now))}
        if not available:
            return None
        types = list(available)
        weights = [INCIDENT_WEIGHTS[t] for t in types]
        itype = self.rng.choices(types, weights=weights, k=1)[0]
        svc = self.rng.choice(available[itype])
        return itype, svc
