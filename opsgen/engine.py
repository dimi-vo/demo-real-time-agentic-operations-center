"""The schedule: incidents, as a per-service state machine.

Ticked once per simulated second. `Engine.tick(now, deployments)` advances the
schedule, mutates the `DeploymentState`s on deploys and rollbacks, and returns the
`Event`s that fired this tick. All scheduling randomness comes from the single rng,
drawn in a fixed order, so `--seed` reproduces the whole schedule.

Rules (brief section 4.5, simplified by D-042):
- One incident active at a time.
- Quiet period: no incidents in the first `quiet_period` seconds after start.
- Spacing: the next incident starts `spacing` (uniform) after the previous one ended.
- Every incident starts on a 10-second window boundary: the end of the quiet period and
  every spacing are rounded up to one. With a fixed `duration` (20 s, D-059) an incident
  covers exactly 2 whole windows and ends on a boundary too.
- Cooldown: a service can't have another incident within `cooldown` of its last ending.
- Pick the type by weight, then a random eligible candidate; if the chosen type has
  no eligible candidate, re-pick among types that do; if none do, skip the slot.
- BAD_DEPLOY fires a DEPLOY and starts degrading on the same tick; it ends with a
  ROLLBACK and metrics return to baseline at that moment. These are the only version
  changes: there are no other deploys (D-057).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from opsgen.deployments import DeploymentState
from opsgen.distributions import ERROR_RATE
from opsgen.services import service_names

# Incident types, their weights, candidate services, trigger and expected action. Every
# type has exactly one expected action, whatever the service (D-043).
BAD_DEPLOY = "BAD_DEPLOY"
TRAFFIC_SURGE = "TRAFFIC_SURGE"
UPSTREAM_DROP = "UPSTREAM_DROP"

INCIDENT_TYPES = (BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP)
INCIDENT_WEIGHTS = {BAD_DEPLOY: 0.50, TRAFFIC_SURGE: 0.25, UPSTREAM_DROP: 0.25}
CANDIDATES = {
    BAD_DEPLOY: tuple(service_names()),
    TRAFFIC_SURGE: ("web-gateway",),
    UPSTREAM_DROP: ("checkout", "payments"),
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


@dataclass
class Config:
    """All timings in seconds. Every one is CLI-overridable."""

    quiet_period: float = 180.0
    spacing_min: float = 240.0
    spacing_max: float = 360.0
    # D-059 (was 40 s, D-042): fixed, so every incident covers 2 whole windows. The detector
    # flags a spike in its first window; more windows only add after-spike false alarms.
    duration: float = 20.0
    cooldown: float = 420.0  # D-042: was the brief's 900
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
    degradation_end: float
    expected_trigger: str
    expected_action: str

    def effect(self) -> Effect:
        if self.type == BAD_DEPLOY:
            return Effect(latency_mult=BAD_DEPLOY_LATENCY_MULT, error_rate=BAD_DEPLOY_ERROR_RATE)
        if self.type == TRAFFIC_SURGE:
            return Effect(lam_mult=SURGE_LAMBDA_MULT)  # traffic only: latency untouched (D-059)
        return Effect(lam_mult=DROP_LAMBDA_MULT)


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

    active: Incident | None = field(default=None, init=False)
    next_incident_at: float = field(default=0.0, init=False)
    last_incident_end: dict[str, float] = field(default_factory=dict, init=False)
    incident_counter: int = field(default=0, init=False)  # last incident number used

    def __post_init__(self) -> None:
        self.next_incident_at = next_window_boundary(self.start + self.config.quiet_period)

    # --- effects ---------------------------------------------------------------

    def effect_for(self, service_name: str, now: float) -> Effect:
        inc = self.active
        if inc is not None and inc.service_name == service_name and inc.degradation_start <= now < inc.degradation_end:
            return inc.effect()
        return NEUTRAL

    # --- tick ------------------------------------------------------------------

    def tick(self, now: float, deployments: dict[str, DeploymentState]) -> list[Event]:
        events: list[Event] = []
        self._advance_active(now, deployments, events)
        self._maybe_start_incident(now, deployments, events)
        return events

    def _advance_active(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        inc = self.active
        if inc is None:
            return
        # End: rollback a bad deploy; everything else just returns to baseline.
        if now >= inc.degradation_end:
            if inc.type == BAD_DEPLOY:
                old, new = deployments[inc.service_name].rollback(now)
                events.append(Event("incident_end", now, inc.service_name, old, new, inc))
            else:
                events.append(Event("incident_end", now, inc.service_name, incident=inc))
            self.last_incident_end[inc.service_name] = now
            self.active = None
            self._schedule_next_incident(now)

    def _schedule_next_incident(self, now: float) -> None:
        """The next incident slot: a spacing gap after `now`, rounded up to a window
        boundary so the incident covers whole windows only."""
        gap = self._uniform(self.config.spacing_min, self.config.spacing_max)
        self.next_incident_at = next_window_boundary(now + gap)

    def _maybe_start_incident(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        if self.config.no_incidents or self.active is not None:
            return
        if now < self.start + self.config.quiet_period or now < self.next_incident_at:
            return
        chosen = self._choose_incident(now)
        if chosen is None:
            # No eligible candidate for any type right now; try again after a spacing gap.
            self._schedule_next_incident(now)
            return
        itype, svc = chosen
        # Whole seconds, so degradation_end falls on a tick: the incident really ends (and a
        # BAD_DEPLOY's ROLLBACK fires) exactly at the ended_at in its end record.
        duration = float(math.ceil(self.config.duration))
        self.incident_counter += 1
        inc = Incident(
            incident_id=f"INC{self.incident_counter:04d}",
            type=itype,
            service_name=svc,
            degradation_start=now,
            degradation_end=now + duration,
            expected_trigger=TRIGGER[itype],
            expected_action=EXPECTED_ACTION[itype],
        )
        # A BAD_DEPLOY's deploy and its degradation start on the same tick (D-042).
        if itype == BAD_DEPLOY:
            old, new = deployments[svc].deploy(now)
            events.append(Event("bad_deploy", now, svc, old, new, inc))
        events.append(Event("incident_start", now, svc, incident=inc))
        self.active = inc

    # --- choice helpers --------------------------------------------------------

    def _eligible(self, itype: str, now: float) -> list[str]:
        out = []
        for svc in CANDIDATES[itype]:
            last_end = self.last_incident_end.get(svc)
            if last_end is not None and now - last_end < self.config.cooldown:
                continue
            out.append(svc)
        return out

    def _choose_incident(self, now: float) -> tuple[str, str] | None:
        available = {t: elig for t in INCIDENT_TYPES if (elig := self._eligible(t, now))}
        if not available:
            return None
        types = list(available)
        weights = [INCIDENT_WEIGHTS[t] for t in types]
        itype = self.rng.choices(types, weights=weights, k=1)[0]
        svc = self.rng.choice(available[itype])
        return itype, svc

    def _uniform(self, lo: float, hi: float) -> float:
        return self.rng.uniform(lo, hi)
