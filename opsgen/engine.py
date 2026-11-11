"""The schedule: benign deploys and incidents, as a per-service state machine.

Ticked once per simulated second. `Engine.tick(now, deployments)` advances the
schedule, mutates the `DeploymentState`s on deploys and rollbacks, and returns the
`Event`s that fired this tick. All scheduling randomness comes from the single rng,
drawn in a fixed order, so `--seed` reproduces the whole schedule.

Rules (brief section 4.5):
- One incident active at a time.
- Quiet period: no incidents in the first `quiet_period` seconds after start.
- Spacing: the next incident starts `spacing` (uniform) after the previous one ended.
- Duration: `duration` (uniform), counted from when degradation starts.
- Cooldown: a service can't have another incident within `cooldown` of its last ending.
- Pick the type by weight, then a random eligible candidate; if the chosen type has
  no eligible candidate, re-pick among types that do; if none do, skip the slot.
- BAD_DEPLOY fires a DEPLOY at t0 and degradation begins at t0 + onset_delay; it ends
  with a ROLLBACK and metrics return to baseline at that moment.
- Benign deploys: first at `benign_first`, then every `benign` (uniform), on a random
  service with no active incident that hasn't deployed in the last `benign_min_gap`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from opsgen.deployments import DeploymentState
from opsgen.services import SERVICES_BY_NAME, service_names

# Incident types, their weights, candidate services, trigger and expected action.
BAD_DEPLOY = "BAD_DEPLOY"
TRAFFIC_SURGE = "TRAFFIC_SURGE"
UPSTREAM_DROP = "UPSTREAM_DROP"

INCIDENT_TYPES = (BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP)
INCIDENT_WEIGHTS = {BAD_DEPLOY: 0.50, TRAFFIC_SURGE: 0.25, UPSTREAM_DROP: 0.25}
CANDIDATES = {
    BAD_DEPLOY: tuple(service_names()),
    TRAFFIC_SURGE: ("web-gateway", "catalog", "auth"),
    UPSTREAM_DROP: ("cart", "checkout", "payments"),
}
TRIGGER = {BAD_DEPLOY: "LATENCY_HIGH", TRAFFIC_SURGE: "TRAFFIC_HIGH", UPSTREAM_DROP: "TRAFFIC_LOW"}
EXPECTED_ACTION = {BAD_DEPLOY: "ROLLBACK", TRAFFIC_SURGE: "SCALE_OUT", UPSTREAM_DROP: "INVESTIGATE_UPSTREAM"}

# Metric effects during degradation.
BAD_DEPLOY_LATENCY_MULT = 4.0
BAD_DEPLOY_SERVER_ERROR_RATE = 0.12
SURGE_LAMBDA_MULT = 3.0
SURGE_LATENCY_MULT = 1.5
DROP_LAMBDA_MULT = 0.2

BAD_DEPLOY_ONSET_DELAY = 10.0  # seconds from the deploy to the start of degradation


@dataclass
class Config:
    """All timings in seconds. Defaults are the brief's; every one is CLI-overridable."""

    quiet_period: float = 180.0
    spacing_min: float = 240.0
    spacing_max: float = 360.0
    duration_min: float = 40.0
    duration_max: float = 70.0
    cooldown: float = 900.0
    benign_first: float = 60.0
    benign_min: float = 180.0
    benign_max: float = 300.0
    benign_min_gap: float = 300.0  # a service won't get a benign deploy within this of its last deploy
    no_incidents: bool = False


@dataclass
class Effect:
    """Metric multipliers applied to a service's traffic this tick. Neutral by default."""

    lam_mult: float = 1.0
    latency_mult: float = 1.0
    server_error_rate: float | None = None


NEUTRAL = Effect()


@dataclass
class Incident:
    incident_id: str
    type: str
    service_name: str
    t0: float
    degradation_start: float
    degradation_end: float
    expected_trigger: str
    expected_action: str
    bad_version: str | None = None
    good_version: str | None = None
    _deploy_fired: bool = False
    _start_fired: bool = False

    def effect(self) -> Effect:
        if self.type == BAD_DEPLOY:
            return Effect(latency_mult=BAD_DEPLOY_LATENCY_MULT, server_error_rate=BAD_DEPLOY_SERVER_ERROR_RATE)
        if self.type == TRAFFIC_SURGE:
            return Effect(lam_mult=SURGE_LAMBDA_MULT, latency_mult=SURGE_LATENCY_MULT)
        return Effect(lam_mult=DROP_LAMBDA_MULT)


@dataclass
class Event:
    """Something that happened this tick, for the console/log and the ground-truth file."""

    kind: str  # "benign_deploy" | "bad_deploy" | "incident_start" | "incident_end"
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
    next_benign_at: float = field(default=0.0, init=False)
    last_incident_end: dict[str, float] = field(default_factory=dict, init=False)
    last_deploy_at: dict[str, float] = field(default_factory=dict, init=False)
    incident_counter: int = field(default=0, init=False)  # last incident number used

    def __post_init__(self) -> None:
        self.next_incident_at = self.start + self.config.quiet_period
        self.next_benign_at = self.start + self.config.benign_first

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
        self._maybe_benign_deploy(now, deployments, events)
        return events

    def _advance_active(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        inc = self.active
        if inc is None:
            return
        # BAD_DEPLOY: fire the deploy at t0 (before degradation).
        if inc.type == BAD_DEPLOY and not inc._deploy_fired and now >= inc.t0:
            old, new = deployments[inc.service_name].deploy(now)
            inc.good_version, inc.bad_version = old, new
            self.last_deploy_at[inc.service_name] = now
            inc._deploy_fired = True
            events.append(Event("bad_deploy", now, inc.service_name, old, new, inc))
        # Degradation onset.
        if not inc._start_fired and now >= inc.degradation_start:
            inc._start_fired = True
            events.append(Event("incident_start", now, inc.service_name, incident=inc))
        # End: rollback a bad deploy; everything else just returns to baseline.
        if now >= inc.degradation_end:
            if inc.type == BAD_DEPLOY:
                old, new = deployments[inc.service_name].rollback(now)
                self.last_deploy_at[inc.service_name] = now
                events.append(Event("incident_end", now, inc.service_name, old, new, inc))
            else:
                events.append(Event("incident_end", now, inc.service_name, incident=inc))
            self.last_incident_end[inc.service_name] = now
            self.active = None
            self.next_incident_at = now + self._uniform(self.config.spacing_min, self.config.spacing_max)

    def _maybe_start_incident(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        if self.config.no_incidents or self.active is not None:
            return
        if now < self.start + self.config.quiet_period or now < self.next_incident_at:
            return
        chosen = self._choose_incident(now)
        if chosen is None:
            # No eligible candidate for any type right now; try again after a spacing gap.
            self.next_incident_at = now + self._uniform(self.config.spacing_min, self.config.spacing_max)
            return
        itype, svc = chosen
        # Whole seconds, so degradation_end falls on a tick: the incident really ends (and a
        # BAD_DEPLOY's ROLLBACK fires) exactly at the ended_at logged to incidents.jsonl.
        duration = float(math.ceil(self._uniform(self.config.duration_min, self.config.duration_max)))
        self.incident_counter += 1
        if itype == BAD_DEPLOY:
            t0 = now
            degradation_start = now + BAD_DEPLOY_ONSET_DELAY
        else:
            t0 = now
            degradation_start = now
        inc = Incident(
            incident_id=f"INC{self.incident_counter:04d}",
            type=itype,
            service_name=svc,
            t0=t0,
            degradation_start=degradation_start,
            degradation_end=degradation_start + duration,
            expected_trigger=TRIGGER[itype],
            expected_action=self._expected_action(itype, svc),
        )
        self.active = inc
        # The deploy for a BAD_DEPLOY and the onset both fire via _advance_active on
        # the next tick(s); run it once now so a 0-onset config still fires promptly.
        self._advance_active(now, deployments, events)

    def _maybe_benign_deploy(self, now: float, deployments: dict[str, DeploymentState], events: list[Event]) -> None:
        if now < self.next_benign_at:
            return
        svc = self._choose_benign_service(now)
        if svc is not None:
            old, new = deployments[svc].deploy(now)
            self.last_deploy_at[svc] = now
            events.append(Event("benign_deploy", now, svc, old, new))
        self.next_benign_at = now + self._uniform(self.config.benign_min, self.config.benign_max)

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

    def _choose_benign_service(self, now: float) -> str | None:
        active_svc = self.active.service_name if self.active is not None else None
        eligible = [
            name
            for name in SERVICES_BY_NAME
            if name != active_svc
            and now - self.last_deploy_at.get(name, float("-inf")) >= self.config.benign_min_gap
        ]
        return self.rng.choice(eligible) if eligible else None

    @staticmethod
    def _expected_action(itype: str, svc: str) -> str:
        # A surge on a service already at its replica ceiling can't scale out.
        if itype == TRAFFIC_SURGE and SERVICES_BY_NAME[svc].ready_replicas >= SERVICES_BY_NAME[svc].max_replicas:
            return "ESCALATE"
        return EXPECTED_ACTION[itype]

    def _uniform(self, lo: float, hi: float) -> float:
        return self.rng.uniform(lo, hi)
