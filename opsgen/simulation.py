"""Ties the engine, deployment state and request generation into per-second ticks.

`Simulation` is clock-agnostic: the live driver feeds it `time.time()`, the
`--simulate` driver feeds it a fast synthetic clock. It never sleeps, never touches
Kafka, and never prints — it just turns a timestamp into the rows that would be
produced. Record dicts carry epoch-millisecond timestamps (what Kafka wants); sinks
format them for display.
"""

from __future__ import annotations

import random

from opsgen.deployments import DeploymentState
from opsgen.distributions import latency_ms, poisson, response_bytes, status_code, trace_id
from opsgen.engine import Config, Engine, Event
from opsgen.instances import instance_ids
from opsgen.services import SERVICES, pick_route

REEMIT_INTERVAL = 5  # seconds; the controller reconciliation loop re-emits all services
TICK_DURATION = 1.0  # seconds per tick


def to_millis(epoch_seconds: float) -> int:
    return int(epoch_seconds * 1000)


class Simulation:
    """One generator run. Two independent RNG streams derived from `seed`: the
    scheduler's (`Random(seed)`) and the traffic's (`Random(seed+1)`), so the incident
    and deploy schedule is reproducible and independent of how much traffic is drawn.
    `seed=None` seeds both from the OS (not reproducible)."""

    def __init__(
        self,
        seed: int | None,
        start: float,
        config: Config | None = None,
        initial_state: dict[str, dict] | None = None,
    ) -> None:
        self.start = start
        self.start_ms = round(start * 1000)
        self.config = config or Config()
        self.sched_rng = random.Random(seed)
        self.traffic_rng = random.Random(None if seed is None else seed + 1)
        self.deployments: dict[str, DeploymentState] = {}
        for svc in SERVICES:
            state = DeploymentState.initial(svc, start)
            if initial_state and svc.name in initial_state:
                saved = initial_state[svc.name]
                state.version = saved.get("version", state.version)
                state.previous_version = saved.get("previous_version", state.previous_version)
                state.deployed_at = saved.get("deployed_at", state.deployed_at)
                state.last_change = saved.get("last_change", state.last_change)
            self.deployments[svc.name] = state
        self.engine = Engine(rng=self.sched_rng, start=start, config=self.config)

    # --- records ---------------------------------------------------------------

    def _dep_record(self, service_name: str, now: float) -> dict:
        d = self.deployments[service_name]
        return {
            "service_name": d.service_name,
            "version": d.version,
            "previous_version": d.previous_version,
            "deployed_at": self.tick_ms(d.deployed_at),
            "last_change": d.last_change,
            "desired_replicas": d.desired_replicas,
            "ready_replicas": d.ready_replicas,
            "tier": d.tier,
            "owner_team": d.owner_team,
            "event_time": self.tick_ms(now),
        }

    def bootstrap(self, now: float | None = None) -> list[dict]:
        """The current state of every service, emitted first on every start."""
        now = self.start if now is None else now
        return [self._dep_record(name, now) for name in self.deployments]

    def tick_ms(self, t: float) -> int:
        """Epoch milliseconds for a tick time, on this run's integer grid
        (`start_ms + 1000 * k`), so consecutive ticks are exactly 1000 ms apart. Times
        that aren't ticks of this run (a deployed_at restored from an earlier run) are
        rounded instead."""
        k = round(t - self.start)
        if abs((t - self.start) - k) < 1e-6:
            return self.start_ms + 1000 * k
        return round(t * 1000)

    def _gen_requests(self, now: float) -> list[dict]:
        # Every request of this tick gets a timestamp in [tick_ms, tick_ms + 999]: never
        # the next tick's millisecond, where a deploy record for the next version may sit
        # (truncating float seconds could produce exactly that collision; see D-028).
        tick_ms = self.tick_ms(now)
        out: list[dict] = []
        for svc in SERVICES:
            dep = self.deployments[svc.name]
            eff = self.engine.effect_for(svc.name, now)
            count = poisson(svc.rps * eff.lam_mult, self.traffic_rng)
            if count == 0:
                continue
            insts = instance_ids(svc.name, dep.version, dep.ready_replicas)
            is_auth = svc.name == "auth"
            for _ in range(count):
                route = pick_route(svc, self.traffic_rng)
                code = status_code(route.success_code, is_auth, self.traffic_rng, eff.server_error_rate)
                lat = latency_ms(svc.median_latency_ms * eff.latency_mult, self.traffic_rng)
                is_err = code >= 400
                rbytes = response_bytes(route.base_bytes, is_err, self.traffic_rng)
                tid = trace_id(self.traffic_rng)
                inst = self.traffic_rng.choice(insts)
                offset_ms = min(999, int(self.traffic_rng.uniform(0, TICK_DURATION) * 1000))
                out.append(
                    {
                        "event_time": tick_ms + offset_ms,
                        "service_name": svc.name,
                        "instance_id": inst,
                        "http_method": route.method,
                        "route": f"{route.method} {route.path}",
                        "status_code": code,
                        "latency_ms": round(lat, 3),
                        "response_bytes": rbytes,
                        "trace_id": tid,
                    }
                )
        return out

    # --- tick ------------------------------------------------------------------

    def tick(self, now: float) -> tuple[list[dict], list[dict], list[Event]]:
        """Advance one tick. Returns (request_records, deployment_records, events)."""
        events = self.engine.tick(now, self.deployments)
        changed = {e.service_name for e in events if e.new_version is not None}
        dep_records: list[dict] = []
        emitted: set[str] = set()
        for name in changed:
            dep_records.append(self._dep_record(name, now))
            emitted.add(name)
        k = round(now - self.start)
        if k != 0 and k % REEMIT_INTERVAL == 0:
            for name in self.deployments:
                if name not in emitted:
                    dep_records.append(self._dep_record(name, now))
        requests = self._gen_requests(now)
        return requests, dep_records, events

    def state_snapshot(self) -> dict[str, dict]:
        """The persistable deployment state (versions, deployed_at) per service."""
        return {
            name: {
                "version": d.version,
                "previous_version": d.previous_version,
                "deployed_at": d.deployed_at,
                "last_change": d.last_change,
            }
            for name, d in self.deployments.items()
        }
