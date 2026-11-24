"""Ties the engine, deployment state, metrics generation and the ticket clock into per-second
ticks.

`Simulation` is clock-agnostic: the live driver feeds it `time.time()`, the
`--simulate` driver feeds it a fast synthetic clock. It never sleeps, never touches
Kafka, and never prints — it just turns a timestamp into the rows that would be
produced. Record dicts carry epoch-millisecond timestamps (what Kafka wants); sinks
format them for display.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from typing import NamedTuple

from opsgen.deployments import DeploymentState, highest
from opsgen.distributions import binomial, mean_latency_ms, poisson
from opsgen.engine import Config, Decision, Engine, Event
from opsgen.services import METRIC_SERVICES
from opsgen.tickets import SOURCES, NewTicket, TicketClock

REEMIT_INTERVAL = 5  # seconds; the controller reconciliation loop re-emits every metric service
# A metrics record covers one tick and is stamped at its end: tick + 999 ms (D-058).
SAMPLE_OFFSET_MS = 999


def to_millis(epoch_seconds: float) -> int:
    return int(epoch_seconds * 1000)


class Tick(NamedTuple):
    """What one tick produced."""

    metrics: list[dict]  # service_metrics records, one per metric service
    deployments: list[dict]  # service_deployments records: version changes and the 5 s re-emit
    events: list[Event]  # incident starts and ends, and deploys
    tickets: list[NewTicket]  # each a support_tickets record and its ticket_answer_key record


class Simulation:
    """One generator run. Four independent RNG streams derived from `seed`:
    - the scheduler's, `Random(seed)`: payments' incidents;
    - the traffic's, `Random(seed + 1)`: the metric values;
    - web-gateway's deploy clock, `Random(seed + 2)` (D-067);
    - checkout's ticket clock, `Random(seed + 3)` (D-068).
    So each schedule is reproducible, and independent of how much the others draw.
    `seed=None` seeds them all from the OS (not reproducible)."""

    def __init__(
        self,
        seed: int | None,
        start: float,
        config: Config | None = None,
        initial_state: dict | None = None,
    ) -> None:
        self.start = start
        self.start_ms = round(start * 1000)
        self.config = config or Config()
        self.sched_rng = random.Random(seed)
        self.traffic_rng = random.Random(None if seed is None else seed + 1)
        # Only the metric services deploy (D-068). A state file from before D-068 also has
        # checkout's versions; they are ignored.
        saved_services = (initial_state or {}).get("services", {})
        self.deployments: dict[str, DeploymentState] = {}
        for svc in METRIC_SERVICES:
            state = DeploymentState.initial(svc, start)
            if svc.name in saved_services:
                saved = saved_services[svc.name]
                state.version = saved.get("version", state.version)
                state.previous_version = saved.get("previous_version", state.previous_version)
                state.deployed_at = saved.get("deployed_at", state.deployed_at)
                state.last_change = saved.get("last_change", state.last_change)
                # A state file from before D-067 has no highest_version: after a rollback,
                # previous_version holds the rolled-back one, so the higher of the two is it.
                state.highest_version = highest(saved.get("highest_version"), state.version, state.previous_version)
            self.deployments[svc.name] = state
        # web-gateway's deploy clock has its own rng, so it never moves the shared schedule (D-067)
        self.deploy_rng = random.Random(None if seed is None else seed + 2)
        self.engine = Engine(rng=self.sched_rng, start=start, config=self.config, deploy_rng=self.deploy_rng)
        # checkout's ticket clock has its own rng too, so tickets never move another service's
        # schedule (D-068). --no-incidents turns it off. Its id counters, one per source,
        # continue across restarts like the incident counter; a state file without them (from
        # before D-068) starts at the defaults.
        self.ticket_rng = random.Random(None if seed is None else seed + 3)
        saved_numbers = (initial_state or {}).get("next_ticket_numbers", {})
        self.ticket_clock = TicketClock(
            rng=self.ticket_rng,
            next_numbers={s.code: int(saved_numbers.get(s.code, s.first_number)) for s in SOURCES},
            on=not self.config.no_incidents,
        )
        # Continue incident numbering across restarts: ops-wall pairs each incident's start
        # and end records by incident_id, so ids must never repeat (D-046).
        self.engine.incident_counter = int((initial_state or {}).get("incident_counter", 0))

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
        """The current state of every metric service, emitted first on every start."""
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

    def _gen_metrics(self, now: float) -> list[dict]:
        """One metrics sample per metric service: the second that starts at `now`, summed up
        (D-058). An incident changes the values, never the number of records.

        The record is stamped at the end of its second (tick + 999 ms): after a deploy
        record on the same tick (D-028), so the temporal join gives it the new version, and
        never on the next tick's millisecond, where the next deploy record may sit."""
        event_time = self.tick_ms(now) + SAMPLE_OFFSET_MS
        out: list[dict] = []
        for svc in METRIC_SERVICES:
            eff = self.engine.effect_for(svc.name, now)
            count = poisson(svc.rps * eff.lam_mult, self.traffic_rng)
            latency = mean_latency_ms(count, svc.median_latency_ms * eff.latency_mult, self.traffic_rng)
            out.append(
                {
                    "event_time": event_time,
                    "service_name": svc.name,
                    "traffic_count": count,
                    "latency_ms": round(latency, 3),
                    "error_count": binomial(count, eff.error_rate, self.traffic_rng),
                }
            )
        return out

    # --- tick ------------------------------------------------------------------

    def tick(self, now: float, decisions: Iterable[Decision] = ()) -> Tick:
        """Advance one tick. Each of the `Tick`'s tickets is its `support_tickets` record and
        its `ticket_answer_key` record, both stamped with the tick.

        `decisions` are the agent decisions read since the last tick. They are applied first
        (D-065), so a decision that resolves an incident ends it on this tick: this tick's
        metrics are normal, and its end (and a bad deploy's ROLLBACK) is in this tick's
        events and records."""
        for decision in decisions:
            self.engine.resolve(decision, now)
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
        metrics = self._gen_metrics(now)
        tickets = self.ticket_clock.tick(self.tick_ms(now))
        return Tick(metrics, dep_records, events, tickets)

    def state_snapshot(self) -> dict:
        """The persistable state: each metric service's deployment (versions, the highest
        version it has run, deployed_at), the last incident number used, and the next ticket
        number of each source (D-068)."""
        return {
            "services": {
                name: {
                    "version": d.version,
                    "previous_version": d.previous_version,
                    "highest_version": d.highest_version,
                    "deployed_at": d.deployed_at,
                    "last_change": d.last_change,
                }
                for name, d in self.deployments.items()
            },
            "incident_counter": self.engine.incident_counter,
            "next_ticket_numbers": dict(self.ticket_clock.next_numbers),
        }
