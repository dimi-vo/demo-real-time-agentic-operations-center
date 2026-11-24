"""The three River Shop services — the one place their defaults live.

These are the values from the build brief (section 4.1), minus the services D-056 removed,
except the traffic means (D-059): 50/30/30 requests/s. Each second, a service's
`service_metrics` record counts Poisson(`rps`) requests and averages their latencies around
`median_latency_ms` (D-058), so the topic carries three records per second whatever the
means, and replaying history costs the same at any traffic level. The means were chosen
offline (scripts/tuning, `.scratch/simplify-demo/research/tune.py`): calmer data shortens
the rests between incidents, rougher data cuts false alarms at a fresh start, and 50/30/30
sits between the two while staying visibly noisy. payments gets BAD_DEPLOY and
UPSTREAM_DROP (a drop needs at least ~6 requests/s, D-031), and web-gateway, the busiest,
deploys on its own clock (good deploys, and BAD_DEPLOY_ERRORS, which only fail requests),
D-067. checkout gets no incident: it becomes a ticket pipeline, and traffic surges, its other
incident type, are retired (D-068).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Service:
    name: str
    tier: str  # CRITICAL | STANDARD
    owner_team: str
    initial_version: str
    median_latency_ms: float
    rps: float  # traffic mean: Poisson mean of traffic_count per second (D-059)
    ready_replicas: int


SERVICES: tuple[Service, ...] = (
    Service(
        name="web-gateway",
        tier="CRITICAL",
        owner_team="edge",
        initial_version="3.4.2",
        median_latency_ms=35,
        rps=50,
        ready_replicas=4,
    ),
    Service(
        name="checkout",
        tier="CRITICAL",
        owner_team="payments",
        initial_version="4.2.1",
        median_latency_ms=140,
        rps=30,
        ready_replicas=3,
    ),
    Service(
        name="payments",
        tier="CRITICAL",
        owner_team="payments",
        initial_version="2.9.4",
        median_latency_ms=220,
        rps=30,
        ready_replicas=2,
    ),
)

SERVICES_BY_NAME: dict[str, Service] = {s.name: s for s in SERVICES}


def service_names() -> list[str]:
    return [s.name for s in SERVICES]
