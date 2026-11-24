"""The three River Shop services — the one place their defaults live.

Two kinds since D-068. The **metric services**, web-gateway and payments, produce one
`service_metrics` record a second and the `service_deployments` records (the bootstrap, the
5 s re-emit, deploys and rollbacks). **checkout** is the ticket service: it produces support
tickets, and no metrics or deploys. `SERVICES` lists all three in the wall's order.

A metric service's values are the build brief's (section 4.1), minus the services D-056
removed, except the traffic means (D-059): 50/30 requests/s. Each second, its
`service_metrics` record counts Poisson(`rps`) requests and averages their latencies around
`median_latency_ms` (D-058), so the topic carries two records per second whatever the
means, and replaying history costs the same at any traffic level. The means were chosen
offline (scripts/tuning, `.scratch/simplify-demo/research/tune.py`): calmer data shortens
the rests between incidents, rougher data cuts false alarms at a fresh start, and these
sit between the two while staying visibly noisy. payments gets BAD_DEPLOY and
UPSTREAM_DROP (a drop needs at least ~6 requests/s, D-031), and web-gateway, the busiest,
deploys on its own clock (good deploys, and BAD_DEPLOY_ERRORS, which only fail requests),
D-067.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Service:
    """A metric service: it produces metrics and deploys."""

    name: str
    tier: str  # CRITICAL | STANDARD
    owner_team: str
    initial_version: str
    median_latency_ms: float
    rps: float  # traffic mean: Poisson mean of traffic_count per second (D-059)
    ready_replicas: int


METRIC_SERVICES: tuple[Service, ...] = (
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
        name="payments",
        tier="CRITICAL",
        owner_team="payments",
        initial_version="2.9.4",
        median_latency_ms=220,
        rps=30,
        ready_replicas=2,
    ),
)

# The services that produce support tickets instead (D-068).
TICKET_SERVICES: tuple[str, ...] = ("checkout",)

# Every service by name, in the wall's order.
SERVICES: tuple[str, ...] = ("web-gateway", "checkout", "payments")

METRIC_SERVICES_BY_NAME: dict[str, Service] = {s.name: s for s in METRIC_SERVICES}


def metric_service_names() -> list[str]:
    return [s.name for s in METRIC_SERVICES]
