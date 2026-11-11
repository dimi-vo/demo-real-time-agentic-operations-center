"""The six River Shop services and their routes — the one place their defaults live.

These are the values from the build brief (section 4.1), except the request rates, which
are the brief's divided by about 3 (D-030): ~29 requests/s in total instead of ~86, so a
statement that replays history (the service_health CTAS, a drop-and-recreate) does a
third of the work, while every incident type stays detectable in 10-second windows.
`auth` deliberately runs at its replica ceiling (ready == max == 4) so a traffic surge on
`auth` exercises the capacity agent's ESCALATE path instead of SCALE_OUT.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    """One templated route. `weight` is relative (normalized per service when picked);
    `base_bytes` is the nominal successful-response size before ±20% jitter."""

    method: str
    path: str
    weight: float
    base_bytes: int

    @property
    def success_code(self) -> int:
        """Status for a successful response: 201 for POST, 204 for DELETE, else 200."""
        if self.method == "POST":
            return 201
        if self.method == "DELETE":
            return 204
        return 200


@dataclass(frozen=True)
class Service:
    name: str
    tier: str  # CRITICAL | STANDARD
    owner_team: str
    initial_version: str
    median_latency_ms: float
    rps: float  # Poisson mean requests per second (lambda); brief value / ~3 (D-030)
    ready_replicas: int
    max_replicas: int
    routes: tuple[Route, ...]


SERVICES: tuple[Service, ...] = (
    Service(
        name="web-gateway",
        tier="CRITICAL",
        owner_team="edge",
        initial_version="3.4.2",
        median_latency_ms=35,
        rps=8,
        ready_replicas=4,
        max_replicas=12,
        routes=(
            Route("GET", "/", 60, 1200),
            Route("GET", "/api/v1/home", 30, 3500),
            Route("GET", "/static/{asset}", 10, 9000),
        ),
    ),
    Service(
        name="auth",
        tier="CRITICAL",
        owner_team="identity",
        initial_version="2.1.7",
        median_latency_ms=20,
        rps=5,
        ready_replicas=4,
        max_replicas=4,  # at the ceiling on purpose -> surge -> ESCALATE
        routes=(
            Route("POST", "/api/v1/login", 60, 420),
            Route("POST", "/api/v1/token/refresh", 30, 260),
            Route("GET", "/api/v1/session", 10, 540),
        ),
    ),
    Service(
        name="catalog",
        tier="STANDARD",
        owner_team="discovery",
        initial_version="5.0.3",
        median_latency_ms=60,
        rps=7,
        ready_replicas=3,
        max_replicas=10,
        routes=(
            Route("GET", "/api/v1/products", 60, 4200),
            Route("GET", "/api/v1/products/{id}", 30, 1600),
            Route("GET", "/api/v1/search", 10, 2800),
        ),
    ),
    Service(
        name="cart",
        tier="STANDARD",
        owner_team="shopping",
        initial_version="1.8.0",
        median_latency_ms=45,
        rps=4,
        ready_replicas=2,
        max_replicas=6,
        routes=(
            Route("GET", "/api/v1/cart", 60, 950),
            Route("POST", "/api/v1/cart/items", 30, 320),
            Route("DELETE", "/api/v1/cart/items/{id}", 10, 180),
        ),
    ),
    Service(
        name="checkout",
        tier="CRITICAL",
        owner_team="payments",
        initial_version="4.2.1",
        median_latency_ms=140,
        rps=3,
        ready_replicas=3,
        max_replicas=8,
        routes=(
            Route("POST", "/api/v1/checkout", 60, 760),
            Route("GET", "/api/v1/checkout/{id}", 40, 1180),
        ),
    ),
    Service(
        name="payments",
        tier="CRITICAL",
        owner_team="payments",
        initial_version="2.9.4",
        median_latency_ms=220,
        rps=2,
        ready_replicas=2,
        max_replicas=6,
        routes=(
            Route("POST", "/api/v1/payments", 60, 480),
            Route("GET", "/api/v1/payments/{id}", 40, 820),
        ),
    ),
)

SERVICES_BY_NAME: dict[str, Service] = {s.name: s for s in SERVICES}


def service_names() -> list[str]:
    return [s.name for s in SERVICES]


def pick_route(service: Service, rng) -> Route:
    """Weighted choice of a route for this service."""
    return rng.choices(service.routes, weights=[r.weight for r in service.routes], k=1)[0]
