"""Sampling primitives for normal operation (build brief section 4.2, D-058).

All randomness flows through a single `random.Random` passed in, so a `--seed` run
is fully reproducible as long as callers draw in a fixed order.

A `service_metrics` record sums up one second of a service's requests (D-058): how many
there were (`poisson`), their mean latency (`mean_latency_ms`, from per-request
`latency_ms` draws) and how many failed (`binomial`). The requests themselves are never
emitted.
"""

from __future__ import annotations

import math
import random

LATENCY_SIGMA = 0.3
# D-030: softened from the brief's 0.5% at 4x. With the lower rates (D-030), a single 4x
# outlier in a ~20-request window pushed the window average over the detector's bound.
SLOW_OUTLIER_PROB = 0.002
SLOW_OUTLIER_MULT = 3.0

# Share of requests that fail in normal operation. A BAD_DEPLOY raises it (opsgen.engine).
ERROR_RATE = 0.005


def poisson(lam: float, rng: random.Random) -> int:
    """Number of events this second — Poisson(lam) via Knuth. lam<=0 -> 0."""
    if lam <= 0:
        return 0
    target = math.exp(-lam)
    k = 0
    p = 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= target:
            return k - 1


def latency_ms(median: float, rng: random.Random) -> float:
    """One request's latency: log-normal with the given median and sigma=0.3;
    SLOW_OUTLIER_PROB (0.2%) of requests are slow outliers at SLOW_OUTLIER_MULT x the
    sampled value."""
    value = math.exp(rng.gauss(math.log(median), LATENCY_SIGMA))
    if rng.random() < SLOW_OUTLIER_PROB:
        value *= SLOW_OUTLIER_MULT
    return value


def mean_latency_ms(count: int, median: float, rng: random.Random) -> float:
    """The mean latency of `count` requests. A second without requests still gets a value
    (one draw), so a record's latency_ms is always set."""
    n = max(1, count)
    return sum(latency_ms(median, rng) for _ in range(n)) / n


def binomial(n: int, p: float, rng: random.Random) -> int:
    """How many of `n` independent trials succeed with probability `p`."""
    return sum(1 for _ in range(n) if rng.random() < p)
