"""Sampling primitives for normal operation (build brief section 4.2).

All randomness flows through a single `random.Random` passed in, so a `--seed` run
is fully reproducible as long as callers draw in a fixed order.
"""

from __future__ import annotations

import math
import random

LATENCY_SIGMA = 0.3
# D-030: softened from the brief's 0.5% at 4x. With the lower rates (D-030), a single 4x
# outlier in a ~20-request window pushed the window average over the detector's bound.
SLOW_OUTLIER_PROB = 0.002
SLOW_OUTLIER_MULT = 3.0

# Normal-operation status mix.
CLIENT_ERROR_RATE = 0.02
SERVER_ERROR_RATE = 0.005

# Server-error code mixes: normal vs. a BAD_DEPLOY ("mostly 500 and 503").
SERVER_CODES_NORMAL = ([500, 502, 503], [0.5, 0.3, 0.2])
SERVER_CODES_BAD_DEPLOY = ([500, 503, 502], [0.45, 0.4, 0.15])

_HEX = "0123456789abcdef"


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
    """Log-normal latency with the given median and sigma=0.3; SLOW_OUTLIER_PROB (0.2%) of requests are
    slow outliers at SLOW_OUTLIER_MULT x the sampled value."""
    value = math.exp(rng.gauss(math.log(median), LATENCY_SIGMA))
    if rng.random() < SLOW_OUTLIER_PROB:
        value *= SLOW_OUTLIER_MULT
    return value


def status_code(success_code: int, is_auth: bool, rng: random.Random, server_error_rate: float | None = None) -> int:
    """A status code. `server_error_rate` overrides the normal 0.5% during a BAD_DEPLOY.
    Client errors stay at 2% (404, plus 401 on auth); the rest succeed."""
    server = SERVER_ERROR_RATE if server_error_rate is None else server_error_rate
    r = rng.random()
    if r < server:
        codes, weights = SERVER_CODES_NORMAL if server_error_rate is None else SERVER_CODES_BAD_DEPLOY
        return rng.choices(codes, weights=weights, k=1)[0]
    if r < server + CLIENT_ERROR_RATE:
        if is_auth:
            return rng.choices([401, 404], weights=[0.7, 0.3], k=1)[0]
        return 404
    return success_code


def response_bytes(base_bytes: int, is_error: bool, rng: random.Random) -> int:
    """Error responses are ~200 bytes; successful ones are the route base ±20%."""
    if is_error:
        return int(200 * rng.uniform(0.9, 1.1))
    return int(base_bytes * rng.uniform(0.8, 1.2))


def trace_id(rng: random.Random) -> str:
    """32 random hex characters."""
    return "".join(rng.choices(_HEX, k=32))
