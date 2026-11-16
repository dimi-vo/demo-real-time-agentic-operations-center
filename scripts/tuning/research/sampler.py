"""A fast sampler for one 10-s window of the service_metrics data model (D-058).

One record per service per second: the window's traffic is the SUM of 10 per-second Poisson
counts, its latency the plain AVG of 10 per-second latencies (each the mean of that second's
requests, or one log-normal draw when the second had none). Latencies follow opsgen's model
(log-normal sigma 0.3, 0.2% slow outliers x3): exact draws for small counts, a 3-moment shifted
gamma for large ones. `tune.py` builds its data from `window()` instead of running
`opsgen.Simulation`, which lets it vary means and spike sizes freely; D-059 checked that the
two agree (means within 0.1%, spreads within 3%).
"""

from __future__ import annotations

import math

from opsgen.distributions import latency_ms, poisson

SIG = 0.3


def poisson_w(lam, rng):
    """Poisson(lam) for one 10-s window (= sum of 10 per-second draws). Knuth below 10, PTRS above."""
    if lam < 10:
        return poisson(lam, rng)
    slam, loglam = math.sqrt(lam), math.log(lam)
    b = 0.931 + 2.53 * slam
    a = -0.059 + 0.02483 * b
    invalpha = 1.1239 + 1.1328 / (b - 3.4)
    vr = 0.9277 - 3.6224 / (b - 2)
    while True:
        u = rng.random() - 0.5
        v = rng.random()
        us = 0.5 - abs(u)
        k = math.floor((2 * a / us + b) * u + lam + 0.43)
        if us >= 0.07 and v <= vr:
            return k
        if k < 0 or (us < 0.013 and v > us):
            continue
        if math.log(v) + math.log(invalpha) - math.log(a / (us * us) + b) <= -lam + k * loglam - math.lgamma(k + 1):
            return k


def latency_sum(n, med, rng):
    """Sum of n opsgen latencies (log-normal 0.3, 0.2% x3 outliers): exact up to 200, else the
    log-normal part by a 3-moment shifted gamma plus exact outliers."""
    if n <= 200:
        return sum(latency_ms(med, rng) for _ in range(n))
    e = math.exp(SIG * SIG)
    m1, v1 = med * math.sqrt(e), med * med * e * (e - 1)
    g1 = (e + 2) * math.sqrt(e - 1)
    mean, var, skew = n * m1, n * v1, g1 / math.sqrt(n)
    k = 4 / (skew * skew)
    theta = math.sqrt(var / k)
    total = mean - k * theta + rng.gammavariate(k, theta)
    for _ in range(poisson(0.002 * n, rng)):  # outliers add 2x their own (log-normal) value
        total += 2 * math.exp(rng.gauss(math.log(med), SIG))
    return total


def lat_sum(n, med, rng):
    if n <= 40:
        return sum(latency_ms(med, rng) for _ in range(n))
    return latency_sum(n, med, rng) if n > 200 else _gamma_sum(n, med, rng)


def _gamma_sum(n, med, rng):
    e = math.exp(0.09)
    m1, v1, g1 = med * math.sqrt(e), med * med * e * (e - 1), (e + 2) * math.sqrt(e - 1)
    skew = g1 / math.sqrt(n)
    k = 4 / (skew * skew)
    theta = math.sqrt(n * v1 / k)
    total = n * m1 - k * theta + rng.gammavariate(k, theta)
    for _ in range(poisson(0.002 * n, rng)):
        total += 2 * math.exp(rng.gauss(math.log(med), 0.3))
    return total


def window(med, rps, lm, latm, rng):
    """One 10-s window from 10 per-second records: (SUM traffic_count, AVG latency_ms).

    `med` is the service's median latency (ms), `rps` its traffic mean (requests/s), `lm` and
    `latm` the incident's traffic and latency multipliers (1.0 outside incidents)."""
    tot, lat = 0, 0.0
    for _ in range(10):
        n = poisson_w(rps * lm, rng)
        tot += n
        lat += lat_sum(n, med * latm, rng) / n if n else math.exp(rng.gauss(math.log(med * latm), 0.3))
    return tot, lat / 10
