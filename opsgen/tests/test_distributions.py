"""Distributions behave as section 4.2 specifies."""

import random
import statistics

from opsgen import distributions as d


def test_poisson_mean_matches_lambda():
    rng = random.Random(1)
    samples = [d.poisson(5.0, rng) for _ in range(20000)]
    assert abs(statistics.mean(samples) - 5.0) < 0.15
    assert d.poisson(0.0, rng) == 0
    assert d.poisson(-3.0, rng) == 0


def test_poisson_never_negative():
    rng = random.Random(2)
    assert all(d.poisson(1.2, rng) >= 0 for _ in range(2000))


def test_latency_median_matches_target():
    rng = random.Random(3)
    samples = [d.latency_ms(100.0, rng) for _ in range(20000)]
    # Median is robust to the 0.5% slow outliers; mean is pulled above the median.
    assert abs(statistics.median(samples) - 100.0) < 8
    assert statistics.mean(samples) > statistics.median(samples)
    assert min(samples) > 0


def test_latency_has_slow_outliers():
    rng = random.Random(4)
    samples = [d.latency_ms(100.0, rng) for _ in range(20000)]
    # ~0.5% at 4x the sampled value -> a meaningful tail beyond ~3x the median.
    assert sum(1 for s in samples if s > 300) > 20


def test_mean_latency_is_the_mean_of_the_seconds_requests():
    # Same draws in the same order: the mean of `count` latency_ms draws.
    rng = random.Random(5)
    expected = sum(d.latency_ms(100.0, rng) for _ in range(7)) / 7
    assert d.mean_latency_ms(7, 100.0, random.Random(5)) == expected
    # A second without requests still gets one draw, so latency_ms is always set.
    assert d.mean_latency_ms(0, 100.0, random.Random(5)) == d.latency_ms(100.0, random.Random(5))


def test_mean_latency_spread_shrinks_with_traffic():
    rng = random.Random(6)
    one = [d.mean_latency_ms(1, 100.0, rng) for _ in range(5000)]
    many = [d.mean_latency_ms(25, 100.0, rng) for _ in range(5000)]
    assert statistics.pstdev(many) < statistics.pstdev(one) / 3


def test_binomial_counts_failures_at_the_rate():
    rng = random.Random(7)
    n = 2000
    assert abs(statistics.mean(d.binomial(100, d.ERROR_RATE, rng) for _ in range(n)) - 0.5) < 0.1
    assert abs(statistics.mean(d.binomial(100, 0.12, rng) for _ in range(n)) - 12) < 0.5
    assert d.binomial(0, 0.12, rng) == 0
    assert all(0 <= d.binomial(5, 0.5, rng) <= 5 for _ in range(200))
