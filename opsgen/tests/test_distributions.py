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


def test_status_code_normal_mix():
    rng = random.Random(5)
    n = 100000
    codes = [d.status_code(200, False, rng) for _ in range(n)]
    server = sum(1 for c in codes if c >= 500) / n
    client = sum(1 for c in codes if 400 <= c < 500) / n
    success = sum(1 for c in codes if c == 200) / n
    assert abs(server - 0.005) < 0.003
    assert abs(client - 0.02) < 0.006
    assert abs(success - 0.975) < 0.01


def test_status_code_success_depends_on_method():
    rng = random.Random(6)
    # POST -> 201, DELETE -> 204 when successful.
    assert any(d.status_code(201, False, rng) == 201 for _ in range(100))
    assert any(d.status_code(204, False, rng) == 204 for _ in range(100))


def test_status_code_bad_deploy_override():
    rng = random.Random(7)
    n = 50000
    codes = [d.status_code(200, False, rng, server_error_rate=0.12) for _ in range(n)]
    server = sum(1 for c in codes if c >= 500) / n
    assert abs(server - 0.12) < 0.01
    # "mostly 500 and 503"
    five00 = sum(1 for c in codes if c == 500)
    five03 = sum(1 for c in codes if c == 503)
    five02 = sum(1 for c in codes if c == 502)
    assert five00 + five03 > 4 * five02


def test_auth_client_errors_include_401():
    rng = random.Random(8)
    codes = [d.status_code(200, True, rng) for _ in range(100000)]
    assert any(c == 401 for c in codes)
    assert any(c == 404 for c in codes)


def test_response_bytes():
    rng = random.Random(9)
    errs = [d.response_bytes(5000, True, rng) for _ in range(1000)]
    assert all(170 <= b <= 230 for b in errs)
    oks = [d.response_bytes(1000, False, rng) for _ in range(1000)]
    assert all(800 <= b <= 1200 for b in oks)


def test_trace_id():
    rng = random.Random(10)
    tid = d.trace_id(rng)
    assert len(tid) == 32
    assert all(c in "0123456789abcdef" for c in tid)
