"""The offline tuning harness: the AR(1) replica and the generator-driven sweep (D-031)."""

import random

from scripts.tuning.arima import DetectorConfig, _detect_ar1_every_row, detect, fit_ar


def _reference(series, cfg):
    """Refit with fit_ar on every row: the slow, obviously-correct version."""
    whole = all(float(v).is_integer() for v in series)
    out = []
    for t in range(len(series)):
        h = series[max(0, t - cfg.max_training) : t]
        if len(h) < cfg.min_training + cfg.warmup_extra:
            out.append(None)
            continue
        (c, phi), sigma = fit_ar(h, 1)
        f = c + phi * h[-1]
        lo, up = f - cfg.z * sigma, f + cfg.z * sigma
        out.append((round(f), round(lo), round(up)) if whole else (f, lo, up))
    return out


def test_fast_ar1_path_matches_the_reference_fit():
    rng = random.Random(0)
    counts = [float(rng.randint(20, 40)) for _ in range(500)] + [5.0] * 6
    counts += [float(rng.randint(20, 40)) for _ in range(100)]
    latency = [rng.gauss(100, 8) for _ in range(600)]
    for series in (counts, latency):
        small = DetectorConfig(min_training=6, max_training=50, update_interval=1)
        configs = (DetectorConfig(update_interval=1), small)
        for cfg in configs:
            fast = _detect_ar1_every_row(series, cfg)
            ref = _reference(series, cfg)
            for p, q in zip(fast, ref, strict=True):
                assert (p.forecast is None) == (q is None)
                if q is not None:
                    assert abs(p.forecast - q[0]) < 1e-6 and abs(p.upper - q[2]) < 1e-6


def test_warm_up_lasts_min_training_plus_one_rows():
    # Confluent's first output came after 7 rows at minTrainingSize 6 and 13 at 12.
    series = [float(i % 7) for i in range(40)]
    for m in (6, 12):
        pts = detect(series, DetectorConfig(min_training=m, update_interval=1))
        assert sum(p.warming_up for p in pts) == m + 1


def test_whole_number_series_get_whole_number_bounds():
    rng = random.Random(1)
    pts = detect([float(rng.randint(20, 40)) for _ in range(100)], DetectorConfig(update_interval=1))
    assert all(float(p.lower).is_integer() and float(p.upper).is_integer() for p in pts if not p.warming_up)


def test_sweep_simulation_windows_are_complete_and_scored():
    from scripts.evaluation.detection import score
    from scripts.tuning.sweep import CURRENT, DataConfig, apply_detector, simulate

    rows, incidents, start, end, rate = simulate(DataConfig(aligned=True), seed=1, hours=0.25)
    assert start % 10 == 0  # aligned start
    per_service = {}
    for r in rows:
        per_service.setdefault(r.service_name, []).append(r)
    assert set(per_service) == {"web-gateway", "payments"}  # checkout has no metrics (D-068)
    for srows in per_service.values():
        assert len(srows) == 90  # 15 minutes of 10-second windows, none missing
    # ~80 requests/s of traffic_count at baseline (D-059 means 50/30 on the two metric
    # services). There are no surges to lift it (D-068), only payments' drops, which lower it a
    # little.
    assert 70 < rate < 82
    rep = score(apply_detector(rows, CURRENT), incidents, start, end - 100)
    assert sum(s.warmup_rows for s in rep.services.values()) == 2 * 13


class _InlinePool:
    """`evaluate_variant` takes a process pool; run its tasks in this process instead."""

    map = staticmethod(map)


def test_the_sweep_scores_payments_alone_and_its_table_has_no_surge_or_checkout_column(capsys):
    # D-068: ARIMA covers payments only, which gets bad deploys and drops.
    from scripts.tuning.sweep import CURRENT, DataConfig, evaluate_variant, print_results

    res = evaluate_variant(DataConfig(aligned=True), CURRENT, 1, 0.25, "one quarter-hour", _InlinePool())
    assert set(res.hours_by_service) == {"payments"}
    assert set(res.by_type) == {"BAD_DEPLOY", "UPSTREAM_DROP"}
    print_results([res])
    header, line = capsys.readouterr().out.splitlines()
    assert "BAD_DEPLOY" in header and "DROP" in header
    assert "SURGE" not in header and "chkout" not in header and "paymt" not in header
    assert line.startswith("one quarter-hour")


def test_the_sweep_rate_presets_name_no_checkout():
    # D-068: checkout produces tickets, not metrics, so a traffic mean for it means nothing.
    from scripts.tuning.sweep import DATA_VARIANTS

    assert all(name != "checkout" for data in DATA_VARIANTS.values() for name, _ in data.rates)
    assert any(data.rates for data in DATA_VARIANTS.values())


def test_sweep_windows_sum_traffic_and_average_the_samples(monkeypatch):
    """D-058: a window is SUM(traffic_count) and the unweighted AVG(latency_ms) of its
    per-second samples, as Flink computes it, not a request-weighted mean."""
    import opsgen.simulation as sim_mod
    from scripts.tuning.sweep import DataConfig, simulate

    class FakeDeployment:
        version, deployed_at = "1.0.0", 0.0

    class FakeSimulation:
        def __init__(self, seed, start, config):
            self.start = start
            self.deployments = {"payments": FakeDeployment()}

        def tick_ms(self, t):
            return round(t * 1000)

        def tick(self, now):
            # Even seconds: 9 requests at 100 ms; odd seconds: 1 request at 300 ms.
            k = round(now - self.start)
            count, latency = (9, 100.0) if k % 2 == 0 else (1, 300.0)
            sample = {"event_time": self.tick_ms(now) + 999, "service_name": "payments",
                      "traffic_count": count, "latency_ms": latency, "error_count": 0}  # fmt: skip
            return [sample], [], [], [], []  # metrics, deploys, events, tickets, findings

    monkeypatch.setattr(sim_mod, "Simulation", FakeSimulation)
    rows, _incidents, start, _end, rate = simulate(DataConfig(aligned=True), seed=0, hours=60 / 3600)
    assert start % 10 == 0
    assert len(rows) == 6
    for r in rows:
        assert r.traffic_count == 50
        assert r.avg_latency_ms == 200.0  # the request-weighted mean would be 120
    assert rate == 5.0
