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
    assert len(per_service) == 6
    for srows in per_service.values():
        assert len(srows) == 90  # 15 minutes of 10-second windows, none missing
    assert 35 < rate < 48  # ~41 msg/s (D-031)
    rep = score(apply_detector(rows, CURRENT), incidents, start, end - 100)
    assert sum(s.warmup_rows for s in rep.services.values()) == 6 * 13
