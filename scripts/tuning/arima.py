"""A small, dependency-free replica of ML_DETECT_ANOMALIES for AR(p) models.

What it reproduces (each point checked against Confluent's recorded output by
`scripts.tuning.calibrate`):
- one model per series (service x metric), trained on the most recent
  `max_training` points *before* the current one;
- no output until there is enough history (warm-up);
- refit every `update_interval` rows, one-step-ahead forecast in between;
- bounds = forecast +/- z * sigma, with z from `confidence` (two-sided) and sigma the
  residual standard deviation of the fit; symmetric (confirmed live);
- forecast and bounds rounded to whole numbers when the series is whole numbers
  (Confluent does this for traffic_count).

The model is AR(p) with an intercept, fitted by ordinary least squares on lagged
values (conditional least squares). Confluent's estimator isn't documented; calibration
shows how close this gets.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist


@dataclass(frozen=True)
class DetectorConfig:
    min_training: int = 12
    max_training: int = 360
    confidence: float = 99.95  # the labs' value (D-059)
    p: int = 1
    update_interval: int = 5
    warmup_extra: int = 1  # Confluent's first output comes after min_training + 1 points
    sigma_ddof: int = 0

    @property
    def z(self) -> float:
        return NormalDist().inv_cdf(0.5 + self.confidence / 200.0)


@dataclass(frozen=True)
class Point:
    actual: float
    forecast: float | None
    lower: float | None
    upper: float | None

    @property
    def warming_up(self) -> bool:
        return self.forecast is None


def _solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """Gaussian elimination with partial pivoting; None if singular."""
    n = len(b)
    m = [[*row, b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        for r in range(n):
            if r != col:
                f = m[r][col] / m[col][col]
                for c in range(col, n + 1):
                    m[r][c] -= f * m[col][c]
    return [m[i][n] / m[i][i] for i in range(n)]


def fit_ar(history: list[float], p: int, ddof: int = 0) -> tuple[list[float], float] | None:
    """OLS fit of y_t = c + sum(phi_i * y_{t-i}) + e. Returns ([c, phi_1..phi_p], sigma)."""
    rows = [[1.0, *history[t - p : t][::-1]] for t in range(p, len(history))]
    ys = history[p:]
    if len(ys) <= p + 1:
        return None
    k = p + 1
    xtx = [[sum(r[i] * r[j] for r in rows) for j in range(k)] for i in range(k)]
    xty = [sum(r[i] * y for r, y in zip(rows, ys, strict=True)) for i in range(k)]
    beta = _solve(xtx, xty)
    if beta is None:  # constant history: fall back to the mean, zero spread
        mean = sum(history) / len(history)
        return [mean] + [0.0] * p, 0.0
    resid = [y - sum(b * x for b, x in zip(beta, r, strict=True)) for r, y in zip(rows, ys, strict=True)]
    dof = max(1, len(resid) - ddof)
    return beta, math.sqrt(sum(e * e for e in resid) / dof)


def _detect_ar1_every_row(series: list[float], cfg: DetectorConfig) -> list[Point]:
    """Fast path for AR(1) refitted on every row: OLS from running sums over the sliding
    window of (y_{t-1}, y_t) pairs, O(1) per row. Same result as fit_ar(history, 1)."""
    whole = all(float(v).is_integer() for v in series)
    z = cfg.z
    out: list[Point] = []
    n = sx = sy = sxx = sxy = syy = 0.0
    # pairs (series[i-1], series[i]) for i in [lo, t) where history = series[t-max : t]
    for t, actual in enumerate(series):
        # add pair ending at t-1
        if t >= 2:
            x, y = series[t - 2], series[t - 1]
            n += 1
            sx += x
            sy += y
            sxx += x * x
            sxy += x * y
            syy += y * y
        # drop pairs that fell out of the window: history starts at h0, first pair ends at h0+1
        h0 = max(0, t - cfg.max_training)
        if h0 >= 1 and t >= 2:
            # the pair ending at h0 (x = series[h0-1], y = series[h0]) just left the window
            x, y = series[h0 - 1], series[h0]
            n -= 1
            sx -= x
            sy -= y
            sxx -= x * x
            sxy -= x * y
            syy -= y * y
        if t - h0 < cfg.min_training + cfg.warmup_extra or n < 3:
            out.append(Point(actual, None, None, None))
            continue
        det = n * sxx - sx * sx
        if abs(det) < 1e-9:
            c, phi = sy / n, 0.0
        else:
            phi = (n * sxy - sx * sy) / det
            c = (sy - phi * sx) / n
        sse = syy - c * sy - phi * sxy
        sigma = math.sqrt(max(sse, 0.0) / max(1.0, n - cfg.sigma_ddof))
        forecast = c + phi * series[t - 1]
        lower, upper = forecast - z * sigma, forecast + z * sigma
        if whole:
            forecast, lower, upper = float(round(forecast)), float(round(lower)), float(round(upper))
        out.append(Point(actual, forecast, lower, upper))
    return out


def detect(series: list[float], cfg: DetectorConfig) -> list[Point]:
    if cfg.p == 1 and cfg.update_interval == 1:
        return _detect_ar1_every_row(series, cfg)
    whole = all(float(v).is_integer() for v in series)
    out: list[Point] = []
    params: tuple[list[float], float] | None = None
    since_fit = cfg.update_interval
    for t, actual in enumerate(series):
        history = series[max(0, t - cfg.max_training) : t]
        if len(history) < cfg.min_training + cfg.warmup_extra:
            out.append(Point(actual, None, None, None))
            continue
        if params is None or since_fit >= cfg.update_interval:
            params = fit_ar(history, cfg.p, cfg.sigma_ddof)
            since_fit = 0
        since_fit += 1
        if params is None:
            out.append(Point(actual, None, None, None))
            continue
        beta, sigma = params
        lags = history[-cfg.p :][::-1]
        forecast = beta[0] + sum(b * y for b, y in zip(beta[1:], lags, strict=True))
        lower, upper = forecast - cfg.z * sigma, forecast + cfg.z * sigma
        if whole:
            forecast, lower, upper = float(round(forecast)), float(round(lower)), float(round(upper))
        out.append(Point(actual, forecast, lower, upper))
    return out
