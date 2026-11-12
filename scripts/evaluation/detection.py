"""Score `service_health` against the generator's ground truth (`incidents.jsonl`).

Pure functions over plain data, so they're unit-tested offline. The definitions
(D-021) are:

- **Signal.** A row carries `LATENCY_HIGH` when `latency_status = 'HIGH'`, and
  `TRAFFIC_HIGH` / `TRAFFIC_LOW` when `traffic_status` is `HIGH` / `LOW`. These names
  are the incidents' `expected_trigger` values. `WARMING_UP` rows carry no signal.
- **Incident span.** The 10-second windows of the incident's service that overlap
  its degradation, `[started_at, ended_at)`. The **onset window** is the window that
  contains `started_at`; it is window 1 of the span, the next one window 2, and so on.
- **Detected** when a span window carries the incident's expected trigger; the
  detection is **in time** if the first such window is window 1, 2 or 3 (brief
  section 11: "within 3 windows of onset"), else **late**. Otherwise **missed**.
- **False positive.** A signal on a row outside every incident span of its service.
  It's tagged `echo` when it falls in the minute after one of that service's
  incidents ended (ARIMA's forecast lagging the return to baseline), and
  `after deploy` when the service deployed less than a minute earlier (a benign
  deploy, which must not move the metrics).
- **Restart artifacts.** When `ops-sim` stops and restarts, the windows on either side
  of the gap hold only part of the normal traffic, so the detectors rightly flag them.
  Signals in the last window before a gap and in the minute after it are counted as
  `restart`, outside the false-positive rate (which is about normal operation).
- **Fresh start.** The first 5 minutes after the first window of each service in the
  topic. Signals there (outside incident spans) are counted as fresh-start noise,
  separately from the false-positive rate.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path

from scripts.common.topic_reader import epoch_seconds

WINDOW_SECONDS = 10.0
DETECT_WITHIN_WINDOWS = 3
FRESH_START_SECONDS = 300.0
ECHO_SECONDS = 60.0
AFTER_DEPLOY_SECONDS = 60
RESTART_SECONDS = 60.0
# ops-sim logs an incident when it ends, so the newest rows may belong to one still in
# progress. An incident lasts at most 70 s; rows ending 100 s before the newest row
# can't be part of an unlogged one.
UNLOGGED_TAIL_SECONDS = 100.0

LATENCY_HIGH = "LATENCY_HIGH"
TRAFFIC_HIGH = "TRAFFIC_HIGH"
TRAFFIC_LOW = "TRAFFIC_LOW"
OPPOSITE = {TRAFFIC_HIGH: TRAFFIC_LOW, TRAFFIC_LOW: TRAFFIC_HIGH}

DETECTED = "DETECTED"
LATE = "LATE"
MISSED = "MISSED"
NO_DATA = "NO_DATA"


@dataclass(frozen=True)
class HealthRow:
    service_name: str
    window_start: float
    window_end: float
    latency_status: str | None
    traffic_status: str | None
    avg_latency_ms: float | None = None
    expected_latency_ms: float | None = None
    latency_upper_bound: float | None = None
    request_count: int | None = None
    expected_request_count: float | None = None
    traffic_lower_bound: float | None = None
    traffic_upper_bound: float | None = None
    error_rate: float | None = None
    error_rate_breach: bool | None = None
    version: str | None = None
    last_change: str | None = None
    seconds_since_deploy: int | None = None
    # Read by the decision half (D-034): the agent's context lives here, not on the
    # decision row.
    ready_replicas: int | None = None
    max_replicas: int | None = None

    @classmethod
    def from_record(cls, rec: dict) -> HealthRow:
        def status(v):
            return v.strip() if isinstance(v, str) else v

        return cls(
            service_name=rec["service_name"],
            window_start=epoch_seconds(rec["window_start"]),
            window_end=epoch_seconds(rec["window_end"]),
            latency_status=status(rec.get("latency_status")),
            traffic_status=status(rec.get("traffic_status")),
            avg_latency_ms=rec.get("avg_latency_ms"),
            expected_latency_ms=rec.get("expected_latency_ms"),
            latency_upper_bound=rec.get("latency_upper_bound"),
            request_count=rec.get("request_count"),
            expected_request_count=rec.get("expected_request_count"),
            traffic_lower_bound=rec.get("traffic_lower_bound"),
            traffic_upper_bound=rec.get("traffic_upper_bound"),
            error_rate=rec.get("error_rate"),
            error_rate_breach=rec.get("error_rate_breach"),
            version=rec.get("version"),
            last_change=rec.get("last_change"),
            seconds_since_deploy=rec.get("seconds_since_deploy"),
            ready_replicas=rec.get("ready_replicas"),
            max_replicas=rec.get("max_replicas"),
        )

    @property
    def warming_up(self) -> bool:
        return self.latency_status == "WARMING_UP" or self.traffic_status == "WARMING_UP"

    @property
    def signals(self) -> tuple[str, ...]:
        out = []
        if self.latency_status == "HIGH":
            out.append(LATENCY_HIGH)
        if self.traffic_status == "HIGH":
            out.append(TRAFFIC_HIGH)
        elif self.traffic_status == "LOW":
            out.append(TRAFFIC_LOW)
        return tuple(out)

    def describe(self, signal: str) -> str:
        """`410 ms vs expected 120 (upper 160)`-style evidence for one signal."""
        if signal == LATENCY_HIGH:
            return (
                f"{_num(self.avg_latency_ms)} ms vs expected {_num(self.expected_latency_ms)} "
                f"(upper {_num(self.latency_upper_bound)})"
            )
        bound = self.traffic_upper_bound if signal == TRAFFIC_HIGH else self.traffic_lower_bound
        side = "upper" if signal == TRAFFIC_HIGH else "lower"
        return f"{self.request_count} req vs expected {_num(self.expected_request_count)} ({side} {_num(bound)})"


def _num(v: float | None) -> str:
    return "?" if v is None else f"{v:.0f}"


@dataclass(frozen=True)
class Incident:
    incident_id: str
    type: str
    service_name: str
    started_at: float
    ended_at: float
    expected_trigger: str
    expected_action: str
    bad_version: str | None = None
    good_version: str | None = None

    @classmethod
    def from_record(cls, rec: dict) -> Incident:
        return cls(
            incident_id=str(rec["incident_id"]),
            type=rec["type"],
            service_name=rec["service_name"],
            started_at=epoch_seconds(rec["started_at"]),
            ended_at=epoch_seconds(rec["ended_at"]),
            expected_trigger=rec["expected_trigger"],
            expected_action=rec["expected_action"],
            bad_version=rec.get("bad_version"),
            good_version=rec.get("good_version"),
        )

    @property
    def onset_window_start(self) -> float:
        return math.floor(self.started_at / WINDOW_SECONDS) * WINDOW_SECONDS

    @property
    def effective_end(self) -> float:
        """When the incident really ended. ops-sim acts on 1-second ticks counted from
        `started_at`, so an `ended_at` between ticks takes effect at the next tick (logs
        written before D-028 have such fractional durations; newer ones are whole seconds)."""
        return self.started_at + math.ceil(self.ended_at - self.started_at - 1e-6)

    def in_span_at(self, window_end: float) -> bool:
        """Whether the 10-second window ending here overlaps the degradation. Takes a
        timestamp rather than a row, so decisions (D-032) can use it too."""
        return window_end > self.started_at and window_end - WINDOW_SECONDS < self.effective_end

    def in_span(self, row: HealthRow) -> bool:
        return row.service_name == self.service_name and self.in_span_at(row.window_end)

    def at_end(self, row: HealthRow) -> bool:
        """The span window that contains the return to normal."""
        return row.window_end > self.effective_end

    def window_number_at(self, window_end: float) -> int:
        return round((window_end - WINDOW_SECONDS - self.onset_window_start) / WINDOW_SECONDS) + 1

    def window_number(self, row: HealthRow) -> int:
        return self.window_number_at(row.window_end)


def load_incidents(path: Path) -> list[Incident]:
    path = Path(path)
    if not path.is_file():
        return []
    out = []
    for line in path.read_text().splitlines():
        if line.strip():
            out.append(Incident.from_record(json.loads(line)))
    return sorted(out, key=lambda i: i.started_at)


@dataclass
class IncidentResult:
    incident: Incident
    status: str
    first_window: int | None = None
    detected_row: HealthRow | None = None
    span_rows: int = 0
    other_signals: list[tuple[int, str, bool]] = field(default_factory=list)  # (window, signal, at end)
    wrong_direction: bool = False
    version_mismatches: int = 0

    @property
    def in_time(self) -> bool:
        return self.status == DETECTED


@dataclass(frozen=True)
class FalsePositive:
    row: HealthRow
    signal: str
    tag: str  # "echo", "after deploy" or ""


@dataclass
class ServiceSummary:
    service_name: str
    rows: int = 0
    first_window_end: float | None = None
    last_window_end: float | None = None
    warmup_rows: int = 0
    last_warmup_end: float | None = None
    warmup_after_fresh_start: int = 0
    duplicates: int = 0
    gaps: int = 0
    missing_windows: int = 0
    scored_hours: float = 0.0
    false_positives: list[FalsePositive] = field(default_factory=list)
    fresh_start_noise: list[FalsePositive] = field(default_factory=list)
    restart_artifacts: list[FalsePositive] = field(default_factory=list)

    @property
    def fp_per_hour(self) -> float | None:
        return len(self.false_positives) / self.scored_hours if self.scored_hours > 0 else None


@dataclass
class DetectionReport:
    range_from: float
    range_to: float
    services: dict[str, ServiceSummary]
    incidents: list[IncidentResult]
    total_rows: int

    def by_type(self) -> dict[str, tuple[int, int]]:
        """incident type -> (detected in time, scored incidents)."""
        out: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for r in self.incidents:
            if r.status == NO_DATA:
                continue
            out[r.incident.type][1] += 1
            out[r.incident.type][0] += int(r.in_time)
        return {k: (v[0], v[1]) for k, v in out.items()}


def rescale_confidence(rows: list[HealthRow], base: float, target: float) -> list[HealthRow]:
    """Re-derive the statuses as if the detectors had run at `target` confidence instead of
    `base`. ML_DETECT_ANOMALIES's bounds are symmetric, forecast ± z·sigma (checked live: the
    99.99 and 99.9 runs differ by the ratio of their z values), so the bounds scale by
    z(target)/z(base) around the forecast. An offline tuning aid; not exact if the model
    itself would differ."""
    from dataclasses import replace
    from statistics import NormalDist

    def z(c: float) -> float:
        return NormalDist().inv_cdf(0.5 + c / 200.0)

    k = z(target) / z(base)
    out = []
    for r in rows:
        if r.warming_up or r.expected_latency_ms is None or r.expected_request_count is None:
            out.append(r)
            continue
        up_l = r.expected_latency_ms + (r.latency_upper_bound - r.expected_latency_ms) * k
        up_t = r.expected_request_count + (r.traffic_upper_bound - r.expected_request_count) * k
        lo_t = r.expected_request_count - (r.expected_request_count - r.traffic_lower_bound) * k
        lat = "HIGH" if r.avg_latency_ms > up_l else "NORMAL"
        trf = "HIGH" if r.request_count > up_t else "LOW" if r.request_count < lo_t else "NORMAL"
        out.append(replace(r, latency_status=lat, traffic_status=trf, latency_upper_bound=up_l,
                           traffic_upper_bound=up_t, traffic_lower_bound=lo_t))  # fmt: skip
    return out


def default_range(rows: list[HealthRow], minutes: float) -> tuple[float, float]:
    """The last `minutes` of data, ending UNLOGGED_TAIL_SECONDS before the newest row."""
    newest = max(r.window_end for r in rows)
    to = newest - UNLOGGED_TAIL_SECONDS
    return to - minutes * 60.0, to


def _dedupe(rows: list[HealthRow]) -> tuple[dict[str, list[HealthRow]], dict[str, int]]:
    per_service: dict[str, dict[float, HealthRow]] = defaultdict(dict)
    dups: dict[str, int] = defaultdict(int)
    for r in rows:
        seen = per_service[r.service_name]
        if r.window_end in seen:
            dups[r.service_name] += 1
        else:
            seen[r.window_end] = r
    return {s: sorted(v.values(), key=lambda r: r.window_end) for s, v in per_service.items()}, dups


def score_incident(inc: Incident, service_rows: list[HealthRow]) -> IncidentResult:
    span = [r for r in service_rows if inc.in_span(r)]
    if not span:
        return IncidentResult(inc, NO_DATA)
    result = IncidentResult(inc, MISSED, span_rows=len(span))
    for r in span:
        k = inc.window_number(r)
        for sig in r.signals:
            if sig == inc.expected_trigger and result.first_window is None:
                result.first_window = k
                result.detected_row = r
            elif sig != inc.expected_trigger:
                end = inc.at_end(r)
                result.other_signals.append((k, sig, end))
                # In the window holding the return to normal, the opposite signal is an echo
                # (the forecast had followed the incident level), not a wrong detection.
                if OPPOSITE.get(inc.expected_trigger) == sig and not end:
                    result.wrong_direction = True
    if result.first_window is not None:
        result.status = DETECTED if result.first_window <= DETECT_WITHIN_WINDOWS else LATE
    if inc.bad_version and inc.good_version:
        # Windows fully inside the incident ran the bad version; the window holding the
        # rollback (at the effective end) ends on the good one (LAST_VALUE, D-011).
        for r in span:
            want = inc.good_version if inc.at_end(r) else inc.bad_version
            if r.version != want:
                result.version_mismatches += 1
    return result


def score(
    rows: list[HealthRow],
    incidents: list[Incident],
    range_from: float,
    range_to: float,
) -> DetectionReport:
    by_service, dups = _dedupe(rows)
    incidents_by_service: dict[str, list[Incident]] = defaultdict(list)
    for inc in incidents:
        incidents_by_service[inc.service_name].append(inc)

    services: dict[str, ServiceSummary] = {}
    for name, srows in sorted(by_service.items()):
        s = ServiceSummary(name, rows=len(srows), duplicates=dups.get(name, 0))
        s.first_window_end, s.last_window_end = srows[0].window_end, srows[-1].window_end
        fresh_until = srows[0].window_start + FRESH_START_SECONDS
        restart_spans: list[tuple[float, float]] = []  # (last window_end before, first window_start after)
        for prev, cur in pairwise(srows):
            step = round((cur.window_end - prev.window_end) / WINDOW_SECONDS)
            if step > 1:
                s.gaps += 1
                s.missing_windows += step - 1
                restart_spans.append((prev.window_end, cur.window_start))
        mine = incidents_by_service.get(name, [])
        for r in srows:
            if r.warming_up:
                s.warmup_rows += 1
                s.last_warmup_end = r.window_end
                if r.window_end > fresh_until:
                    s.warmup_after_fresh_start += 1
                continue
            in_fresh = r.window_end <= fresh_until
            in_range = range_from <= r.window_end <= range_to
            if in_range and not in_fresh:
                s.scored_hours += WINDOW_SECONDS / 3600.0
            if not r.signals or any(inc.in_span(r) for inc in mine):
                continue
            if not (in_fresh or in_range):
                continue
            restart = any(
                r.window_end == before or after <= r.window_start < after + RESTART_SECONDS
                for before, after in restart_spans
            )
            for sig in r.signals:
                if restart:
                    s.restart_artifacts.append(FalsePositive(r, sig, "restart"))
                    continue
                fp = FalsePositive(r, sig, _tag(r, mine))
                (s.fresh_start_noise if in_fresh else s.false_positives).append(fp)
        services[name] = s

    results = [
        score_incident(inc, by_service.get(inc.service_name, []))
        for inc in incidents
        if range_from <= inc.started_at <= range_to
    ]
    return DetectionReport(range_from, range_to, services, results, total_rows=len(rows))


def _tag(row: HealthRow, incidents: list[Incident]) -> str:
    for inc in incidents:
        if inc.effective_end <= row.window_start < inc.effective_end + ECHO_SECONDS:
            return "echo"
    if row.seconds_since_deploy is not None and 0 <= row.seconds_since_deploy < AFTER_DEPLOY_SECONDS:
        return "after deploy"
    return ""
