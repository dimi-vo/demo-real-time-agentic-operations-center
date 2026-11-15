"""Simulation tick: record shape, one sample per service per second, re-emit cadence,
bootstrap, incident effects, and window statistics."""

import json
import random
import statistics
from collections import defaultdict
from pathlib import Path

import pytest

from opsgen import engine
from opsgen.distributions import latency_ms, poisson
from opsgen.engine import BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP, Config, Incident
from opsgen.services import SERVICES, SERVICES_BY_NAME, service_names
from opsgen.simulation import REEMIT_INTERVAL, Simulation

METRICS_FIELDS = {"event_time", "service_name", "traffic_count", "latency_ms", "error_count"}
DEPLOYMENT_FIELDS = {
    "service_name",
    "version",
    "previous_version",
    "deployed_at",
    "last_change",
    "desired_replicas",
    "ready_replicas",
    "tier",
    "owner_team",
    "event_time",
}

AVSC_DIR = Path(__file__).resolve().parents[2] / "schemas"


def _sim(seed=0, config=None):
    return Simulation(seed, start=0.0, config=config or Config())


def test_bootstrap_emits_every_service():
    sim = _sim()
    boot = sim.bootstrap()
    assert {r["service_name"] for r in boot} == set(service_names())
    assert all(r["last_change"] == "INITIAL" for r in boot)


def test_metrics_record_fields_match_schema():
    sim = _sim(seed=1)
    metrics, _, _ = sim.tick(0.0)
    for r in metrics:
        assert set(r) == METRICS_FIELDS
        assert isinstance(r["traffic_count"], int) and isinstance(r["error_count"], int)
        assert isinstance(r["latency_ms"], float)
    avsc = json.loads((AVSC_DIR / "service_metrics.avsc").read_text())
    assert {f["name"] for f in avsc["fields"]} == METRICS_FIELDS


def test_deployment_record_fields_match_schema():
    sim = _sim()
    rec = sim._dep_record("checkout", 0.0)
    assert set(rec) == DEPLOYMENT_FIELDS
    # The record carries the key column too; Flink registers service_name in the key
    # subject only, so the value schema has every other field.
    value = json.loads((AVSC_DIR / "service_deployments.avsc").read_text())
    key = json.loads((AVSC_DIR / "service_deployments_key.avsc").read_text())
    assert {f["name"] for f in value["fields"]} == DEPLOYMENT_FIELDS - {"service_name"}
    assert {f["name"] for f in key["fields"]} == {"service_name"}


def test_reemit_every_five_seconds():
    sim = _sim(seed=3, config=Config(no_incidents=True))  # no deploys to interfere
    # No re-emit at tick 0 (bootstrap covers it); full re-emit at the 5s boundary.
    _, deps0, _ = sim.tick(0.0)
    assert deps0 == []
    _, deps5, _ = sim.tick(float(REEMIT_INTERVAL))
    assert {r["service_name"] for r in deps5} == set(service_names())
    _, deps6, _ = sim.tick(6.0)
    assert deps6 == []


def test_deploy_emits_deployment_record_and_mutates_version():
    # Force a bad deploy on checkout at tick 0 (the only kind of deploy, D-057).
    sim = _sim(seed=4, config=Config(quiet_period=0))
    sim.engine._choose_incident = lambda now: (BAD_DEPLOY, "checkout")
    _, deps, events = sim.tick(0.0)
    deployed = [e for e in events if e.kind == "bad_deploy"]
    assert deployed, "expected a bad deploy at tick 0"
    svc = deployed[0].service_name
    assert any(r["service_name"] == svc and r["last_change"] == "DEPLOY" for r in deps)
    assert sim.deployments[svc].version != sim.deployments[svc].previous_version


def _forced(itype: str, service: str, start: float, end: float) -> Incident:
    return Incident(
        incident_id="INC0001",
        type=itype,
        service_name=service,
        degradation_start=start,
        degradation_end=end,
        expected_trigger=engine.TRIGGER[itype],
        expected_action=engine.EXPECTED_ACTION[itype],
    )


def _run(sim: Simulation, ticks: range) -> dict[str, list[dict]]:
    """Every service's records over `ticks`, checking there is exactly one per service
    per tick (D-058)."""
    by_service: dict[str, list[dict]] = defaultdict(list)
    for k in ticks:
        metrics, _, _ = sim.tick(float(k))
        assert sorted(r["service_name"] for r in metrics) == sorted(service_names()), k
        for r in metrics:
            by_service[r["service_name"]].append(r)
    return by_service


@pytest.mark.parametrize(
    ("itype", "service", "traffic_mult", "latency_mult"),
    [
        (TRAFFIC_SURGE, "web-gateway", engine.SURGE_LAMBDA_MULT, 1.0),  # no latency effect (D-059)
        (UPSTREAM_DROP, "checkout", engine.DROP_LAMBDA_MULT, 1.0),
        (BAD_DEPLOY, "payments", 1.0, engine.BAD_DEPLOY_LATENCY_MULT),
    ],
    ids=["surge", "drop", "bad_deploy"],
)
def test_an_incident_moves_the_values_not_the_record_count(itype, service, traffic_mult, latency_mult):
    # D-058: one record per service per second, normal or not; the incident's multipliers
    # show in the values. 600 normal seconds, then 600 under the incident.
    sim = _sim(seed=12, config=Config(no_incidents=True))
    sim.engine.active = _forced(itype, service, start=600.0, end=1200.0)
    before = _run(sim, range(600))[service]
    during = _run(sim, range(600, 1200))[service]
    assert len(before) == len(during) == 600

    def mean_of(records, field):
        return statistics.mean(r[field] for r in records)

    traffic = mean_of(during, "traffic_count") / mean_of(before, "traffic_count")
    latency = mean_of(during, "latency_ms") / mean_of(before, "latency_ms")
    assert traffic == pytest.approx(traffic_mult, rel=0.1)
    assert latency == pytest.approx(latency_mult, rel=0.1)
    error_rate = sum(r["error_count"] for r in during) / sum(r["traffic_count"] for r in during)
    if itype == BAD_DEPLOY:
        assert error_rate == pytest.approx(engine.BAD_DEPLOY_ERROR_RATE, rel=0.2)
    else:
        assert error_rate < 0.02  # the normal 0.5%


def test_a_second_without_traffic_still_has_a_latency():
    # A drop can leave a second with no requests; the record is still sent, with one
    # latency draw, so latency_ms is never null.
    sim = _sim(seed=5, config=Config(no_incidents=True))
    sim.engine.active = _forced(UPSTREAM_DROP, "payments", start=0.0, end=600.0)
    records = _run(sim, range(600))["payments"]
    empty = [r for r in records if r["traffic_count"] == 0]
    assert empty, "expected some empty seconds at 30 x 0.1 requests/s"
    assert all(r["latency_ms"] > 0 and r["error_count"] == 0 for r in empty)


def test_upstream_drop_never_zeroes_a_window():
    sim = _sim(seed=5, config=Config(no_incidents=True))
    # Force a drop on payments (the lowest-rps service) for a 10s window.
    sim.engine.active = _forced(UPSTREAM_DROP, "payments", start=0.0, end=70.0)
    total = sum(r["traffic_count"] for r in _run(sim, range(10))["payments"])
    assert total > 0, "a dropped service must still count at least one request per window"


def test_bad_deploy_degrades_from_its_own_tick():
    # D-042: no onset delay. The deploy fires on a window boundary, and the 10-second
    # window it opens is already fully degraded.
    sim = _sim(seed=11, config=Config(quiet_period=10, spacing_min=30, spacing_max=30))
    for k in range(3000):
        metrics, _, events = sim.tick(float(k))
        bad = next((e for e in events if e.kind == "bad_deploy"), None)
        if bad is not None:
            break
    else:
        raise AssertionError("expected a BAD_DEPLOY in this seeded run")
    assert bad.now % 10 == 0
    assert any(e.kind == "incident_start" and e.now == bad.now for e in events)
    window = [r for r in metrics if r["service_name"] == bad.service_name]
    for j in range(1, 10):
        window += [r for r in sim.tick(float(k + j))[0] if r["service_name"] == bad.service_name]
    baseline = SERVICES_BY_NAME[bad.service_name].median_latency_ms
    assert statistics.mean(r["latency_ms"] for r in window) > 2.5 * baseline  # x4 under a bad deploy


def test_records_sit_at_the_end_of_their_tick():
    # D-028, D-058: a record covers its tick and is stamped tick + 999 ms, never on the
    # next tick's millisecond, where a deploy record for the next version may sit. The
    # fractional start reproduces the live case (start * 1000 had a .599 ms fraction when
    # a collision was observed).
    sim = Simulation(7, start=1_794_390_513.319599, config=Config())
    for k in range(600):
        now = sim.start + k
        metrics, deps, _ = sim.tick(now)
        tick_ms = sim.start_ms + 1000 * k
        assert [r["event_time"] for r in metrics] == [tick_ms + 999] * len(SERVICES)
        assert all(d["event_time"] == tick_ms for d in deps)


def test_a_record_follows_the_deploy_record_of_its_tick():
    # A forced bad deploy on checkout: both its deploy and its rollback change the version.
    # On that tick the deploy record is at the tick and checkout's record 999 ms later, so
    # the temporal join gives the record the new version.
    sim = Simulation(42, start=1_794_390_513.319599, config=Config(quiet_period=0))
    sim.engine._choose_incident = lambda now: (BAD_DEPLOY, "checkout")
    seen = []
    for k in range(100):
        metrics, deps, events = sim.tick(sim.start + k)
        for ev in events:
            if ev.new_version is None:
                continue
            seen.append(ev.kind)
            dep = next(d for d in deps if d["service_name"] == ev.service_name)
            assert dep["version"] == ev.new_version
            (mine,) = [r for r in metrics if r["service_name"] == ev.service_name]
            assert mine["event_time"] == dep["event_time"] + 999
    assert seen[:2] == ["bad_deploy", "incident_end"]


def _old_window(median: float, rps: float, rng: random.Random) -> tuple[int, float | None]:
    """One 10-second window as the per-request topic built it: COUNT of the requests and
    their request-weighted mean latency (None for an empty window)."""
    n = poisson(10 * rps, rng)
    return n, (sum(latency_ms(median, rng) for _ in range(n)) / n if n else None)


def test_windows_of_samples_match_the_old_per_request_windows():
    # D-058, research/ps_compare.out: SUM(traffic_count) and AVG(latency_ms) over 10
    # per-second samples have the mean and spread of the old COUNT(*) and AVG(latency_ms)
    # over single requests (latency spread x1.05-1.11, count spread the same), so the
    # detector sees the same windows.
    windows = 1500
    sim = _sim(seed=8, config=Config(no_incidents=True))
    new: dict[str, dict[int, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for k in range(10 * windows):
        for r in sim.tick(float(k))[0]:
            new[r["service_name"]][r["event_time"] // 10_000].append(r)
    rng = random.Random(9)
    for svc in SERVICES:
        assert all(len(rs) == 10 for rs in new[svc.name].values())
        counts = [sum(r["traffic_count"] for r in rs) for rs in new[svc.name].values()]
        lats = [statistics.mean(r["latency_ms"] for r in rs) for rs in new[svc.name].values()]
        old = [_old_window(svc.median_latency_ms, svc.rps, rng) for _ in range(windows)]
        old_counts = [n for n, _ in old]
        old_lats = [lat for _, lat in old if lat is not None]
        assert statistics.mean(counts) == pytest.approx(statistics.mean(old_counts), rel=0.03), svc.name
        assert statistics.pstdev(counts) == pytest.approx(statistics.pstdev(old_counts), rel=0.1), svc.name
        assert statistics.mean(lats) == pytest.approx(statistics.mean(old_lats), rel=0.02), svc.name
        assert 0.9 < statistics.pstdev(lats) / statistics.pstdev(old_lats) < 1.3, svc.name


def test_incident_ends_on_a_tick_at_the_logged_ended_at():
    # Whole-second durations: the ROLLBACK/end fires exactly at degradation_end, so the
    # ended_at in the end record is when traffic really returned to normal.
    sim = Simulation(42, start=1_794_390_513.319599, config=Config())
    ends = []
    for k in range(3600):
        _, _, events = sim.tick(sim.start + k)
        ends += [(ev.now, ev.incident) for ev in events if ev.kind == "incident_end"]
    assert ends
    for now, inc in ends:
        assert now == inc.degradation_end
        assert float(inc.degradation_end - inc.degradation_start).is_integer()


def test_aligned_start_is_the_next_ten_second_boundary():
    from opsgen.driver import aligned_start

    assert aligned_start(1_800_000_003.4) == 1_800_000_010.0
    assert aligned_start(1_800_000_010.0) == 1_800_000_010.0
    # starting there, the first window is full: every tick of window 0 is produced
    sim = Simulation(3, start=aligned_start(1_800_000_003.4), config=Config())
    first_window = {r["event_time"] // 10_000 for k in range(10) for r in sim.tick(sim.start + k)[0]}
    assert first_window == {180_000_001}
