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
from opsgen.engine import (
    BAD_DEPLOY,
    BAD_DEPLOY_ERRORS,
    BAD_DEPLOY_TYPES,
    UPSTREAM_DROP,
    Config,
    Decision,
    Incident,
)
from opsgen.groundtruth import end_record
from opsgen.services import METRIC_SERVICES, METRIC_SERVICES_BY_NAME, SERVICES, TICKET_SERVICES
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


def _force(sim, itype, svc):
    """Make the engine's next choice `(itype, svc)`, and then no other."""
    picks = iter([(itype, svc)])
    sim.engine._choose_incident = lambda now: next(picks, None)


def test_two_services_produce_metrics_and_checkout_produces_tickets():
    # D-068: SERVICES lists all three in the wall's order; web-gateway and payments produce
    # metrics and deploys, checkout produces support tickets.
    assert SERVICES == ("web-gateway", "checkout", "payments")
    assert [s.name for s in METRIC_SERVICES] == ["web-gateway", "payments"]
    assert TICKET_SERVICES == ("checkout",)
    assert set(METRIC_SERVICES_BY_NAME) == {"web-gateway", "payments"}


def test_bootstrap_emits_the_metric_services():
    sim = _sim()
    boot = sim.bootstrap()
    assert [r["service_name"] for r in boot] == ["web-gateway", "payments"]  # no checkout (D-068)
    assert all(r["last_change"] == "INITIAL" for r in boot)


def test_checkout_writes_no_metrics_and_no_deployment_records():
    # D-068: two metrics records a second, and deployment records (the 5 s re-emit, deploys
    # and rollbacks) for web-gateway and payments only, over an hour with incidents.
    sim = _sim(seed=1)
    changes: dict[str, set[str]] = defaultdict(set)
    for k in range(3600):
        metrics, deps, _ = sim.tick(float(k))
        assert [r["service_name"] for r in metrics] == ["web-gateway", "payments"], k
        for d in deps:
            changes[d["service_name"]].add(d["last_change"])
    assert set(changes) == {"web-gateway", "payments"}
    assert all({"DEPLOY", "ROLLBACK"} <= seen for seen in changes.values()), changes


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
    rec = sim._dep_record("payments", 0.0)
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
    assert {r["service_name"] for r in deps5} == {"web-gateway", "payments"}
    _, deps6, _ = sim.tick(6.0)
    assert deps6 == []


def test_deploy_emits_deployment_record_and_mutates_version():
    # Force a bad deploy on payments at tick 0 (the only kind of deploy there, D-057).
    sim = _sim(seed=4, config=Config(quiet_period=0))
    _force(sim, BAD_DEPLOY, "payments")
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
        scheduled_end=end,
    )


def _run(sim: Simulation, ticks: range) -> dict[str, list[dict]]:
    """Every service's records over `ticks`, checking there is exactly one per metric service
    per tick (D-058)."""
    by_service: dict[str, list[dict]] = defaultdict(list)
    for k in ticks:
        metrics, _, _ = sim.tick(float(k))
        assert sorted(r["service_name"] for r in metrics) == ["payments", "web-gateway"], k
        for r in metrics:
            by_service[r["service_name"]].append(r)
    return by_service


@pytest.mark.parametrize(
    ("itype", "service", "traffic_mult", "latency_mult"),
    [
        (UPSTREAM_DROP, "payments", engine.DROP_LAMBDA_MULT, 1.0),  # no latency effect (D-059)
        (BAD_DEPLOY, "payments", 1.0, engine.BAD_DEPLOY_LATENCY_MULT),
    ],
    ids=["drop", "bad_deploy"],
)
def test_an_incident_moves_the_values_not_the_record_count(itype, service, traffic_mult, latency_mult):
    # D-058: one record per service per second, normal or not; the incident's multipliers
    # show in the values. 600 normal seconds, then 600 under the incident.
    sim = _sim(seed=12, config=Config(no_incidents=True))
    sim.engine.active[service] = _forced(itype, service, start=600.0, end=1200.0)
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
    sim.engine.active["payments"] = _forced(UPSTREAM_DROP, "payments", start=0.0, end=600.0)
    records = _run(sim, range(600))["payments"]
    empty = [r for r in records if r["traffic_count"] == 0]
    assert empty, "expected some empty seconds at 30 x 0.1 requests/s"
    assert all(r["latency_ms"] > 0 and r["error_count"] == 0 for r in empty)


def test_upstream_drop_never_zeroes_a_window():
    sim = _sim(seed=5, config=Config(no_incidents=True))
    # Force a drop on payments (the lowest-rps service) for a 10s window.
    sim.engine.active["payments"] = _forced(UPSTREAM_DROP, "payments", start=0.0, end=70.0)
    total = sum(r["traffic_count"] for r in _run(sim, range(10))["payments"])
    assert total > 0, "a dropped service must still count at least one request per window"


def test_bad_deploy_degrades_from_its_own_tick():
    # D-042: no onset delay. The deploy fires on a window boundary, and the 10-second
    # window it opens is already fully degraded.
    sim = _sim(seed=11)
    for k in range(3000):
        metrics, _, events = sim.tick(float(k))
        # a latency bad deploy; web-gateway's BAD_DEPLOY_ERRORS only fails requests (D-067)
        bad = next((e for e in events if e.kind == "bad_deploy" and e.incident.type == BAD_DEPLOY), None)
        if bad is not None:
            break
    else:
        raise AssertionError("expected a BAD_DEPLOY in this seeded run")
    assert bad.now % 10 == 0
    assert any(e.kind == "incident_start" and e.now == bad.now for e in events)
    window = [r for r in metrics if r["service_name"] == bad.service_name]
    for j in range(1, 10):
        window += [r for r in sim.tick(float(k + j))[0] if r["service_name"] == bad.service_name]
    baseline = METRIC_SERVICES_BY_NAME[bad.service_name].median_latency_ms
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
        assert [r["event_time"] for r in metrics] == [tick_ms + 999] * 2
        assert all(d["event_time"] == tick_ms for d in deps)


def test_a_record_follows_the_deploy_record_of_its_tick():
    # A forced bad deploy on payments: both its deploy and its rollback change the version.
    # On that tick the deploy record is at the tick and payments' record 999 ms later, so
    # the temporal join gives the record the new version.
    sim = Simulation(42, start=1_794_390_513.319599, config=Config(quiet_period=0))
    _force(sim, BAD_DEPLOY, "payments")
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
    for svc in METRIC_SERVICES:
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


# --- resolving an incident (D-065) ---------------------------------------------

AGENT_FOR_TRIGGER = {
    "LATENCY_HIGH": "release_agent",
    "ERROR_RATE_HIGH": "release_agent",  # D-067
    "TRAFFIC_HIGH": "capacity_agent",
    "TRAFFIC_LOW": "capacity_agent",
}
ONE_OF_EACH = [(BAD_DEPLOY, "payments"), (UPSTREAM_DROP, "payments")]


def _resolved_on_tick_17(itype, svc):
    """A run whose only incident is an `itype` on `svc` from 0, answered correctly (for its
    first window) by a decision read before tick 17. Returns the run, the start's events,
    and ticks 16 and 17."""
    sim = _sim(seed=4, config=Config(quiet_period=0))
    _force(sim, itype, svc)
    _, _, started = sim.tick(0.0)
    inc = started[-1].incident
    for k in range(1, 16):
        assert sim.tick(float(k))[2] == []
    tick_16 = sim.tick(16.0)
    answer = Decision(svc, 10.0, AGENT_FOR_TRIGGER[inc.expected_trigger], inc.expected_trigger, inc.expected_action)
    tick_17 = sim.tick(17.0, decisions=[answer])
    return sim, started, tick_16, tick_17


@pytest.mark.parametrize(("itype", "svc"), [*ONE_OF_EACH, (BAD_DEPLOY_ERRORS, "web-gateway")])
def test_a_resolved_incident_and_its_records_end_on_the_tick_that_read_the_decision(itype, svc):
    sim, started, _, (_, deps, events) = _resolved_on_tick_17(itype, svc)
    [end] = events
    assert (end.kind, end.now, end.incident) == ("incident_end", 17.0, started[-1].incident)
    assert end_record(end.incident, end.now)["ended_at"] == 17_000  # the real end, not the cap
    if itype in BAD_DEPLOY_TYPES:  # its ROLLBACK restores the version the bad deploy replaced
        [dep] = deps
        assert (dep["service_name"], dep["last_change"], dep["event_time"]) == (svc, "ROLLBACK", 17_000)
        assert dep["version"] == started[0].old_version
    else:
        assert deps == []
    assert all(sim.tick(float(k))[2] == [] for k in range(18, 40))  # it doesn't end again at 35


def _times_normal(record, itype):
    """The sample's incident metric as a multiple of its service's normal level."""
    field, normal = ("latency_ms", "median_latency_ms") if itype == BAD_DEPLOY else ("traffic_count", "rps")
    return record[field] / getattr(METRIC_SERVICES_BY_NAME[record["service_name"]], normal)


@pytest.mark.parametrize(("itype", "svc"), ONE_OF_EACH)
def test_the_tick_that_reads_the_decision_samples_normal_metrics(itype, svc):
    _, _, (before, _, _), (at, _, _) = _resolved_on_tick_17(itype, svc)
    [before] = [r for r in before if r["service_name"] == svc]
    [at] = [r for r in at if r["service_name"] == svc]
    assert not 0.5 < _times_normal(before, itype) < 2, before  # x4 or x0.1 on tick 16
    assert 0.5 < _times_normal(at, itype) < 2, at


def test_aligned_start_is_the_next_ten_second_boundary():
    from opsgen.driver import aligned_start

    assert aligned_start(1_800_000_003.4) == 1_800_000_010.0
    assert aligned_start(1_800_000_010.0) == 1_800_000_010.0
    # starting there, the first window is full: every tick of window 0 is produced
    sim = Simulation(3, start=aligned_start(1_800_000_003.4), config=Config())
    first_window = {r["event_time"] // 10_000 for k in range(10) for r in sim.tick(sim.start + k)[0]}
    assert first_window == {180_000_001}
