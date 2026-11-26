"""web-gateway's deploy clock (D-067): good deploys, and bad ones that fail 40% of requests."""

import random
import statistics
from itertools import pairwise

import pytest

from opsgen import engine
from opsgen.deployments import DEPLOY, DeploymentState
from opsgen.engine import (
    BAD_DEPLOY_ERRORS,
    BAD_DEPLOY_ERRORS_ERROR_RATE,
    WINDOW_SECONDS,
    Config,
    Decision,
    Engine,
)
from opsgen.groundtruth import start_record
from opsgen.services import METRIC_SERVICES
from opsgen.simulation import Simulation

GATEWAY = "web-gateway"


def _engine(seed, config=None):
    return Engine(rng=random.Random(seed), start=0.0, config=config or Config(), deploy_rng=random.Random(seed + 2))


def _deployments():
    return {s.name: DeploymentState.initial(s, 0.0) for s in METRIC_SERVICES}


def _run(eng, horizon, resolve_after=None):
    """Tick `eng` over `horizon` seconds. With `resolve_after`, every BAD_DEPLOY_ERRORS gets
    its correct answer that many seconds after it started. Returns the events."""
    deployments = _deployments()
    events = []
    for k in range(horizon):
        now = float(k)
        inc = eng.active.get(GATEWAY)
        if resolve_after is not None and inc is not None and now == inc.degradation_start + resolve_after:
            answer = Decision(GATEWAY, inc.degradation_start + 10, "release_agent", "ERROR_RATE_HIGH", "ROLLBACK")
            assert eng.resolve(answer, now)
        events.extend(eng.tick(now, deployments))
    return events


def _gateway_deploys(events):
    """web-gateway's deploys in order, as (time, "good" | "bad")."""
    kinds = {"deploy": "good", "bad_deploy": "bad"}
    return [(e.now, kinds[e.kind]) for e in events if e.service_name == GATEWAY and e.kind in kinds]


@pytest.mark.parametrize("seed", range(10))
def test_the_opening_is_a_good_deploy_then_a_bad_one(seed):
    events = _run(_engine(seed), 400)
    (t1, k1), (t2, k2) = _gateway_deploys(events)[:2]
    assert (t1, k1) == (60.0, "good")
    assert k2 == "bad" and t2 - t1 in {60.0, 70.0, 80.0, 90.0}
    # the bad deploy starts its incident on the same tick, before the 150 s quiet period ends
    [inc] = [e.incident for e in events if e.kind == "incident_start" and e.service_name == GATEWAY][:1]
    assert (inc.type, inc.degradation_start) == (BAD_DEPLOY_ERRORS, t2)
    assert (inc.expected_trigger, inc.expected_action) == ("ERROR_RATE_HIGH", "ROLLBACK")


@pytest.mark.parametrize("seed", range(4))
def test_deploys_land_on_boundaries_60_to_90_seconds_apart(seed):
    deploys = _gateway_deploys(_run(_engine(seed), 3 * 3600))
    assert all(t % WINDOW_SECONDS == 0 for t, _ in deploys)
    gaps = {b - a for (a, _), (b, _) in pairwise(deploys)}
    assert gaps == {60.0, 70.0, 80.0, 90.0}


def test_never_three_good_deploys_in_a_row_and_four_in_seven_are_bad():
    # Bad with probability 1/2, but the third after two good ones is forced bad: in the long
    # run that makes 4/7 of the deploys bad (a three-state chain: 0, 1 or 2 good in a row).
    kinds = []
    for seed in range(10):
        mine = [k for _, k in _gateway_deploys(_run(_engine(seed), 3 * 3600))]
        assert "good,good,good" not in ",".join(mine), seed
        kinds += mine
    assert kinds.count("bad") / len(kinds) == pytest.approx(4 / 7, abs=0.03)


@pytest.mark.parametrize("seed", range(4))
def test_no_deploy_while_a_web_gateway_incident_is_active(seed):
    events = _run(_engine(seed), 3 * 3600)
    busy_until = -1.0
    for e in events:
        if e.service_name != GATEWAY:
            continue
        if e.kind in ("deploy", "bad_deploy"):
            assert e.now >= busy_until, e
        if e.kind == "incident_end":
            busy_until = e.now


def test_the_clock_is_the_same_however_incidents_end():
    never = _gateway_deploys(_run(_engine(5), 2 * 3600))
    resolved = _gateway_deploys(_run(_engine(5), 2 * 3600, resolve_after=11))
    assert len(never) > 50 and resolved == never


def _shared_incidents(events):
    return [
        (e.now, e.service_name, e.incident.type)
        for e in events
        if e.kind == "incident_start" and e.service_name != GATEWAY
    ]


def test_the_clock_never_moves_the_shared_schedule(monkeypatch):
    # D-067: the clock has its own rng, so how often web-gateway deploys, and whether at all,
    # leaves payments' incidents where they were (the shared scheduler's only service, D-068).
    normal = _shared_incidents(_run(_engine(7), 2 * 3600))
    off = _engine(7)
    off.clock = None
    assert _shared_incidents(_run(off, 2 * 3600)) == normal
    monkeypatch.setattr(engine, "DEPLOY_CLOCK_BAD_SHARE", 1.0)  # every deploy bad
    assert _shared_incidents(_run(_engine(7), 2 * 3600)) == normal
    assert len(normal) > 30  # about 19 an hour


def test_an_error_rate_answer_resolves_a_bad_deploy_and_a_latency_one_does_not():
    eng, deployments = _engine(1), _deployments()
    k = 0
    while GATEWAY not in eng.active:
        eng.tick(float(k), deployments)
        k += 1
    inc = eng.active[GATEWAY]
    bad_version, good_version = deployments[GATEWAY].version, deployments[GATEWAY].previous_version
    window_end = inc.degradation_start + 10
    latency = Decision(GATEWAY, window_end, "release_agent", "LATENCY_HIGH", "ROLLBACK")
    assert eng.resolve(latency, window_end + 1) is False
    errors = Decision(GATEWAY, window_end, "release_agent", "ERROR_RATE_HIGH", "ROLLBACK")
    assert eng.resolve(errors, window_end + 1) is True
    [end] = eng.tick(window_end + 1, deployments)
    assert (end.kind, end.old_version, end.new_version) == ("incident_end", bad_version, good_version)
    assert deployments[GATEWAY].version == good_version


def test_the_start_record_expects_an_error_rate_trigger():
    eng, deployments = _engine(2), _deployments()
    k = 0
    while GATEWAY not in eng.active:
        eng.tick(float(k), deployments)
        k += 1
    record = start_record(eng.active[GATEWAY], float(k - 1))
    assert record["incident_type"] == BAD_DEPLOY_ERRORS
    assert (record["expected_trigger"], record["expected_action"]) == ("ERROR_RATE_HIGH", "ROLLBACK")


def test_a_bad_deploy_record_looks_like_a_good_one():
    # The deployments stream can't tell them apart: only the errors do (D-067).
    sim = Simulation(3, start=0.0)
    seen = {}
    for k in range(600):
        _, deps, events, *_ = sim.tick(float(k))
        for e in events:
            if e.service_name == GATEWAY and e.kind in ("deploy", "bad_deploy"):
                [rec] = [d for d in deps if d["service_name"] == GATEWAY]
                seen.setdefault(e.kind, rec)
                assert (rec["last_change"], rec["previous_version"], rec["version"]) == (
                    DEPLOY,
                    e.old_version,
                    e.new_version,
                )
    assert set(seen) == {"deploy", "bad_deploy"}
    assert seen["deploy"].keys() == seen["bad_deploy"].keys()


def test_a_bad_deploy_fails_40_percent_and_leaves_traffic_and_latency_alone():
    sim = Simulation(4, start=0.0)
    during, outside = [], []
    for k in range(3600):
        metrics = sim.tick(float(k)).metrics
        inc = sim.engine.active.get(GATEWAY)
        [rec] = [m for m in metrics if m["service_name"] == GATEWAY]
        degraded = inc is not None and inc.degradation_start <= k < inc.degradation_end
        (during if degraded else outside).append(rec)

    def error_rate(recs):
        return sum(r["error_count"] for r in recs) / sum(r["traffic_count"] for r in recs)

    assert len(during) > 500
    assert error_rate(during) == pytest.approx(BAD_DEPLOY_ERRORS_ERROR_RATE, abs=0.01) == pytest.approx(0.40)
    assert error_rate(outside) < 0.01
    for field, tolerance in (("traffic_count", 0.03), ("latency_ms", 0.03)):
        a = statistics.mean(r[field] for r in during)
        b = statistics.mean(r[field] for r in outside)
        assert a == pytest.approx(b, rel=tolerance), field


def test_no_incidents_means_web_gateway_never_deploys():
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True))
    for k in range(3600):
        events = sim.tick(float(k)).events
        assert not events
    assert sim.deployments[GATEWAY].version == "3.4.2"
