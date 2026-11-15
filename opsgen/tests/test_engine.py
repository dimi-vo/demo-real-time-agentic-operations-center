"""Incident scheduler rules (section 4.5)."""

import random
from itertools import pairwise

import pytest

from opsgen.deployments import DeploymentState
from opsgen.engine import (
    BAD_DEPLOY,
    CANDIDATES,
    INCIDENT_TYPES,
    TRAFFIC_SURGE,
    UPSTREAM_DROP,
    Config,
    Engine,
)
from opsgen.services import SERVICES, service_names


def _deployments(now=0.0):
    return {s.name: DeploymentState.initial(s, now) for s in SERVICES}


def _run(config, seed, horizon):
    rng = random.Random(seed)
    deployments = _deployments()
    eng = Engine(rng=rng, start=0.0, config=config)
    events = []
    for k in range(horizon):
        events.extend(eng.tick(float(k), deployments))
    return events, eng, deployments


def _incidents(events):
    """Incident objects, one per incident_start event, in start order."""
    return [e.incident for e in events if e.kind == "incident_start"]


def test_quiet_period_no_incident_before_it():
    config = Config(quiet_period=180, spacing_min=240, spacing_max=360)
    events, _, _ = _run(config, seed=1, horizon=1200)
    incs = _incidents(events)
    assert incs, "expected at least one incident in 20 minutes"
    assert all(inc.degradation_start >= 180 for inc in incs)
    # The first incident should start promptly once the quiet period ends.
    assert min(inc.degradation_start for inc in incs) == 180


def test_spacing_between_incidents():
    config = Config(quiet_period=120, spacing_min=240, spacing_max=360)
    events, _, _ = _run(config, seed=2, horizon=3600)
    incs = sorted(_incidents(events), key=lambda i: i.degradation_start)
    assert len(incs) >= 3
    for prev, nxt in pairwise(incs):
        gap = nxt.degradation_start - prev.degradation_end
        # the spacing, rounded up to the next 10-second boundary
        assert 240 <= gap < 360 + 10, f"spacing {gap} out of range"


def test_only_one_incident_active_at_a_time():
    config = Config(quiet_period=60, spacing_min=120, spacing_max=180)
    events, _, _ = _run(config, seed=3, horizon=3600)
    depth = 0
    for e in events:
        if e.kind == "incident_start":
            depth += 1
            assert depth == 1
        elif e.kind == "incident_end":
            depth -= 1
            assert depth == 0


def test_cooldown_default_is_seven_minutes():
    assert Config().cooldown == 420


def test_cooldown_per_service():
    config = Config(quiet_period=30, spacing_min=30, spacing_max=45)
    events, _, _ = _run(config, seed=4, horizon=7200)
    by_service: dict[str, list] = {}
    for inc in _incidents(events):
        by_service.setdefault(inc.service_name, []).append(inc)
    assert any(len(incs) >= 2 for incs in by_service.values()), "expected a repeat to check"
    for incs in by_service.values():
        incs.sort(key=lambda i: i.degradation_start)
        for prev, nxt in pairwise(incs):
            assert nxt.degradation_start - prev.degradation_end >= 420


def test_bad_deploy_lifecycle_and_rollback():
    config = Config(quiet_period=10, spacing_min=30, spacing_max=30)
    events, _, _ = _run(config, seed=42, horizon=2000)
    bad = next((i for i in _incidents(events) if i.type == BAD_DEPLOY), None)
    assert bad is not None, "expected a BAD_DEPLOY in this seeded run"
    mine = [e for e in events if e.incident is bad]
    # The deploy and the degradation start on the same tick (D-042), deploy first.
    assert [(e.kind, e.now) for e in mine] == [
        ("bad_deploy", bad.degradation_start),
        ("incident_start", bad.degradation_start),
        ("incident_end", bad.degradation_end),
    ]
    deploy, _, end = mine
    good, broken = deploy.old_version, deploy.new_version
    assert good is not None and broken is not None and good != broken
    # the end rolls the broken version back to the good one
    assert (end.old_version, end.new_version) == (broken, good)


def test_every_incident_lasts_the_duration_on_window_boundaries():
    # Start off a boundary to prove the rounding: the windows are epoch-aligned, not
    # aligned to the generator's start.
    rng = random.Random(9)
    deployments = _deployments(3.0)
    eng = Engine(rng=rng, start=3.0, config=Config())
    events = []
    for k in range(2 * 3600):
        events.extend(eng.tick(3.0 + k, deployments))
    incs = _incidents(events)
    assert len(incs) >= 10
    for inc in incs:
        assert inc.degradation_start % 10 == 0, inc
        assert inc.degradation_end - inc.degradation_start == Config().duration == 20, inc  # D-059
    assert {i.type for i in incs} == {BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP}


def test_no_incidents_runs_an_hour_without_a_deploy():
    # D-057: the only deploys are bad deploys. With --no-incidents the deployments stream
    # is the bootstrap record and then the 5-s heartbeat, which never changes a version.
    from opsgen.cli import _build_parser, _config_from_args
    from opsgen.simulation import Simulation

    sim = Simulation(5, start=0.0, config=_config_from_args(_build_parser().parse_args(["--no-incidents"])))
    boot = {r["service_name"]: r for r in sim.bootstrap()}
    heartbeats = 0
    for k in range(3600):
        _, deps, events = sim.tick(float(k))
        assert events == [], events
        for r in deps:
            heartbeats += 1
            assert r["last_change"] == "INITIAL", r
            assert (r["version"], r["deployed_at"]) == (
                boot[r["service_name"]]["version"],
                boot[r["service_name"]]["deployed_at"],
            ), r
    assert heartbeats == 3 * (3600 // 5 - 1)  # every 5 s, except tick 0 (the bootstrap covers it)


def test_versions_change_only_on_bad_deploys_and_their_rollbacks():
    events, _, _ = _run(Config(), seed=7, horizon=3 * 3600)
    assert {e.kind for e in events} == {"bad_deploy", "incident_start", "incident_end"}
    changes = [e for e in events if e.old_version is not None]
    assert changes, "expected a bad deploy in three hours"
    for e in changes:
        assert e.incident is not None and e.incident.type == BAD_DEPLOY, e


def test_choose_incident_repicks_when_a_type_has_no_candidate():
    eng = Engine(rng=random.Random(0), start=0.0, config=Config(cooldown=900))
    now = 1000.0
    # Put every TRAFFIC_SURGE candidate in cooldown.
    for svc in CANDIDATES[TRAFFIC_SURGE]:
        eng.last_incident_end[svc] = now - 10
    picks = [eng._choose_incident(now) for _ in range(500)]
    assert all(p is not None for p in picks)
    assert all(p[0] != TRAFFIC_SURGE for p in picks)


def test_choose_incident_returns_none_when_all_in_cooldown():
    eng = Engine(rng=random.Random(0), start=0.0, config=Config(cooldown=900))
    now = 1000.0
    for name in service_names():
        eng.last_incident_end[name] = now - 10
    assert eng._choose_incident(now) is None


ONE_ANSWER = {BAD_DEPLOY: "ROLLBACK", TRAFFIC_SURGE: "SCALE_OUT", UPSTREAM_DROP: "INVESTIGATE_UPSTREAM"}


@pytest.mark.parametrize(("itype", "svc"), [(t, s) for t in INCIDENT_TYPES for s in CANDIDATES[t]])
def test_every_incident_has_exactly_one_expected_action(itype, svc):
    # D-043: the action depends on the type only, whatever the service.
    eng = Engine(rng=random.Random(0), start=0.0, config=Config(quiet_period=0))
    eng._choose_incident = lambda now: (itype, svc)
    [inc] = _incidents(eng.tick(0.0, _deployments()))
    assert (inc.type, inc.service_name, inc.expected_action) == (itype, svc, ONE_ANSWER[itype])


def test_incident_candidates_match_spec():
    # D-056: three services. Surges only on web-gateway, drops on checkout and payments,
    # bad deploys on all three.
    assert set(service_names()) == {"web-gateway", "checkout", "payments"}
    assert CANDIDATES[TRAFFIC_SURGE] == ("web-gateway",)
    assert set(CANDIDATES[UPSTREAM_DROP]) == {"checkout", "payments"}
    assert set(CANDIDATES[BAD_DEPLOY]) == {"web-gateway", "checkout", "payments"}


def test_incident_numbering_continues_after_a_restart(tmp_path):
    # D-046: the counter lives in the generator state file, next to the versions.
    from opsgen.simulation import Simulation
    from opsgen.state import load_state, save_state, state_path

    path = state_path(tmp_path)
    assert load_state(path) is None
    config = Config(quiet_period=0, spacing_min=1, spacing_max=2)
    first = Simulation(1, start=0.0, config=config)
    started = []
    for now in range(0, 400):
        _, _, events = first.tick(float(now))
        started += [ev.incident.incident_id for ev in events if ev.kind == "incident_start"]
    assert len(started) >= 2
    save_state(path, first.state_snapshot())
    # a restarted generator picks up after the last id it used, and keeps the versions
    second = Simulation(2, start=1000.0, config=config, initial_state=load_state(path))
    assert {n: d.version for n, d in second.deployments.items()} == {
        n: d.version for n, d in first.deployments.items()
    }
    ids = []
    for now in range(1000, 1300):
        _, _, events = second.tick(float(now))
        ids += [ev.incident.incident_id for ev in events if ev.kind == "incident_start"]
    assert ids and ids[0] == f"INC{len(started) + 1:04d}"
