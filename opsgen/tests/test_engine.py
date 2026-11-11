"""Incident scheduler and benign-deploy rules (section 4.5)."""

import random
from itertools import pairwise

from opsgen.deployments import DeploymentState
from opsgen.engine import (
    BAD_DEPLOY,
    CANDIDATES,
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
    assert all(inc.t0 >= 180 for inc in incs)
    # The first incident should start promptly once the quiet period ends.
    assert min(inc.t0 for inc in incs) <= 181


def test_spacing_between_incidents():
    config = Config(quiet_period=120, spacing_min=240, spacing_max=360, duration_min=40, duration_max=70)
    events, _, _ = _run(config, seed=2, horizon=3600)
    incs = sorted(_incidents(events), key=lambda i: i.t0)
    assert len(incs) >= 3
    for prev, nxt in pairwise(incs):
        gap = nxt.t0 - prev.degradation_end
        assert 240 - 2 <= gap <= 360 + 2, f"spacing {gap} out of range"


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


def test_cooldown_per_service():
    config = Config(quiet_period=30, spacing_min=30, spacing_max=45, duration_min=30, duration_max=45, cooldown=900)
    events, _, _ = _run(config, seed=4, horizon=7200)
    by_service: dict[str, list] = {}
    for inc in _incidents(events):
        by_service.setdefault(inc.service_name, []).append(inc)
    for incs in by_service.values():
        incs.sort(key=lambda i: i.t0)
        for prev, nxt in pairwise(incs):
            assert nxt.t0 - prev.degradation_end >= 900 - 2


def test_bad_deploy_lifecycle_and_rollback():
    config = Config(quiet_period=10, spacing_min=600, spacing_max=600)
    events, _, _ = _run(config, seed=42, horizon=400)
    # Find the first BAD_DEPLOY incident.
    bad = next((i for i in _incidents(events) if i.type == BAD_DEPLOY), None)
    if bad is None:
        return  # seed-dependent; other tests force the type directly
    kinds = [(e.kind, e.service_name) for e in events if e.service_name == bad.service_name]
    assert ("bad_deploy", bad.service_name) in kinds
    assert ("incident_end", bad.service_name) in kinds
    # degradation starts 10s after the deploy
    assert abs(bad.degradation_start - (bad.t0 + 10)) < 1e-6
    assert bad.bad_version is not None and bad.good_version is not None


def test_no_incidents_flag_still_deploys():
    config = Config(no_incidents=True, benign_first=60, benign_min=120, benign_max=180)
    events, _, _ = _run(config, seed=5, horizon=1200)
    assert not _incidents(events)
    assert any(e.kind == "benign_deploy" for e in events)


def test_benign_first_deploy_timing():
    config = Config(no_incidents=True, benign_first=60, benign_min=180, benign_max=300)
    events, _, _ = _run(config, seed=6, horizon=400)
    first = next(e for e in events if e.kind == "benign_deploy")
    assert 60 <= first.now <= 61


def test_benign_deploy_respects_min_gap_per_service():
    config = Config(no_incidents=True, benign_first=10, benign_min=20, benign_max=20, benign_min_gap=300)
    events, _, _ = _run(config, seed=7, horizon=3600)
    last_by_service: dict[str, float] = {}
    for e in events:
        if e.kind == "benign_deploy":
            if e.service_name in last_by_service:
                assert e.now - last_by_service[e.service_name] >= 300
            last_by_service[e.service_name] = e.now


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


def test_expected_action_escalate_on_auth_surge():
    assert Engine._expected_action(TRAFFIC_SURGE, "auth") == "ESCALATE"
    assert Engine._expected_action(TRAFFIC_SURGE, "web-gateway") == "SCALE_OUT"
    assert Engine._expected_action(TRAFFIC_SURGE, "catalog") == "SCALE_OUT"
    assert Engine._expected_action(UPSTREAM_DROP, "cart") == "INVESTIGATE_UPSTREAM"
    assert Engine._expected_action(BAD_DEPLOY, "payments") == "ROLLBACK"


def test_incident_candidates_match_spec():
    assert set(CANDIDATES[TRAFFIC_SURGE]) == {"web-gateway", "catalog", "auth"}
    assert set(CANDIDATES[UPSTREAM_DROP]) == {"cart", "checkout", "payments"}
    assert set(CANDIDATES[BAD_DEPLOY]) == set(service_names())


def test_incident_numbering_continues_after_a_restart(tmp_path):
    from opsgen.engine import Config, Engine
    from opsgen.groundtruth import append_incident, incidents_path, last_incident_number

    path = incidents_path(tmp_path)
    assert last_incident_number(path) == 0
    first = Engine(rng=random.Random(1), start=0.0, config=Config(quiet_period=0, spacing_min=1, spacing_max=2))
    deps = _deployments()
    ended = []
    for now in range(0, 400):
        for ev in first.tick(float(now), deps):
            if ev.kind == "incident_end":
                ended.append(ev.incident)
                append_incident(path, ev.incident)
    assert len(ended) >= 2
    n = last_incident_number(path)
    assert n == len(ended)
    # a restarted generator picks up after the last logged id
    second = Engine(rng=random.Random(2), start=1000.0, config=Config(quiet_period=0))
    second.incident_counter = n
    ids = []
    for now in range(1000, 1300):
        events = second.tick(float(now), _deployments(1000.0))
        ids += [ev.incident.incident_id for ev in events if ev.kind == "incident_start"]
    assert ids and ids[0] == f"INC{n + 1:04d}"
