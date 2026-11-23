"""Incident scheduler rules (section 4.5, D-060)."""

import random
import re
from itertools import combinations, pairwise
from pathlib import Path

import pytest

from opsgen import engine
from opsgen.deployments import DeploymentState
from opsgen.engine import (
    BAD_DEPLOY,
    CANDIDATES,
    INCIDENT_TYPES,
    INCIDENT_WEIGHTS,
    SHARED_SERVICES,
    TRAFFIC_SURGE,
    UPSTREAM_DROP,
    WINDOW_SECONDS,
    Config,
    Decision,
    Engine,
    Incident,
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


def _incident(itype, svc, start):
    return Incident(
        incident_id="INC0001",
        type=itype,
        service_name=svc,
        degradation_start=start,
        degradation_end=start + Config().max_duration,
        expected_trigger=engine.TRIGGER[itype],
        expected_action=engine.EXPECTED_ACTION[itype],
        scheduled_end=start + Config().max_duration,
    )


def _by_service(incidents):
    out: dict[str, list] = {}
    for inc in incidents:
        out.setdefault(inc.service_name, []).append(inc)
    for incs in out.values():
        incs.sort(key=lambda i: i.degradation_start)
    return out


def _force(eng, itype, svc):
    """Make the engine's next choice `(itype, svc)`, and then no other."""
    picks = iter([(itype, svc)])
    eng._choose_incident = lambda now: next(picks, None)


def test_quiet_period_follows_the_detector_warm_up():
    # D-060: the detector scores nothing for its first minTrainingSize + 1 windows; the
    # quiet period covers them plus one window of margin, so every incident is scored.
    sql = (Path(__file__).resolve().parents[2] / "sql/2.3_anomaly_status.sql").read_text()
    sizes = {int(m) for m in re.findall(r"'minTrainingSize'\s+VALUE\s+(\d+)", sql)}
    assert len(sizes) == 1, sizes
    (min_training,) = sizes
    assert Config().quiet_period >= (min_training + 1) * WINDOW_SECONDS + WINDOW_SECONDS


def test_quiet_period_no_incident_before_it():
    events, _, _ = _run(Config(), seed=1, horizon=1200)
    incs = _incidents(events)
    assert incs, "expected incidents in 20 minutes"
    assert Config().quiet_period == 150
    # Every service is free when the quiet period ends: the first incident starts then.
    assert min(inc.degradation_start for inc in incs) == 150


def test_incidents_overlap_across_services():
    # D-060: one schedule per service, so incidents on different services overlap.
    events, _, _ = _run(Config(), seed=2, horizon=3600)
    incs = _incidents(events)
    overlapping = [
        (a, b)
        for a, b in combinations(incs, 2)
        if a.service_name != b.service_name
        and a.degradation_start < b.degradation_end
        and b.degradation_start < a.degradation_end
    ]
    assert len(overlapping) >= 10, len(overlapping)


@pytest.mark.parametrize("seed", range(5))
def test_never_two_incidents_at_once_on_a_service(seed):
    events, _, _ = _run(Config(), seed=seed, horizon=2 * 3600)
    open_on: dict[str, int] = {}
    for e in events:
        if e.kind == "incident_start":
            open_on[e.service_name] = open_on.get(e.service_name, 0) + 1
            assert open_on[e.service_name] == 1, e
        elif e.kind == "incident_end":
            open_on[e.service_name] -= 1
    assert set(open_on) == set(SHARED_SERVICES)


@pytest.mark.parametrize("seed", range(5))
def test_the_shared_scheduler_never_starts_an_incident_on_web_gateway(seed):
    # D-067: web-gateway deploys on its own clock. The shared scheduler serves checkout (bad
    # deploys and surges) and payments (bad deploys and drops).
    events, _, _ = _run(Config(), seed=seed, horizon=3 * 3600)
    by_service = {svc: {i.type for i in incs} for svc, incs in _by_service(_incidents(events)).items()}
    assert by_service == {"checkout": {BAD_DEPLOY, TRAFFIC_SURGE}, "payments": {BAD_DEPLOY, UPSTREAM_DROP}}


@pytest.mark.parametrize("seed", range(5))
def test_twenty_seconds_between_incidents_on_a_service(seed):
    events, _, _ = _run(Config(), seed=seed, horizon=2 * 3600)
    gaps = []
    for incs in _by_service(_incidents(events)).values():
        gaps += [nxt.degradation_start - prev.degradation_end for prev, nxt in pairwise(incs)]
    assert gaps and min(gaps) >= Config().gap == 20
    # A service starts again as soon as it may: at the first boundary 20 s after its last
    # incident ended. A 35 s incident ends mid-window (D-065), so that is 25 s after (T+60).
    assert min(gaps) == 25, "a service should start again as soon as it may"


@pytest.mark.parametrize("seed", range(5))
def test_no_same_type_repeat_on_a_service_within_its_rest(seed):
    # D-059's rests, measured from the end of that type's last incident on that service.
    assert Config().rests == {BAD_DEPLOY: 390, TRAFFIC_SURGE: 300, UPSTREAM_DROP: 300}
    events, _, _ = _run(Config(), seed=seed, horizon=2 * 3600)
    for incs in _by_service(_incidents(events)).values():
        for itype in INCIDENT_TYPES:
            same = [i for i in incs if i.type == itype]
            for prev, nxt in pairwise(same):
                assert nxt.degradation_start - prev.degradation_end >= Config().rests[itype], (prev, nxt)


def test_the_first_three_incidents_are_one_of_each_type():
    # D-060: the first surge, drop and bad deploy all come in the first minute or so,
    # in a random order.
    orders = set()
    for seed in range(100):
        events, _, _ = _run(Config(), seed=seed, horizon=300)
        first_three = [i.type for i in _incidents(events)[:3]]
        assert sorted(first_three) == sorted(INCIDENT_TYPES), (seed, first_three)
        orders.add(tuple(first_three))
    assert len(orders) >= 4, orders


def test_about_37_incidents_an_hour():
    # About 19 an hour per service: D-065 had 56 on three services, D-067 leaves two
    # (checkout and payments). These seeds give 36 in the first hour for every one and 38 an
    # hour at steady state (hours 2 and 3 of a 3-hour run).
    first_hour, steady = [], []
    for seed in range(20):
        events, _, _ = _run(Config(), seed=seed, horizon=3 * 3600)
        starts = [i.degradation_start for i in _incidents(events)]
        first_hour.append(sum(t < 3600 for t in starts))
        steady.append(sum(t >= 3600 for t in starts) / 2)
    assert all(34 <= n <= 39 for n in first_hour), first_hour
    assert 36 <= sum(steady) / len(steady) <= 40, steady


def test_bad_deploy_lifecycle_and_rollback():
    events, _, _ = _run(Config(), seed=42, horizon=2000)
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


def test_every_incident_lasts_the_max_duration_from_a_window_boundary():
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
        # With no agent in the loop, every incident runs to its cap (D-065).
        assert inc.degradation_end - inc.degradation_start == Config().max_duration == 35, inc
    assert {i.type for i in incs} == {BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP}


def test_the_cap_ends_an_incident_mid_window():
    # D-065: an incident that runs to its cap ends mid-window, so the return to baseline
    # splits over two windows, in two steps small enough to stay inside the detector's band.
    # A cap on a boundary brings the after-spike echoes back: offline, 35 s had none, where
    # 30 s and 40 s had 80x and 670x the echoes of the old fixed 20 s.
    assert Config().max_duration % WINDOW_SECONDS != 0


def test_gap_and_rests_count_from_the_scheduled_end():
    # D-065: an incident that ends early leaves the schedule as it was. Cut a surge on
    # checkout to 10 s by hand: the service's next incident still waits for the gap after
    # start + 35 s, and its next surge for the surge's rest after start + 35 s.
    eng = _past_first_three(Engine(rng=random.Random(0), start=0.0, config=Config(quiet_period=0)))
    deployments = _deployments()
    real_choice = eng._choose_incident
    _force(eng, TRAFFIC_SURGE, "checkout")
    eng.tick(0.0, deployments)
    eng._choose_incident = real_choice
    surge = eng.active["checkout"]
    assert surge.scheduled_end == 35
    surge.degradation_end = 10.0
    on_checkout = []
    for k in range(1, 400):
        started = _incidents(eng.tick(float(k), deployments))
        on_checkout += [(i.type, i.degradation_start) for i in started if i.service_name == "checkout"]
    # Dated from the real end (10 s), the gap would allow a bad deploy at 30 and the rest a
    # surge at 310.
    assert on_checkout[:2] == [(BAD_DEPLOY, 60.0), (TRAFFIC_SURGE, 340.0)]


# --- resolving an incident (D-065) ---------------------------------------------

AGENT_FOR_TRIGGER = {"LATENCY_HIGH": "release_agent", "TRAFFIC_HIGH": "capacity_agent", "TRAFFIC_LOW": "capacity_agent"}
ONE_OF_EACH = [(BAD_DEPLOY, "payments"), (TRAFFIC_SURGE, "checkout"), (UPSTREAM_DROP, "payments")]


def _started(itype, svc):
    """An engine whose only incident is an `itype` on `svc`, started at 0."""
    eng = Engine(rng=random.Random(0), start=0.0, config=Config(quiet_period=0))
    deployments = _deployments()
    _force(eng, itype, svc)
    events = eng.tick(0.0, deployments)
    return eng, deployments, events


def _answer(inc, window_end=10.0, **changes):
    """The agent's decision on `inc`'s service for the window ending at `window_end`: the
    correct one, unless `changes` say otherwise."""
    fields = {
        "service_name": inc.service_name,
        "window_end": window_end,
        "agent_name": AGENT_FOR_TRIGGER[inc.expected_trigger],
        "trigger": inc.expected_trigger,
        "action": inc.expected_action,
    } | changes
    return Decision(**fields)


def _tick_through(eng, deployments, ticks, decisions=None):
    """Tick over `ticks`, resolving `decisions[k]` before the tick at k, as
    `Simulation.tick` does. Returns the events."""
    events = []
    for k in ticks:
        for d in (decisions or {}).get(k, ()):
            eng.resolve(d, float(k))
        events.extend(eng.tick(float(k), deployments))
    return events


@pytest.mark.parametrize(("itype", "svc"), ONE_OF_EACH)
def test_a_correct_decision_ends_its_incident_on_the_tick_that_reads_it(itype, svc):
    eng, deployments, started = _started(itype, svc)
    inc = eng.active[svc]
    assert _tick_through(eng, deployments, range(1, 17)) == []
    assert eng.resolve(_answer(inc), 17.0) is True
    [end] = eng.tick(17.0, deployments)
    assert (end.kind, end.now, end.incident) == ("incident_end", 17.0, inc)
    assert inc.degradation_end == 17.0 and svc not in eng.active
    assert inc.resolved_by == f"{AGENT_FOR_TRIGGER[inc.expected_trigger]} {inc.expected_action}"
    if itype == BAD_DEPLOY:  # the ROLLBACK restores the version the bad deploy replaced
        deploy = started[0]
        assert (end.old_version, end.new_version) == (deploy.new_version, deploy.old_version)


@pytest.mark.parametrize(
    ("itype", "svc", "action"),
    [
        (BAD_DEPLOY, "payments", "OBSERVE"),
        (BAD_DEPLOY, "payments", "INVALID"),
        (TRAFFIC_SURGE, "checkout", "OBSERVE"),
        (TRAFFIC_SURGE, "checkout", "INVESTIGATE_UPSTREAM"),  # the wrong capacity action
        (UPSTREAM_DROP, "payments", "INVALID"),
        (UPSTREAM_DROP, "payments", "SCALE_OUT"),  # the wrong capacity action
    ],
)
def test_any_other_action_leaves_the_incident_running_to_its_cap(itype, svc, action):
    eng, deployments, _ = _started(itype, svc)
    inc = eng.active[svc]
    assert eng.resolve(_answer(inc, action=action), 17.0) is False
    [end] = _tick_through(eng, deployments, range(17, 40))
    assert end.now == inc.degradation_end == 35.0 and inc.resolved_by is None


def test_a_correct_answer_on_a_later_window_still_resolves_it():
    eng, deployments, _ = _started(TRAFFIC_SURGE, "checkout")
    inc = eng.active["checkout"]
    decisions = {13: [_answer(inc, window_end=10.0, action="OBSERVE")], 23: [_answer(inc, window_end=20.0)]}
    [end] = _tick_through(eng, deployments, range(1, 40), decisions)
    assert end.now == inc.degradation_end == 23.0
    assert inc.resolved_by == "capacity_agent SCALE_OUT"


@pytest.mark.parametrize(
    "changes",
    [
        {"trigger": "TRAFFIC_HIGH"},  # the right action under the wrong trigger
        {"window_end": 0.0},  # the window ended as the incident started
        {"window_end": -10.0},  # a stale answer, for a window before the incident
        {"service_name": "checkout"},  # another service, which has no incident
    ],
    ids=["trigger", "window-at-start", "window-before", "service"],
)
def test_a_decision_that_does_not_match_the_incident_is_ignored(changes):
    eng, deployments, _ = _started(BAD_DEPLOY, "payments")
    inc = eng.active["payments"]
    assert eng.resolve(_answer(inc, **changes), 17.0) is False
    [end] = _tick_through(eng, deployments, range(17, 40))
    assert end.now == inc.degradation_end == 35.0 and inc.resolved_by is None


def test_only_the_first_correct_decision_resolves_it():
    eng, deployments, _ = _started(UPSTREAM_DROP, "payments")
    inc = eng.active["payments"]
    assert eng.resolve(_answer(inc, window_end=10.0), 17.0) is True
    # a second correct answer read on the same tick, and another after the incident ended
    assert eng.resolve(_answer(inc, window_end=20.0, agent_name="someone_else"), 17.0) is False
    assert _tick_through(eng, deployments, [17])[0].now == 17.0
    assert eng.resolve(_answer(inc, window_end=20.0), 18.0) is False
    assert inc.degradation_end == 17.0 and inc.resolved_by == "capacity_agent INVESTIGATE_UPSTREAM"


def test_a_correct_decision_after_the_incident_expired_is_ignored():
    eng, deployments, _ = _started(UPSTREAM_DROP, "payments")
    inc = eng.active["payments"]
    [end] = _tick_through(eng, deployments, range(1, 36))
    assert end.now == 35.0
    assert eng.resolve(_answer(inc, window_end=40.0), 41.0) is False
    assert inc.degradation_end == 35.0 and inc.resolved_by is None


def test_the_schedule_is_the_same_whether_incidents_are_resolved_or_not():
    # D-065: the gap and the rests count from the scheduled end, so agents never move the
    # schedule, and the live one still follows --seed.
    def starts(resolve_after):
        eng = Engine(rng=random.Random(3), start=0.0, config=Config())
        deployments = _deployments()
        out = []
        for k in range(3 * 3600):
            for inc in list(eng.active.values()):
                if resolve_after is not None and k == inc.degradation_start + resolve_after:
                    assert eng.resolve(_answer(inc, window_end=inc.degradation_start + 10), float(k))
            started = _incidents(eng.tick(float(k), deployments))
            out += [(i.degradation_start, i.service_name, i.type) for i in started]
        return out

    never = starts(None)
    assert len(never) > 100  # about 37 an hour (D-067)
    assert starts(15) == never


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


def _past_first_three(eng):
    eng.seen.update(INCIDENT_TYPES)
    return eng


def test_choose_incident_skips_a_type_still_resting():
    eng = _past_first_three(Engine(rng=random.Random(0), start=0.0))
    now = 1000.0
    # checkout is the only surge candidate; its surge ended 100 s ago (rest 300 s).
    eng.last_incident_end["checkout"] = eng.last_type_end[("checkout", TRAFFIC_SURGE)] = now - 100
    picks = [eng._choose_incident(now) for _ in range(500)]
    assert all(p is not None for p in picks)
    assert all(p[0] != TRAFFIC_SURGE for p in picks)
    assert ("checkout", BAD_DEPLOY) in {(svc, t) for t, svc in picks}  # its other type is allowed


def test_choose_incident_skips_a_service_mid_incident_or_in_its_gap():
    eng = _past_first_three(Engine(rng=random.Random(0), start=0.0))
    now = 1000.0
    eng.active["checkout"] = _incident(TRAFFIC_SURGE, "checkout", start=now - 10)
    eng.last_incident_end["payments"] = now - 19  # within the 20 s gap
    assert eng._choose_incident(now) is None  # and web-gateway is never a candidate
    picks = {eng._choose_incident(now + 1) for _ in range(500)}
    assert {svc for _, svc in picks} == {"payments"}  # payments' gap is over


def test_choose_incident_draws_the_type_by_weight():
    # Past the first three, with every type allowed, the draw follows 50/25/25.
    eng = _past_first_three(Engine(rng=random.Random(0), start=0.0))
    picks = [eng._choose_incident(1000.0) for _ in range(8000)]
    for itype, weight in INCIDENT_WEIGHTS.items():
        share = sum(t == itype for t, _ in picks) / len(picks)
        assert share == pytest.approx(weight, abs=0.02), (itype, share)
        assert {svc for t, svc in picks if t == itype} == set(CANDIDATES[itype])


def test_choose_incident_only_unseen_types_until_all_three_have_happened():
    eng = Engine(rng=random.Random(0), start=0.0)
    eng.seen.add(BAD_DEPLOY)
    picks = {eng._choose_incident(1000.0) for _ in range(500)}
    assert {t for t, _ in picks} == {TRAFFIC_SURGE, UPSTREAM_DROP}


ONE_ANSWER = {BAD_DEPLOY: "ROLLBACK", TRAFFIC_SURGE: "SCALE_OUT", UPSTREAM_DROP: "INVESTIGATE_UPSTREAM"}


@pytest.mark.parametrize(("itype", "svc"), [(t, s) for t in INCIDENT_TYPES for s in CANDIDATES[t]])
def test_every_incident_has_exactly_one_expected_action(itype, svc):
    # D-043: the action depends on the type only, whatever the service.
    eng = Engine(rng=random.Random(0), start=0.0, config=Config(quiet_period=0))
    _force(eng, itype, svc)
    [inc] = _incidents(eng.tick(0.0, _deployments()))
    assert (inc.type, inc.service_name, inc.expected_action) == (itype, svc, ONE_ANSWER[itype])


def test_incident_candidates_match_spec():
    # D-056, D-067: three services. The shared scheduler serves checkout and payments only
    # (web-gateway deploys on its own clock): bad deploys on both, surges on checkout, drops on
    # payments. A surge and a drop never share a traffic series.
    assert set(service_names()) == {"web-gateway", "checkout", "payments"}
    assert SHARED_SERVICES == ("checkout", "payments")
    assert CANDIDATES == {BAD_DEPLOY: SHARED_SERVICES, TRAFFIC_SURGE: ("checkout",), UPSTREAM_DROP: ("payments",)}
    assert not set(CANDIDATES[TRAFFIC_SURGE]) & set(CANDIDATES[UPSTREAM_DROP])


def test_incident_numbering_continues_after_a_restart(tmp_path):
    # D-046: the counter lives in the generator state file, next to the versions.
    from opsgen.simulation import Simulation
    from opsgen.state import load_state, save_state, state_path

    path = state_path(tmp_path)
    assert load_state(path) is None
    config = Config(quiet_period=0)
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
    # D-067: and the highest version each service has run, so a deploy never reuses a number
    assert {n: d.highest_version for n, d in second.deployments.items()} == {
        n: d.highest_version for n, d in first.deployments.items()
    }
    ids = []
    for now in range(1000, 1300):
        _, _, events = second.tick(float(now))
        ids += [ev.incident.incident_id for ev in events if ev.kind == "incident_start"]
    assert ids and ids[0] == f"INC{len(started) + 1:04d}"


def test_a_state_file_without_highest_version_still_never_reuses_one():
    # A state file saved before D-067 has no highest_version: it is the higher of the saved
    # version and previous_version (after a rollback, previous_version is the bad one).
    from opsgen.simulation import Simulation

    saved = {
        "services": {"checkout": {"version": "4.2.2", "previous_version": "4.2.3", "last_change": "ROLLBACK"}},
        "incident_counter": 3,
    }
    sim = Simulation(1, start=0.0, initial_state=saved)
    assert sim.deployments["checkout"].highest_version == "4.2.3"
    assert sim.deployments["checkout"].deploy(10.0)[1] == "4.2.4"
