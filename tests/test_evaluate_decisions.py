"""evaluate's decision scoring: validity, routing, coverage, per-incident matching."""

from scripts.evaluation.decisions import (
    CAPACITY_AGENT,
    INVALID,
    INVESTIGATE_UPSTREAM,
    OBSERVE,
    RELEASE_AGENT,
    ROLLBACK,
    SCALE_OUT,
    DecisionRow,
    expected_agent,
    score_decisions,
)
from scripts.evaluation.detection import HealthRow, Incident

T0 = 1_800_000_000.0  # a 10-second boundary


def _row(service, k, lat="NORMAL", trf="NORMAL", version="1.0.0", ssd=600):
    return HealthRow(
        service_name=service, window_start=T0 + 10 * k, window_end=T0 + 10 * k + 10,
        latency_status=lat, traffic_status=trf, avg_latency_ms=100.0, expected_latency_ms=50.0,
        latency_upper_bound=70.0, request_count=30, expected_request_count=10.0,
        traffic_lower_bound=5.0, traffic_upper_bound=15.0, version=version, seconds_since_deploy=ssd,
        last_change="DEPLOY", ready_replicas=3,
    )  # fmt: skip


def _series(service, n, overrides=None):
    overrides = overrides or {}
    return [_row(service, k, **overrides.get(k, {})) for k in range(n)]


def _dec(service, k, agent=RELEASE_AGENT, trigger="LATENCY_HIGH", action=ROLLBACK, **kw):
    return DecisionRow(
        window_end=T0 + 10 * k + 10, service_name=service, agent_name=agent,
        trigger=trigger, action=action, **kw,
    )  # fmt: skip


def _inc(
    service, start_k, start_offset=3, duration=50,
    trigger="LATENCY_HIGH", typ="BAD_DEPLOY", action=ROLLBACK, **kw,
):  # fmt: skip
    start = T0 + 10 * start_k + start_offset
    return Incident(
        incident_id="i1", type=typ, service_name=service, started_at=start, ended_at=start + duration,
        expected_trigger=trigger, expected_action=action, **kw,
    )  # fmt: skip


def _score(decisions, rows, incidents, frm=T0, to=None):
    return score_decisions(decisions, rows, incidents, frm, to if to is not None else T0 + 10_000)


# --- validity and routing ----------------------------------------------------------


def test_an_action_outside_the_agents_set_is_invalid():
    ok = _dec("cart", 10, action=ROLLBACK)
    # SCALE_OUT is a real action, but not one the release agent may take.
    wrong = _dec("cart", 11, action=SCALE_OUT)
    failed = _dec("cart", 12, action=INVALID)
    rep = _score([ok, wrong, failed], _series("cart", 20), [])
    assert rep.agents[RELEASE_AGENT].valid == 1
    assert rep.valid_share == 1 / 3


def test_a_trigger_belonging_to_the_other_agent_is_a_routing_error():
    d = _dec("cart", 10, agent=RELEASE_AGENT, trigger="TRAFFIC_LOW", action=OBSERVE)
    rep = _score([d], _series("cart", 20), [])
    assert rep.agents[RELEASE_AGENT].routing_errors == 1


def test_one_window_under_both_agents_breaks_mutual_exclusivity():
    rows = _series("cart", 20, {10: {"lat": "HIGH", "trf": "HIGH"}})
    both = [
        _dec("cart", 10, agent=RELEASE_AGENT, action=ROLLBACK),
        _dec("cart", 10, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=SCALE_OUT),
    ]
    assert _score(both, rows, []).both_agents == [("cart", T0 + 110)]
    assert _score(both[:1], rows, []).both_agents == []


def test_a_repeated_decision_for_one_window_is_a_duplicate():
    rows = _series("cart", 20, {10: {"lat": "HIGH"}})
    rep = _score([_dec("cart", 10), _dec("cart", 10)], rows, [])
    assert len(rep.coverage.duplicates) == 1


# --- coverage ----------------------------------------------------------------------


def test_expected_agent_follows_the_routing_rules():
    assert expected_agent(_row("cart", 1, lat="HIGH")) == RELEASE_AGENT
    assert expected_agent(_row("cart", 1, trf="LOW")) == CAPACITY_AGENT
    # Latency and traffic both anomalous is a load problem: capacity only.
    assert expected_agent(_row("cart", 1, lat="HIGH", trf="HIGH")) == CAPACITY_AGENT
    assert expected_agent(_row("cart", 1)) is None
    assert expected_agent(_row("cart", 1, lat="WARMING_UP", trf="WARMING_UP")) is None


def test_calls_track_anomalous_rows_not_windows():
    rows = _series("cart", 100, {10: {"lat": "HIGH"}, 20: {"trf": "LOW"}})
    decisions = [
        _dec("cart", 10),
        _dec("cart", 20, agent=CAPACITY_AGENT, trigger="TRAFFIC_LOW", action=INVESTIGATE_UPSTREAM),
    ]
    cov = _score(decisions, rows, []).coverage
    assert (cov.expected, cov.decided) == (2, 2)
    assert cov.total_rows == 90  # counted from the first decision, at window 10
    assert cov.missing == [] and cov.unexpected == []


def test_an_anomalous_row_without_a_decision_is_missing_unless_it_is_recent():
    rows = _series("cart", 100, {5: {"lat": "HIGH"}, 10: {"lat": "HIGH"}, 98: {"lat": "HIGH"}})
    cov = _score([_dec("cart", 5)], rows, [], to=T0 + 1000).coverage
    assert cov.missing == [("cart", T0 + 110, RELEASE_AGENT)]
    assert cov.in_flight == 1  # window 98 closes inside the in-flight tail


def test_anomalies_from_before_the_agents_started_are_not_missing():
    """The INSERTs read service_health from the latest offset, so windows older than the
    first decision were never offered to an agent."""
    rows = _series("cart", 100, {10: {"lat": "HIGH"}, 60: {"lat": "HIGH"}})
    cov = _score([_dec("cart", 60)], rows, []).coverage
    assert cov.counted_from == T0 + 610
    assert (cov.expected, cov.decided) == (1, 1)
    assert cov.missing == []


def test_coverage_is_empty_when_no_agent_ever_answered():
    rows = _series("cart", 100, {10: {"lat": "HIGH"}})
    cov = _score([], rows, []).coverage
    assert cov.counted_from is None and cov.expected == 0 and cov.missing == []


def test_a_decision_on_a_normal_row_is_unexpected():
    rows = _series("cart", 20, {10: {"lat": "HIGH"}})
    rep = _score([_dec("cart", 10), _dec("cart", 15)], rows, [])
    assert [d.window_end for d in rep.coverage.unexpected] == [T0 + 160]


# --- per incident ------------------------------------------------------------------


def test_the_headline_decision_is_the_one_on_the_first_window_of_the_span():
    rows = _series("cart", 60, {40: {"lat": "HIGH"}, 41: {"lat": "HIGH"}})
    inc = _inc("cart", 40, duration=50, good_version="1.0.0", bad_version="1.0.1")
    decisions = [
        _dec("cart", 41, action=OBSERVE),  # later window, listed first
        _dec("cart", 40, action=ROLLBACK, target="1.0.0"),
    ]
    item = _score(decisions, rows, [inc]).incidents[0]
    assert item.headline.action == ROLLBACK
    assert item.matched and item.target_ok is True
    assert item.actions[OBSERVE] == 1


def test_a_rollback_naming_the_wrong_version_fails_the_target_check():
    rows = _series("cart", 60, {40: {"lat": "HIGH"}})
    inc = _inc("cart", 40, good_version="1.0.0", bad_version="1.0.1")
    item = _score([_dec("cart", 40, target="9.9.9")], rows, [inc]).incidents[0]
    assert item.matched and item.target_ok is False


def test_a_scale_out_must_carry_the_replica_count_the_sql_computed():
    rows = _series("cart", 60, {40: {"trf": "HIGH"}})
    inc = _inc("cart", 40, trigger="TRAFFIC_HIGH", typ="TRAFFIC_SURGE", action=SCALE_OUT)
    good = _dec("cart", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=SCALE_OUT, target="9")
    bad = _dec("cart", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=SCALE_OUT, target="4")
    assert _score([good], rows, [inc]).incidents[0].target_ok is True
    assert _score([bad], rows, [inc]).incidents[0].target_ok is False


def test_an_action_that_needs_no_target_is_not_target_checked():
    rows = _series("cart", 60, {40: {"trf": "LOW"}})
    inc = _inc("cart", 40, trigger="TRAFFIC_LOW", typ="UPSTREAM_DROP", action=INVESTIGATE_UPSTREAM)
    d = _dec("cart", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_LOW", action=INVESTIGATE_UPSTREAM)
    item = _score([d], rows, [inc]).incidents[0]
    assert item.matched and item.target_ok is None


def test_a_wrong_headline_still_reports_a_later_matching_window():
    rows = _series("cart", 60, {40: {"lat": "HIGH"}, 41: {"lat": "HIGH"}})
    inc = _inc("cart", 40, duration=50)
    decisions = [_dec("cart", 40, action=OBSERVE), _dec("cart", 41, action=ROLLBACK)]
    item = _score(decisions, rows, [inc]).incidents[0]
    assert not item.matched and item.any_matched


def test_decisions_outside_every_span_are_listed_separately():
    rows = _series("cart", 60, {10: {"lat": "HIGH"}, 40: {"lat": "HIGH"}})
    inc = _inc("cart", 40, duration=50)
    rep = _score([_dec("cart", 10, action=OBSERVE), _dec("cart", 40)], rows, [inc])
    assert len(rep.incidents[0].decisions) == 1
    assert [d.action for d in rep.outside] == [OBSERVE]


def test_an_incident_on_another_service_claims_nothing():
    rows = _series("cart", 60, {40: {"lat": "HIGH"}})
    inc = _inc("payments", 40, duration=50)
    rep = _score([_dec("cart", 40)], rows, [inc])
    assert rep.incidents[0].decisions == [] and len(rep.outside) == 1


def test_only_decisions_inside_the_scored_range_count():
    rows = _series("cart", 60, {10: {"lat": "HIGH"}})
    rep = _score([_dec("cart", 10)], rows, [], frm=T0 + 200, to=T0 + 400)
    assert rep.total_decisions == 0


def test_from_record_parses_a_remediation_decisions_row():
    d = DecisionRow.from_record(
        {
            "window_end": "2027-01-15T12:00:10Z",
            "service_name": "checkout",
            "agent_name": RELEASE_AGENT,
            "trigger": "LATENCY_HIGH",
            "action": " rollback ",
            "target": " 4.2.1 ",
            "reason": "The deploy is the prime suspect.",
            "raw_response": "Action: ROLLBACK\nTarget: 4.2.1\nReason: ...",
        }
    )
    assert d.action == ROLLBACK and d.valid and d.routed_correctly
    assert d.target == "4.2.1"
    assert d.key == ("checkout", d.window_end)


def test_a_target_on_an_action_that_needs_none_is_flagged():
    """D-034: the SQL stores whatever the agent wrote, so a contradiction is visible."""
    rows = _series("cart", 60, {40: {"lat": "HIGH"}})
    inc = _inc("cart", 40, good_version="1.0.0")
    rep = _score([_dec("cart", 40, action=OBSERVE, target="1.0.0")], rows, [inc])
    assert len(rep.bad_targets) == 1
    assert rep.incidents[0].target_ok is False


def test_context_comes_from_the_matching_health_row():
    rows = _series("cart", 60, {40: {"lat": "HIGH"}})
    rep = _score([_dec("cart", 40)], rows, [])
    assert "100ms vs 50ms expected" in rep.context(rep.outside[0])


def test_a_decision_without_its_health_row_says_so():
    rep = _score([_dec("cart", 40)], [], [])
    assert "no matching service_health row" in rep.context(rep.outside[0])
