"""evaluate's decision scoring: validity, routing, coverage, per-incident matching."""

from scripts.evaluation.decisions import (
    ALLOWED_ACTIONS,
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


def _row(service, k, lat="NORMAL", trf="NORMAL", version="1.0.1", prev="1.0.0", ssd=600):
    return HealthRow(
        service_name=service, window_start=T0 + 10 * k, window_end=T0 + 10 * k + 10,
        latency_status=lat, traffic_status=trf, avg_latency_ms=100.0, expected_latency_ms=50.0,
        latency_upper_bound=70.0, traffic_count=30, expected_traffic_count=10.0,
        traffic_lower_bound=5.0, traffic_upper_bound=15.0, version=version, seconds_since_deploy=ssd,
        last_change="DEPLOY", previous_version=prev, ready_replicas=3,
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
    ok = _dec("checkout", 10, action=ROLLBACK)
    # SCALE_OUT is a real action, but not one the release agent may take.
    wrong = _dec("checkout", 11, action=SCALE_OUT)
    failed = _dec("checkout", 12, action=INVALID)
    rep = _score([ok, wrong, failed], _series("checkout", 20), [])
    assert rep.agents[RELEASE_AGENT].valid == 1
    assert rep.valid_share == 1 / 3


def test_the_capacity_agent_may_investigate_upstream_or_observe_and_nothing_else():
    # D-068: SCALE_OUT left the capacity agent with the surges.
    assert ALLOWED_ACTIONS[CAPACITY_AGENT] == (INVESTIGATE_UPSTREAM, OBSERVE)
    rows = _series("payments", 20, {10: {"trf": "LOW"}, 11: {"trf": "HIGH"}, 12: {"trf": "HIGH"}})
    decisions = [
        _dec("payments", 10, agent=CAPACITY_AGENT, trigger="TRAFFIC_LOW", action=INVESTIGATE_UPSTREAM),
        _dec("payments", 11, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=OBSERVE),
        _dec("payments", 12, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=SCALE_OUT),
    ]
    rep = _score(decisions, rows, [])
    assert (rep.agents[CAPACITY_AGENT].decisions, rep.agents[CAPACITY_AGENT].valid) == (3, 2)


def test_a_trigger_belonging_to_the_other_agent_is_a_routing_error():
    d = _dec("checkout", 10, agent=RELEASE_AGENT, trigger="TRAFFIC_LOW", action=OBSERVE)
    rep = _score([d], _series("checkout", 20), [])
    assert rep.agents[RELEASE_AGENT].routing_errors == 1


def test_one_window_under_both_agents_breaks_mutual_exclusivity():
    rows = _series("checkout", 20, {10: {"lat": "HIGH", "trf": "HIGH"}})
    both = [
        _dec("checkout", 10, agent=RELEASE_AGENT, action=ROLLBACK),
        _dec("checkout", 10, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=OBSERVE),
    ]
    assert _score(both, rows, []).both_agents == [("checkout", T0 + 110)]
    assert _score(both[:1], rows, []).both_agents == []


def test_a_repeated_decision_for_one_window_is_a_duplicate():
    rows = _series("checkout", 20, {10: {"lat": "HIGH"}})
    rep = _score([_dec("checkout", 10), _dec("checkout", 10)], rows, [])
    assert len(rep.coverage.duplicates) == 1


# --- coverage ----------------------------------------------------------------------


def test_expected_agent_follows_the_routing_rules():
    assert expected_agent(_row("checkout", 1, lat="HIGH")) == RELEASE_AGENT
    assert expected_agent(_row("checkout", 1, trf="LOW")) == CAPACITY_AGENT
    # Latency and traffic both anomalous is a load problem: capacity only.
    assert expected_agent(_row("checkout", 1, lat="HIGH", trf="HIGH")) == CAPACITY_AGENT
    assert expected_agent(_row("checkout", 1)) is None
    assert expected_agent(_row("checkout", 1, lat="WARMING_UP", trf="WARMING_UP")) is None


def test_calls_track_anomalous_rows_not_windows():
    rows = _series("checkout", 100, {10: {"lat": "HIGH"}, 20: {"trf": "LOW"}})
    decisions = [
        _dec("checkout", 10),
        _dec("checkout", 20, agent=CAPACITY_AGENT, trigger="TRAFFIC_LOW", action=INVESTIGATE_UPSTREAM),
    ]
    cov = _score(decisions, rows, []).coverage
    assert (cov.expected, cov.decided) == (2, 2)
    assert cov.total_rows == 90  # counted from the first decision, at window 10
    assert cov.missing == [] and cov.unexpected == []


def test_an_anomalous_row_without_a_decision_is_missing_unless_it_is_recent():
    rows = _series("checkout", 100, {5: {"lat": "HIGH"}, 10: {"lat": "HIGH"}, 98: {"lat": "HIGH"}})
    cov = _score([_dec("checkout", 5)], rows, [], to=T0 + 1000).coverage
    assert cov.missing == [("checkout", T0 + 110, RELEASE_AGENT)]
    assert cov.in_flight == 1  # window 98 closes inside the in-flight tail


def test_anomalies_from_before_the_agents_started_are_not_missing():
    """The INSERTs read service_health from the latest offset, so windows older than the
    first decision were never offered to an agent."""
    rows = _series("checkout", 100, {10: {"lat": "HIGH"}, 60: {"lat": "HIGH"}})
    cov = _score([_dec("checkout", 60)], rows, []).coverage
    assert cov.counted_from == T0 + 610
    assert (cov.expected, cov.decided) == (1, 1)
    assert cov.missing == []


def test_coverage_is_empty_when_no_agent_ever_answered():
    rows = _series("checkout", 100, {10: {"lat": "HIGH"}})
    cov = _score([], rows, []).coverage
    assert cov.counted_from is None and cov.expected == 0 and cov.missing == []


def test_a_decision_on_a_normal_row_is_unexpected():
    rows = _series("checkout", 20, {10: {"lat": "HIGH"}})
    rep = _score([_dec("checkout", 10), _dec("checkout", 15)], rows, [])
    assert [d.window_end for d in rep.coverage.unexpected] == [T0 + 160]


# --- per incident ------------------------------------------------------------------


def test_the_headline_decision_is_the_one_on_the_first_window_of_the_span():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}, 41: {"lat": "HIGH"}})
    inc = _inc("checkout", 40, duration=50)
    decisions = [
        _dec("checkout", 41, action=OBSERVE),  # later window, listed first
        _dec("checkout", 40, action=ROLLBACK, target="1.0.0"),
    ]
    item = _score(decisions, rows, [inc]).incidents[0]
    assert item.headline.action == ROLLBACK
    assert item.matched and item.target_ok is True
    assert item.actions[OBSERVE] == 1


def test_a_rollback_naming_the_wrong_version_fails_the_target_check():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    inc = _inc("checkout", 40)
    item = _score([_dec("checkout", 40, target="9.9.9")], rows, [inc]).incidents[0]
    assert item.matched and item.target_ok is False


def test_a_rollback_target_is_judged_against_the_health_row_it_answered():
    """D-045: the right target is the Previous Version the agent was given, read from the
    service_health row with the decision's (service_name, window_end); no ground truth."""
    rows = _series("checkout", 60, {40: {"lat": "HIGH", "version": "2.0.5", "prev": "2.0.4"}})
    right = _score([_dec("checkout", 40, target="2.0.4")], rows, [])
    assert right.bad_targets == []
    wrong = _score([_dec("checkout", 40, target="1.0.0")], rows, [])
    assert len(wrong.bad_targets) == 1
    # no health row (or no previous version): nothing to check, unless the target is missing
    assert _score([_dec("checkout", 99, target="2.0.4")], rows, []).bad_targets == []
    assert len(_score([_dec("checkout", 99)], rows, []).bad_targets) == 1
    no_prev = _series("checkout", 60, {40: {"lat": "HIGH", "prev": None}})
    assert _score([_dec("checkout", 40, target="2.0.4")], no_prev, []).bad_targets == []


def test_only_a_rollback_may_carry_a_target():
    # D-068: no replica count any more. Before, a SCALE_OUT carried the count the capacity
    # INSERT computed (9 for 3 replicas at three times the expected load).
    rows = _series("payments", 60, {40: {"trf": "HIGH"}})
    for action in (SCALE_OUT, OBSERVE):
        with_count = _dec("payments", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=action, target="9")
        assert _score([with_count], rows, []).bad_targets == [with_count], action
        without = _dec("payments", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_HIGH", action=action)
        assert _score([without], rows, []).bad_targets == [], action


def test_an_action_that_needs_no_target_is_not_target_checked():
    rows = _series("checkout", 60, {40: {"trf": "LOW"}})
    inc = _inc("checkout", 40, trigger="TRAFFIC_LOW", typ="UPSTREAM_DROP", action=INVESTIGATE_UPSTREAM)
    d = _dec("checkout", 40, agent=CAPACITY_AGENT, trigger="TRAFFIC_LOW", action=INVESTIGATE_UPSTREAM)
    item = _score([d], rows, [inc]).incidents[0]
    assert item.matched and item.target_ok is None


def test_a_wrong_headline_still_reports_a_later_matching_window():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}, 41: {"lat": "HIGH"}})
    inc = _inc("checkout", 40, duration=50)
    decisions = [_dec("checkout", 40, action=OBSERVE), _dec("checkout", 41, action=ROLLBACK)]
    item = _score(decisions, rows, [inc]).incidents[0]
    assert not item.matched and item.any_matched


def test_decisions_outside_every_span_are_listed_separately():
    rows = _series("checkout", 60, {10: {"lat": "HIGH"}, 40: {"lat": "HIGH"}})
    inc = _inc("checkout", 40, duration=50)
    rep = _score([_dec("checkout", 10, action=OBSERVE), _dec("checkout", 40)], rows, [inc])
    assert len(rep.incidents[0].decisions) == 1
    assert [d.action for d in rep.outside] == [OBSERVE]


def test_an_incident_on_another_service_claims_nothing():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    inc = _inc("payments", 40, duration=50)
    rep = _score([_dec("checkout", 40)], rows, [inc])
    assert rep.incidents[0].decisions == [] and len(rep.outside) == 1


def test_only_decisions_inside_the_scored_range_count():
    rows = _series("checkout", 60, {10: {"lat": "HIGH"}})
    rep = _score([_dec("checkout", 10)], rows, [], frm=T0 + 200, to=T0 + 400)
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
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    inc = _inc("checkout", 40)
    rep = _score([_dec("checkout", 40, action=OBSERVE, target="1.0.0")], rows, [inc])
    assert len(rep.bad_targets) == 1
    assert rep.incidents[0].target_ok is False


def test_context_comes_from_the_matching_health_row():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    rep = _score([_dec("checkout", 40)], rows, [])
    assert "100ms vs 50ms expected" in rep.context(rep.outside[0])


def test_a_decision_without_its_health_row_says_so():
    rep = _score([_dec("checkout", 40)], [], [])
    assert "no matching service_health or service_error_rate row" in rep.context(rep.outside[0])


# --- the decisions table always exists now (D-050) ----------------------------------


def _main_with_tables(monkeypatch, capsys, decisions):
    """Run `evaluate` against fake Kafka reads: real health rows, no incidents."""
    import json
    from pathlib import Path

    from scripts import evaluate

    health = [
        json.loads(line)
        for line in (Path(__file__).parent / "fixtures/opswall/service_health.jsonl").read_text().splitlines()
        if line.strip()
    ]
    tables = {
        evaluate.SERVICE_HEALTH: health,
        evaluate.SERVICE_INCIDENTS: [],
        evaluate.REMEDIATION_DECISIONS: decisions,
    }
    # a table not listed doesn't exist, as `_read_table` reports it (service_error_rate here)
    monkeypatch.setattr(evaluate, "_read_table", lambda topic, creds, root: tables.get(topic))
    evaluate.main([])
    return " ".join(capsys.readouterr().out.split())  # rich wraps long lines


def test_an_empty_decisions_topic_skips_the_decision_half(monkeypatch, capsys):
    out = _main_with_tables(monkeypatch, capsys, decisions=[])
    assert "decision half is skipped" in out
    assert "Run the agent INSERTs" in out
    assert "Create it with" not in out  # the table is the platform team's, nobody creates it in a lab


def test_a_missing_decisions_topic_skips_the_decision_half_too(monkeypatch, capsys):
    out = _main_with_tables(monkeypatch, capsys, decisions=None)
    assert "decision half is skipped" in out


# --- the release agent's error-rate leg (D-067) ---------------------------------------


def _err(service, k, status="NORMAL", prev="3.4.3"):
    errors = 200 if status == "HIGH" else 2
    return HealthRow(
        service_name=service, window_start=T0 + 10 * k, window_end=T0 + 10 * k + 10,
        latency_status=None, traffic_status=None, traffic_count=500, error_count=errors,
        error_rate=errors / 500, error_status=status, version="3.4.4", previous_version=prev,
        last_change="DEPLOY", seconds_since_deploy=10,
    )  # fmt: skip


def test_an_error_rate_decision_belongs_to_the_release_agent():
    assert _dec("web-gateway", 40, trigger="ERROR_RATE_HIGH").routed_correctly
    assert not _dec("web-gateway", 40, agent=CAPACITY_AGENT, trigger="ERROR_RATE_HIGH").routed_correctly


def test_a_high_error_rate_row_expects_one_release_agent_call():
    assert expected_agent(_err("web-gateway", 40, "HIGH")) == RELEASE_AGENT
    assert expected_agent(_err("web-gateway", 41)) is None


def test_an_error_rate_rollback_is_scored_against_the_rows_previous_version():
    rows = [_err("web-gateway", k, "HIGH" if k == 40 else "NORMAL") for k in range(60)]
    inc = _inc("web-gateway", 40, 0, 11, trigger="ERROR_RATE_HIGH", typ="BAD_DEPLOY_ERRORS")
    right = _score([_dec("web-gateway", 40, trigger="ERROR_RATE_HIGH", target="3.4.3")], rows, [inc])
    [item] = right.incidents
    assert item.matched and item.target_ok is True
    assert "error rate 40.0 %" in right.context(item.headline)
    wrong = _score([_dec("web-gateway", 40, trigger="ERROR_RATE_HIGH", target="3.4.4")], rows, [inc])
    assert wrong.incidents[0].target_ok is False
    assert right.coverage.expected == right.coverage.decided == 1


def _main_with_error_rate(monkeypatch, capsys, health, argv=()):
    """Run `evaluate` against fake Kafka reads: lab 2.2's rows for a web-gateway bad deploy
    and its ground truth, with `health` as service_health (None: the table is missing)."""
    from scripts import evaluate
    from scripts.common.topic_reader import iso

    def rec(k, status):
        return {"service_name": "web-gateway", "window_start": iso(T0 + 10 * k), "window_end": iso(T0 + 10 * k + 10),
                "traffic_count": 500, "error_count": 200 if status == "HIGH" else 2,
                "error_rate": 0.4 if status == "HIGH" else 0.004, "error_status": status, "version": "3.4.4",
                "previous_version": "3.4.3", "last_change": "DEPLOY", "seconds_since_deploy": 10}  # fmt: skip

    start = T0 + 400
    incident = {"incident_id": "INC0001", "incident_type": "BAD_DEPLOY_ERRORS", "service_name": "web-gateway",
                "started_at": iso(start), "ended_at": iso(start + 11), "event_time": iso(start + 11),
                "expected_trigger": "ERROR_RATE_HIGH", "expected_action": "ROLLBACK"}  # fmt: skip
    tables = {
        evaluate.SERVICE_ERROR_RATE: [rec(k, "HIGH" if k == 40 else "NORMAL") for k in range(80)],
        evaluate.SERVICE_INCIDENTS: [incident],
        evaluate.REMEDIATION_DECISIONS: [],
    }
    if health is not None:
        tables[evaluate.SERVICE_HEALTH] = health
    monkeypatch.setattr(evaluate, "_read_table", lambda topic, creds, root: tables.get(topic))
    evaluate.main(["--minutes", "11", "--to", iso(T0 + 700), *argv])
    return " ".join(capsys.readouterr().out.split())


def test_evaluate_scores_the_error_rate_table_without_service_health(monkeypatch, capsys):
    out = _main_with_error_rate(monkeypatch, capsys, health=None)
    assert "`service_health` doesn't exist yet" in out and "sql/2.5_service_health.sql" in out
    assert "BAD_DEPLOY_ERRORS: 1/1 detected within 3 windows" in out


def test_evaluate_dumps_and_rereads_the_error_rate_rows(monkeypatch, capsys, tmp_path):
    dump = tmp_path / "error_rate.jsonl"
    _main_with_error_rate(monkeypatch, capsys, health=None, argv=["--dump-error-rate", str(dump)])
    assert len(dump.read_text().splitlines()) == 80


# --- ticket decisions (D-068) ------------------------------------------------------------


def _ticket_decision(k):
    return _dec("checkout", k, agent="ticket_responder", trigger="HIGH_SEVERITY_TICKET", action=SCALE_OUT,
                ticket_id=f"ZD-{48215 + k}")  # fmt: skip


def test_a_ticket_decision_is_never_an_unexpected_metric_decision_and_never_moves_counted_from():
    rows = _series("payments", 100, {60: {"lat": "HIGH"}})
    rep = _score([_ticket_decision(5), _dec("payments", 60), _ticket_decision(80)], rows, [])
    assert rep.total_decisions == 1
    assert rep.coverage.counted_from == T0 + 610
    assert rep.coverage.unexpected == []
    assert [d.service_name for d in rep.outside] == ["payments"] and set(rep.agents) == {RELEASE_AGENT}


def test_a_ticket_decision_alone_leaves_the_metric_half_empty():
    rep = _score([_ticket_decision(5)], _series("payments", 20, {5: {"lat": "HIGH"}}), [])
    assert rep.total_decisions == 0 and rep.coverage.counted_from is None


def test_from_record_reads_a_ticket_decision_and_its_kafka_timestamp():
    d = DecisionRow.from_record({
        "window_end": "2027-01-15T12:00:03.000Z", "service_name": "checkout", "agent_name": "ticket_responder",
        "trigger": "HIGH_SEVERITY_TICKET", "action": "SCALE_OUT", "target": None, "reason": "Sale traffic.",
        "raw_response": "Action: SCALE_OUT\nReason: Sale traffic.", "ticket_id": "ZD-48215",
        "kafka_timestamp": "2027-01-15T12:00:12.500Z",
    })  # fmt: skip
    assert d.ticket_id == "ZD-48215" and d.valid and d.routed_correctly
    assert d.written_at - d.window_end == 9.5
    metric = DecisionRow.from_record({
        "window_end": "2027-01-15T12:00:10Z", "service_name": "payments", "agent_name": RELEASE_AGENT,
        "trigger": "LATENCY_HIGH", "action": "OBSERVE",
    })  # fmt: skip
    assert metric.ticket_id is None and metric.written_at is None
