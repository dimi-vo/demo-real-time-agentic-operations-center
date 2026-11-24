"""evaluate's ticket scoring (D-068): the classifier's verdicts against the answer key, the
responder's actions, filter leaks and the two hops' times."""

import pytest

from scripts.common.topic_reader import iso
from scripts.evaluation.decisions import (
    INVALID,
    INVESTIGATE_UPSTREAM,
    ROLLBACK,
    SCALE_OUT,
    DecisionRow,
)
from scripts.evaluation.tickets import (
    AnswerKey,
    Ticket,
    Triage,
    score_tickets,
)

T0 = 1_800_000_000.0
HIGH, LOW = "HIGH", "LOW"


def _id(n: int) -> str:
    return f"ZD-{48215 + n}"


def _ticket(n: int, priority: str = "P1") -> Ticket:
    return Ticket(ticket_id=_id(n), event_time=T0 + 10 * n, priority=priority, subject=f"subject {n}")


def _truth(n: int, severity: str, action: str | None = None) -> AnswerKey:
    return AnswerKey(ticket_id=_id(n), true_severity=severity, expected_action=action)


def _triage(n: int, severity: str, after: float | None = 4.0, priority: str = "P1") -> Triage:
    at = None if after is None else T0 + 10 * n + after
    return Triage(ticket_id=_id(n), event_time=T0 + 10 * n, priority=priority, severity=severity,
                  reason=f"reason {n}", raw_response=f"Severity: {severity}", written_at=at)  # fmt: skip


def _answer(n: int, action: str, after: float | None = 10.0, **kw) -> DecisionRow:
    kw = {"agent_name": "ticket_responder", "trigger": "HIGH_SEVERITY_TICKET", **kw}
    return DecisionRow(window_end=T0 + 10 * n, service_name="checkout", action=action, ticket_id=_id(n),
                       written_at=None if after is None else T0 + 10 * n + after, **kw)  # fmt: skip


def _score(cases, extra_tickets=(), p1=(), triage=(), decisions=(), frm=T0, to=T0 + 10_000):
    """One P1 ticket per case, numbered from 0: (true severity, verdict or None for never
    classified[, expected action[, the responder's action]])."""
    tickets, keys, rows, answers = list(extra_tickets), [], list(triage), list(decisions)
    for n, case in enumerate(cases):
        truth, verdict, expected, action = (*case, None, None)[:4]
        tickets.append(_ticket(n))
        keys.append(_truth(n, truth, expected if truth == HIGH else None))
        if verdict is not None:
            rows.append(_triage(n, verdict))
        if action is not None:
            answers.append(_answer(n, action))
    p1_rows = [t for t in tickets if t.priority == "P1"] + list(p1)
    return score_tickets(tickets, keys, p1_rows, rows, answers, frm, to)


# --- classification --------------------------------------------------------------------


def test_the_two_by_two_counts_true_severity_against_the_verdict():
    rep = _score([(HIGH, HIGH), (HIGH, HIGH), (HIGH, LOW), (LOW, LOW), (LOW, LOW), (LOW, LOW), (LOW, HIGH)])
    assert rep.matrix[(HIGH, HIGH)] == 2
    assert rep.matrix[(HIGH, LOW)] == 1
    assert rep.matrix[(LOW, HIGH)] == 1
    assert rep.matrix[(LOW, LOW)] == 3
    assert (rep.classified_right, len(rep.results)) == (5, 7)


def test_invalid_and_never_classified_count_as_wrong():
    rep = _score([(HIGH, HIGH), (LOW, LOW), (HIGH, "INVALID"), (LOW, None)])
    assert rep.invalid == 1 and rep.never_classified == 1
    assert rep.matrix[(HIGH, "INVALID")] == 1 and rep.matrix[(LOW, "NEVER")] == 1
    assert rep.classifier_accuracy == 0.5


def test_a_verdict_written_more_than_30_s_after_the_span_ends_is_never_classified():
    to = T0 + 100  # tickets 0..9 are in the span
    late, on_time = _triage(9, LOW, after=41.0), _triage(8, LOW, after=50.0)  # 131 s and 130 s
    rep = _score([(LOW, None)] * 10, triage=[late, on_time], to=to)
    assert len(rep.results) == 10
    assert [r.ticket.ticket_id for r in rep.results if r.verdict == "NEVER"] == [_id(n) for n in range(8)] + [_id(9)]
    unstamped = _score([(LOW, None)], triage=[_triage(0, LOW, after=None)], to=T0 + 5)
    assert unstamped.never_classified == 0  # no Kafka timestamp to judge it by: it counts


# --- response ----------------------------------------------------------------------------


def test_response_accuracy_is_right_actions_over_answered_high_tickets():
    rep = _score([
        (HIGH, HIGH, ROLLBACK, ROLLBACK),
        (HIGH, HIGH, SCALE_OUT, SCALE_OUT),
        (HIGH, HIGH, INVESTIGATE_UPSTREAM, ROLLBACK),
        (HIGH, HIGH, SCALE_OUT, INVALID),
        (HIGH, HIGH, ROLLBACK),  # not answered (yet)
        (HIGH, LOW, ROLLBACK),  # the classifier missed it: nothing to answer
    ])  # fmt: skip
    assert len(rep.to_answer) == 5
    assert len(rep.answered) == 4
    assert rep.right_actions == 2 and rep.invalid_actions == 1
    assert rep.responder_accuracy == 0.5


def test_a_target_on_a_ticket_decision_is_an_error():
    with_target = _answer(0, ROLLBACK, target="2.9.4")
    rep = _score([(HIGH, HIGH, ROLLBACK), (HIGH, HIGH, SCALE_OUT, SCALE_OUT)], decisions=[with_target])
    assert rep.bad_targets == [with_target]
    assert rep.right_actions == 2  # the action itself is still the expected one


def test_an_answer_to_a_really_low_ticket_called_high_is_unexpected_not_scored():
    rep = _score([(LOW, HIGH, None, SCALE_OUT), (HIGH, HIGH, ROLLBACK, ROLLBACK)])
    assert [d.ticket_id for d in rep.unexpected] == [_id(0)]
    assert len(rep.answered) == 1 and rep.responder_accuracy == 1.0
    assert rep.routing_errors == []


def test_an_answer_to_a_ticket_with_no_high_verdict_is_a_routing_error():
    rep = _score([
        (HIGH, LOW, ROLLBACK, ROLLBACK),  # the 5.3 INSERT answered a LOW verdict
        (LOW, LOW, None, SCALE_OUT),
        (HIGH, None, SCALE_OUT, SCALE_OUT),  # never classified, yet answered
        (HIGH, HIGH, ROLLBACK, ROLLBACK),
    ])  # fmt: skip
    assert [d.ticket_id for d in rep.routing_errors] == [_id(0), _id(1), _id(2)]
    assert rep.unexpected == []
    assert len(rep.answered) == 1


def test_a_ticket_decision_from_another_agent_or_trigger_is_misrouted():
    assert _answer(0, ROLLBACK).routed_correctly and _answer(0, ROLLBACK).valid
    assert not _answer(0, "OBSERVE").valid
    wrong_agent = _answer(0, ROLLBACK, agent_name="release_agent")
    wrong_trigger = _answer(1, SCALE_OUT, trigger="LATENCY_HIGH")
    rep = _score([(HIGH, HIGH, ROLLBACK), (HIGH, HIGH, SCALE_OUT)], decisions=[wrong_agent, wrong_trigger])
    assert rep.misrouted == [wrong_agent, wrong_trigger]


# --- filter leaks ---------------------------------------------------------------------------


def test_p2_and_p3_tickets_in_p1_tickets_or_ticket_triage_are_leaks():
    p2, p3, late_p2 = _ticket(50, "P2"), _ticket(51, "P3"), _ticket(2000, "P2")
    rep = _score(
        [(LOW, LOW)],
        extra_tickets=[p2, p3, late_p2, _ticket(52, "P3")],
        p1=[p2, late_p2],
        triage=[_triage(51, LOW, priority="P3"), _triage(2000, LOW, priority="P2")],
    )
    assert rep.leaks == [("p1_tickets", _id(50), "P2"), ("ticket_triage", _id(51), "P3")]
    assert len(rep.results) == 1  # a leaked ticket is not scored as a P1


def test_no_leaks_when_only_p1_tickets_pass_the_filter():
    assert _score([(LOW, LOW), (HIGH, HIGH)], extra_tickets=[_ticket(50, "P2")]).leaks == []


# --- timing -----------------------------------------------------------------------------------


def test_the_medians_run_from_the_ticket_to_its_verdict_and_from_the_verdict_to_the_action():
    triage = [_triage(0, HIGH, after=2.0), _triage(1, HIGH, after=3.0), _triage(2, LOW, after=9.0),
              _triage(3, LOW, after=None)]  # fmt: skip
    answers = [_answer(0, ROLLBACK, after=7.0), _answer(1, SCALE_OUT, after=12.0), _answer(2, ROLLBACK, after=None)]
    rep = _score([(HIGH, None, ROLLBACK), (HIGH, None, SCALE_OUT), (LOW, None), (LOW, None), (LOW, None)],
                 triage=triage, decisions=answers)  # fmt: skip
    assert rep.median_to_verdict == 3.0  # 2, 3 and 9 s; ticket 3 has no timestamp, ticket 4 no verdict
    assert rep.median_to_action == 7.0  # 7 - 2 and 12 - 3


def test_no_timestamps_no_medians():
    rep = _score([(LOW, None)], triage=[_triage(0, LOW, after=None)])
    assert rep.median_to_verdict is None and rep.median_to_action is None


# --- acceptance --------------------------------------------------------------------------------


def test_no_p1_tickets_in_the_span_is_no_data_not_a_failure():
    rep = _score([], extra_tickets=[_ticket(0, "P2"), _ticket(1, "P3")])
    assert (rep.classifier_status, rep.responder_status, rep.leaks_status) == ("NO_DATA",) * 3
    outside = _score([(HIGH, None)], frm=T0 + 1_000)  # its only P1 is from before the span
    assert outside.results == [] and outside.classifier_status == "NO_DATA"


def test_the_classifier_passes_at_95_percent_and_fails_below():
    right = [(LOW, LOW)] * 19
    assert _score([*right, (HIGH, LOW)]).classifier_status == "PASS"  # 19 of 20
    assert _score([*right[:18], (HIGH, LOW), (LOW, "INVALID")]).classifier_status == "FAIL"  # 18 of 20
    assert _score([*right[:18], (HIGH, LOW), (LOW, None)]).classifier_status == "FAIL"


def test_the_responder_passes_at_95_percent_of_its_answers_and_fails_below():
    right = [(HIGH, HIGH, ROLLBACK, ROLLBACK)] * 19
    assert _score([*right, (HIGH, HIGH, SCALE_OUT, ROLLBACK)]).responder_status == "PASS"
    assert _score([*right[:18], *[(HIGH, HIGH, SCALE_OUT, ROLLBACK)] * 2]).responder_status == "FAIL"
    unanswered = _score([*right, (HIGH, HIGH, SCALE_OUT)])  # one HIGH ticket still waiting: not counted
    assert unanswered.responder_status == "PASS" and len(unanswered.answered) == 19


def test_the_responder_has_no_data_until_it_answers():
    rep = _score([(HIGH, HIGH, ROLLBACK)])
    assert rep.classifier_status == "PASS" and rep.responder_status == "NO_DATA"


def test_one_leak_fails():
    p2 = _ticket(50, "P2")
    assert _score([(LOW, LOW)], extra_tickets=[p2]).leaks_status == "PASS"
    assert _score([(LOW, LOW)], extra_tickets=[p2], p1=[p2]).leaks_status == "FAIL"


# --- reading: evaluate's options, hints and report --------------------------------------------


def _run_tables():
    """Kafka-shaped records of a short run: 20 P1 tickets, the first ten really HIGH, then a P2
    and a P3. The classifier calls ticket 0 LOW (one wrong verdict in twenty); the responder
    answers the other nine HIGH tickets with the expected action. Verdicts are written 4 s
    after the ticket, answers 6 s after the verdict."""
    actions = (ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM)
    tables = {name: [] for name in ("support_tickets", "ticket_answer_key", "p1_tickets", "ticket_triage",
                                    "remediation_decisions")}  # fmt: skip
    for n in range(22):
        at = T0 + 10 * n
        priority = "P1" if n < 20 else "P2" if n == 20 else "P3"
        severity = HIGH if n < 10 else LOW
        expected = actions[n % 3] if severity == HIGH else None
        ticket = {"event_time": iso(at), "ticket_id": _id(n), "source": "ZENDESK", "service_name": "checkout",
                  "priority": priority, "subject": f"subject {n}", "description": f"description {n}"}  # fmt: skip
        tables["support_tickets"].append(ticket)
        tables["ticket_answer_key"].append({"event_time": iso(at), "ticket_id": _id(n), "true_severity": severity,
                                            "expected_action": expected})  # fmt: skip
        if priority != "P1":
            continue
        tables["p1_tickets"].append({**ticket, "kafka_timestamp": iso(at + 1)})
        verdict = LOW if n == 0 else severity
        tables["ticket_triage"].append({**ticket, "severity": verdict, "reason": f"reason {n}",
                                        "raw_response": f"Severity: {verdict}\nReason: reason {n}",
                                        "kafka_timestamp": iso(at + 4)})  # fmt: skip
        if verdict == HIGH:
            tables["remediation_decisions"].append({
                "window_end": iso(at), "service_name": "checkout", "agent_name": "ticket_responder",
                "trigger": "HIGH_SEVERITY_TICKET", "action": expected, "target": None, "reason": f"because {n}",
                "raw_response": f"Action: {expected}\nReason: because {n}", "ticket_id": _id(n),
                "kafka_timestamp": iso(at + 10),
            })  # fmt: skip
    return tables


def _health(n):
    """`service_health` records for payments, n quiet windows from T0."""
    return [{"service_name": "payments", "window_start": iso(T0 + 10 * k), "window_end": iso(T0 + 10 * k + 10),
             "latency_status": "NORMAL", "traffic_status": "NORMAL", "avg_latency_ms": 230.0, "traffic_count": 300,
             "version": "2.9.4", "last_change": "INITIAL", "seconds_since_deploy": 600} for k in range(n)]  # fmt: skip


def _main(monkeypatch, capsys, tables, argv=()):
    """Run `evaluate` against fake Kafka reads; a table not in `tables` doesn't exist."""
    from scripts import evaluate

    monkeypatch.setattr(evaluate, "_read_table", lambda topic, creds, root: tables.get(topic))
    evaluate.main(["--to", iso(T0 + 1000), *argv])
    return " ".join(capsys.readouterr().out.split())  # rich wraps long lines


def test_evaluate_scores_the_ticket_pipeline_without_any_metric_table(monkeypatch, capsys):
    out = _main(monkeypatch, capsys, _run_tables())
    assert "`service_health` doesn't exist yet" in out and "`service_error_rate` doesn't exist yet" in out
    assert "20 P1 tickets created in the scored range" in out
    assert "Classifier: 19/20 right (95%, target ≥ 95%)" in out
    assert "Responder: 9/9 answers right (100%, target ≥ 95%)" in out
    assert "Filter leaks: 0 (target 0)" in out
    assert "ticket → verdict 4.0 s" in out and "verdict → action 6.0 s" in out


def test_read_topic_can_add_each_records_kafka_timestamp(monkeypatch):
    """Over fake Kafka and Schema Registry clients (the network boundary): the timings need
    the time each triage record and decision was written, which is not a column."""
    from types import SimpleNamespace

    import confluent_kafka
    import confluent_kafka.schema_registry
    import confluent_kafka.schema_registry.avro

    from scripts.common.topic_reader import KAFKA_TIMESTAMP, jsonable, read_topic

    eof = SimpleNamespace(code=lambda: confluent_kafka.KafkaError._PARTITION_EOF)

    def msg(value=None, ts=None):
        return SimpleNamespace(error=lambda: None if value else eof, value=lambda: value, partition=lambda: 0,
                               timestamp=lambda: (1, ts))  # fmt: skip

    class FakeConsumer:
        def __init__(self, conf):
            self.queue = [msg({"ticket_id": "ZD-48215"}, 1_800_000_004_250), msg()]

        def list_topics(self, topic, timeout):
            return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions={0: None})})

        def assign(self, partitions):
            pass

        def poll(self, timeout):
            return self.queue.pop(0) if self.queue else None

        def close(self):
            pass

    monkeypatch.setattr(confluent_kafka, "Consumer", FakeConsumer)
    monkeypatch.setattr(confluent_kafka.schema_registry, "SchemaRegistryClient", lambda conf: None)
    monkeypatch.setattr(confluent_kafka.schema_registry.avro, "AvroDeserializer",
                        lambda sr: lambda value, ctx: dict(value))  # fmt: skip
    card = {k: "x" for k in ("AIOPS_KAFKA_BOOTSTRAP", "AIOPS_KAFKA_API_KEY", "AIOPS_KAFKA_API_SECRET",
                             "AIOPS_SCHEMA_REGISTRY_URL", "AIOPS_SR_API_KEY", "AIOPS_SR_API_SECRET")}  # fmt: skip
    assert read_topic(card, "ticket_triage") == [{"ticket_id": "ZD-48215"}]
    [stamped] = [jsonable(r) for r in read_topic(card, "ticket_triage", timestamps=True)]
    assert stamped == {"ticket_id": "ZD-48215", KAFKA_TIMESTAMP: "2027-01-15T08:00:04.250Z"}


@pytest.mark.parametrize(
    ("table", "hint", "scored"),
    [
        ("support_tickets", "`support_tickets` doesn't exist yet. Ask your platform team to re-apply, then run "
         "ops-sim.", False),
        ("ticket_answer_key", "`ticket_answer_key` doesn't exist yet. Ask your platform team to re-apply, then run "
         "ops-sim.", False),
        ("p1_tickets", "`p1_tickets` doesn't exist yet. Ask your platform team to re-apply, then run the INSERT in "
         "sql/5.1_p1_tickets.sql.", True),
        ("ticket_triage", "`ticket_triage` doesn't exist yet. Ask your platform team to re-apply, then run the "
         "INSERT in sql/5.2_insert_ticket_classifier.sql.", True),
    ],
)  # fmt: skip
def test_a_missing_ticket_table_names_what_fills_it(monkeypatch, capsys, table, hint, scored):
    tables = {**_run_tables(), "service_health": _health(100), "service_incidents": []}
    del tables[table]
    out = _main(monkeypatch, capsys, tables)
    assert hint in out
    assert "Pipeline checks" in out  # the metric half runs either way
    assert ("Acceptance (D-068)" in out) == scored


def test_empty_ticket_tables_and_no_ticket_decisions_name_what_fills_them(monkeypatch, capsys):
    tables = {**_run_tables(), "p1_tickets": [], "ticket_triage": [], "remediation_decisions": []}
    out = _main(monkeypatch, capsys, tables)
    assert "`p1_tickets` has no rows yet. Run the INSERT in sql/5.1_p1_tickets.sql to fill it." in out
    assert "`ticket_triage` has no rows yet. Run the INSERT in sql/5.2_insert_ticket_classifier.sql to fill it." in out
    assert "No ticket decisions in `remediation_decisions` yet. Run the INSERT in " in out
    assert "sql/5.3_insert_ticket_responder.sql to fill it." in out
    assert "Classifier: 0/20 right" in out  # every P1 never classified
    assert "Responder: NO_DATA" in out


def test_with_no_ticket_and_no_metric_rows_there_is_nothing_to_score(monkeypatch, capsys):
    with pytest.raises(SystemExit, match="support_tickets"):
        _main(monkeypatch, capsys, {"support_tickets": []})


def _metric_tables():
    """A payments latency anomaly at window 30 and the release agent's decision on it."""
    health = _health(100)
    health[30]["latency_status"] = "HIGH"
    decision = {"window_end": health[30]["window_end"], "service_name": "payments", "agent_name": "release_agent",
                "trigger": "LATENCY_HIGH", "action": "OBSERVE", "target": None, "reason": "r",
                "raw_response": "Action: OBSERVE", "ticket_id": None}  # fmt: skip
    return {"service_health": health, "service_incidents": [], "remediation_decisions": [decision]}


def test_the_metric_half_reads_the_same_with_or_without_the_ticket_tables(monkeypatch, capsys):
    def metric_half(out):
        start = out.index("service_health:")
        return (out[start : out.index("Tickets (")] if "Tickets (" in out else out[start:]).strip()

    alone = _main(monkeypatch, capsys, _metric_tables())
    assert "`support_tickets` doesn't exist yet" in alone and "Acceptance (D-068)" not in alone
    run, metric = _run_tables(), _metric_tables()
    decisions = run["remediation_decisions"] + metric["remediation_decisions"]
    both = _main(monkeypatch, capsys, {**run, **metric, "remediation_decisions": decisions})
    assert "1 decisions in the scored range (1 read in total)" in metric_half(both)
    assert metric_half(both) == metric_half(alone)
    assert "Classifier: 19/20 right" in both


def test_the_ticket_tables_dump_and_score_the_same_offline(monkeypatch, capsys, tmp_path):
    dumps = {flag: str(tmp_path / f"{name}.jsonl") for flag, name in (
        ("tickets", "support_tickets"), ("answer-key", "ticket_answer_key"), ("p1-tickets", "p1_tickets"),
        ("triage", "ticket_triage"), ("decisions", "remediation_decisions"))}  # fmt: skip
    online = _main(monkeypatch, capsys, _run_tables(), [arg for flag, path in dumps.items()
                                                        for arg in (f"--dump-{flag}", path)])  # fmt: skip
    offline = _main(monkeypatch, capsys, {}, [
        "--ticket-rows", dumps["tickets"], "--answer-key-rows", dumps["answer-key"],
        "--p1-ticket-rows", dumps["p1-tickets"], "--triage-rows", dumps["triage"],
        "--decision-rows", dumps["decisions"],
    ])  # fmt: skip
    assert "Wrote 22 rows" in online and "Wrote 20 rows" in online
    assert f"Tickets ({dumps['tickets']})".replace(" ", "") in offline.replace(" ", "")  # rich may wrap the path
    assert online[online.index("Classification:") :] == offline[offline.index("Classification:") :]
    assert "ticket → verdict 4.0 s" in offline  # the Kafka timestamps survive the dump


def test_details_lists_every_wrong_verdict_and_wrong_action_with_the_raw_response(monkeypatch, capsys):
    tables = _run_tables()
    for rec in tables["ticket_triage"][10:]:  # the ten really-LOW tickets: 11 wrong verdicts with ticket 0
        rec["severity"], rec["raw_response"] = "INVALID", "I cannot tell."
    [one] = [d for d in tables["remediation_decisions"] if d["ticket_id"] == _id(1)]
    one["action"], one["raw_response"] = ROLLBACK, "Action: ROLLBACK\nReason: a release went out."
    brief = _main(monkeypatch, capsys, tables)
    full = _main(monkeypatch, capsys, tables, ["--details"])
    assert f"{_id(19)} really LOW, got INVALID" not in brief  # the eleventh
    assert brief.count("really LOW, got INVALID") == 9 and "raw:" not in brief  # ticket 0 and the first nine
    assert "--details lists them all" in brief
    assert full.count("really LOW, got INVALID") == 10 and full.count("raw: I cannot tell.") == 10
    assert f"{_id(0)} really HIGH, got LOW: subject 0 reason: reason 0 raw: Severity: LOW Reason: reason 0" in full
    assert f"{_id(1)} expected SCALE_OUT, got ROLLBACK: subject 1" in full
    assert "raw: Action: ROLLBACK Reason: a release went out." in full


def test_evaluate_reports_no_data_when_no_p1_ticket_was_created_in_the_span(monkeypatch, capsys):
    from scripts import evaluate

    tables = _run_tables()
    monkeypatch.setattr(evaluate, "_read_table", lambda topic, creds, root: tables.get(topic))
    evaluate.main(["--to", iso(T0 - 100)])
    out = " ".join(capsys.readouterr().out.split())
    assert "0 P1 tickets created in the scored range" in out
    assert "the ticket half reports NO_DATA" in out
    assert "Classifier: NO_DATA" in out and "Responder: NO_DATA" in out and "Filter leaks: NO_DATA" in out


def test_unexpected_answers_and_routing_errors_are_listed_by_ticket(monkeypatch, capsys):
    tables = _run_tables()
    tables["ticket_triage"][10]["severity"] = HIGH  # a false alarm on a really-LOW ticket
    extra = {**tables["remediation_decisions"][0], "action": SCALE_OUT, "kafka_timestamp": iso(T0 + 120)}
    tables["remediation_decisions"] += [{**extra, "ticket_id": _id(10), "window_end": iso(T0 + 100)},
                                        {**extra, "ticket_id": _id(11), "window_end": iso(T0 + 110)}]  # fmt: skip
    out = _main(monkeypatch, capsys, tables)
    assert f"Unexpected answers (to really-LOW tickets the classifier called HIGH): 1 ({_id(10)})" in out
    assert f"Routing errors (answers to tickets with no HIGH verdict): 1 ({_id(11)})" in out
