"""`uv run reset`: what it stops, drops and empties, driven through `main(argv)` against a
fake Flink session and a fake Kafka admin (the `install_fakes` fixture in tests/conftest.py).

The pinned lists matter: the output tables are the platform team's, so `reset` empties their
topics and never drops them, and it must purge every source topic the generator writes to."""

import pytest

from opsgen.state import state_path
from scripts import reset
from scripts.reset import OUTPUT_TOPICS, SOURCE_TOPICS

ALL_TOPICS = (*SOURCE_TOPICS, *OUTPUT_TOPICS)


def statement(name: str, sql: str, phase: str = "RUNNING") -> dict:
    return {"name": name, "sql": sql, "phase": phase}


def test_source_topics_include_service_incidents():
    assert "service_incidents" in SOURCE_TOPICS


def test_source_topics_are_exactly_the_generator_streams():
    # Mirror of opsgen/kafka_io.py: the five topics the KafkaSink writes to, checkout's tickets
    # and their answer key included (D-068).
    assert set(SOURCE_TOPICS) == {
        "service_metrics",
        "service_deployments",
        "service_incidents",
        "support_tickets",
        "ticket_answer_key",
    }


def test_output_topics_are_exactly_the_five_lab_tables():
    # D-067's error rate; D-068's P1 tickets (lab 5.1) and the classifier's verdicts (lab 5.2).
    assert set(OUTPUT_TOPICS) == {
        "service_error_rate",
        "service_health",
        "remediation_decisions",
        "p1_tickets",
        "ticket_triage",
    }


@pytest.fixture
def run_reset(install_fakes):
    def run(*args, **fakes):
        h = install_fakes(reset, topics=fakes.pop("topics", ALL_TOPICS), **fakes)
        reset.main(["--creds", h.card, *args])
        return h

    return run


def test_empties_all_ten_topics(run_reset):
    h = run_reset()
    assert sorted(h.admin.emptied) == sorted(ALL_TOPICS)


def test_keep_source_empties_only_the_output_topics(run_reset):
    h = run_reset("--keep-source")
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)


def test_drops_the_agents_and_never_a_table(run_reset):
    # CREATE AGENT has no IF NOT EXISTS: labs 4.2, 5.2 and 5.3 could not run again otherwise.
    h = run_reset()
    assert h.session.submitted == [
        "DROP AGENT IF EXISTS `release_agent`;",
        "DROP AGENT IF EXISTS `capacity_agent`;",
        "DROP AGENT IF EXISTS `ticket_classifier`;",
        "DROP AGENT IF EXISTS `ticket_responder`;",
    ]
    assert not any("TABLE" in sql for sql in h.session.submitted)


def test_the_next_steps_name_every_insert_and_agent_lab_in_order(run_reset, capsys):
    run_reset()
    out = capsys.readouterr().out
    tail = out[out.index("Reset complete.") :]
    assert "Dropping the four agents..." in out
    assert "INSERTs (2.2, 2.5, 4.3, 5.1, 5.2 and 5.3)" in tail
    assert "create the agents again in labs 4.2, 5.2 and 5.3" in tail


def test_stops_running_statements_before_emptying_any_topic(run_reset):
    h = run_reset(
        statements=[
            statement("insert-health", "INSERT INTO service_health SELECT 1"),
            statement("old", "SELECT 1", phase="COMPLETED"),
        ],
    )
    kinds = [e[0] for e in h.events]
    assert h.events[0] == ("deleted", "insert-health")
    assert ("deleted", "old") not in h.events  # already terminal, nothing to stop
    assert kinds.index("deleted") < kinds.index("emptied")


def test_a_statement_that_will_not_go_away_is_a_problem(run_reset, monkeypatch, capsys):
    monkeypatch.setattr(reset, "STATEMENTS_GONE_TIMEOUT", 0)
    with pytest.raises(SystemExit) as exit_:
        run_reset(
            statements=[statement("stuck", "INSERT INTO service_health SELECT 1")],
            stuck=("stuck",),
        )
    assert exit_.value.code == 1
    assert "statement stuck is still running" in capsys.readouterr().out


def test_a_missing_topic_is_reported_and_exits_nonzero(run_reset, capsys):
    with pytest.raises(SystemExit) as exit_:
        run_reset(topics=tuple(t for t in ALL_TOPICS if t != "remediation_decisions"))
    assert exit_.value.code == 1
    out = capsys.readouterr().out
    assert "Topic remediation_decisions: MISSING — ask your platform team to re-apply" in out
    assert "Topic service_health: cleared" in out  # the others are still emptied


def _state_file(root):
    path = state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"incident_counter": 7, "next_ticket_numbers": {"ZENDESK": 48231, "SERVICENOW": 41889}}')
    return path


def test_deletes_the_generator_state_file(run_reset, tmp_path):
    path = _state_file(tmp_path)
    run_reset()
    assert not path.exists()


def test_keep_source_keeps_the_generator_state_file(run_reset, tmp_path, capsys):
    # The kept source topics hold the incidents, versions and tickets the file counts past, so
    # deleting it would make the next ops-sim reuse their ids next to them (D-046, D-067, D-068).
    path = _state_file(tmp_path)
    before = path.read_text()
    run_reset("--keep-source")
    assert path.read_text() == before
    assert "Kept the generator's persisted state" in capsys.readouterr().out


def test_refuses_while_a_local_ops_sim_runs(install_fakes, monkeypatch, capsys):
    h = install_fakes(reset, topics=ALL_TOPICS)
    monkeypatch.setattr(reset, "_local_ops_sim", lambda: ["123  uv run ops-sim"])
    with pytest.raises(SystemExit) as exit_:
        reset.main(["--creds", h.card])
    assert exit_.value.code == 1
    assert h.admin.emptied == [] and h.session.submitted == []
    assert "Stop `uv run ops-sim`" in capsys.readouterr().out
    reset.main(["--creds", h.card, "--force"])  # --force overrides the guard
    assert sorted(h.admin.emptied) == sorted(ALL_TOPICS)


def test_with_labs_is_gone():
    with pytest.raises(SystemExit):
        reset.main(["--with-labs"])
