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
    # Mirror of opsgen/kafka_io.py: the three topics the KafkaSink writes to.
    assert set(SOURCE_TOPICS) == {"service_metrics", "service_deployments", "service_incidents"}


def test_output_topics_are_exactly_the_three_lab_tables():
    assert set(OUTPUT_TOPICS) == {"service_error_rate", "service_health", "remediation_decisions"}  # D-067


@pytest.fixture
def run_reset(install_fakes):
    def run(*args, **fakes):
        h = install_fakes(reset, topics=fakes.pop("topics", ALL_TOPICS), **fakes)
        reset.main(["--creds", h.card, *args])
        return h

    return run


def test_empties_all_six_topics(run_reset):
    h = run_reset()
    assert sorted(h.admin.emptied) == sorted(ALL_TOPICS)


def test_keep_source_empties_only_the_output_topics(run_reset):
    h = run_reset("--keep-source")
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)


def test_drops_the_agents_and_never_a_table(run_reset):
    h = run_reset()
    assert h.session.submitted == [
        "DROP AGENT IF EXISTS `release_agent`;",
        "DROP AGENT IF EXISTS `capacity_agent`;",
    ]
    assert not any("TABLE" in sql for sql in h.session.submitted)


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


def test_deletes_the_generator_state_file(run_reset, tmp_path):
    path = state_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}")
    run_reset()
    assert not path.exists()


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
