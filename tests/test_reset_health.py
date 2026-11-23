"""`uv run reset-health`: which statements it deletes, and that nothing is emptied until they
are gone. Driven through `main(argv)` against a fake Flink session and a fake Kafka admin
(the `install_fakes` fixture in tests/conftest.py), so no network is involved."""

import pytest

from scripts import reset_health
from scripts.reset_health import OUTPUT_TOPICS, writes_output_table


def statement(name: str, sql: str, phase: str = "RUNNING") -> dict:
    return {"name": name, "sql": sql, "phase": phase}


HEALTH = "INSERT INTO `service_health`\nWITH enriched AS (SELECT 1) SELECT * FROM enriched;"
AGENT = "INSERT INTO `remediation_decisions` SELECT 1;"


# --- the matcher: what counts as "writes an output table" --------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO service_health SELECT 1",
        "INSERT INTO `service_health` SELECT 1",
        "INSERT INTO remediation_decisions SELECT 1",
        "insert into SERVICE_HEALTH select 1",
        "INSERT  INTO\n  `service_health`\nSELECT 1",
        "INSERT INTO `my-env`.`my-cluster`.`service_health` SELECT 1",
        "INSERT INTO my_env.my_cluster.remediation_decisions SELECT 1",
        "INSERT INTO `env` . `cluster` . `service_health` SELECT 1",
        "INSERT INTO `service_health`(a, b) SELECT 1, 2",
        "INSERT INTO service_health;",
        "-- 2.4 pasted from the file\n-- more comments\nINSERT INTO `service_health` SELECT 1",
        "/* a block\n   comment */\nINSERT INTO service_health SELECT 1",
        "-- c\n/* d */\n-- e\n  INSERT INTO remediation_decisions SELECT 1",
        "  \n INSERT INTO service_health SELECT 1",
    ],
)
def test_matches_every_way_of_writing_an_insert_into_an_output_table(sql):
    assert writes_output_table(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO service_health_other SELECT 1",
        "INSERT INTO `service_health_backup` SELECT 1",
        "INSERT INTO my_service_health SELECT 1",
        "SELECT * FROM service_health",
        "SELECT * FROM `service_health` WHERE service_name = 'checkout'",
        "INSERT INTO other_table SELECT * FROM service_health",
        "INSERT INTO `service_health`.`other` SELECT 1",
        "DROP TABLE service_health",
        "CREATE AGENT `release_agent` USING MODEL m",
        "-- INSERT INTO service_health\nSELECT 1",
        "/* INSERT INTO service_health */ SELECT 1",
        "",
    ],
)
def test_ignores_everything_else(sql):
    assert not writes_output_table(sql)


# --- the command ----------------------------------------------------------------------


@pytest.fixture
def run(install_fakes, monkeypatch):
    monkeypatch.setattr(reset_health, "WAIT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(reset_health, "POLL_SECONDS", 0.01)

    def go(*, statements=(), stuck=(), topics=OUTPUT_TOPICS):
        h = install_fakes(reset_health, statements=list(statements), stuck=stuck, topics=topics)
        reset_health.main(["--creds", h.card])
        return h

    return go


def test_deletes_both_agents_and_the_health_insert_but_not_other_statements(run):
    h = run(
        statements=[
            statement("health", HEALTH),
            statement("release", "-- release\n" + AGENT),
            statement("capacity", AGENT.lower()),
            statement("my-exploration", "SELECT * FROM `service_health` LIMIT 5"),
            statement("other-insert", "INSERT INTO other_table SELECT * FROM service_health"),
            statement("incidents-watch", "SELECT * FROM service_incidents"),
        ],
    )
    deleted = {name for kind, name in h.events if kind == "deleted"}
    assert deleted == {"health", "release", "capacity"}
    assert set(h.session.statements) == {"my-exploration", "other-insert", "incidents-watch"}


def test_ignores_statements_that_have_already_finished(run):
    h = run(
        statements=[
            statement("done", HEALTH, phase="COMPLETED"),
            statement("failed", AGENT, phase="FAILED"),
            statement("stopped", HEALTH, phase="STOPPED"),
            statement("live", HEALTH, phase="DEGRADED"),
        ],
    )
    assert [e for e in h.events if e[0] == "deleted"] == [("deleted", "live")]


def test_empties_exactly_the_two_output_topics_after_the_statements_are_gone(run):
    h = run(
        statements=[statement("health", HEALTH), statement("agent", AGENT)],
        topics=(*OUTPUT_TOPICS, "service_metrics", "service_deployments", "service_incidents"),
    )
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)  # never the source topics
    assert [kind for kind, _ in h.events] == ["deleted", "deleted", "emptied", "emptied"]


def test_a_statement_that_never_goes_away_means_nothing_is_emptied(install_fakes, monkeypatch, capsys):
    monkeypatch.setattr(reset_health, "WAIT_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(reset_health, "POLL_SECONDS", 0.01)
    h = install_fakes(
        reset_health,
        statements=[statement("health", HEALTH), statement("agent", AGENT)],
        stuck=("agent",),
        topics=OUTPUT_TOPICS,
    )
    with pytest.raises(SystemExit) as exit_:
        reset_health.main(["--creds", h.card])
    assert exit_.value.code == 1
    assert h.admin.emptied == []  # a running INSERT would write into the topic again
    out = capsys.readouterr().out
    assert "Nothing was emptied" in out and "agent" in out
    assert "INSERT INTO `remediation_decisions` SELECT 1;" in out  # its SQL was still printed


def test_nothing_running_still_empties_the_topics_and_exits_zero(run, capsys):
    h = run(statements=[statement("explore", "SELECT 1")])
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)
    assert not any(kind == "deleted" for kind, _ in h.events)
    assert "No running statement writes" in capsys.readouterr().out


def test_a_missing_topic_is_reported_and_exits_nonzero(run, capsys):
    with pytest.raises(SystemExit) as exit_:
        run(topics=("service_health",))
    assert exit_.value.code == 1
    out = capsys.readouterr().out
    assert "Topic remediation_decisions: MISSING" in out
    assert "Topic service_health: cleared" in out


def test_does_not_touch_the_agents_or_run_anything_for_the_developer(run):
    h = run(statements=[statement("health", HEALTH)])
    assert h.session.submitted == []  # no DROP AGENT, no INSERT: the developer clicks Run


def test_prints_each_deleted_statement_then_what_was_emptied_then_the_restart_order(run, capsys):
    run(statements=[statement("health-1", HEALTH), statement("agent-1", AGENT)])
    out = capsys.readouterr().out
    # the SQL of every deleted statement, in full
    assert "--- health-1 ---" in out and "WITH enriched AS (SELECT 1) SELECT * FROM enriched;" in out
    assert "--- agent-1 ---" in out and AGENT in out
    positions = [
        out.index("health-1"),
        out.index("Topic service_health: cleared"),
        out.index("Topic remediation_decisions: cleared"),
        out.index("`INSERT INTO service_health` first"),
        out.index("agent INSERTs"),
        out.index("Restart the ops wall"),
    ]
    assert positions == sorted(positions)
    assert "right-hand edge" in out
    assert "replays up to 24 hours" in out


def test_help_explains_the_command_in_two_lines(capsys):
    with pytest.raises(SystemExit) as exit_:
        reset_health.main(["--help"])
    assert exit_.value.code == 0
    out = capsys.readouterr().out
    assert "service_health" in out and "remediation_decisions" in out
    assert "Run" in out
