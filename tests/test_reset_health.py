"""`uv run reset-health`: which statements it deletes, and that nothing is emptied until they
are gone. Driven through `main(argv)` against a fake Flink session and a fake Kafka admin
(the `install_fakes` fixture in tests/conftest.py), so no network is involved."""

import pytest

from scripts import reset_health
from scripts.reset_health import OUTPUT_TOPICS, writes_output_table


def statement(name: str, sql: str, phase: str = "RUNNING") -> dict:
    return {"name": name, "sql": sql, "phase": phase}


HEALTH = "INSERT INTO `service_health`\nWITH enriched AS (SELECT 1) SELECT * FROM enriched;"
ERRORS = "INSERT INTO `service_error_rate`\nWITH enriched AS (SELECT 1) SELECT * FROM enriched;"  # lab 2.2, D-067
AGENT = "INSERT INTO `remediation_decisions` SELECT 1;"
# Section 5 (D-068): the P1 filter, the classifier, and the responder, which writes decisions.
P1_FILTER = "INSERT INTO `p1_tickets`\nSELECT * FROM `support_tickets` WHERE `priority` = 'P1';"
CLASSIFIER = "-- 5.2\nINSERT INTO `ticket_triage` SELECT 1;"
RESPONDER = "-- 5.3\nINSERT INTO `remediation_decisions` SELECT 1;"
# Section 6 (D-070): the critical filter and the security agent, which writes its own table.
CRITICAL_FILTER = "INSERT INTO `critical_cves`\nSELECT `event_time` FROM `cve_scans`\nWHERE `cvss_score` >= 9.0;"
SECURITY = "-- 6.4\nINSERT INTO `patch_decisions`\nSELECT 1;"


def test_the_output_topics_are_the_seven_the_labs_write():
    assert OUTPUT_TOPICS == (
        "service_error_rate",
        "service_health",
        "remediation_decisions",
        "p1_tickets",
        "ticket_triage",
        "critical_cves",
        "patch_decisions",
    )


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
        "-- 2.5 pasted from the file\n-- more comments\nINSERT INTO `service_health` SELECT 1",
        "/* a block\n   comment */\nINSERT INTO service_health SELECT 1",
        "-- c\n/* d */\n-- e\n  INSERT INTO remediation_decisions SELECT 1",
        "  \n INSERT INTO service_health SELECT 1",
        "INSERT INTO `service_error_rate` SELECT 1",
        "INSERT INTO remediation_decisions SELECT 1",
        "INSERT INTO `p1_tickets` SELECT * FROM `support_tickets`",
        "-- 5.2\nINSERT INTO my_env.my_cluster.ticket_triage SELECT 1",
        "INSERT INTO `critical_cves` SELECT * FROM `cve_scans` WHERE `cvss_score` >= 9.0",
        "-- 6.4\nINSERT INTO my_env.my_cluster.patch_decisions SELECT 1",
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
        "SELECT * FROM `p1_tickets`",
        "INSERT INTO support_tickets SELECT 1",  # a source table: the generator's, not a lab's
        "INSERT INTO ticket_answer_key SELECT 1",
        "SELECT * FROM `critical_cves`",
        "INSERT INTO cve_scans SELECT 1",  # the generator's, like support_tickets
        "INSERT INTO cve_answer_key SELECT 1",
        "INSERT INTO `patch_decisions_backup` SELECT 1",
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


def test_deletes_the_agents_and_both_detection_inserts_but_not_other_statements(run):
    h = run(
        statements=[
            statement("errors", ERRORS),
            statement("health", HEALTH),
            statement("release", "-- release\n" + AGENT),
            statement("capacity", AGENT.lower()),
            statement("my-exploration", "SELECT * FROM `service_health` LIMIT 5"),
            statement("other-insert", "INSERT INTO other_table SELECT * FROM service_health"),
            statement("incidents-watch", "SELECT * FROM service_incidents"),
        ],
    )
    deleted = {name for kind, name in h.events if kind == "deleted"}
    assert deleted == {"errors", "health", "release", "capacity"}
    assert set(h.session.statements) == {"my-exploration", "other-insert", "incidents-watch"}


def test_deletes_the_three_section_5_inserts_too(run):
    # D-068: the P1 filter and the classifier write their own tables; the responder's INSERT
    # goes with the other remediation_decisions writers.
    h = run(
        statements=[
            statement("p1-filter", P1_FILTER),
            statement("classifier", CLASSIFIER),
            statement("responder", RESPONDER),
            statement("tickets-watch", "SELECT * FROM `support_tickets` WHERE `priority` = 'P1'"),
        ],
    )
    deleted = {name for kind, name in h.events if kind == "deleted"}
    assert deleted == {"p1-filter", "classifier", "responder"}
    assert set(h.session.statements) == {"tickets-watch"}
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)
    assert {"p1_tickets", "ticket_triage"} <= set(h.admin.emptied)


def test_deletes_the_two_section_6_inserts_too(run):
    # D-070: lab 6.1's critical filter and lab 6.4's security agent write their own tables.
    h = run(
        statements=[
            statement("critical-filter", CRITICAL_FILTER),
            statement("security", SECURITY),
            statement("scans-watch", "SELECT * FROM `cve_scans` WHERE `cvss_score` >= 9.0"),
        ],
    )
    deleted = {name for kind, name in h.events if kind == "deleted"}
    assert deleted == {"critical-filter", "security"}
    assert set(h.session.statements) == {"scans-watch"}
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)
    assert {"critical_cves", "patch_decisions"} <= set(h.admin.emptied)


def test_names_all_seven_tables_when_it_deletes(run, capsys):
    run(statements=[statement("security", SECURITY)])
    out = capsys.readouterr().out
    assert (
        "write service_error_rate, service_health, remediation_decisions, p1_tickets, ticket_triage, "
        "critical_cves or patch_decisions:" in out
    )


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


def test_empties_exactly_the_output_topics_after_the_statements_are_gone(run):
    h = run(
        statements=[statement("health", HEALTH), statement("agent", AGENT)],
        topics=(*OUTPUT_TOPICS, "service_metrics", "service_deployments", "service_incidents"),
    )
    assert sorted(h.admin.emptied) == sorted(OUTPUT_TOPICS)  # never the source topics
    assert [kind for kind, _ in h.events] == ["deleted", "deleted", *["emptied"] * len(OUTPUT_TOPICS)]


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


def test_the_next_steps_name_every_insert_in_lab_order(run, capsys):
    run(statements=[])
    out = capsys.readouterr().out
    steps = [out.index(s) for s in ("INSERT INTO service_error_rate", "INSERT INTO service_health",
                                      "4.3_insert_release_agent_errors.sql",
                                      "5.1_p1_tickets.sql", "5.2_insert_ticket_classifier.sql",
                                      "5.3_insert_ticket_responder.sql",
                                      "6.1_critical_cves.sql", "6.4_insert_security_agent.sql")]  # fmt: skip
    assert steps == sorted(steps)
    # 5.2 and 5.3 read from the latest offset: started during 5.1's replay, the classifier
    # would call the model for every replayed P1.
    assert "once 5.1 has caught up" in out
    # 6.4 too (D-070): started during 6.1's replay, the security agent would call the model,
    # and the patch agent on the laptop, for every replayed critical finding.
    assert "once 6.1 has caught up" in out
    assert "uv run patch-agent" in out


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
    assert "critical_cves" in out and "patch_decisions" in out
    assert "Run" in out
