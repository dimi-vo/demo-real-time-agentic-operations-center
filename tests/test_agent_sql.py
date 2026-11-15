"""The section 4 files must keep the contract the agents, the table and the scorer share.

`remediation_decisions` is written by two INSERT statements and read by
`uv run evaluate`, so the column list, the allowed actions, the output format and the
routing rules exist in four places at once (two prompts, two INSERTs, one DDL, one
scorer). These tests fail when they drift apart.
"""

import re
from pathlib import Path

import pytest

from scripts.evaluation.decisions import (
    ACTIONS,
    ALLOWED_ACTIONS,
    CAPACITY_AGENT,
    RELEASE_AGENT,
    suggested_replicas,
)

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

AGENT_FILES = {RELEASE_AGENT: "4.2_release_agent.sql", CAPACITY_AGENT: "4.2_capacity_agent.sql"}
INSERT_FILES = {RELEASE_AGENT: "4.3_insert_release_agent.sql", CAPACITY_AGENT: "4.3_insert_capacity_agent.sql"}
TABLE_FILE = "sink_remediation_decisions.sql"

# Brief section 5.5, cut down in D-034.
OUTPUT_LINES = ("Action:", "Target:", "Reason:")
DECISION_COLUMNS = (
    "window_end", "service_name", "agent_name", "trigger",
    "action", "target", "reason", "raw_response",
)  # fmt: skip


def read(name: str) -> str:
    return (SQL / name).read_text()


@pytest.fixture(scope="module")
def table_sql() -> str:
    return read(TABLE_FILE)


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_states_the_three_output_lines_in_order(agent):
    prompt = read(AGENT_FILES[agent])
    positions = [prompt.index(line) for line in OUTPUT_LINES]
    assert positions == sorted(positions), "the output format must list the three lines in order"
    for line in ("Confidence:", "Summary:", "Reasoning:", "Target Version:", "Target Replicas:"):
        assert line not in prompt, "D-034 cut the output down to three lines"


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_has_one_example_answer_per_allowed_action(agent):
    prompt = read(AGENT_FILES[agent])
    for action in ALLOWED_ACTIONS[agent]:
        assert f"Correct {action} example:" in prompt
    # Every example starts with the Action line, so counting them counts the examples.
    assert prompt.count("Correct ") == len(ALLOWED_ACTIONS[agent])


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_allows_exactly_the_actions_the_scorer_accepts(agent):
    prompt = read(AGENT_FILES[agent])
    stated = re.search(r"the Action line must be exactly ([^.]+)\.", prompt)
    assert stated, "the prompt must state the allowed actions"
    named = {w for w in re.findall(r"\b[A-Z][A-Z_]{2,}\b", stated.group(1))}
    assert named == set(ALLOWED_ACTIONS[agent])


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_is_a_single_quoted_string_without_apostrophes(agent):
    """No escaping rule for a literal apostrophe inside USING PROMPT is documented
    (D-033), so the prompts avoid them; the SQL is one statement per file."""
    sql = read(AGENT_FILES[agent])
    start = sql.index("USING PROMPT '") + len("USING PROMPT '")
    end = sql.index("'\nWITH (", start)
    assert "'" not in sql[start:end], "an apostrophe would close the prompt literal early"
    assert sql.rstrip().endswith(");"), "one statement per file: an attendee pastes one file per statement"


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_agent_continues_on_a_failed_model_call(agent):
    """D-007: one failed LLM call must not stop a statement that runs all workshop."""
    sql = read(AGENT_FILES[agent])
    assert "'handle_exception' = 'continue'" in sql
    assert "'max_consecutive_failures'" in sql, "the documented default is contradictory; set it"


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_insert_calls_its_own_agent_by_name_as_a_string(agent):
    """The docs type the first AI_RUN_AGENT argument as STRING (D-033)."""
    sql = read(INSERT_FILES[agent])
    assert f"AI_RUN_AGENT(\n  '{agent}'," in sql
    assert sql.count(f"'{agent}'") == 2, "once as the written agent_name, once as the call"


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_insert_filters_below_the_lateral_join(agent):
    """The agent must be called once per anomalous row, not once per window: the WHERE
    sits in a derived table that is the left input of the LATERAL join."""
    sql = read(INSERT_FILES[agent])
    where = sql.index("WHERE")
    assert where < sql.index("LATERAL TABLE")
    assert sql.index("FROM `service_health`") < where


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_insert_reads_service_health_from_the_latest_offset(agent):
    """Brief 5.5: starting a statement must not replay history through the model."""
    sql = read(INSERT_FILES[agent])
    assert "`service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */" in sql


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_insert_maps_a_failed_or_out_of_set_answer_to_invalid(agent):
    sql = read(INSERT_FILES[agent])
    assert "`agent_status` = 'SUCCESS'" in sql
    assert "ELSE 'INVALID'" in sql
    for action in ALLOWED_ACTIONS[agent]:
        assert f"'{action}'" in sql
    for action in set(ACTIONS) - set(ALLOWED_ACTIONS[agent]):
        assert f"'{action}'" not in sql, "an agent must not accept another agent's action"
    assert "a.`agent_response`" in sql, "the raw answer is always written"


def test_routing_is_mutually_exclusive():
    """No window may reach both agents: traffic anomalies go to capacity, and the
    release leg takes latency only when traffic is normal."""
    assert "WHERE `traffic_status` IN ('HIGH', 'LOW')" in read(INSERT_FILES[CAPACITY_AGENT])
    assert (
        "WHERE `latency_status` = 'HIGH' AND `traffic_status` NOT IN ('HIGH', 'LOW')"
        in read(INSERT_FILES[RELEASE_AGENT])
    )


def test_table_declares_the_columns_in_order(table_sql):
    found = re.findall(r"^\s+`(\w+)`", table_sql, re.MULTILINE)
    assert tuple(found) == DECISION_COLUMNS


def test_table_is_append_only_and_readable_before_commit(table_sql):
    assert "'changelog.mode' = 'append'" in table_sql
    assert "'kafka.consumer.isolation-level' = 'read-uncommitted'" in table_sql


def test_table_is_created_by_the_platform_team_and_can_be_emptied(table_sql):
    """D-050: Terraform runs this DDL, so it must be re-runnable; `reset` and `reset-health`
    empty the topic with delete-records, which needs delete cleanup."""
    assert "CREATE TABLE IF NOT EXISTS `remediation_decisions`" in table_sql
    assert "'kafka.cleanup-policy' = 'delete'" in table_sql


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_inserts_start_with_the_four_identity_columns(agent):
    """Both INSERTs are positional: the first four values are the identity columns, in
    the DDL's order. The rest is checked by the arity test below."""
    sql = read(INSERT_FILES[agent])
    head = sql[sql.index("INSERT INTO") : sql.index("CASE")]
    assert "h.`window_end`," in head and "h.`service_name`," in head
    assert head.index("h.`window_end`") < head.index("h.`service_name`") < head.index(f"'{agent}'")


def test_capacity_insert_computes_the_replica_suggestion_in_sql():
    """Brief 5.5: the arithmetic is deterministic, the agent only decides to use it."""
    sql = read(INSERT_FILES[CAPACITY_AGENT])
    assert "GREATEST(" in sql and "CEIL(" in sql
    assert "Suggested Replicas: " in sql, "the computed value must reach the prompt"
    # D-043: no autoscaling ceiling, so no cap and no Max Replicas in the prompt.
    assert "LEAST(" not in sql and "max_replicas" not in sql and "Max Replicas" not in sql


@pytest.mark.parametrize(
    ("ready", "count", "expected", "want"),
    [
        (3, 600, 200, 9),  # three times the load
        (3, 210, 200, 4),  # a nudge over: always at least one more replica
        (4, 450, 150, 12),  # no ceiling any more (D-043)
        (2, 90, 30, 6),  # three times the load on two replicas
        (2, 90, 0, 3),  # an expected count of zero must not divide
        (2, 90, None, 3),  # nor a missing one
        (None, 90, 30, None),
    ],
)
def test_suggested_replicas_matches_the_sql_formula(ready, count, expected, want):
    assert suggested_replicas(ready, count, expected) == want


def test_escalate_is_gone_from_every_agent_file():
    """D-043: every incident type has exactly one correct answer, and none is ESCALATE."""
    for name in (*AGENT_FILES.values(), *INSERT_FILES.values(), TABLE_FILE):
        assert "ESCALATE" not in read(name), name


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_inserts_select_exactly_as_many_values_as_the_table_has_columns(table_sql, agent):
    """Both INSERTs are positional: a missing or extra value shifts every column."""
    sql = read(INSERT_FILES[agent])
    head = sql[sql.index("INSERT INTO") : sql.index("FROM (")]
    body = head[head.index("SELECT") + len("SELECT") :]
    depth, items = 0, 1
    for ch in body:
        depth += (ch == "(") - (ch == ")")
        items += ch == "," and depth == 0
    assert items == len(DECISION_COLUMNS)


def test_no_select_star_with_extra_expressions():
    """`SELECT *, expr` is not worth the parser risk; the capacity leg lists its
    columns (D-033)."""
    for name in INSERT_FILES.values():
        assert "*," not in read(name)
