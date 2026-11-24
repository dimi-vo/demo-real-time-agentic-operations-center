"""The section 4 files must keep the contract the agents, the table and the scorer share.

`remediation_decisions` is written by three INSERT statements and read by
`uv run evaluate`, so the column list, the allowed actions, the output format and the
routing rules exist in several places at once (two prompts, three INSERTs, one DDL, one
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
)

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

AGENT_FILES = {RELEASE_AGENT: "4.2_release_agent.sql", CAPACITY_AGENT: "4.2_capacity_agent.sql"}
# Each INSERT: the agent it calls and the table it reads. The release agent has two legs:
# error-rate breaches (lab 2.2's table, D-067) and latency anomalies.
INSERTS = {
    "4.3_insert_release_agent_errors.sql": (RELEASE_AGENT, "service_error_rate"),
    "4.3_insert_release_agent.sql": (RELEASE_AGENT, "service_health"),
    "4.3_insert_capacity_agent.sql": (CAPACITY_AGENT, "service_health"),
}
ERRORS_INSERT, LATENCY_INSERT, CAPACITY_INSERT = INSERTS
TABLE_FILE = "sink_remediation_decisions.sql"

# Brief section 5.5, cut down in D-034.
OUTPUT_LINES = ("Action:", "Target:", "Reason:")
# D-034's eight, then the ticket a ticket_responder decision is about (D-068).
DECISION_COLUMNS = (
    "window_end", "service_name", "agent_name", "trigger",
    "action", "target", "reason", "raw_response", "ticket_id",
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


def _examples(prompt: str) -> list[tuple[str, str]]:
    """(action, the example's text) for every `Correct <ACTION> example:` block."""
    head = prompt[: prompt.index("\n---\n")]
    parts = re.split(r"Correct (\w+) example:", head)
    return list(zip(parts[1::2], parts[2::2], strict=True))


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_has_an_example_answer_for_every_allowed_action_and_no_other(agent):
    examples = _examples(read(AGENT_FILES[agent]))
    assert {action for action, _ in examples} == set(ALLOWED_ACTIONS[agent])
    for action, text in examples:
        assert f"Action: {action}\n" in text


def test_the_release_prompt_rolls_back_on_either_trigger():
    """D-067: the release agent answers latency anomalies and error-rate breaches by the same
    rule. With a ROLLBACK example for only one of them, the model could learn the trigger
    instead of the rule (errors roll back, latency is observed)."""
    prompt = read(AGENT_FILES[RELEASE_AGENT])
    rollbacks = [text for action, text in _examples(prompt) if action == "ROLLBACK"]
    assert any("latency" in t for t in rollbacks) and any("error rate" in t for t in rollbacks)
    assert "LATENCY_HIGH" in prompt and "ERROR_RATE_HIGH" in prompt
    assert "25 percent" in prompt


def _reason(text: str) -> str:
    return text.split("Reason: ", 1)[1]


def test_examples_put_each_trigger_on_a_service_that_gets_it():
    """D-067, D-068: web-gateway's only incident is an error-rate breach and payments is the
    only ARIMA service, so an example must not pair web-gateway with latency or traffic, and
    no example is about checkout (it produces tickets)."""
    for action, text in _examples(read(AGENT_FILES[RELEASE_AGENT])):
        if "Web-gateway" in text:
            assert "error rate" in text and "latency" not in text, (action, text)
        assert "checkout" not in text.lower(), (action, text)
    capacity = read(AGENT_FILES[CAPACITY_AGENT])
    assert "web-gateway" not in capacity.lower() and "checkout" not in capacity.lower()


def test_the_release_agent_rolls_back_a_payments_latency_regression():
    """D-068: the latency ROLLBACK example moves from checkout to payments: its version line
    (2.9.x) and its latency (about 4 x 220 ms against about 220 ms)."""
    rollbacks = [t for a, t in _examples(read(AGENT_FILES[RELEASE_AGENT])) if a == "ROLLBACK"]
    [example] = [t for t in rollbacks if "latency" in t]
    assert _reason(example).startswith("Payments latency is ")
    measured, expected = (int(n) for n in re.findall(r"(\d+) ms", example)[:2])
    assert 3.5 * 220 <= measured <= 4.5 * 220 and 200 <= expected <= 240
    assert re.search(r"Target: 2\.9\.\d+\n", example)


def test_the_capacity_agent_investigates_drops_and_observes_more_traffic():
    """D-068: surges are retired, so the capacity agent has no SCALE_OUT. Rule 1 answers a
    drop, rule 2 observes more traffic than forecast (noise, or the echo after a drop). One
    example each, both on payments, and the Target line is always N/A."""
    capacity = read(AGENT_FILES[CAPACITY_AGENT])
    examples = _examples(capacity)
    assert [action for action, _ in examples] == ["INVESTIGATE_UPSTREAM", "OBSERVE"]
    for action, text in examples:
        assert _reason(text).startswith("Payments"), (action, text)
        assert "Target: N/A\n" in text, (action, text)
    assert "1. INVESTIGATE_UPSTREAM — the trigger is TRAFFIC_LOW." in capacity
    assert "2. OBSERVE — the trigger is TRAFFIC_HIGH." in capacity
    assert "SCALE" not in capacity and "Suggested" not in capacity


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


@pytest.mark.parametrize("name", INSERTS)
def test_insert_calls_its_own_agent_by_name_as_a_string(name):
    """The docs type the first AI_RUN_AGENT argument as STRING (D-033)."""
    agent, _ = INSERTS[name]
    sql = read(name)
    assert f"AI_RUN_AGENT(\n  '{agent}'," in sql
    assert sql.count(f"'{agent}'") == 2, "once as the written agent_name, once as the call"


@pytest.mark.parametrize("name", INSERTS)
def test_insert_filters_below_the_lateral_join(name):
    """The agent must be called once per anomalous row, not once per window: the WHERE
    sits in a derived table that is the left input of the LATERAL join."""
    _, table = INSERTS[name]
    sql = read(name)
    where = sql.index("WHERE")
    assert where < sql.index("LATERAL TABLE")
    assert sql.index(f"FROM `{table}`") < where


@pytest.mark.parametrize("name", INSERTS)
def test_insert_reads_its_table_from_the_latest_offset(name):
    """Brief 5.5: starting a statement must not replay history through the model."""
    _, table = INSERTS[name]
    assert f"`{table}` /*+ OPTIONS('scan.startup.mode'='latest-offset') */" in read(name)


@pytest.mark.parametrize("name", INSERTS)
def test_insert_maps_a_failed_or_out_of_set_answer_to_invalid(name):
    agent, _ = INSERTS[name]
    sql = read(name)
    assert "`agent_status` = 'SUCCESS'" in sql
    assert "ELSE 'INVALID'" in sql
    for action in ALLOWED_ACTIONS[agent]:
        assert f"'{action}'" in sql
    for action in set(ACTIONS) - set(ALLOWED_ACTIONS[agent]):
        assert f"'{action}'" not in sql, "an agent must not accept another agent's action"
    assert "a.`agent_response`" in sql, "the raw answer is always written"


def test_routing_is_mutually_exclusive():
    """No window may reach two legs: traffic anomalies go to capacity, the latency leg takes
    latency only when traffic is normal, and the error-rate leg reads service_error_rate,
    where only web-gateway has rows (web-gateway has none in service_health, D-067)."""
    assert "WHERE `traffic_status` IN ('HIGH', 'LOW')" in read(CAPACITY_INSERT)
    assert "WHERE `latency_status` = 'HIGH' AND `traffic_status` NOT IN ('HIGH', 'LOW')" in read(LATENCY_INSERT)
    assert "WHERE `error_status` = 'HIGH'" in read(ERRORS_INSERT)


@pytest.mark.parametrize(("name", "trigger"), [(ERRORS_INSERT, "ERROR_RATE_HIGH"), (LATENCY_INSERT, "LATENCY_HIGH")])
def test_each_release_leg_writes_and_tells_its_own_trigger(name, trigger):
    sql = read(name)
    head = sql[sql.index("INSERT INTO") : sql.index("CASE")]
    assert f"'{trigger}'," in head, "the written trigger"
    assert f"'Trigger: {trigger}\\n'" in sql, "the trigger line the agent reads"


def _parsing(sql: str) -> str:
    """The Action, Target and Reason expressions: from the first CASE to the raw response."""
    return sql[sql.index("  CASE") : sql.index("  a.`agent_response`,\n")]


def test_both_release_legs_parse_the_answer_the_same_way():
    assert _parsing(read(ERRORS_INSERT)) == _parsing(read(LATENCY_INSERT))


def test_the_error_rate_leg_tells_the_agent_the_rate_against_the_threshold():
    sql = read(ERRORS_INSERT)
    for line in ("'Error Rate: '", "'  Threshold: 25 %\\n'", "'Errors: '", "' requests\\n'",
                 "'Last Change: '", "'  Seconds Since Deploy: '", "'Previous Version: '"):  # fmt: skip
        assert line in sql, line
    assert "ROUND(h.`error_rate` * 100, 1)" in sql


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


@pytest.mark.parametrize("name", INSERTS)
def test_inserts_start_with_the_four_identity_columns(name):
    """The INSERTs are positional: the first four values are the identity columns, in
    the DDL's order. The rest is checked by the arity test below."""
    agent, _ = INSERTS[name]
    sql = read(name)
    head = sql[sql.index("INSERT INTO") : sql.index("CASE")]
    assert "h.`window_end`," in head and "h.`service_name`," in head
    assert head.index("h.`window_end`") < head.index("h.`service_name`") < head.index(f"'{agent}'")


def test_capacity_insert_has_no_replica_arithmetic():
    """D-068: with SCALE_OUT gone there is no replica count to compute, so the Suggested
    Replicas formula and the Ready Replicas input line go, and the header says nothing of
    replicas."""
    sql = read(CAPACITY_INSERT)
    for gone in ("GREATEST(", "CEIL(", "ready_replicas", "Replicas", "replica"):
        assert gone not in sql, gone


def test_escalate_is_gone_from_every_agent_file():
    """D-043: every incident type has exactly one correct answer, and none is ESCALATE."""
    for name in (*AGENT_FILES.values(), *INSERTS, TABLE_FILE):
        assert "ESCALATE" not in read(name), name


@pytest.mark.parametrize("name", INSERTS)
def test_inserts_select_exactly_as_many_values_as_the_table_has_columns(table_sql, name):
    """The INSERTs are positional: a missing or extra value shifts every column."""
    sql = read(name)
    head = sql[sql.index("INSERT INTO") : sql.index("FROM (")]
    body = head[head.index("SELECT") + len("SELECT") :]
    depth, items = 0, 1
    for ch in body:
        depth += (ch == "(") - (ch == ")")
        items += ch == "," and depth == 0
    assert items == len(DECISION_COLUMNS)


@pytest.mark.parametrize("name", INSERTS)
def test_a_metric_decision_has_no_ticket(name):
    """D-068: ticket_id is the last column, and only the ticket responder (lab 5.3) sets it.
    Each 4.3 leg writes a typed null there, after the raw answer."""
    sql = read(name)
    assert "  a.`agent_response`,\n  CAST(NULL AS STRING)\nFROM (" in sql


def test_ticket_id_is_a_nullable_string_after_the_raw_response(table_sql):
    line = re.search(r"^\s+`ticket_id` (.+)$", table_sql, re.MULTILINE).group(1)
    assert line.startswith("STRING COMMENT ") and "NOT NULL" not in line
    assert "The ticket a ticket_responder decision is about; null for the metric agents" in line


def test_the_column_comments_name_every_agent_that_writes_and_the_ticket_trigger(table_sql):
    """Three agents write the table: the two metric agents (4.3) and the ticket responder (5.3).
    The classifier writes ticket_triage, never this table."""

    def comment(column):
        return re.search(rf"^\s+`{column}` .*COMMENT '([^']*)'", table_sql, re.MULTILINE).group(1)

    for agent in ("release_agent", "capacity_agent", "ticket_responder"):
        assert agent in comment("agent_name"), agent
    assert "ticket_classifier" not in comment("agent_name")
    assert "HIGH_SEVERITY_TICKET" in comment("trigger")
    assert "event_time" in comment("window_end")  # a ticket decision carries the ticket's time there


def test_no_select_star_with_extra_expressions():
    """`SELECT *, expr` is not worth the parser risk; the capacity leg lists its
    columns (D-033)."""
    for name in INSERTS:
        assert "*," not in read(name)
