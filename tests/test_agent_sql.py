"""The agent files must keep the contract the agents, the tables and the scorer share.

`remediation_decisions` is written by three INSERT statements (section 4) and read by
`uv run evaluate`, so the column list, the allowed actions, the output format and the
routing rules exist in several places at once (two prompts, three INSERTs, one DDL, one
scorer). These tests fail when they drift apart.

Section 5 (D-068) adds the ticket classifier (5.2). Its prompt answers a Severity and a
Reason, with a HIGH, a plain LOW and a shouting LOW example filled in from the approved
ticket templates. Its INSERT writes every P1 ticket's verdict to `ticket_triage`, parsed the
way the 4.3 legs parse an action. The rules each INSERT follows are keyed by its file, and
the output lines and allowed answers by its agent.
"""

import re
import string
from pathlib import Path

import pytest

from opsgen.ticket_templates import POOLS, TEMPLATES, Template
from scripts.evaluation.decisions import (
    ACTIONS,
    ALLOWED_ACTIONS,
    CAPACITY_AGENT,
    INVESTIGATE_UPSTREAM,
    RELEASE_AGENT,
    ROLLBACK,
    SCALE_OUT,
    TICKET_RESPONDER,
)
from scripts.evaluation.tickets import HIGH, LOW

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

TICKET_CLASSIFIER = "ticket_classifier"  # lab 5.2 (D-068)
AGENT_FILES = {
    RELEASE_AGENT: "4.2_release_agent.sql",
    CAPACITY_AGENT: "4.2_capacity_agent.sql",
    TICKET_CLASSIFIER: "5.2_ticket_classifier.sql",
    TICKET_RESPONDER: "5.3_ticket_responder.sql",
}
# Each INSERT: the agent it calls and the table it reads. The release agent has two legs:
# error-rate breaches (lab 2.2's table, D-067) and latency anomalies. The classifier reads
# lab 5.1's P1 tickets (D-068).
INSERTS = {
    "4.3_insert_release_agent_errors.sql": (RELEASE_AGENT, "service_error_rate"),
    "4.3_insert_release_agent.sql": (RELEASE_AGENT, "service_health"),
    "4.3_insert_capacity_agent.sql": (CAPACITY_AGENT, "service_health"),
    "5.2_insert_ticket_classifier.sql": (TICKET_CLASSIFIER, "p1_tickets"),
}
ERRORS_INSERT, LATENCY_INSERT, CAPACITY_INSERT, CLASSIFIER_INSERT = INSERTS
# The 4.3 legs: one decision per flagged window, into remediation_decisions.
METRIC_INSERTS = (ERRORS_INSERT, LATENCY_INSERT, CAPACITY_INSERT)
TABLE_FILE = "sink_remediation_decisions.sql"

# Each agent's answer, line by line: brief section 5.5, cut down in D-034, for the metric
# agents; a severity and a reason for the classifier, and an action and a reason for the
# responder, which never has a target (D-068).
OUTPUT_LINES = {
    RELEASE_AGENT: ("Action:", "Target:", "Reason:"),
    CAPACITY_AGENT: ("Action:", "Target:", "Reason:"),
    TICKET_CLASSIFIER: ("Severity:", "Reason:"),
    TICKET_RESPONDER: ("Action:", "Reason:"),
}
# What the first line may say: a metric agent's action, or the classifier's verdict.
ALLOWED_ANSWERS = {**ALLOWED_ACTIONS, TICKET_CLASSIFIER: (HIGH, LOW)}
# D-034's eight, then the ticket a ticket_responder decision is about (D-068).
DECISION_COLUMNS = (
    "window_end", "service_name", "agent_name", "trigger",
    "action", "target", "reason", "raw_response", "ticket_id",
)  # fmt: skip


def read(name: str) -> str:
    return (SQL / name).read_text()


def _written_columns(sql: str) -> list[str]:
    """The columns of the table an INSERT writes, from that table's DDL."""
    table = re.search(r"INSERT INTO `(\w+)`", sql).group(1)
    return re.findall(r"^\s+`(\w+)`", read(f"sink_{table}.sql"), re.MULTILINE)


def _select_items(sql: str) -> list[str]:
    """An INSERT's values, split at the select list's top-level commas."""
    head = sql[sql.index("INSERT INTO") : sql.index("FROM (")]
    body = head[head.index("SELECT") + len("SELECT") :]
    items, depth, item = [], 0, ""
    for ch in body:
        depth += (ch == "(") - (ch == ")")
        if ch == "," and depth == 0:
            items.append(item.strip())
            item = ""
        else:
            item += ch
    return [*items, item.strip()]


@pytest.fixture(scope="module")
def table_sql() -> str:
    return read(TABLE_FILE)


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_states_its_own_output_lines_in_order(agent):
    prompt = read(AGENT_FILES[agent])
    lines = OUTPUT_LINES[agent]
    positions = [prompt.index(line) for line in lines]
    assert positions == sorted(positions), "the output format must list the lines in order"
    assert f"respond with exactly these {len(lines)} labeled lines, in this order." in prompt
    for line in ("Confidence:", "Summary:", "Reasoning:", "Target Version:", "Target Replicas:"):
        assert line not in prompt, "D-034 cut the output down to three lines"
    for other in {line for each in OUTPUT_LINES.values() for line in each} - set(lines):
        assert other not in prompt, f"{other} is another agent's line"


def _examples(prompt: str) -> list[tuple[str, str]]:
    """(action, the example's text) for every `Correct <ACTION> example:` block."""
    head = prompt[: prompt.index("\n---\n")]
    parts = re.split(r"Correct (\w+) example:", head)
    return list(zip(parts[1::2], parts[2::2], strict=True))


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_has_an_example_answer_for_every_allowed_answer_and_no_other(agent):
    examples = _examples(read(AGENT_FILES[agent]))
    assert {answer for answer, _ in examples} == set(ALLOWED_ANSWERS[agent])
    for answer, text in examples:
        assert f"{OUTPUT_LINES[agent][0]} {answer}\n" in text


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


# --- lab 5.2: the ticket classifier (D-068) ------------------------------------------------


def test_the_classifier_answers_a_severity_and_a_reason():
    """Spec decision 20: two labelled lines, plain text."""
    assert "Severity: [HIGH | LOW]\nReason: [one or two sentences]\n" in read(AGENT_FILES[TICKET_CLASSIFIER])


def test_the_classifier_calls_high_only_when_many_customers_cannot_buy_right_now():
    """Spec decision 21: rule 1 is HIGH, rule 2 is LOW for everything else, however the ticket
    is worded. Every ticket it reads is a P1, and reporters over-prioritise."""
    prompt = read(AGENT_FILES[TICKET_CLASSIFIER])
    high, low, strict = (prompt.index(s) for s in ("1. HIGH — ", "2. LOW — ", "STRICT VALUES"))
    assert high < low < strict
    assert "many customers cannot complete checkout or pay, right now" in prompt[high:low]
    rule_low = prompt[low:strict]
    for wording in ("one customer", "a question", "a request", "something cosmetic",
                    "Capitals", "exclamation marks", "urgent", "the P1 itself"):  # fmt: skip
        assert wording in rule_low, wording
    assert "never make a ticket HIGH" in rule_low
    assert "marked P1 by whoever filed it" in prompt and "over-prioritise" in prompt


# An example shows its ticket as the INSERT shows one, then the answer. A responder example
# also shows the classifier's reason, as 5.3's INSERT passes it on.
_EXAMPLE_TICKET = re.compile(
    r"\nTICKET (?:ZD-\d{5}|INC\d{7}) from (?:ZENDESK|SERVICENOW)\nPriority: P1\n"
    r"Subject: (?P<subject>[^\n]+)\nDescription: (?P<description>[^\n]+)\n"
    r"(?:Triage Reason: (?P<triage>[^\n]+)\n)?Answer:\n"
)


def _filled(text: str) -> re.Pattern:
    """A template's text, each placeholder matching any value of its pool."""
    pattern = ""
    for literal, name, _, _ in string.Formatter().parse(text):
        pattern += re.escape(literal)
        if name is not None:
            pattern += "(?:" + "|".join(map(re.escape, POOLS[name])) + ")"
    return re.compile(pattern)


def _ticket_examples(agent: str) -> list[tuple[str, Template, str | None, str]]:
    """(answer, the approved template its ticket fills in, Triage Reason or None, the answer's
    Reason) for each example of a ticket agent."""
    found = []
    for answer, text in _examples(read(AGENT_FILES[agent])):
        ticket = _EXAMPLE_TICKET.match(text)
        assert ticket, text
        [template] = [
            t
            for t in TEMPLATES
            if _filled(t.subject).fullmatch(ticket["subject"])
            and _filled(t.description).fullmatch(ticket["description"])
        ]
        found.append((answer, template, ticket["triage"], _reason(text[ticket.end() :]).strip()))
    return found


def _classifier_examples() -> list[tuple[str, Template, str]]:
    """(severity, the approved template its ticket fills in, Reason) for each example."""
    found = []
    for severity, template, triage, reason in _ticket_examples(TICKET_CLASSIFIER):
        assert triage is None, "the classifier is the first agent: nothing has triaged the ticket"
        found.append((severity, template, reason))
    return found


def test_the_classifier_examples_are_approved_p1_tickets_filled_in():
    """One HIGH, one plain LOW and one shouting LOW, each a P1 template the user approved
    (issue 03) with its placeholders filled from their pools, answered with its true severity."""
    examples = _classifier_examples()
    for severity, template, _ in examples:
        assert (template.priority, template.true_severity) == ("P1", severity), template.subject
    assert sorted((severity, t.shouting) for severity, t, _ in examples) == [(HIGH, False), (LOW, False), (LOW, True)]


def test_each_classifier_example_reason_names_the_deciding_fact():
    """How many customers, and whether they can still buy or pay."""
    examples = _classifier_examples()
    assert examples
    for severity, _, reason in examples:
        assert "customer" in reason.lower(), (severity, reason)
        assert re.search(r"\b(buy|pay)\b", reason), (severity, reason)


def test_the_shouting_example_is_low_for_one_customer_while_checkout_works():
    [reason] = [reason for _, template, reason in _classifier_examples() if template.shouting]
    assert reason.startswith("One customer ") and "checkout works" in reason
    assert "capitals" in reason.lower() and "urgen" in reason.lower(), "why the wording does not count"


def test_the_classifier_insert_writes_every_p1_ticket_to_ticket_triage():
    """One statement; every P1 is classified and written, LOW ones included, so the wall and
    `evaluate` see each verdict. The latest offset is checked with the other INSERTs."""
    sql = read(CLASSIFIER_INSERT)
    body = sql[sql.index("INSERT INTO") :]
    assert body.startswith("INSERT INTO `ticket_triage`\nSELECT\n")
    assert "FROM `p1_tickets` /*+" in body and "WHERE" not in body
    assert body.count(";") == 1 and body.rstrip().endswith(");")


def test_the_classifier_insert_writes_the_ticket_triage_columns_in_order():
    """The seven ticket columns as they came, then the verdict, the reason and the raw answer
    (spec decision 16)."""
    sql = read(CLASSIFIER_INSERT)
    columns, items = _written_columns(sql), _select_items(sql)
    assert columns[7:] == ["severity", "reason", "raw_response"]
    assert items[:7] == [f"t.`{c}`" for c in columns[:7]]
    assert items[7].startswith("CASE") and "Severity:" in items[7]
    assert items[8].startswith("TRIM(") and "Reason:" in items[8]
    assert items[9] == "a.`agent_response`"


def _severity_or_action(sql: str) -> str:
    """The multi-line CASE that turns the answer's first line into a value or INVALID."""
    return sql[sql.index("  CASE\n") : sql.index("  END,\n")]


def test_the_classifier_parses_its_severity_as_the_metric_legs_parse_an_action():
    """The same agent_status handling and the same regex, on the Severity line: HIGH and LOW
    are the only values, and anything else is INVALID."""
    capacity = _severity_or_action(read(CAPACITY_INSERT))
    expected = capacity.replace("Action:", "Severity:").replace(
        "IN ('INVESTIGATE_UPSTREAM', 'OBSERVE')", "IN ('HIGH', 'LOW')"
    )
    assert "Severity:" in expected and "IN ('HIGH', 'LOW')" in expected
    assert _severity_or_action(read(CLASSIFIER_INSERT)) == expected


def test_every_insert_parses_the_reason_the_same_way():
    def reason(name):
        [line] = [line for line in read(name).splitlines() if "REGEXP_EXTRACT" in line and "Reason:" in line]
        return line.strip().rstrip(",")

    assert len({reason(name) for name in INSERTS}) == 1


def test_the_classifier_input_names_the_ticket_and_survives_a_null_text():
    """Its id, source, priority, subject and description, labelled as in the prompt examples.
    CONCAT returns null when any argument is null, so the two free-text columns fall back to
    N/A, as Last Change does in 4.3."""
    sql = read(CLASSIFIER_INSERT)
    concat = sql[sql.index("  CONCAT(\n") : sql.index(")) AS a(")]
    for line in ("'TICKET ', t.`ticket_id`, ' from ', t.`source`, '\\n',",
                 "'Priority: ', t.`priority`, '\\n',",
                 "'Subject: ', COALESCE(t.`subject`, 'N/A'), '\\n',",
                 "'Description: ', COALESCE(t.`description`, 'N/A')\n"):  # fmt: skip
        assert line in concat, line


# --- lab 5.3: the ticket responder (D-068) -------------------------------------------------


def test_the_responder_answers_an_action_and_a_reason_and_never_a_target():
    """Spec decision 22: two labelled lines. A ticket has no version to roll back to and no
    replica count, so there is no Target line (checked with the other agents' lines)."""
    prompt = read(AGENT_FILES[TICKET_RESPONDER])
    assert "Action: [ROLLBACK | SCALE_OUT | INVESTIGATE_UPSTREAM]\nReason: [one or two sentences]\n" in prompt
    assert "There is no Target line: there is never a version or a replica count to give." in prompt


def test_the_responder_rolls_back_a_release_then_scales_out_a_load_then_looks_upstream():
    """Spec decision 22: the rules in order, each on its cue. Every ticket it reads was already
    classified HIGH, and the classifier's reason comes with it."""
    prompt = read(AGENT_FILES[TICKET_RESPONDER])
    rules = ("1. ROLLBACK — ", "2. SCALE_OUT — ", "3. INVESTIGATE_UPSTREAM — ", "STRICT VALUES")
    rollback, scale_out, upstream, strict = (prompt.index(s) for s in rules)
    assert rollback < scale_out < upstream < strict
    for cue in ("tied to a release", "an update", "a new version"):
        assert cue in prompt[rollback:scale_out], cue
    for cue in ("slow or timing out under heavy load", "a sale", "a campaign", "a peak"):
        assert cue in prompt[scale_out:upstream], cue
    assert "payments fail at a payment provider or a payment gateway" in prompt[upstream:strict]
    assert "stop at the first one that matches" in prompt
    assert "already been classified high severity by the triage agent" in prompt
    assert "Recommend the one action that fixes it fastest." in prompt
    assert "Use only what the ticket and the triage reason say" in prompt[strict:]


def test_the_responder_examples_are_approved_high_tickets_filled_in():
    """One per action, each a really-HIGH P1 template the user approved (issue 03) that expects
    that action, with its placeholders filled from their pools."""
    examples = _ticket_examples(TICKET_RESPONDER)
    assert sorted(action for action, *_ in examples) == sorted(ALLOWED_ACTIONS[TICKET_RESPONDER])
    for action, template, _, _ in examples:
        assert (template.priority, template.true_severity, template.expected_action) == ("P1", HIGH, action)


def test_each_responder_example_carries_a_triage_reason_the_classifier_would_give():
    """The input ends with the classifier's reason, so each example does too: many customers,
    who cannot buy or pay."""
    examples = _ticket_examples(TICKET_RESPONDER)
    assert examples
    for action, _, triage, _ in examples:
        assert triage, f"the {action} example has no Triage Reason line"
        assert "customers" in triage and re.search(r"\b(buy|pay)\b", triage), (action, triage)


# The cue each action's example names in its Reason (spec decision 10): the release, the load,
# the provider.
RESPONDER_CUES = {ROLLBACK: "release", SCALE_OUT: "load", INVESTIGATE_UPSTREAM: "provider"}


def test_each_responder_example_reason_names_its_cue():
    examples = _ticket_examples(TICKET_RESPONDER)
    assert examples
    for action, _, _, reason in examples:
        assert RESPONDER_CUES[action] in reason, (action, reason)


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_allows_exactly_the_answers_the_scorer_accepts(agent):
    """The Action line of a metric agent, the Severity line of the classifier."""
    prompt = read(AGENT_FILES[agent])
    label = OUTPUT_LINES[agent][0].rstrip(":")
    stated = re.search(rf"the {label} line must be exactly ([^.]+)\.", prompt)
    assert stated, "the prompt must state the allowed answers"
    named = {w for w in re.findall(r"\b[A-Z][A-Z_]{2,}\b", stated.group(1))}
    assert named == set(ALLOWED_ANSWERS[agent])


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
    written = "agent_name" in _written_columns(sql)  # ticket_triage has no agent column
    assert sql.count(f"'{agent}'") == 1 + written, "once as the call, and as the agent_name if written"


@pytest.mark.parametrize("name", METRIC_INSERTS)
def test_insert_filters_below_the_lateral_join(name):
    """The agent must be called once per anomalous row, not once per window: the WHERE
    sits in a derived table that is the left input of the LATERAL join. (The classifier
    has no filter: lab 5.1 already kept only the P1s.)"""
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
    for answer in ALLOWED_ANSWERS[agent]:
        assert f"'{answer}'" in sql
    for action in set(ACTIONS) - set(ALLOWED_ANSWERS[agent]):
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


@pytest.mark.parametrize("name", METRIC_INSERTS)
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
def test_inserts_select_exactly_as_many_values_as_the_table_has_columns(name):
    """The INSERTs are positional: a missing or extra value shifts every column."""
    sql = read(name)
    assert len(_select_items(sql)) == len(_written_columns(sql))


@pytest.mark.parametrize("name", METRIC_INSERTS)
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
