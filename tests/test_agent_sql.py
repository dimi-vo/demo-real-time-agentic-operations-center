"""The agent files must keep the contract the agents, the tables and the scorer share.

`remediation_decisions` is written by three INSERT statements (section 4) and read by
`uv run evaluate`, so the column list, the allowed actions, the output format and the
routing rules exist in several places at once (two prompts, three INSERTs, one DDL, one
scorer). These tests fail when they drift apart.

Section 5 (D-068) adds the ticket classifier (5.2). Its prompt answers a Severity and a
Reason, with a HIGH, a plain LOW and a shouting LOW example filled in from the approved
ticket templates. Its INSERT writes every P1 ticket's verdict to `ticket_triage`, parsed the
way the 4.3 legs parse an action. The ticket responder (5.3) answers an Action and a Reason,
never a Target, with one example per action, also filled in from the templates. Its INSERT
is a fourth writer of `remediation_decisions`: one decision per HIGH verdict, with the
ticket's id, the trigger `HIGH_SEVERITY_TICKET` and a null target. The rules each INSERT
follows are keyed by its file, and the output lines and allowed answers by its agent.

Section 6 (D-070) adds the security agent (6.3), the only agent with a tool. Its prompt
answers an Action, `PATCH` or `DISMISS`, a Reason and a Patch line. On `PATCH` it calls lab
6.2's `patch_agent_tool` with the request line `scripts.patchagent.reply` parses and copies
the patch agent's reply into the Patch line, which lab 6.4 parses back. Its examples are one
`PATCH` and one `DISMISS` per reason, each an approved critical finding template filled in
with a real advisory. Lab 6.4's INSERT shows the agent each critical finding as those examples
show one and writes its decision to `patch_decisions`: the action, the reason, and the commit
and the patch's start and finish read out of the Patch line with the patch agent's own reply
patterns, then a patch status it works out itself. Its tests run the INSERT's expressions on
answers, with a small evaluator of the SQL it uses.
"""

import itertools
import re
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from opsgen import finding_templates
from opsgen.finding_templates import ARCHIVED, DEV_ONLY, DISMISS, DISMISS_REASONS, NOT_CALLED, PATCH, FindingTemplate
from opsgen.ticket_templates import POOLS, TEMPLATES, Template
from scripts.evaluation.decisions import (
    ACTIONS,
    AGENT_FOR_TRIGGER,
    ALLOWED_ACTIONS,
    CAPACITY_AGENT,
    HIGH_SEVERITY_TICKET,
    INVESTIGATE_UPSTREAM,
    RELEASE_AGENT,
    ROLLBACK,
    SCALE_OUT,
    TICKET_RESPONDER,
)
from scripts.evaluation.tickets import HIGH, LOW
from scripts.patchagent.graph import DEFAULT_MAX_DELAY, DEFAULT_MIN_DELAY
from scripts.patchagent.reply import REPLY_COMMIT, REPLY_FINISHED, REPLY_STARTED, REQUEST, commit_for, format_reply

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

TICKET_CLASSIFIER = "ticket_classifier"  # lab 5.2 (D-068)
SECURITY_AGENT = "security_agent"  # lab 6.3 (D-070)
AGENT_FILES = {
    RELEASE_AGENT: "4.2_release_agent.sql",
    CAPACITY_AGENT: "4.2_capacity_agent.sql",
    TICKET_CLASSIFIER: "5.2_ticket_classifier.sql",
    TICKET_RESPONDER: "5.3_ticket_responder.sql",
    SECURITY_AGENT: "6.3_security_agent.sql",
}
TOOL_FILE = "6.2_patch_agent_tool.sql"  # the connection and the tool the security agent calls
# Each INSERT: the agent it calls and the table it reads. The release agent has two legs:
# error-rate breaches (lab 2.2's table, D-067) and latency anomalies. The classifier reads
# lab 5.1's P1 tickets, and the responder the classifier's verdicts (D-068). The security agent
# reads lab 6.1's critical findings (D-070).
INSERTS = {
    "4.3_insert_release_agent_errors.sql": (RELEASE_AGENT, "service_error_rate"),
    "4.3_insert_release_agent.sql": (RELEASE_AGENT, "service_health"),
    "4.3_insert_capacity_agent.sql": (CAPACITY_AGENT, "service_health"),
    "5.2_insert_ticket_classifier.sql": (TICKET_CLASSIFIER, "p1_tickets"),
    "5.3_insert_ticket_responder.sql": (TICKET_RESPONDER, "ticket_triage"),
    "6.4_insert_security_agent.sql": (SECURITY_AGENT, "critical_cves"),
}
ERRORS_INSERT, LATENCY_INSERT, CAPACITY_INSERT, CLASSIFIER_INSERT, RESPONDER_INSERT, SECURITY_INSERT = INSERTS
# The 4.3 legs: one decision per flagged window, into remediation_decisions.
METRIC_INSERTS = (ERRORS_INSERT, LATENCY_INSERT, CAPACITY_INSERT)
# The INSERTs whose agent sees only some of the rows they read: the 4.3 legs, and the
# responder (HIGH verdicts only). The classifier and the security agent have no filter: labs
# 5.1 and 6.1 kept only the P1s and the critical findings.
FILTERED_INSERTS = (*METRIC_INSERTS, RESPONDER_INSERT)
TABLE_FILE = "sink_remediation_decisions.sql"

# Each agent's answer, line by line: brief section 5.5, cut down in D-034, for the metric
# agents; a severity and a reason for the classifier, and an action and a reason for the
# responder, which never has a target (D-068); an action, a reason and the patch agent's reply
# for the security agent, which lab 6.4 parses (D-070).
OUTPUT_LINES = {
    RELEASE_AGENT: ("Action:", "Target:", "Reason:"),
    CAPACITY_AGENT: ("Action:", "Target:", "Reason:"),
    TICKET_CLASSIFIER: ("Severity:", "Reason:"),
    TICKET_RESPONDER: ("Action:", "Reason:"),
    SECURITY_AGENT: ("Action:", "Reason:", "Patch:"),
}
# What the first line may say: a metric agent's action, the classifier's verdict, or the
# security agent's action, which the CVE answer key also uses.
ALLOWED_ANSWERS = {**ALLOWED_ACTIONS, TICKET_CLASSIFIER: (HIGH, LOW), SECURITY_AGENT: (PATCH, DISMISS)}
# D-034's eight, then the ticket a ticket_responder decision is about (D-068).
DECISION_COLUMNS = (
    "window_end", "service_name", "agent_name", "trigger",
    "action", "target", "reason", "raw_response", "ticket_id",
)  # fmt: skip


def read(name: str) -> str:
    return (SQL / name).read_text()


def _prompt(sql: str) -> str:
    """An agent file's USING PROMPT string, without its quotes. It ends before USING TOOLS, if
    the agent has tools, or before WITH."""
    start = sql.index("USING PROMPT '") + len("USING PROMPT '")
    return sql[start : start + re.search(r"'\n(?:USING TOOLS [^\n]+\n)?WITH \(\n", sql[start:]).start()]


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
    r"(?:Classifier said: (?P<triage>[^\n]+)\n)?Answer:\n"
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
    """(answer, the approved template its ticket fills in, what the classifier said or None, the
    answer's Reason) for each example of a ticket agent."""
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
    """One Reason expression, with two endings: to the end of the answer when Reason is the agent's
    last line, and to the end of its own line when another line follows it, as the security agent's
    Patch line does (D-070)."""

    def reason(name):
        [line] = [line for line in read(name).splitlines() if "REGEXP_EXTRACT" in line and "Reason:" in line]
        return line.strip().rstrip(",")

    to_the_end = r"TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1))"
    to_the_line_end = to_the_end.replace(r"([\s\S]+?)$", r"([^\n]+)")
    found = {}
    for name, (agent, _) in INSERTS.items():
        found.setdefault(OUTPUT_LINES[agent][-1] == "Reason:", set()).add(reason(name))
    assert found == {True: {to_the_end}, False: {to_the_line_end}}


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
    assert "already been classified high severity by the classifier" in prompt
    assert "Recommend the one action that fixes it fastest." in prompt
    assert "Use only what the ticket and the classifier say" in prompt[strict:]


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
        assert triage, f"the {action} example has no Classifier said line"
        assert "customers" in triage and re.search(r"\b(buy|pay)\b", triage), (action, triage)


# The cue each action's example names in its Reason (spec decision 10): the release, the load,
# the provider.
RESPONDER_CUES = {ROLLBACK: "release", SCALE_OUT: "load", INVESTIGATE_UPSTREAM: "provider"}


def test_each_responder_example_reason_names_its_cue():
    examples = _ticket_examples(TICKET_RESPONDER)
    assert examples
    for action, _, _, reason in examples:
        assert RESPONDER_CUES[action] in reason, (action, reason)


def test_the_responder_insert_turns_every_high_verdict_into_a_decision():
    """One statement, from ticket_triage into remediation_decisions. The HIGH filter is here,
    as the error-rate leg's is: 5.2 writes every verdict, and there is no high-severity topic.
    The latest offset and the filter below the join are checked with the other INSERTs."""
    sql = read(RESPONDER_INSERT)
    body = sql[sql.index("INSERT INTO") :]
    assert body.startswith("INSERT INTO `remediation_decisions`\nSELECT\n")
    assert "FROM `ticket_triage` /*+" in body
    assert body.count("WHERE") == 1 and "  WHERE `severity` = 'HIGH'\n) tr,\n" in body
    assert body.count(";") == 1 and body.rstrip().endswith(");")


def test_the_responder_insert_writes_a_ticket_decision():
    """Spec decision 17: the ticket's time in window_end, the responder on the ticket trigger,
    the parsed action, a null target (never a version or a replica count), the reason, the raw
    answer, and the ticket's id last."""
    sql = read(RESPONDER_INSERT)
    written = dict(zip(_written_columns(sql), _select_items(sql), strict=True))
    assert list(written) == list(DECISION_COLUMNS)
    assert written["window_end"] == "tr.`event_time`"
    assert written["service_name"] == "tr.`service_name`"
    assert written["agent_name"] == f"'{TICKET_RESPONDER}'"
    assert written["trigger"] == f"'{HIGH_SEVERITY_TICKET}'"
    assert written["action"].startswith("CASE") and "Action:" in written["action"]
    assert written["target"] == "CAST(NULL AS STRING)"
    assert written["reason"].startswith("TRIM(") and "Reason:" in written["reason"]
    assert written["raw_response"] == "a.`agent_response`"
    assert written["ticket_id"] == "tr.`ticket_id`"


def test_the_responder_parses_its_action_as_the_metric_legs_do():
    """The same agent_status handling and the same regex: its three actions are the only
    values, and anything else is INVALID."""
    capacity = _severity_or_action(read(CAPACITY_INSERT))
    expected = capacity.replace(
        "IN ('INVESTIGATE_UPSTREAM', 'OBSERVE')", "IN ('ROLLBACK', 'SCALE_OUT', 'INVESTIGATE_UPSTREAM')"
    )
    assert expected != capacity
    assert _severity_or_action(read(RESPONDER_INSERT)) == expected


def test_the_responder_input_is_the_ticket_and_the_triage_reason():
    """Spec decision 22: the ticket, labelled as the classifier reads it, then the classifier's
    reason, which shows the chain. The reason is null when the classifier's answer had no
    Reason line, and CONCAT returns null when any argument is null, so it falls back to N/A,
    like the subject and the description."""
    sql = read(RESPONDER_INSERT)
    concat = sql[sql.index("  CONCAT(\n") : sql.index(")) AS a(")]
    lines = ("'TICKET ', tr.`ticket_id`, ' from ', tr.`source`, '\\n',",
             "'Priority: ', tr.`priority`, '\\n',",
             "'Subject: ', COALESCE(tr.`subject`, 'N/A'), '\\n',",
             "'Description: ', COALESCE(tr.`description`, 'N/A'), '\\n',",
             "'Classifier said: ', COALESCE(tr.`reason`, 'N/A')\n")  # fmt: skip
    positions = [concat.index(line) for line in lines]
    assert positions == sorted(positions)


# --- lab 6.3: the security agent (D-070) ---------------------------------------------------


def test_the_security_agent_answers_an_action_a_reason_and_the_patch_agent_reply():
    """Spec decision 16: three labelled lines, plain text. The Patch line holds the patch agent's
    reply as it came, for lab 6.4 to parse, or N/A. Their order and the allowed actions are
    checked with the other agents', from OUTPUT_LINES and ALLOWED_ANSWERS, which lab 6.4's
    tests read too."""
    prompt = _prompt(read(AGENT_FILES[SECURITY_AGENT]))
    assert (
        "Action: [PATCH | DISMISS]\nReason: [one or two sentences]\n"
        "Patch: [the answer from patch_agent_tool, copied exactly | N/A]\n"
    ) in prompt
    assert "nothing before or after" in prompt


def test_the_security_agent_patches_a_shipped_used_package_and_dismisses_the_rest():
    """Spec decision 16: PATCH needs both, a runtime dependency of a deployed service and the
    vulnerable code used; DISMISS takes any of the three reasons the templates carry. Every
    finding it reads is critical (lab 6.1), so the score never decides."""
    prompt = _prompt(read(AGENT_FILES[SECURITY_AGENT]))
    patch, dismiss, strict = (prompt.index(s) for s in ("1. PATCH — ", "2. DISMISS — ", "STRICT VALUES"))
    assert patch < dismiss < strict
    for cue in ("both of these hold", "runtime dependencies of a deployed service", "the vulnerable code is used"):
        assert cue in prompt[patch:dismiss], cue
    for cue in ("any of these holds", "only a development, test or build dependency",
                "archived or not deployed", "no call to the vulnerable code"):  # fmt: skip
        assert cue in prompt[dismiss:strict], cue
    assert "stop at the first one that matches" in prompt
    assert "The CVSS score is always critical here. It never decides on its own." in prompt
    assert "Use only what the finding says" in prompt[strict:]


def test_the_security_agent_calls_lab_62s_tool_and_is_the_only_agent_with_tools():
    """USING TOOLS names the tool lab 6.2 creates, and the prompt names it too, so the model knows
    what to call. No other file in sql/ has USING TOOLS, and no other agent sets max_iterations,
    which bounds tool calls (D-033): the other agents reason over their input alone."""
    tool = re.search(r"^CREATE TOOL `(\w+)`", read(TOOL_FILE), re.MULTILINE).group(1)
    sql = read(AGENT_FILES[SECURITY_AGENT])
    statement = sql[sql.index("\nCREATE AGENT ") + 1 :]
    assert statement.startswith("CREATE AGENT `security_agent`\nUSING MODEL `llm_textgen_model`\nUSING PROMPT '")
    assert f"'\nUSING TOOLS `{tool}`\nWITH (\n" in statement
    assert tool in _prompt(sql)
    with_tools = [p.name for p in sorted(SQL.glob("*.sql")) if re.search(r"^USING TOOLS ", p.read_text(), re.MULTILINE)]
    assert with_tools == [AGENT_FILES[SECURITY_AGENT]]
    for agent, name in AGENT_FILES.items():
        assert ("'max_iterations'" in read(name)) == (agent == SECURITY_AGENT), name


def test_the_security_agent_calls_the_tool_once_on_patch_only_and_copies_its_answer():
    """Spec decision 16: one call, with the request line and nothing else, and its answer copied
    exactly into the Patch line. A failed call says FAILED and why, and a commit is never made up:
    lab 6.4 derives FAILED from a PATCH without a commit. On DISMISS, no call and N/A."""
    prompt = _prompt(read(AGENT_FILES[SECURITY_AGENT]))
    rules = prompt[prompt.index("PATCH AGENT — ") : prompt.index("STRICT VALUES")]
    for wording in ("Use it only for PATCH", "Call patch_agent_tool once, with exactly this line and nothing else",
                    "copy that answer exactly into the Patch line",
                    "If the call fails, write Patch: FAILED and a few words on why.",
                    "Never write a commit the tool did not return.",
                    "For DISMISS, never call the tool, and write Patch: N/A."):  # fmt: skip
        assert wording in rules, wording


# The request line as the prompt states it: PATCH, then the request's fields in angle brackets.
_STATED_REQUEST = re.compile(r"PATCH(?: (?:<\w+>|->))+")


def _stated_request(prompt: str) -> str:
    [stated] = set(_STATED_REQUEST.findall(prompt))
    return stated


def _request_for(stated: str, fields: dict[str, str]) -> str:
    """The stated request line, each placeholder replaced by the field it names."""
    return re.sub(r"<(\w+)>", lambda m: fields[m[1]], stated)


def test_the_stated_request_line_is_the_one_the_patch_agent_parses():
    """The placeholders are REQUEST's fields, in its order, and the line filled in with any
    critical advisory is a request patch-agent parses back into the same values, so the bump and
    the commit in its reply are about the finding (D-070)."""
    stated = _stated_request(_prompt(read(AGENT_FILES[SECURITY_AGENT])))
    assert re.findall(r"<(\w+)>", stated) == sorted(REQUEST.groupindex, key=REQUEST.groupindex.get)
    for advisory in finding_templates.ADVISORIES[True]:
        fields = {"repository": "checkout"} | {
            k: getattr(advisory, k) for k in ("cve_id", "package", "installed_version", "fixed_version")
        }
        request = REQUEST.fullmatch(_request_for(stated, fields))
        assert request and request.groupdict() == fields, advisory.cve_id


# A security agent example shows its finding as lab 6.4's input shows one (issue 07), then, for
# a PATCH, the call to the tool and the patch agent's reply, then the answer.
_EXAMPLE_FINDING = re.compile(
    r"\nFINDING (?P<finding_id>SCAN-\d{6}) in (?P<repository>[\w-]+)\n"
    r"CVE: (?P<cve_id>CVE-\d{4}-\d{4,}) \(CVSS (?P<cvss_score>\d+\.\d)\)\n"
    r"Package: (?P<package>\S+) (?P<installed_version>\S+), fixed in (?P<fixed_version>\S+)\n"
    r"Scanner said: (?P<finding>[^\n]+)\n"
    r"(?:You call patch_agent_tool with: (?P<request>[^\n]+)\npatch_agent_tool replies: (?P<reply>[^\n]+)\n)?"
    r"Answer:\nAction: (?P<action>\w+)\nReason: (?P<reason>[^\n]+)\nPatch: (?P<patch>[^\n]+)\n"
)


def _critical_template(example: dict[str, str]) -> FindingTemplate:
    """The approved critical template an example's scanner sentence fills in: with the advisory its
    CVE line names, the repository its FINDING line names, and a value of each other pool."""
    [advisory] = [a for a in finding_templates.ADVISORIES[True] if a.cve_id == example["cve_id"]]
    assert [advisory.package, advisory.installed_version, advisory.fixed_version] == [
        example["package"], example["installed_version"], example["fixed_version"]
    ], example["cve_id"]  # fmt: skip
    values = {f: getattr(advisory, f) for f in finding_templates.ADVISORY_FIELDS}
    values["repository"] = example["repository"]
    pools = finding_templates.POOLS
    draws = [dict(zip(pools, values_drawn, strict=True)) for values_drawn in itertools.product(*pools.values())]
    [template] = [
        t
        for t in finding_templates.TEMPLATES
        if t.critical
        and example["repository"] in t.repositories
        and any(t.finding.format(**values, **drawn) == example["finding"] for drawn in draws)
    ]
    return template


def _security_examples() -> list[tuple[str, FindingTemplate, dict[str, str | None]]]:
    """(action, the approved template its finding fills in, the example's parts) per example."""
    found = []
    for action, text in _examples(read(AGENT_FILES[SECURITY_AGENT])):
        assert not re.search(r"[{}]|<\w+>", text), f"a placeholder is left in the {action} example"
        example = _EXAMPLE_FINDING.fullmatch(text.rstrip("\n") + "\n")
        assert example, text
        assert example["action"] == action
        found.append((action, _critical_template(example.groupdict()), example.groupdict()))
    return found


def test_the_security_agent_examples_are_approved_critical_findings_filled_in():
    """One PATCH and one DISMISS per reason (spec decision 16), each a critical template the user
    approved (issue 03) that expects the example's action, filled in with a critical advisory, a
    repository the template may name and a value of its pools. The scores are critical and the
    finding ids are distinct."""
    examples = _security_examples()
    expected = [(PATCH, None), *((DISMISS, reason) for reason in DISMISS_REASONS)]
    assert sorted((a, t.dismiss_reason or "") for a, t, _ in examples) == sorted((a, r or "") for a, r in expected)
    for action, template, example in examples:
        assert template.expected_action == action, example["finding"]
        assert 9.0 <= float(example["cvss_score"]) <= 10.0, example["cvss_score"]
    assert len({example["finding_id"] for *_, example in examples}) == len(examples)


def test_the_patch_example_sends_the_request_line_and_copies_a_real_reply():
    """The example's request is the stated line filled in from its finding, which patch-agent
    parses back. The reply is what patch-agent answers that request (its commit, its format, a
    delay within its bounds), and the Patch line is that reply, copied exactly."""
    stated = _stated_request(_prompt(read(AGENT_FILES[SECURITY_AGENT])))
    [example] = [example for action, _, example in _security_examples() if action == PATCH]
    fields = {k: example[k] for k in REQUEST.groupindex}
    assert example["request"] == _request_for(stated, fields)
    assert REQUEST.fullmatch(example["request"]).groupdict() == fields
    reply = example["reply"]
    started, finished = (
        datetime.fromisoformat(pattern.search(reply).group(1).replace("Z", "+00:00"))
        for pattern in (REPLY_STARTED, REPLY_FINISHED)
    )
    assert reply == format_reply(example["request"], started, finished)
    assert DEFAULT_MIN_DELAY <= (finished - started).total_seconds() <= DEFAULT_MAX_DELAY
    assert example["patch"] == reply


def test_the_dismiss_examples_call_nothing_and_have_no_patch():
    examples = [(template, example) for action, template, example in _security_examples() if action == DISMISS]
    assert examples
    for template, example in examples:
        assert example["request"] is None and example["reply"] is None, template.dismiss_reason
        assert example["patch"] == "N/A", template.dismiss_reason


# What each example's Reason names (spec decision 16): where the package sits, and for a PATCH
# that the vulnerable code is reached.
SECURITY_CUES = {
    PATCH: ("deployed", "call path"),
    DEV_ONLY: ("dev dependency",),
    ARCHIVED: ("archived",),
    NOT_CALLED: ("no code path",),
}


def test_each_security_example_reason_names_where_the_package_sits():
    examples = _security_examples()
    assert examples
    for action, template, example in examples:
        for cue in SECURITY_CUES[template.dismiss_reason or action]:
            assert cue in example["reason"], (action, template.dismiss_reason, example["reason"])


def test_the_security_agent_sets_its_own_with_block():
    """Spec decision 16: the other agents' failure handling, max_iterations for a tool call and
    then the answer, and a 60 s request_timeout, longer than the tool's own (lab 6.2)."""
    sql = read(AGENT_FILES[SECURITY_AGENT])
    options = re.findall(r"^  '(\w+)' = '([^']*)'", sql[sql.index("\nWITH (\n") :], re.MULTILINE)
    assert options == [
        ("handle_exception", "continue"),
        ("max_consecutive_failures", "3"),
        ("max_iterations", "3"),
        ("request_timeout", "60"),
    ]
    tool_sql = read(TOOL_FILE)
    tool = dict(re.findall(r"^  '(\w+)' = '([^']*)'", tool_sql[tool_sql.index("CREATE TOOL") :], re.MULTILINE))
    assert int(tool["request_timeout"]) < int(dict(options)["request_timeout"])


# --- lab 6.4: the security agent's INSERT (D-070) ------------------------------------------

# A small evaluator of the SQL lab 6.4's select list and input are made of, so these tests run
# the INSERT's own parsing on an answer rather than read its text. It knows only what the
# INSERT uses, and follows Flink where that matters: a function of a null is null (CONCAT
# included), a comparison with a null is null, a CASE takes only a true WHEN, REGEXP_EXTRACT
# is Java's find (the first match, ASCII classes) and null without one, TRIM trims spaces, and
# TO_TIMESTAMP reads a Java date-time pattern "under the UTC+0 time zone" (Confluent's
# reference) into a naive datetime, null when the string does not parse. A string literal is
# taken as written, doubled quotes aside: '\n' stays a backslash and an n, which a regex reads
# as a newline.
_SQL_TOKEN = re.compile(
    r"\s*(?:(?P<string>'(?:[^']|'')*')|(?P<column>\w+\.`\w+`)|(?P<number>\d+)|(?P<word>\w+)|(?P<symbol>[=(),]))"
)
_JAVA_DATE_FIELDS = {"yyyy": "%Y", "MM": "%m", "dd": "%d", "HH": "%H", "mm": "%M", "ss": "%S", "SSS": "%f"}


def _regexp_extract(text: str | None, pattern: str, group: int) -> str | None:
    match = None if text is None else re.search(pattern, text, re.ASCII)
    return None if match is None else match.group(group)


def _to_timestamp(text: str | None, pattern: str) -> datetime | None:
    if text is None:
        return None
    parts = re.findall(r"'[^']*'|yyyy|MM|dd|HH|mm|ss|SSS|[^'A-Za-z]+", pattern)
    assert "".join(parts) == pattern, f"a pattern letter this evaluator does not know: {pattern}"
    python = "".join(part[1:-1] if part.startswith("'") else _JAVA_DATE_FIELDS.get(part, part) for part in parts)
    try:
        return datetime.strptime(text, python)
    except (ValueError, re.error):
        return None


_SQL_FUNCTIONS = {
    "CONCAT": lambda *args: None if None in args else "".join(args),
    "COALESCE": lambda *args: next((arg for arg in args if arg is not None), None),
    "UPPER": lambda text: None if text is None else text.upper(),
    "TRIM": lambda text: None if text is None else text.strip(" "),
    "REGEXP_EXTRACT": _regexp_extract,
    "TO_TIMESTAMP": _to_timestamp,
}


class _Evaluation:
    """One SQL expression's value, with the columns it reads in `env` (f.`finding_id`, ...)."""

    def __init__(self, text: str, env: dict[str, object]):
        self.tokens, self.pos, self.env, text = [], 0, env, text.strip()
        while self.pos < len(text):
            token = _SQL_TOKEN.match(text, self.pos)
            assert token, f"SQL this evaluator cannot read: {text[self.pos :]!r}"
            self.tokens.append((token.lastgroup, token[token.lastgroup]))
            self.pos = token.end()
        self.pos = 0

    def value(self) -> object:
        value = self.condition()
        assert self.pos == len(self.tokens), self.tokens[self.pos :]
        return value

    def at(self, *words: str) -> bool:
        return self.pos < len(self.tokens) and self.tokens[self.pos][1].upper() in words

    def take(self, *words: str) -> tuple[str, str]:
        kind, text = self.tokens[self.pos]
        assert not words or (kind in ("word", "symbol") and text.upper() in words), (text, words)
        self.pos += 1
        return kind, text

    def condition(self) -> object:
        """Comparisons joined by AND, in SQL's three values: false wins, then null."""
        value = self.comparison()
        while self.at("AND"):
            self.take()
            both = (value, self.comparison())
            value = False if False in both else None if None in both else True
        return value

    def comparison(self) -> object:
        left = self.operand()
        if self.at("="):
            self.take()
            right = self.operand()
            return None if left is None or right is None else left == right
        if self.at("IN"):
            self.take()
            self.take("(")
            options = [self.operand()]
            while self.at(","):
                self.take()
                options.append(self.operand())
            self.take(")")
            return None if left is None else left in options
        if self.at("IS"):
            self.take()
            negated = self.at("NOT")
            if negated:
                self.take()
            self.take("NULL")
            return (left is None) != negated
        return left

    def operand(self) -> object:
        kind, text = self.take()
        if kind == "string":
            return text[1:-1].replace("''", "'")
        if kind == "number":
            return int(text)
        if kind == "column":
            return self.env[text]
        if text.upper() == "CASE":
            return self.case()
        self.take("(")
        if text.upper() == "CAST":
            value = self.condition()
            self.take("AS")
            self.take("STRING")
            self.take(")")
            # Java prints a double of this range as Python does: 9.0, 9.8, 10.0.
            return None if value is None else repr(value) if isinstance(value, float) else str(value)
        args = [self.condition()]
        while self.at(","):
            self.take()
            args.append(self.condition())
        self.take(")")
        return _SQL_FUNCTIONS[text.upper()](*args)

    def case(self) -> object:
        chosen = []
        while self.at("WHEN"):
            self.take()
            when = self.condition()
            self.take("THEN")
            then = self.condition()
            if when is True:
                chosen.append(then)
        if self.at("ELSE"):
            self.take()
            chosen.append(self.condition())
        self.take("END")
        return chosen[0] if chosen else None


def _flink(expression: str, env: dict[str, object]) -> object:
    return _Evaluation(expression, env).value()


def _columns(table: str) -> list[str]:
    return re.findall(r"^\s+`(\w+)`", read(f"sink_{table}.sql"), re.MULTILINE)


def _security_items() -> dict[str, str]:
    """patch_decisions' columns, in its DDL's order, each with the expression lab 6.4 writes to it."""
    sql = read(SECURITY_INSERT)
    return dict(zip(_written_columns(sql), _select_items(sql), strict=True))


def _agent_input(sql: str) -> str:
    """The expression an INSERT hands AI_RUN_AGENT after the agent's name: what the agent reads."""
    call = sql[sql.index("AI_RUN_AGENT(") : sql.index(")) AS a(")]
    return call[call.index("',") + 2 :]


# A critical finding as lab 6.4 reads it from critical_cves: the patch agent's own example. Its
# event_time is a naive datetime holding UTC, as a TIMESTAMP(3) is decoded (D-048).
_FINDING = {
    "event_time": datetime(2026, 12, 8, 11, 42, 1, 500_000),
    "finding_id": "SCAN-000123",
    "repository": "checkout",
    "cve_id": "CVE-2026-41234",
    "package": "urllib3",
    "installed_version": "1.26.4",
    "fixed_version": "1.26.18",
    "cvss_score": 9.8,
    "finding": "The deployed checkout service loads urllib3 1.26.4 at runtime, and the scanner found calls to it.",
}
_REQUEST = "PATCH checkout CVE-2026-41234 urllib3 1.26.4 -> 1.26.18"
_STARTED = datetime(2026, 12, 8, 11, 42, 3, 120_000, tzinfo=timezone.utc)
_FINISHED = datetime(2026, 12, 8, 11, 42, 9, 870_000, tzinfo=timezone.utc)
_REPLY = format_reply(_REQUEST, _STARTED, _FINISHED)
_REASON = "urllib3 ships in the deployed checkout service, and the scanner found calls to the vulnerable code."
# What issue 02 saw when patch-agent was stopped: Flink could not fetch its agent card.
_A2A_ERROR = (
    "Exception while initializing A2A client for agent: `env-000000`.`lkc-000000`.`security_agent` with URL: "
    "https://patch-agent.example.com. ERROR: Failed to obtain agent card: 502"
)


def _naive_utc(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def _decision(answer: str | None, status: str = "SUCCESS", finding: dict | None = None) -> dict[str, object]:
    """The patch_decisions row lab 6.4 writes for a critical finding and the agent's answer."""
    env = {f"f.`{column}`": value for column, value in (finding or _FINDING).items()}
    env |= {"a.`agent_status`": status, "a.`agent_response`": answer}
    return {column: _flink(expression, env) for column, expression in _security_items().items()}


def test_the_security_insert_writes_a_decision_for_every_critical_finding():
    """One statement, from critical_cves into patch_decisions, with no filter: lab 6.1 kept only the
    critical findings, and each one is decided and written, DISMISS ones included. The latest
    offset is checked with the other INSERTs."""
    sql = read(SECURITY_INSERT)
    body = sql[sql.index("INSERT INTO") :]
    assert body.startswith("INSERT INTO `patch_decisions`\nSELECT\n")
    assert "FROM `critical_cves` /*+" in body and "WHERE" not in body
    assert body.count(";") == 1 and body.rstrip().endswith(");")


def test_the_security_insert_writes_the_patch_decisions_columns_in_order():
    """Spec decision 11: the finding's five columns as they came, then the decision, the patch and
    the raw answer, in patch_decisions' DDL order."""
    written = _security_items()
    assert list(written) == [
        "event_time", "finding_id", "repository", "cve_id", "cvss_score",
        "action", "reason", "patch_status", "commit_sha", "patch_started_at", "patch_finished_at",
        "raw_response",
    ]  # fmt: skip
    copied = list(written)[:5]
    assert [written[column] for column in copied] == [f"f.`{column}`" for column in copied]
    assert written["action"].startswith("CASE") and written["patch_status"].startswith("CASE")
    assert written["reason"].startswith("TRIM(REGEXP_EXTRACT(")
    assert written["commit_sha"].startswith("REGEXP_EXTRACT(")
    for column in ("patch_started_at", "patch_finished_at"):
        assert re.match(r"TO_TIMESTAMP\(\s+REGEXP_EXTRACT\(", written[column]), column
    assert written["raw_response"] == "a.`agent_response`"
    decision = _decision(f"Action: PATCH\nReason: {_REASON}\nPatch: {_REPLY}")
    assert {column: decision[column] for column in copied} == {column: _FINDING[column] for column in copied}
    assert list(_FINDING) == _columns("critical_cves")


def test_the_security_insert_parses_its_action_as_the_metric_legs_do():
    """The same agent_status handling and the same regex: PATCH and DISMISS are the only values,
    and anything else is INVALID."""
    capacity = _severity_or_action(read(CAPACITY_INSERT))
    expected = capacity.replace("IN ('INVESTIGATE_UPSTREAM', 'OBSERVE')", "IN ('PATCH', 'DISMISS')")
    assert expected != capacity
    assert _severity_or_action(read(SECURITY_INSERT)) == expected


def _patterns(expression: str) -> list[str]:
    """The regexes an expression hands REGEXP_EXTRACT, as Flink gets them."""
    found = re.findall(r"REGEXP_EXTRACT\(a\.`agent_response`, '((?:[^']|'')*)', 1\)", expression)
    return [pattern.replace("''", "'") for pattern in found]


def test_the_security_insert_reads_the_lines_the_security_agent_declares():
    """Action and Reason are found by their labels, as in the other INSERTs. The commit and the
    patch's start and finish are found on the Patch line, by the patch agent's own reply patterns
    (scripts/patchagent/reply.py), the first match on that line, so the reply and the parsing
    cannot drift apart."""

    def label(line: str) -> str:
        return r"\*{0,2}" + line + r"\*{0,2}"

    action, reason, patch = OUTPUT_LINES[SECURITY_AGENT]
    written = _security_items()
    assert _patterns(written["action"]) == [label(action) + r"\s*([^\n]+)"] * 2
    assert _patterns(written["reason"]) == [label(reason) + r"\s*([^\n]+)"]
    replies = {"commit_sha": REPLY_COMMIT, "patch_started_at": REPLY_STARTED, "patch_finished_at": REPLY_FINISHED}
    for column, reply in replies.items():
        assert _patterns(written[column]) == [label(patch) + r"[^\n]*?" + reply.pattern], column


@pytest.mark.parametrize(
    "request_text",
    [_REQUEST, "please bump urllib3 in checkout to 1.26.18"],
    ids=["parsed request", "unparsed request"],
)
def test_the_insert_reads_the_commit_and_both_times_out_of_any_reply(request_text):
    """The reply contract (D-070): patch-agent's reply, to a request it parsed or to one it echoes,
    copied into the Patch line, gives back its commit and the patch's start and finish. The times
    come back as UTC whatever zone patch-agent ran in, as event_time holds UTC (D-048), so the
    wall can subtract one from the other."""
    cet = timezone(timedelta(hours=1))
    reply = format_reply(request_text, _STARTED.astimezone(cet), _FINISHED.astimezone(cet))
    assert reply.startswith("Patched (") == (request_text != _REQUEST)
    decision = _decision(f"Action: PATCH\nReason: {_REASON}\nPatch: {reply}")
    assert decision["commit_sha"] == commit_for(request_text)
    assert decision["patch_started_at"] == _naive_utc(_STARTED)
    assert decision["patch_finished_at"] == _naive_utc(_FINISHED)
    assert decision["patch_started_at"] - decision["event_time"] == timedelta(seconds=1.62)
    assert decision["patch_status"] == "PATCHED"


@pytest.mark.parametrize(
    ("status", "answer", "action", "patch_status", "commit"),
    [
        pytest.param("SUCCESS", f"Action: PATCH\nReason: {_REASON}\nPatch: {_REPLY}",
                     PATCH, "PATCHED", commit_for(_REQUEST), id="a patch with its commit"),
        pytest.param("SUCCESS", f"**Action:** PATCH\n**Reason:** {_REASON}\n**Patch:** {_REPLY}",
                     PATCH, "PATCHED", commit_for(_REQUEST), id="in markdown bold"),
        pytest.param("SUCCESS", f"You call patch_agent_tool with: {_REQUEST}\npatch_agent_tool replies: {_REPLY}\n"
                     f"Answer:\nAction: PATCH\nReason: {_REASON}\nPatch: {_REPLY}",
                     PATCH, "PATCHED", commit_for(_REQUEST), id="after the example's tool lines"),
        pytest.param("SUCCESS", f"Action: PATCH\nReason: {_REASON}\nPatch: FAILED, the patch agent did not answer.",
                     PATCH, "FAILED", None, id="a failed call, said"),
        pytest.param("SUCCESS", f"Action: PATCH\nReason: {_REASON}\nPatch: {_REPLY.split(' in commit ')[0]}",
                     PATCH, "FAILED", None, id="a reply copied without its commit"),
        pytest.param("SUCCESS", f"Action: PATCH\nReason: {_REASON}",
                     PATCH, "FAILED", None, id="no Patch line"),
        pytest.param("SUCCESS", f"Action: DISMISS\nReason: {_REASON}\nPatch: N/A",
                     DISMISS, "N/A", None, id="a dismissal"),
        pytest.param("SUCCESS", f"Action: UPGRADE\nReason: {_REASON}\nPatch: {_REPLY}",
                     "INVALID", "N/A", commit_for(_REQUEST), id="an action out of the set"),
        pytest.param("SUCCESS", _A2A_ERROR, "INVALID", "N/A", None, id="the patch agent stopped"),
        pytest.param("FAILED", _A2A_ERROR, "INVALID", "N/A", None, id="a failed agent call"),
        pytest.param("FAILED", f"Action: PATCH\nReason: {_REASON}\nPatch: FAILED",
                     "INVALID", "N/A", None, id="a failed agent call that says PATCH"),
    ],
)  # fmt: skip
def test_patch_status_is_patched_failed_or_not_applicable(status, answer, action, patch_status, commit):
    """Spec decision 17: PATCHED for a PATCH whose Patch line holds a commit, FAILED for a PATCH
    without one, N/A otherwise, worked out by the INSERT and never by the agent. The commit and the
    times say what the Patch line holds, whatever the action; null when they do not parse."""
    decision = _decision(answer, status)
    assert (decision["action"], decision["patch_status"], decision["commit_sha"]) == (action, patch_status, commit)
    times = (decision["patch_started_at"], decision["patch_finished_at"])
    assert times == ((_naive_utc(_STARTED), _naive_utc(_FINISHED)) if commit else (None, None))
    assert decision["raw_response"] == answer


def _finding_of(example: dict[str, str | None]) -> dict[str, object]:
    """The critical_cves row a security agent example shows."""
    return _FINDING | {k: example[k] for k in _FINDING if k in example} | {"cvss_score": float(example["cvss_score"])}


def test_the_security_input_shows_a_finding_as_the_examples_do():
    """Spec decision 17: built from each of lab 6.3's examples, the input is that example's four
    lines, word for word, so the agent reads a finding as it was shown one. The lines are joined by
    a backslash and an n, as in the other INSERTs."""
    sql = read(SECURITY_INSERT)
    examples = _examples(read(AGENT_FILES[SECURITY_AGENT]))
    assert examples
    for action, text in examples:
        example = _EXAMPLE_FINDING.fullmatch(text.rstrip("\n") + "\n").groupdict()
        env = {f"f.`{column}`": value for column, value in _finding_of(example).items()}
        assert _flink(_agent_input(sql), env).split("\\n") == text.strip("\n").split("\n")[:4], action


@pytest.mark.parametrize("column", [c for c in _FINDING if c not in ("event_time", "cvss_score")])
def test_a_null_column_still_gives_the_security_agent_a_finding(column):
    """CONCAT returns null when any argument is null, so each text column falls back to N/A. The
    score needs none: lab 6.1 copies only scores of 9.0 and above, never a null one."""
    sql = read(SECURITY_INSERT)
    env = {f"f.`{c}`": None if c == column else value for c, value in _FINDING.items()}
    text = _flink(_agent_input(sql), env)
    assert text is not None and "N/A" in text, column
    assert "CAST(f.`cvss_score` AS STRING)" in sql and "WHERE `cvss_score` >= 9.0;" in read("6.1_critical_cves.sql")


def test_the_insert_reads_back_every_answer_the_security_agent_is_shown():
    """Each of lab 6.3's example answers, under its own finding, gives back its action and its
    reason. The PATCH example's Patch line gives the commit patch-agent derives from its request and
    the times of its reply; the DISMISS ones, no patch."""
    examples = _security_examples()
    assert examples
    for action, _, example in examples:
        answer = f"Action: {example['action']}\nReason: {example['reason']}\nPatch: {example['patch']}"
        decision = _decision(answer, finding=_finding_of(example))
        assert (decision["action"], decision["reason"]) == (action, example["reason"])
        patch = [decision[c] for c in ("patch_status", "commit_sha", "patch_started_at", "patch_finished_at")]
        if action == PATCH:
            times = [pattern.search(example["reply"]).group(1) for pattern in (REPLY_STARTED, REPLY_FINISHED)]
            utc = [_naive_utc(datetime.fromisoformat(t.replace("Z", "+00:00"))) for t in times]
            assert patch == ["PATCHED", commit_for(example["request"]), *utc]
        else:
            assert patch == ["N/A", None, None, None], example["finding_id"]


def test_the_security_insert_header_says_what_it_costs_and_how_its_times_compare():
    """The header: the latest offset (every row is a model call, and on PATCH a call to the laptop),
    patch_status worked out here, two runs writing every decision twice, and the time zones
    checked: event_time and TO_TIMESTAMP both hold UTC."""
    sql = read(SECURITY_INSERT)
    header = " ".join(line.removeprefix("--").strip() for line in sql[: sql.index("INSERT INTO")].splitlines())
    assert header.startswith("6.4 ")
    for words in ("from the latest offset", "the patch agent on your laptop", "patch_status is worked out here",
                  "writes every decision twice", "TO_TIMESTAMP", "UTC+0", "event_time"):  # fmt: skip
        assert words in header, words


@pytest.mark.parametrize("name", INSERTS)
def test_no_input_label_holds_a_label_the_answer_is_parsed_by(name):
    """Each INSERT parses the answer with REGEXP_EXTRACT on its labels (`Action:`, `Reason:`, ...),
    which takes the first match. An input label that holds one, as `Triage Reason:` holds
    `Reason:`, would be parsed instead whenever the model echoes its input."""
    sql = read(name)
    concat = sql[sql.index("AI_RUN_AGENT(") : sql.index(")) AS a(")]
    labels = re.findall(r"[A-Z][A-Za-z ]*:", " ".join(re.findall(r"'([^']*)'", concat)))
    assert labels, name
    parsed = {line for lines in OUTPUT_LINES.values() for line in lines}
    assert [(label, line) for label in labels for line in parsed if line in label] == [], name


def test_only_the_responder_insert_writes_the_ticket_trigger():
    """HIGH_SEVERITY_TICKET comes from lab 5.3 alone, and the responder writes no other
    trigger: `evaluate` and sql/checks/remediation_routing.sql hold every decision to that."""
    for name in INSERTS:
        assert (HIGH_SEVERITY_TICKET in read(name)) == (name == RESPONDER_INSERT), name
    sql = read(RESPONDER_INSERT)
    assert [t for t in AGENT_FOR_TRIGGER if f"'{t}'" in sql] == [HIGH_SEVERITY_TICKET]
    assert AGENT_FOR_TRIGGER[HIGH_SEVERITY_TICKET] == TICKET_RESPONDER


@pytest.mark.parametrize("agent", sorted(AGENT_FILES))
def test_prompt_allows_exactly_the_answers_the_scorer_accepts(agent):
    """The Action line of a metric agent or the responder, the Severity line of the classifier."""
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
    assert "'" not in _prompt(sql), "an apostrophe would close the prompt literal early"
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


@pytest.mark.parametrize("name", FILTERED_INSERTS)
def test_insert_filters_below_the_lateral_join(name):
    """The agent must be called once per anomalous row (or HIGH verdict), not once per
    window (or ticket): the WHERE sits in a derived table that is the left input of the
    LATERAL join. (The classifier and the security agent have no filter: labs 5.1 and 6.1 already
    kept only the P1s and the critical findings.)"""
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
