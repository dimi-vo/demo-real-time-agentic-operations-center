"""platform-team/ must work when copied out of the repo, so its developer module
carries its own copies of the fourteen CREATE TABLE statements (seven source tables, seven
output tables) and of the labs' twelve statements (D-069). This test keeps those copies in
step with the single source of truth in sql/ (the tables with whitespace normalized, the
labs character for character), the labs' statements behind deploy_labs, the statements
chained in the order they are created in, and the Real-Time Context Engine on
patch_decisions once its table exists (D-070)."""

import re
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STATEMENTS_TF = ROOT / "platform-team/modules/developer/statements.tf"

# (table, the sql/ file its CREATE TABLE is the source of truth for)
COPIED_TABLES = (
    ("service_metrics", "sql/source_service_metrics.sql"),
    ("service_deployments", "sql/source_service_deployments.sql"),
    ("service_incidents", "sql/source_service_incidents.sql"),
    ("support_tickets", "sql/source_support_tickets.sql"),  # D-068
    ("ticket_answer_key", "sql/source_ticket_answer_key.sql"),
    ("cve_scans", "sql/source_cve_scans.sql"),  # D-070
    ("cve_answer_key", "sql/source_cve_answer_key.sql"),
    ("service_error_rate", "sql/sink_service_error_rate.sql"),  # D-067
    ("service_health", "sql/sink_service_health.sql"),
    ("remediation_decisions", "sql/sink_remediation_decisions.sql"),
    ("p1_tickets", "sql/sink_p1_tickets.sql"),  # D-068: labs 5.1 and 5.2
    ("ticket_triage", "sql/sink_ticket_triage.sql"),
    ("critical_cves", "sql/sink_critical_cves.sql"),  # D-070: labs 6.1 and 6.4
    ("patch_decisions", "sql/sink_patch_decisions.sql"),
)

# (resource, the sql/ file its statement is copied from), in lab order (D-069).
COPIED_LAB_STATEMENTS = (
    ("insert_service_error_rate", "sql/2.2_service_error_rate.sql"),
    ("insert_service_health", "sql/2.5_service_health.sql"),
    ("create_release_agent", "sql/4.2_release_agent.sql"),
    ("create_capacity_agent", "sql/4.2_capacity_agent.sql"),
    ("insert_release_agent_errors", "sql/4.3_insert_release_agent_errors.sql"),
    ("insert_release_agent", "sql/4.3_insert_release_agent.sql"),
    ("insert_capacity_agent", "sql/4.3_insert_capacity_agent.sql"),
    ("insert_p1_tickets", "sql/5.1_p1_tickets.sql"),
    ("create_ticket_classifier", "sql/5.2_ticket_classifier.sql"),
    ("insert_ticket_classifier", "sql/5.2_insert_ticket_classifier.sql"),
    ("create_ticket_responder", "sql/5.3_ticket_responder.sql"),
    ("insert_ticket_responder", "sql/5.3_insert_ticket_responder.sql"),
)

_HEREDOC = re.compile(r"<<-(\w+)\n(.*?)\n\s*\1$", re.DOTALL | re.MULTILINE)
_CREATE_TABLE = re.compile(r"CREATE TABLE (?:IF NOT EXISTS )?`(\w+)`")


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


def _create_statement_from_sql(path: Path) -> str:
    """The CREATE TABLE statement from a sql/ file (drops the leading -- comments)."""
    text = path.read_text()
    # The statement starts on a line of its own; a comment may mention CREATE TABLE earlier.
    match = re.search(r"^CREATE TABLE ", text, re.MULTILINE)
    assert match, f"no CREATE TABLE statement in {path.name}"
    return text[match.start() :]


def _heredoc_tables() -> dict[str, str]:
    """Every heredoc CREATE TABLE statement in statements.tf, keyed by table name."""
    tables = {}
    for _, body in _HEREDOC.findall(STATEMENTS_TF.read_text()):
        m = _CREATE_TABLE.search(body)
        if m:
            tables[m.group(1)] = body
    return tables


@pytest.mark.parametrize(("table", "sql_path"), COPIED_TABLES, ids=[t for t, _ in COPIED_TABLES])
def test_statements_tf_matches_sql_source(table, sql_path):
    heredocs = _heredoc_tables()
    assert table in heredocs, f"no CREATE TABLE `{table}` heredoc in {STATEMENTS_TF.relative_to(ROOT)}"
    source = _create_statement_from_sql(ROOT / sql_path)
    assert _normalize(heredocs[table]) == _normalize(source), (
        f"CREATE TABLE `{table}` in {STATEMENTS_TF.relative_to(ROOT)} has drifted from {sql_path}. "
        f"Copy the statement from {sql_path} verbatim (keeping IF NOT EXISTS)."
    )


def test_all_fourteen_tables_are_present():
    assert set(_heredoc_tables()) == {
        "service_metrics",
        "service_deployments",
        "service_incidents",
        "support_tickets",
        "ticket_answer_key",
        "cve_scans",
        "cve_answer_key",
        "service_error_rate",
        "service_health",
        "remediation_decisions",
        "p1_tickets",
        "ticket_triage",
        "critical_cves",
        "patch_decisions",
    }


_RESOURCE = re.compile(r'^resource "(\w+)" "(\w+)" \{$', re.MULTILINE)
_DEPENDS_ON = re.compile(r"^  depends_on = \[(?:confluent_flink_statement\.)?([\w.]+)\]$", re.MULTILINE)


def _bodies(resource_type: str) -> dict[str, str]:
    """The text of every resource of this type in statements.tf, by name, each up to the next
    resource of any type."""
    text = STATEMENTS_TF.read_text()
    starts = [(m.start(), m.group(1), m.group(2)) for m in _RESOURCE.finditer(text)]
    ends = [start for start, _, _ in starts[1:]] + [len(text)]
    spans = zip(starts, ends, strict=True)
    return {name: text[start:end] for (start, type_, name), end in spans if type_ == resource_type}


def _resource_bodies() -> dict[str, str]:
    """The text of every confluent_flink_statement resource in statements.tf, by name."""
    return _bodies("confluent_flink_statement")


def _lab_statement_from_sql(path: Path) -> str:
    """The statement in a lab's sql/ file: from its first line (INSERT INTO or CREATE AGENT,
    after the leading -- comments) to the end of the file."""
    text = path.read_text()
    match = re.search(r"^(?:INSERT INTO|CREATE AGENT) ", text, re.MULTILINE)
    assert match, f"no INSERT INTO or CREATE AGENT statement in {path.name}"
    return text[match.start() :]


@pytest.mark.parametrize(("resource", "sql_path"), COPIED_LAB_STATEMENTS, ids=[r for r, _ in COPIED_LAB_STATEMENTS])
def test_each_lab_statement_is_an_exact_copy_of_its_sql_file(resource, sql_path):
    """Exact, not normalized: the agents' prompts are multi-line strings, so a changed line
    break or indent would change what the model reads. An indented heredoc (<<-) drops the
    smallest indent of its non-blank lines, as textwrap.dedent does."""
    body = _resource_bodies().get(resource)
    assert body, f'no resource "confluent_flink_statement" "{resource}" in {STATEMENTS_TF.relative_to(ROOT)}'
    [(_, heredoc)] = _HEREDOC.findall(body)
    assert textwrap.dedent(heredoc + "\n") == _lab_statement_from_sql(ROOT / sql_path), (
        f"{resource} in {STATEMENTS_TF.relative_to(ROOT)} has drifted from {sql_path}. Copy the "
        f"statement from {sql_path} verbatim, from its first line, indented by four spaces."
    )


@pytest.mark.parametrize("resource", [r for r, _ in COPIED_LAB_STATEMENTS])
def test_each_lab_statement_runs_only_with_deploy_labs(resource):
    """D-069: attendees write these statements themselves, so a workshop apply creates none."""
    assert "\n  count = var.deploy_labs ? 1 : 0\n" in _resource_bodies()[resource]


def test_only_the_lab_statements_are_behind_deploy_labs():
    behind = {name for name, body in _resource_bodies().items() if "var.deploy_labs" in body}
    assert behind == {r for r, _ in COPIED_LAB_STATEMENTS}


def test_the_statements_run_one_after_another_sources_then_outputs_then_the_model_then_the_labs():
    """D-037: one chain, each statement waiting for the one before. D-068: the sources first
    (D-070's two CVE sources after the tickets'), then the output tables in the order the labs
    fill them (D-070's two after the tickets'), and the model. D-069: then the labs'
    statements, in lab order."""
    bodies = _resource_bodies()
    after = {}
    for name, body in bodies.items():
        [dep] = _DEPENDS_ON.findall(body)
        after[dep] = name
    chain, step = [], "time_sleep.cluster_settle"
    while step in after:
        step = after[step]
        chain.append(step)
    assert chain == [
        "create_service_metrics_table",
        "create_service_deployments_table",
        "create_service_incidents_table",
        "create_support_tickets_table",
        "create_ticket_answer_key_table",
        "create_cve_scans_table",
        "create_cve_answer_key_table",
        "create_service_error_rate_table",
        "create_service_health_table",
        "create_remediation_decisions_table",
        "create_p1_tickets_table",
        "create_ticket_triage_table",
        "create_critical_cves_table",
        "create_patch_decisions_table",
        "llm_textgen_model",
        *(resource for resource, _ in COPIED_LAB_STATEMENTS),
    ]
    assert len(after) == len(bodies)  # every statement is on the chain


def test_rtce_is_on_for_patch_decisions_once_its_table_exists():
    """D-070, spec decision 12: Terraform turns the Real-Time Context Engine on for
    patch_decisions' topic, and only after the CREATE TABLE that makes the topic. It hangs off
    the chain rather than joining it: nothing waits for it (the chain test above would break)."""
    rtce = _bodies("confluent_rtce_topic")
    assert list(rtce) == ["patch_decisions"]
    body = rtce["patch_decisions"]
    assert _DEPENDS_ON.findall(body) == ["create_patch_decisions_table"]
    assert re.search(r'^  topic_name += "patch_decisions"$', body, re.MULTILINE)


def test_model_statement_present():
    text = STATEMENTS_TF.read_text()
    assert "CREATE MODEL IF NOT EXISTS" in text
    assert "`llm_textgen_model`" in text
    assert "driver_race_history" not in text  # dropped in this workshop (D-012)
