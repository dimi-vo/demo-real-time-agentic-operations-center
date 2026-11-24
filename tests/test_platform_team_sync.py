"""platform-team/ must work when copied out of the repo, so its developer module
carries its own copies of the ten CREATE TABLE statements (five source tables, five
output tables). This test keeps those copies byte-for-byte (normalized) in step with
the single source of truth in sql/, and the statements chained in the order they are
created in."""

import re
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
    ("service_error_rate", "sql/sink_service_error_rate.sql"),  # D-067
    ("service_health", "sql/sink_service_health.sql"),
    ("remediation_decisions", "sql/sink_remediation_decisions.sql"),
    ("p1_tickets", "sql/sink_p1_tickets.sql"),  # D-068: labs 5.1 and 5.2
    ("ticket_triage", "sql/sink_ticket_triage.sql"),
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


def test_all_ten_tables_are_present():
    assert set(_heredoc_tables()) == {
        "service_metrics",
        "service_deployments",
        "service_incidents",
        "support_tickets",
        "ticket_answer_key",
        "service_error_rate",
        "service_health",
        "remediation_decisions",
        "p1_tickets",
        "ticket_triage",
    }


_RESOURCE = re.compile(r'^resource "confluent_flink_statement" "(\w+)" \{$', re.MULTILINE)
_DEPENDS_ON = re.compile(r"^  depends_on = \[(?:confluent_flink_statement\.)?([\w.]+)\]$", re.MULTILINE)


def test_the_statements_run_one_after_another_sources_then_outputs_in_lab_order_then_the_model():
    """D-037: one chain, each statement waiting for the one before. D-068: the sources first,
    then the output tables in the order the labs fill them, and the model last."""
    text = STATEMENTS_TF.read_text()
    starts = [(m.start(), m.group(1)) for m in _RESOURCE.finditer(text)]
    after = {}
    for (start, name), (end, _) in zip(starts, [*starts[1:], (len(text), "")], strict=True):
        [dep] = _DEPENDS_ON.findall(text[start:end])
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
        "create_service_error_rate_table",
        "create_service_health_table",
        "create_remediation_decisions_table",
        "create_p1_tickets_table",
        "create_ticket_triage_table",
        "llm_textgen_model",
    ]
    assert len(after) == len(starts)  # every statement is on the chain


def test_model_statement_present():
    text = STATEMENTS_TF.read_text()
    assert "CREATE MODEL IF NOT EXISTS" in text
    assert "`llm_textgen_model`" in text
    assert "driver_race_history" not in text  # dropped in this workshop (D-012)
