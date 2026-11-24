"""platform-team/ must work when copied out of the repo, so its developer module
carries its own copies of the eight CREATE TABLE statements (five source tables, three
output tables). This test keeps those copies byte-for-byte (normalized) in step with
the single source of truth in sql/."""

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


def test_all_eight_tables_are_present():
    assert set(_heredoc_tables()) == {
        "service_metrics",
        "service_deployments",
        "service_incidents",
        "support_tickets",
        "ticket_answer_key",
        "service_error_rate",
        "service_health",
        "remediation_decisions",
    }


def test_model_statement_present():
    text = STATEMENTS_TF.read_text()
    assert "CREATE MODEL IF NOT EXISTS" in text
    assert "`llm_textgen_model`" in text
    assert "driver_race_history" not in text  # dropped in this workshop (D-012)
