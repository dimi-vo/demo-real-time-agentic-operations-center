"""The .avsc copies in schemas/ must not drift from the source-table DDL in sql/.

The DDL files (sql/source_*.sql) are the single source of truth for the schemas.
This test fails if a column is added/removed/retyped in the DDL without the matching
change to the .avsc copy used by --dry-run and tests. It compares field names and
base types (ignoring nullability, which the DDL leaves implicit).

As registered by Confluent Cloud (checked in Phase 3): TIMESTAMP(3) maps to
`local-timestamp-millis`, and an upsert table's PRIMARY KEY columns are in the key
subject only, not in the value.

The two CVE source tables (D-070) copy support_tickets' options, checked here too.

The two ticket output tables (D-068) have no .avsc (Flink writes them), but they copy
support_tickets' columns and options, so their DDL is checked against it here too. So do
section 6's two (D-070): critical_cves copies cve_scans, and patch_decisions carries the
finding, the security agent's decision and the patch agent's answer.
"""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# (DDL, value .avsc, key .avsc or None)
PAIRS = [
    ("sql/source_service_metrics.sql", "schemas/service_metrics.avsc", None),
    ("sql/source_service_deployments.sql", "schemas/service_deployments.avsc", "schemas/service_deployments_key.avsc"),
    ("sql/source_service_incidents.sql", "schemas/service_incidents.avsc", None),
    ("sql/source_support_tickets.sql", "schemas/support_tickets.avsc", None),  # D-068
    ("sql/source_ticket_answer_key.sql", "schemas/ticket_answer_key.avsc", None),
    ("sql/source_cve_scans.sql", "schemas/cve_scans.avsc", None),  # D-070
    ("sql/source_cve_answer_key.sql", "schemas/cve_answer_key.avsc", None),
]
_PRIMARY_KEY = re.compile(r"PRIMARY KEY \(([^)]*)\)")

# A column line looks like:  `name` TYPE COMMENT '...'   (TYPE may be TIMESTAMP(3))
_COLUMN = re.compile(r"^\s*`(\w+)`\s+([A-Za-z]+(?:\(\d+\))?)")
_SKIP = ("WATERMARK", "PRIMARY", "CONSTRAINT")


def _flink_base_type(flink_type: str) -> str:
    t = flink_type.upper()
    if t == "STRING":
        return "string"
    if t == "INT":
        return "int"
    if t == "DOUBLE":
        return "double"
    if t.startswith("TIMESTAMP"):
        return "local-timestamp-millis"
    raise AssertionError(f"unmapped Flink type {flink_type!r}; extend _flink_base_type")


def _ddl_columns(sql: str) -> dict[str, str]:
    cols: dict[str, str] = {}
    for line in sql.splitlines():
        stripped = line.strip()
        if any(stripped.upper().startswith(k) for k in _SKIP):
            continue
        m = _COLUMN.match(line)
        if m:
            cols[m.group(1)] = _flink_base_type(m.group(2))
    return cols


def _avsc_base_type(field_type) -> str:
    if isinstance(field_type, list):  # nullable union ["null", X]
        members = [m for m in field_type if m != "null"]
        field_type = members[0]
    if isinstance(field_type, dict):
        return field_type.get("logicalType") or field_type["type"]
    return field_type


def _avsc_columns(avsc: dict) -> dict[str, str]:
    return {f["name"]: _avsc_base_type(f["type"]) for f in avsc["fields"]}


def _primary_key(sql: str) -> list[str]:
    m = _PRIMARY_KEY.search(sql)
    return [c.strip().strip("`") for c in m.group(1).split(",")] if m else []


@pytest.mark.parametrize(("sql_path", "avsc_path", "key_path"), PAIRS, ids=[p[1] for p in PAIRS])
def test_avsc_matches_ddl(sql_path, avsc_path, key_path):
    sql = (ROOT / sql_path).read_text()
    ddl = _ddl_columns(sql)
    assert ddl, f"no columns parsed from {sql_path}"
    pk = _primary_key(sql)
    value_cols = {k: v for k, v in ddl.items() if k not in pk}
    avsc = _avsc_columns(json.loads((ROOT / avsc_path).read_text()))
    assert value_cols == avsc, f"{avsc_path} has drifted from {sql_path}.\n  DDL:  {value_cols}\n  AVSC: {avsc}"
    if key_path:
        key = _avsc_columns(json.loads((ROOT / key_path).read_text()))
        assert key == {k: ddl[k] for k in pk}, f"{key_path} has drifted from the PRIMARY KEY in {sql_path}"
    else:
        assert not pk, f"{sql_path} has a PRIMARY KEY but no key .avsc is listed"


def test_ddl_parsed_expected_columns():
    metrics = _ddl_columns((ROOT / "sql/source_service_metrics.sql").read_text())
    assert metrics == {  # D-058: one metrics sample per service per second
        "event_time": "local-timestamp-millis",
        "service_name": "string",
        "traffic_count": "int",
        "latency_ms": "double",
        "error_count": "int",
    }
    deps = _ddl_columns((ROOT / "sql/source_service_deployments.sql").read_text())
    assert deps["service_name"] == "string"
    assert "max_replicas" not in deps  # D-043: no autoscaling ceiling
    assert len(deps) == 10


def test_the_ticket_tables_have_the_specs_columns_in_order():
    # D-068, spec decisions 13 and 14.
    tickets = _ddl_columns((ROOT / "sql/source_support_tickets.sql").read_text())
    assert list(tickets.items()) == [
        ("event_time", "local-timestamp-millis"),
        ("ticket_id", "string"),
        ("source", "string"),
        ("service_name", "string"),
        ("priority", "string"),
        ("subject", "string"),
        ("description", "string"),
    ]
    answer_key = _ddl_columns((ROOT / "sql/source_ticket_answer_key.sql").read_text())
    assert list(answer_key.items()) == [
        ("event_time", "local-timestamp-millis"),
        ("ticket_id", "string"),
        ("true_severity", "string"),
        ("expected_action", "string"),
    ]


@pytest.mark.parametrize("table", ["support_tickets", "ticket_answer_key"])
def test_the_ticket_tables_take_service_incidents_options(table):
    # Same WITH options, bucket and watermark as service_incidents; no key columns.
    def tail(name):
        sql = (ROOT / f"sql/source_{name}.sql").read_text()
        return sql[sql.index("  WATERMARK") :]

    assert tail(table) == tail("service_incidents")


def test_the_cve_tables_have_the_specs_columns_in_order():
    # D-070, spec decisions 8 and 9.
    scans = _ddl_columns((ROOT / "sql/source_cve_scans.sql").read_text())
    assert list(scans.items()) == [
        ("event_time", "local-timestamp-millis"),
        ("finding_id", "string"),
        ("repository", "string"),
        ("cve_id", "string"),
        ("package", "string"),
        ("installed_version", "string"),
        ("fixed_version", "string"),
        ("cvss_score", "double"),
        ("finding", "string"),
    ]
    answer_key = _ddl_columns((ROOT / "sql/source_cve_answer_key.sql").read_text())
    assert list(answer_key.items()) == [
        ("event_time", "local-timestamp-millis"),
        ("finding_id", "string"),
        ("expected_action", "string"),
    ]


@pytest.mark.parametrize("table", ["cve_scans", "cve_answer_key"])
def test_the_cve_tables_take_support_tickets_options(table):
    # The same WITH options, bucket and 5 s watermark as support_tickets; no key columns.
    def tail(name):
        sql = (ROOT / f"sql/source_{name}.sql").read_text()
        return sql[sql.index("  WATERMARK") :]

    assert tail(table) == tail("support_tickets")


def test_every_cve_column_has_a_comment_and_the_answer_key_says_who_reads_it():
    for table in ("cve_scans", "cve_answer_key"):
        sql = (ROOT / f"sql/source_{table}.sql").read_text()
        columns = [ln for ln in sql.splitlines() if _COLUMN.match(ln)]
        assert columns and all(" COMMENT '" in ln for ln in columns), table
    answer_key = (ROOT / "sql/source_cve_answer_key.sql").read_text()
    assert "FOR THE WALL AND EVALUATE ONLY" in answer_key
    action = re.search(r"^\s+`expected_action` (.+)$", answer_key, re.MULTILINE).group(1)
    for word in ("PATCH", "DISMISS", "NULL"):
        assert word in action, word


TICKET_COLUMNS = [
    ("event_time", "local-timestamp-millis"),
    ("ticket_id", "string"),
    ("source", "string"),
    ("service_name", "string"),
    ("priority", "string"),
    ("subject", "string"),
    ("description", "string"),
]


def test_the_ticket_output_tables_carry_the_ticket_then_the_verdict():
    # D-068, spec decisions 15 and 16: p1_tickets is the ticket as it came in; ticket_triage
    # adds the classifier's verdict, so the responder reads one table without a join.
    p1 = _ddl_columns((ROOT / "sql/sink_p1_tickets.sql").read_text())
    assert list(p1.items()) == TICKET_COLUMNS
    triage = _ddl_columns((ROOT / "sql/sink_ticket_triage.sql").read_text())
    assert list(triage.items()) == [
        *TICKET_COLUMNS,
        ("severity", "string"),
        ("reason", "string"),
        ("raw_response", "string"),
    ]


@pytest.mark.parametrize("table", ["p1_tickets", "ticket_triage"])
def test_the_ticket_output_tables_take_support_tickets_options(table):
    # The same WITH options, bucket and 5 s watermark as support_tickets: append-only and
    # read-uncommitted, so the classifier never waits about a minute for a commit (D-063).
    def tail(path):
        sql = (ROOT / path).read_text()
        return sql[sql.index("  WATERMARK") :]

    assert tail(f"sql/sink_{table}.sql") == tail("sql/source_support_tickets.sql")


def test_the_verdict_is_never_null_and_names_its_three_values():
    sql = (ROOT / "sql/sink_ticket_triage.sql").read_text()
    line = re.search(r"^\s+`severity` (.+)$", sql, re.MULTILINE).group(1)
    assert line.startswith("STRING NOT NULL COMMENT ")
    for value in ("HIGH", "LOW", "INVALID"):
        assert value in line, value


# Section 6 (D-070, spec decisions 10 and 11): lab 6.1 copies the critical findings, lab 6.4
# writes the security agent's decision with what the patch agent answered.
FINDING_COLUMNS = [
    ("event_time", "local-timestamp-millis"),
    ("finding_id", "string"),
    ("repository", "string"),
    ("cve_id", "string"),
    ("package", "string"),
    ("installed_version", "string"),
    ("fixed_version", "string"),
    ("cvss_score", "double"),
    ("finding", "string"),
]


def _not_null_columns(sql: str) -> list[str]:
    return [m.group(1) for ln in sql.splitlines() if (m := _COLUMN.match(ln)) and " NOT NULL" in ln]


def test_critical_cves_is_the_finding_as_it_came_in():
    sql = (ROOT / "sql/sink_critical_cves.sql").read_text()
    assert list(_ddl_columns(sql).items()) == FINDING_COLUMNS
    assert _not_null_columns(sql) == []
    # The column comments say only lab 6.1's critical findings belong here.
    score = re.search(r"^\s+`cvss_score` (.+)$", sql, re.MULTILINE).group(1)
    assert "9.0" in score and "6.1" in score


def test_patch_decisions_carries_the_finding_then_the_decision_then_the_patch():
    sql = (ROOT / "sql/sink_patch_decisions.sql").read_text()
    assert list(_ddl_columns(sql).items()) == [
        ("event_time", "local-timestamp-millis"),
        ("finding_id", "string"),
        ("repository", "string"),
        ("cve_id", "string"),
        ("cvss_score", "double"),
        ("action", "string"),
        ("reason", "string"),
        ("patch_status", "string"),
        ("commit_sha", "string"),
        ("patch_started_at", "local-timestamp-millis"),
        ("patch_finished_at", "local-timestamp-millis"),
        ("raw_response", "string"),
    ]
    # Lab 6.4 always writes an action and a patch status; everything it parses can be null.
    assert _not_null_columns(sql) == ["action", "patch_status"]
    for column in ("patch_started_at", "patch_finished_at"):
        assert re.search(rf"^\s+`{column}` TIMESTAMP\(3\) COMMENT ", sql, re.MULTILINE), column


@pytest.mark.parametrize(
    ("column", "values"),
    [("action", ("PATCH", "DISMISS", "INVALID")), ("patch_status", ("PATCHED", "FAILED", "N/A"))],
)
def test_the_decision_and_the_patch_status_name_their_three_values(column, values):
    sql = (ROOT / "sql/sink_patch_decisions.sql").read_text()
    line = re.search(rf"^\s+`{column}` (.+)$", sql, re.MULTILINE).group(1)
    assert line.startswith("STRING NOT NULL COMMENT ")
    for value in values:
        assert value in line, value


@pytest.mark.parametrize("table", ["critical_cves", "patch_decisions"])
def test_the_cve_output_tables_take_support_tickets_options(table):
    # The WITH block of p1_tickets, which is support_tickets': append-only, read-uncommitted
    # so the security agent never waits about a minute for a commit (D-063), delete cleanup,
    # 24 h, one bucket, and a 5 s watermark on event_time.
    def tail(path):
        sql = (ROOT / path).read_text()
        return sql[sql.index("  WATERMARK") :]

    assert tail(f"sql/sink_{table}.sql") == tail("sql/sink_p1_tickets.sql")


@pytest.mark.parametrize("table", ["critical_cves", "patch_decisions"])
def test_every_cve_output_column_has_a_comment_without_an_apostrophe(table):
    sql = (ROOT / f"sql/sink_{table}.sql").read_text()
    body = sql[sql.index("CREATE TABLE") :]
    columns = [ln for ln in body.splitlines() if _COLUMN.match(ln)]
    assert columns and all(" COMMENT '" in ln for ln in columns)
    for ln in columns:  # a COMMENT is a quoted string: one apostrophe inside would end it
        assert ln.count("'") == 2, ln
