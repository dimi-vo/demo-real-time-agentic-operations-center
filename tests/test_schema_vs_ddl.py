"""The .avsc copies in schemas/ must not drift from the source-table DDL in sql/.

The DDL files (sql/source_*.sql) are the single source of truth for the schemas.
This test fails if a column is added/removed/retyped in the DDL without the matching
change to the .avsc copy used by --dry-run and tests. It compares field names and
base types (ignoring nullability, which the DDL leaves implicit).

As registered by Confluent Cloud (checked in Phase 3): TIMESTAMP(3) maps to
`local-timestamp-millis`, and an upsert table's PRIMARY KEY columns are in the key
subject only, not in the value.
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
