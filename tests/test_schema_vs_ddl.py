"""The .avsc copies in schemas/ must not drift from the source-table DDL in sql/.

The DDL files (sql/source_*.sql) are the single source of truth for the schemas.
This test fails if a column is added/removed/retyped in the DDL without the matching
change to the .avsc copy used by --dry-run and tests. It compares field names and
base types (ignoring nullability, which the DDL leaves implicit).
"""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

PAIRS = [
    ("sql/source_service_requests.sql", "schemas/service_requests.avsc"),
    ("sql/source_service_deployments.sql", "schemas/service_deployments.avsc"),
]

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
        return "timestamp-millis"
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


@pytest.mark.parametrize(("sql_path", "avsc_path"), PAIRS, ids=[p[1] for p in PAIRS])
def test_avsc_matches_ddl(sql_path, avsc_path):
    ddl = _ddl_columns((ROOT / sql_path).read_text())
    avsc = _avsc_columns(json.loads((ROOT / avsc_path).read_text()))
    assert ddl, f"no columns parsed from {sql_path}"
    assert ddl == avsc, (
        f"{avsc_path} has drifted from {sql_path}.\n"
        f"  DDL:  {ddl}\n"
        f"  AVSC: {avsc}"
    )


def test_ddl_parsed_expected_columns():
    reqs = _ddl_columns((ROOT / "sql/source_service_requests.sql").read_text())
    assert reqs["event_time"] == "timestamp-millis"
    assert reqs["latency_ms"] == "double"
    assert reqs["status_code"] == "int"
    assert len(reqs) == 9
    deps = _ddl_columns((ROOT / "sql/source_service_deployments.sql").read_text())
    assert deps["service_name"] == "string"
    assert deps["max_replicas"] == "int"
    assert len(deps) == 11
