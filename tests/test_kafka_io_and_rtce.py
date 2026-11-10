"""The deployments key shape and the Lightning Query request (no network)."""

import json

import pytest

from opsgen.kafka_io import key_payload
from scripts.setup_rtce import LIGHTNING_QUERY, basic_token, lightning_command


def test_key_payload_primitive_schema():
    assert key_payload('"string"', "checkout") == "checkout"


def test_key_payload_record_schema():
    schema = json.dumps(
        {"type": "record", "name": "k", "fields": [{"name": "service_name", "type": "string"}]}
    )
    assert key_payload(schema, "checkout") == {"service_name": "checkout"}


def _card():
    return {
        "AIOPS_RTCE_MCP_ENDPOINT": (
            "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/"
            "organizations/org-1/environments/env-1/kafka-clusters/lkc-1"
        ),
        "AIOPS_ENVIRONMENT_ID": "env-1",
        "AIOPS_CLUSTER_ID": "lkc-1",
        "AIOPS_RTCE_API_KEY": "GKEY",
        "AIOPS_RTCE_API_SECRET": "GSEC",
    }


def test_lightning_command_uses_query_v1_and_service_health():
    cmd = lightning_command(_card())
    assert "https://sql.us-east-1.aws.confluent.cloud/query/v1" in cmd
    assert "/query/v1alpha1" not in cmd  # D-008: current docs use /query/v1
    assert "service_health" in cmd
    assert '"catalog_name": "env-1"' in cmd
    assert '"database_name": "lkc-1"' in cmd
    assert f"Authorization: Basic {basic_token('GKEY', 'GSEC')}" in cmd


def test_lightning_query_has_no_aggregate_or_join():
    # Lightning SQL supports only SELECT/WHERE/ORDER BY/LIMIT (D-008 research).
    lowered = LIGHTNING_QUERY.lower()
    for banned in (" group by ", " join ", "count(", "sum(", "avg("):
        assert banned not in lowered


def test_lightning_rejects_bad_endpoint():
    with pytest.raises(ValueError):
        lightning_command({"AIOPS_RTCE_MCP_ENDPOINT": "https://example.com/not-rtce"})
