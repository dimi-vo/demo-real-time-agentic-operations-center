"""The deployments key shape, what the Kafka sink produces, and the Lightning Query request
(no network)."""

import json
from types import SimpleNamespace

import pytest

from opsgen.kafka_io import key_payload
from scripts.setup_rtce import LIGHTNING_QUERIES, LIGHTNING_QUERY, basic_token, lightning_command


def test_key_payload_primitive_schema():
    assert key_payload('"string"', "checkout") == "checkout"


def test_key_payload_record_schema():
    schema = json.dumps(
        {"type": "record", "name": "k", "fields": [{"name": "service_name", "type": "string"}]}
    )
    assert key_payload(schema, "checkout") == {"service_name": "checkout"}


def _kafka_sink(monkeypatch):
    """A KafkaSink over fake Kafka and Schema Registry clients (the network boundary). The
    fake serializer returns what it was asked to encode, and against which subject."""
    from confluent_kafka.serialization import MessageField

    from opsgen import kafka_io

    produced, serializer_conf = [], {}

    class FakeProducer:
        def __init__(self, conf):
            pass

        def produce(self, topic, key, value, timestamp):
            produced.append((topic, key, value, timestamp))

        def poll(self, timeout):
            pass

    class FakeSchemaRegistry:
        def __init__(self, conf):
            pass

        def get_latest_version(self, subject):
            return SimpleNamespace(schema=SimpleNamespace(schema_str='"string"'))

    class FakeSerializer:
        def __init__(self, sr, schema_str, conf):
            serializer_conf.update(conf)

        def __call__(self, obj, ctx):
            field = "value" if ctx.field == MessageField.VALUE else "key"
            return (f"{ctx.topic}-{field}", obj)

    monkeypatch.setattr(kafka_io, "Producer", FakeProducer)
    monkeypatch.setattr(kafka_io, "SchemaRegistryClient", FakeSchemaRegistry)
    monkeypatch.setattr(kafka_io, "AvroSerializer", FakeSerializer)
    card = {
        "AIOPS_KAFKA_BOOTSTRAP": "SASL_SSL://pkc-1.example:9092",
        "AIOPS_SCHEMA_REGISTRY_URL": "https://psrc-1.example",
        "AIOPS_SR_API_KEY": "SRKEY",
        "AIOPS_SR_API_SECRET": "SRSEC",
        "AIOPS_KAFKA_API_KEY": "KKEY",
        "AIOPS_KAFKA_API_SECRET": "KSEC",
    }
    return kafka_io.KafkaSink(card), produced, serializer_conf


def test_kafka_sink_produces_a_ticket_and_its_answer_key_keyed_by_raw_strings(monkeypatch):
    # D-068: no key schemas. A ticket is keyed by its service name, like service_incidents; an
    # answer-key record by its ticket id. Both value schemas are looked up, never registered.
    from opsgen.tickets import NewTicket

    sink, produced, serializer_conf = _kafka_sink(monkeypatch)
    ticket = {"event_time": 1_800_000_005_000, "ticket_id": "INC0041872", "source": "SERVICENOW",
              "service_name": "checkout", "priority": "P1", "subject": "s", "description": "d"}  # fmt: skip
    key = {"event_time": 1_800_000_005_000, "ticket_id": "INC0041872", "true_severity": "LOW", "expected_action": None}
    sink.emit_tickets([NewTicket(ticket, key)])
    assert produced == [
        ("support_tickets", b"checkout", ("support_tickets-value", ticket), 1_800_000_005_000),
        ("ticket_answer_key", b"INC0041872", ("ticket_answer_key-value", key), 1_800_000_005_000),
    ]
    assert serializer_conf["auto.register.schemas"] is False


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


def test_every_lightning_example_stays_within_the_supported_subset():
    """Lightning SQL has no aggregates, GROUP BY or joins (D-008)."""
    for table, query in LIGHTNING_QUERIES.items():
        lowered = query.lower()
        assert f"from {table}" in lowered
        assert " join " not in lowered and "group by" not in lowered
        assert "count(" not in lowered and "max(" not in lowered


def test_the_decisions_example_answers_the_walkthrough_question():
    """Brief section 8, step 7: the most recent decision for a trigger, with reasoning."""
    query = LIGHTNING_QUERIES["remediation_decisions"]
    assert "where trigger =" in query.lower()
    assert "reason" in query and "order by window_end desc limit 1" in query.lower()


def test_lightning_command_can_target_the_decisions_table():
    cmd = lightning_command(_card(), "remediation_decisions")
    assert "remediation_decisions" in cmd and "/query/v1" in cmd
