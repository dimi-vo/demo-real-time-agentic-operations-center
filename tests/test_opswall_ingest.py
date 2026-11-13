"""The live ops wall must understand records the way Kafka actually delivers them.

The mock feeds OpsWallState plain dicts with epoch milliseconds and a service_name on
every record, so it never exercised the live decoder. Two differences matter (D-048):

- Flink's TIMESTAMP(3) columns are registered as `local-timestamp-millis` (D-027), and
  the Avro decoder turns them into naive `datetime`s (UTC wall-clock here).
- `service_deployments` keeps its primary key out of the value: `service_name` is only
  in the message key (D-027).

The service_health and remediation_decisions fixtures are real rows read from Confluent
Cloud in the Phase 4 live run (the payments bad deploy, 2026-11-12 09:45-09:47), minus
the columns dropped since (D-043, D-044). The dump stored the decoder's datetimes as ISO
text; `_as_decoded` turns them back. The other two topics go through the real
AvroSerializer/AvroDeserializer pair against the repo's schemas in an in-memory Schema
Registry, keys included.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.opswall.consumer import (
    DEPLOYMENTS_TOPIC,
    INCIDENTS_TOPIC,
    decode_record,
    make_deserializer,
)
from scripts.opswall.state import OpsWallState, _to_seconds

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "opswall"
TIMESTAMP_COLUMNS = ("window_start", "window_end")


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _as_decoded(record: dict) -> dict:
    """A dumped row as the Avro decoder handed it over: timestamps as naive datetimes."""
    out = dict(record)
    for col in TIMESTAMP_COLUMNS:
        if isinstance(out.get(col), str):
            out[col] = datetime.fromtimestamp(_epoch(out[col]), tz=timezone.utc).replace(tzinfo=None)
    return out


def _fixture(name: str) -> list[dict]:
    lines = (FIXTURES / name).read_text().splitlines()
    return [_as_decoded(json.loads(line)) for line in lines if line.strip()]


def _service(st: OpsWallState, name: str, now: float) -> dict:
    return next(s for s in st.snapshot(now=now)["services"] if s["name"] == name)


# --- timestamps --------------------------------------------------------------------


def test_a_naive_datetime_is_read_as_utc_wall_clock():
    assert _to_seconds(datetime(2026, 11, 12, 9, 46, 0)) == _epoch("2026-11-12T09:46:00Z")
    assert _to_seconds(datetime(2026, 11, 12, 9, 46, 0, tzinfo=timezone.utc)) == _epoch("2026-11-12T09:46:00Z")
    # the mock's shapes still work
    assert _to_seconds(1_800_000_000_000) == 1_800_000_000.0
    assert _to_seconds(1_800_000_000.5) == 1_800_000_000.5


# --- real service_health + remediation_decisions rows ------------------------------

HEALTH = _fixture("service_health.jsonl")
DECISIONS = _fixture("remediation_decisions.jsonl")
NOW = _epoch("2026-11-12T09:47:30Z")
# The dump has no Kafka timestamps; a decision lands a few seconds after its window.
DECISION_LAG = 8.0


def _live_payments_wall() -> OpsWallState:
    st = OpsWallState()
    for row in HEALTH:
        st.apply_health(row)
    for d in DECISIONS:
        st.apply_decision(d, kafka_ts=_to_seconds(d["window_end"]) + DECISION_LAG)
    return st


def test_real_health_rows_reach_the_wall():
    payments = _service(_live_payments_wall(), "payments", NOW)
    assert payments["status"] != "warming"
    last = HEALTH[-1]
    assert payments["rateRps"] == last["request_count"] / 10.0
    assert payments["detectedLatency"], "the HIGH latency windows must draw a detection"


def test_the_first_rollback_closes_the_real_detection():
    payments = _service(_live_payments_wall(), "payments", NOW)
    rollbacks = [d for d in payments["decisions"] if d["action"] == "ROLLBACK"]
    assert len(rollbacks) == len(DECISIONS) == 5
    first_span = payments["detectedLatency"][0]
    assert first_span["end"] == pytest.approx(rollbacks[0]["at"])


# --- service_deployments and service_incidents through the real Avro path ----------


@pytest.fixture
def registry() -> SchemaRegistryClient:
    """An in-memory Schema Registry holding the repo's copies of the registered schemas
    (kept in step with the DDL by tests/test_schema_vs_ddl.py)."""
    sr = SchemaRegistryClient.new_client({"url": "mock://"})
    for subject, avsc in (
        ("service_deployments-key", "service_deployments_key.avsc"),
        ("service_deployments-value", "service_deployments.avsc"),
        ("service_incidents-value", "service_incidents.avsc"),
    ):
        sr.register_schema(subject, Schema((ROOT / "schemas" / avsc).read_text(), "AVRO"))
    return sr


def _produce(sr, topic: str, field: MessageField, record: dict) -> bytes:
    """Serialize exactly as opsgen/kafka_io.KafkaSink does."""
    ser = AvroSerializer(sr, schema_str=None, conf={"auto.register.schemas": False, "use.latest.version": True})
    return ser(record, SerializationContext(topic, field))


def test_a_live_deployment_record_updates_its_tile(registry):
    from opsgen.services import SERVICES_BY_NAME
    from opsgen.simulation import Simulation

    sim = Simulation(1, start=1_800_000_000.0)
    sim.deployments["checkout"].deploy(1_800_000_000.0)
    record = sim._dep_record("checkout", 1_800_000_000.0)  # what ops-sim produces
    key = _produce(registry, DEPLOYMENTS_TOPIC, MessageField.KEY, {"service_name": "checkout"})
    value = _produce(registry, DEPLOYMENTS_TOPIC, MessageField.VALUE, record)

    deser = make_deserializer(registry)
    decoded = decode_record(value, key, DEPLOYMENTS_TOPIC, deser, key_deserializer=deser)
    assert isinstance(decoded["deployed_at"], datetime), "the decoder hands over datetimes"
    assert decoded["service_name"] == "checkout", "service_name comes from the key"

    st = OpsWallState()
    st.apply_deployment(decoded)
    tile = _service(st, "checkout", 1_800_000_030.0)
    initial = SERVICES_BY_NAME["checkout"].initial_version
    assert (tile["version"], tile["previousVersion"], tile["lastChange"]) == ("4.2.2", initial, "DEPLOY")
    assert tile["secondsSinceDeploy"] == pytest.approx(30.0)


def test_a_live_incident_record_draws_its_ground_truth(registry):
    from opsgen.engine import BAD_DEPLOY, Incident
    from opsgen.groundtruth import end_record, start_record

    inc = Incident("INC0001", BAD_DEPLOY, "checkout", 1_800_000_000.0, 1_800_000_040.0, "LATENCY_HIGH", "ROLLBACK")
    deser = make_deserializer(registry)
    st = OpsWallState()
    for rec in (start_record(inc, 1_800_000_000.0), end_record(inc, 1_800_000_040.0)):
        value = _produce(registry, INCIDENTS_TOPIC, MessageField.VALUE, rec)
        # service_incidents keys are raw strings, not Avro: the value carries service_name.
        decoded = decode_record(value, b"checkout", INCIDENTS_TOPIC, deser)
        st.apply_incident(decoded)
    now = 1_800_000_060.0
    [truth] = _service(st, "checkout", now)["groundTruth"]
    axis_left = now - st.axis_seconds
    assert truth["start"] == pytest.approx(1_800_000_000.0 - axis_left)
    assert truth["end"] == pytest.approx(1_800_000_040.0 - axis_left)
    assert truth["open"] is False
