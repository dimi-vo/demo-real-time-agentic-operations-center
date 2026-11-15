"""The live ops wall must understand records the way Kafka actually delivers them.

Plain dicts with epoch milliseconds and a service_name on every record never exercised
the live decoder. Two differences matter (D-048):

- Flink's TIMESTAMP(3) columns are registered as `local-timestamp-millis` (D-027), and
  the Avro decoder turns them into naive `datetime`s (UTC wall-clock here).
- `service_deployments` keeps its primary key out of the value: `service_name` is only
  in the message key (D-027).

The service_health and remediation_decisions fixtures are real rows read from Confluent
Cloud in the Phase 4 live run (the payments bad deploy, 2026-11-12 09:45-09:47), minus
the columns dropped since (D-043, D-044, D-058), with the two count columns renamed
traffic_count and expected_traffic_count (D-058). The dump stored the decoder's datetimes
as ISO text; `_as_decoded` turns them back. The real rows predate `detected_at` (D-054), so
`_fixture` adds a synthetic one to each health row. The other three topics go through the
real AvroSerializer/AvroDeserializer pair against the repo's schemas in an in-memory Schema
Registry, keys included. The last section checks which topics the consumer tails, and from
when, without a broker.
"""

from __future__ import annotations

import json
import statistics
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.opswall import consumer
from scripts.opswall.consumer import (
    DEPLOYMENTS_TOPIC,
    INCIDENTS_TOPIC,
    METRICS_TOPIC,
    decode_record,
    make_deserializer,
)
from scripts.opswall.state import OpsWallState, _to_seconds

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "opswall"
TIMESTAMP_COLUMNS = ("window_start", "window_end", "detected_at")
# SYNTHETIC: the Phase 4 rows were read before service_health had detected_at (D-054). Each
# health row gets one this long after its window_end, an estimate of the real delay (the
# window closes on the tick at its end, D-064, plus processing).
SYNTHETIC_DETECTION_DELAY = 1.0


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _naive(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(tzinfo=None)


def _as_decoded(record: dict) -> dict:
    """A dumped row as the Avro decoder handed it over: timestamps as naive datetimes."""
    out = dict(record)
    for col in TIMESTAMP_COLUMNS:
        if isinstance(out.get(col), str):
            out[col] = _naive(_epoch(out[col]))
    return out


def _with_synthetic_detected_at(record: dict) -> dict:
    """detected_at is a TIMESTAMP_LTZ (timestamp-millis): the decoder hands over an aware datetime."""
    detected = _epoch(record["window_end"]) + SYNTHETIC_DETECTION_DELAY
    return {**record, "detected_at": datetime.fromtimestamp(detected, tz=timezone.utc)}


def _fixture(name: str) -> list[dict]:
    lines = (FIXTURES / name).read_text().splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    if name == "service_health.jsonl":
        records = [_with_synthetic_detected_at(r) for r in records]
    return [_as_decoded(r) for r in records]


def _service(st: OpsWallState, name: str, now: float) -> dict:
    return next(s for s in st.snapshot(now=now)["services"] if s["name"] == name)


# --- timestamps --------------------------------------------------------------------


def test_a_naive_datetime_is_read_as_utc_wall_clock():
    assert _to_seconds(datetime(2026, 11, 12, 9, 46, 0)) == _epoch("2026-11-12T09:46:00Z")
    assert _to_seconds(datetime(2026, 11, 12, 9, 46, 0, tzinfo=timezone.utc)) == _epoch("2026-11-12T09:46:00Z")


# --- real service_health + remediation_decisions rows ------------------------------

HEALTH = _fixture("service_health.jsonl")
DECISIONS = _fixture("remediation_decisions.jsonl")
NOW = _epoch("2026-11-12T09:47:30Z")
# The dump has no Kafka timestamps; a decision is written a few seconds after the detection.
ANSWER_LAG = 8.0


def _written_at(decision: dict) -> float:
    return _to_seconds(decision["window_end"]) + SYNTHETIC_DETECTION_DELAY + ANSWER_LAG


def _live_payments_wall() -> OpsWallState:
    st = OpsWallState()
    for row in HEALTH:
        st.apply_health(row)
    for d in DECISIONS:
        st.apply_decision(d, kafka_ts=_written_at(d))
    return st


def test_real_health_rows_reach_the_wall():
    payments = _service(_live_payments_wall(), "payments", NOW)
    assert payments["blocks"], "the HIGH latency windows must draw a block"
    assert payments["rateRps"] is None, "the rate comes from service_metrics, not service_health (D-061)"


def test_the_first_rollback_closes_the_real_detection():
    """4 flagged windows, one normal, one more: two blocks, and all 5 rollbacks counted."""
    st = _live_payments_wall()
    payments = _service(st, "payments", NOW)
    first, second = payments["blocks"]
    left = NOW - st.axis_seconds
    first_flag = next(r for r in HEALTH if r["latency_status"] == "HIGH")
    assert first["start"] == pytest.approx(_to_seconds(first_flag["detected_at"]) - left)
    assert first["end"] == pytest.approx(_written_at(DECISIONS[0]) - left)
    assert (first["action"], first["target"]) == ("ROLLBACK", "2.9.4")
    assert first["decisions"]["ROLLBACK"] + second["decisions"]["ROLLBACK"] == len(DECISIONS) == 5


# --- service_deployments, service_incidents and service_metrics through the real Avro path


@pytest.fixture
def registry() -> SchemaRegistryClient:
    """An in-memory Schema Registry holding the repo's copies of the registered schemas
    (kept in step with the DDL by tests/test_schema_vs_ddl.py)."""
    sr = SchemaRegistryClient.new_client({"url": "mock://"})
    for subject, avsc in (
        ("service_deployments-key", "service_deployments_key.avsc"),
        ("service_deployments-value", "service_deployments.avsc"),
        ("service_incidents-value", "service_incidents.avsc"),
        ("service_metrics-value", "service_metrics.avsc"),
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


def test_live_metrics_records_reach_the_charts_and_the_rail(registry):
    from opsgen.simulation import Simulation

    start = 1_800_000_000.0
    sim = Simulation(1, start=start)
    deser = make_deserializer(registry)
    st = OpsWallState()
    sent = []
    for k in range(10):
        for rec in sim._gen_metrics(start + k):  # what ops-sim produces, one per service
            value = _produce(registry, METRICS_TOPIC, MessageField.VALUE, rec)
            st.apply_metric(decode_record(value, None, METRICS_TOPIC, deser))
            sent.append(rec)

    now = start + 10
    checkout = _service(st, "checkout", now)
    mine = [r for r in sent if r["service_name"] == "checkout"]
    left = now - st.axis_seconds
    assert [m["t"] for m in checkout["metrics"]] == pytest.approx([start + k + 0.999 - left for k in range(10)])
    assert [m["traffic"] for m in checkout["metrics"]] == [r["traffic_count"] for r in mine]
    assert [m["latency"] for m in checkout["metrics"]] == [round(r["latency_ms"], 1) for r in mine]
    assert checkout["rateRps"] == pytest.approx(statistics.mean(r["traffic_count"] for r in mine))
    errors, requests = sum(r["error_count"] for r in mine), sum(r["traffic_count"] for r in mine)
    assert checkout["errorRatePct"] == pytest.approx(100 * errors / requests)


# --- the topics the consumer tails ---------------------------------------------------


def test_the_consumer_tails_service_metrics_from_about_11_minutes_back(monkeypatch):
    """start_threads is what both a new wall and "Refresh data" call (D-053), so this is
    also what a refresh re-attaches."""
    tails, seeks = [], []
    monkeypatch.setattr(consumer, "_schema_registry", lambda card: object())
    monkeypatch.setattr(consumer, "make_deserializer", lambda sr: object())
    monkeypatch.setattr(consumer, "_tail", lambda **kw: tails.append(kw))
    monkeypatch.setattr(consumer, "_assign_from_timestamp",
                        lambda c, topic, since: seeks.append((topic, since)) or True)
    st = OpsWallState()
    for t in consumer.start_threads(st, card={}, stop=threading.Event()):
        t.join()

    by_topic = {kw["topic"]: kw for kw in tails}
    assert set(by_topic) == {"service_deployments", "service_health", "remediation_decisions",
                             "service_incidents", "service_metrics"}
    metrics = by_topic[METRICS_TOPIC]
    assert metrics["apply"] == st.apply_metric
    assert metrics["key_deserializer"] is None and metrics["use_kafka_ts"] is False
    assert metrics["assign"](object()) is True
    assert seeks == [(METRICS_TOPIC, 660.0)]  # the 10-minute chart plus the 60 s margin


# --- detected_at, the one instant in service_health (D-054) ------------------------

# The two time columns that matter, as Confluent Cloud registers them: TIMESTAMP(3) as
# local-timestamp-millis, TIMESTAMP_LTZ(3) as timestamp-millis (the Flink serialization
# reference). The rest of the row is left out; the wall reads by name.
HEALTH_TIME_SCHEMA = json.dumps({
    "type": "record", "name": "record", "namespace": "org.apache.flink.avro.generated",
    "fields": [
        {"name": "service_name", "type": ["null", "string"], "default": None},
        {"name": "window_end", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
        {"name": "latency_status", "type": "string"},
        {"name": "traffic_status", "type": "string"},
        {"name": "detected_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


def test_detected_at_is_read_as_an_instant_through_the_avro_path():
    """A TIMESTAMP_LTZ arrives as an aware datetime and must land at its instant, whatever
    time zone the INSERT ran in: the workspace runs statements in the browser's zone, and a
    TIMESTAMP(3) cast put every detection an hour in the future on GMT+01:00."""
    sr = SchemaRegistryClient.new_client({"url": "mock://"})
    sr.register_schema("service_health-value", Schema(HEALTH_TIME_SCHEMA, "AVRO"))
    window_end = 1_800_000_000.0
    detected = window_end + 6.0
    record = {
        "service_name": "payments", "window_end": _naive(window_end),
        "latency_status": "HIGH", "traffic_status": "NORMAL",
        # what the statement computed in a GMT+01:00 session: the same instant, shown at +01:00
        "detected_at": datetime.fromtimestamp(detected, tz=timezone(timedelta(hours=1))),
    }
    value = _produce(sr, "service_health", MessageField.VALUE, record)
    decoded = decode_record(value, None, "service_health", make_deserializer(sr))
    assert decoded["detected_at"].tzinfo is not None, "timestamp-millis decodes to an aware datetime"

    st = OpsWallState()
    st.apply_health(decoded)
    now = detected + 2.0
    [block] = _service(st, "payments", now)["blocks"]
    assert block["start"] == pytest.approx(detected - (now - st.axis_seconds))
    assert block["open"] is True
