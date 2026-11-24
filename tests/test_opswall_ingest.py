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
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.opswall import consumer
from scripts.opswall.consumer import (
    DECISIONS_TOPIC,
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
    assert payments["metrics"] == [], "the lines come from service_metrics, not service_health (D-061)"


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
    from opsgen.services import METRIC_SERVICES_BY_NAME
    from opsgen.simulation import Simulation

    sim = Simulation(1, start=1_800_000_000.0)
    sim.deployments["payments"].deploy(1_800_000_000.0)
    record = sim._dep_record("payments", 1_800_000_000.0)  # what ops-sim produces
    key = _produce(registry, DEPLOYMENTS_TOPIC, MessageField.KEY, {"service_name": "payments"})
    value = _produce(registry, DEPLOYMENTS_TOPIC, MessageField.VALUE, record)

    deser = make_deserializer(registry)
    decoded = decode_record(value, key, DEPLOYMENTS_TOPIC, deser, key_deserializer=deser)
    assert isinstance(decoded["deployed_at"], datetime), "the decoder hands over datetimes"
    assert decoded["service_name"] == "payments", "service_name comes from the key"

    st = OpsWallState()
    st.apply_deployment(decoded)
    tile = _service(st, "payments", 1_800_000_030.0)
    initial = METRIC_SERVICES_BY_NAME["payments"].initial_version
    assert (tile["previousVersion"], tile["lastChange"]) == (initial, "DEPLOY")
    assert tile["secondsSinceDeploy"] == pytest.approx(30.0)


def test_a_live_incident_record_draws_its_ground_truth(registry):
    from opsgen.engine import BAD_DEPLOY, Incident
    from opsgen.groundtruth import end_record, start_record

    inc = Incident(
        "INC0001", BAD_DEPLOY, "payments", 1_800_000_000.0, 1_800_000_040.0, "LATENCY_HIGH", "ROLLBACK", 1_800_000_040.0
    )
    deser = make_deserializer(registry)
    st = OpsWallState()
    for rec in (start_record(inc, 1_800_000_000.0), end_record(inc, 1_800_000_040.0)):
        value = _produce(registry, INCIDENTS_TOPIC, MessageField.VALUE, rec)
        # service_incidents keys are raw strings, not Avro: the value carries service_name.
        decoded = decode_record(value, b"payments", INCIDENTS_TOPIC, deser)
        st.apply_incident(decoded)
    now = 1_800_000_060.0
    [truth] = _service(st, "payments", now)["groundTruth"]
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
    payments = _service(st, "payments", now)
    mine = [r for r in sent if r["service_name"] == "payments"]
    left = now - st.axis_seconds
    assert [m["t"] for m in payments["metrics"]] == pytest.approx([start + k + 0.999 - left for k in range(10)])
    assert [m["traffic"] for m in payments["metrics"]] == [r["traffic_count"] for r in mine]
    assert [m["latency"] for m in payments["metrics"]] == [round(r["latency_ms"], 1) for r in mine]


# --- checkout's tickets through the real Avro path (D-068) ---------------------------

# The tables the labs write have no repo .avsc: their value schemas as Confluent Cloud
# registers them from sql/sink_p1_tickets.sql, sink_ticket_triage.sql and
# sink_remediation_decisions.sql (TIMESTAMP(3) as local-timestamp-millis, D-027; NOT NULL
# columns bare, the rest nullable).
_TS = {"type": "long", "logicalType": "local-timestamp-millis"}
_TICKET_FIELDS = [
    {"name": "event_time", "type": ["null", _TS], "default": None},
    *({"name": n, "type": ["null", "string"], "default": None}
      for n in ("ticket_id", "source", "service_name", "priority", "subject", "description")),
]


def _flink_schema(fields: list[dict]) -> Schema:
    return Schema(json.dumps({"type": "record", "name": "record", "namespace": "org.apache.flink.avro.generated",
                              "fields": fields}), "AVRO")


P1_TICKETS_SCHEMA = _flink_schema(_TICKET_FIELDS)
TRIAGE_SCHEMA = _flink_schema([
    *_TICKET_FIELDS,
    {"name": "severity", "type": "string"},
    {"name": "reason", "type": ["null", "string"], "default": None},
    {"name": "raw_response", "type": ["null", "string"], "default": None},
])
DECISIONS_SCHEMA = _flink_schema([
    {"name": "window_end", "type": _TS},
    *({"name": n, "type": "string"} for n in ("service_name", "agent_name", "trigger", "action")),
    *({"name": n, "type": ["null", "string"], "default": None}
      for n in ("target", "reason", "raw_response", "ticket_id")),
])


def test_a_record_from_each_ticket_topic_reaches_checkouts_row(registry):
    from opsgen.simulation import Simulation

    for subject, avsc in (("support_tickets-value", "support_tickets.avsc"),
                          ("ticket_answer_key-value", "ticket_answer_key.avsc")):
        registry.register_schema(subject, Schema((ROOT / "schemas" / avsc).read_text(), "AVRO"))
    registry.register_schema("p1_tickets-value", P1_TICKETS_SCHEMA)
    registry.register_schema("ticket_triage-value", TRIAGE_SCHEMA)
    registry.register_schema("remediation_decisions-value", DECISIONS_SCHEMA)

    # The first P1 the real ticket clock produces that is really HIGH (seed 1).
    start = 1_800_000_000.0
    sim = Simulation(1, start=start)
    new = next(n for k in range(3600) for n in sim.tick(start + k).tickets
               if n.ticket["priority"] == "P1" and n.answer_key["true_severity"] == "HIGH")
    created = new.ticket["event_time"] / 1000

    deser = make_deserializer(registry)
    st = OpsWallState()

    def through(topic: str, record: dict) -> dict:
        return decode_record(_produce(registry, topic, MessageField.VALUE, record), None, topic, deser)

    ticket = through("support_tickets", new.ticket)  # what ops-sim produces
    st.apply_ticket(ticket)
    st.apply_answer_key(through("ticket_answer_key", new.answer_key))
    # what labs 5.1-5.3 write, with each record's Kafka timestamp
    st.apply_p1_ticket(through("p1_tickets", ticket), kafka_ts=created + 1)
    st.apply_triage(through("ticket_triage", {**ticket, "severity": "HIGH", "reason": "Everyone is affected.",
                                              "raw_response": "Severity: HIGH"}), kafka_ts=created + 4)
    st.apply_decision(through("remediation_decisions", {
        "window_end": ticket["event_time"], "service_name": "checkout", "agent_name": "ticket_responder",
        "trigger": "HIGH_SEVERITY_TICKET", "action": new.answer_key["expected_action"], "target": None,
        "reason": "It matches the cue.", "raw_response": "Action: ...", "ticket_id": new.ticket["ticket_id"]}),
        kafka_ts=created + 9)

    row = _service(st, "checkout", created + 30)
    assert row["tickets"]["newest"] == [{
        "id": new.ticket["ticket_id"], "source": new.ticket["source"], "priority": "P1",
        "subject": new.ticket["subject"], "eventTime": pytest.approx(created)}]
    classifier, responder = row["runs"]
    assert (classifier["label"], classifier["reason"]) == ("High in 3s", "Everyone is affected.")
    assert responder["label"].endswith(" in 5s") and responder["reason"] == "It matches the cue."
    assert row["issues"] == {"injected": 1, "detected": 1, "missed": 0,
                             "medianTimeToDetect": pytest.approx(4.0), "medianTimeToRemediate": pytest.approx(9.0)}


# --- the topics the consumer tails ---------------------------------------------------


def test_the_consumer_tails_its_topics_from_where_the_chart_starts(monkeypatch):
    """start_threads is what a new wall calls."""
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
    assert set(by_topic) == {"service_deployments", "service_health", "service_error_rate",
                             "remediation_decisions", "service_incidents", "service_metrics",
                             # checkout's tickets (D-068)
                             "support_tickets", "p1_tickets", "ticket_triage", "ticket_answer_key"}
    assert len(tails) == 10
    metrics = by_topic[METRICS_TOPIC]
    assert metrics["apply"] == st.apply_metric
    assert metrics["key_deserializer"] is None and metrics["use_kafka_ts"] is False
    assert metrics["assign"](object()) is True
    assert seeks == [(METRICS_TOPIC, 660.0)]  # the 10-minute chart plus the 60 s margin
    # lab 2.2's table (D-067), read like service_health: from the chart's 10 minutes back
    errors = by_topic["service_error_rate"]
    assert errors["apply"] == st.apply_error_rate
    assert errors["key_deserializer"] is None and errors["use_kafka_ts"] is False
    assert errors["assign"](object()) is True
    assert seeks[-1] == ("service_error_rate", 600)
    # The ticket topics, from the same 10 minutes back. The two the labs write are read with
    # their Kafka timestamps, as decisions are: they are when each agent started and answered.
    for topic, apply, uses_ts in (("support_tickets", st.apply_ticket, False),
                                  ("p1_tickets", st.apply_p1_ticket, True),
                                  ("ticket_triage", st.apply_triage, True),
                                  ("ticket_answer_key", st.apply_answer_key, False)):
        tail = by_topic[topic]
        assert (tail["apply"], tail["key_deserializer"], tail["use_kafka_ts"]) == (apply, None, uses_ts), topic
        assert tail["assign"](object()) is True
        assert seeks[-1] == (topic, 600)


def test_the_consumer_reads_uncommitted_records(monkeypatch):
    """Flink commits its sink transactions about once a minute, so a read_committed wall
    showed service_health and remediation_decisions up to a minute late (D-063)."""
    configs = []
    monkeypatch.setattr(consumer, "Consumer", lambda conf: configs.append(conf))
    card = {k: "x" for k in ("AIOPS_KAFKA_BOOTSTRAP", "AIOPS_KAFKA_API_KEY",
                             "AIOPS_KAFKA_API_SECRET")}
    consumer._build_consumer(card, DECISIONS_TOPIC)
    assert configs[0]["isolation.level"] == "read_uncommitted"


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
