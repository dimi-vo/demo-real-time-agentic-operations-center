"""Kafka → OpsWallState consumer threads, one per topic.

The dashboard reads four topics with the keys on the attendee's credential card:
``service_deployments`` (keyed upsert, from earliest), ``service_health`` and
``remediation_decisions`` (from ~10 min ago by timestamp), ``service_incidents``
(from ~10 min ago by timestamp).

Each topic gets one thread with a dedicated consumer group id (so partitions
aren't shared across threads and nothing is ever committed; we always tail).
Avro deserialization is per-topic, driven off the Schema Registry's latest
version for each subject. ``service_deployments`` also decodes its key and merges
``service_name`` into the record, because Flink keeps the primary key out of the
value (D-027, D-048). Timestamps arrive as ``datetime``s; OpsWallState converts
them. A missing topic is NOT fatal — it stays
silent until Flink registers it; the connection banner on the page surfaces the
reason so an empty wall never leaves the attendee guessing.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.opswall.state import AXIS_SECONDS, OpsWallState

logger = logging.getLogger("ops-wall.consumer")

DEPLOYMENTS_TOPIC = "service_deployments"
HEALTH_TOPIC = "service_health"
DECISIONS_TOPIC = "remediation_decisions"
INCIDENTS_TOPIC = "service_incidents"

STARTUP_BACKREAD_SECONDS = AXIS_SECONDS  # read ~10 minutes of history on connect
POLL_TIMEOUT_SEC = 0.5

BENIGN_ERROR_CODES = frozenset({
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError._UNKNOWN_TOPIC,
    KafkaError._UNKNOWN_PARTITION,
    KafkaError._PARTITION_EOF,
})


def _need(card: dict[str, str], key: str) -> str:
    v = card.get(key)
    if not v:
        raise SystemExit(
            f"Credential card is missing {key}. "
            "Re-run `uv run deploy --track pre-provisioned`."
        )
    return v


def _bootstrap(raw: str) -> str:
    return raw.split("://", 1)[-1] if "://" in raw else raw


def _schema_registry(card: dict[str, str]) -> SchemaRegistryClient:
    return SchemaRegistryClient({
        "url": _need(card, "AIOPS_SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": (
            f"{_need(card, 'AIOPS_SR_API_KEY')}:{_need(card, 'AIOPS_SR_API_SECRET')}"
        ),
    })


def _build_consumer(card: dict[str, str], topic: str) -> Consumer:
    """One consumer per topic; fresh group every launch, no offset commits."""
    return Consumer({
        "bootstrap.servers": _bootstrap(_need(card, "AIOPS_KAFKA_BOOTSTRAP")),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": _need(card, "AIOPS_KAFKA_API_KEY"),
        "sasl.password": _need(card, "AIOPS_KAFKA_API_SECRET"),
        "group.id": f"ops-wall-{topic}-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",  # overridden by explicit assignment below
        "isolation.level": "read_committed",
        "log_level": 0,  # silence librdkafka's own noise; we route via callbacks
    })


def _assign_from_timestamp(consumer: Consumer, topic: str, since_seconds: float) -> bool:
    """Assign every partition from the offset nearest `since_seconds` ago.

    Returns False if the topic doesn't exist yet — the thread will keep sleeping
    and retrying. Returns True once partitions are assigned.
    """
    md = consumer.list_topics(topic=topic, timeout=10)
    tmd = md.topics.get(topic)
    if tmd is None or tmd.error is not None:
        return False
    ts_ms = int((time.time() - since_seconds) * 1000)
    parts = [TopicPartition(topic, p, ts_ms) for p in tmd.partitions]
    resolved = consumer.offsets_for_times(parts, timeout=10)
    # If a partition has no messages at/after that time, fall back to beginning.
    for i, tp in enumerate(resolved):
        if tp.offset < 0:
            tp.offset = OFFSET_BEGINNING
            resolved[i] = tp
    consumer.assign(resolved)
    return True


def _assign_from_earliest(consumer: Consumer, topic: str) -> bool:
    md = consumer.list_topics(topic=topic, timeout=10)
    tmd = md.topics.get(topic)
    if tmd is None or tmd.error is not None:
        return False
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in tmd.partitions])
    return True


def make_deserializer(sr: SchemaRegistryClient) -> AvroDeserializer:
    # schema_str=None + use.latest.version=True resolves the registered schema per
    # subject, same pattern as opsgen/kafka_io.py on the produce side.
    return AvroDeserializer(sr, schema_str=None, conf={"use.latest.version": True})


def decode_record(
    value: bytes,
    key: bytes | None,
    topic: str,
    value_deserializer: AvroDeserializer,
    key_deserializer: AvroDeserializer | None = None,
) -> dict | None:
    """One Kafka message as the dict OpsWallState.apply_* expects.

    With a `key_deserializer`, the key's fields are merged in: Flink keeps a table's
    primary key out of the value, so a `service_deployments` value has no `service_name`
    (D-027, D-048). Fields already in the value win."""
    record = value_deserializer(value, SerializationContext(topic, MessageField.VALUE))
    if record is None or key_deserializer is None or key is None:
        return record
    key_record = key_deserializer(key, SerializationContext(topic, MessageField.KEY))
    if isinstance(key_record, dict):
        return {**key_record, **record}
    if isinstance(key_record, str):  # a primitive string key schema
        return {"service_name": key_record, **record}
    return record


def _tail(
    state: OpsWallState,
    card: dict[str, str],
    topic: str,
    apply: callable,
    stop: threading.Event,
    assign: callable,
    deserializer: AvroDeserializer,
    key_deserializer: AvroDeserializer | None = None,
    use_kafka_ts: bool = False,
) -> None:
    """One consumer thread: wait for topic, assign, poll, deserialize, apply, repeat."""
    consumer = _build_consumer(card, topic)
    assigned = False
    try:
        while not stop.is_set():
            if not assigned:
                try:
                    assigned = assign(consumer)
                except KafkaException as exc:
                    state.record_error(topic, "ASSIGN_FAILED", str(exc))
                if not assigned:
                    time.sleep(2.0)
                    continue
                logger.info("Tailing %s", topic)
                state.clear_error(topic)
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() in BENIGN_ERROR_CODES:
                    continue
                logger.warning("[%s] %s: %s", topic, err.name(), err)
                state.record_error(topic, err.name(), err.str() or str(err))
                continue
            value = msg.value()
            if value is None:
                continue  # tombstones never happen on these topics, but belt-and-braces
            try:
                # One bad record must not kill the tail: catch anything the
                # deserializer raises and surface it in /healthz, continue polling.
                record = decode_record(value, msg.key(), topic, deserializer, key_deserializer)
            except Exception as exc:
                logger.warning("[%s] deserialize failed: %s", topic, exc)
                state.record_error(topic, "DESERIALIZE_FAILED", str(exc))
                continue
            if record is None:
                continue
            if use_kafka_ts:
                _, ts_ms = msg.timestamp()
                apply(record, kafka_ts=(ts_ms / 1000.0 if ts_ms else None))
            else:
                apply(record)
            state.clear_error(topic)
    finally:
        with _suppress():
            consumer.close()


def start_threads(state: OpsWallState, card: dict[str, str], stop: threading.Event) -> list[threading.Thread]:
    """Start four daemon consumer threads and return them."""
    sr = _schema_registry(card)

    # (topic, apply, assign, decode the key?, use the Kafka timestamp?). Only
    # service_deployments needs its key: it is the only topic whose value lacks
    # service_name. service_incidents keys are raw strings, not Avro.
    topics = [
        (DEPLOYMENTS_TOPIC, state.apply_deployment,
         lambda c: _assign_from_earliest(c, DEPLOYMENTS_TOPIC), True, False),
        (HEALTH_TOPIC, state.apply_health,
         lambda c: _assign_from_timestamp(c, HEALTH_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (DECISIONS_TOPIC, state.apply_decision,
         lambda c: _assign_from_timestamp(c, DECISIONS_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
        (INCIDENTS_TOPIC, state.apply_incident,
         lambda c: _assign_from_timestamp(c, INCIDENTS_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
    ]
    threads = []
    for topic, apply, assign, uses_key, uses_ts in topics:
        deser = make_deserializer(sr)
        t = threading.Thread(
            target=_tail,
            kwargs=dict(state=state, card=card, topic=topic, apply=apply,
                         stop=stop, assign=assign, deserializer=deser,
                         key_deserializer=deser if uses_key else None,
                         use_kafka_ts=uses_ts),
            name=f"ops-wall-{topic}", daemon=True,
        )
        t.start()
        threads.append(t)
    return threads


class _suppress:
    """Tiny context manager — swallow anything from close() during shutdown."""

    def __enter__(self): return self
    def __exit__(self, *exc): return True
