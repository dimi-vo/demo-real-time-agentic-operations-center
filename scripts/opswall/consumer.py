"""Kafka → OpsWallState consumer threads, one per topic.

The dashboard reads fourteen topics with the keys on the attendee's credential card:
``service_deployments`` (keyed upsert, from earliest), ``service_health``,
``service_error_rate`` (lab 2.2, D-067), ``remediation_decisions`` and ``service_incidents``
(from ~10 min ago by timestamp), ``service_metrics`` (from ~11 min ago by timestamp:
the chart's 10 minutes plus the margin the state keeps, D-061), checkout's four ticket
topics, ``support_tickets``, ``p1_tickets`` (lab 5.1), ``ticket_triage`` (lab 5.2) and
``ticket_answer_key`` (from ~10 min ago by timestamp, D-068), and the four topics of the
code security incidents, ``cve_scans``, ``critical_cves`` (lab 6.1), ``cve_answer_key`` and
``patch_decisions`` (lab 6.4), from ~10 min ago by timestamp (D-070).

Each topic gets one thread with a dedicated consumer group id (so partitions
aren't shared across threads and nothing is ever committed; we always tail). The
consumers read uncommitted records: Flink's transactional sinks commit about once a
minute, and the wall shouldn't wait for that (D-063).
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

from scripts.opswall.state import HISTORY_SECONDS, RETAIN_MARGIN_SECONDS, OpsWallState

logger = logging.getLogger("ops-wall.consumer")

DEPLOYMENTS_TOPIC = "service_deployments"
HEALTH_TOPIC = "service_health"
ERROR_RATE_TOPIC = "service_error_rate"
DECISIONS_TOPIC = "remediation_decisions"
INCIDENTS_TOPIC = "service_incidents"
METRICS_TOPIC = "service_metrics"
TICKETS_TOPIC = "support_tickets"
P1_TICKETS_TOPIC = "p1_tickets"
TRIAGE_TOPIC = "ticket_triage"
ANSWER_KEY_TOPIC = "ticket_answer_key"
CVE_SCANS_TOPIC = "cve_scans"
CRITICAL_CVES_TOPIC = "critical_cves"
CVE_ANSWER_KEY_TOPIC = "cve_answer_key"
PATCH_DECISIONS_TOPIC = "patch_decisions"

STARTUP_BACKREAD_SECONDS = HISTORY_SECONDS  # read ~10 minutes of history on connect
METRICS_BACKREAD_SECONDS = HISTORY_SECONDS + RETAIN_MARGIN_SECONDS  # ~11 minutes
POLL_TIMEOUT_SEC = 0.5

IGNORED_ERROR_CODES = frozenset({
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
        # Flink commits its sink transactions about once a minute; read_committed held
        # service_health and remediation_decisions back that long. The appliers keep one
        # record per natural key, so a row an aborted transaction wrote and the replay
        # rewrote counts once (D-063).
        "isolation.level": "read_uncommitted",
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
                    stop.wait(2.0)  # wakes at once on a stop
                    continue
                logger.info("Tailing %s", topic)
                state.clear_error(topic)
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if stop.is_set():
                break  # shutting down: leave the record
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() in IGNORED_ERROR_CODES:
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
    """Start fourteen daemon consumer threads and return them, each attached from its usual
    starting point."""
    sr = _schema_registry(card)

    # (topic, apply, assign, decode the key?, use the Kafka timestamp?). Only
    # service_deployments needs its key: it is the only topic whose value lacks
    # service_name. The keys of service_incidents and the ticket and finding topics are raw
    # strings or none, not Avro. A decision's Kafka timestamp is when the agent answered; a
    # p1_tickets record's is when the classifier's run starts, and a ticket_triage record's when
    # it ends (D-068); a critical_cves record's is when the security agent's run starts, and a
    # patch_decisions record's when it ends (D-070).
    topics = [
        (DEPLOYMENTS_TOPIC, state.apply_deployment,
         lambda c: _assign_from_earliest(c, DEPLOYMENTS_TOPIC), True, False),
        (HEALTH_TOPIC, state.apply_health,
         lambda c: _assign_from_timestamp(c, HEALTH_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (ERROR_RATE_TOPIC, state.apply_error_rate,
         lambda c: _assign_from_timestamp(c, ERROR_RATE_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (DECISIONS_TOPIC, state.apply_decision,
         lambda c: _assign_from_timestamp(c, DECISIONS_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
        (INCIDENTS_TOPIC, state.apply_incident,
         lambda c: _assign_from_timestamp(c, INCIDENTS_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (METRICS_TOPIC, state.apply_metric,
         lambda c: _assign_from_timestamp(c, METRICS_TOPIC, METRICS_BACKREAD_SECONDS), False, False),
        (TICKETS_TOPIC, state.apply_ticket,
         lambda c: _assign_from_timestamp(c, TICKETS_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (P1_TICKETS_TOPIC, state.apply_p1_ticket,
         lambda c: _assign_from_timestamp(c, P1_TICKETS_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
        (TRIAGE_TOPIC, state.apply_triage,
         lambda c: _assign_from_timestamp(c, TRIAGE_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
        (ANSWER_KEY_TOPIC, state.apply_answer_key,
         lambda c: _assign_from_timestamp(c, ANSWER_KEY_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (CVE_SCANS_TOPIC, state.apply_finding,
         lambda c: _assign_from_timestamp(c, CVE_SCANS_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (CRITICAL_CVES_TOPIC, state.apply_critical_cve,
         lambda c: _assign_from_timestamp(c, CRITICAL_CVES_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
        (CVE_ANSWER_KEY_TOPIC, state.apply_cve_answer_key,
         lambda c: _assign_from_timestamp(c, CVE_ANSWER_KEY_TOPIC, STARTUP_BACKREAD_SECONDS), False, False),
        (PATCH_DECISIONS_TOPIC, state.apply_patch_decision,
         lambda c: _assign_from_timestamp(c, PATCH_DECISIONS_TOPIC, STARTUP_BACKREAD_SECONDS), False, True),
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
