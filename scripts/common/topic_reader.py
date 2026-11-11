"""Read a whole Flink-produced topic over Kafka, deserialized with its registered Avro schema.

Used by `uv run evaluate` (and later the dashboard) to read `service_health` and
`remediation_decisions` without starting a Flink statement: a plain Kafka consumer with
the card's Kafka key, manual partition assignment from the beginning, no committed
offsets, and `read_committed` isolation (Flink's sink is transactional, so this skips
anything a failed checkpoint aborted). It stops when every partition reports EOF.

These topics are small (`service_health` is 6 rows per 10 s, about 2,200 rows an hour),
so reading from the beginning is cheap and gives callers the topic's head too, which
`evaluate` needs for its fresh-start measurement.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any


class TopicNotFound(Exception):
    """The topic doesn't exist (yet)."""


def _need(card: dict[str, str], key: str) -> str:
    value = card.get(key)
    if not value:
        raise SystemExit(f"Credential card is missing {key}. Re-run `uv run deploy --track pre-provisioned`.")
    return value


def read_topic(
    card: dict[str, str],
    topic: str,
    *,
    max_seconds: float = 180.0,
    idle_seconds: float = 15.0,
    isolation: str = "read_committed",
) -> list[dict]:
    """Every record value in `topic`, oldest first per partition, as plain dicts.

    Raises TopicNotFound if the topic doesn't exist. Tombstones are skipped.
    `isolation="read_uncommitted"` also shows rows of transactions Flink hasn't committed
    yet (useful to tell "slow" from "not committing").
    """
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    consumer = Consumer(
        {
            "bootstrap.servers": _need(card, "AIOPS_KAFKA_BOOTSTRAP").split("://", 1)[-1],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": _need(card, "AIOPS_KAFKA_API_KEY"),
            "sasl.password": _need(card, "AIOPS_KAFKA_API_SECRET"),
            "group.id": f"aiops-reader-{uuid.uuid4().hex[:12]}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "isolation.level": isolation,
        }
    )
    sr = SchemaRegistryClient(
        {
            "url": _need(card, "AIOPS_SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": f"{_need(card, 'AIOPS_SR_API_KEY')}:{_need(card, 'AIOPS_SR_API_SECRET')}",
        }
    )
    deserialize = AvroDeserializer(sr)
    ctx = SerializationContext(topic, MessageField.VALUE)
    try:
        meta = consumer.list_topics(topic, timeout=30).topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            raise TopicNotFound(topic)
        partitions = sorted(meta.partitions)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        remaining = set(partitions)
        rows: list[dict] = []
        deadline = time.monotonic() + max_seconds
        last_activity = time.monotonic()
        while remaining and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_activity > idle_seconds:
                    break
                continue
            last_activity = time.monotonic()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    remaining.discard(msg.partition())
                    continue
                raise KafkaException(msg.error())
            if msg.value() is None:
                continue
            value = deserialize(msg.value(), ctx)
            if value is not None:
                rows.append(value)
        return rows
    finally:
        consumer.close()


def epoch_seconds(value: Any) -> float | None:
    """A timestamp field as epoch seconds: a datetime from fastavro (naive values are
    Flink TIMESTAMPs, which are UTC wall-clock here), epoch milliseconds, or ISO text."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value) / 1000.0
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    raise TypeError(f"not a timestamp: {value!r}")


def iso(epoch: float | None) -> str | None:
    """Epoch seconds as ISO-8601 UTC with a Z, millisecond precision."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def jsonable(record: dict) -> dict:
    """A record with datetimes as ISO text, so it can be dumped to JSONL and read back."""
    out = {}
    for k, v in record.items():
        if isinstance(v, datetime):
            out[k] = iso(epoch_seconds(v))
        elif isinstance(v, (bytes, bytearray)):
            out[k] = v.hex()
        else:
            out[k] = v
    return out
