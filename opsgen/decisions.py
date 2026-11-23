"""Live ops-sim reads `remediation_decisions`: the agents' answers, for the engine (D-065).

`DecisionFeed` tails the topic on one daemon thread with the card's Kafka key: a fresh group
every launch, no offset commits, uncommitted records (D-063), and every partition assigned
at its end once the topic is found, so decisions already in it are never replayed. Each
record is decoded through Schema Registry into an `opsgen.engine.Decision` and queued; the
driver drains the queue before every tick and hands the decisions to `Simulation.tick`,
where a correct answer resolves its incident.

Trouble never stops the generator: a missing topic, an unreachable broker or a record that
won't decode is warned once per kind on stderr, and the feed keeps trying. Meanwhile
incidents expire at their cap, as they did before. `--dry-run` and `--simulate` never build
a feed.
"""

from __future__ import annotations

import queue
import sys
import threading
import uuid

from confluent_kafka import OFFSET_END, Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from opsgen.engine import Decision
from scripts.common.topic_reader import epoch_seconds

DECISIONS_TOPIC = "remediation_decisions"
_FIELDS = ("service_name", "window_end", "agent_name", "trigger", "action")
POLL_TIMEOUT_SECONDS = 0.5
RETRY_SECONDS = 3.0
# What a consumer reports while a topic or partition isn't there yet, or at its end: not
# trouble, as on the ops wall.
IGNORED_ERROR_CODES = frozenset({
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError._UNKNOWN_TOPIC,
    KafkaError._UNKNOWN_PARTITION,
    KafkaError._PARTITION_EOF,
})  # fmt: skip


def _need(card: dict[str, str], key: str) -> str:
    value = card.get(key)
    if not value:
        raise SystemExit(f"Credential card is missing {key}. Re-run `uv run deploy --track pre-provisioned`.")
    return value


def _build_consumer(card: dict[str, str]) -> Consumer:
    """The card's Kafka key, a fresh group every launch and no offset commits: ops-sim
    assigns its partitions itself and always tails. Uncommitted records: Flink commits its
    sink transactions about once a minute (D-063), so a committed reader would see every
    decision after its incident had already expired (D-065)."""
    return Consumer({
        "bootstrap.servers": _need(card, "AIOPS_KAFKA_BOOTSTRAP").split("://", 1)[-1],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": _need(card, "AIOPS_KAFKA_API_KEY"),
        "sasl.password": _need(card, "AIOPS_KAFKA_API_SECRET"),
        "group.id": f"ops-sim-decisions-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",  # the assignment sets the offset anyway
        "isolation.level": "read_uncommitted",
        "log_level": 0,  # librdkafka's own logging off: trouble is warned once, below
    })  # fmt: skip


def _schema_registry(card: dict[str, str]) -> SchemaRegistryClient:
    return SchemaRegistryClient({
        "url": _need(card, "AIOPS_SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": f"{_need(card, 'AIOPS_SR_API_KEY')}:{_need(card, 'AIOPS_SR_API_SECRET')}",
    })  # fmt: skip


def make_deserializer(sr: SchemaRegistryClient) -> AvroDeserializer:
    """Decodes against the registered schema, as the ops wall does."""
    return AvroDeserializer(sr, schema_str=None, conf={"use.latest.version": True})


def decode_decision(value: bytes, deserializer: AvroDeserializer) -> Decision:
    """One `remediation_decisions` message value as a `Decision`. `window_end` arrives as a
    naive datetime, UTC wall-clock (D-048). Raises if the record can't be decoded or lacks
    one of the fields the engine needs."""
    record = deserializer(value, SerializationContext(DECISIONS_TOPIC, MessageField.VALUE))
    missing = [f for f in _FIELDS if (record or {}).get(f) is None]
    if missing:
        raise ValueError(f"record lacks {', '.join(missing)}")
    return Decision(
        service_name=record["service_name"],
        window_end=epoch_seconds(record["window_end"]),
        agent_name=record["agent_name"],
        trigger=record["trigger"],
        action=record["action"],
    )


class DecisionFeed:
    """Tails `remediation_decisions` on one daemon thread and queues each decision for the
    driver, which `drain`s the queue before every tick."""

    def __init__(
        self, card: dict[str, str], *, consumer=None, deserializer=None, log=None, retry_seconds=RETRY_SECONDS
    ) -> None:
        # A missing card key exits here, on the main thread, before anything is produced.
        self._consumer = consumer if consumer is not None else _build_consumer(card)
        self._deserializer = deserializer if deserializer is not None else make_deserializer(_schema_registry(card))
        self._log = log if log is not None else sys.stderr
        self._retry_seconds = retry_seconds
        self._queue: queue.Queue = queue.Queue()
        self._warned: set[str] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="ops-sim-decisions", daemon=True)
        self._thread.start()

    def drain(self) -> list[Decision]:
        """Every decision queued since the last call, in arrival order. Never blocks."""
        out = []
        while True:
            try:
                out.append(self._queue.get_nowait())
            except queue.Empty:
                return out

    def stop(self) -> None:
        """End the thread; it closes the consumer. Waits about one poll, so Ctrl-C stays
        quick: a thread stuck in a metadata lookup (an unreachable broker) is a daemon, and
        ends with the process. Nothing is lost, since ops-sim never commits offsets."""
        self._stop.set()
        self._thread.join(timeout=POLL_TIMEOUT_SECONDS + 1)

    def _run(self) -> None:
        assigned = False
        try:
            while not self._stop.is_set():
                if not assigned:
                    assigned = self._assign()
                    if not assigned:
                        self._stop.wait(self._retry_seconds)  # wakes at once on stop()
                    continue
                self._poll_once()
        finally:
            try:
                self._consumer.close()
            except Exception:
                pass

    def _assign(self) -> bool:
        """Assign every partition at its end, so decisions already in the topic are never
        replayed. False while the topic can't be found or assigned; the caller retries."""
        try:
            md = self._consumer.list_topics(topic=DECISIONS_TOPIC, timeout=10)
            topic = md.topics.get(DECISIONS_TOPIC)
            if topic is None or topic.error is not None:
                self._warn("missing", f"{DECISIONS_TOPIC} not found. {self._retrying}")
                return False
            self._consumer.assign([TopicPartition(DECISIONS_TOPIC, p, OFFSET_END) for p in topic.partitions])
        except Exception as exc:  # KafkaException mostly: the broker can't be reached
            self._warn("assign", f"can't read {DECISIONS_TOPIC} ({exc}). {self._retrying}")
            return False
        return True

    def _poll_once(self) -> None:
        try:
            msg = self._consumer.poll(POLL_TIMEOUT_SECONDS)
        except Exception as exc:
            self._warn("poll", f"reading {DECISIONS_TOPIC} failed ({exc}). {self._retrying}")
            self._stop.wait(self._retry_seconds)
            return
        if msg is None:
            return
        if msg.error():
            err = msg.error()
            if err.code() not in IGNORED_ERROR_CODES:  # librdkafka keeps retrying; say so once
                self._warn(f"poll {err.name()}", f"reading {DECISIONS_TOPIC}: {err.name()} ({err}). Still trying")
            return
        if msg.value() is None:
            return
        try:
            decision = decode_decision(msg.value(), self._deserializer)
        except SchemaRegistryError as exc:
            self._warn("schema registry", f"skipped a {DECISIONS_TOPIC} record: Schema Registry failed ({exc})")
            return
        except Exception as exc:
            self._warn("bad record", f"skipped a {DECISIONS_TOPIC} record that doesn't decode ({exc})")
            return
        self._queue.put(decision)

    @property
    def _retrying(self) -> str:
        return f"Retrying every {self._retry_seconds:g} s; meanwhile incidents expire at their cap"

    def _warn(self, kind: str, message: str) -> None:
        """Once per kind of trouble: the generator carries on, and so does the feed."""
        if kind in self._warned:
            return
        self._warned.add(kind)
        self._log.write(f"ops-sim: {message}.\n")
        self._log.flush()
