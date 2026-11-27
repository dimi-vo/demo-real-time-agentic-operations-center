"""Kafka output path — Schema Registry lookup and Avro key/value serialization.

Mirrors the reference simulator: one `AvroSerializer` with `auto.register.schemas`
off and `use.latest.version` on resolves whichever subject the `SerializationContext`
names, so the latest schema Flink registered from the DDL is used for each of
`service_metrics-value`, `service_deployments-value`, `service_deployments-key`,
`service_incidents-value`, `support_tickets-value`, `ticket_answer_key-value`,
`cve_scans-value` and `cve_answer_key-value`.

- `service_metrics` is append-only with no primary key: the Kafka message key is the
  raw `service_name` string (no key schema), like the reference `car_telemetry`.
  `service_incidents` and `support_tickets` are keyed the same way; `ticket_answer_key`
  by the raw `ticket_id` string (D-068). `cve_scans` by the raw `repository` string and
  `cve_answer_key` by the raw `finding_id` string (D-070).
- `service_deployments` is a keyed upsert table: the key is Avro-encoded against the
  registered `service_deployments-key` subject (primitive or record, inspected once),
  like the reference `race_standings`.

Timestamps (`event_time`, `deployed_at`) are epoch-millisecond ints; fastavro passes
ints through for a `timestamp-millis` logical field, and each record is produced with
its `event_time` as the Kafka record timestamp.
"""

from __future__ import annotations

import json
import sys

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from opsgen.findings import NewFinding
from opsgen.sinks import format_event
from opsgen.tickets import NewTicket

METRICS_TOPIC = "service_metrics"
DEPLOYMENTS_TOPIC = "service_deployments"
INCIDENTS_TOPIC = "service_incidents"
TICKETS_TOPIC = "support_tickets"  # checkout's tickets (D-068)
ANSWER_KEY_TOPIC = "ticket_answer_key"
CVE_SCANS_TOPIC = "cve_scans"  # the scanner's findings (D-070)
CVE_ANSWER_KEY_TOPIC = "cve_answer_key"
VALUE_TOPICS = (METRICS_TOPIC, DEPLOYMENTS_TOPIC, INCIDENTS_TOPIC, TICKETS_TOPIC, ANSWER_KEY_TOPIC,
                CVE_SCANS_TOPIC, CVE_ANSWER_KEY_TOPIC)


def key_payload(key_schema_str: str, service_name: str):
    """Shape a service_deployments key to match its registered key schema: a bare string
    for a primitive subject, or a one-field record `{<field>: service_name}` for a record
    subject. Mirrors how the reference simulator handles the race_standings key."""
    parsed = json.loads(key_schema_str)
    if isinstance(parsed, dict) and parsed.get("type") == "record":
        return {parsed["fields"][0]["name"]: service_name}
    return service_name


class KafkaSink:
    """Produces generator records to Confluent Cloud over Kafka + Schema Registry."""

    def __init__(self, card: dict[str, str]) -> None:
        def need(key: str) -> str:
            v = card.get(key)
            if not v:
                raise SystemExit(f"Credential card is missing {key}. Re-run `uv run deploy --track pre-provisioned`.")
            return v

        bootstrap = need("AIOPS_KAFKA_BOOTSTRAP").split("://", 1)[-1]
        self._sr = SchemaRegistryClient(
            {
                "url": need("AIOPS_SCHEMA_REGISTRY_URL"),
                "basic.auth.user.info": f"{need('AIOPS_SR_API_KEY')}:{need('AIOPS_SR_API_SECRET')}",
            }
        )
        self._serializer = AvroSerializer(
            self._sr, schema_str=None, conf={"auto.register.schemas": False, "use.latest.version": True}
        )
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": need("AIOPS_KAFKA_API_KEY"),
                "sasl.password": need("AIOPS_KAFKA_API_SECRET"),
            }
        )
        self._value_ctx = {topic: SerializationContext(topic, MessageField.VALUE) for topic in VALUE_TOPICS}
        self._dep_key_fn = self._build_deployments_key_fn()

    def _build_deployments_key_fn(self):
        """Encode a service_deployments message key against its registered key schema,
        handling both the primitive-string and record shapes Confluent may register."""
        ctx = SerializationContext(DEPLOYMENTS_TOPIC, MessageField.KEY)
        schema_str = self._sr.get_latest_version(f"{DEPLOYMENTS_TOPIC}-key").schema.schema_str

        def key_fn(service_name: str):
            return self._serializer(key_payload(schema_str, service_name), ctx)

        return key_fn

    def _produce(self, topic: str, key_field: str, record: dict) -> None:
        """One record with no key subject: the message key is the raw string of its `key_field`,
        and the Kafka record timestamp its event_time (ms)."""
        self._producer.produce(
            topic,
            key=record[key_field].encode("utf-8"),
            value=self._serializer(record, self._value_ctx[topic]),
            timestamp=int(record["event_time"]),
        )

    def emit_metrics(self, records: list[dict]) -> None:
        for r in records:
            self._produce(METRICS_TOPIC, "service_name", r)
        self._producer.poll(0)

    def emit_deployments(self, records: list[dict]) -> None:
        for r in records:
            self._producer.produce(
                DEPLOYMENTS_TOPIC,
                key=self._dep_key_fn(r["service_name"]),
                value=self._serializer(r, self._value_ctx[DEPLOYMENTS_TOPIC]),
                timestamp=int(r["event_time"]),
            )
        self._producer.poll(0)

    def emit_incident(self, record: dict) -> None:
        """One ground-truth incident record, keyed by the raw service_name string like
        service_metrics: service_incidents has no registered key subject."""
        self._produce(INCIDENTS_TOPIC, "service_name", record)
        self._producer.poll(0)

    def emit_tickets(self, tickets: list[NewTicket]) -> None:
        """Each ticket and its answer key, on the same tick: the ticket keyed by its raw
        service_name, like service_incidents, the answer key by its raw ticket_id, and both at the
        ticket's event_time. Live mode prints no line for them (only `--show-records checkout`
        does)."""
        for t in tickets:
            self._produce(TICKETS_TOPIC, "service_name", t.ticket)
            self._produce(ANSWER_KEY_TOPIC, "ticket_id", t.answer_key)
        self._producer.poll(0)

    def emit_findings(self, findings: list[NewFinding]) -> None:
        """Each finding's scan and its answer key, on the same tick: the scan keyed by its raw
        repository, as support_tickets is by its service, the answer key by its raw finding_id,
        and both at the finding's event_time. Live mode prints no line for them."""
        for f in findings:
            self._produce(CVE_SCANS_TOPIC, "repository", f.scan)
            self._produce(CVE_ANSWER_KEY_TOPIC, "finding_id", f.answer_key)
        self._producer.poll(0)

    def emit_event(self, ev) -> None:
        sys.stderr.write(format_event(ev) + "\n")
        sys.stderr.flush()

    def flush(self) -> None:
        self._producer.flush(timeout=5)
