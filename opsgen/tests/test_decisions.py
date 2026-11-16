"""Live ops-sim reads `remediation_decisions` and feeds the engine (D-065).

The decoding goes through the real AvroSerializer/AvroDeserializer pair against the schema
Flink registers for the table (an in-memory Schema Registry), with a real row from the
Phase 4 live run. The feed runs its real thread against a fake consumer, so no broker.
"""

from __future__ import annotations

import io
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import OFFSET_END, KafkaError, KafkaException
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from opsgen import decisions
from opsgen.decisions import DECISIONS_TOPIC, decode_decision, make_deserializer
from opsgen.engine import Decision

ROOT = Path(__file__).resolve().parents[2]

# remediation_decisions as Confluent Cloud registers it from sql/sink_remediation_decisions.sql:
# TIMESTAMP(3) as local-timestamp-millis (D-027), NOT NULL columns bare, the rest nullable.
DECISIONS_SCHEMA = json.dumps({
    "type": "record", "name": "record", "namespace": "org.apache.flink.avro.generated",
    "fields": [
        {"name": "window_end", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
        {"name": "service_name", "type": "string"},
        {"name": "agent_name", "type": "string"},
        {"name": "trigger", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "target", "type": ["null", "string"], "default": None},
        {"name": "reason", "type": ["null", "string"], "default": None},
        {"name": "raw_response", "type": ["null", "string"], "default": None},
    ],
})  # fmt: skip


def _naive(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def _live_rows() -> list[dict]:
    """The Phase 4 live run's decisions (the payments bad deploy), as the decoder hands them
    over: window_end a naive datetime."""
    path = ROOT / "tests" / "fixtures" / "opswall" / "remediation_decisions.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return [{**r, "window_end": _naive(r["window_end"])} for r in rows]


@pytest.fixture
def registry() -> SchemaRegistryClient:
    sr = SchemaRegistryClient.new_client({"url": "mock://"})
    sr.register_schema(f"{DECISIONS_TOPIC}-value", Schema(DECISIONS_SCHEMA, "AVRO"))
    return sr


def _value(sr, record: dict) -> bytes:
    """Serialized as Flink writes it: against the registered schema."""
    ser = AvroSerializer(sr, schema_str=None, conf={"auto.register.schemas": False, "use.latest.version": True})
    return ser(record, SerializationContext(DECISIONS_TOPIC, MessageField.VALUE))


def test_the_schema_here_has_the_columns_of_the_table():
    ddl = (ROOT / "sql" / "sink_remediation_decisions.sql").read_text()
    columns = re.findall(r"^\s*`(\w+)`\s+[A-Z]", ddl, flags=re.MULTILINE)
    assert [f["name"] for f in json.loads(DECISIONS_SCHEMA)["fields"]] == columns


def test_a_live_decision_decodes_with_window_end_in_epoch_seconds(registry):
    row = _live_rows()[0]
    decision = decode_decision(_value(registry, row), make_deserializer(registry))
    assert decision == Decision(
        service_name="payments",
        window_end=_epoch("2026-11-12T09:46:00Z"),  # a naive window_end is UTC (D-048)
        agent_name="release_agent",
        trigger="LATENCY_HIGH",
        action="ROLLBACK",
    )


# --- the feed ------------------------------------------------------------------------


class _Msg:
    def __init__(self, value=None, error=None):
        self._value, self._error = value, error

    def value(self):
        return self._value

    def key(self):
        return None

    def error(self):
        return self._error


class _FakeConsumer:
    """Just enough of `confluent_kafka.Consumer`. The topic shows up after `missing_for`
    lookups; `poll` hands out `messages` in order, then nothing."""

    def __init__(self, messages=(), missing_for=0):
        self.messages = list(messages)
        self.missing_for = missing_for
        self.lookups = 0
        self.assigned = None
        self.closed = False
        self.commits = 0
        self.idle = threading.Event()  # set once a poll found nothing left to hand out

    def list_topics(self, topic=None, timeout=None):
        self.lookups += 1
        if self.lookups <= self.missing_for:
            return SimpleNamespace(topics={})
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions={0: None})})

    def assign(self, partitions):
        self.assigned = partitions

    def poll(self, timeout):
        assert self.assigned is not None, "polled before assigning"
        if self.messages:
            return self.messages.pop(0)
        self.idle.set()
        time.sleep(0.005)
        return None

    def commit(self, *args, **kwargs):
        self.commits += 1

    def close(self):
        self.closed = True


def _feed(consumer, registry, log=None):
    return decisions.DecisionFeed(
        {}, consumer=consumer, deserializer=make_deserializer(registry), log=log or io.StringIO(), retry_seconds=0.01
    )


def _expected(rows):
    """The decisions the rows stand for, from the fixture's own text."""
    path = ROOT / "tests" / "fixtures" / "opswall" / "remediation_decisions.jsonl"
    raw = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    by_text = {_naive(r["window_end"]): _epoch(r["window_end"]) for r in raw}
    return [
        Decision(r["service_name"], by_text[r["window_end"]], r["agent_name"], r["trigger"], r["action"]) for r in rows
    ]


def test_the_feed_queues_each_decision_and_drain_hands_them_over_once(registry):
    rows = _live_rows()
    consumer = _FakeConsumer([_Msg(_value(registry, r)) for r in rows])
    feed = _feed(consumer, registry)
    try:
        assert consumer.idle.wait(5)
        assert feed.drain() == _expected(rows)  # all of them, in the order they came
        assert feed.drain() == []
    finally:
        feed.stop()
    assert consumer.closed


def test_a_missing_topic_warns_once_and_the_feed_keeps_looking(registry):
    # Before lab 4.3 there is no remediation_decisions: ops-sim says so once, then picks the
    # topic up when it appears. Meanwhile incidents expire at their cap (D-065).
    rows = _live_rows()[:1]
    consumer = _FakeConsumer([_Msg(_value(registry, r)) for r in rows], missing_for=3)
    log = io.StringIO()
    feed = _feed(consumer, registry, log)
    try:
        assert consumer.idle.wait(5)
        assert feed.drain() == _expected(rows)
    finally:
        feed.stop()
    assert consumer.lookups == 4
    assert log.getvalue().count("remediation_decisions not found") == 1, log.getvalue()


def test_a_record_that_does_not_decode_is_skipped_with_one_warning(registry):
    first, second = _live_rows()[:2]
    messages = [_Msg(_value(registry, first)), _Msg(b"not avro"), _Msg(b"\x00junk"), _Msg(_value(registry, second))]
    consumer = _FakeConsumer(messages)
    log = io.StringIO()
    feed = _feed(consumer, registry, log)
    try:
        assert consumer.idle.wait(5), "the feed stopped at the bad record"
        assert feed.drain() == _expected([first, second])
    finally:
        feed.stop()
    assert log.getvalue().count("skipped") == 1, log.getvalue()


class _UnreachableAtFirst(_FakeConsumer):
    """The broker can't be reached for the first `failures` lookups."""

    def __init__(self, messages=(), failures=2):
        super().__init__(messages)
        self.failures = failures

    def list_topics(self, topic=None, timeout=None):
        if self.failures:
            self.failures -= 1
            self.lookups += 1
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return super().list_topics(topic, timeout)


def test_a_broker_that_cannot_be_reached_warns_once_and_the_feed_keeps_trying(registry):
    rows = _live_rows()[:1]
    consumer = _UnreachableAtFirst([_Msg(_value(registry, r)) for r in rows], failures=2)
    log = io.StringIO()
    feed = _feed(consumer, registry, log)
    try:
        assert consumer.idle.wait(5)
        assert feed.drain() == _expected(rows)
    finally:
        feed.stop()
    assert consumer.lookups == 3
    assert log.getvalue().count("can't read remediation_decisions") == 1, log.getvalue()


def test_a_poll_error_warns_once_per_kind_and_the_end_of_a_partition_is_no_error(registry):
    first, second = _live_rows()[:2]
    transport = KafkaError(KafkaError._TRANSPORT)
    messages = [
        _Msg(error=KafkaError(KafkaError._PARTITION_EOF)),
        _Msg(_value(registry, first)),
        _Msg(error=transport),
        _Msg(error=transport),
        _Msg(_value(registry, second)),
    ]
    consumer = _FakeConsumer(messages)
    log = io.StringIO()
    feed = _feed(consumer, registry, log)
    try:
        assert consumer.idle.wait(5)
        assert feed.drain() == _expected([first, second])
    finally:
        feed.stop()
    assert log.getvalue().count("ops-sim:") == 1 and "_TRANSPORT" in log.getvalue(), log.getvalue()


CARD = {
    "AIOPS_KAFKA_BOOTSTRAP": "SASL_SSL://pkc-test.example:9092",
    "AIOPS_KAFKA_API_KEY": "key",
    "AIOPS_KAFKA_API_SECRET": "secret",
    "AIOPS_SCHEMA_REGISTRY_URL": "https://psrc-test.example",
    "AIOPS_SR_API_KEY": "sr-key",
    "AIOPS_SR_API_SECRET": "sr-secret",
}


def test_the_card_builds_an_uncommitted_reader_from_the_end_that_never_commits(monkeypatch):
    # read_uncommitted: Flink commits about once a minute (D-063), so a committed reader
    # would see every decision after its incident had expired. From the end: decisions
    # already in the topic are never replayed (D-065).
    configs, consumers = [], []

    def fake_consumer(conf):
        configs.append(conf)
        consumers.append(_FakeConsumer())
        return consumers[-1]

    monkeypatch.setattr(decisions, "Consumer", fake_consumer)
    feeds = [decisions.DecisionFeed(CARD, log=io.StringIO()) for _ in range(2)]
    try:
        assert all(c.idle.wait(5) for c in consumers)
    finally:
        for feed in feeds:
            feed.stop()
    conf = configs[0]
    assert conf["isolation.level"] == "read_uncommitted"
    assert conf["enable.auto.commit"] is False and consumers[0].commits == 0
    assert conf["bootstrap.servers"] == "pkc-test.example:9092"
    assert (conf["security.protocol"], conf["sasl.username"], conf["sasl.password"]) == ("SASL_SSL", "key", "secret")
    assert conf["group.id"].startswith("ops-sim-decisions-")
    assert configs[1]["group.id"] != conf["group.id"]  # a fresh group every launch
    assert [(tp.topic, tp.partition, tp.offset) for tp in consumers[0].assigned] == [(DECISIONS_TOPIC, 0, OFFSET_END)]


# --- the driver ----------------------------------------------------------------------


class _RecordingSink:
    def __init__(self):
        self.out = []

    def emit_metrics(self, records):
        self.out += [("metrics", r) for r in records]

    def emit_deployments(self, records):
        self.out += [("deployments", r) for r in records]

    def emit_incident(self, record):
        self.out.append(("incident", record))

    def emit_event(self, ev):
        self.out.append(("event", ev))

    def flush(self):
        pass


class _FakeFeed:
    """Hands out one list of decisions per drain, in order, then nothing."""

    def __init__(self, per_drain):
        self.per_drain = list(per_drain)

    def drain(self):
        return self.per_drain.pop(0) if self.per_drain else []


def test_the_driver_ticks_with_the_drained_decisions_and_emits_the_end_on_that_tick():
    from opsgen.driver import tick_once
    from opsgen.engine import BAD_DEPLOY, Config
    from opsgen.services import SERVICES_BY_NAME
    from opsgen.simulation import Simulation

    start = 1_800_000_000.0
    sim = Simulation(1, start=start, config=Config(quiet_period=0))
    picks = iter([(BAD_DEPLOY, "payments")])
    sim.engine._choose_incident = lambda now: next(picks, None)
    answer = Decision("payments", start + 10, "release_agent", "LATENCY_HIGH", "ROLLBACK")
    feed = _FakeFeed([[]] * 17 + [[answer]])  # read before tick 17
    sink = _RecordingSink()
    for k in range(18):
        sink.out.clear()
        tick_once(sim, sink, start + k, decisions=feed)

    tick_ms = 1_800_000_017_000
    [rollback] = [r for kind, r in sink.out if kind == "deployments"]
    assert (rollback["service_name"], rollback["last_change"]) == ("payments", "ROLLBACK")
    assert rollback["event_time"] == tick_ms
    [sample] = [r for kind, r in sink.out if kind == "metrics" and r["service_name"] == "payments"]
    assert 0.5 < sample["latency_ms"] / SERVICES_BY_NAME["payments"].median_latency_ms < 2  # normal again (x4 before)
    [end] = [r for kind, r in sink.out if kind == "incident"]
    assert (end["incident_id"], end["ended_at"]) == ("INC0001", tick_ms)
    [event] = [ev for kind, ev in sink.out if kind == "event"]
    assert event.incident.resolved_by == "release_agent ROLLBACK"


# --- the command line ----------------------------------------------------------------


def test_dry_run_and_simulate_never_build_a_feed(monkeypatch):
    # No Kafka in either: every incident runs its 35 s (D-065).
    import opsgen.cli as cli

    def no_feed(*args, **kwargs):
        raise AssertionError("built a DecisionFeed")

    monkeypatch.setattr(decisions, "DecisionFeed", no_feed)
    runs = []
    monkeypatch.setattr(cli, "run_realtime", lambda sim, sink, **kw: runs.append(kw))
    monkeypatch.setattr(cli, "run_simulate", lambda sim, minutes: None)
    cli.main(["--dry-run"])
    cli.main(["--dry-run", "--simulate", "1"])
    [dry_run] = runs
    assert dry_run.get("decisions") is None


def test_live_mode_reads_the_decisions_and_says_so(monkeypatch, tmp_path, capsys):
    import opsgen.cli as cli
    import opsgen.kafka_io
    import scripts.common.credentials
    import scripts.common.paths

    class FakeFeed:
        def __init__(self, card):
            self.card = card

    monkeypatch.setattr(decisions, "DecisionFeed", FakeFeed)
    monkeypatch.setattr(opsgen.kafka_io, "KafkaSink", lambda card: "the sink")
    monkeypatch.setattr(scripts.common.credentials, "load_card", lambda creds, root: (tmp_path / "card", CARD))
    monkeypatch.setattr(scripts.common.paths, "get_project_root", lambda: tmp_path)
    runs = []
    monkeypatch.setattr(cli, "run_realtime", lambda sim, sink, **kw: runs.append((sink, kw)))
    cli.main([])
    [(sink, kw)] = runs
    assert sink == "the sink" and kw["root"] == tmp_path
    assert isinstance(kw["decisions"], FakeFeed) and kw["decisions"].card == CARD
    assert "Reading remediation_decisions: a correct agent answer resolves its incident." in capsys.readouterr().out
