"""Shared test fixtures: a fake Flink session and a fake Kafka admin for `reset` and `reset-health`.

The fake session holds statements (name, SQL, phase); deleting one removes it, like
Confluent Cloud's DELETE. The fake admin records which topics were emptied. Both append to
one `events` list, so a test can assert what happened in which order.

(Lives in conftest.py because .gitignore only admits `tests/conftest.py` and `tests/test_*.py`.)
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


class FakeSession:
    def __init__(self, events: list, statements: list[dict] | None = None, stuck: tuple[str, ...] = ()):
        self.events = events
        self.statements = {s["name"]: s for s in (statements or [])}
        self.stuck = set(stuck)  # statements that ignore a delete
        self.submitted: list[str] = []

    def list_statements(self) -> list[dict]:
        return [
            {"name": s["name"], "spec": {"statement": s["sql"]}, "status": {"phase": s.get("phase", "RUNNING")}}
            for s in self.statements.values()
        ]

    def stop(self, name: str) -> None:
        self.events.append(("deleted", name))
        if name not in self.stuck:
            self.statements.pop(name, None)

    def submit(self, sql: str, label: str = "aiops") -> str:
        self.submitted.append(sql)
        return f"submitted-{len(self.submitted)}"

    def wait(self, name: str, timeout: int = 180) -> dict:
        return {"status": {"phase": "COMPLETED"}}


class FakeFuture:
    def result(self):
        return None


class FakeAdmin:
    def __init__(self, events: list, topics: tuple[str, ...]):
        self.events = events
        self.topics = {t: SimpleNamespace(partitions={0: None, 1: None}) for t in topics}
        self.emptied: list[str] = []

    def list_topics(self, topic: str | None = None, timeout: float = 0):
        return SimpleNamespace(topics=self.topics)

    def delete_records(self, partitions):
        topic = partitions[0].topic
        self.emptied.append(topic)
        self.events.append(("emptied", topic))
        return {tp: FakeFuture() for tp in partitions}


class Harness(SimpleNamespace):
    """What `install_fakes(module)` returns: the shared event log, the fakes and the card path
    to pass as --creds."""


@pytest.fixture
def install_fakes(monkeypatch, tmp_path):
    """Point `module.main` (reset or reset-health) at the fakes: no network, no ops-sim, tmp repo root."""

    def install(module, *, statements=None, stuck=(), topics=()) -> Harness:
        from scripts.common import topics as topics_mod

        events: list = []
        session = FakeSession(events, statements, stuck)
        admin = FakeAdmin(events, topics)
        card = tmp_path / "DEMO.env"
        card.write_text("AIOPS_FLINK_REST_ENDPOINT=https://flink.example\n")
        monkeypatch.setattr(module, "get_project_root", lambda: tmp_path)
        monkeypatch.setattr(module, "FlinkSession", lambda _card: session)
        monkeypatch.setattr(topics_mod, "kafka_admin", lambda _card: admin)
        monkeypatch.setattr(module, "_local_ops_sim", lambda: [], raising=False)
        return Harness(events=events, session=session, admin=admin, card=str(card))

    return install
