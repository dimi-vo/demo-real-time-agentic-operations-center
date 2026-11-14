"""/healthz reports the state; /ws pushes a snapshot on connect; /static serves no-store;
POST /refresh restarts the feed behind the wall (D-053)."""

from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from scripts.opswall.feed import Feed
from scripts.opswall.server import create_app
from scripts.opswall.state import OpsWallState


@pytest.fixture
def client():
    state = OpsWallState()
    app = create_app(state)
    with TestClient(app) as c:
        c.app.state.state = state  # stash so tests can poke the backing store
        yield c, state


def test_healthz_reports_service_count_and_no_errors(client):
    c, _ = client
    r = c.get("/healthz")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["serviceCount"] == 6
    assert data["connectionErrors"] == {}


def test_healthz_surfaces_connection_errors(client):
    c, state = client
    state.record_error("service_health", "UNKNOWN_TOPIC", "service_health does not exist yet")
    err = c.get("/healthz").json()["connectionErrors"]
    assert "service_health" in err
    assert err["service_health"]["code"] == "UNKNOWN_TOPIC"


def test_index_returns_html_with_no_cache(client):
    c, _ = client
    r = c.get("/")
    assert r.status_code == 200
    assert "no-store" in r.headers["cache-control"]
    assert "<" in r.text


def test_static_assets_served_no_store(client):
    c, _ = client
    r = c.get("/static/index.html")
    assert r.status_code == 200
    assert "no-store" in r.headers["cache-control"]


def test_websocket_sends_a_snapshot_on_connect(client):
    c, _ = client
    with c.websocket_connect("/ws") as ws:
        payload = ws.receive_text()
        snap = json.loads(payload)
        assert snap["axisSeconds"] == 600
        assert isinstance(snap["services"], list) and len(snap["services"]) == 6
        assert snap["summary"]["injected"] == 0
        for s in snap["services"]:
            assert "blocks" in s and "groundTruth" in s


# --- POST /refresh -------------------------------------------------------------


class _FakeThread:
    """What a feed starter returns: something to stop and join, no Kafka behind it."""

    name = "fake-consumer"

    def join(self, timeout=None):
        pass

    def is_alive(self):
        return False


def _refreshable_app(starter):
    """The app on a real state and a real Feed whose starter is `starter(state, stop)`."""
    state = OpsWallState()
    feed = Feed(state, lambda stop: starter(state, stop))
    feed.start()
    return create_app(state, feed), state


def test_refresh_returns_the_new_generation_and_the_next_snapshot_is_clean():
    seeded = []

    def starter(state, stop):
        # what a consumer set does on attach: re-read the topic into the wall
        state.apply_deployment({"service_name": "checkout", "version": "2.0.0"})
        seeded.append(stop)
        return [_FakeThread()]

    app, state = _refreshable_app(starter)
    state.apply_health({"service_name": "cart", "window_end": int((time.time() - 20) * 1000),
                        "detected_at": int((time.time() - 14) * 1000),
                        "latency_status": "HIGH", "traffic_status": "NORMAL"})
    state.record_error("service_health", "UNKNOWN_TOPIC", "gone")
    before = state.snapshot()
    assert {s["name"]: s for s in before["services"]}["cart"]["blocks"]  # something to clear

    with TestClient(app) as c:
        r = c.post("/refresh")
        assert r.status_code == 200
        assert r.json()["generation"] > before["generation"]
        with c.websocket_connect("/ws") as ws:
            snap = json.loads(ws.receive_text())

    assert snap["generation"] == r.json()["generation"]
    assert snap["connectionErrors"] == {}
    by_name = {s["name"]: s for s in snap["services"]}
    assert by_name["cart"]["blocks"] == []                   # old history is gone
    assert by_name["checkout"]["version"] == "2.0.0"         # and the new consumers re-seeded the wall
    assert len(seeded) == 2 and seeded[0].is_set() and not seeded[1].is_set()  # old set stopped, new set running


def test_a_refresh_calls_the_feed_once_per_request():
    calls = []

    class _StubFeed:
        def restart(self):
            calls.append("restart")

    state = OpsWallState()
    with TestClient(create_app(state, _StubFeed())) as c:
        assert c.post("/refresh").status_code == 200
    assert calls == ["restart"]


def test_refresh_is_a_post_only():
    app, _ = _refreshable_app(lambda state, stop: [_FakeThread()])
    with TestClient(app) as c:
        assert c.get("/refresh").status_code == 405


def test_refresh_without_a_feed_says_so():
    with TestClient(create_app(OpsWallState())) as c:
        assert c.post("/refresh").status_code == 503


def test_overlapping_refreshes_restart_one_at_a_time():
    active = 0
    max_active = 0
    starts = 0
    guard = threading.Lock()

    def starter(state, stop):
        nonlocal active, max_active, starts
        with guard:
            active += 1
            starts += 1
            max_active = max(max_active, active)
        time.sleep(0.15)  # long enough for the other request to arrive meanwhile
        with guard:
            active -= 1
        return [_FakeThread()]

    app, state = _refreshable_app(starter)
    with TestClient(app) as c:
        start_generation = state.generation
        results = []

        def click():
            results.append(c.post("/refresh").status_code)

        clicks = [threading.Thread(target=click) for _ in range(2)]
        for t in clicks:
            t.start()
        for t in clicks:
            t.join()

    assert results == [200, 200]
    assert max_active == 1                       # never two sets of consumers being started at once
    assert starts == 3                           # the first start plus one per click
    assert state.generation == start_generation + 2
