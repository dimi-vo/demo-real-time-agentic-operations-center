"""/healthz reports the state; /ws pushes a snapshot on connect; /static serves no-store."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

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
    assert data["serviceCount"] == 3
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
        assert isinstance(snap["services"], list) and len(snap["services"]) == 3
        assert snap["summary"]["injected"] == 0
        for s in snap["services"]:
            assert "blocks" in s and "groundTruth" in s
