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
    assert data["serviceCount"] == 3  # services: the "Code security incidents" row is none (D-070)
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
        assert snap["chartSeconds"] == 120  # every row's time axis, checkout's timeline included
        assert isinstance(snap["services"], list) and len(snap["services"]) == 4  # three services, then security
        assert "summary" not in snap  # each row has its own Issues panel (D-068)
        assert snap["actionWords"]["ROLLBACK"] == "Roll back"
        for s in snap["services"]:
            assert ("blocks" in s and "groundTruth" in s) == (s["row"] not in ("tickets", "security"))
            assert s["issues"] == {"injected": 0, "detected": 0, "missed": 0,
                                   "medianTimeToDetect": None, "medianTimeToRemediate": None,
                                   "medianTimeToDetectText": "—", "medianTimeToRemediateText": "—"}
        # checkout's row is the ticket row (D-068), with its sources, list and timeline
        checkout = next(s for s in snap["services"] if s["row"] == "tickets")
        assert [src["name"] for src in checkout["sources"]] == ["Zendesk", "ServiceNow"]
        assert checkout["tickets"] == {"received": 0, "newest": []}
        assert (checkout["marks"], checkout["runs"], checkout["openP1"], checkout["agentsWorking"]) == ([], [], 0, 0)
        assert [lane["state"] for lane in checkout["lanes"]] == ["Idle", "Idle"]
        assert checkout["agentsText"] == "Both agents idle"
        # the "Code security incidents" row (D-070), last, with its list and timeline
        security = snap["services"][-1]
        assert (security["name"], security["row"]) == ("Code security incidents", "security")
        assert security["findings"] == {"scanned": 0, "newest": []}
        assert (security["marks"], security["runs"], security["openCriticals"]) == ([], [], 0)
        assert [lane["name"] for lane in security["lanes"]] == ["Security agent", "External · LangGraph · A2A"]
