"""OpsWallState: appliers, trimming, span consolidation, snapshot shape."""

from __future__ import annotations

import threading
import time

import pytest

from scripts.opswall.state import AXIS_SECONDS, CLOSING_ACTIONS, OpsWallState

NOW = 1_800_000_000.0  # fixed epoch so snapshots are deterministic
LEFT = NOW - AXIS_SECONDS


def _state():
    return OpsWallState()


# --- plumbing & dedup ---------------------------------------------------------


def test_services_are_preseeded_with_tier_and_replicas():
    st = _state()
    snap = st.snapshot(now=NOW)
    names = {s["name"]: s for s in snap["services"]}
    assert "checkout" in names and "web-gateway" in names
    assert names["checkout"]["tier"] in ("CRITICAL", "STANDARD")
    assert names["checkout"]["replicas"] is not None
    assert "maxReplicas" not in names["checkout"]  # D-043: no ceiling


def test_deployment_upsert_keeps_latest():
    st = _state()
    st.apply_deployment({"service_name": "checkout", "version": "1.0.0", "ready_replicas": 3,
                         "deployed_at": int((NOW - 300) * 1000)})
    st.apply_deployment({"service_name": "checkout", "version": "1.0.1", "ready_replicas": 4})
    s = _get(st, "checkout")
    assert s["version"] == "1.0.1"
    assert s["replicas"] == 4
    assert 299 <= s["secondsSinceDeploy"] <= 301


def test_health_dedup_by_window_end():
    st = _state()
    we = NOW - 60
    st.apply_health({"service_name": "cart", "window_end": int(we * 1000),
                     "latency_status": "HIGH", "traffic_status": "NORMAL"})
    st.apply_health({"service_name": "cart", "window_end": int(we * 1000),
                     "latency_status": "NORMAL", "traffic_status": "NORMAL"})
    s = _get(st, "cart")
    assert s["detectedLatency"] == []  # the second (NORMAL) row overwrote


def test_decision_dedup_by_window_and_agent():
    st = _state()
    we = NOW - 60
    r = {"service_name": "cart", "window_end": int(we * 1000),
         "agent_name": "release_agent", "action": "OBSERVE", "trigger": "LATENCY_HIGH"}
    st.apply_decision(r, kafka_ts=we)
    st.apply_decision({**r, "action": "ROLLBACK", "target": "2.9.4"}, kafka_ts=we)
    s = _get(st, "cart")
    assert [d["action"] for d in s["decisions"]] == ["ROLLBACK"]
    assert s["decisions"][0]["target"] == "2.9.4"


def test_unknown_service_is_silently_ignored():
    st = _state()
    st.apply_deployment({"service_name": "not-a-real-service", "version": "1.0.0"})
    st.apply_health({"service_name": "nope", "window_end": int(NOW * 1000)})
    st.apply_decision({"service_name": "nope"})
    st.apply_incident({"service_name": "nope", "incident_id": "X"})
    # Still renders the six real services without raising.
    assert len(st.snapshot(now=NOW)["services"]) == 6


# --- incidents ----------------------------------------------------------------


def test_start_record_opens_span_end_record_closes_it():
    st = _state()
    st.apply_incident({"service_name": "checkout", "incident_id": "INC0001",
                       "incident_type": "BAD_DEPLOY",
                       "event_time": int((NOW - 200) * 1000),
                       "started_at": int((NOW - 200) * 1000),
                       "expected_trigger": "LATENCY_HIGH", "expected_action": "ROLLBACK"})
    s = _get(st, "checkout")
    assert len(s["groundTruth"]) == 1
    assert s["groundTruth"][0]["open"] is True

    st.apply_incident({"service_name": "checkout", "incident_id": "INC0001",
                       "incident_type": "BAD_DEPLOY",
                       "event_time": int((NOW - 140) * 1000),
                       "started_at": int((NOW - 200) * 1000),
                       "ended_at": int((NOW - 140) * 1000),
                       "expected_trigger": "LATENCY_HIGH", "expected_action": "ROLLBACK"})
    s = _get(st, "checkout")
    gt = s["groundTruth"][0]
    assert gt["open"] is False
    assert pytest.approx(gt["start"], abs=0.1) == AXIS_SECONDS - 200
    assert pytest.approx(gt["end"], abs=0.1) == AXIS_SECONDS - 140


def test_start_record_after_end_is_ignored():
    st = _state()
    end = {"service_name": "cart", "incident_id": "INC0001", "incident_type": "TRAFFIC_SURGE",
           "event_time": int((NOW - 100) * 1000),
           "started_at": int((NOW - 200) * 1000), "ended_at": int((NOW - 100) * 1000),
           "expected_trigger": "TRAFFIC_HIGH", "expected_action": "SCALE_OUT"}
    st.apply_incident(end)
    st.apply_incident({**end, "ended_at": None, "event_time": int((NOW - 200) * 1000)})
    gt = _get(st, "cart")["groundTruth"][0]
    assert gt["open"] is False


# --- span consolidation -------------------------------------------------------


def test_contiguous_anomalous_health_rows_merge_into_one_span():
    st = _state()
    for k in range(5):
        we = NOW - 100 + k * 10
        st.apply_health({"service_name": "checkout", "window_end": int(we * 1000),
                         "latency_status": "HIGH", "traffic_status": "NORMAL"})
    spans = _get(st, "checkout")["detectedLatency"]
    assert len(spans) == 1
    assert pytest.approx(spans[0]["start"], abs=0.1) == AXIS_SECONDS - 110  # first window's start
    assert pytest.approx(spans[0]["end"], abs=0.1) == AXIS_SECONDS - 60


def test_a_normal_row_in_the_middle_splits_the_span():
    st = _state()
    statuses = ["HIGH", "HIGH", "NORMAL", "HIGH"]
    for k, status in enumerate(statuses):
        we = NOW - 100 + k * 10
        st.apply_health({"service_name": "cart", "window_end": int(we * 1000),
                         "latency_status": status, "traffic_status": "NORMAL"})
    spans = _get(st, "cart")["detectedLatency"]
    assert len(spans) == 2


def test_actionable_decision_closes_the_span():
    st = _state()
    for k in range(6):
        we = NOW - 100 + k * 10
        st.apply_health({"service_name": "checkout", "window_end": int(we * 1000),
                         "latency_status": "HIGH", "traffic_status": "NORMAL"})
    rollback_at = NOW - 70
    st.apply_decision({"service_name": "checkout", "window_end": int((NOW - 80) * 1000),
                       "agent_name": "release_agent", "action": "ROLLBACK",
                       "trigger": "LATENCY_HIGH", "target": "2.9.4"},
                      kafka_ts=rollback_at)
    span = _get(st, "checkout")["detectedLatency"][0]
    assert pytest.approx(span["end"], abs=0.1) == AXIS_SECONDS - 70


def test_observe_does_not_close_the_span():
    st = _state()
    for k in range(4):
        we = NOW - 60 + k * 10
        st.apply_health({"service_name": "cart", "window_end": int(we * 1000),
                         "latency_status": "HIGH", "traffic_status": "NORMAL"})
    st.apply_decision({"service_name": "cart", "window_end": int((NOW - 60) * 1000),
                       "agent_name": "release_agent", "action": "OBSERVE",
                       "trigger": "LATENCY_HIGH"}, kafka_ts=NOW - 55)
    span = _get(st, "cart")["detectedLatency"][0]
    assert pytest.approx(span["end"], abs=0.1) == AXIS_SECONDS - 30  # last HIGH window_end
    assert all(a in CLOSING_ACTIONS or a == "OBSERVE"
               for a in [d["action"] for d in _get(st, "cart")["decisions"]])


def test_traffic_up_and_down_land_on_different_spans():
    st = _state()
    patterns = ["HIGH", "LOW"]
    for i, status in enumerate(patterns):
        we = NOW - 60 + i * 30
        st.apply_health({"service_name": "cart", "window_end": int(we * 1000),
                         "latency_status": "NORMAL", "traffic_status": status})
    directions = [d["direction"] for d in _get(st, "cart")["detectedTraffic"]]
    assert set(directions) == {"up", "down"}


# --- status derivation --------------------------------------------------------


@pytest.mark.parametrize(("lat", "trf", "expected"), [
    ("NORMAL", "NORMAL", "healthy"),
    ("HIGH", "NORMAL", "anomaly"),
    ("NORMAL", "HIGH", "anomaly"),
    ("NORMAL", "LOW", "anomaly"),
    ("WARMING_UP", "WARMING_UP", "warming"),
])
def test_status_follows_latest_health_row(lat, trf, expected):
    st = _state()
    st.apply_health({"service_name": "checkout", "window_end": int((NOW - 10) * 1000),
                     "latency_status": lat, "traffic_status": trf})
    assert _get(st, "checkout")["status"] == expected


def test_recent_actionable_decision_marks_service_remediated():
    st = _state()
    st.apply_health({"service_name": "checkout", "window_end": int((NOW - 60) * 1000),
                     "latency_status": "HIGH", "traffic_status": "NORMAL"})
    st.apply_health({"service_name": "checkout", "window_end": int((NOW - 10) * 1000),
                     "latency_status": "NORMAL", "traffic_status": "NORMAL"})
    st.apply_decision({"service_name": "checkout", "window_end": int((NOW - 60) * 1000),
                       "agent_name": "release_agent", "action": "ROLLBACK",
                       "trigger": "LATENCY_HIGH", "target": "2.9.4"},
                      kafka_ts=NOW - 50)
    s = _get(st, "checkout")
    assert s["status"] == "remediated"
    assert "2.9.4" in s["statusText"]


# --- trim --------------------------------------------------------------------


def test_trim_drops_entries_older_than_the_axis():
    st = _state()
    old = NOW - AXIS_SECONDS * 2
    st.apply_health({"service_name": "cart", "window_end": int(old * 1000),
                     "latency_status": "HIGH", "traffic_status": "NORMAL"})
    st.apply_health({"service_name": "cart", "window_end": int((NOW - 10) * 1000),
                     "latency_status": "NORMAL", "traffic_status": "NORMAL"})
    st.trim(NOW)
    # Only the recent row survives — the old span is gone.
    assert _get(st, "cart")["detectedLatency"] == []


# --- thread safety -----------------------------------------------------------


def test_concurrent_writes_do_not_raise():
    st = _state()

    def writer():
        for _ in range(200):
            st.apply_health({"service_name": "cart",
                             "window_end": int(time.time() * 1000),
                             "latency_status": "HIGH", "traffic_status": "NORMAL"})

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st.snapshot()  # no exception


# --- helpers -----------------------------------------------------------------


def _get(st, name):
    return next(s for s in st.snapshot(now=NOW)["services"] if s["name"] == name)
