"""OpsWallState: what the wall shows, read from snapshot(now) after feeding it records the way
the Kafka consumer delivers them (D-048, D-054).

Health rows and incidents carry naive UTC datetimes (Flink's TIMESTAMP(3) through the Avro
decoder), except `detected_at`, a TIMESTAMP_LTZ, which arrives as an aware UTC datetime; a
decision's moment is its Kafka write time, passed as `kafka_ts`. Every scenario
places an incident at T0 on a window boundary, flags windows the way the detector would and
detects each one `DETECT` seconds after it closed.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import pytest

from scripts.opswall.state import AXIS_SECONDS, OpsWallState

NOW = 1_800_000_000.0  # fixed epoch so snapshots are deterministic; a multiple of 10
T0 = NOW - 300         # incidents start here, on a window boundary
DETECT = 6.0           # a window's verdict lands this long after window_end
ANSWER = 8.0           # an agent's decision is written this long after the detection


def _dt(t: float) -> datetime:
    """Epoch seconds as the Avro decoder hands over a TIMESTAMP(3): a naive UTC datetime."""
    return datetime.fromtimestamp(t, tz=timezone.utc).replace(tzinfo=None)


def _health(st, service, window_end, lat="NORMAL", trf="NORMAL", detected_at="default", **extra):
    record = {"service_name": service, "window_start": _dt(window_end - 10), "window_end": _dt(window_end),
              "latency_status": lat, "traffic_status": trf, **extra}
    if detected_at == "default":
        detected_at = window_end + DETECT
    if detected_at is not None:
        record["detected_at"] = datetime.fromtimestamp(detected_at, tz=timezone.utc)  # timestamp-millis
    st.apply_health(record)


def _decide(st, service, window_end, at, action="ROLLBACK", trigger="LATENCY_HIGH", target="4.2.1",
            agent="release_agent"):
    st.apply_decision({"service_name": service, "window_end": _dt(window_end), "agent_name": agent,
                       "trigger": trigger, "action": action, "target": target, "reason": "because"},
                      kafka_ts=at)


def _incident(st, service, start, end=None, incident_type="BAD_DEPLOY", iid="INC0001"):
    record = {"service_name": service, "incident_id": iid, "incident_type": incident_type,
              "started_at": _dt(start), "event_time": _dt(start), "ended_at": None}
    st.apply_incident(record)
    if end is not None:
        st.apply_incident({**record, "event_time": _dt(end), "ended_at": _dt(end)})


def _svc(st, name, now=NOW):
    return next(s for s in st.snapshot(now=now)["services"] if s["name"] == name)


def _pos(t, now=NOW):
    """Where absolute time `t` sits on the axis of a snapshot taken at `now`."""
    return t - (now - AXIS_SECONDS)


# --- plumbing -----------------------------------------------------------------


def test_services_are_preseeded_with_tier_and_replicas():
    snap = OpsWallState().snapshot(now=NOW)
    names = {s["name"]: s for s in snap["services"]}
    assert "checkout" in names and "web-gateway" in names
    assert names["checkout"]["tier"] in ("CRITICAL", "STANDARD")
    assert names["checkout"]["replicas"] is not None
    assert names["checkout"]["status"] == "healthy"
    assert "maxReplicas" not in names["checkout"]  # D-043: no ceiling


def test_deployment_upsert_keeps_latest():
    st = OpsWallState()
    st.apply_deployment({"service_name": "checkout", "version": "1.0.0", "ready_replicas": 3,
                         "deployed_at": int((NOW - 300) * 1000)})
    st.apply_deployment({"service_name": "checkout", "version": "1.0.1", "ready_replicas": 4})
    s = _svc(st, "checkout")
    assert s["version"] == "1.0.1"
    assert s["replicas"] == 4
    assert 299 <= s["secondsSinceDeploy"] <= 301


def test_unknown_service_is_silently_ignored():
    st = OpsWallState()
    st.apply_deployment({"service_name": "not-a-real-service", "version": "1.0.0"})
    st.apply_health({"service_name": "nope", "window_end": int(NOW * 1000)})
    st.apply_decision({"service_name": "nope"})
    st.apply_incident({"service_name": "nope", "incident_id": "X"})
    assert len(st.snapshot(now=NOW)["services"]) == 6


def test_rate_and_errors_come_from_the_newest_health_row():
    st = OpsWallState()
    _health(st, "cart", NOW - 20, request_count=500, error_count=5)
    _health(st, "cart", NOW - 10, request_count=800, error_count=20)
    s = _svc(st, "cart")
    assert s["rateRps"] == pytest.approx(80.0)
    assert s["errorRatePct"] == pytest.approx(2.5)


# --- incidents ----------------------------------------------------------------


def test_start_record_opens_the_box_end_record_closes_it():
    st = OpsWallState()
    _incident(st, "checkout", NOW - 200)
    [gt] = _svc(st, "checkout")["groundTruth"]
    assert gt["open"] is True and gt["metric"] == "latency"

    _incident(st, "checkout", NOW - 200, NOW - 160)
    [gt] = _svc(st, "checkout")["groundTruth"]
    assert gt["open"] is False
    assert gt["start"] == pytest.approx(AXIS_SECONDS - 200)
    assert gt["end"] == pytest.approx(AXIS_SECONDS - 160)


def test_start_record_after_end_is_ignored():
    st = OpsWallState()
    end = {"service_name": "cart", "incident_id": "INC0001", "incident_type": "UPSTREAM_DROP",
           "event_time": _dt(NOW - 100), "started_at": _dt(NOW - 140), "ended_at": _dt(NOW - 100)}
    st.apply_incident(end)
    st.apply_incident({**end, "ended_at": None, "event_time": _dt(NOW - 140)})
    [gt] = _svc(st, "cart")["groundTruth"]
    assert gt["open"] is False and gt["metric"] == "traffic"


# --- the block: detected → answered ------------------------------------------


def _phase4_payments(st):
    """The real Phase 4 shape: a 40 s bad deploy flags 4 windows, one normal, one more flag,
    and the release agent answers all 5 flagged windows."""
    _incident(st, "payments", T0, T0 + 40)
    statuses = ["HIGH", "HIGH", "HIGH", "HIGH", "NORMAL", "HIGH", "NORMAL"]
    for k, status in enumerate(statuses, start=1):
        we = T0 + 10 * k
        _health(st, "payments", we, lat=status)
        if status == "HIGH":
            _decide(st, "payments", we, at=we + DETECT + ANSWER, target="2.9.4")


def test_phase4_payments_draws_two_answered_blocks_owned_by_the_incident():
    st = OpsWallState()
    _phase4_payments(st)
    s = _svc(st, "payments")

    first, second = s["blocks"]
    assert first["incidentId"] == second["incidentId"] == "INC0001"
    # opens at the first window's detected_at, closes at the first decision's write time
    assert first["start"] == pytest.approx(_pos(T0 + 10 + DETECT))
    assert first["end"] == pytest.approx(_pos(T0 + 10 + DETECT + ANSWER))
    assert first["open"] is False
    assert (first["action"], first["target"]) == ("ROLLBACK", "2.9.4")
    assert first["timeToAnswer"] == pytest.approx(ANSWER)
    assert (first["metric"], first["direction"], first["trigger"]) == ("latency", "up", "LATENCY_HIGH")
    # the tooltip counts: every decision of the run, 5 across the two blocks
    assert first["decisions"] == {"ROLLBACK": 4} and second["decisions"] == {"ROLLBACK": 1}
    assert first["decisionCount"] + second["decisionCount"] == 5
    assert second["start"] == pytest.approx(_pos(T0 + 60 + DETECT))
    assert (first["timeToDetect"], second["timeToDetect"]) == (pytest.approx(10 + DETECT), pytest.approx(60 + DETECT))

    [gt] = s["groundTruth"]
    assert gt["verdict"] == "answered"
    assert gt["timeToDetect"] == pytest.approx(10 + DETECT)  # measured on the first block
    assert gt["timeToAnswer"] == pytest.approx(ANSWER)
    assert gt["detectedAt"] == pytest.approx(first["start"])


def test_a_block_before_its_answer_is_open_and_grows_with_now():
    st = OpsWallState()
    _incident(st, "checkout", T0)
    _health(st, "checkout", T0 + 10, lat="HIGH")  # detected at T0 + 16

    [early], [later] = (_svc(st, "checkout", now)["blocks"] for now in (T0 + 18, T0 + 25))
    for block, now in ((early, T0 + 18), (later, T0 + 25)):
        assert block["open"] is True and block["action"] is None and block["timeToAnswer"] is None
        assert block["end"] == AXIS_SECONDS  # at the right edge, i.e. now
        assert block["start"] == pytest.approx(_pos(T0 + 16, now))
    assert later["end"] - later["start"] == pytest.approx(9.0)
    assert early["end"] - early["start"] == pytest.approx(2.0)
    assert _svc(st, "checkout", T0 + 25)["groundTruth"][0]["verdict"] == "pending"


def test_with_no_answer_the_block_goes_30_s_after_the_last_detection_and_the_incident_is_missed():
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)
    for k in range(1, 5):
        _health(st, "checkout", T0 + 10 * k, lat="HIGH")  # last detection at T0 + 46
    _health(st, "checkout", T0 + 50)

    assert [b["open"] for b in _svc(st, "checkout", T0 + 76)["blocks"]] == [True]
    assert _svc(st, "checkout", T0 + 77)["blocks"] == []

    def verdict(now):
        return _svc(st, "checkout", now)["groundTruth"][0]["verdict"]

    assert verdict(T0 + 30) == "pending"   # during the incident
    assert verdict(T0 + 70) == "pending"   # not yet more than 30 s after it ended
    assert verdict(T0 + 71) == "missed"
    assert _svc(st, "checkout", T0 + 71)["groundTruth"][0]["timeToAnswer"] is None


@pytest.mark.parametrize("action", ["OBSERVE", "INVALID"])
def test_any_answer_closes_the_block(action):
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)
    _health(st, "checkout", T0 + 10, lat="HIGH")
    _health(st, "checkout", T0 + 20)
    _decide(st, "checkout", T0 + 10, at=T0 + 30, action=action, target=None)

    s = _svc(st, "checkout")
    [block] = s["blocks"]
    assert (block["open"], block["action"]) == (False, action)
    assert block["end"] == pytest.approx(_pos(T0 + 30))
    assert s["groundTruth"][0]["verdict"] == "answered"


def test_agents_starting_mid_run_still_close_the_block():
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)
    for k in range(1, 5):
        _health(st, "checkout", T0 + 10 * k, lat="HIGH")
    _decide(st, "checkout", T0 + 30, at=T0 + 50)  # the third window is the first one answered

    s = _svc(st, "checkout")
    [block] = s["blocks"]
    assert block["start"] == pytest.approx(_pos(T0 + 16))
    assert block["end"] == pytest.approx(_pos(T0 + 50))
    assert s["groundTruth"][0]["verdict"] == "answered"
    assert s["groundTruth"][0]["timeToAnswer"] == pytest.approx(34.0)


def test_a_decision_for_another_trigger_or_no_run_is_ignored():
    st = OpsWallState()
    _health(st, "checkout", T0 + 10, lat="HIGH")
    _decide(st, "checkout", T0 + 10, at=T0 + 20, action="SCALE_OUT", trigger="TRAFFIC_HIGH",
            agent="capacity_agent")
    _decide(st, "checkout", T0 + 100, at=T0 + 110)  # a window that was never flagged
    [block] = _svc(st, "checkout", T0 + 30)["blocks"]
    assert block["open"] is True and block["decisionCount"] == 0


def test_traffic_high_and_low_are_separate_blocks_with_their_direction():
    st = OpsWallState()
    _health(st, "cart", T0 + 10, trf="HIGH")
    _health(st, "cart", T0 + 20, trf="LOW")
    blocks = _svc(st, "cart", T0 + 30)["blocks"]
    assert [(b["metric"], b["direction"], b["trigger"]) for b in blocks] == [
        ("traffic", "up", "TRAFFIC_HIGH"), ("traffic", "down", "TRAFFIC_LOW")]


def test_a_row_without_detected_at_opens_no_block():
    st = OpsWallState()
    _health(st, "checkout", T0 + 10, lat="HIGH", detected_at=None)
    s = _svc(st, "checkout", T0 + 20)
    assert s["blocks"] == [] and s["status"] == "healthy"


def test_a_replayed_window_off_the_chart_opens_no_block():
    """A restarted INSERT replays old windows with detected_at = now; only windows whose
    window_end is on the chart count."""
    st = OpsWallState()
    old = NOW - AXIS_SECONDS - 60
    for k in range(4):
        _health(st, "payments", old + 10 * k, lat="HIGH", detected_at=NOW - 2)
        _decide(st, "payments", old + 10 * k, at=NOW - 1)
    s = _svc(st, "payments")
    assert s["blocks"] == [] and s["status"] == "healthy"


def test_a_window_written_twice_keeps_its_first_detection_and_the_newest_status():
    st = OpsWallState()
    _health(st, "checkout", T0 + 10, lat="HIGH", detected_at=T0 + 16)
    _health(st, "checkout", T0 + 10, lat="HIGH", detected_at=T0 + 25)
    [block] = _svc(st, "checkout", T0 + 30)["blocks"]
    assert block["start"] == pytest.approx(_pos(T0 + 16, T0 + 30))

    _health(st, "checkout", T0 + 10, lat="NORMAL", detected_at=T0 + 26)
    assert _svc(st, "checkout", T0 + 30)["blocks"] == []


def test_a_decision_written_twice_counts_once_and_keeps_its_first_write_time():
    st = OpsWallState()
    _health(st, "checkout", T0 + 10, lat="HIGH")
    _decide(st, "checkout", T0 + 10, at=T0 + 24, action="OBSERVE", target=None)
    _decide(st, "checkout", T0 + 10, at=T0 + 40, action="ROLLBACK")
    [block] = _svc(st, "checkout")["blocks"]
    assert block["decisionCount"] == 1
    assert block["end"] == pytest.approx(_pos(T0 + 24))
    assert block["action"] == "ROLLBACK"


# --- ownership ------------------------------------------------------------------


def test_a_block_with_no_incident_is_a_false_alarm():
    st = OpsWallState()
    _health(st, "web-gateway", T0 + 10, lat="HIGH")
    _decide(st, "web-gateway", T0 + 10, at=T0 + 24, action="OBSERVE", target=None)
    snap = st.snapshot(now=NOW)
    [block] = next(s for s in snap["services"] if s["name"] == "web-gateway")["blocks"]
    assert block["incidentId"] is None
    assert snap["summary"]["falseAlarms"] == 1


@pytest.mark.parametrize(("first_window_end", "owned"), [
    (T0 + 70, True),    # window [T0+60, T0+70]: starts 20 s after the incident ended
    (T0 + 80, False),   # window [T0+70, T0+80]: starts 30 s after, outside the grace
])
def test_a_block_starting_shortly_after_its_incident_still_belongs_to_it(first_window_end, owned):
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)
    _health(st, "checkout", first_window_end, lat="HIGH")
    _decide(st, "checkout", first_window_end, at=first_window_end + DETECT + ANSWER)
    s = _svc(st, "checkout")
    [block] = s["blocks"]
    assert (block["incidentId"] == "INC0001") is owned
    assert s["groundTruth"][0]["verdict"] == ("answered" if owned else "missed")


def test_a_block_on_the_other_metric_does_not_belong_to_the_incident():
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)  # a bad deploy is latency
    _health(st, "checkout", T0 + 10, trf="HIGH")
    _decide(st, "checkout", T0 + 10, at=T0 + 24, action="OBSERVE", trigger="TRAFFIC_HIGH", target=None,
            agent="capacity_agent")
    s = _svc(st, "checkout")
    assert s["blocks"][0]["incidentId"] is None
    assert s["groundTruth"][0]["verdict"] == "missed"


# --- tile status ------------------------------------------------------------------


def test_the_tile_says_healthy_with_nothing_open_or_fixed():
    st = OpsWallState()
    _health(st, "checkout", NOW - 10)
    assert (_svc(st, "checkout")["status"], _svc(st, "checkout")["statusText"]) == ("healthy", "Healthy")


def test_the_tile_waits_for_the_agent_while_a_block_is_open():
    st = OpsWallState()
    _health(st, "checkout", T0 + 10, lat="HIGH")
    _health(st, "checkout", T0 + 20, trf="LOW")
    s = _svc(st, "checkout", T0 + 30)
    assert s["status"] == "anomaly"
    assert s["statusText"] == "Anomaly detected: latency high and traffic low, waiting for the agent"


@pytest.mark.parametrize(("action", "trigger", "target", "text"), [
    ("ROLLBACK", "LATENCY_HIGH", "4.2.1", "Rolled back to v4.2.1"),
    ("SCALE_OUT", "TRAFFIC_HIGH", "6", "Scaled out to 6 replicas"),
    ("INVESTIGATE_UPSTREAM", "TRAFFIC_LOW", None, "Traced to upstream"),
])
def test_the_tile_shows_the_fix_for_two_minutes(action, trigger, target, text):
    st = OpsWallState()
    lat, trf = ("HIGH", "NORMAL") if trigger == "LATENCY_HIGH" else ("NORMAL", trigger.split("_")[1])
    _health(st, "checkout", T0 + 10, lat=lat, trf=trf)
    _health(st, "checkout", T0 + 20)
    _decide(st, "checkout", T0 + 10, at=T0 + 24, action=action, trigger=trigger, target=target)

    s = _svc(st, "checkout", T0 + 24 + 120)
    assert (s["status"], s["statusText"]) == ("remediated", text)
    assert _svc(st, "checkout", T0 + 24 + 121)["status"] == "healthy"


def test_an_observe_answer_leaves_the_tile_healthy():
    st = OpsWallState()
    _health(st, "web-gateway", T0 + 10, lat="HIGH")
    _decide(st, "web-gateway", T0 + 10, at=T0 + 24, action="OBSERVE", target=None)
    assert _svc(st, "web-gateway", T0 + 30)["status"] == "healthy"


# --- the summary ------------------------------------------------------------------


def test_the_summary_counts_verdicts_and_takes_medians_over_answered_incidents():
    st = OpsWallState()
    # answered: detected 16 s after it started, answered 8 s later
    _incident(st, "checkout", T0, T0 + 40, iid="INC-A")
    _health(st, "checkout", T0 + 10, lat="HIGH")
    _decide(st, "checkout", T0 + 10, at=T0 + 24)
    # answered: a surge detected 26 s after it started, answered 12 s later
    _incident(st, "catalog", T0, T0 + 40, incident_type="TRAFFIC_SURGE", iid="INC-B")
    _health(st, "catalog", T0 + 20, trf="HIGH")
    _decide(st, "catalog", T0 + 20, at=T0 + 38, action="SCALE_OUT", trigger="TRAFFIC_HIGH", target="5",
            agent="capacity_agent")
    # missed: detected, never answered
    _incident(st, "payments", T0, T0 + 40, iid="INC-C")
    _health(st, "payments", T0 + 10, lat="HIGH")
    # pending: still running, nothing detected yet
    _incident(st, "auth", NOW - 5, iid="INC-D")
    # false alarm
    _health(st, "web-gateway", T0 + 100, lat="HIGH")
    _decide(st, "web-gateway", T0 + 100, at=T0 + 114, action="OBSERVE", target=None)

    assert st.snapshot(now=NOW)["summary"] == {
        "injected": 4,
        "answered": 2,
        "missed": 1,
        "pending": 1,
        "falseAlarms": 1,
        "medianTimeToDetect": pytest.approx(21.0),
        "medianTimeToAnswer": pytest.approx(10.0),
    }


def test_an_empty_wall_has_an_empty_summary():
    assert OpsWallState().snapshot(now=NOW)["summary"] == {
        "injected": 0, "answered": 0, "missed": 0, "pending": 0, "falseAlarms": 0,
        "medianTimeToDetect": None, "medianTimeToAnswer": None,
    }


# --- trim --------------------------------------------------------------------


def test_trim_drops_entries_older_than_the_axis():
    st = OpsWallState()
    old = NOW - AXIS_SECONDS * 2
    _incident(st, "cart", old, old + 40, incident_type="UPSTREAM_DROP")
    _health(st, "cart", old, trf="LOW")
    _decide(st, "cart", old, at=old + 10, action="INVESTIGATE_UPSTREAM", trigger="TRAFFIC_LOW")
    _health(st, "cart", NOW - 10)
    st.trim(NOW)
    s = _svc(st, "cart")
    assert s["blocks"] == [] and s["groundTruth"] == []
    assert s["rateRps"] is None or s["rateRps"] >= 0


# --- clear (the "Refresh data" button, D-053) --------------------------------


def _fill(st):
    st.apply_deployment({"service_name": "checkout", "version": "9.9.9", "ready_replicas": 7,
                         "deployed_at": int((NOW - 60) * 1000)})
    _incident(st, "checkout", NOW - 40, iid="INC-1")
    _health(st, "checkout", NOW - 20, lat="HIGH")
    _decide(st, "checkout", NOW - 20, at=NOW - 5)
    st.record_error("service_health", "UNKNOWN_TOPIC", "gone")


def test_clear_reseeds_the_services_and_drops_history_blocks_and_errors():
    fresh = OpsWallState().snapshot(now=NOW)
    st = OpsWallState()
    _fill(st)
    assert _svc(st, "checkout")["blocks"]  # something to clear

    st.clear()

    snap = st.snapshot(now=NOW)
    assert snap["connectionErrors"] == {}
    cleared = _svc(st, "checkout")
    assert cleared["version"] is None
    assert cleared["groundTruth"] == [] and cleared["blocks"] == []
    assert cleared["tier"] and cleared["replicas"] is not None  # pre-seeded again, like a new wall
    assert snap["services"] == fresh["services"]
    assert snap["summary"] == fresh["summary"]


def test_clear_bumps_the_generation_the_snapshot_carries():
    st = OpsWallState()
    first = st.snapshot(now=NOW)["generation"]
    st.clear()
    second = st.snapshot(now=NOW)["generation"]
    st.clear()
    assert second > first
    assert st.snapshot(now=NOW)["generation"] > second


def test_a_cleared_state_takes_new_records():
    st = OpsWallState()
    _fill(st)
    st.clear()
    st.apply_deployment({"service_name": "checkout", "version": "1.0.0", "ready_replicas": 3})
    assert _svc(st, "checkout")["version"] == "1.0.0"


# --- thread safety -----------------------------------------------------------


def test_concurrent_writes_do_not_raise():
    st = OpsWallState()

    def writer():
        for _ in range(200):
            t = time.time()
            _health(st, "cart", t, lat="HIGH", detected_at=t)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st.snapshot()  # no exception
