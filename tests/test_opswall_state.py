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

from scripts.opswall.state import AXIS_SECONDS, ERROR_RATE_THRESHOLD_PCT, OpsWallState

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


def _decide(st, service, window_end, at, action="ROLLBACK", trigger="LATENCY_HIGH", target="2.9.4",
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


def _sample(st, service, event_time, traffic=50, latency=35.0):
    """One service_metrics record: one second of one service, stamped at its end (D-058)."""
    st.apply_metric({"service_name": service, "event_time": _dt(event_time), "traffic_count": traffic,
                     "latency_ms": latency, "error_count": 0})


def _svc(st, name, now=NOW):
    return next(s for s in st.snapshot(now=now)["services"] if s["name"] == name)


def _pos(t, now=NOW):
    """Where absolute time `t` sits on the axis of a snapshot taken at `now`."""
    return t - (now - AXIS_SECONDS)


# --- plumbing -----------------------------------------------------------------


def test_services_are_preseeded():
    snap = OpsWallState().snapshot(now=NOW)
    assert [s["name"] for s in snap["services"]] == ["web-gateway", "checkout", "payments"]  # the wall's order
    names = {s["name"]: s for s in snap["services"]}
    assert names["checkout"]["status"] == "healthy"
    assert "maxReplicas" not in names["payments"]  # D-043: no ceiling


def test_deployment_upsert_keeps_latest():
    st = OpsWallState()
    st.apply_deployment({"service_name": "payments", "version": "1.0.0", "previous_version": "0.9.0",
                         "ready_replicas": 3, "deployed_at": _dt(NOW - 300)})
    st.apply_deployment({"service_name": "payments", "version": "1.0.1", "previous_version": "1.0.0",
                         "ready_replicas": 4})
    s = _svc(st, "payments")
    assert s["previousVersion"] == "1.0.0"
    assert 299 <= s["secondsSinceDeploy"] <= 301


def test_unknown_service_is_silently_ignored():
    st = OpsWallState()
    st.apply_deployment({"service_name": "not-a-real-service", "version": "1.0.0"})
    st.apply_health({"service_name": "nope", "window_end": _dt(NOW)})
    st.apply_decision({"service_name": "nope"})
    st.apply_incident({"service_name": "nope", "incident_id": "X"})
    _sample(st, "nope", NOW - 1)
    assert len(st.snapshot(now=NOW)["services"]) == 3


# --- service_metrics samples (D-061) -------------------------------------------


def test_the_samples_on_the_chart_reach_the_snapshot_oldest_first():
    st = OpsWallState()
    _sample(st, "payments", NOW - 630.001)                 # kept, but left of the chart
    _sample(st, "payments", NOW - 2.001, traffic=31, latency=140.04)
    _sample(st, "payments", NOW - 4.001, traffic=29)        # late: put in its place
    _sample(st, "payments", NOW - 3.001, traffic=1)
    _sample(st, "payments", NOW - 3.001, traffic=30)        # the same second again: newer wins
    _sample(st, "payments", NOW + 0.999)                    # covers the running second: waits
    _sample(st, "web-gateway", NOW - 1.001, traffic=7)
    metrics = _svc(st, "payments")["metrics"]
    assert [m["t"] for m in metrics] == pytest.approx([_pos(NOW - 4.001), _pos(NOW - 3.001), _pos(NOW - 2.001)])
    assert [m["traffic"] for m in metrics] == [29, 30, 31]
    assert metrics[-1] == {"t": pytest.approx(_pos(NOW - 2.001)), "traffic": 31, "latency": 140.0}
    assert [m["traffic"] for m in _svc(st, "web-gateway")["metrics"]] == [7]


def test_a_sample_in_the_future_shows_once_its_second_is_over():
    st = OpsWallState()
    _sample(st, "payments", NOW + 0.999)
    assert _svc(st, "payments")["metrics"] == []
    assert len(_svc(st, "payments", now=NOW + 1)["metrics"]) == 1


def test_trim_drops_the_samples_older_than_the_axis_plus_the_margin():
    st = OpsWallState()
    _sample(st, "payments", NOW - 670, traffic=1)
    _sample(st, "payments", NOW - 650, traffic=2)
    st.trim(NOW)  # keeps the last 600 + 60 s
    # A snapshot whose chart still reaches back 700 s would show the dropped one.
    assert [m["traffic"] for m in _svc(st, "payments", now=NOW - 100)["metrics"]] == [2]


def test_health_rows_alone_draw_no_line():
    st = OpsWallState()
    _health(st, "payments", NOW - 10, traffic_count=800)
    assert _svc(st, "payments")["metrics"] == []  # the lines come from service_metrics (D-061)


# --- incidents ----------------------------------------------------------------


def test_the_wall_knows_the_chart_of_every_incident_type_the_simulator_starts_and_no_other():
    # D-068: surges are retired, so TRAFFIC_SURGE leaves the map with the engine's types.
    from opsgen import engine
    from scripts.opswall.state import INCIDENT_METRIC

    assert set(INCIDENT_METRIC) == {*engine.INCIDENT_TYPES, engine.BAD_DEPLOY_ERRORS}
    assert INCIDENT_METRIC[engine.UPSTREAM_DROP] == "traffic"


def test_start_record_opens_the_box_end_record_closes_it():
    st = OpsWallState()
    _incident(st, "payments", NOW - 200)
    [gt] = _svc(st, "payments")["groundTruth"]
    assert gt["open"] is True and gt["metric"] == "latency"

    _incident(st, "payments", NOW - 200, NOW - 160)
    [gt] = _svc(st, "payments")["groundTruth"]
    assert gt["open"] is False
    assert gt["start"] == pytest.approx(AXIS_SECONDS - 200)
    assert gt["end"] == pytest.approx(AXIS_SECONDS - 160)


def test_start_record_after_end_is_ignored():
    st = OpsWallState()
    end = {"service_name": "payments", "incident_id": "INC0001", "incident_type": "UPSTREAM_DROP",
           "event_time": _dt(NOW - 100), "started_at": _dt(NOW - 140), "ended_at": _dt(NOW - 100)}
    st.apply_incident(end)
    st.apply_incident({**end, "ended_at": None, "event_time": _dt(NOW - 140)})
    [gt] = _svc(st, "payments")["groundTruth"]
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


def test_a_block_keeps_its_key_as_the_wall_moves_and_no_two_blocks_share_one():
    st = OpsWallState()
    _phase4_payments(st)
    first, second = _svc(st, "payments")["blocks"]
    assert first["key"] != second["key"]
    later = _svc(st, "payments", NOW + 30)["blocks"]
    assert [b["key"] for b in later] == [first["key"], second["key"]]
    assert later[0]["start"] != first["start"]  # the positions moved, the keys didn't

def test_a_block_before_its_answer_is_open_and_grows_with_now():
    st = OpsWallState()
    _incident(st, "payments", T0)
    _health(st, "payments", T0 + 10, lat="HIGH")  # detected at T0 + 16

    [early], [later] = (_svc(st, "payments", now)["blocks"] for now in (T0 + 18, T0 + 25))
    for block, now in ((early, T0 + 18), (later, T0 + 25)):
        assert block["open"] is True and block["action"] is None and block["timeToAnswer"] is None
        assert block["end"] == AXIS_SECONDS  # at the right edge, i.e. now
        assert block["start"] == pytest.approx(_pos(T0 + 16, now))
    assert later["end"] - later["start"] == pytest.approx(9.0)
    assert early["end"] - early["start"] == pytest.approx(2.0)
    assert _svc(st, "payments", T0 + 25)["groundTruth"][0]["verdict"] == "pending"


def test_with_no_answer_the_block_goes_30_s_after_the_last_detection_and_the_incident_is_missed():
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)
    for k in range(1, 5):
        _health(st, "payments", T0 + 10 * k, lat="HIGH")  # last detection at T0 + 46
    _health(st, "payments", T0 + 50)

    assert [b["open"] for b in _svc(st, "payments", T0 + 76)["blocks"]] == [True]
    assert _svc(st, "payments", T0 + 77)["blocks"] == []

    def verdict(now):
        return _svc(st, "payments", now)["groundTruth"][0]["verdict"]

    assert verdict(T0 + 30) == "pending"   # during the incident
    assert verdict(T0 + 70) == "pending"   # not yet more than 30 s after it ended
    assert verdict(T0 + 71) == "missed"
    assert _svc(st, "payments", T0 + 71)["groundTruth"][0]["timeToAnswer"] is None


@pytest.mark.parametrize("action", ["OBSERVE", "INVALID"])
def test_any_answer_closes_the_block(action):
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", T0 + 10, lat="HIGH")
    _health(st, "payments", T0 + 20)
    _decide(st, "payments", T0 + 10, at=T0 + 30, action=action, target=None)

    s = _svc(st, "payments")
    [block] = s["blocks"]
    assert (block["open"], block["action"]) == (False, action)
    assert block["end"] == pytest.approx(_pos(T0 + 30))
    assert s["groundTruth"][0]["verdict"] == "answered"


def test_agents_starting_mid_run_still_close_the_block():
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)
    for k in range(1, 5):
        _health(st, "payments", T0 + 10 * k, lat="HIGH")
    _decide(st, "payments", T0 + 30, at=T0 + 50)  # the third window is the first one answered

    s = _svc(st, "payments")
    [block] = s["blocks"]
    assert block["start"] == pytest.approx(_pos(T0 + 16))
    assert block["end"] == pytest.approx(_pos(T0 + 50))
    assert s["groundTruth"][0]["verdict"] == "answered"
    assert s["groundTruth"][0]["timeToAnswer"] == pytest.approx(34.0)


def test_a_decision_for_another_trigger_or_no_run_is_ignored():
    st = OpsWallState()
    _health(st, "payments", T0 + 10, lat="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 20, action="SCALE_OUT", trigger="TRAFFIC_HIGH",
            agent="capacity_agent")
    _decide(st, "payments", T0 + 100, at=T0 + 110)  # a window that was never flagged
    [block] = _svc(st, "payments", T0 + 30)["blocks"]
    assert block["open"] is True and block["decisionCount"] == 0


def test_traffic_high_and_low_are_separate_blocks_with_their_direction():
    st = OpsWallState()
    _health(st, "payments", T0 + 10, trf="HIGH")
    _health(st, "payments", T0 + 20, trf="LOW")
    blocks = _svc(st, "payments", T0 + 30)["blocks"]
    assert [(b["metric"], b["direction"], b["trigger"]) for b in blocks] == [
        ("traffic", "up", "TRAFFIC_HIGH"), ("traffic", "down", "TRAFFIC_LOW")]


def test_a_row_without_detected_at_opens_no_block():
    st = OpsWallState()
    _health(st, "payments", T0 + 10, lat="HIGH", detected_at=None)
    s = _svc(st, "payments", T0 + 20)
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
    _health(st, "payments", T0 + 10, lat="HIGH", detected_at=T0 + 16)
    _health(st, "payments", T0 + 10, lat="HIGH", detected_at=T0 + 25)
    [block] = _svc(st, "payments", T0 + 30)["blocks"]
    assert block["start"] == pytest.approx(_pos(T0 + 16, T0 + 30))

    _health(st, "payments", T0 + 10, lat="NORMAL", detected_at=T0 + 26)
    assert _svc(st, "payments", T0 + 30)["blocks"] == []


def test_a_decision_written_twice_counts_once_and_keeps_its_first_write_time():
    st = OpsWallState()
    _health(st, "payments", T0 + 10, lat="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 24, action="OBSERVE", target=None)
    _decide(st, "payments", T0 + 10, at=T0 + 40, action="ROLLBACK")
    [block] = _svc(st, "payments")["blocks"]
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
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", first_window_end, lat="HIGH")
    _decide(st, "payments", first_window_end, at=first_window_end + DETECT + ANSWER)
    s = _svc(st, "payments")
    [block] = s["blocks"]
    assert (block["incidentId"] == "INC0001") is owned
    assert s["groundTruth"][0]["verdict"] == ("answered" if owned else "missed")


def test_a_block_on_the_other_metric_does_not_belong_to_the_incident():
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)  # a bad deploy is latency
    _health(st, "payments", T0 + 10, trf="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 24, action="OBSERVE", trigger="TRAFFIC_HIGH", target=None,
            agent="capacity_agent")
    s = _svc(st, "payments")
    assert s["blocks"][0]["incidentId"] is None
    assert s["groundTruth"][0]["verdict"] == "missed"


# --- tile status ------------------------------------------------------------------


def test_the_tile_says_healthy_with_nothing_open_or_fixed():
    st = OpsWallState()
    _health(st, "payments", NOW - 10)
    assert (_svc(st, "payments")["status"], _svc(st, "payments")["statusText"]) == ("healthy", "Healthy")


def test_the_tile_waits_for_the_agent_while_a_block_is_open():
    st = OpsWallState()
    _health(st, "payments", T0 + 10, lat="HIGH")
    _health(st, "payments", T0 + 20, trf="LOW")
    s = _svc(st, "payments", T0 + 30)
    assert s["status"] == "anomaly"
    assert s["statusText"] == "Anomaly detected: latency high and traffic low, waiting for the agent"


@pytest.mark.parametrize(("action", "trigger", "target", "text"), [
    ("ROLLBACK", "LATENCY_HIGH", "2.9.4", "Rolled back to v2.9.4"),
    ("SCALE_OUT", "TRAFFIC_HIGH", "6", "Scaled out to 6 replicas"),
    ("INVESTIGATE_UPSTREAM", "TRAFFIC_LOW", None, "Traced to upstream"),
])
def test_the_tile_shows_the_fix_for_two_minutes(action, trigger, target, text):
    st = OpsWallState()
    lat, trf = ("HIGH", "NORMAL") if trigger == "LATENCY_HIGH" else ("NORMAL", trigger.split("_")[1])
    _health(st, "payments", T0 + 10, lat=lat, trf=trf)
    _health(st, "payments", T0 + 20)
    _decide(st, "payments", T0 + 10, at=T0 + 24, action=action, trigger=trigger, target=target)

    s = _svc(st, "payments", T0 + 24 + 120)
    assert (s["status"], s["statusText"]) == ("remediated", text)
    assert _svc(st, "payments", T0 + 24 + 121)["status"] == "healthy"


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
    # answered: a drop detected 26 s after it started, answered 12 s later
    _incident(st, "web-gateway", T0, T0 + 40, incident_type="UPSTREAM_DROP", iid="INC-B")
    _health(st, "web-gateway", T0 + 20, trf="LOW")
    _decide(st, "web-gateway", T0 + 20, at=T0 + 38, action="INVESTIGATE_UPSTREAM", trigger="TRAFFIC_LOW",
            target=None, agent="capacity_agent")
    # missed: detected, never answered
    _incident(st, "payments", T0, T0 + 40, iid="INC-C")
    _health(st, "payments", T0 + 10, lat="HIGH")
    # pending: still running, nothing detected yet (checkout's second incident)
    _incident(st, "checkout", NOW - 5, iid="INC-D")
    # false alarm: latency, so the drop on the same service doesn't own it
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
    _incident(st, "payments", old, old + 40, incident_type="UPSTREAM_DROP")
    _health(st, "payments", old, trf="LOW")
    _decide(st, "payments", old, at=old + 10, action="INVESTIGATE_UPSTREAM", trigger="TRAFFIC_LOW")
    _health(st, "payments", NOW - 10)
    _sample(st, "payments", old)
    st.trim(NOW)
    s = _svc(st, "payments")
    assert s["blocks"] == [] and s["groundTruth"] == []
    assert _svc(st, "payments", now=old + 10)["metrics"] == []


def test_trim_takes_an_incident_that_never_ended_off_the_wall_after_11_minutes():
    # The simulator restarted mid-incident: no end record ever comes (D-060). Untrimmed, the
    # incident stays open, pending and drawn across the whole lane for as long as the wall runs.
    st = OpsWallState()
    _incident(st, "checkout", NOW - 670, iid="ORPHAN")
    _incident(st, "payments", NOW - 650, iid="RECENT", incident_type="UPSTREAM_DROP")
    assert _svc(st, "checkout")["groundTruth"][0]["verdict"] == "pending"
    st.trim(NOW)
    assert _svc(st, "checkout")["groundTruth"] == []
    assert [g["incidentId"] for g in _svc(st, "payments")["groundTruth"]] == ["RECENT"]
    assert st.snapshot(now=NOW)["summary"]["pending"] == 1


# --- thread safety -----------------------------------------------------------


def test_concurrent_writes_do_not_raise():
    st = OpsWallState()

    def writer():
        for _ in range(200):
            t = time.time()
            _health(st, "payments", t, lat="HIGH", detected_at=t)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st.snapshot()  # no exception


# --- web-gateway: the error-rate chart and deploy marks (D-067) ------------------------


def _error_rate(st, window_end, status="NORMAL", detected_at="default", service="web-gateway"):
    """One service_error_rate row (lab 2.2), as the consumer delivers it."""
    errors = 200 if status == "HIGH" else 2
    record = {"service_name": service, "window_start": _dt(window_end - 10), "window_end": _dt(window_end),
              "traffic_count": 500, "error_count": errors, "error_rate": errors / 500, "error_status": status}
    if detected_at == "default":
        detected_at = window_end + DETECT
    if detected_at is not None:
        record["detected_at"] = datetime.fromtimestamp(detected_at, tz=timezone.utc)
    st.apply_error_rate(record)


def _deployment(st, service, deployed_at, last_change, version, previous_version):
    st.apply_deployment({"service_name": service, "version": version, "previous_version": previous_version,
                         "last_change": last_change, "deployed_at": _dt(deployed_at)})


def test_each_card_names_its_charts():
    charts = {s["name"]: s["charts"] for s in OpsWallState().snapshot(now=NOW)["services"]}
    assert charts == {"web-gateway": ["errors"], "checkout": [], "payments": ["latency", "traffic"]}


def test_checkout_has_no_charts_and_its_card_stays_healthy():
    # D-068: checkout produces tickets, not metrics, so its card has a header and a status and
    # nothing to draw. Its row comes with the wall's new frame.
    st = OpsWallState()
    _health(st, "payments", NOW - 10, lat="HIGH")
    s = _svc(st, "checkout")
    assert s["charts"] == [] and s["metrics"] == [] and s["deploys"] == []
    assert (s["status"], s["statusText"]) == ("healthy", "Healthy")


def test_an_error_rate_breach_is_a_block_on_the_errors_chart_owned_by_its_bad_deploy():
    st = OpsWallState()
    _incident(st, "web-gateway", T0, T0 + 11, incident_type="BAD_DEPLOY_ERRORS")
    _error_rate(st, T0 + 10, "HIGH")
    _error_rate(st, T0 + 20)
    _decide(st, "web-gateway", T0 + 10, at=T0 + 10 + DETECT + ANSWER, trigger="ERROR_RATE_HIGH", target="3.4.3")
    s = _svc(st, "web-gateway")
    [block] = s["blocks"]
    assert (block["metric"], block["trigger"], block["direction"]) == ("errors", "ERROR_RATE_HIGH", "up")
    assert (block["incidentId"], block["open"], block["action"]) == ("INC0001", False, "ROLLBACK")
    assert block["timeToDetect"] == pytest.approx(10 + DETECT)
    [truth] = s["groundTruth"]
    assert (truth["metric"], truth["verdict"]) == ("errors", "answered")


def test_the_tile_waits_for_the_agent_on_an_error_rate_breach():
    st = OpsWallState()
    _error_rate(st, NOW - 10, "HIGH", detected_at=NOW - 4)
    s = _svc(st, "web-gateway")
    assert s["status"] == "anomaly"
    assert s["statusText"] == "Anomaly detected: error rate high, waiting for the agent"


def test_a_latency_decision_does_not_close_an_error_rate_block():
    st = OpsWallState()
    _error_rate(st, NOW - 10, "HIGH", detected_at=NOW - 4)
    _decide(st, "web-gateway", NOW - 10, at=NOW - 1, trigger="LATENCY_HIGH")
    [block] = _svc(st, "web-gateway")["blocks"]
    assert block["open"]


def test_error_rate_rows_and_health_rows_do_not_overwrite_each_other():
    # Two tables, two row sources: the same window in both keeps both verdicts.
    st = OpsWallState()
    _error_rate(st, T0 + 10, "HIGH")
    _health(st, "web-gateway", T0 + 10)  # an old service_health row for the same window
    _decide(st, "web-gateway", T0 + 10, at=T0 + 20, trigger="ERROR_RATE_HIGH")
    assert [b["trigger"] for b in _svc(st, "web-gateway")["blocks"]] == ["ERROR_RATE_HIGH"]


def test_an_error_rate_window_written_twice_keeps_its_first_detection_and_the_newest_status():
    st = OpsWallState()
    _error_rate(st, T0 + 10, "HIGH", detected_at=T0 + 16)
    _error_rate(st, T0 + 10, "HIGH", detected_at=T0 + 40)  # the INSERT ran again
    _decide(st, "web-gateway", T0 + 10, at=T0 + 50, trigger="ERROR_RATE_HIGH")
    [block] = _svc(st, "web-gateway")["blocks"]
    assert block["start"] == pytest.approx(_pos(T0 + 16))
    _error_rate(st, T0 + 10, "NORMAL", detected_at=T0 + 60)
    assert _svc(st, "web-gateway")["blocks"] == []


def test_the_errors_chart_gets_the_error_share_of_each_second():
    st = OpsWallState()
    st.apply_metric({"service_name": "web-gateway", "event_time": _dt(NOW - 3.001), "traffic_count": 50,
                     "latency_ms": 35.0, "error_count": 20})
    st.apply_metric({"service_name": "web-gateway", "event_time": _dt(NOW - 2.001), "traffic_count": 0,
                     "latency_ms": 35.0, "error_count": 0})
    st.apply_metric({"service_name": "web-gateway", "event_time": _dt(NOW - 1.001), "traffic_count": 48,
                     "latency_ms": 35.0, "error_count": None})
    _sample(st, "payments", NOW - 1.001)
    shares = [m["errorPct"] for m in _svc(st, "web-gateway")["metrics"]]
    assert shares == [40.0, None, 0.0]  # a second with no requests is a gap, not 0 %
    assert "errorPct" not in _svc(st, "payments")["metrics"][0]  # only where a chart reads it


def test_deploys_and_rollbacks_become_marks_and_heartbeats_do_not():
    st = OpsWallState()
    _deployment(st, "web-gateway", NOW - 500, "INITIAL", "3.4.2", None)  # the bootstrap record
    _deployment(st, "web-gateway", T0, "DEPLOY", "3.4.3", "3.4.2")
    _deployment(st, "web-gateway", T0, "DEPLOY", "3.4.3", "3.4.2")  # its 5-s heartbeat
    _deployment(st, "web-gateway", T0 + 11, "ROLLBACK", "3.4.2", "3.4.3")
    _deployment(st, "payments", T0, "DEPLOY", "2.9.5", "2.9.4")
    marks = _svc(st, "web-gateway")["deploys"]
    assert [(m["kind"], m["previousVersion"], m["version"]) for m in marks] == [
        ("DEPLOY", "3.4.2", "3.4.3"),
        ("ROLLBACK", "3.4.3", "3.4.2"),
    ]
    assert [m["t"] for m in marks] == pytest.approx([_pos(T0), _pos(T0 + 11)])
    assert len({m["key"] for m in marks}) == 2
    # Only a card whose chart shows them gets marks; the review of service_deployments
    # decides about the others (opswall-redesign issue 01).
    assert _svc(st, "payments")["deploys"] == []


def test_trim_drops_old_error_rate_rows_and_deploy_marks():
    st = OpsWallState()
    old = NOW - AXIS_SECONDS - 120
    _error_rate(st, old, "HIGH")
    _deployment(st, "web-gateway", old, "DEPLOY", "3.4.3", "3.4.2")
    st.trim(NOW)
    with st._lock:
        s = st._services["web-gateway"]
        assert s.error_rates == [] and s.deploys == []


def test_the_threshold_line_is_lab_2_2s_rule():
    import re
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[1] / "sql/2.2_error_rate.sql").read_text()
    (share,) = re.findall(r"error_count >= ([\d.]+) \* traffic_count", sql)
    assert ERROR_RATE_THRESHOLD_PCT == pytest.approx(100 * float(share))
    assert OpsWallState().snapshot(now=NOW)["errorThresholdPct"] == ERROR_RATE_THRESHOLD_PCT
