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

from scripts.opswall.state import CHART_SECONDS, ERROR_RATE_THRESHOLD_PCT, HISTORY_SECONDS, OpsWallState
from scripts.patchagent.reply import commit_for, format_reply

NOW = 1_800_000_000.0  # fixed epoch so snapshots are deterministic; a multiple of 10
T0 = NOW - 100         # incidents start here, on a window boundary, on the 2-minute chart
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
    """Where absolute time `t` sits on the chart of a snapshot taken at `now`."""
    return t - (now - CHART_SECONDS)


# --- plumbing -----------------------------------------------------------------


def test_services_are_preseeded():
    snap = OpsWallState().snapshot(now=NOW)
    # the wall's order, then the "Code security incidents" row, which is no service (D-070)
    assert [s["name"] for s in snap["services"]] == ["web-gateway", "checkout", "payments", "Code security incidents"]
    names = {s["name"]: s for s in snap["services"]}
    assert names["checkout"]["status"] == "healthy"
    assert "maxReplicas" not in names["payments"]  # D-043: no ceiling


def test_the_running_version_is_the_latest_deployment_records_the_heartbeat_included():
    st = OpsWallState()
    assert _svc(st, "web-gateway")["version"] is None  # before the first record
    # The wall started after the last deploy: the 5-s heartbeat alone says what runs.
    st.apply_deployment({"service_name": "web-gateway", "version": "3.4.2", "previous_version": None,
                         "last_change": "INITIAL", "deployed_at": _dt(NOW - 500)})
    assert _svc(st, "web-gateway")["version"] == "3.4.2"
    _deployment(st, "web-gateway", T0, "DEPLOY", "3.4.3", "3.4.2")
    assert _svc(st, "web-gateway")["version"] == "3.4.3"
    _deployment(st, "web-gateway", T0 + 11, "ROLLBACK", "3.4.2", "3.4.3")
    _deployment(st, "web-gateway", T0 + 11, "ROLLBACK", "3.4.2", "3.4.3")  # its heartbeat
    assert _svc(st, "web-gateway")["version"] == "3.4.2"


def test_a_metric_row_carries_none_of_the_deployment_leftovers():
    # Nothing on the page read them (opswall-redesign issue 01); the version and the Releases
    # list are what the design shows, on the releases row alone.
    st = OpsWallState()
    _deployment(st, "web-gateway", T0, "DEPLOY", "3.4.3", "3.4.2")
    _deployment(st, "payments", T0, "DEPLOY", "2.9.5", "2.9.4")
    for name in ("web-gateway", "payments"):
        assert not {"previousVersion", "lastChange", "secondsSinceDeploy"} & set(_svc(st, name))
    assert not {"version", "releases"} & set(_svc(st, "payments"))


def test_unknown_service_is_silently_ignored():
    st = OpsWallState()
    st.apply_deployment({"service_name": "not-a-real-service", "version": "1.0.0"})
    st.apply_health({"service_name": "nope", "window_end": _dt(NOW)})
    st.apply_decision({"service_name": "nope"})
    st.apply_incident({"service_name": "nope", "incident_id": "X"})
    _sample(st, "nope", NOW - 1)
    assert [s["name"] for s in st.snapshot(now=NOW)["services"]] == [
        "web-gateway", "checkout", "payments", "Code security incidents"]


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


def test_trim_drops_the_samples_older_than_the_history_plus_the_margin():
    st = OpsWallState()
    _sample(st, "payments", NOW - 670, traffic=1)
    _sample(st, "payments", NOW - 650, traffic=2)
    st.trim(NOW)  # keeps the last 600 + 60 s
    # A snapshot whose chart still reaches back 680 s would show the dropped one.
    assert [m["traffic"] for m in _svc(st, "payments", now=NOW - 560)["metrics"]] == [2]


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
    _incident(st, "payments", NOW - 100)
    [gt] = _svc(st, "payments")["groundTruth"]
    assert gt["open"] is True and gt["metric"] == "latency"

    _incident(st, "payments", NOW - 100, NOW - 60)
    [gt] = _svc(st, "payments")["groundTruth"]
    assert gt["open"] is False
    assert gt["start"] == pytest.approx(CHART_SECONDS - 100)
    assert gt["end"] == pytest.approx(CHART_SECONDS - 60)


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
        assert block["end"] == CHART_SECONDS  # at the right edge, i.e. now
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
    window_end is in the history count."""
    st = OpsWallState()
    old = NOW - HISTORY_SECONDS - 60
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
    s = _svc(st, "web-gateway")
    [block] = s["blocks"]
    assert block["incidentId"] is None
    assert (s["issues"]["injected"], s["issues"]["detected"]) == (0, 0)  # the Issues panel ignores it


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


# --- the Issues panel (D-068) ------------------------------------------------------


def test_each_service_counts_its_own_issues_over_the_last_10_minutes():
    st = OpsWallState()
    # payments: a bad deploy detected 16 s after it started, answered 8 s later
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", T0 + 10, lat="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 24)
    issues = {s["name"]: s["issues"] for s in st.snapshot(now=NOW)["services"]}
    assert issues["payments"] == {
        "injected": 1, "detected": 1, "missed": 0,
        "medianTimeToDetect": pytest.approx(16.0), "medianTimeToRemediate": pytest.approx(24.0),
        "medianTimeToDetectText": "16s", "medianTimeToRemediateText": "24s",
    }
    nothing = {"injected": 0, "detected": 0, "missed": 0, "medianTimeToDetect": None, "medianTimeToRemediate": None,
               "medianTimeToDetectText": "—", "medianTimeToRemediateText": "—"}
    assert issues["web-gateway"] == nothing
    assert issues["checkout"] == nothing


def test_time_to_remediate_runs_from_the_incidents_start_to_its_answer():
    # The old summary measured detection -> answer (8 s and 12 s here); remediation is start -> answer.
    st = OpsWallState()
    # detected 16 s after the start, answered 8 s later: 24 s
    _incident(st, "payments", T0, T0 + 40, iid="INC-A")
    _health(st, "payments", T0 + 10, lat="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 24)
    # a drop detected 26 s after the start, answered 12 s later: 38 s
    _incident(st, "payments", T0 + 50, T0 + 90, incident_type="UPSTREAM_DROP", iid="INC-B")
    _health(st, "payments", T0 + 70, trf="LOW")
    _decide(st, "payments", T0 + 70, at=T0 + 88, action="INVESTIGATE_UPSTREAM", trigger="TRAFFIC_LOW",
            target=None, agent="capacity_agent")
    # detected, not answered yet: counted, but in neither median
    _incident(st, "payments", NOW - 20, iid="INC-C")
    _health(st, "payments", NOW - 10, lat="HIGH")
    issues = _svc(st, "payments")["issues"]
    assert (issues["injected"], issues["detected"], issues["missed"]) == (3, 3, 0)
    assert issues["medianTimeToDetect"] == pytest.approx(21.0)
    assert issues["medianTimeToRemediate"] == pytest.approx(31.0)


def _two_runs_on_one_incident(st):
    """A bad deploy flagged in two runs, its first detected at T0 + 16 and its second at T0 + 36;
    the second run is answered at T0 + 38."""
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", T0 + 10, lat="HIGH")
    _health(st, "payments", T0 + 20)  # a normal window ends the first run
    _health(st, "payments", T0 + 30, lat="HIGH")
    _decide(st, "payments", T0 + 30, at=T0 + 38)


def test_time_to_remediate_is_the_first_blocks_time_to_detect_plus_its_time_to_answer():
    # Spec decision 29: incident start -> its answer, timeToDetect + timeToAnswer, which are both
    # the first block's. A later block answered sooner doesn't shorten it.
    st = OpsWallState()
    _two_runs_on_one_incident(st)
    _decide(st, "payments", T0 + 10, at=T0 + 45)  # the first run is answered last
    s = _svc(st, "payments")
    [gt] = s["groundTruth"]
    assert gt["timeToDetect"] + gt["timeToAnswer"] == pytest.approx(45.0)
    assert s["issues"]["medianTimeToRemediate"] == pytest.approx(45.0)


def test_while_the_first_block_waits_unanswered_time_to_remediate_runs_to_the_first_answer():
    # The first block has no timeToAnswer until it is answered or removed (30 s after its last
    # detection), so the spec's sum is undefined: start -> the first answer, the second run's.
    st = OpsWallState()
    _two_runs_on_one_incident(st)
    s = _svc(st, "payments", now=T0 + 40)
    [gt] = s["groundTruth"]
    assert (gt["verdict"], gt["timeToDetect"], gt["timeToAnswer"]) == ("answered", pytest.approx(16.0), None)
    assert s["issues"]["medianTimeToRemediate"] == pytest.approx(38.0)


def test_a_missed_incident_counts_as_missed_and_not_detected():
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)
    # Flagged from the first window on and never answered. The run outlasts the incident, so at
    # T0 + 80 its block is still drawn (last detection T0 + 76) while the incident is missed.
    for w in range(10, 80, 10):
        _health(st, "payments", T0 + w, lat="HIGH")
    s = _svc(st, "payments", now=T0 + 80)
    assert s["groundTruth"][0]["verdict"] == "missed" and s["blocks"][0]["incidentId"] == "INC0001"
    assert s["issues"] == {"injected": 1, "detected": 0, "missed": 1,
                           "medianTimeToDetect": None, "medianTimeToRemediate": None,
                           "medianTimeToDetectText": "—", "medianTimeToRemediateText": "—"}


def test_an_open_incident_counts_as_injected_but_neither_detected_nor_missed():
    st = OpsWallState()
    _incident(st, "web-gateway", NOW - 5, incident_type="BAD_DEPLOY_ERRORS")
    issues = _svc(st, "web-gateway")["issues"]
    assert (issues["injected"], issues["detected"], issues["missed"]) == (1, 0, 0)


def test_only_incidents_that_started_in_the_last_10_minutes_count():
    st = OpsWallState()
    # ended before the window
    _incident(st, "payments", NOW - 700, NOW - 660, iid="OLD")
    # started 10 s before the window and ended in it, answered: not in the last 10 minutes
    _incident(st, "payments", NOW - 610, NOW - 575, iid="EDGE")
    _health(st, "payments", NOW - 590, lat="HIGH")
    _decide(st, "payments", NOW - 590, at=NOW - 580)
    # started in the window: counted
    _incident(st, "payments", T0, T0 + 40, incident_type="UPSTREAM_DROP", iid="IN")
    s = _svc(st, "payments")
    assert [g["incidentId"] for g in s["groundTruth"]] == ["IN"]  # the one on the 2-minute chart
    assert s["issues"] == {"injected": 1, "detected": 0, "missed": 1,
                           "medianTimeToDetect": None, "medianTimeToRemediate": None,
                           "medianTimeToDetectText": "—", "medianTimeToRemediateText": "—"}


def test_the_snapshot_has_no_global_summary():
    # The Issues panels replace the summary table; false alarms are evaluate's to report (D-068).
    st = OpsWallState()
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", T0 + 10, lat="HIGH")
    _decide(st, "payments", T0 + 10, at=T0 + 24)
    assert "summary" not in st.snapshot(now=NOW)


def test_checkouts_panel_counts_tickets_and_never_incidents():
    st = OpsWallState()
    _incident(st, "checkout", T0, T0 + 40)  # never happens (D-068): checkout's numbers come from tickets
    assert _svc(st, "checkout")["issues"] == {"injected": 0, "detected": 0, "missed": 0,
                                              "medianTimeToDetect": None, "medianTimeToRemediate": None,
                                              "medianTimeToDetectText": "—", "medianTimeToRemediateText": "—"}


def test_the_issues_durations_come_in_words_as_the_design_writes_them():
    st = OpsWallState()
    _story(st, "A", NOW - 300, verdict="HIGH", verdict_after=65, answer_after=130)
    issues = _row(st)["issues"]
    assert (issues["medianTimeToDetectText"], issues["medianTimeToRemediateText"]) == ("1m 05s", "2m 10s")
    _story(st, "B", NOW - 200, verdict="HIGH", verdict_after=4, answer_after=10)
    issues = _row(st)["issues"]  # medians of 65 and 4, 130 and 10: a half rounds up, as the page did
    assert (issues["medianTimeToDetectText"], issues["medianTimeToRemediateText"]) == ("35s", "1m 10s")
    _story(st, "C", NOW - 100, verdict="HIGH", verdict_after=4.5)
    assert _row(st)["issues"]["medianTimeToDetectText"] == "5s"  # 4.5, the middle of 4, 4.5 and 65


# --- trim --------------------------------------------------------------------


def test_trim_drops_entries_older_than_the_history():
    st = OpsWallState()
    old = NOW - HISTORY_SECONDS * 2
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
    _incident(st, "web-gateway", NOW - 670, iid="ORPHAN", incident_type="BAD_DEPLOY_ERRORS")
    _incident(st, "payments", NOW - 650, iid="RECENT", incident_type="UPSTREAM_DROP")
    assert _svc(st, "web-gateway")["groundTruth"][0]["verdict"] == "pending"
    st.trim(NOW)
    assert _svc(st, "web-gateway")["groundTruth"] == []
    assert [g["incidentId"] for g in _svc(st, "payments")["groundTruth"]] == ["RECENT"]


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


def test_each_metric_row_names_its_charts():
    charts = {s["name"]: s["charts"] for s in OpsWallState().snapshot(now=NOW)["services"]
              if s["row"] not in ("tickets", "security")}
    assert charts == {"web-gateway": ["errors"], "payments": ["latency", "traffic"]}


def test_checkouts_row_carries_what_a_ticket_row_draws_and_nothing_of_a_metric_row():
    # D-068: checkout produces tickets, not metrics: no charts, samples, deploys, blocks or
    # incidents, only its header, its Issues panel, the sources, the list and the timeline.
    st = OpsWallState()
    _health(st, "payments", NOW - 10, lat="HIGH")
    s = _svc(st, "checkout")
    assert set(s) == {"name", "row", "status", "statusText", "issues", "sources", "tickets",
                      "marks", "runs", "lanes", "agentsText", "agentsWorking", "openP1"}
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
    # payments' charts draw its own: its bad deploys and their rollbacks (opswall-redesign 03).
    assert [(m["kind"], m["version"]) for m in _svc(st, "payments")["deploys"]] == [("DEPLOY", "2.9.5")]


def test_trim_drops_old_error_rate_rows_and_deploy_marks():
    st = OpsWallState()
    old = NOW - HISTORY_SECONDS - 120
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


# --- the 2-minute charts and web-gateway's Releases list (D-068) -----------------------


def test_the_charts_show_the_last_2_minutes_and_the_issues_panel_the_last_10():
    st = OpsWallState()
    assert st.snapshot(now=NOW)["chartSeconds"] == CHART_SECONDS == 120
    for iid, start in (("OLD", NOW - 300), ("NEW", NOW - 60)):  # both answered, one off the chart
        _incident(st, "payments", start, start + 35, incident_type="UPSTREAM_DROP", iid=iid)
        _health(st, "payments", start + 10, trf="LOW")
        _health(st, "payments", start + 20)  # back to normal: the run ends
        _decide(st, "payments", start + 10, at=start + 10 + DETECT + ANSWER, action="INVESTIGATE_UPSTREAM",
                trigger="TRAFFIC_LOW")
    _sample(st, "payments", NOW - 130.001, traffic=1)
    _sample(st, "payments", NOW - 1.001, traffic=2)
    _deployment(st, "web-gateway", NOW - 130, "DEPLOY", "3.4.3", "3.4.2")
    _deployment(st, "web-gateway", NOW - 50, "DEPLOY", "3.4.4", "3.4.3")

    s = _svc(st, "payments")
    assert [g["incidentId"] for g in s["groundTruth"]] == ["NEW"]
    assert [b["incidentId"] for b in s["blocks"]] == ["NEW"]
    assert s["groundTruth"][0]["start"] == pytest.approx(60)
    assert [m["traffic"] for m in s["metrics"]] == [2]
    assert (s["issues"]["injected"], s["issues"]["detected"]) == (2, 2)
    assert [(d["version"], d["t"]) for d in _svc(st, "web-gateway")["deploys"]] == [("3.4.4", pytest.approx(70))]


def test_a_block_open_since_before_the_chart_stays_on_it():
    st = OpsWallState()
    for w in range(int(NOW) - 150, int(NOW) + 1, 10):  # flagged for 2.5 minutes, unanswered
        _health(st, "payments", w, lat="HIGH", detected_at=w + DETECT if w + DETECT <= NOW else None)
    [block] = _svc(st, "payments")["blocks"]
    assert block["open"] and (block["start"], block["end"]) == (0.0, CHART_SECONDS)


def test_the_releases_list_has_the_newest_5_deploys_of_the_last_10_minutes_and_their_count():
    st = OpsWallState()
    _deployment(st, "web-gateway", NOW - 610, "DEPLOY", "3.4.3", "3.4.2")  # before the last 10 minutes
    for i, t in enumerate((NOW - 590, NOW - 500, NOW - 420, NOW - 340, NOW - 260, NOW - 180)):
        _deployment(st, "web-gateway", t, "DEPLOY", f"3.4.{4 + i}", f"3.4.{3 + i}")
    releases = _svc(st, "web-gateway")["releases"]
    assert releases["count"] == 6
    assert [r["version"] for r in releases["newest"]] == ["3.4.9", "3.4.8", "3.4.7", "3.4.6", "3.4.5"]
    assert releases["newest"][0]["deployedAt"] == NOW - 180  # absolute: the list prints the date
    assert len({r["key"] for r in releases["newest"]}) == 5


def test_a_rolled_back_release_says_how_long_it_ran_and_the_version_it_went_back_to_says_nothing():
    st = OpsWallState()
    _deployment(st, "web-gateway", NOW - 200, "DEPLOY", "3.4.3", "3.4.2")
    _deployment(st, "web-gateway", NOW - 120, "DEPLOY", "3.4.4", "3.4.3")
    _deployment(st, "web-gateway", NOW - 98, "ROLLBACK", "3.4.3", "3.4.4")
    _deployment(st, "web-gateway", NOW - 98, "ROLLBACK", "3.4.3", "3.4.4")  # its heartbeat
    releases = _svc(st, "web-gateway")["releases"]
    assert [(r["version"], r["rolledBackAfter"], r["rolledBackAfterText"]) for r in releases["newest"]] == [
        ("3.4.4", 22.0, "22s"),
        ("3.4.3", None, None),  # 3.4.4's rollback went back to it: it was never rolled back
    ]
    assert releases["count"] == 2  # a rollback is no release


def test_a_rollback_whose_deploy_is_older_than_the_list_marks_nothing():
    st = OpsWallState()
    _deployment(st, "web-gateway", NOW - 590, "ROLLBACK", "3.4.2", "3.4.3")  # 3.4.3 was deployed before
    _deployment(st, "web-gateway", NOW - 100, "DEPLOY", "3.4.4", "3.4.2")
    releases = _svc(st, "web-gateway")["releases"]
    assert [(r["version"], r["rolledBackAfter"]) for r in releases["newest"]] == [("3.4.4", None)]


# --- payments' aggregated records: the service_health windows (D-068) ---------------------


def _window(st, window_end, lat="NORMAL", trf="NORMAL", **extra):
    """One service_health row with the columns the record line shows."""
    fields = {"traffic_count": 300, "avg_latency_ms": 231.44, "version": "2.9.4", "previous_version": None,
              "last_change": "INITIAL", **extra}
    _health(st, "payments", window_end, lat=lat, trf=trf, **fields)


def test_payments_row_says_its_window_and_carries_the_newest_7_windows_newest_first():
    st = OpsWallState()
    for k in range(9):
        _window(st, NOW - 90 + 10 * k, traffic_count=300 + k)
    row = _svc(st, "payments")
    assert row["windowSeconds"] == 10
    newest = row["windows"]["newest"]
    assert [w["windowEnd"] for w in newest] == [NOW - 10 * k for k in range(1, 8)]
    assert [w["record"]["traffic_count"] for w in newest] == [308, 307, 306, 305, 304, 303, 302]
    assert len({w["key"] for w in newest}) == 7


def test_a_window_keeps_its_whole_record_with_its_times_in_epoch_seconds():
    st = OpsWallState()
    _window(st, NOW - 10, version="2.9.5", previous_version="2.9.4", last_change="DEPLOY")
    [w] = _svc(st, "payments")["windows"]["newest"]
    record = w["record"]
    assert record["service_name"] == "payments"
    assert (record["window_start"], record["window_end"]) == (NOW - 20, NOW - 10)
    assert record["detected_at"] == NOW - 10 + DETECT
    assert (record["avg_latency_ms"], record["traffic_count"], record["version"]) == (231.44, 300, "2.9.5")
    assert (record["latency_status"], record["traffic_status"]) == ("NORMAL", "NORMAL")
    assert record["previous_version"] == "2.9.4"
    assert list(record)[:3] == ["service_name", "window_start", "window_end"]  # in the record's order


def test_a_window_written_twice_shows_the_newer_record_and_its_first_detection():
    st = OpsWallState()
    _window(st, NOW - 10, avg_latency_ms=230.0, detected_at=NOW - 4)
    _window(st, NOW - 10, avg_latency_ms=990.0, lat="HIGH", detected_at=NOW - 1)
    [w] = _svc(st, "payments")["windows"]["newest"]
    assert (w["record"]["avg_latency_ms"], w["record"]["latency_status"]) == (990.0, "HIGH")
    assert w["record"]["detected_at"] == NOW - 4


def test_a_window_flagged_high_or_low_names_its_statuses_and_their_values_and_counts_over_10_minutes():
    st = OpsWallState()
    _window(st, NOW - 620, lat="HIGH")                # before the last 10 minutes
    _window(st, NOW - 300, lat="HIGH", trf="HIGH")
    _window(st, NOW - 30, trf="LOW")
    _window(st, NOW - 20, lat="WARMING_UP", trf="WARMING_UP")
    _window(st, NOW - 10)
    windows = _svc(st, "payments")["windows"]
    assert windows["flagged"] == 2
    assert [(w["windowEnd"], w["flags"]) for w in windows["newest"]][:4] == [
        (NOW - 10, []), (NOW - 20, []), (NOW - 30, ["traffic_count", "traffic_status"]),
        (NOW - 300, ["avg_latency_ms", "latency_status", "traffic_count", "traffic_status"])]


def test_only_the_windows_row_carries_windows():
    snap = OpsWallState().snapshot(now=NOW)
    assert [s["name"] for s in snap["services"] if "windows" in s] == ["payments"]
    assert _svc(OpsWallState(), "payments")["windows"] == {"flagged": 0, "newest": []}


# --- checkout's row: tickets and agent runs (D-068) ------------------------------------

ZD = "ZENDESK"
SN = "SERVICENOW"


def _ticket(st, tid, t, priority="P1", source=ZD, subject=None):
    """One support_tickets record, as ops-sim writes it (event_time a TIMESTAMP(3))."""
    st.apply_ticket({"event_time": _dt(t), "ticket_id": tid, "source": source, "service_name": "checkout",
                     "priority": priority, "subject": subject or f"Subject of {tid}", "description": "Text."})


def _row(st, now=NOW):
    return _svc(st, "checkout", now)


def test_the_snapshot_says_which_kind_of_row_each_service_gets():
    # From opsgen.services' split (issue 02), so the page holds no service names.
    rows = {s["name"]: s["row"] for s in OpsWallState().snapshot(now=NOW)["services"]}
    assert rows == {"web-gateway": "releases", "checkout": "tickets", "payments": "windows",
                    "Code security incidents": "security"}  # D-070: beside the services, not one of them


def test_the_list_has_the_newest_5_tickets_and_the_count_of_the_last_10_minutes():
    st = OpsWallState()
    _ticket(st, "ZD-48200", NOW - 601)                       # 10 minutes and a second ago: not received
    _ticket(st, "ZD-48201", NOW - 599, priority="P3")
    for k, tid in enumerate(["INC0041872", "ZD-48202", "INC0041873", "ZD-48203", "ZD-48204"]):
        _ticket(st, tid, NOW - 50 + 10 * k, priority=("P1", "P2", "P3")[k % 3], source=SN if tid[0] == "I" else ZD)
    tickets = _row(st)["tickets"]
    assert tickets["received"] == 6
    assert [t["id"] for t in tickets["newest"]] == ["ZD-48204", "ZD-48203", "INC0041873", "ZD-48202", "INC0041872"]
    assert tickets["newest"][2] == {"id": "INC0041873", "source": SN, "sourceName": "ServiceNow", "priority": "P3",
                                    "subject": "Subject of INC0041873", "eventTime": pytest.approx(NOW - 30)}


def test_the_ticket_row_names_its_two_sources_in_the_order_the_page_draws_them():
    # opsgen's table of sources, so the page holds no source names (D-068).
    assert _row(OpsWallState())["sources"] == [{"key": ZD, "name": "Zendesk"}, {"key": SN, "name": "ServiceNow"}]


def test_every_ticket_of_the_last_2_minutes_is_a_mark_on_the_timeline():
    st = OpsWallState()
    _ticket(st, "ZD-48201", NOW - 121)                      # just off the timeline
    _ticket(st, "ZD-48202", NOW - 120, priority="P2")
    _ticket(st, "INC0041872", NOW - 30, priority="P3", source=SN)
    _ticket(st, "ZD-48203", NOW - 5)
    row = _row(st)
    assert st.snapshot(now=NOW)["chartSeconds"] == 120  # the timeline is the charts' 2 minutes
    # t is the position on the timeline: 0 at its left edge, 120 at now
    assert row["marks"] == [
        {"id": "ZD-48202", "priority": "P2", "t": pytest.approx(0.0)},
        {"id": "INC0041872", "priority": "P3", "t": pytest.approx(90.0)},
        {"id": "ZD-48203", "priority": "P1", "t": pytest.approx(115.0)},
    ]


TK = NOW - 60  # a ticket created a minute ago


def _fields(tid, t, priority="P1", source=ZD):
    """The seven ticket columns, which p1_tickets and ticket_triage copy from support_tickets."""
    return {"event_time": _dt(t), "ticket_id": tid, "source": source, "service_name": "checkout",
            "priority": priority, "subject": f"Subject of {tid}", "description": "Text."}


def _p1(st, tid, at, t=TK):
    """Lab 5.1's p1_tickets record; `at` is its Kafka timestamp."""
    st.apply_p1_ticket(_fields(tid, t), kafka_ts=at)


def _triage(st, tid, at, severity="HIGH", reason="Many customers cannot pay.", t=TK):
    """Lab 5.2's ticket_triage record; `at` is its Kafka timestamp."""
    st.apply_triage({**_fields(tid, t), "severity": severity, "reason": reason,
                     "raw_response": f"Severity: {severity}\nReason: {reason}"}, kafka_ts=at)


def _truth(st, tid, severity="HIGH", action="SCALE_OUT", t=TK):
    """The simulator's ticket_answer_key record."""
    st.apply_answer_key({"event_time": _dt(t), "ticket_id": tid, "true_severity": severity,
                         "expected_action": action if severity == "HIGH" else None})


def _respond(st, tid, at, action="SCALE_OUT", reason="Checkout times out under the sale.", t=TK):
    """Lab 5.3's decision in remediation_decisions: window_end is the ticket's event_time."""
    st.apply_decision({"window_end": _dt(t), "service_name": "checkout", "agent_name": "ticket_responder",
                       "trigger": "HIGH_SEVERITY_TICKET", "action": action, "target": None, "reason": reason,
                       "raw_response": f"Action: {action}\nReason: {reason}", "ticket_id": tid}, kafka_ts=at)


def _tpos(t, now=NOW):
    """Where absolute time `t` sits on the 2-minute timeline of a snapshot taken at `now`."""
    return t - (now - 120)


def test_a_p1_gets_a_classifier_run_and_a_high_verdict_a_responder_run():
    st = OpsWallState()
    _ticket(st, "ZD-48215", TK)
    _p1(st, "ZD-48215", at=TK + 1)
    _triage(st, "ZD-48215", at=TK + 5)
    _respond(st, "ZD-48215", at=TK + 11)
    classifier, responder = _row(st)["runs"]
    assert classifier == {
        "key": "classifier@ZD-48215", "lane": "classifier", "ticketId": "ZD-48215", "priority": "P1",
        "subject": "Subject of ZD-48215", "start": pytest.approx(_tpos(TK + 1)), "end": pytest.approx(_tpos(TK + 5)),
        "open": False, "label": "High in 4s", "outcome": "High", "reason": "Many customers cannot pay.",
    }
    assert responder == {
        "key": "responder@ZD-48215", "lane": "responder", "ticketId": "ZD-48215", "priority": "P1",
        "subject": "Subject of ZD-48215", "start": pytest.approx(_tpos(TK + 5)), "end": pytest.approx(_tpos(TK + 11)),
        "open": False, "label": "Scale out in 6s", "outcome": "Scale out",
        "reason": "Checkout times out under the sale.",
    }


@pytest.mark.parametrize("severity", ["LOW", "INVALID"])
def test_a_low_or_invalid_verdict_stops_after_the_classifier(severity):
    st = OpsWallState()
    _ticket(st, "ZD-48215", TK)
    _p1(st, "ZD-48215", at=TK + 1)
    _triage(st, "ZD-48215", at=TK + 4, severity=severity)
    [run] = _row(st)["runs"]
    assert (run["lane"], run["label"]) == ("classifier", f"{severity.capitalize()} in 3s")


def test_the_snapshot_carries_the_action_words_for_every_tooltip_on_the_page():
    # One table for the responder's bars and the metric rows' tooltips, so "Roll back" reads the
    # same everywhere (the bars' "Roll back in 5s", issue 09).
    assert OpsWallState().snapshot(now=NOW)["actionWords"] == {
        "ROLLBACK": "Roll back", "SCALE_OUT": "Scale out", "INVESTIGATE_UPSTREAM": "Investigate upstream",
        "OBSERVE": "Observe", "INVALID": "Invalid",
    }


@pytest.mark.parametrize(("action", "label"), [
    ("ROLLBACK", "Roll back in 5s"),
    ("SCALE_OUT", "Scale out in 5s"),
    ("INVESTIGATE_UPSTREAM", "Investigate upstream in 5s"),
    ("INVALID", "Invalid in 5s"),
])
def test_a_responder_bar_reads_its_action_in_words(action, label):
    st = OpsWallState()
    _p1(st, "ZD-48215", at=TK + 1)
    _triage(st, "ZD-48215", at=TK + 4)
    _respond(st, "ZD-48215", at=TK + 9.4, action=action)
    assert _row(st)["runs"][1]["label"] == label


def test_an_open_run_reads_working_and_after_30_s_no_answer_and_is_no_longer_open():
    st = OpsWallState()
    _ticket(st, "ZD-48215", TK)
    _p1(st, "ZD-48215", at=TK + 1)
    [run] = _row(st, now=TK + 4.2)["runs"]
    assert (run["open"], run["label"], run["outcome"]) == (True, "Working 3s", None)
    assert run["end"] == 120  # at now, the right edge
    [run] = _row(st, now=TK + 31)["runs"]
    assert (run["open"], run["label"]) == (True, "Working 30s")
    [run] = _row(st, now=TK + 31.5)["runs"]
    assert (run["open"], run["label"], run["outcome"]) == (False, "No answer", None)
    assert run["end"] == pytest.approx(_tpos(TK + 31, now=TK + 31.5))  # it ends 30 s after it started

    # The verdict comes, then the responder works on it, and gives up the same way.
    _triage(st, "ZD-48215", at=TK + 40)
    classifier, responder = _row(st, now=TK + 45)["runs"]
    assert (classifier["open"], classifier["label"]) == (False, "High in 39s")
    assert (responder["open"], responder["label"]) == (True, "Working 5s")
    assert _row(st, now=TK + 71)["runs"][1]["label"] == "No answer"


def test_two_runs_overlapping_in_one_lane_are_both_drawn():
    # Flink calls the agent per row, with no queue: the second ticket's run starts while the
    # first one's is still going, and the two bars draw over each other.
    st = OpsWallState()
    _p1(st, "ZD-48215", at=TK + 1)
    _p1(st, "INC0041872", at=TK + 3, t=TK + 2)
    _triage(st, "ZD-48215", at=TK + 7, severity="LOW")
    _triage(st, "INC0041872", at=TK + 6, severity="LOW", t=TK + 2)
    runs = _row(st)["runs"]
    assert [(r["key"], r["lane"], r["label"]) for r in runs] == [
        ("classifier@ZD-48215", "classifier", "Low in 6s"),
        ("classifier@INC0041872", "classifier", "Low in 3s"),
    ]
    assert runs[1]["start"] < runs[0]["end"]


def test_a_run_that_ended_before_the_timeline_is_left_out():
    st = OpsWallState()
    _p1(st, "ZD-48215", at=NOW - 130, t=NOW - 131)
    _triage(st, "ZD-48215", at=NOW - 121, t=NOW - 131)
    _respond(st, "ZD-48215", at=NOW - 110, t=NOW - 131)
    [responder] = _row(st)["runs"]
    assert (responder["lane"], responder["start"], responder["end"]) == ("responder", 0.0, pytest.approx(10.0))


def test_the_status_and_the_counters_follow_the_open_runs():
    st = OpsWallState()
    row = _row(st)
    assert (row["status"], row["statusText"], row["openP1"], row["agentsWorking"]) == ("healthy", "Healthy", 0, 0)

    _p1(st, "ZD-48215", at=NOW - 20)                 # the classifier works on it
    _p1(st, "INC0041872", at=NOW - 10, t=NOW - 11)   # and on this one
    row = _row(st)
    assert (row["status"], row["statusText"]) == ("working", "Agent resolving P1")
    assert (row["openP1"], row["agentsWorking"]) == (2, 1)

    _triage(st, "ZD-48215", at=NOW - 5)              # HIGH: the responder takes it over
    row = _row(st)
    assert (row["openP1"], row["agentsWorking"]) == (2, 2)

    _triage(st, "INC0041872", at=NOW - 4, severity="LOW", t=NOW - 11)
    _respond(st, "ZD-48215", at=NOW - 1)
    row = _row(st)
    assert (row["status"], row["statusText"], row["openP1"], row["agentsWorking"]) == ("healthy", "Healthy", 0, 0)


def test_each_lane_says_whether_its_agent_works_and_the_words_above_the_chart_count_them():
    st = OpsWallState()
    row = _row(st)
    assert row["lanes"] == [{"key": "classifier", "name": "Classifier", "working": False, "state": "Idle"},
                            {"key": "responder", "name": "Responder", "working": False, "state": "Idle"}]
    assert row["agentsText"] == "Both agents idle"

    _p1(st, "ZD-48215", at=NOW - 20)                 # the classifier works on it
    row = _row(st)
    assert [(lane["working"], lane["state"]) for lane in row["lanes"]] == [(True, "Working"), (False, "Idle")]
    assert row["agentsText"] == "1 of 2 agents working"

    _triage(st, "ZD-48215", at=NOW - 5)              # HIGH: the responder takes it over
    _p1(st, "INC0041872", at=NOW - 3, t=NOW - 4)     # and the classifier has a new one
    row = _row(st)
    assert [(lane["working"], lane["state"]) for lane in row["lanes"]] == [(True, "Working"), (True, "Working")]
    assert row["agentsText"] == "2 of 2 agents working"


def test_a_run_that_got_no_answer_no_longer_counts_as_open():
    st = OpsWallState()
    _p1(st, "ZD-48215", at=NOW - 31)
    row = _row(st)
    assert (row["status"], row["openP1"], row["agentsWorking"]) == ("healthy", 0, 0)


def _story(st, tid, t, truth="HIGH", priority="P1", verdict=None, verdict_after=None, answer_after=None):
    """A ticket created at `t`: its answer key, and the agents' answers that many seconds after."""
    _ticket(st, tid, t, priority=priority)
    _truth(st, tid, severity=truth, t=t)
    if priority == "P1":
        _p1(st, tid, at=t + 1, t=t)
    if verdict is not None:
        _triage(st, tid, at=t + verdict_after, severity=verdict, t=t)
    if answer_after is not None:
        _respond(st, tid, at=t + answer_after, t=t)


def test_checkouts_issues_count_the_really_high_p1_tickets_of_the_last_10_minutes():
    st = OpsWallState()
    _story(st, "A", NOW - 300, verdict="HIGH", verdict_after=4, answer_after=10)
    _story(st, "B", NOW - 250, verdict="HIGH", verdict_after=6, answer_after=20)
    _story(st, "J", NOW - 200, verdict="HIGH", verdict_after=8)                 # detected, not answered yet
    _story(st, "K", NOW - 50, verdict="HIGH", verdict_after=40)                 # detected late: still detected
    _story(st, "C", NOW - 150, verdict="LOW", verdict_after=3)                  # missed
    _story(st, "D", NOW - 140, verdict="INVALID", verdict_after=3)              # missed
    _story(st, "E", NOW - 31)                                                   # no verdict after 30 s: missed
    _story(st, "F", NOW - 29)                                                   # no verdict yet: injected only
    _story(st, "G", NOW - 100, truth="LOW", verdict="HIGH", verdict_after=3, answer_after=9)  # left to evaluate
    _story(st, "H", NOW - 90, truth="LOW", priority="P2")
    _story(st, "I", NOW - 601, verdict="HIGH", verdict_after=4, answer_after=10)  # created before the window
    assert _row(st)["issues"] == {
        "injected": 8, "detected": 4, "missed": 3,
        "medianTimeToDetect": pytest.approx(7.0),       # 4, 6, 8, 40: ticket -> HIGH verdict
        "medianTimeToRemediate": pytest.approx(15.0),   # 10, 20: ticket -> the responder's action
        "medianTimeToDetectText": "7s", "medianTimeToRemediateText": "15s",
    }


def test_a_ticket_fills_in_from_its_five_records_in_any_order():
    import itertools

    records = [
        lambda st: _ticket(st, "ZD-48215", TK),
        lambda st: _p1(st, "ZD-48215", at=TK + 1),
        lambda st: _triage(st, "ZD-48215", at=TK + 5),
        lambda st: _truth(st, "ZD-48215"),
        lambda st: _respond(st, "ZD-48215", at=TK + 11),
    ]
    for order in itertools.permutations(records):
        st = OpsWallState()
        for apply in order:
            apply(st)
        row = _row(st)
        assert row["tickets"]["newest"] == [{"id": "ZD-48215", "source": ZD, "sourceName": "Zendesk", "priority": "P1",
                                             "subject": "Subject of ZD-48215", "eventTime": pytest.approx(TK)}]
        assert [(r["label"], r["reason"]) for r in row["runs"]] == [
            ("High in 4s", "Many customers cannot pay."), ("Scale out in 6s", "Checkout times out under the sale.")]
        assert row["issues"] == {"injected": 1, "detected": 1, "missed": 0,
                                 "medianTimeToDetect": pytest.approx(5.0), "medianTimeToRemediate": pytest.approx(11.0),
                                 "medianTimeToDetectText": "5s", "medianTimeToRemediateText": "11s"}


def test_p2_and_p3_tickets_are_listed_and_marked_but_get_no_runs():
    st = OpsWallState()
    _ticket(st, "ZD-48215", TK, priority="P2")
    _truth(st, "ZD-48215", severity="LOW")
    _ticket(st, "INC0041872", TK + 1, priority="P3", source=SN)
    row = _row(st)
    assert [t["priority"] for t in row["tickets"]["newest"]] == ["P3", "P2"]
    assert [m["priority"] for m in row["marks"]] == ["P2", "P3"]
    assert row["runs"] == [] and row["issues"]["injected"] == 0


def test_a_ticket_decision_stays_out_of_the_metric_rows():
    st = OpsWallState()
    # payments waits for an agent on a latency breach
    _incident(st, "payments", T0, T0 + 40)
    _health(st, "payments", T0 + 10, lat="HIGH")
    before = _svc(st, "payments", T0 + 20)
    # A decision carrying a ticket_id is the responder's, whatever else it says: it must not
    # close the block, count anywhere, or turn the tile to "Rolled back".
    st.apply_decision({"window_end": _dt(T0 + 10), "service_name": "payments", "agent_name": "ticket_responder",
                       "trigger": "LATENCY_HIGH", "action": "ROLLBACK", "target": None, "reason": "r",
                       "ticket_id": "ZD-48215"}, kafka_ts=T0 + 18)
    _respond(st, "INC0041872", at=T0 + 19, action="SCALE_OUT", t=T0 + 10)  # an ordinary one, on checkout
    after = _svc(st, "payments", T0 + 20)
    assert after["blocks"] == before["blocks"] and after["blocks"][0]["open"] is True
    assert (after["status"], after["issues"]) == ("anomaly", before["issues"])
    assert _svc(st, "payments", T0 + 140)["status"] == "healthy"  # no fix shown, even once the block is gone
    checkout = _svc(st, "checkout", T0 + 20)
    assert "blocks" not in checkout and checkout["status"] == "healthy"  # a ticket row draws no metric block


def test_trim_drops_the_tickets_older_than_the_axis_plus_the_margin():
    st = OpsWallState()
    _story(st, "OLD", NOW - 680, verdict="HIGH", verdict_after=4, answer_after=19)  # last record at NOW - 661
    _story(st, "KEPT", NOW - 650)
    st.trim(NOW)  # keeps the last 600 + 60 s
    # A snapshot whose window still reaches back 700 s would count the dropped one.
    row = _row(st, now=NOW - 100)
    assert [t["id"] for t in row["tickets"]["newest"]] == ["KEPT"]
    assert row["tickets"]["received"] == 1


def test_time_to_remediate_counts_any_action():
    st = OpsWallState()
    _story(st, "A", NOW - 100, verdict="HIGH", verdict_after=4)
    _respond(st, "A", at=NOW - 100 + 9, action="INVALID", t=NOW - 100)
    assert _row(st)["issues"]["medianTimeToRemediate"] == pytest.approx(9.0)


# --- the "Code security incidents" row: findings and agent runs (D-070) -------------------


SECURITY = "Code security incidents"
FT = NOW - 60  # a finding reported a minute ago
REQUEST = "PATCH checkout CVE-2026-41234 urllib3 1.26.4 -> 1.26.18"
COMMIT = commit_for(REQUEST)
PATCH_REASON = "urllib3 ships in the checkout runtime and the vulnerable code is called."
# What lab 6.4 writes for a stopped patch agent (issue 02's step 5): the agent card fetch fails
# before the model runs, so the answer is Flink's error alone, INVALID and N/A (issue 07).
A2A_ERROR = ("Exception while initializing A2A client for agent: `env-mny3mw`.`lkc-9ko79yv`.`security_agent` "
             "with URL: https://story-josh-dishes-ontario.trycloudflare.com. ERROR: Failed to obtain agent card: 502")


def _scan_fields(fid, t=FT, score=9.8, repository="checkout", cve="CVE-2026-41234"):
    """The nine columns of a finding, which critical_cves copies from cve_scans."""
    return {"event_time": _dt(t), "finding_id": fid, "repository": repository, "cve_id": cve,
            "package": "urllib3", "installed_version": "1.26.4", "fixed_version": "1.26.18",
            "cvss_score": score, "finding": "urllib3 1.26.4 is a runtime dependency of checkout."}


def _scan(st, fid, t=FT, score=9.8, repository="checkout", cve="CVE-2026-41234"):
    """One cve_scans record, as ops-sim writes it (event_time a TIMESTAMP(3))."""
    st.apply_finding(_scan_fields(fid, t, score, repository, cve))


def _cve_truth(st, fid, action="PATCH", t=FT):
    """The simulator's cve_answer_key record: null when the finding isn't critical."""
    st.apply_cve_answer_key({"event_time": _dt(t), "finding_id": fid, "expected_action": action})


def _critical(st, fid, at, t=FT, score=9.8):
    """Lab 6.1's critical_cves record; `at` is its Kafka timestamp."""
    st.apply_critical_cve(_scan_fields(fid, t, score), kafka_ts=at)


def _decision_record(fid, action="PATCH", status="PATCHED", reason=PATCH_REASON, started=None, finished=None,
                     commit="default", raw=None, t=FT):
    """Lab 6.4's patch_decisions record. A PATCHED one carries the patch agent's reply in its
    Patch line, and the commit and times lab 6.4 parsed out of it."""
    if commit == "default":
        commit = COMMIT if status == "PATCHED" else None
    if raw is None:
        if status == "PATCHED":
            reply = format_reply(REQUEST, datetime.fromtimestamp(started, tz=timezone.utc),
                                 datetime.fromtimestamp(finished, tz=timezone.utc))
            raw = f"Action: PATCH\nReason: {reason}\nPatch: {reply}"
        else:
            raw = f"Action: {action}\nReason: {reason}\nPatch: N/A"
    return {"event_time": _dt(t), "finding_id": fid, "repository": "checkout", "cve_id": "CVE-2026-41234",
            "cvss_score": 9.8, "action": action, "reason": reason, "patch_status": status, "commit_sha": commit,
            "patch_started_at": None if started is None else _dt(started),
            "patch_finished_at": None if finished is None else _dt(finished), "raw_response": raw}


def _patch_decision(st, fid, at, **kw):
    """Lab 6.4's patch_decisions record; `at` is its Kafka timestamp."""
    st.apply_patch_decision(_decision_record(fid, **kw), kafka_ts=at)


def _patched(st, fid, at, started, finished, t=FT):
    _patch_decision(st, fid, at=at, started=started, finished=finished, t=t)


def _dismissed(st, fid, at, reason="Only a dev dependency of the test suite.", t=FT):
    _patch_decision(st, fid, at=at, action="DISMISS", status="N/A", reason=reason, t=t)


def _sec(st, now=NOW):
    return next(s for s in st.snapshot(now=now)["services"] if s["row"] == "security")


def test_the_security_row_comes_after_the_three_services_and_is_no_service():
    snap = OpsWallState().snapshot(now=NOW)
    assert [(s["name"], s["row"]) for s in snap["services"]] == [
        ("web-gateway", "releases"), ("checkout", "tickets"), ("payments", "windows"), (SECURITY, "security")]
    row = snap["services"][-1]
    assert set(row) == {"name", "row", "status", "statusText", "issues", "findings", "marks", "runs", "lanes",
                        "openCriticals"}
    assert (row["status"], row["statusText"], row["openCriticals"]) == ("healthy", "Healthy", 0)
    assert (row["findings"], row["marks"], row["runs"]) == ({"scanned": 0, "newest": []}, [], [])
    assert row["issues"] == {"injected": 0, "detected": 0, "missed": 0, "medianTimeToDetect": None,
                             "medianTimeToRemediate": None, "medianTimeToDetectText": "—",
                             "medianTimeToRemediateText": "—"}


def test_a_patch_is_our_closed_bar_hatched_while_it_waits_with_the_external_bar_inside_it():
    st = OpsWallState()
    _scan(st, "SCAN-000123")
    _critical(st, "SCAN-000123", at=FT + 0.6)
    _patched(st, "SCAN-000123", at=FT + 12.6, started=FT + 3.1, finished=FT + 9.1)
    security, external = _sec(st)["runs"]
    assert security == {
        "key": "security-agent@SCAN-000123", "lane": "security-agent", "findingId": "SCAN-000123",
        "repository": "checkout", "cveId": "CVE-2026-41234", "cvssScore": 9.8, "start": pytest.approx(_tpos(FT + 0.6)),
        "end": pytest.approx(_tpos(FT + 12.6)), "open": False, "label": "Patched in 12s",
        "action": "PATCH", "outcome": "Patch", "reason": PATCH_REASON, "commit": COMMIT, "patchStatus": "PATCHED",
        "unreachable": False,
        # the hatch: the external run's span, on our bar
        "waitFrom": pytest.approx(_tpos(FT + 3.1)), "waitTo": pytest.approx(_tpos(FT + 9.1)),
    }
    reply = format_reply(REQUEST, datetime.fromtimestamp(FT + 3.1, tz=timezone.utc),
                         datetime.fromtimestamp(FT + 9.1, tz=timezone.utc))
    assert external == {
        "key": "external@SCAN-000123", "lane": "external", "findingId": "SCAN-000123", "repository": "checkout",
        "cveId": "CVE-2026-41234", "cvssScore": 9.8, "start": pytest.approx(_tpos(FT + 3.1)),
        "end": pytest.approx(_tpos(FT + 9.1)), "open": False, "label": f"{COMMIT} in 6s",
        "commit": COMMIT, "reply": reply,  # the Patch line: what the patch agent answered
        "startedAt": pytest.approx(FT + 3.1), "finishedAt": pytest.approx(FT + 9.1),
        "duration": pytest.approx(6.0), "durationText": "6s",
    }
    assert security["start"] < external["start"] < external["end"] < security["end"]


def test_a_dismiss_is_one_bar_and_no_external_one():
    st = OpsWallState()
    _critical(st, "SCAN-000124", at=FT + 1)
    _dismissed(st, "SCAN-000124", at=FT + 9.4, reason="Only a dev dependency of the test suite.")
    [run] = _sec(st)["runs"]
    assert (run["lane"], run["open"], run["label"]) == ("security-agent", False, "Dismissed in 8s")
    assert (run["action"], run["outcome"], run["reason"]) == (
        "DISMISS", "Dismiss", "Only a dev dependency of the test suite.")
    assert (run["commit"], run["patchStatus"], run["unreachable"]) == (None, "N/A", False)
    assert (run["waitFrom"], run["waitTo"]) == (None, None)


@pytest.mark.parametrize(("action", "status", "raw", "label", "outcome", "unreachable"), [
    # lab 6.4: a PATCH whose Patch line holds no commit
    ("PATCH", "FAILED", "Action: PATCH\nReason: r\nPatch: FAILED, the call timed out", "Patch agent unreachable",
     "Patch", True),
    ("INVALID", "N/A", "I am not sure.", "Invalid answer", "Invalid", False),
    # Flink's error for a patch agent it could not reach, but not the whole answer: the agent answered
    ("INVALID", "N/A", f"Action: unsure\nPatch: {A2A_ERROR}", "Invalid answer", "Invalid", False),
])
def test_a_failed_patch_and_an_invalid_answer_read_their_words(action, status, raw, label, outcome, unreachable):
    st = OpsWallState()
    _critical(st, "SCAN-000125", at=FT + 1)
    _patch_decision(st, "SCAN-000125", at=FT + 31, action=action, status=status, reason=None, raw=raw)
    [run] = _sec(st)["runs"]
    assert (run["open"], run["label"], run["outcome"], run["unreachable"]) == (False, label, outcome, unreachable)
    assert run["end"] == pytest.approx(_tpos(FT + 31))


@pytest.mark.parametrize("truth", ["PATCH", "DISMISS"])
def test_a_stopped_patch_agent_reads_patch_agent_unreachable_in_red_and_is_a_miss(truth):
    # Issue 02's step 5: Flink fetches the agent card on every call, so a stopped patch agent
    # fails the whole agent call before the model answers, and lab 6.4 writes INVALID and N/A,
    # not FAILED (issue 07), whatever the finding called for. The user's decision (the code
    # review, S-c1): such an INVALID reads "Patch agent unreachable", red like FAILED, and is
    # still a miss. It draws no external bar.
    st = OpsWallState()
    _scan(st, "SCAN-000126")
    _cve_truth(st, "SCAN-000126", truth)
    _critical(st, "SCAN-000126", at=FT + 1)
    _patch_decision(st, "SCAN-000126", at=FT + 2, action="INVALID", status="N/A", reason=None, raw=A2A_ERROR)
    row = _sec(st)
    [run] = row["runs"]
    assert (run["label"], run["unreachable"], run["action"], run["outcome"]) == (
        "Patch agent unreachable", True, "INVALID", "Invalid")
    assert (run["commit"], run["waitFrom"], run["waitTo"]) == (None, None, None)
    assert (row["issues"]["injected"], row["issues"]["detected"], row["issues"]["missed"]) == (1, 0, 1)


def test_with_no_decision_our_bar_works_for_60_s_then_reads_no_answer():
    st = OpsWallState()
    _critical(st, "SCAN-000127", at=FT + 1)
    [run] = _sec(st, now=FT + 4.2)["runs"]
    assert (run["open"], run["label"], run["outcome"], run["end"]) == (True, "Working 3s", None, 120)
    [run] = _sec(st, now=FT + 61)["runs"]
    assert (run["open"], run["label"]) == (True, "Working 1m 00s")  # checkout gives up after 30 s; ours after 60
    [run] = _sec(st, now=FT + 61.5)["runs"]
    assert (run["open"], run["label"], run["outcome"]) == (False, "No answer", None)
    assert run["end"] == pytest.approx(_tpos(FT + 61, now=FT + 61.5))  # it ends 60 s after it started
    # A late decision still closes it, as a ticket's does.
    _dismissed(st, "SCAN-000127", at=FT + 76)
    [run] = _sec(st, now=FT + 80)["runs"]
    assert (run["open"], run["label"]) == (False, "Dismissed in 1m 15s")


def test_the_external_bar_appears_only_once_our_agent_has_answered():
    # Spec 24: the patch's times come with the patch decision, so while the patch runs our bar
    # reads Working and the external lane is empty (D-070, option a).
    st = OpsWallState()
    _critical(st, "SCAN-000128", at=FT + 0.6)
    [run] = _sec(st, now=FT + 6)["runs"]  # the patch agent is patching right now
    assert (run["lane"], run["open"], run["label"], run["waitFrom"], run["waitTo"]) == (
        "security-agent", True, "Working 5s", None, None)
    _patched(st, "SCAN-000128", at=FT + 12.6, started=FT + 3.1, finished=FT + 9.1)
    assert [(r["lane"], r["open"]) for r in _sec(st, now=FT + 13)["runs"]] == [("security-agent", False),
                                                                           ("external", False)]


def test_an_external_bar_needs_both_times():
    st = OpsWallState()
    _critical(st, "SCAN-000129", at=FT + 1)
    _patch_decision(st, "SCAN-000129", at=FT + 12, started=FT + 3, finished=None, raw="Action: PATCH\nPatch: odd")
    [run] = _sec(st)["runs"]
    assert (run["lane"], run["label"], run["waitFrom"]) == ("security-agent", "Patched in 11s", None)


def test_the_hatch_is_the_part_of_our_bar_the_patch_covers():
    # The patch's times are the laptop's clock, our bar's are Kafka's: a laptop running ahead
    # puts the finish after our answer. The hatch stays on our bar; the external bar is drawn as
    # it came.
    st = OpsWallState()
    _critical(st, "SCAN-000130", at=FT + 1)
    _patched(st, "SCAN-000130", at=FT + 10, started=FT + 4, finished=FT + 11.5)
    security, external = _sec(st)["runs"]
    assert (security["waitFrom"], security["waitTo"]) == (pytest.approx(_tpos(FT + 4)), pytest.approx(_tpos(FT + 10)))
    assert external["end"] == pytest.approx(_tpos(FT + 11.5))


def test_a_decision_without_its_critical_cves_record_starts_our_bar_at_the_finding():
    # The wall reads each topic from 10 minutes back: the cut can fall between a finding's
    # critical_cves record and its decision.
    st = OpsWallState()
    _dismissed(st, "SCAN-000131", at=FT + 7)
    [run] = _sec(st)["runs"]
    assert (run["start"], run["label"]) == (pytest.approx(_tpos(FT)), "Dismissed in 7s")
    _critical(st, "SCAN-000131", at=FT + 0.5)  # its critical_cves record arrives after all
    assert _sec(st)["runs"][0]["start"] == pytest.approx(_tpos(FT + 0.5))


def test_a_critical_finding_nobody_routed_gets_no_bar():
    # Like a ticket before lab 5.1: no critical_cves record, no agent working on it.
    st = OpsWallState()
    _scan(st, "SCAN-000132", score=9.9)
    _cve_truth(st, "SCAN-000132", "DISMISS")
    row = _sec(st)
    assert (row["runs"], row["status"], row["openCriticals"]) == ([], "healthy", 0)


def test_every_finding_of_the_last_2_minutes_is_a_mark_with_its_cve_id():
    st = OpsWallState()
    _scan(st, "SCAN-000140", t=NOW - 121, score=9.5)                          # just off the timeline
    _scan(st, "SCAN-000141", t=NOW - 120, score=6.1, cve="CVE-2026-40007")
    _scan(st, "SCAN-000142", t=NOW - 30, score=9.0, cve="CVE-2026-46471")     # 9.0 is critical: lab 6.1's >=
    _scan(st, "SCAN-000143", t=NOW - 5, score=8.9, cve="CVE-2026-40311")
    assert _sec(st)["marks"] == [
        {"id": "SCAN-000141", "cveId": "CVE-2026-40007", "critical": False, "t": pytest.approx(0.0)},
        {"id": "SCAN-000142", "cveId": "CVE-2026-46471", "critical": True, "t": pytest.approx(90.0)},
        {"id": "SCAN-000143", "cveId": "CVE-2026-40311", "critical": False, "t": pytest.approx(115.0)},
    ]


def test_the_list_has_the_newest_5_findings_and_the_count_of_the_last_10_minutes():
    st = OpsWallState()
    _scan(st, "SCAN-000150", t=NOW - 601)                       # 10 minutes and a second ago: not scanned
    _scan(st, "SCAN-000151", t=NOW - 599, score=5.0)
    for k, (repo, score) in enumerate([("payments", 9.1), ("ci-tools", 4.3), ("storefront-legacy", 9.7),
                                       ("web-gateway", 7.2), ("checkout", 10.0)]):
        _scan(st, f"SCAN-00016{k}", t=NOW - 50 + 10 * k, score=score, repository=repo)
    findings = _sec(st)["findings"]
    assert findings["scanned"] == 6
    assert [f["id"] for f in findings["newest"]] == [
        "SCAN-000164", "SCAN-000163", "SCAN-000162", "SCAN-000161", "SCAN-000160"]
    # what a row shows, and what the page's dot rule reads: a new id, at most 10 s old
    assert findings["newest"][2] == {"id": "SCAN-000162", "repository": "storefront-legacy", "cveId": "CVE-2026-41234",
                                     "cvssScore": 9.7, "critical": True, "eventTime": pytest.approx(NOW - 30)}
    assert [f["critical"] for f in findings["newest"]] == [True, False, True, False, True]


def test_the_header_counts_the_critical_findings_our_agent_works_on_and_the_lane_says_so():
    st = OpsWallState()
    row = _sec(st)
    assert row["lanes"] == [
        {"key": "security-agent", "name": "Security agent", "working": False, "state": "Idle"},
        # the wall learns of the external call only when our agent answers (spec 24): no state
        {"key": "external", "name": "External · LangGraph · A2A", "working": False, "state": None},
    ]
    _critical(st, "SCAN-000170", at=NOW - 20)
    _critical(st, "SCAN-000171", at=NOW - 10, t=NOW - 11)
    _critical(st, "SCAN-000172", at=NOW - 70, t=NOW - 71)   # gave up 10 s ago
    _critical(st, "SCAN-000173", at=NOW - 30, t=NOW - 31)
    _dismissed(st, "SCAN-000173", at=NOW - 25, t=NOW - 31)  # answered
    row = _sec(st)
    assert (row["status"], row["statusText"], row["openCriticals"]) == ("working", "Agent on critical CVE", 2)
    assert [(lane["working"], lane["state"]) for lane in row["lanes"]] == [(True, "Working"), (False, None)]
    _patched(st, "SCAN-000170", at=NOW - 1, started=NOW - 15, finished=NOW - 5)
    _dismissed(st, "SCAN-000171", at=NOW - 2, t=NOW - 11)
    row = _sec(st)
    assert (row["status"], row["statusText"], row["openCriticals"]) == ("healthy", "Healthy", 0)
    assert row["lanes"][0]["state"] == "Idle"


def _cve_story(st, fid, t, truth="PATCH", score=9.6, action=None, decided_after=None, status=None,
               finished_after=None, routed=True):
    """A finding reported at `t`: its answer key, lab 6.1's record a second later, and lab 6.4's
    decision that many seconds after the finding."""
    _scan(st, fid, t=t, score=score)
    _cve_truth(st, fid, truth, t=t)
    if routed and score >= 9.0:
        _critical(st, fid, at=t + 1, t=t, score=score)
    if action is not None:
        status = status or ("PATCHED" if action == "PATCH" else "N/A")
        started = t + finished_after - 5 if finished_after is not None else None
        finished = t + finished_after if finished_after is not None else None
        _patch_decision(st, fid, at=t + decided_after, action=action, status=status,
                        started=started, finished=finished, t=t)


def test_the_issues_panel_scores_the_critical_findings_of_the_last_10_minutes_against_the_answer_key():
    st = OpsWallState()
    _cve_story(st, "A", NOW - 300, action="PATCH", decided_after=12, finished_after=9)      # right
    _cve_story(st, "B", NOW - 280, truth="DISMISS", action="DISMISS", decided_after=6)      # right
    _cve_story(st, "C", NOW - 260, action="DISMISS", decided_after=8)                       # wrong: missed
    _cve_story(st, "D", NOW - 240, truth="DISMISS", action="INVALID", decided_after=2)      # missed
    _cve_story(st, "E", NOW - 220, action="PATCH", status="FAILED", decided_after=40)       # right, not patched
    _cve_story(st, "J", NOW - 200, action="PATCH", decided_after=20, finished_after=15)     # right
    # right, and it called the tool anyway: lab 6.4 keeps the times, but it is no patch (N/A)
    _cve_story(st, "L", NOW - 180, truth="DISMISS", action="DISMISS", decided_after=10, finished_after=7)
    _cve_story(st, "F", NOW - 62)                       # no decision 60 s after lab 6.1's record: missed
    _cve_story(st, "G", NOW - 60.5)                     # 59.5 s after its record: still waiting
    _cve_story(st, "K", NOW - 61, routed=False)         # never routed: missed 60 s after the finding
    _cve_story(st, "H", NOW - 100, truth=None, score=7.5)                                   # not critical
    _cve_story(st, "I", NOW - 601, action="PATCH", decided_after=12, finished_after=9)      # before the window
    assert _sec(st)["issues"] == {
        "injected": 10, "detected": 5, "missed": 4,
        "medianTimeToDetect": pytest.approx(10.0),      # 2, 6, 8, 10, 12, 20, 40: the finding -> the decision
        "medianTimeToRemediate": pytest.approx(12.0),   # 9, 15: the finding -> the patch's finish, PATCHED only
        "medianTimeToDetectText": "10s", "medianTimeToRemediateText": "12s",
    }


def test_a_right_decision_that_comes_after_60_s_is_a_miss():
    # Spec 26: missed is wrong, INVALID, or none within 60 s. Our bar gives up 60 s after lab 6.1's
    # record, so a decision after that is a miss however right it is; checkout's rule would have
    # counted it as detected (the code review, S-c3).
    st = OpsWallState()
    _cve_story(st, "A", NOW - 300, action="PATCH", decided_after=61, finished_after=9)    # 60 s after 6.1: in time
    _cve_story(st, "B", NOW - 280, truth="DISMISS", action="DISMISS", decided_after=61.5)  # 60.5 s after it: late
    _cve_story(st, "C", NOW - 260, action="PATCH", decided_after=90, finished_after=9)    # late
    _cve_story(st, "D", NOW - 240, action="DISMISS", decided_after=70)                    # wrong and late: one miss
    # never routed: the 60 s run from the finding, as for one never decided
    _cve_story(st, "E", NOW - 220, truth="DISMISS", action="DISMISS", decided_after=60.5, routed=False)
    issues = _sec(st)["issues"]
    assert (issues["injected"], issues["detected"], issues["missed"]) == (5, 1, 4)
    # The times still count the late decisions: that is how long they took.
    assert issues["medianTimeToDetect"] == pytest.approx(61.5)     # 60.5, 61, 61.5, 70, 90
    assert issues["medianTimeToRemediate"] == pytest.approx(9.0)   # A and C, both patched


def test_a_finding_fills_in_from_its_four_records_in_any_order():
    import itertools

    records = [
        lambda st: _scan(st, "SCAN-000180"),
        lambda st: _cve_truth(st, "SCAN-000180", "PATCH"),
        lambda st: _critical(st, "SCAN-000180", at=FT + 1),
        lambda st: _patched(st, "SCAN-000180", at=FT + 12, started=FT + 3, finished=FT + 9),
    ]
    for order in itertools.permutations(records):
        st = OpsWallState()
        for apply in order:
            apply(st)
        row = _sec(st)
        assert [f["id"] for f in row["findings"]["newest"]] == ["SCAN-000180"]
        assert [(r["lane"], r["label"]) for r in row["runs"]] == [("security-agent", "Patched in 11s"),
                                                                  ("external", f"{COMMIT} in 6s")]
        assert (row["issues"]["detected"], row["issues"]["medianTimeToDetect"],
                row["issues"]["medianTimeToRemediate"]) == (1, pytest.approx(12.0), pytest.approx(9.0))


def test_a_record_written_twice_keeps_its_first_kafka_timestamp_and_the_scan_its_first_fields():
    st = OpsWallState()
    _scan(st, "SCAN-000181", repository="payments")
    _critical(st, "SCAN-000181", at=FT + 1)
    _critical(st, "SCAN-000181", at=FT + 30)  # lab 6.1 ran again
    _dismissed(st, "SCAN-000181", at=FT + 9)
    _dismissed(st, "SCAN-000181", at=FT + 40, reason="Archived.")  # an aborted transaction's replay
    [run] = _sec(st)["runs"]
    assert (run["start"], run["end"]) == (pytest.approx(_tpos(FT + 1)), pytest.approx(_tpos(FT + 9)))
    assert (run["repository"], run["reason"]) == ("payments", "Archived.")  # the newer decision's words


def test_a_field_a_record_leaves_empty_waits_for_a_record_that_carries_it():
    # One rule for a ticket's fields and a finding's (the code review's M1): None or "" is no
    # value, so a later record that copies the field fills it in; a CVSS of 0.0 is a score.
    st = OpsWallState()
    st.apply_ticket({**_fields("ZD-48216", TK), "subject": ""})
    _p1(st, "ZD-48216", at=TK + 1)  # p1_tickets copies the subject
    assert _row(st)["tickets"]["newest"][0]["subject"] == "Subject of ZD-48216"
    st.apply_finding({**_scan_fields("SCAN-000182"), "repository": ""})
    _critical(st, "SCAN-000182", at=FT + 1)  # critical_cves copies the repository
    st.apply_finding({**_scan_fields("SCAN-000183", t=FT + 2), "cvss_score": 0.0})
    newest = {f["id"]: f for f in _sec(st)["findings"]["newest"]}
    assert newest["SCAN-000182"]["repository"] == "checkout"
    assert (newest["SCAN-000183"]["cvssScore"], newest["SCAN-000183"]["critical"]) == (0.0, False)


def test_trim_drops_the_findings_older_than_the_history_plus_the_margin():
    st = OpsWallState()
    _cve_story(st, "OLD", NOW - 680, action="DISMISS", truth="DISMISS", decided_after=19)  # last at NOW - 661
    _cve_story(st, "KEPT", NOW - 650)
    st.trim(NOW)  # keeps the last 600 + 60 s
    row = _sec(st, now=NOW - 100)
    assert [f["id"] for f in row["findings"]["newest"]] == ["KEPT"]
    assert row["findings"]["scanned"] == 1


def test_trim_keeps_a_ticket_or_a_finding_while_the_last_thing_that_happened_to_it_is_recent():
    # Each is 700 s old; trim keeps the last 660 s, and what kept each one happened 659 s ago.
    st = OpsWallState()
    _ticket(st, "ROUTED", NOW - 700)
    _p1(st, "ROUTED", at=NOW - 659, t=NOW - 700)                                         # lab 5.1's record
    _story(st, "CLASSIFIED", NOW - 700, truth="LOW", verdict="LOW", verdict_after=41)     # the verdict
    _story(st, "ANSWERED", NOW - 700, verdict="HIGH", verdict_after=4, answer_after=41)   # the responder's action
    _story(st, "DROPPED", NOW - 700, verdict="HIGH", verdict_after=4, answer_after=39)
    _scan(st, "ROUTED", t=NOW - 700)
    _critical(st, "ROUTED", at=NOW - 659, t=NOW - 700)                                   # lab 6.1's record
    _cve_story(st, "DECIDED", NOW - 700, truth="DISMISS", action="DISMISS", decided_after=41)  # lab 6.4's
    _cve_story(st, "PATCHED", NOW - 700, action="PATCH", decided_after=30, finished_after=41)  # the patch's finish
    _cve_story(st, "GONE", NOW - 700, action="PATCH", decided_after=39, finished_after=35)
    st.trim(NOW)
    assert {t["id"] for t in _row(st, now=NOW - 100)["tickets"]["newest"]} == {"ROUTED", "CLASSIFIED", "ANSWERED"}
    assert {f["id"] for f in _sec(st, now=NOW - 100)["findings"]["newest"]} == {"ROUTED", "DECIDED", "PATCHED"}


def test_a_run_that_ended_before_the_timeline_is_left_out_and_one_open_since_before_it_starts_at_its_edge():
    st = OpsWallState()
    _critical(st, "SCAN-000190", at=NOW - 140, t=NOW - 141)
    _dismissed(st, "SCAN-000190", at=NOW - 121, t=NOW - 141)
    _critical(st, "SCAN-000191", at=NOW - 130, t=NOW - 131)
    _dismissed(st, "SCAN-000191", at=NOW - 100, t=NOW - 131)
    [run] = _sec(st)["runs"]
    assert (run["findingId"], run["start"], run["end"]) == ("SCAN-000191", 0.0, pytest.approx(20.0))


def test_the_critical_line_is_lab_6_1s_rule():
    import re
    from pathlib import Path

    from scripts.opswall.state import CRITICAL_CVSS

    sql = (Path(__file__).resolve().parents[1] / "sql/6.1_critical_cves.sql").read_text()
    (score,) = re.findall(r"`cvss_score` >= ([\d.]+)", sql)
    assert CRITICAL_CVSS == float(score)


def test_the_security_row_leaves_the_three_service_rows_as_they_were():
    def services(st):
        _phase4_payments(st)
        _story(st, "ZD-48215", NOW - 100, verdict="HIGH", verdict_after=4, answer_after=10)
        _deployment(st, "web-gateway", T0, "DEPLOY", "3.4.3", "3.4.2")
        return st

    before = services(OpsWallState()).snapshot(now=NOW)
    st = services(OpsWallState())
    _cve_story(st, "SCAN-000200", NOW - 90, action="PATCH", decided_after=12, finished_after=9)
    _cve_story(st, "SCAN-000201", NOW - 40)
    after = st.snapshot(now=NOW)
    assert after["services"][:3] == before["services"][:3]
    assert {k: v for k, v in after.items() if k != "services"} == {k: v for k, v in before.items() if k != "services"}
