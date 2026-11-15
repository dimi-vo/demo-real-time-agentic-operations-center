"""evaluate's detection scoring: spans, window numbering, false positives, fresh start."""

import json

from scripts.common.topic_reader import epoch_seconds, iso, jsonable
from scripts.evaluation.detection import (
    DETECTED,
    LATE,
    MISSED,
    NO_DATA,
    HealthRow,
    Incident,
    default_range,
    incidents_from_records,
    score,
)

T0 = 1_800_000_000.0  # a 10-second boundary


def _row(service, k, lat="NORMAL", trf="NORMAL", version="1.0.0", ssd=600):
    """Window k of `service`: [T0 + 10k, T0 + 10k + 10)."""
    return HealthRow(
        service_name=service, window_start=T0 + 10 * k, window_end=T0 + 10 * k + 10,
        latency_status=lat, traffic_status=trf, avg_latency_ms=100.0, expected_latency_ms=50.0,
        latency_upper_bound=70.0, traffic_count=30, expected_traffic_count=10.0,
        traffic_lower_bound=5.0, traffic_upper_bound=15.0, version=version, seconds_since_deploy=ssd,
    )  # fmt: skip


def _series(service, n, overrides=None):
    """n windows of `service`; overrides maps k -> dict of _row kwargs."""
    overrides = overrides or {}
    return [_row(service, k, **overrides.get(k, {})) for k in range(n)]


def _inc(service, start_k, start_offset, duration, trigger="LATENCY_HIGH", typ="BAD_DEPLOY", **kw):
    start = T0 + 10 * start_k + start_offset
    return Incident(
        incident_id="i1", type=typ, service_name=service, started_at=start, ended_at=start + duration,
        expected_trigger=trigger, expected_action="ROLLBACK", **kw,
    )  # fmt: skip


def _score(rows, incidents, frm=T0, to=None):
    return score(rows, incidents, frm, to if to is not None else T0 + 10_000)


def test_detected_in_onset_window_counts_as_window_1():
    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    inc = _inc("checkout", 40, 3, 50)  # starts 3 s into window 40
    r = _score(rows, [inc]).incidents[0]
    assert r.status == DETECTED and r.first_window == 1


def test_detected_on_window_3_is_in_time_window_4_is_late():
    inc = _inc("checkout", 40, 3, 60)
    r3 = _score(_series("checkout", 60, {42: {"lat": "HIGH"}}), [inc]).incidents[0]
    r4 = _score(_series("checkout", 60, {43: {"lat": "HIGH"}}), [inc]).incidents[0]
    assert (r3.status, r3.first_window) == (DETECTED, 3)
    assert (r4.status, r4.first_window) == (LATE, 4)


def test_missed_and_wrong_direction():
    inc = _inc("checkout", 40, 0, 50, trigger="TRAFFIC_LOW", typ="UPSTREAM_DROP")
    r = _score(_series("checkout", 60, {41: {"trf": "HIGH"}}), [inc]).incidents[0]
    assert r.status == MISSED and r.wrong_direction
    assert r.other_signals == [(2, "TRAFFIC_HIGH", False)]


def test_no_rows_for_incident():
    inc = _inc("checkout", 400, 0, 50)
    assert _score(_series("checkout", 60), [inc]).incidents[0].status == NO_DATA


def test_signals_inside_span_are_not_false_positives_outside_are():
    rows = _series("checkout", 400, {40: {"lat": "HIGH"}, 41: {"lat": "HIGH"}, 200: {"trf": "LOW"}})
    rep = _score(rows, [_inc("checkout", 40, 0, 20)])
    s = rep.services["checkout"]
    assert [(fp.signal, fp.row.window_start) for fp in s.false_positives] == [("TRAFFIC_LOW", T0 + 2000)]
    assert s.false_positives[0].tag == ""


def test_echo_tag_only_in_the_minute_after_an_incident():
    # D-057: no tag for a recent deploy. Every deploy is a bad deploy (inside its span)
    # or its rollback (at the incident's end, where the echo minute starts).
    rows = _series("web-gateway", 400, {46: {"trf": "LOW"}, 200: {"lat": "HIGH", "ssd": 15}})
    rep = _score(rows, [_inc("web-gateway", 40, 0, 55, trigger="TRAFFIC_HIGH", typ="TRAFFIC_SURGE")])
    tags = {fp.row.window_start: fp.tag for fp in rep.services["web-gateway"].false_positives}
    assert tags == {T0 + 460: "echo", T0 + 2000: ""}


def test_fresh_start_noise_is_separate_from_false_positives():
    # windows 0-5 warm up; window 10 (inside the first 5 minutes) fires; window 100 fires
    warm = {k: {"lat": "WARMING_UP", "trf": "WARMING_UP"} for k in range(6)}
    rows = _series("payments", 400, {**warm, 10: {"lat": "HIGH"}, 100: {"lat": "HIGH"}})
    s = _score(rows, []).services["payments"]
    assert [fp.row.window_start for fp in s.fresh_start_noise] == [T0 + 100]
    assert [fp.row.window_start for fp in s.false_positives] == [T0 + 1000]
    assert s.warmup_rows == 6 and s.warmup_after_fresh_start == 0
    # scored hours exclude the fresh-start 5 minutes (30 windows) and warm-up rows
    assert abs(s.scored_hours - (400 - 30) * 10 / 3600) < 1e-9


def test_warmup_after_fresh_start_is_flagged():
    rows = _series("payments", 100, {80: {"lat": "WARMING_UP"}})
    assert _score(rows, []).services["payments"].warmup_after_fresh_start == 1


def test_false_positives_only_counted_in_range():
    rows = _series("payments", 400, {100: {"lat": "HIGH"}, 300: {"lat": "HIGH"}})
    s = score(rows, [], T0 + 2500, T0 + 4000).services["payments"]
    assert [fp.row.window_start for fp in s.false_positives] == [T0 + 3000]


def test_duplicates_and_gaps():
    rows = _series("web-gateway", 10) + [_row("web-gateway", 3)] + [_row("web-gateway", k) for k in range(15, 20)]
    s = _score(rows, []).services["web-gateway"]
    assert s.duplicates == 1
    assert (s.gaps, s.missing_windows) == (1, 5)


def test_incidents_scored_only_when_they_start_in_range():
    inc = _inc("checkout", 40, 0, 50)
    assert score(_series("checkout", 60), [inc], T0 + 1000, T0 + 2000).incidents == []


def test_default_range_ends_before_the_unlogged_tail():
    rows = _series("checkout", 400)
    frm, to = default_range(rows, 60)
    assert to == T0 + 4000 - 100
    assert frm == to - 3600


def test_record_roundtrip_through_jsonl(tmp_path):
    from datetime import datetime, timezone

    rec = {
        "service_name": "checkout", "window_start": datetime.fromtimestamp(T0, tz=timezone.utc),
        "window_end": datetime.fromtimestamp(T0 + 10),  # naive -> treated as UTC
        "latency_status": "HIGH", "traffic_status": "NORMAL",
    }  # fmt: skip
    rec["window_end"] = rec["window_end"].astimezone(timezone.utc).replace(tzinfo=None)
    row = HealthRow.from_record(json.loads(json.dumps(jsonable(rec))))
    assert row.window_start == T0 and row.window_end == T0 + 10
    assert row.signals == ("LATENCY_HIGH",)


def test_a_row_with_detected_at_is_read_and_scored_like_before():
    """D-054: service_health's last column, a naive datetime from fastavro like the other
    time columns, changes nothing: evaluate reads the columns it needs by name."""
    from datetime import datetime, timezone

    def naive(t):
        return datetime.fromtimestamp(t, tz=timezone.utc).replace(tzinfo=None)

    rows = _series("checkout", 60, {40: {"lat": "HIGH"}})
    records = [
        {**r.__dict__, "window_start": naive(r.window_start), "window_end": naive(r.window_end),
         "detected_at": naive(r.window_end + 6)}
        for r in rows
    ]  # fmt: skip
    read = [HealthRow.from_record(rec) for rec in records]
    assert read == rows
    r = _score(read, [_inc("checkout", 40, 3, 50)]).incidents[0]
    assert r.status == DETECTED and r.first_window == 1


def test_padded_char_statuses_are_trimmed():
    rec = {"service_name": "checkout", "window_start": iso(T0), "window_end": iso(T0 + 10),
           "latency_status": "HIGH      ", "traffic_status": "LOW       "}  # fmt: skip
    assert HealthRow.from_record(rec).signals == ("LATENCY_HIGH", "TRAFFIC_LOW")


def test_epoch_seconds_formats():
    assert epoch_seconds(1_800_000_000_000) == T0
    assert epoch_seconds("2027-01-15T08:00:00Z") == epoch_seconds("2027-01-15 08:00:00")
    assert epoch_seconds(None) is None


def _gen_records(incident_id="INC0001", start=T0, duration=40):
    """The two service_incidents records the generator writes for one BAD_DEPLOY."""
    from opsgen.engine import BAD_DEPLOY
    from opsgen.engine import Incident as GenIncident
    from opsgen.groundtruth import end_record, start_record

    gen = GenIncident(
        incident_id=incident_id, type=BAD_DEPLOY, service_name="web-gateway",
        degradation_start=start, degradation_end=start + duration, expected_trigger="LATENCY_HIGH",
        expected_action="ROLLBACK",
    )  # fmt: skip
    return start_record(gen, start), end_record(gen, start + duration)


def test_evaluate_reads_what_the_generator_emits():
    [inc] = incidents_from_records(_gen_records())
    assert (inc.incident_id, inc.service_name) == ("INC0001", "web-gateway")
    assert (inc.started_at, inc.ended_at) == (T0, T0 + 40)
    assert (inc.type, inc.expected_trigger, inc.expected_action) == ("BAD_DEPLOY", "LATENCY_HIGH", "ROLLBACK")


def test_opposite_signal_in_the_end_window_is_an_echo_not_wrong_direction():
    # surge from window 40 to 45.5: window 45 holds the return to normal
    inc = _inc("web-gateway", 40, 0, 55, trigger="TRAFFIC_HIGH", typ="TRAFFIC_SURGE")
    r = _score(_series("web-gateway", 60, {40: {"trf": "HIGH"}, 45: {"trf": "LOW"}}), [inc]).incidents[0]
    assert r.status == DETECTED and not r.wrong_direction
    assert r.other_signals == [(6, "TRAFFIC_LOW", True)]


def test_fractional_ended_at_takes_effect_at_the_next_tick():
    # logged ended_at = started + 49.5 s; ops-sim ends it at the 50 s tick
    inc = _inc("checkout", 40, 2, 49.5)
    assert inc.effective_end == inc.started_at + 50
    inc2 = _inc("checkout", 40, 2, 50.0)
    assert inc2.effective_end == inc2.started_at + 50


def test_rollback_window_found_with_a_fractional_ended_at():
    # INC0007 shape: started on a tick at :x9.32, logged ended_at 50.5 s later at :x9.84,
    # the ROLLBACK really fires at the :x0.32 tick, i.e. in the following window.
    start = T0 + 9.32
    inc = Incident("i", "BAD_DEPLOY", "payments", start, start + 50.5, "LATENCY_HIGH", "ROLLBACK")
    rows = [_row("payments", k) for k in range(10)]
    span = [k for k, r in enumerate(rows) if inc.in_span(r)]
    assert span == [0, 1, 2, 3, 4, 5, 6]
    assert [k for k in span if inc.at_end(rows[k])] == [6]


def test_signals_next_to_a_data_gap_are_restart_artifacts():
    # windows 0-99, gap (ops-sim stopped) for 100-104, then 105-199
    rows = _series("checkout", 100, {99: {"trf": "LOW"}}) + [
        _row("checkout", k, trf="LOW" if k in (105, 110) else "NORMAL") for k in range(105, 200)
    ]
    rows.append(_row("checkout", 150, lat="HIGH"))  # far from the gap: a real false positive (duplicate row ignored)
    rows = [r for r in rows if not (r.window_start == T0 + 1500 and r.latency_status == "NORMAL")]
    s = _score(rows, []).services["checkout"]
    assert [fp.row.window_start for fp in s.restart_artifacts] == [T0 + 990, T0 + 1050, T0 + 1100]
    assert [fp.row.window_start for fp in s.false_positives] == [T0 + 1500]


def test_rescale_confidence_widens_symmetric_bounds():
    from scripts.evaluation.detection import rescale_confidence

    # expected 50, upper 70 at 99.9 -> at 99.99 the half-width grows by z(99.99)/z(99.9) ~ 1.18
    r = _row("checkout", 10, lat="HIGH")  # avg 100 vs upper 70 stays HIGH
    edge = HealthRow(**{**r.__dict__, "avg_latency_ms": 72.0})  # just over 70, under ~73.6
    out = rescale_confidence([r, edge], 99.9, 99.99)
    assert out[0].latency_status == "HIGH"
    assert out[1].latency_status == "NORMAL"
    assert abs(out[1].latency_upper_bound - (50 + 20 * 3.8906 / 3.2905)) < 0.01


def test_start_and_end_records_make_one_incident_in_either_order():
    start, end = _gen_records()
    assert len(incidents_from_records([start, end])) == 1
    assert len(incidents_from_records([end, start])) == 1


def test_an_incident_that_has_not_ended_is_not_scored():
    start, _ = _gen_records()
    assert incidents_from_records([start]) == []


def test_incidents_come_back_in_start_order():
    later = _gen_records("INC0002", start=T0 + 600)
    earlier = _gen_records("INC0001", start=T0)
    assert [i.incident_id for i in incidents_from_records([*later, *earlier])] == ["INC0001", "INC0002"]


def test_incident_timestamps_load_from_kafka_a_dump_or_epoch_millis():
    """fastavro gives naive datetimes, a --dump-incidents file has ISO text (jsonable),
    and the generator's own records carry epoch milliseconds."""
    from datetime import datetime, timezone

    _, end = _gen_records()
    as_datetime = {
        **end,
        "started_at": datetime.fromtimestamp(T0, tz=timezone.utc).replace(tzinfo=None),
        "ended_at": datetime.fromtimestamp(T0 + 40, tz=timezone.utc).replace(tzinfo=None),
    }
    as_dump = json.loads(json.dumps(jsonable(as_datetime)))
    for rec in (end, as_datetime, as_dump):
        [inc] = incidents_from_records([rec])
        assert (inc.started_at, inc.ended_at) == (T0, T0 + 40), rec
