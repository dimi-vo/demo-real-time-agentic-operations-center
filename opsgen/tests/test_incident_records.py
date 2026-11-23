"""start_record and end_record + the driver's hand-off to the sink."""

from __future__ import annotations

from opsgen.driver import run_realtime
from opsgen.engine import BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP, Incident
from opsgen.groundtruth import end_record, start_record
from opsgen.simulation import Simulation

_TRIGGER = {BAD_DEPLOY: "LATENCY_HIGH", TRAFFIC_SURGE: "TRAFFIC_HIGH", UPSTREAM_DROP: "TRAFFIC_LOW"}
_ACTION = {BAD_DEPLOY: "ROLLBACK", TRAFFIC_SURGE: "SCALE_OUT", UPSTREAM_DROP: "INVESTIGATE_UPSTREAM"}


def _inc(itype=BAD_DEPLOY):
    return Incident(
        incident_id="INC0007",
        type=itype,
        service_name="checkout",
        degradation_start=1010.0,
        degradation_end=1070.0,
        expected_trigger=_TRIGGER[itype],
        expected_action=_ACTION[itype],
        scheduled_end=1070.0,
    )


def test_start_record_has_null_end_and_both_ids():
    inc = _inc()
    r = start_record(inc, now=1010.5)
    assert r["incident_id"] == "INC0007"
    assert r["service_name"] == "checkout"
    assert r["incident_type"] == "BAD_DEPLOY"
    assert r["started_at"] == 1_010_000  # inc.degradation_start in ms
    assert r["ended_at"] is None
    assert r["expected_trigger"] == "LATENCY_HIGH"
    assert r["expected_action"] == "ROLLBACK"
    assert r["event_time"] == 1_010_500


def test_end_record_carries_both_timestamps():
    r = end_record(_inc(), now=1070.0)
    assert r["started_at"] == 1_010_000
    assert r["ended_at"] == 1_070_000
    assert r["event_time"] == 1_070_000


def test_ground_truth_carries_no_versions():
    # D-045: a ROLLBACK target is judged against service_health, so the answer key
    # holds only what happened and what the right action is.
    for itype in (BAD_DEPLOY, TRAFFIC_SURGE, UPSTREAM_DROP):
        for rec in (start_record(_inc(itype), now=1010.0), end_record(_inc(itype), now=1070.0)):
            assert not any("version" in key for key in rec)


def test_schema_fields_match_the_avsc():
    """The dict keys must match schemas/service_incidents.avsc exactly."""
    import json
    from pathlib import Path

    avsc = json.loads((Path(__file__).resolve().parents[2] / "schemas/service_incidents.avsc").read_text())
    expected = {f["name"] for f in avsc["fields"]}
    assert set(start_record(_inc(), now=1010.0)) == expected
    assert set(end_record(_inc(), now=1070.0)) == expected


class _RecordingSink:
    def __init__(self):
        self.events, self.incidents = [], []

    def emit_deployments(self, rs): pass
    def emit_metrics(self, rs): pass
    def emit_event(self, ev): self.events.append(ev)
    def emit_incident(self, r): self.incidents.append(r)
    def flush(self): pass


def test_driver_emits_two_records_per_incident(monkeypatch):
    """Every incident_start/incident_end event feeds through sink.emit_incident."""
    monkeypatch.setattr("opsgen.driver.wait_until", lambda _: None)
    monkeypatch.setattr("opsgen.driver.time.time", lambda: 10**12)  # trip the no-sleep path
    sim = Simulation(seed=3, start=0.0)
    sim.bootstrap()
    sink = _RecordingSink()

    # Drive the loop by hand with a KeyboardInterrupt stop-gap after enough ticks
    # to guarantee incidents: the first ones start at 150 s (D-060).
    k_counter = {"k": 0}
    real_tick = sim.tick

    def stopping_tick(now, decisions=()):
        k_counter["k"] += 1
        if k_counter["k"] > 600:  # 10 simulated minutes
            raise KeyboardInterrupt
        return real_tick(now, decisions)

    sim.tick = stopping_tick
    run_realtime(sim, sink, root=None)

    starts_sent = [r for r in sink.incidents if r["ended_at"] is None]
    ends_sent = [r for r in sink.incidents if r["ended_at"] is not None]
    starts_seen = [e for e in sink.events if e.kind == "incident_start"]
    ends_seen = [e for e in sink.events if e.kind == "incident_end"]
    assert len(starts_sent) == len(starts_seen)
    assert len(ends_sent) == len(ends_seen)
    # Every ended record pairs with a started one by id.
    start_ids = {r["incident_id"] for r in starts_sent}
    for r in ends_sent:
        assert r["incident_id"] in start_ids
