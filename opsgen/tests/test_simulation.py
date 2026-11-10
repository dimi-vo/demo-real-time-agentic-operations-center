"""Simulation tick: record shape, re-emit cadence, bootstrap, and never-zero traffic."""

import json
from pathlib import Path

from opsgen.engine import UPSTREAM_DROP, Config, Incident
from opsgen.services import service_names
from opsgen.simulation import REEMIT_INTERVAL, Simulation

REQUEST_FIELDS = {
    "event_time",
    "service_name",
    "instance_id",
    "http_method",
    "route",
    "status_code",
    "latency_ms",
    "response_bytes",
    "trace_id",
}
DEPLOYMENT_FIELDS = {
    "service_name",
    "version",
    "previous_version",
    "deployed_at",
    "last_change",
    "desired_replicas",
    "ready_replicas",
    "max_replicas",
    "tier",
    "owner_team",
    "event_time",
}

AVSC_DIR = Path(__file__).resolve().parents[2] / "schemas"


def _sim(seed=0, config=None):
    return Simulation(seed, start=0.0, config=config or Config())


def test_bootstrap_emits_every_service():
    sim = _sim()
    boot = sim.bootstrap()
    assert {r["service_name"] for r in boot} == set(service_names())
    assert all(r["last_change"] == "INITIAL" for r in boot)


def test_request_record_fields_match_schema():
    sim = _sim(seed=1)
    reqs, _, _ = sim.tick(0.0)
    assert reqs, "expected some requests in a tick"
    for r in reqs:
        assert set(r) == REQUEST_FIELDS
    avsc = json.loads((AVSC_DIR / "service_requests.avsc").read_text())
    assert {f["name"] for f in avsc["fields"]} == REQUEST_FIELDS


def test_deployment_record_fields_match_schema():
    sim = _sim()
    rec = sim._dep_record("checkout", 0.0)
    assert set(rec) == DEPLOYMENT_FIELDS
    avsc = json.loads((AVSC_DIR / "service_deployments.avsc").read_text())
    assert {f["name"] for f in avsc["fields"]} == DEPLOYMENT_FIELDS


def test_route_field_combines_method_and_path():
    sim = _sim(seed=2)
    reqs, _, _ = sim.tick(0.0)
    for r in reqs:
        assert r["route"].startswith(r["http_method"] + " ")


def test_reemit_every_five_seconds():
    sim = _sim(seed=3, config=Config(no_incidents=True, benign_first=1e9))  # no deploys to interfere
    # No re-emit at tick 0 (bootstrap covers it); full re-emit at the 5s boundary.
    _, deps0, _ = sim.tick(0.0)
    assert deps0 == []
    _, deps5, _ = sim.tick(float(REEMIT_INTERVAL))
    assert {r["service_name"] for r in deps5} == set(service_names())
    _, deps6, _ = sim.tick(6.0)
    assert deps6 == []


def test_deploy_emits_deployment_record_and_mutates_version():
    # Force an immediate benign deploy by making the engine fire one at tick 0.
    sim = _sim(seed=4, config=Config(no_incidents=True, benign_first=0.0, benign_min=1e9, benign_max=1e9))
    _, deps, events = sim.tick(0.0)
    deployed = [e for e in events if e.kind == "benign_deploy"]
    assert deployed, "expected a benign deploy at tick 0"
    svc = deployed[0].service_name
    assert any(r["service_name"] == svc and r["last_change"] == "DEPLOY" for r in deps)
    assert sim.deployments[svc].version != sim.deployments[svc].previous_version


def test_upstream_drop_never_zeroes_a_window():
    sim = _sim(seed=5, config=Config(no_incidents=True, benign_first=1e9))
    # Force a drop on payments (the lowest-rps service) for a 10s window.
    sim.engine.active = Incident(
        incident_id="INC0001",
        type=UPSTREAM_DROP,
        service_name="payments",
        t0=0.0,
        degradation_start=0.0,
        degradation_end=70.0,
        expected_trigger="TRAFFIC_LOW",
        expected_action="INVESTIGATE_UPSTREAM",
        _start_fired=True,
    )
    total = 0
    for k in range(10):
        reqs, _, _ = sim.tick(float(k))
        total += sum(1 for r in reqs if r["service_name"] == "payments")
    assert total > 0, "a dropped service must still produce at least one request per window"


def test_bad_deploy_window_raises_latency_and_errors():
    from opsgen.engine import BAD_DEPLOY

    sim = _sim(seed=6, config=Config(no_incidents=True, benign_first=1e9))
    sim.engine.active = Incident(
        incident_id="INC0001",
        type=BAD_DEPLOY,
        service_name="checkout",
        t0=0.0,
        degradation_start=0.0,
        degradation_end=70.0,
        expected_trigger="LATENCY_HIGH",
        expected_action="ROLLBACK",
        _deploy_fired=True,
        _start_fired=True,
    )
    lat = []
    errors = 0
    count = 0
    for k in range(10):
        reqs, _, _ = sim.tick(float(k))
        for r in reqs:
            if r["service_name"] == "checkout":
                lat.append(r["latency_ms"])
                count += 1
                if r["status_code"] >= 500:
                    errors += 1
    assert sum(lat) / len(lat) > 300  # baseline checkout median is 140ms; x4 under bad deploy
    assert errors / count > 0.05  # ~12% server errors
