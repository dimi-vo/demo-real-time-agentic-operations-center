"""--dry-run JSON lines and --simulate summary output (sections 4.5 and the acceptance
criteria)."""

import io
import json

from opsgen.driver import run_simulate
from opsgen.engine import Config
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink


def test_dry_run_sink_emits_tagged_json_lines():
    out, log = io.StringIO(), io.StringIO()
    sink = DryRunSink(out=out, log=log)
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True, benign_first=1e9))
    sink.emit_deployments(sim.bootstrap())
    reqs, _, _ = sim.tick(0.0)
    sink.emit_requests(reqs)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    deployments = [x for x in lines if x.get("_stream") == "service_deployments"]
    requests = [x for x in lines if x.get("_stream") == "service_requests"]
    assert len(deployments) == 6
    assert requests
    r = requests[0]
    for field in ("event_time", "service_name", "instance_id", "http_method", "route", "status_code", "latency_ms",
                  "response_bytes", "trace_id"):
        assert field in r


def _simulate(seed=42, minutes=60, **cfg):
    sim = Simulation(seed, start=0.0, config=Config(**cfg))
    buf = io.StringIO()
    run_simulate(sim, minutes, out=buf)
    return buf.getvalue()


def test_simulate_has_per_window_summaries_for_all_services():
    text = _simulate(minutes=1)
    # first window closes at t=00:10, one line per service
    for name in ("web-gateway", "auth", "catalog", "cart", "checkout", "payments"):
        assert f"] {name:<12} reqs=" in text


def test_simulate_quiet_period_before_first_incident():
    text = _simulate(minutes=60, quiet_period=180)
    starts = [ln for ln in text.splitlines() if "INCIDENT START" in ln]
    assert starts, "expected incidents over an hour"
    # No incident starts before t=03:00.
    for ln in starts:
        mmss = ln.split("[", 1)[1].split("]", 1)[0]
        mm, ss = (int(x) for x in mmss.split(":"))
        assert mm * 60 + ss >= 180


def test_simulate_shows_benign_deploys_before_first_incident():
    text = _simulate(minutes=60)
    lines = text.splitlines()
    first_incident = next((i for i, ln in enumerate(lines) if "INCIDENT START" in ln), len(lines))
    assert any("DEPLOY" in ln and "(benign)" in ln for ln in lines[:first_incident])


def test_simulate_covers_all_incident_types_over_an_hour():
    text = _simulate(minutes=60)
    assert "BAD_DEPLOY" in text
    assert "TRAFFIC_SURGE" in text
    assert "UPSTREAM_DROP" in text


def test_simulate_bad_deploy_rolls_back():
    text = _simulate(minutes=60)
    assert "INCIDENT END" in text
    assert "rollback" in text  # a bad deploy ends with a rollback


def test_no_incidents_simulate_has_no_incidents():
    text = _simulate(minutes=30, no_incidents=True)
    assert "INCIDENT START" not in text
    assert "DEPLOY" in text
