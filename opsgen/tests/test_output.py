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
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True))
    sink.emit_deployments(sim.bootstrap())
    metrics, _, _ = sim.tick(0.0)
    sink.emit_metrics(metrics)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    deployments = [x for x in lines if x.get("_stream") == "service_deployments"]
    samples = [x for x in lines if x.get("_stream") == "service_metrics"]
    assert len(deployments) == 3
    assert len(samples) == 3  # one per service per second (D-058)
    for r in samples:
        assert set(r) == {"_stream", "event_time", "service_name", "traffic_count", "latency_ms", "error_count"}


def _simulate(seed=42, minutes=60, **cfg):
    sim = Simulation(seed, start=0.0, config=Config(**cfg))
    buf = io.StringIO()
    run_simulate(sim, minutes, out=buf)
    return buf.getvalue()


def test_simulate_has_per_window_summaries_for_all_services():
    text = _simulate(minutes=1)
    # first window closes at t=00:10, one line per service
    for name in ("web-gateway", "checkout", "payments"):
        assert f"[t=00:10] {name:<12} traffic=" in text


def test_simulate_summary_sums_the_window_records():
    # One line per service per window: SUM(traffic_count), AVG(latency_ms) and the share of
    # failed requests, like lab 2.2 (D-058).
    sim = Simulation(5, start=0.0, config=Config(no_incidents=True))
    expect = {}
    shadow = Simulation(5, start=0.0, config=Config(no_incidents=True))
    window = [r for k in range(10) for r in shadow.tick(float(k))[0] if r["service_name"] == "checkout"]
    traffic = sum(r["traffic_count"] for r in window)
    expect["traffic"] = f"traffic={traffic:>4}"
    expect["lat"] = f"lat={sum(r['latency_ms'] for r in window) / 10:6.1f}ms"
    expect["err"] = f"err={100 * sum(r['error_count'] for r in window) / traffic:4.1f}%"
    buf = io.StringIO()
    run_simulate(sim, 10 / 60, out=buf)
    (line,) = [ln for ln in buf.getvalue().splitlines() if ln.startswith("[t=00:10] checkout")]
    for part in expect.values():
        assert part in line, (part, line)


def test_simulate_quiet_period_before_first_incident():
    text = _simulate(minutes=60, quiet_period=180)
    starts = [ln for ln in text.splitlines() if "INCIDENT START" in ln]
    assert starts, "expected incidents over an hour"
    # No incident starts before t=03:00.
    for ln in starts:
        mmss = ln.split("[", 1)[1].split("]", 1)[0]
        mm, ss = (int(x) for x in mmss.split(":"))
        assert mm * 60 + ss >= 180


def test_simulate_every_deploy_is_a_bad_deploy():
    # D-057: a deploy line is always a BAD DEPLOY, followed on the same tick by its
    # incident's start.
    lines = [ln for ln in _simulate(minutes=60).splitlines() if not ln.startswith(("#", "[t="))]
    deploys = [i for i, ln in enumerate(lines) if "DEPLOY" in ln and "INCIDENT" not in ln]
    assert deploys, "expected a bad deploy over an hour"
    for i in deploys:
        clock = lines[i].split("]", 1)[0]
        assert lines[i].startswith(f"{clock}] BAD DEPLOY "), lines[i]
        assert lines[i + 1].startswith(f"{clock}] INCIDENT START  BAD_DEPLOY "), lines[i + 1]


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
    assert "DEPLOY" not in text


def test_cli_schedule_overrides_reach_the_config():
    from opsgen.cli import _build_parser, _config_from_args

    defaults = _config_from_args(_build_parser().parse_args([]))
    assert defaults == Config()
    assert (defaults.duration, defaults.cooldown) == (20.0, 420.0)
    args = _build_parser().parse_args(["--duration", "30", "--cooldown", "600"])
    config = _config_from_args(args)
    assert (config.duration, config.cooldown) == (30.0, 600.0)
