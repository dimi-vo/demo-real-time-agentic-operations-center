"""--dry-run JSON lines and --simulate summary output (sections 4.5 and the acceptance
criteria)."""

import io
import json

import pytest

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
    text = _simulate(minutes=60)
    starts = [ln for ln in text.splitlines() if "INCIDENT START" in ln]
    assert starts, "expected incidents over an hour"
    # No incident starts before t=02:30, the detector's warm-up (D-060).
    clocks = []
    for ln in starts:
        mmss = ln.split("[", 1)[1].split("]", 1)[0]
        mm, ss = (int(x) for x in mmss.split(":"))
        clocks.append(mm * 60 + ss)
    assert min(clocks) == Config().quiet_period == 150


def test_simulate_banner_shows_the_schedule():
    first = _simulate(minutes=1).splitlines()[0]
    assert first == (
        "# ops-sim --simulate 1min  (quiet=150s duration=20s gap=20s "
        "rests: BAD_DEPLOY=390s TRAFFIC_SURGE=300s UPSTREAM_DROP=300s)"
    )
    assert _simulate(minutes=1, no_incidents=True).splitlines()[0] == "# ops-sim --simulate 1min  (no incidents)"


def test_simulate_an_hour_has_overlapping_incidents():
    # D-060: incidents on different services overlap. Track what is open per service.
    open_now: dict[str, str] = {}
    most = 0
    for ln in _simulate(minutes=60).splitlines():
        if not ln.startswith("[") or "INCIDENT" not in ln:
            continue
        _, kind, itype, svc = ln.split("]", 1)[1].split()[:4]  # INCIDENT START|END type service
        if kind == "START":
            assert svc not in open_now, ln  # never two at once on one service
            open_now[svc] = itype
            most = max(most, len(open_now))
        else:
            assert open_now.pop(svc) == itype, ln
    assert most == 3


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


def test_cli_has_no_timing_flags():
    # D-060: timing lives in Config only. --seed, --no-incidents, --simulate and --dry-run stay.
    from opsgen.cli import _build_parser, _config_from_args

    parser = _build_parser()
    assert _config_from_args(parser.parse_args([])) == Config()
    assert _config_from_args(parser.parse_args(["--no-incidents"])) == Config(no_incidents=True)
    flags = {opt for action in parser._actions for opt in action.option_strings}
    assert flags == {"-h", "--help", "--dry-run", "--simulate", "--seed", "--no-incidents", "--creds"}
    for gone in ("--quiet-period", "--spacing-min", "--spacing-max", "--duration", "--cooldown"):
        with pytest.raises(SystemExit):
            parser.parse_args([gone, "30"])
