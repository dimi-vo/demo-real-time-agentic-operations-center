"""--dry-run JSON lines and --simulate summary output (sections 4.5 and the acceptance
criteria)."""

import io
import json

import pytest

from opsgen.driver import run_simulate
from opsgen.engine import BAD_DEPLOY, Config, Decision, Event
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink, event_json, format_event


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
    # One line per service per window: SUM(traffic_count) and AVG(latency_ms) like lab 2.3,
    # and the share of failed requests like lab 2.2 (D-058, D-067).
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


def _clock(line):
    mm, ss = (int(x) for x in line.split("[", 1)[1].split("]", 1)[0].split(":"))
    return mm * 60 + ss


def test_simulate_quiet_period_before_first_incident():
    lines = _simulate(minutes=60).splitlines()
    starts = [ln for ln in lines if "INCIDENT START" in ln and "web-gateway" not in ln]
    assert starts, "expected incidents over an hour"
    # No payments incident starts before t=02:30, the detector's warm-up (D-060).
    assert min(_clock(ln) for ln in starts) == Config().quiet_period == 150
    # web-gateway's clock ignores it (D-067): a good deploy at 01:00, then a bad one.
    deploys = [ln for ln in lines if "DEPLOY " in ln and "web-gateway" in ln and "INCIDENT" not in ln]
    assert deploys[0].startswith("[01:00] DEPLOY          web-gateway  3.4.2 -> 3.4.3")
    assert deploys[1].startswith(f"[{deploys[1][1:6]}] BAD DEPLOY      web-gateway  3.4.3 -> 3.4.4")
    assert _clock(deploys[1]) < Config().quiet_period


def test_simulate_banner_shows_the_schedule():
    first = _simulate(minutes=1).splitlines()[0]
    assert first == (
        "# ops-sim --simulate 1min  (quiet=150s max=35s gap=20s "
        "rests: BAD_DEPLOY=390s UPSTREAM_DROP=300s; "
        "web-gateway deploys: first=60s every=60-90s)"
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
    # payments' incidents and web-gateway's bad deploys (its own clock, D-067); checkout has
    # none (D-068)
    assert most == 2


def test_simulate_deploy_lines():
    # D-057: on payments a deploy is always a BAD DEPLOY, followed on the same
    # tick by its incident's start. D-067: web-gateway also has good deploys (DEPLOY, no
    # incident), and its bad ones start a BAD_DEPLOY_ERRORS.
    lines = [ln for ln in _simulate(minutes=60).splitlines() if not ln.startswith(("#", "[t="))]
    deploys = [i for i, ln in enumerate(lines) if "DEPLOY " in ln and "INCIDENT" not in ln]
    kinds = set()
    for i in deploys:
        clock = lines[i].split("]", 1)[0]
        if "web-gateway" not in lines[i]:
            assert lines[i].startswith(f"{clock}] BAD DEPLOY      payments "), lines[i]
            assert lines[i + 1].startswith(f"{clock}] INCIDENT START  BAD_DEPLOY "), lines[i + 1]
            kinds.add("shared")
        elif lines[i].startswith(f"{clock}] DEPLOY "):
            assert not lines[i + 1].startswith(f"{clock}] INCIDENT START"), lines[i + 1]
            kinds.add("good")
        else:
            assert lines[i].startswith(f"{clock}] BAD DEPLOY "), lines[i]
            assert lines[i + 1].startswith(f"{clock}] INCIDENT START  BAD_DEPLOY_ERRORS web-gateway"), lines[i + 1]
            kinds.add("bad")
    assert kinds == {"shared", "good", "bad"}


def test_simulate_web_gateway_bad_deploy_ends_with_a_rollback():
    ends = [ln for ln in _simulate(minutes=10).splitlines() if "INCIDENT END    BAD_DEPLOY_ERRORS" in ln]
    assert ends and all(" rollback " in ln and ln.endswith("expired after 35 s") for ln in ends), ends


def test_simulate_web_gateway_crosses_25_percent_errors_only_in_a_bad_deploy():
    # The summary lines sum a window's errors and requests the way lab 2.2 does. Nothing resolves
    # an incident in --simulate, so each bad deploy fails 40 % of requests for 35 s: the three
    # windows ending 10, 20 and 30 s after it are fully bad, and the one ending at 40 s holds
    # 5 bad seconds (about 20 %).
    lines = _simulate(minutes=60).splitlines()
    bad = [_clock(ln) for ln in lines if "BAD DEPLOY      web-gateway" in ln]
    err = {}  # window end -> err %
    for ln in lines:
        if ln.startswith("[t=") and "web-gateway" in ln:
            err[_clock("[" + ln[3:])] = float(ln.split("err=")[1].split("%")[0])
    full = {b + j for b in bad for j in (10, 20, 30)} & err.keys()
    high = {t for t, e in err.items() if e >= 25}
    assert len(full) > 30 and full <= high
    assert all(30 <= err[t] <= 50 for t in full)
    assert high <= {b + j for b in bad for j in (10, 20, 30, 40)}


def test_simulate_gives_payments_bad_deploys_and_drops_and_nobody_a_surge():
    # D-068: the shared scheduler serves payments alone; surges are retired.
    text = _simulate(minutes=60)
    starts = [ln.split("]", 1)[1].split()[2:4] for ln in text.splitlines() if "INCIDENT START" in ln]
    shared = {(itype, svc) for itype, svc in starts if svc != "web-gateway"}
    assert shared == {("BAD_DEPLOY", "payments"), ("UPSTREAM_DROP", "payments")}
    assert {itype for itype, svc in starts if svc == "web-gateway"} == {"BAD_DEPLOY_ERRORS"}
    assert "SURGE" not in text and "TRAFFIC_HIGH" not in text and "SCALE_OUT" not in text


def test_simulate_bad_deploy_rolls_back():
    text = _simulate(minutes=60)
    assert "INCIDENT END" in text
    assert "rollback" in text  # a bad deploy ends with a rollback


def test_no_incidents_simulate_has_no_incidents():
    text = _simulate(minutes=30, no_incidents=True)
    assert "INCIDENT START" not in text
    assert "DEPLOY" not in text


def test_an_incident_end_line_says_who_resolved_it_and_when():
    # D-065. A bad deploy on payments from 0, its ROLLBACK read before tick 17.
    sim = Simulation(1, start=0.0, config=Config(quiet_period=0))
    picks = iter([(BAD_DEPLOY, "payments")])
    sim.engine._choose_incident = lambda now: next(picks, None)
    sim.tick(0.0)
    for k in range(1, 17):
        sim.tick(float(k))
    answer = Decision("payments", 10.0, "release_agent", "LATENCY_HIGH", "ROLLBACK")
    [end] = sim.tick(17.0, decisions=[answer])[2]
    assert format_event(end, clock="00:17") == (
        "[00:17] INCIDENT END    BAD_DEPLOY     payments     rollback 2.9.5 -> 2.9.4, "
        "resolved by release_agent ROLLBACK after 17 s"
    )


def test_simulate_without_decisions_every_incident_expires_at_the_cap():
    # --simulate reads no decisions (D-065), so every incident runs its 35 s.
    ends = [ln for ln in _simulate(minutes=60).splitlines() if "INCIDENT END" in ln]
    on_payments = [ln for ln in ends if " payments " in ln]
    assert 16 <= len(on_payments) <= 20  # 18 in the first hour on payments alone (D-068)
    assert all(ln.endswith(" expired after 35 s") for ln in ends), ends
    assert any(", expired after 35 s" in ln and "rollback" in ln for ln in ends)


def test_cli_has_no_timing_flags():
    # D-060: timing lives in Config only. --seed, --no-incidents, --simulate and --dry-run stay.
    # --show-records only prints (at a fixed 5 s) and leaves the schedule alone.
    from opsgen.cli import _build_parser, _config_from_args

    parser = _build_parser()
    assert _config_from_args(parser.parse_args([])) == Config()
    assert _config_from_args(parser.parse_args(["--no-incidents"])) == Config(no_incidents=True)
    assert _config_from_args(parser.parse_args(["--show-records", "checkout"])) == Config()
    flags = {opt for action in parser._actions for opt in action.option_strings}
    assert flags == {"-h", "--help", "--dry-run", "--simulate", "--seed", "--no-incidents", "--creds", "--show-records"}
    for gone in ("--quiet-period", "--spacing-min", "--spacing-max", "--duration", "--cooldown"):
        with pytest.raises(SystemExit):
            parser.parse_args([gone, "30"])


def test_a_good_deploy_in_the_dry_run_json_and_on_the_console():
    # D-067: web-gateway's good deploy is an event of its own, with no incident.
    ev = Event("deploy", 60.0, "web-gateway", "3.4.2", "3.4.3")
    assert event_json(ev) == {
        "_event": "deploy",
        "service_name": "web-gateway",
        "event_time": 60_000,
        "old_version": "3.4.2",
        "new_version": "3.4.3",
    }
    assert format_event(ev, clock="01:00") == "[01:00] DEPLOY          web-gateway  3.4.2 -> 3.4.3"


# --- --show-records SERVICE: one service_metrics record every 5 s -----------------------


def test_show_records_takes_one_of_the_three_services():
    from opsgen.cli import _build_parser

    parser = _build_parser()
    assert parser.parse_args([]).show_records is None
    assert parser.parse_args(["--show-records", "checkout"]).show_records == "checkout"
    for bad in (["--show-records"], ["--show-records", "catalog"]):
        with pytest.raises(SystemExit):
            parser.parse_args(bad)


def test_show_records_needs_real_time(capsys):
    from opsgen.cli import main

    with pytest.raises(SystemExit) as exit_:
        main(["--dry-run", "--simulate", "1", "--show-records", "checkout"])
    assert "--show-records" in str(exit_.value.code) and "--simulate" in str(exit_.value.code)


def _ticks(show_records, seconds=12):
    """Tick a dry run for `seconds` the way the real-time loop does. Returns stdout's JSON
    records."""
    from opsgen.driver import tick_once

    sim = Simulation(1, start=1_800_000_000.0)
    out = io.StringIO()
    sink = DryRunSink(out=out, log=io.StringIO())
    for k in range(seconds):
        tick_once(sim, sink, sim.start + k, show_records=show_records)
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_show_records_prints_the_services_record_every_5_seconds_as_sent(capsys):
    sent = _ticks("checkout")
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 3  # ticks 0, 5 and 10: the first one right away, then with the heartbeat
    checkout = [{k: v for k, v in r.items() if k != "_stream"} for r in sent
                if r["_stream"] == "service_metrics" and r["service_name"] == "checkout"]  # fmt: skip
    for k, line in zip((0, 5, 10), lines, strict=True):
        clock, rest = line.split("] ", 1)
        assert clock.startswith("[") and len(clock) == len("[10:42:05")
        prefix = "service_metrics key=checkout "
        assert rest.startswith(prefix)
        assert json.loads(rest[len(prefix):]) == checkout[k]  # the record the sink was given


def test_without_show_records_nothing_more_is_printed_and_stdout_is_unchanged(capsys):
    plain = _ticks(None)
    assert capsys.readouterr().err == ""
    assert _ticks("payments") == plain  # the sample goes to stderr only
