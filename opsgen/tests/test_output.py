"""--dry-run JSON lines and --simulate summary output (sections 4.5 and the acceptance
criteria)."""

import io
import json
import re
from collections import Counter
from datetime import datetime, timezone

import pytest

from opsgen.driver import run_simulate
from opsgen.engine import BAD_DEPLOY, Config, Decision, Event
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink, event_json, format_event
from opsgen.tickets import NewTicket


def test_dry_run_sink_emits_tagged_json_lines():
    out, log = io.StringIO(), io.StringIO()
    sink = DryRunSink(out=out, log=log)
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True))
    sink.emit_deployments(sim.bootstrap())
    metrics, _, _, _ = sim.tick(0.0)
    sink.emit_metrics(metrics)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    deployments = [x for x in lines if x.get("_stream") == "service_deployments"]
    samples = [x for x in lines if x.get("_stream") == "service_metrics"]
    assert len(deployments) == 2
    assert len(samples) == 2  # one per metric service per second (D-058, D-068)
    for r in samples:
        assert set(r) == {"_stream", "event_time", "service_name", "traffic_count", "latency_ms", "error_count"}


def _simulate(seed=42, minutes=60, **cfg):
    sim = Simulation(seed, start=0.0, config=Config(**cfg))
    buf = io.StringIO()
    run_simulate(sim, minutes, out=buf)
    return buf.getvalue()


def test_simulate_has_per_window_summaries_for_the_metric_services():
    text = _simulate(minutes=1)
    # first window closes at t=00:10, one line per metric service; checkout has no metrics (D-068)
    summaries = [ln.split()[1] for ln in text.splitlines() if ln.startswith("[t=00:10]")]
    assert summaries == ["web-gateway", "payments"]
    assert not any(ln.startswith("[t=") and " checkout " in ln for ln in text.splitlines())


def test_simulate_summary_sums_the_window_records():
    # One line per service per window: SUM(traffic_count) and AVG(latency_ms) like lab 2.3,
    # and the share of failed requests like lab 2.2 (D-058, D-067).
    sim = Simulation(5, start=0.0, config=Config(no_incidents=True))
    expect = {}
    shadow = Simulation(5, start=0.0, config=Config(no_incidents=True))
    window = [r for k in range(10) for r in shadow.tick(float(k))[0] if r["service_name"] == "payments"]
    traffic = sum(r["traffic_count"] for r in window)
    expect["traffic"] = f"traffic={traffic:>4}"
    expect["lat"] = f"lat={sum(r['latency_ms'] for r in window) / 10:6.1f}ms"
    expect["err"] = f"err={100 * sum(r['error_count'] for r in window) / traffic:4.1f}%"
    buf = io.StringIO()
    run_simulate(sim, 10 / 60, out=buf)
    (line,) = [ln for ln in buf.getvalue().splitlines() if ln.startswith("[t=00:10] payments")]
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
        "web-gateway deploys: first=60s every=60-90s; "
        "checkout tickets: 1/15s P1=30% P2=30% P3=40% P1-HIGH=50%)"  # D-068
    )
    no_incidents = _simulate(minutes=1, no_incidents=True).splitlines()[0]
    assert no_incidents == "# ops-sim --simulate 1min  (no incidents, no tickets)"


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
    # The schedule's lines: checkout's tickets, and their summary, expect SCALE_OUT (D-068).
    schedule = "\n".join(ln for ln in text.splitlines() if ln.startswith("[") and " TICKET " not in ln)
    assert "SURGE" not in schedule and "TRAFFIC_HIGH" not in schedule and "SCALE_OUT" not in schedule


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
    assert _config_from_args(parser.parse_args(["--show-records", "payments"])) == Config()
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


def test_show_records_takes_any_of_the_three_services():
    from opsgen.cli import _build_parser

    parser = _build_parser()
    assert parser.parse_args([]).show_records is None
    for svc in ("web-gateway", "checkout", "payments"):  # checkout shows its tickets (D-068)
        assert parser.parse_args(["--show-records", svc]).show_records == svc
    for bad in (["--show-records"], ["--show-records", "catalog"]):
        with pytest.raises(SystemExit):
            parser.parse_args(bad)
    help_text = " ".join(parser.format_help().split())
    assert "(web-gateway, checkout, payments)" in help_text
    assert "support_tickets" in help_text and "ticket_answer_key" in help_text


def test_the_announcement_says_what_show_records_prints(capsys):
    from opsgen.cli import _announce

    _announce("checkout")
    assert capsys.readouterr().err == (
        "Showing checkout's support_tickets and ticket_answer_key records as they are produced.\n"
    )
    _announce("payments")
    assert capsys.readouterr().err == "Showing payments's service_metrics record every 5 s, as it is sent.\n"


def test_show_records_needs_real_time(capsys):
    from opsgen.cli import main

    with pytest.raises(SystemExit) as exit_:
        main(["--dry-run", "--simulate", "1", "--show-records", "payments"])
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
    sent = _ticks("payments")
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 3  # ticks 0, 5 and 10: the first one right away, then with the heartbeat
    payments = [{k: v for k, v in r.items() if k != "_stream"} for r in sent
                if r["_stream"] == "service_metrics" and r["service_name"] == "payments"]  # fmt: skip
    for k, line in zip((0, 5, 10), lines, strict=True):
        clock, rest = line.split("] ", 1)
        assert clock.startswith("[") and len(clock) == len("[10:42:05")
        prefix = "service_metrics key=payments "
        assert rest.startswith(prefix)
        assert json.loads(rest[len(prefix):]) == payments[k]  # the record the sink was given


def test_without_show_records_nothing_more_is_printed_and_stdout_is_unchanged(capsys):
    plain = _ticks(None)
    assert capsys.readouterr().err == ""
    assert _ticks("web-gateway") == plain  # the sample goes to stderr only


# --- checkout's tickets (D-068) ------------------------------------------------------------

HIGH_TICKET = NewTicket(
    {
        "event_time": 1_800_000_005_000,
        "ticket_id": "ZD-48215",
        "source": "ZENDESK",
        "service_name": "checkout",
        "priority": "P1",
        "subject": "Checkout times out during the flash sale",
        "description": "Since the sale started at 10:05, checkout spins for a minute and then times out.",
    },
    {"event_time": 1_800_000_005_000, "ticket_id": "ZD-48215", "true_severity": "HIGH", "expected_action": "SCALE_OUT"},
)
LOW_TICKET = NewTicket(
    {
        "event_time": 1_800_000_007_000,
        "ticket_id": "INC0041872",
        "source": "SERVICENOW",
        "service_name": "checkout",
        "priority": "P3",
        "subject": "Order confirmation email delayed",
        "description": "I placed order RS-482913 an hour ago and the confirmation email has not arrived yet.",
    },
    {"event_time": 1_800_000_007_000, "ticket_id": "INC0041872", "true_severity": "LOW", "expected_action": None},
)


def test_dry_run_writes_each_ticket_and_its_answer_key_and_a_console_line():
    out, log = io.StringIO(), io.StringIO()
    DryRunSink(out=out, log=log).emit_tickets([HIGH_TICKET, LOW_TICKET])
    assert [json.loads(line) for line in out.getvalue().splitlines()] == [
        {"_stream": "support_tickets", **HIGH_TICKET.ticket},
        {"_stream": "ticket_answer_key", **HIGH_TICKET.answer_key},
        {"_stream": "support_tickets", **LOW_TICKET.ticket},
        {"_stream": "ticket_answer_key", **LOW_TICKET.answer_key},
    ]
    # The answer key is set apart, so nobody takes it for part of the ticket.
    assert log.getvalue().splitlines() == [
        '[08:00:05] TICKET          P1  ZD-48215    Zendesk     "Checkout times out during the flash sale"'
        "  [truth: HIGH, SCALE_OUT]",
        '[08:00:07] TICKET          P3  INC0041872  ServiceNow  "Order confirmation email delayed"  [truth: LOW]',
    ]


def test_a_live_or_dry_run_tick_sends_its_tickets_to_the_sink():
    sent = _ticks(None, seconds=120)
    tickets = [r for r in sent if r.get("_stream") == "support_tickets"]
    keys = [r for r in sent if r.get("_stream") == "ticket_answer_key"]
    assert len(tickets) >= 3
    assert [r["ticket_id"] for r in tickets] == [r["ticket_id"] for r in keys]


def test_show_records_checkout_prints_each_ticket_and_its_answer_key_as_produced(capsys):
    sent = _ticks("checkout", seconds=120)
    lines = capsys.readouterr().err.splitlines()
    produced = [r for r in sent if r.get("_stream") in ("support_tickets", "ticket_answer_key")]
    assert len(produced) >= 6 and len(lines) == len(produced)  # every one, not every 5 s
    for record, line in zip(produced, lines, strict=True):
        topic = record.pop("_stream")
        key = record["service_name"] if topic == "support_tickets" else record["ticket_id"]
        clock, rest = line.split("] ", 1)
        assert clock == "[" + datetime.fromtimestamp(record["event_time"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        prefix = f"{topic} key={key} "
        assert rest.startswith(prefix), line
        assert json.loads(rest[len(prefix):]) == record  # the record the sink was given
    assert "service_metrics" not in " ".join(lines)  # checkout has none


TICKET_LINE = re.compile(
    r'\[(\d\d:\d\d)\] TICKET {10}(P[123])  (?:ZD-\d{5}  |INC\d{7})  (?:Zendesk   |ServiceNow)  ".+"  '
    r"\[truth: (HIGH|LOW)(?:, (ROLLBACK|SCALE_OUT|INVESTIGATE_UPSTREAM))?\]"
)


def test_simulate_prints_every_ticket_from_the_first_minute():
    lines = _simulate(seed=1, minutes=15).splitlines()
    tickets = [ln for ln in lines if " TICKET " in ln]
    assert all(TICKET_LINE.fullmatch(ln) for ln in tickets), [ln for ln in tickets if not TICKET_LINE.fullmatch(ln)]
    assert _clock(tickets[0]) < 60  # no quiet period
    assert 40 <= len(tickets) <= 80  # about 4 a minute


def test_simulate_ends_with_the_tickets_by_priority_and_true_severity_and_the_high_ones_by_action():
    lines = _simulate(seed=3, minutes=60).splitlines()
    drawn = [TICKET_LINE.fullmatch(ln).groups() for ln in lines if " TICKET " in ln]
    by = Counter((prio, sev) for _, prio, sev, _ in drawn)
    actions = Counter(action for _, _, sev, action in drawn if sev == "HIGH")
    summary = lines[-5:]
    assert summary[0] == f"# tickets: {len(drawn)} in 60 min, {len(drawn) / 60:.1f} a minute"
    for prio, line in zip(("P1", "P2", "P3"), summary[1:4], strict=True):
        assert line == f"#   {prio}: {by[prio, 'HIGH']} HIGH, {by[prio, 'LOW']} LOW"
    assert summary[4] == (
        f"#   HIGH by expected action: ROLLBACK {actions['ROLLBACK']}, SCALE_OUT {actions['SCALE_OUT']}, "
        f"INVESTIGATE_UPSTREAM {actions['INVESTIGATE_UPSTREAM']}"
    )
    assert by["P2", "HIGH"] == by["P3", "HIGH"] == 0 and min(actions.values()) > 0


def test_simulate_without_incidents_says_tickets_are_off():
    text = _simulate(minutes=10, no_incidents=True)
    assert " TICKET " not in text
    assert text.splitlines()[-1] == "# tickets: off (--no-incidents)"
