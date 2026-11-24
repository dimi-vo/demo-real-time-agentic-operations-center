"""checkout's support tickets (D-068): the templates, the ticket clock and the answer key."""

import io
import json
import random
import re
import string
from collections import Counter
from pathlib import Path

import pytest

from opsgen import tickets
from opsgen.driver import tick_once
from opsgen.engine import Config
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink
from opsgen.state import load_state, save_state, state_path
from opsgen.ticket_templates import POOLS, TEMPLATES
from opsgen.tickets import TicketClock


def _placeholders(text):
    return {name for _, name, _, _ in string.Formatter().parse(text) if name is not None}


# --- templates -----------------------------------------------------------------------------


def test_every_placeholder_in_every_template_has_a_pool():
    used = set()
    for t in TEMPLATES:
        for text in (t.subject, t.description):
            names = _placeholders(text)
            assert "" not in names, f"positional placeholder in {text!r}"
            assert names <= POOLS.keys(), (names - POOLS.keys(), text)
            used |= names
    assert all(POOLS[name] for name in POOLS), "an empty pool"
    assert used == POOLS.keys(), f"pools no template uses: {POOLS.keys() - used}"


def test_the_mix_is_12_high_p1_12_low_p1_and_8_each_of_p2_and_p3():
    by = Counter((t.priority, t.true_severity) for t in TEMPLATES)
    assert by == {("P1", "HIGH"): 12, ("P1", "LOW"): 12, ("P2", "LOW"): 8, ("P3", "LOW"): 8}
    actions = Counter(t.expected_action for t in TEMPLATES if t.true_severity == "HIGH")
    assert actions == {"ROLLBACK": 4, "SCALE_OUT": 4, "INVESTIGATE_UPSTREAM": 4}
    assert all(t.expected_action is None for t in TEMPLATES if t.true_severity == "LOW")


def test_at_least_four_low_p1s_shout_and_only_low_p1s_do():
    shouting = [t for t in TEMPLATES if t.shouting]
    assert len(shouting) >= 4
    assert {(t.priority, t.true_severity) for t in shouting} == {("P1", "LOW")}
    for t in shouting:
        text = f"{t.subject} {t.description}"
        capitals = re.findall(r"\b[A-Z]{3,}\b", text)
        assert "!!" in text or len(capitals) >= 2, text


# Spec decision 10: a HIGH ticket says many customers are affected, and a LOW one never does.
MANY = re.compile(r"\bevery (customer|order|payment)|\ball customers\b|\beveryone\b|\bnobody\b|\bnot a single\b", re.I)
# A HIGH ticket has exactly one cue to its action, and none to the other two.
RELEASE = re.compile(r"\b(update|release|deploy|new (checkout )?version|rolled out|went live)\b", re.I)
LOAD = re.compile(r"\b(sale|campaign|peak|rush|traffic|busier)\b", re.I)
SLOW = re.compile(
    r"\b(slowness|times out|timing out|timeout|spins|crawling|takes over a minute|loads for ages)\b", re.I
)
UPSTREAM = re.compile(r"\b(payment provider|payment gateway|gateway)\b", re.I)


def _cues(text):
    found = set()
    if RELEASE.search(text):
        found.add("ROLLBACK")
    if LOAD.search(text) or SLOW.search(text):
        found.add("SCALE_OUT")
    if UPSTREAM.search(text):
        found.add("INVESTIGATE_UPSTREAM")
    return found


def test_every_template_matches_its_labels():
    for t in TEMPLATES:
        text = f"{t.subject} {t.description}"
        if t.true_severity == "HIGH":
            assert t.priority == "P1", text  # P2 and P3 are always really LOW
            assert MANY.search(text), f"a HIGH ticket says many customers are affected: {text}"
            assert "{time}" in t.description, f"a HIGH ticket gives a start time: {text}"
            assert _cues(text) == {t.expected_action}, (_cues(text), text)
            if t.expected_action == "SCALE_OUT":
                assert LOAD.search(text) and SLOW.search(text), f"load and slowness: {text}"
        else:
            assert not MANY.search(text), f"a LOW ticket affects one customer: {text}"


def test_a_description_is_one_paragraph_of_one_to_three_sentences():
    for t in TEMPLATES:
        sentences = re.findall(r"[.!?]+(?=\s|$)", t.description)
        assert 1 <= len(sentences) <= 3, t.description
        assert "\n" not in t.description


# --- the ticket clock -----------------------------------------------------------------------


def _run_clock(seed, seconds):
    clock = TicketClock(random.Random(seed))
    return [t for k in range(seconds) for t in clock.tick(1_800_000_000_000 + 1000 * k)]


def test_about_4_tickets_a_minute_with_the_spec_mix():
    hours = 20
    tickets = _run_clock(11, hours * 3600)
    assert len(tickets) / (hours * 60) == pytest.approx(4, abs=0.2)  # Poisson(1/15) a second
    prio = Counter(t.ticket["priority"] for t in tickets)
    n = len(tickets)
    assert prio["P1"] / n == pytest.approx(0.30, abs=0.02)
    assert prio["P2"] / n == pytest.approx(0.30, abs=0.02)
    assert prio["P3"] / n == pytest.approx(0.40, abs=0.02)
    p1 = [t for t in tickets if t.ticket["priority"] == "P1"]
    high = [t for t in p1 if t.answer_key["true_severity"] == "HIGH"]
    assert len(high) / len(p1) == pytest.approx(0.5, abs=0.04)
    assert all(t.answer_key["true_severity"] == "LOW" for t in tickets if t.ticket["priority"] != "P1")
    actions = Counter(t.answer_key["expected_action"] for t in high)
    assert set(actions) == {"ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM"}
    assert all(c / len(high) == pytest.approx(1 / 3, abs=0.05) for c in actions.values())
    sources = Counter(t.ticket["source"] for t in tickets)
    assert set(sources) == {"ZENDESK", "SERVICENOW"}
    assert sources["ZENDESK"] / n == pytest.approx(0.5, abs=0.03)


# --- in the simulation ----------------------------------------------------------------------

TICKET_FIELDS = {"event_time", "ticket_id", "source", "service_name", "priority", "subject", "description"}
ANSWER_KEY_FIELDS = {"event_time", "ticket_id", "true_severity", "expected_action"}


def _sim_tickets(sim, seconds, start=0):
    """Tick `sim` from `start` for `seconds`. Returns (tick time, ticket) pairs."""
    return [(now, t) for k in range(seconds) for now in [sim.start + start + k] for t in sim.tick(now)[3]]


def test_every_ticket_has_its_answer_key_on_the_same_tick():
    sim = Simulation(1, start=1_800_000_000.0)
    tickets = _sim_tickets(sim, 1800)
    assert len(tickets) > 60
    for now, (ticket, key) in tickets:
        assert set(ticket) == TICKET_FIELDS and set(key) == ANSWER_KEY_FIELDS
        assert ticket["event_time"] == key["event_time"] == round(now * 1000)  # the tick itself
        assert ticket["ticket_id"] == key["ticket_id"]
        assert ticket["service_name"] == "checkout"
        assert ticket["subject"] and ticket["description"] and "{" not in ticket["description"]
        assert (key["expected_action"] is None) == (key["true_severity"] == "LOW")


def test_the_records_have_the_fields_of_their_avsc():
    avsc = Path(__file__).resolve().parents[2] / "schemas"
    for name, fields in (("support_tickets", TICKET_FIELDS), ("ticket_answer_key", ANSWER_KEY_FIELDS)):
        schema = json.loads((avsc / f"{name}.avsc").read_text())
        assert {f["name"] for f in schema["fields"]} == fields, name


def test_ids_count_up_per_source_from_the_designs_next_ones():
    tickets = [t for _, (t, _) in _sim_tickets(Simulation(2, start=0.0), 900)]
    zendesk = [t["ticket_id"] for t in tickets if t["source"] == "ZENDESK"]
    servicenow = [t["ticket_id"] for t in tickets if t["source"] == "SERVICENOW"]
    assert zendesk[:3] == ["ZD-48215", "ZD-48216", "ZD-48217"]
    assert servicenow[:3] == ["INC0041872", "INC0041873", "INC0041874"]


def test_ids_never_repeat_through_a_restart_from_the_state_file(tmp_path):
    path = state_path(tmp_path)
    first = Simulation(1, start=0.0)
    before = [t["ticket_id"] for _, (t, _) in _sim_tickets(first, 1200)]
    save_state(path, first.state_snapshot())
    # the restarted generator has the same seed, so it would draw the same sources again
    second = Simulation(1, start=5000.0, initial_state=load_state(path))
    after = [t["ticket_id"] for _, (t, _) in _sim_tickets(second, 1200)]
    assert len(before) > 40 and len(after) > 40
    assert len(set(before + after)) == len(before) + len(after)
    assert f"ZD-{48215 + sum(i.startswith('ZD-') for i in before)}" in after  # carries on


def test_live_saves_the_counters_on_every_tick_with_a_ticket(tmp_path):
    # So a generator killed between two 5-second saves never hands out an id twice.
    path = state_path(tmp_path)
    sim = Simulation(1, start=1_800_000_000.0)
    sink = DryRunSink(out=io.StringIO(), log=io.StringIO())
    for k in range(120):
        tick_once(sim, sink, sim.start + k, st_path=path)
        assert load_state(path)["next_ticket_numbers"] == sim.state_snapshot()["next_ticket_numbers"], k


def test_a_state_file_without_ticket_counters_starts_at_the_defaults():
    # A state file saved before D-068 has versions and the incident counter only.
    saved = {"services": {"payments": {"version": "2.9.4"}}, "incident_counter": 3}
    tickets = [t for _, (t, _) in _sim_tickets(Simulation(2, start=0.0, initial_state=saved), 900)]
    assert min(t["ticket_id"] for t in tickets if t["source"] == "ZENDESK") == "ZD-48215"
    assert min(t["ticket_id"] for t in tickets if t["source"] == "SERVICENOW") == "INC0041872"


def _schedule(sim, seconds):
    """Everything but the tickets: metrics, deploy records and events, tick by tick."""
    out = []
    for k in range(seconds):
        metrics, deps, events, _ = sim.tick(float(k))
        out.append((metrics, deps, [(e.kind, e.service_name, e.old_version, e.new_version) for e in events]))
    return out


def test_tickets_never_move_the_other_services(monkeypatch):
    # D-068: the ticket clock has its own rng, so tickets on, off, or many more of them leave
    # payments' incidents, web-gateway's deploys and every metric value where they were.
    normal = _schedule(Simulation(7, start=0.0), 2 * 3600)
    off = Simulation(7, start=0.0)
    off.ticket_clock.on = False
    assert _schedule(off, 2 * 3600) == normal
    monkeypatch.setattr(tickets, "RATE", 2.0)  # 30 times as many tickets
    assert _schedule(Simulation(7, start=0.0), 2 * 3600) == normal
    assert sum(len(events) for _, _, events in normal) > 100


def test_the_same_seed_gives_the_same_tickets():
    def run(seed):
        return _sim_tickets(Simulation(seed, start=0.0), 1800)

    assert run(5) == run(5)
    assert run(5) != run(6)


def test_tickets_start_on_the_first_tick_with_no_quiet_period():
    # 200 runs' first minutes hold as many tickets as any minute: about 4 each.
    first_minutes = [len(_sim_tickets(Simulation(seed, start=0.0), 60)) for seed in range(200)]
    assert sum(first_minutes) / 200 == pytest.approx(4, abs=0.4)
    assert sum(1 for seed in range(200) if Simulation(seed, start=0.0).tick(0.0)[3]) > 0


def test_no_incidents_means_no_tickets():
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True))
    assert _sim_tickets(sim, 3600) == []
