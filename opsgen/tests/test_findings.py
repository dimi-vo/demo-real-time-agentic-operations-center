"""Code security incidents (D-070): the finding templates, the scan clock and the CVE answer key."""

import io
import json
import random
import re
import string
from collections import Counter
from pathlib import Path

import pytest

from opsgen import findings
from opsgen.driver import tick_once
from opsgen.engine import Config
from opsgen.finding_templates import (
    ADVISORIES,
    ADVISORY_FIELDS,
    DISMISS_REASONS,
    POOLS,
    REPOSITORIES,
    TEMPLATES,
    fill,
)
from opsgen.findings import ScanClock
from opsgen.services import SERVICES
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink
from opsgen.state import load_state, save_state, state_path


def _placeholders(text):
    return {name for _, name, _, _ in string.Formatter().parse(text) if name is not None}


# --- templates -----------------------------------------------------------------------------


def test_the_mix_is_6_patch_6_dismiss_two_per_reason_and_8_non_critical():
    by = Counter((t.expected_action, t.dismiss_reason) for t in TEMPLATES)
    assert by == {
        ("PATCH", None): 6,
        ("DISMISS", "dev_only"): 2,
        ("DISMISS", "archived"): 2,
        ("DISMISS", "not_called"): 2,
        (None, None): 8,
    }
    assert DISMISS_REASONS == ("dev_only", "archived", "not_called")


def test_the_repositories_are_river_shops_three_services_ci_tools_and_storefront_legacy():
    assert REPOSITORIES == ("web-gateway", "checkout", "payments", "ci-tools", "storefront-legacy")


def test_patch_names_only_the_three_services_archived_only_storefront_legacy():
    for t in TEMPLATES:
        assert t.repositories and set(t.repositories) <= set(REPOSITORIES), t.sentence
        if t.expected_action == "PATCH":
            assert t.repositories == SERVICES, t.sentence
        if t.dismiss_reason == "archived":
            assert t.repositories == ("storefront-legacy",), t.sentence
        if t.dismiss_reason == "not_called":  # shipped, so in a deployed service
            assert t.repositories == SERVICES, t.sentence
        if t.expected_action is None:  # an ordinary finding, any repository
            assert t.repositories == REPOSITORIES, t.sentence


def test_every_placeholder_in_every_template_has_a_pool():
    used = set()
    for t in TEMPLATES:
        names = _placeholders(t.sentence)
        assert "" not in names, f"positional placeholder in {t.sentence!r}"
        known = {"repository", *ADVISORY_FIELDS, *POOLS}
        assert names <= known, (names - known, t.sentence)
        used |= names
    assert all(POOLS[name] for name in POOLS), "an empty pool"
    assert set(POOLS) <= used, f"pools no template uses: {set(POOLS) - used}"


def test_every_template_fills_without_leftover_placeholders():
    rng = random.Random(0)
    for t in TEMPLATES:
        for _ in range(40):
            filled = fill(t, rng)
            assert set(filled) == {"repository", "cve_id", "package", "installed_version", "fixed_version", "finding"}
            assert "{" not in filled["finding"] and "}" not in filled["finding"], filled["finding"]
            assert filled["repository"] in t.repositories
            # The record's columns are the advisory the sentence names.
            advisory = next(a for a in ADVISORIES[t.critical] if a.cve_id == filled["cve_id"])
            assert (filled["package"], filled["installed_version"], filled["fixed_version"]) == (
                advisory.package,
                advisory.installed_version,
                advisory.fixed_version,
            )
            for name in _placeholders(t.sentence) & {"repository", "package", "installed_version"}:
                assert filled[name] in filled["finding"], (name, filled["finding"])


def test_fill_draws_in_a_fixed_order_so_a_seed_gives_the_same_finding():
    for t in TEMPLATES:
        assert fill(t, random.Random(3)) == fill(t, random.Random(3))


def _version(text):
    return tuple(int(n) for n in re.findall(r"\d+", text))


def test_the_advisories_are_fictional_cves_on_real_packages_with_plausible_versions():
    # Spec decision 5: CVE-2026-4xxxx, so nobody takes a demo record for a real advisory. A CVE
    # is critical or not, never both, so the two pools share no id.
    critical, ordinary = ADVISORIES[True], ADVISORIES[False]
    ids = [a.cve_id for a in (*critical, *ordinary)]
    assert len(ids) == len(set(ids))
    assert all(re.fullmatch(r"CVE-2026-4\d{4}", i) for i in ids), ids
    assert {a.package for a in critical} >= {"urllib3", "jackson-databind", "lodash", "pyyaml", "netty-codec-http"}
    assert len(critical) >= 6 and len(ordinary) >= 6
    for a in (*critical, *ordinary):
        assert _version(a.fixed_version) > _version(a.installed_version), a
        assert a.function and a.flaw and not a.flaw.endswith("."), a


def test_the_spec_example_is_an_advisory():
    # The patch agent's example reply (spec decision 20) names this one.
    assert any(
        (a.cve_id, a.package, a.installed_version, a.fixed_version)
        == ("CVE-2026-41234", "urllib3", "1.26.4", "1.26.18")
        for a in ADVISORIES[True]
    )


def test_no_template_says_patch_or_dismiss_or_holds_an_apostrophe():
    # The answer must come from where the package sits, not from the word. No apostrophes, so
    # lab 6.3 can take its examples from the templates (an agent prompt cannot hold one).
    texts = [t.sentence for t in TEMPLATES]
    texts += [v for a in (*ADVISORIES[True], *ADVISORIES[False]) for v in (a.function, a.flaw)]
    texts += [v for pool in POOLS.values() for v in pool]
    for text in texts:
        assert not re.search(r"\b(patch|dismiss)", text, re.I), text
        assert "'" not in text and "\u2019" not in text, text


def test_a_finding_is_one_or_two_sentences():
    for t in TEMPLATES:
        sentences = re.findall(r"[.!?]+(?=\s|$)", t.sentence)
        assert 1 <= len(sentences) <= 2, t.sentence
        assert "\n" not in t.sentence and t.sentence.endswith(".")


# Spec decision 16's rules. PATCH: the package ships in a deployed service and the vulnerable
# code is used. DISMISS: dev or test only, the repository archived, or the code never called.
SHIPPED = re.compile(
    r"\b(runtime|production (image|lock file|traffic)|runs in production|deployed (\{repository\} )?service)\b", re.I
)
USED = re.compile(r"\b(calls?|call path|uses)\b", re.I)
NOT_USED = re.compile(r"\bno (call|code path)\b|\bnever called\b", re.I)
DEV = re.compile(r"\b(dev dependency|unit tests|test suite|build scripts|build tooling)\b", re.I)
ARCHIVED = re.compile(r"\barchived\b", re.I)
CUES = {"SHIPPED": SHIPPED, "USED": USED, "NOT_USED": NOT_USED, "DEV": DEV, "ARCHIVED": ARCHIVED}


def _cues(text):
    return {name for name, cue in CUES.items() if cue.search(text)}


def test_every_critical_template_says_exactly_what_decides_it():
    for t in TEMPLATES:
        cues = _cues(t.sentence)
        if t.expected_action == "PATCH":
            assert cues == {"SHIPPED", "USED"}, (cues, t.sentence)
        elif t.dismiss_reason == "not_called":
            assert {"SHIPPED", "NOT_USED"} <= cues <= {"SHIPPED", "NOT_USED", "USED"}, (cues, t.sentence)
        elif t.dismiss_reason == "dev_only":
            assert cues <= {"DEV", "USED"} and "DEV" in cues, (cues, t.sentence)
        elif t.dismiss_reason == "archived":
            assert cues == {"ARCHIVED"}, (cues, t.sentence)


def test_no_advisory_or_pool_value_carries_a_cue():
    # So filling a template never adds a second answer to it.
    for a in ADVISORIES[True]:
        for text in (a.package, a.function, a.flaw):
            assert not _cues(text), text
    for pool in POOLS.values():
        for text in pool:
            assert not _cues(text), text


# --- the scan clock -------------------------------------------------------------------------


def _run_clock(seed, seconds):
    clock = ScanClock(random.Random(seed))
    return [f for k in range(seconds) for f in clock.tick(1_800_000_000_000 + 1000 * k)]


def test_about_4_findings_a_minute_30_percent_critical_half_of_them_patch():
    hours = 20
    drawn = _run_clock(11, hours * 3600)
    n = len(drawn)
    assert n / (hours * 60) == pytest.approx(4, abs=0.2)  # Poisson(1/15) a second
    critical = [f for f in drawn if f.answer_key["expected_action"] is not None]
    assert len(critical) / n == pytest.approx(0.30, abs=0.02)
    actions = Counter(f.answer_key["expected_action"] for f in critical)
    assert set(actions) == {"PATCH", "DISMISS"}
    assert actions["PATCH"] / len(critical) == pytest.approx(0.5, abs=0.04)


def test_the_three_dismiss_reasons_come_equally_often(monkeypatch):
    # The reason is in the template, not the answer key: watch which templates the clock fills.
    used = []

    def spy(template, rng):
        used.append(template)
        return fill(template, rng)

    monkeypatch.setattr(findings, "fill", spy)
    drawn = _run_clock(12, 20 * 3600)
    assert [t.expected_action for t in used] == [f.answer_key["expected_action"] for f in drawn]
    reasons = Counter(t.dismiss_reason for t in used if t.expected_action == "DISMISS")
    assert set(reasons) == set(DISMISS_REASONS)
    dismissed = sum(reasons.values())
    assert all(c / dismissed == pytest.approx(1 / 3, abs=0.05) for c in reasons.values())


def test_critical_scores_are_9_to_10_and_the_rest_4_to_8_9_with_one_decimal():
    drawn = _run_clock(13, 6 * 3600)
    for f in drawn:
        score = f.scan["cvss_score"]
        assert isinstance(score, float) and round(score, 1) == score
        if f.answer_key["expected_action"] is None:
            assert 4.0 <= score <= 8.9, f
        else:
            assert 9.0 <= score <= 10.0, f
    critical = {f.scan["cvss_score"] for f in drawn if f.answer_key["expected_action"]}
    ordinary = {f.scan["cvss_score"] for f in drawn if not f.answer_key["expected_action"]}
    assert {9.0, 10.0} <= critical and {4.0, 8.9} <= ordinary  # both ends are drawn


def test_a_finding_takes_its_template_and_its_score_from_its_category():
    for f in _run_clock(14, 3 * 3600):
        record, key = f
        critical = key["expected_action"] is not None
        assert (record["cvss_score"] >= 9.0) == critical
        assert record["cve_id"] in {a.cve_id for a in ADVISORIES[critical]}
        if key["expected_action"] == "PATCH":
            assert record["repository"] in SERVICES


# --- in the simulation ----------------------------------------------------------------------

FINDING_FIELDS = [
    "event_time",
    "finding_id",
    "repository",
    "cve_id",
    "package",
    "installed_version",
    "fixed_version",
    "cvss_score",
    "finding",
]
ANSWER_KEY_FIELDS = ["event_time", "finding_id", "expected_action"]


def _sim_findings(sim, seconds, start=0):
    """Tick `sim` from `start` for `seconds`. Returns (tick time, finding) pairs."""
    return [(now, f) for k in range(seconds) for now in [sim.start + start + k] for f in sim.tick(now).findings]


def test_every_finding_has_exactly_one_answer_key_on_the_same_tick():
    sim = Simulation(1, start=1_800_000_000.0)
    drawn = _sim_findings(sim, 1800)
    assert len(drawn) > 60
    for now, (finding, key) in drawn:
        assert list(finding) == FINDING_FIELDS and list(key) == ANSWER_KEY_FIELDS
        assert finding["event_time"] == key["event_time"] == round(now * 1000)  # the tick itself
        assert finding["finding_id"] == key["finding_id"]
        assert (key["expected_action"] is None) == (finding["cvss_score"] < 9.0)
        assert key["expected_action"] in (None, "PATCH", "DISMISS")
    ids = [f["finding_id"] for _, (f, _) in drawn]
    assert len(ids) == len(set(ids))


def test_the_records_have_the_fields_of_their_avsc():
    avsc = Path(__file__).resolve().parents[2] / "schemas"
    for name, names in (("cve_scans", FINDING_FIELDS), ("cve_answer_key", ANSWER_KEY_FIELDS)):
        schema = json.loads((avsc / f"{name}.avsc").read_text())
        assert [f["name"] for f in schema["fields"]] == names, name


def test_ids_count_up_from_scan_000001():
    drawn = [f["finding_id"] for _, (f, _) in _sim_findings(Simulation(2, start=0.0), 900)]
    assert drawn[:3] == ["SCAN-000001", "SCAN-000002", "SCAN-000003"]
    assert drawn == [f"SCAN-{n:06d}" for n in range(1, len(drawn) + 1)]


def test_ids_keep_increasing_through_a_restart_from_the_state_file(tmp_path):
    path = state_path(tmp_path)
    first = Simulation(1, start=0.0)
    before = [f["finding_id"] for _, (f, _) in _sim_findings(first, 1200)]
    save_state(path, first.state_snapshot())
    assert load_state(path)["next_finding_number"] == len(before) + 1
    # the restarted generator has the same seed, so it would draw the same findings again
    second = Simulation(1, start=5000.0, initial_state=load_state(path))
    after = [f["finding_id"] for _, (f, _) in _sim_findings(second, 1200)]
    assert len(before) > 40 and len(after) > 40
    assert after[0] == f"SCAN-{len(before) + 1:06d}"  # carries on
    assert before + after == sorted(before + after) and len(set(before + after)) == len(before) + len(after)


def test_live_saves_the_counter_on_every_tick_with_a_finding(tmp_path):
    # So a generator killed between two 5-second saves never hands out an id twice.
    path = state_path(tmp_path)
    sim = Simulation(1, start=1_800_000_000.0, config=Config())
    sim.ticket_clock.on = False  # so only a finding can make a tick save
    sink = DryRunSink(out=io.StringIO(), log=io.StringIO())
    saved_on_a_finding = 0
    for k in range(120):
        before = sim.scan_clock.next_number
        tick_once(sim, sink, sim.start + k, st_path=path)
        assert load_state(path)["next_finding_number"] == sim.scan_clock.next_number, k
        saved_on_a_finding += sim.scan_clock.next_number != before and k % 5 != 0
    assert saved_on_a_finding > 0


def test_a_state_file_without_the_finding_counter_starts_at_scan_000001():
    # A state file saved before D-070 has versions, the incident counter and ticket numbers.
    saved = {"services": {"payments": {"version": "2.9.4"}}, "incident_counter": 3, "next_ticket_numbers": {}}
    drawn = [f for _, (f, _) in _sim_findings(Simulation(2, start=0.0, initial_state=saved), 900)]
    assert drawn[0]["finding_id"] == "SCAN-000001"


def _everything_else(sim, seconds):
    """Everything but the findings: metrics, deploy records, events and tickets, tick by tick."""
    out = []
    for k in range(seconds):
        metrics, deps, events, new_tickets, _ = sim.tick(float(k))
        events = [(e.kind, e.service_name, e.old_version, e.new_version) for e in events]
        out.append((metrics, deps, events, new_tickets))
    return out


def test_findings_never_move_the_other_services_or_the_tickets(monkeypatch):
    # D-070: the scan clock has its own rng, Random(seed + 4), so findings on, off, or many
    # more of them leave payments' incidents, web-gateway's deploys, every metric value and
    # checkout's tickets where they were.
    normal = _everything_else(Simulation(7, start=0.0), 2 * 3600)
    off = Simulation(7, start=0.0)
    off.scan_clock.on = False
    assert _everything_else(off, 2 * 3600) == normal
    monkeypatch.setattr(findings, "RATE", 2.0)  # 30 times as many findings
    assert _everything_else(Simulation(7, start=0.0), 2 * 3600) == normal
    assert sum(len(events) for _, _, events, _ in normal) > 100
    assert sum(len(t) for *_, t in normal) > 300


def test_the_scan_clock_draws_from_seed_plus_4():
    sim = Simulation(7, start=0.0)
    assert sim.scan_clock.rng.getstate() == random.Random(11).getstate()
    rngs = (sim.sched_rng, sim.traffic_rng, sim.deploy_rng, sim.ticket_rng, sim.scan_clock.rng)
    assert len({id(r) for r in rngs}) == 5


def test_the_same_seed_gives_the_same_findings():
    def run(seed):
        return _sim_findings(Simulation(seed, start=0.0), 1800)

    assert run(5) == run(5)
    assert run(5) != run(6)


def test_no_incidents_means_no_findings():
    sim = Simulation(1, start=0.0, config=Config(no_incidents=True))
    assert _sim_findings(sim, 3600) == []
    assert sim.state_snapshot()["next_finding_number"] == 1  # the counter is still saved
