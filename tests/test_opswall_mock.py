"""The mock loop exercises every rendering path at least once per cycle."""

from __future__ import annotations

from opsgen.engine import CANDIDATES, EXPECTED_ACTION
from scripts.opswall.mock import (
    DEGRADATION_SECONDS,
    LOOP_SECONDS,
    SCENES,
    WARMUP_SECONDS,
    advance_to,
    aligned_start,
    all_actions,
)
from scripts.opswall.state import OpsWallState

START = 1_800_000_000.0  # fixed wall-clock epoch so snapshots are deterministic


def _run_full_loop():
    """Drive one 10-minute loop at `now = start + LOOP_SECONDS + 30 s`
    (tail buffer so the last scene's end record and decision land)."""
    st = OpsWallState()
    advance_to(st, start_wall=START, now=START + LOOP_SECONDS + 30)
    return st.snapshot(now=START + LOOP_SECONDS + 30)


def test_catalogue_covers_every_rendering_path():
    assert all_actions() == {"ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM", "OBSERVE", "INVALID"}


def test_observe_appears_only_on_the_false_alarm():
    # D-047: OBSERVE is the right answer to a false alarm, never to a real incident.
    observing = [sc for sc in SCENES if any(d.action == "OBSERVE" for d in sc.decisions)]
    assert [sc.incident_type for sc in observing] == [None]


def test_every_scene_follows_the_simulator_rules():
    """D-043: each type happens only on its candidate services, and each actionable
    decision is that type's one answer."""
    for sc in SCENES:
        if sc.incident_type is None:
            continue
        assert sc.service in CANDIDATES[sc.incident_type], sc
        for d in sc.decisions:
            if d.action not in ("OBSERVE", "INVALID"):
                assert d.action == EXPECTED_ACTION[sc.incident_type], sc


def test_every_incident_is_40_s_on_a_window_boundary_inside_the_loop():
    assert DEGRADATION_SECONDS == 40
    assert LOOP_SECONDS == 600
    for sc in SCENES:
        assert sc.start_offset % 10 == 0, sc
        assert sc.start_offset >= WARMUP_SECONDS, sc
        # the incident and its trailing NORMAL window both finish inside the loop
        assert sc.start_offset + DEGRADATION_SECONDS + 10 < LOOP_SECONDS, sc
    assert aligned_start(1_800_000_007.9) == 1_800_000_000.0


def test_a_bad_deploy_scene_changes_the_version_and_rolls_it_back():
    checkout = next(sc for sc in SCENES if sc.service == "checkout")
    st = OpsWallState()

    def tile(at):
        advance_to(st, start_wall=START, now=START + at)
        return next(s for s in st.snapshot(now=START + at)["services"] if s["name"] == "checkout")

    assert tile(checkout.start_offset - 1)["version"] == "4.2.1"
    during = tile(checkout.start_offset + 5)
    assert (during["version"], during["previousVersion"], during["lastChange"]) == ("4.2.2", "4.2.1", "DEPLOY")
    after = tile(checkout.start_offset + DEGRADATION_SECONDS + 1)
    assert (after["version"], after["lastChange"]) == ("4.2.1", "ROLLBACK")


def test_every_actionable_decision_appears_in_one_loop():
    snap = _run_full_loop()
    seen = {d["action"] for s in snap["services"] for d in s["decisions"]}
    assert {"ROLLBACK", "SCALE_OUT", "INVESTIGATE_UPSTREAM"} <= seen


def test_every_incident_has_a_ground_truth_span():
    snap = _run_full_loop()
    seen_services = {gt["incidentId"][:12]: s["name"]
                     for s in snap["services"] for gt in s["groundTruth"]}
    expected = {f"INC-M0-{i:03d}" for i, scene in enumerate(SCENES) if scene.incident_type}
    assert expected <= set(seen_services)


def test_missed_scene_has_truth_but_no_detection():
    """Scene 6 (payments BAD_DEPLOY) has no detected windows."""
    snap = _run_full_loop()
    payments = next(s for s in snap["services"] if s["name"] == "payments")
    # payments has an UPSTREAM_DROP earlier in the loop plus the missed BAD_DEPLOY,
    # so only check that at least one ground-truth span has no matching detection.
    detections = payments["detectedLatency"]
    bad_deploy_truths = [gt for gt in payments["groundTruth"] if gt["type"] == "BAD_DEPLOY"]
    assert bad_deploy_truths, "scene 6 should be present"
    overlapping = [gt for gt in bad_deploy_truths
                   if any(d["start"] <= gt["end"] and d["end"] >= gt["start"] for d in detections)]
    assert len(overlapping) < len(bad_deploy_truths), "at least one BAD_DEPLOY should be missed"


def test_false_alarm_has_detection_but_no_truth():
    """Scene 4 (web-gateway) has no ground truth; the detection stands alone."""
    snap = _run_full_loop()
    web = next(s for s in snap["services"] if s["name"] == "web-gateway")
    assert web["detectedLatency"], "false alarm should produce a detection"
    assert not any(gt for gt in web["groundTruth"]), "and no ground-truth incident"


def test_rollback_closes_the_checkout_span_at_the_decision():
    snap = _run_full_loop()
    checkout = next(s for s in snap["services"] if s["name"] == "checkout")
    rollback = next((d for d in checkout["decisions"] if d["action"] == "ROLLBACK"), None)
    # the target is checkout's real version before the bad deploy (D-047)
    assert rollback and rollback["target"] == "4.2.1"
    # The latency span's end should be close to the rollback's `at`.
    span = checkout["detectedLatency"][0]
    assert abs(span["end"] - rollback["at"]) < 5


def test_tiles_are_preseeded_from_tick_zero():
    st = OpsWallState()
    advance_to(st, start_wall=START, now=START + 1)
    snap = st.snapshot(now=START + 1)
    versions = {s["name"]: s["version"] for s in snap["services"]}
    assert versions["checkout"] is not None
    assert versions["auth"] is not None


def test_driver_is_idempotent():
    """Replaying the same ticks should not duplicate anything."""
    st = OpsWallState()
    advance_to(st, start_wall=START, now=START + 300)
    first = st.snapshot(now=START + 300)
    advance_to(st, start_wall=START, now=START + 300)
    advance_to(st, start_wall=START, now=START + 300)
    second = st.snapshot(now=START + 300)
    # Same number of ground-truth spans, same number of decisions.
    def counts(snap):
        return (
            sum(len(s["groundTruth"]) for s in snap["services"]),
            sum(len(s["decisions"]) for s in snap["services"]),
            sum(len(s["detectedLatency"]) + len(s["detectedTraffic"]) for s in snap["services"]),
        )
    assert counts(first) == counts(second)


def test_warmup_rows_land_in_the_first_two_minutes():
    """A snapshot taken 90 s after start should show WARMING_UP status."""
    st = OpsWallState()
    advance_to(st, start_wall=START, now=START + 90)
    snap = st.snapshot(now=START + 90)
    warming = [s for s in snap["services"] if s["status"] == "warming"]
    assert len(warming) == len(snap["services"])
