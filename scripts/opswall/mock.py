"""`uv run ops-wall --mock` — drive OpsWallState from a scripted incident sequence.

Twelve minutes, eight scenes, no Kafka. Exists so every rendering path the live
wall could show is visible on demand: the four actionable decisions, OBSERVE,
INVALID, a missed detection, a false alarm, and the open-right-edge "still
happening" state.

Each scene has a start time, a service, an incident (or None, for a false
alarm), the detection timing, and the decisions the agent would make. The
driver converts these into the same record dicts the consumer would apply, then
calls ``state.apply_*(...)``. On every pass the loop counter advances so
incident ids stay unique across cycles (``INC-M<loop>-<n>``).

Pre-seeded deployments give every service a version and replica count up front;
the dashboard tiles look lived-in from the first render.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from opsgen.services import SERVICES
from scripts.opswall.state import OpsWallState

LOOP_SECONDS = 720  # 12 minutes
DEGRADATION_SECONDS = 60
POST_WINDOWS_AFTER_CLOSE = 3
WINDOW_SECONDS = 10

TRIGGER_OF_TYPE = {
    "BAD_DEPLOY": "LATENCY_HIGH",
    "TRAFFIC_SURGE": "TRAFFIC_HIGH",
    "UPSTREAM_DROP": "TRAFFIC_LOW",
}
ACTION_OF_TYPE = {
    "BAD_DEPLOY": "ROLLBACK",
    "TRAFFIC_SURGE": "SCALE_OUT",
    "UPSTREAM_DROP": "INVESTIGATE_UPSTREAM",
}
AGENT_OF_TRIGGER = {
    "LATENCY_HIGH": "release_agent",
    "TRAFFIC_HIGH": "capacity_agent",
    "TRAFFIC_LOW": "capacity_agent",
}


@dataclass(frozen=True)
class Decision:
    at_offset: float   # seconds after detection_offset
    action: str
    target: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class Scene:
    start_offset: float
    service: str
    incident_type: str | None       # None = false alarm (detection without ground truth)
    detection_offset: float         # seconds after start_offset
    detection_duration: float       # seconds the detector keeps flagging
    decisions: tuple[Decision, ...] = ()
    good_version: str | None = None
    omit_detection: bool = False    # True = missed (ground truth, no detection)

    @property
    def trigger(self) -> str:
        if self.incident_type:
            return TRIGGER_OF_TYPE[self.incident_type]
        # false alarm: pretend latency HIGH on a healthy service
        return "LATENCY_HIGH"


# The eight scenes (plan §9), with substitutions where the service list differs:
#   "recommendations" → "catalog"   (catalog is the standard-tier stand-in)
#   "search"          → "web-gateway" (used here for the false-alarm scene)
SCENES: tuple[Scene, ...] = (
    # Everything starts after the 120 s warm-up so detector output and warm-up
    # rows don't overlap for the same window.
    Scene(  # ROLLBACK
        start_offset=140, service="checkout", incident_type="BAD_DEPLOY",
        detection_offset=20, detection_duration=35,
        decisions=(
            Decision(at_offset=20, action="OBSERVE"),
            Decision(at_offset=35, action="ROLLBACK", target="2.9.3"),
        ),
        good_version="2.9.3",
    ),
    Scene(  # SCALE_OUT
        start_offset=220, service="cart", incident_type="TRAFFIC_SURGE",
        detection_offset=20, detection_duration=35,
        decisions=(
            Decision(at_offset=20, action="OBSERVE"),
            Decision(at_offset=35, action="SCALE_OUT", target="6"),
        ),
    ),
    Scene(  # INVESTIGATE_UPSTREAM
        start_offset=300, service="payments", incident_type="UPSTREAM_DROP",
        detection_offset=15, detection_duration=25,
        decisions=(Decision(at_offset=25, action="INVESTIGATE_UPSTREAM"),),
    ),
    Scene(  # ESCALATE (auth is already at max replicas, can't scale)
        start_offset=380, service="auth", incident_type="TRAFFIC_SURGE",
        detection_offset=20, detection_duration=30,
        decisions=(Decision(at_offset=30, action="ESCALATE"),),
    ),
    Scene(  # resolved-without-action: only OBSERVE, span closes naturally
        start_offset=460, service="catalog", incident_type="BAD_DEPLOY",
        detection_offset=20, detection_duration=40,
        decisions=(Decision(at_offset=20, action="OBSERVE"),),
        good_version="1.8.1",
    ),
    Scene(  # false alarm (detection with no ground truth)
        start_offset=540, service="web-gateway", incident_type=None,
        detection_offset=0, detection_duration=20,
        decisions=(Decision(at_offset=10, action="OBSERVE"),),
    ),
    Scene(  # INVALID decision (agent returned junk)
        start_offset=600, service="cart", incident_type="TRAFFIC_SURGE",
        detection_offset=15, detection_duration=30,
        decisions=(Decision(at_offset=30, action="INVALID"),),
    ),
    Scene(  # missed: ground truth, no detection
        start_offset=670, service="payments", incident_type="BAD_DEPLOY",
        detection_offset=0, detection_duration=0,
        omit_detection=True, good_version="1.3.1",
    ),
)

PRESEED_VERSIONS = {
    "web-gateway": "3.4.2", "auth": "4.1.0", "catalog": "1.8.2",
    "cart": "5.0.1", "checkout": "2.9.4", "payments": "1.3.2",
}


@dataclass
class MockDriver:
    """Feeds a scripted sequence into an OpsWallState as simulated time advances."""

    state: OpsWallState
    start_wall: float                    # epoch seconds this run began
    scenes: tuple[Scene, ...] = SCENES
    loop_seconds: float = LOOP_SECONDS
    _emitted: set = field(default_factory=set)

    def seed_deployments(self) -> None:
        """Pre-seed each service so tiles have a version and replica count from tick 1."""
        for svc in SERVICES:
            self.state.apply_deployment({
                "service_name": svc.name,
                "version": PRESEED_VERSIONS.get(svc.name, svc.initial_version),
                "previous_version": None,
                "deployed_at": int((self.start_wall - 3600) * 1000),
                "last_change": "INITIAL",
                "ready_replicas": svc.ready_replicas,
                "max_replicas": svc.max_replicas,
                "tier": svc.tier,
            })

    def tick(self, now: float) -> None:
        """Emit everything that should have landed by ``now``. Idempotent — the
        state's dedup keys mean replaying ticks is harmless. Loops every
        ``loop_seconds`` with a fresh pass counter so incident ids stay unique.

        Emission order matters: the state dedups health rows by window_end, so
        whichever record is applied LAST wins. We want detection rows to override
        the surrounding normal rows, and warm-up rows to sit alone at the start
        of each loop. So: warm-up first (windows [base, base+120)), then normal
        (from base+120 on), then scene-driven detections (overwrite normal in
        their span)."""
        elapsed = max(0.0, now - self.start_wall)
        current_loop = int(elapsed // self.loop_seconds)
        for loop in (current_loop - 1, current_loop):
            if loop < 0:
                continue
            base = self.start_wall + loop * self.loop_seconds
            effective_now = min(now, base + self.loop_seconds + 60)
            self._emit_warmup_health(base, min(effective_now, base + 120))
            self._emit_normal_health(base + 120, effective_now)
            self._emit_loop(loop, base, effective_now)

    def _emit_loop(self, loop: int, base: float, now: float) -> None:
        for n, scene in enumerate(self.scenes):
            inc_id = f"INC-M{loop}-{n:03d}"
            start_at = base + scene.start_offset
            end_at = start_at + DEGRADATION_SECONDS
            det_start = start_at + scene.detection_offset
            det_end = det_start + scene.detection_duration
            if scene.incident_type and start_at <= now:
                if self._once(("inc_start", inc_id)):
                    self.state.apply_incident(_incident_start(
                        inc_id=inc_id, service=scene.service,
                        incident_type=scene.incident_type, started_at=start_at,
                        now=start_at, good_version=scene.good_version,
                    ))
                if end_at <= now and self._once(("inc_end", inc_id)):
                    self.state.apply_incident(_incident_end(
                        inc_id=inc_id, service=scene.service,
                        incident_type=scene.incident_type, started_at=start_at, ended_at=end_at,
                        now=end_at, good_version=scene.good_version,
                    ))
            if not scene.omit_detection:
                self._emit_detection_health(
                    scene=scene, loop=loop, base=base, now=now,
                    det_start=det_start, det_end=det_end,
                )
            # Decisions come from the agent inside its invocation; emit when due.
            for i, d in enumerate(scene.decisions):
                at = det_start + d.at_offset
                if at > now:
                    continue
                dec_id = ("dec", inc_id, i)
                if self._once(dec_id):
                    self.state.apply_decision(_decision_record(
                        service=scene.service, trigger=scene.trigger,
                        action=d.action, target=d.target,
                        window_end=_window_floor(det_start + d.at_offset),
                    ), kafka_ts=at)

    def _emit_detection_health(self, scene: Scene, loop: int, base: float, now: float,
                               det_start: float, det_end: float) -> None:
        """Emit anomalous health rows every 10 s between det_start and det_end, then
        a NORMAL row at det_end + 10 so the span closes if no actionable decision landed."""
        key = f"detw-{loop}-{scene.service}-{scene.start_offset}"
        # Which 10 s window boundaries fall inside the detection span?
        first = _window_ceil(det_start)
        last = _window_ceil(det_end)
        trigger = scene.trigger
        is_latency = trigger == "LATENCY_HIGH"
        direction_hi = trigger == "TRAFFIC_HIGH"
        for we in _windows_between(first, last):
            if we > now:
                return
            if self._once((key, we)):
                self.state.apply_health(_health_record(
                    service=scene.service, window_end=we,
                    latency_status="HIGH" if is_latency else "NORMAL",
                    traffic_status=("HIGH" if direction_hi
                                    else "LOW" if trigger == "TRAFFIC_LOW" else "NORMAL"),
                    request_count=120 if direction_hi else 30 if trigger == "TRAFFIC_LOW" else 60,
                    error_count=4 if is_latency else 0,
                ))
        trailer = last + WINDOW_SECONDS
        if trailer <= now and self._once((key, "trailer")):
            self.state.apply_health(_health_record(
                service=scene.service, window_end=trailer,
                latency_status="NORMAL", traffic_status="NORMAL",
                request_count=60, error_count=0,
            ))

    def _emit_normal_health(self, loop_start: float, now: float) -> None:
        """One NORMAL row per service per 10 s window, so tiles stay fresh when quiet."""
        first = _window_ceil(loop_start)
        last = _window_floor(now)
        if first > last:
            return
        for svc in SERVICES:
            for we in _windows_between(first, last, step=WINDOW_SECONDS):
                key = ("norm", svc.name, we)
                if self._once(key):
                    self.state.apply_health(_health_record(
                        service=svc.name, window_end=we,
                        latency_status="NORMAL", traffic_status="NORMAL",
                        request_count=int(svc.rps * WINDOW_SECONDS), error_count=0,
                    ))

    def _emit_warmup_health(self, loop_start: float, until: float) -> None:
        """First ~120 s of each loop emits WARMING_UP; the dashboard draws the hatched band."""
        first = _window_ceil(loop_start)
        last = _window_floor(until)
        for svc in SERVICES:
            for we in _windows_between(first, last, step=WINDOW_SECONDS):
                key = ("warm", svc.name, we)
                if self._once(key):
                    self.state.apply_health(_health_record(
                        service=svc.name, window_end=we,
                        latency_status="WARMING_UP", traffic_status="WARMING_UP",
                        request_count=int(svc.rps * WINDOW_SECONDS), error_count=0,
                    ))

    def _once(self, key) -> bool:
        if key in self._emitted:
            return False
        self._emitted.add(key)
        return True


# ---- helpers ------------------------------------------------------------------


def _window_floor(t: float) -> float:
    return (int(t) // WINDOW_SECONDS) * WINDOW_SECONDS


def _window_ceil(t: float) -> float:
    return _window_floor(t) + (WINDOW_SECONDS if int(t) % WINDOW_SECONDS else 0)


def _windows_between(first: float, last: float, step: int = WINDOW_SECONDS):
    k = first
    while k <= last:
        yield k
        k += step


def _incident_start(inc_id: str, service: str, incident_type: str,
                     started_at: float, now: float, good_version: str | None) -> dict:
    return {
        "event_time": int(now * 1000),
        "incident_id": inc_id,
        "service_name": service,
        "incident_type": incident_type,
        "started_at": int(started_at * 1000),
        "ended_at": None,
        "expected_trigger": TRIGGER_OF_TYPE[incident_type],
        "expected_action": ACTION_OF_TYPE[incident_type],
        "good_version": good_version if incident_type == "BAD_DEPLOY" else None,
    }


def _incident_end(inc_id: str, service: str, incident_type: str,
                   started_at: float, ended_at: float, now: float,
                   good_version: str | None) -> dict:
    rec = _incident_start(inc_id, service, incident_type, started_at, now, good_version)
    rec["ended_at"] = int(ended_at * 1000)
    rec["event_time"] = int(ended_at * 1000)
    return rec


def _health_record(service: str, window_end: float, latency_status: str, traffic_status: str,
                    request_count: int, error_count: int) -> dict:
    return {
        "service_name": service,
        "window_end": int(window_end * 1000),
        "latency_status": latency_status,
        "traffic_status": traffic_status,
        "request_count": request_count,
        "error_count": error_count,
    }


def _decision_record(service: str, trigger: str, action: str,
                      target: str | None, window_end: float) -> dict:
    return {
        "service_name": service,
        "window_end": int(window_end * 1000),
        "agent_name": AGENT_OF_TRIGGER[trigger],
        "trigger": trigger,
        "action": action,
        "target": target,
        "reason": None,
        "raw_response": None,
    }


def run_until_interrupt(state: OpsWallState, start_wall: float | None = None) -> None:
    """Advance the mock driver once a second until Ctrl-C."""
    import sys

    start = start_wall if start_wall is not None else time.time()
    driver = MockDriver(state=state, start_wall=start)
    driver.seed_deployments()
    try:
        while True:
            now = time.time()
            driver.tick(now)
            time.sleep(0.25)  # 4 Hz, matches the broadcast cadence
    except KeyboardInterrupt:
        sys.stderr.write("\nStopped.\n")


# For callers that want to drive it synchronously (tests, offline preview).
def advance_to(state: OpsWallState, start_wall: float, now: float) -> MockDriver:
    driver = MockDriver(state=state, start_wall=start_wall)
    driver.seed_deployments()
    driver.tick(now)
    return driver


def all_actions() -> set[str]:
    """Catalogue of action strings exercised by one loop. Used by the test."""
    return {d.action for scene in SCENES for d in scene.decisions}
