"""`uv run ops-wall --mock` — drive OpsWallState from a scripted incident sequence.

Ten minutes, six scenes, no Kafka. Exists so every rendering path the live wall could
show is visible on demand: ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, OBSERVE on a
false alarm, an INVALID answer, a missed incident, and the open-right-edge "still
happening" state. The scenes follow the simulator's rules (D-042, D-043, D-047): every
incident starts on a 10-second boundary and lasts 40 s, each incident type happens only
on its candidate services, and each actionable decision is that type's one answer.

Each scene has a start time, a service, an incident (or None, for a false alarm), the
detection timing, and the decisions the agent would make. The driver converts these
into the same record dicts the consumer would apply, then calls ``state.apply_*(...)``.
A bad-deploy scene also deploys the next patch version at its start and rolls it back
at its end, as ops-sim does. On every pass the loop counter advances so incident ids
stay unique across cycles (``INC-M<loop>-<n>``).

Pre-seeded deployments give every service its catalogue version and replica count up
front; the dashboard tiles look lived-in from the first render.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from opsgen.deployments import next_patch_version
from opsgen.engine import EXPECTED_ACTION, TRIGGER, WINDOW_SECONDS
from opsgen.services import SERVICES, SERVICES_BY_NAME
from scripts.opswall.state import OpsWallState

LOOP_SECONDS = 600  # 10 minutes
DEGRADATION_SECONDS = 40  # D-042: every incident lasts 40 s, i.e. 4 whole windows
WARMUP_SECONDS = 120
# The agent's first decision lands after the first window closes: 10 s for the window,
# 5 s of watermark, a few seconds for the model call.
DECISION_DELAY = 18

AGENT_OF_TRIGGER = {
    "LATENCY_HIGH": "release_agent",
    "TRAFFIC_HIGH": "capacity_agent",
    "TRAFFIC_LOW": "capacity_agent",
}


@dataclass(frozen=True)
class Decision:
    at_offset: float   # seconds after the scene starts
    action: str
    target: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class Scene:
    start_offset: float
    service: str
    incident_type: str | None       # None = false alarm (detection without ground truth)
    detected_windows: int = 4       # anomalous windows flagged from the start; 0 = missed
    decisions: tuple[Decision, ...] = ()

    @property
    def trigger(self) -> str:
        if self.incident_type:
            return TRIGGER[self.incident_type]
        # false alarm: pretend latency HIGH on a healthy service
        return "LATENCY_HIGH"


def _first_decision(action: str, target: str | None = None) -> tuple[Decision, ...]:
    return (Decision(at_offset=DECISION_DELAY, action=action, target=target),)


# Six scenes, 80 s apart after the warm-up, each on a 10-second boundary (D-047).
SCENES: tuple[Scene, ...] = (
    Scene(  # ROLLBACK to the version before the bad deploy
        start_offset=140, service="checkout", incident_type="BAD_DEPLOY",
        decisions=_first_decision("ROLLBACK", SERVICES_BY_NAME["checkout"].initial_version),
    ),
    Scene(  # SCALE_OUT: 3 times the traffic on 3 replicas -> 9 (the uncapped SQL formula)
        start_offset=220, service="catalog", incident_type="TRAFFIC_SURGE",
        decisions=_first_decision("SCALE_OUT", str(SERVICES_BY_NAME["catalog"].ready_replicas * 3)),
    ),
    Scene(  # INVESTIGATE_UPSTREAM
        start_offset=300, service="payments", incident_type="UPSTREAM_DROP",
        decisions=_first_decision("INVESTIGATE_UPSTREAM"),
    ),
    Scene(  # false alarm (detection with no ground truth): OBSERVE is the right answer
        start_offset=380, service="web-gateway", incident_type=None, detected_windows=2,
        decisions=_first_decision("OBSERVE"),
    ),
    Scene(  # INVALID decision (agent returned junk); the span stays open until it ends
        start_offset=460, service="cart", incident_type="UPSTREAM_DROP",
        decisions=_first_decision("INVALID"),
    ),
    Scene(  # missed: ground truth, no detection
        start_offset=540, service="payments", incident_type="BAD_DEPLOY", detected_windows=0,
    ),
)


def aligned_start(now: float) -> float:
    """The 10-second boundary at or before `now`: scene offsets are multiples of 10, so
    every incident then starts and ends on a window edge, as in ops-sim."""
    return float(_window_floor(now))


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
            self.state.apply_deployment(_deployment_record(
                svc.name, svc.initial_version, None, self.start_wall - 3600, "INITIAL",
            ))

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
            self._emit_warmup_health(base, min(effective_now, base + WARMUP_SECONDS))
            self._emit_normal_health(base + WARMUP_SECONDS, effective_now)
            self._emit_loop(loop, base, effective_now)

    def _emit_loop(self, loop: int, base: float, now: float) -> None:
        for n, scene in enumerate(self.scenes):
            inc_id = f"INC-M{loop}-{n:03d}"
            start_at = base + scene.start_offset
            end_at = start_at + DEGRADATION_SECONDS
            if scene.incident_type and start_at <= now:
                if self._once(("inc_start", inc_id)):
                    if scene.incident_type == "BAD_DEPLOY":
                        self._deploy(scene.service, start_at)
                    self.state.apply_incident(_incident_start(
                        inc_id=inc_id, service=scene.service,
                        incident_type=scene.incident_type, started_at=start_at,
                        now=start_at,
                    ))
                if end_at <= now and self._once(("inc_end", inc_id)):
                    if scene.incident_type == "BAD_DEPLOY":
                        self._rollback(scene.service, end_at)
                    self.state.apply_incident(_incident_end(
                        inc_id=inc_id, service=scene.service,
                        incident_type=scene.incident_type, started_at=start_at, ended_at=end_at,
                        now=end_at,
                    ))
            self._emit_detection_health(scene=scene, loop=loop, start_at=start_at, now=now)
            # Decisions come from the agent inside its invocation; emit when due.
            for i, d in enumerate(scene.decisions):
                at = start_at + d.at_offset
                if at > now:
                    continue
                dec_id = ("dec", inc_id, i)
                if self._once(dec_id):
                    self.state.apply_decision(_decision_record(
                        service=scene.service, trigger=scene.trigger,
                        action=d.action, target=d.target,
                        window_end=_window_floor(at),
                    ), kafka_ts=at)

    def _deploy(self, service: str, now: float) -> None:
        """A bad deploy: the next patch version goes live, as in ops-sim."""
        good = SERVICES_BY_NAME[service].initial_version
        self.state.apply_deployment(_deployment_record(service, next_patch_version(good), good, now, "DEPLOY"))

    def _rollback(self, service: str, now: float) -> None:
        """The end of a bad deploy: back to the catalogue version, as in ops-sim."""
        good = SERVICES_BY_NAME[service].initial_version
        self.state.apply_deployment(_deployment_record(service, good, next_patch_version(good), now, "ROLLBACK"))

    def _emit_detection_health(self, scene: Scene, loop: int, start_at: float, now: float) -> None:
        """Emit the scene's anomalous windows (the first `detected_windows` windows from
        its start), then a NORMAL row after them so the span closes if no actionable
        decision landed."""
        if not scene.detected_windows:
            return
        key = f"detw-{loop}-{scene.service}-{scene.start_offset}"
        trigger = scene.trigger
        is_latency = trigger == "LATENCY_HIGH"
        direction_hi = trigger == "TRAFFIC_HIGH"
        last = start_at + scene.detected_windows * WINDOW_SECONDS
        for we in _windows_between(start_at + WINDOW_SECONDS, last):
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


def _deployment_record(service: str, version: str, previous: str | None, deployed_at: float,
                       last_change: str) -> dict:
    svc = SERVICES_BY_NAME[service]
    return {
        "service_name": service,
        "version": version,
        "previous_version": previous,
        "deployed_at": int(deployed_at * 1000),
        "last_change": last_change,
        "ready_replicas": svc.ready_replicas,
        "tier": svc.tier,
    }


def _incident_start(inc_id: str, service: str, incident_type: str,
                     started_at: float, now: float) -> dict:
    return {
        "event_time": int(now * 1000),
        "incident_id": inc_id,
        "service_name": service,
        "incident_type": incident_type,
        "started_at": int(started_at * 1000),
        "ended_at": None,
        "expected_trigger": TRIGGER[incident_type],
        "expected_action": EXPECTED_ACTION[incident_type],
    }


def _incident_end(inc_id: str, service: str, incident_type: str,
                   started_at: float, ended_at: float, now: float) -> dict:
    rec = _incident_start(inc_id, service, incident_type, started_at, now)
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


def start_threads(
    state: OpsWallState,
    stop: threading.Event,
    clock: Callable[[], float] = time.time,
    interval: float = 0.25,
) -> list[threading.Thread]:
    """Start the scripted loop and return its thread: a fresh driver on a fresh aligned start,
    deployments pre-seeded, ticked every ``interval`` seconds (4 Hz, matching the broadcast
    cadence) until ``stop`` is set. Calling it again after ``state.clear()`` replays the loop
    from its beginning, which is what "Refresh data" does in mock mode (D-053)."""
    driver = MockDriver(state=state, start_wall=aligned_start(clock()))
    driver.seed_deployments()

    def pump() -> None:
        while not stop.is_set():
            driver.tick(clock())
            stop.wait(interval)

    t = threading.Thread(target=pump, name="ops-wall-mock", daemon=True)
    t.start()
    return [t]


# For callers that want to drive it synchronously (tests, offline preview).
def advance_to(state: OpsWallState, start_wall: float, now: float) -> MockDriver:
    driver = MockDriver(state=state, start_wall=start_wall)
    driver.seed_deployments()
    driver.tick(now)
    return driver


def all_actions() -> set[str]:
    """Catalogue of action strings exercised by one loop. Used by the test."""
    return {d.action for scene in SCENES for d in scene.decisions}
