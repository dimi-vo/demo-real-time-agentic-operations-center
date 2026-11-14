"""Feed: the restartable owner of the threads behind the wall (D-053).

No Kafka: the live starter is replaced by a fake that records what happens to it, and the
mock starter runs for real against a fake clock.
"""

from __future__ import annotations

import threading
import time

import pytest

from opsgen.services import SERVICES_BY_NAME
from scripts.opswall import mock
from scripts.opswall.feed import Feed
from scripts.opswall.state import OpsWallState


class _FakeThread:
    """A started thread as the Feed sees it: it can be joined and asked if it is alive."""

    def __init__(self, name: str, events: list, stays_alive: bool = False) -> None:
        self.name = name
        self._events = events
        self._stays_alive = stays_alive

    def join(self, timeout=None) -> None:
        self._events.append(("joined", self.name))

    def is_alive(self) -> bool:
        return self._stays_alive


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not reached in time")


# --- restart ordering -----------------------------------------------------------


def test_restart_stops_and_joins_the_old_threads_clears_the_state_then_starts_a_new_set():
    events: list = []
    stops: list[threading.Event] = []
    state = OpsWallState()

    def starter(stop):
        n = len(stops)
        stops.append(stop)
        events.append(("start", n, state.generation))
        return [_FakeThread(f"consumer-{n}", events)]

    feed = Feed(state, starter)
    feed.start()
    state.apply_deployment({"service_name": "checkout", "version": "9.9.9"})
    feed.restart()

    assert stops[0].is_set() and not stops[1].is_set()
    # old threads are gone before the state is cleared (generation 0 -> 1) and before the new start
    assert events == [("start", 0, 0), ("joined", "consumer-0"), ("start", 1, 1)]
    checkout = next(s for s in state.snapshot()["services"] if s["name"] == "checkout")
    assert checkout["version"] is None


def test_the_first_start_leaves_the_state_alone():
    state = OpsWallState()
    Feed(state, lambda stop: []).start()
    assert state.generation == 0


def test_a_failed_start_leaves_no_threads_and_the_next_restart_works():
    state = OpsWallState()
    events: list = []
    attempts = []

    def starter(stop):
        attempts.append(stop)
        if len(attempts) == 2:
            raise RuntimeError("cannot attach")
        return [_FakeThread(f"consumer-{len(attempts)}", events)]

    feed = Feed(state, starter)
    feed.start()
    with pytest.raises(RuntimeError):
        feed.restart()
    feed.restart()  # the lock was released and nothing stale is joined twice

    assert len(attempts) == 3 and not attempts[2].is_set()
    assert events == [("joined", "consumer-1")]


def test_a_thread_that_will_not_stop_does_not_block_the_restart():
    events: list = []
    starts = []

    def starter(stop):
        starts.append(stop)
        return [_FakeThread("stuck", events, stays_alive=True)]

    feed = Feed(OpsWallState(), starter, join_timeout=0.01)
    feed.start()
    feed.restart()
    assert len(starts) == 2


def test_stop_signals_the_running_threads():
    stops = []
    feed = Feed(OpsWallState(), lambda stop: stops.append(stop) or [])
    feed.start()
    feed.stop()
    assert stops[0].is_set()


# --- the mock feed ---------------------------------------------------------------


def test_the_mock_feed_starts_over_after_a_refresh_with_its_first_scene_at_the_same_offset():
    first = SERVICES_BY_NAME["checkout"]
    t0 = 1_800_000_003.0   # 3 s past a 10 s boundary: the loop starts on the boundary before it
    clock = {"t": t0}
    state = OpsWallState()
    threads: list[threading.Thread] = []

    def starter(stop):
        started = mock.start_threads(state, stop, clock=lambda: clock["t"], interval=0.001)
        threads.extend(started)
        return started

    def checkout(now):
        return next(s for s in state.snapshot(now=now)["services"] if s["name"] == "checkout")

    feed = Feed(state, starter)
    feed.start()
    try:
        # run past the end of loop 0 into loop 1: incidents of two passes are on the wall
        clock["t"] = t0 + mock.LOOP_SECONDS + 160
        _wait_until(lambda: len(checkout(clock["t"])["groundTruth"]) == 2)

        # refresh later, on a different 10 s boundary
        t1 = t0 + 5000.0
        clock["t"] = t1
        generation = state.generation
        feed.restart()
        assert state.generation == generation + 1
        assert not threads[0].is_alive()                       # the old loop is gone
        assert checkout(t1)["groundTruth"] == []               # nothing of the old run survives
        assert checkout(t1)["version"] == first.initial_version  # tiles are pre-seeded again

        # play the new loop up to just past its first scene's start
        base = float(int(t1) // 10 * 10)
        clock["t"] = base + 160
        _wait_until(lambda: checkout(clock["t"])["groundTruth"] != [])
        incidents = checkout(clock["t"])["groundTruth"]
        assert [i["incidentId"] for i in incidents] == ["INC-M0-000"]  # the loop counter starts over
        assert incidents[0]["start"] == pytest.approx((base + mock.SCENES[0].start_offset) - (clock["t"] - 600))
    finally:
        feed.stop()
        for t in threads:
            t.join(2.0)
