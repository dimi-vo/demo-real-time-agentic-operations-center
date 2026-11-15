"""Feed: the restartable owner of the threads behind the wall (D-053).

No Kafka: the consumer starter is replaced by a fake that records what happens to it.
"""

from __future__ import annotations

import threading
import time

import pytest

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


# --- trimming --------------------------------------------------------------------


class _TrimCounter(OpsWallState):
    def __init__(self) -> None:
        super().__init__()
        self.trims: list[float] = []

    def trim(self, now: float) -> None:
        self.trims.append(now)


def _wait_for(cond, timeout=2.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond() and time.monotonic() < deadline:
        time.sleep(0.005)


def test_the_feed_trims_the_state_on_the_wall_clock_until_stopped():
    state = _TrimCounter()
    feed = Feed(state, lambda stop: [], trim_interval=0.01)
    before = time.time()
    feed.start()
    _wait_for(lambda: len(state.trims) >= 3)
    feed.stop()
    assert len(state.trims) >= 3
    assert all(before <= t <= time.time() for t in state.trims)
    time.sleep(0.05)
    stopped_at = len(state.trims)
    time.sleep(0.05)  # five more intervals
    assert len(state.trims) == stopped_at


def test_a_restart_restarts_the_trimming():
    state = _TrimCounter()
    feed = Feed(state, lambda stop: [], trim_interval=0.01)
    feed.start()
    feed.restart()
    seen = len(state.trims)
    _wait_for(lambda: len(state.trims) > seen)
    feed.stop()
    assert len(state.trims) > seen
