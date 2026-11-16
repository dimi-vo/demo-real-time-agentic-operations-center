"""Feed: the owner of the threads behind the wall.

No Kafka: the consumer starter is replaced by a fake that records what happens to it.
"""

from __future__ import annotations

import time

import pytest

from scripts.opswall.feed import Feed
from scripts.opswall.state import OpsWallState

# --- start and stop -----------------------------------------------------------------


def test_start_hands_the_starter_a_stop_event_and_stop_sets_it():
    stops = []
    feed = Feed(OpsWallState(), lambda stop: stops.append(stop) or [])
    feed.start()
    assert len(stops) == 1 and not stops[0].is_set()
    feed.stop()
    assert stops[0].is_set()


def test_a_failed_start_starts_no_trimmer():
    state = _TrimCounter()

    def starter(stop):
        raise RuntimeError("cannot attach")

    with pytest.raises(RuntimeError):
        Feed(state, starter, trim_interval=0.01).start()
    time.sleep(0.05)
    assert state.trims == []


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
