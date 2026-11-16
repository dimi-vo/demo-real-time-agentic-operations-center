"""The owner of the threads that feed an OpsWallState.

``uv run ops-wall`` builds one ``Feed`` around a *starter*: a function that, given a stop
event, starts the threads that write into the state and returns them. The wall's starter
(``consumer.start_threads``) runs one Kafka consumer per topic; tests pass fakes.

Next to the starter's threads, the feed runs one more: every 10 s it calls
``OpsWallState.trim()``, which drops what is more than 11 minutes old (the 10-minute chart
plus a margin). That keeps the state's size flat over a workshop of several hours (the
``service_metrics`` samples alone arrive three a second), and it is what takes an incident
left open by a simulator restart off the wall after 11 minutes (D-060). It stops with the
other threads.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from scripts.opswall.state import OpsWallState

Starter = Callable[[threading.Event], list[threading.Thread]]

TRIM_INTERVAL_SECONDS = 10.0


class Feed:
    def __init__(self, state: OpsWallState, start: Starter,
                 trim_interval: float = TRIM_INTERVAL_SECONDS) -> None:
        self._state = state
        self._starter = start
        self._trim_interval = trim_interval
        self._stop = threading.Event()

    def start(self) -> None:
        """Start the starter's threads, then the trimmer (not started if the starter fails)."""
        self._starter(self._stop)
        self._start_trimmer()

    def stop(self) -> None:
        """Signal every thread to stop (shutdown: the threads are daemons, so no join)."""
        self._stop.set()

    def _start_trimmer(self) -> None:
        def trim_until_stopped() -> None:
            while not self._stop.wait(self._trim_interval):
                self._state.trim(time.time())

        threading.Thread(target=trim_until_stopped, name="ops-wall-trim", daemon=True).start()
