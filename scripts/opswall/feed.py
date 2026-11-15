"""The restartable owner of the threads that feed an OpsWallState.

``uv run ops-wall`` builds one ``Feed`` around a *starter*: a function that, given a stop
event, starts the threads that write into the state and returns them. The wall's starter
(``consumer.start_threads``) runs one Kafka consumer per topic; tests pass fakes. The
server's ``POST /refresh`` calls ``Feed.restart()``: stop the running threads, wait for
them, clear the state, start a new set (D-053). A restart re-attaches every consumer, so a
topic that was deleted and recreated under a running wall is read again from its usual
starting point.

Next to the starter's threads, the feed runs one more: every 10 s it calls
``OpsWallState.trim()``, which drops what is more than 11 minutes old (the 10-minute chart
plus a margin). That keeps the state's size flat over a workshop of several hours (the
``service_metrics`` samples alone arrive three a second), and it is what takes an incident
left open by a simulator restart off the wall after 11 minutes (D-060). It stops and
restarts with the other threads.

Restarts are serialised by one lock, so two overlapping clicks can never leave two sets of
consumers running.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from scripts.opswall.state import OpsWallState

logger = logging.getLogger("ops-wall.feed")

Starter = Callable[[threading.Event], list[threading.Thread]]

# A consumer thread notices its stop event within one poll (0.5 s); the slow case is a thread
# blocked in a broker metadata call (up to 10 s) when the stop arrives.
JOIN_TIMEOUT_SECONDS = 15.0
TRIM_INTERVAL_SECONDS = 10.0


class Feed:
    def __init__(self, state: OpsWallState, start: Starter, join_timeout: float = JOIN_TIMEOUT_SECONDS,
                 trim_interval: float = TRIM_INTERVAL_SECONDS) -> None:
        self._state = state
        self._starter = start
        self._join_timeout = join_timeout
        self._trim_interval = trim_interval
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start the first set of threads. The state is new, so nothing is cleared."""
        with self._lock:
            self._start_threads()

    def restart(self) -> None:
        """Stop the running threads, clear the state, start a new set. Blocks until the new
        set is started; concurrent calls run one after the other."""
        with self._lock:
            self._stop_and_join()
            self._state.clear()
            self._start_threads()

    def stop(self) -> None:
        """Signal every thread to stop (shutdown: the threads are daemons, so no join)."""
        with self._lock:
            self._stop.set()

    def _start_threads(self) -> None:
        """The starter's threads, then the trimmer (not started if the starter fails)."""
        self._stop = threading.Event()
        self._threads = [*self._starter(self._stop), self._start_trimmer(self._stop)]

    def _start_trimmer(self, stop: threading.Event) -> threading.Thread:
        def trim_until_stopped() -> None:
            while not stop.wait(self._trim_interval):
                self._state.trim(time.time())

        t = threading.Thread(target=trim_until_stopped, name="ops-wall-trim", daemon=True)
        t.start()
        return t

    def _stop_and_join(self) -> None:
        self._stop.set()
        deadline = time.monotonic() + self._join_timeout
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
            if t.is_alive():
                logger.warning("%s did not stop within %.0f s; continuing", t.name, self._join_timeout)
        self._threads = []
