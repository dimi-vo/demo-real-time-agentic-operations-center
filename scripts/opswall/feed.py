"""The restartable owner of the threads that feed an OpsWallState.

``uv run ops-wall`` builds one ``Feed`` around a *starter*: a function that, given a stop
event, starts the threads that write into the state and returns them. The wall's starter
(``consumer.start_threads``) runs one Kafka consumer per topic; tests pass fakes. The
server's ``POST /refresh`` calls ``Feed.restart()``: stop the running threads, wait for
them, clear the state, start a new set (D-053). A restart re-attaches every consumer, so a
topic that was deleted and recreated under a running wall is read again from its usual
starting point.

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


class Feed:
    def __init__(self, state: OpsWallState, start: Starter, join_timeout: float = JOIN_TIMEOUT_SECONDS) -> None:
        self._state = state
        self._starter = start
        self._join_timeout = join_timeout
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start the first set of threads. The state is new, so nothing is cleared."""
        with self._lock:
            self._stop = threading.Event()
            self._threads = self._starter(self._stop)

    def restart(self) -> None:
        """Stop the running threads, clear the state, start a new set. Blocks until the new
        set is started; concurrent calls run one after the other."""
        with self._lock:
            self._stop_and_join()
            self._state.clear()
            self._stop = threading.Event()
            self._threads = self._starter(self._stop)

    def stop(self) -> None:
        """Signal every thread to stop (shutdown: the threads are daemons, so no join)."""
        with self._lock:
            self._stop.set()

    def _stop_and_join(self) -> None:
        self._stop.set()
        deadline = time.monotonic() + self._join_timeout
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
            if t.is_alive():
                logger.warning("%s did not stop within %.0f s; continuing", t.name, self._join_timeout)
        self._threads = []
