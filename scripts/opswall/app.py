"""`uv run ops-wall` — local live dashboard for the Self-Healing IT workshop.

Reads the attendee's four topics with the Kafka + Schema Registry keys on their
credential card, serves a FastAPI + static-JS single-page app at
http://localhost:8000, and pushes updates over a websocket at ~4 Hz. Only
consumes — never touches Flink, never produces, costs nothing beyond the Kafka
egress. Stop with Ctrl-C.

Two feed modes:

- default: Kafka → OpsWallState (``scripts/opswall/consumer.py``). Requires a
  credential card (``credentials.env`` or ``--creds <prefix>.env``). Missing
  topics are not fatal — the wall renders with empty lanes, and ``/healthz``
  carries the reason.
- ``--mock``: no Kafka. Drives the state from a scripted 12-minute incident
  sequence (``scripts/opswall/mock.py``) so every rendering path is on screen
  on demand. Useful for dashboard development and for previewing without an
  environment.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
import webbrowser

import uvicorn

from scripts.common.credentials import load_card
from scripts.opswall.server import create_app
from scripts.opswall.state import OpsWallState

logger = logging.getLogger("ops-wall")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ops-wall — live Self-Healing IT dashboard")
    p.add_argument("--creds", help="Path to your <prefix>.env credential card (default: credentials.env)")
    p.add_argument("--port", type=int, default=8000, help="Local port (default 8000)")
    p.add_argument(
        "--host", default="127.0.0.1",
        help="Bind host (default 127.0.0.1; use 0.0.0.0 to show on a second device)",
    )
    p.add_argument("--no-browser", action="store_true", help="Do not auto-open a browser")
    p.add_argument("--mock", action="store_true", help="Scripted demo feed — no Kafka, no credential card needed")
    p.add_argument("-v", "--verbose", action="store_true", help="Also log consumer debug lines")
    return p


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    state = OpsWallState()
    stop = threading.Event()
    feeds: list[threading.Thread] = []

    if args.mock:
        from scripts.opswall.mock import MockDriver, aligned_start

        driver = MockDriver(state=state, start_wall=aligned_start(time.time()))
        driver.seed_deployments()

        def pump():
            while not stop.is_set():
                driver.tick(time.time())
                time.sleep(0.25)

        feeds.append(threading.Thread(target=pump, name="ops-wall-mock", daemon=True))
    else:
        path, card = load_card(args.creds)
        logger.info("Using credential card: %s", path)
        from scripts.opswall.consumer import start_threads

        feeds.extend(start_threads(state=state, card=card, stop=stop))

    for t in feeds:
        if not t.is_alive():
            t.start()

    url = f"http://{args.host if args.host != '0.0.0.0' else 'localhost'}:{args.port}"
    logger.info("ops-wall live at %s  (Ctrl-C to stop)%s", url, "  [MOCK FEED]" if args.mock else "")
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    app = create_app(state)
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
