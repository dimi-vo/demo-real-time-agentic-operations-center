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

The "Refresh data" button in the page header restarts whichever feed is running (live:
every consumer re-attached, mock: the loop from its beginning) after clearing the state.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import webbrowser

import uvicorn

from scripts.common.credentials import load_card
from scripts.opswall.feed import Feed
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

    if args.mock:
        from scripts.opswall.mock import start_threads as start_mock

        def starter(stop: threading.Event) -> list[threading.Thread]:
            return start_mock(state, stop)
    else:
        path, card = load_card(args.creds)
        logger.info("Using credential card: %s", path)
        from scripts.opswall.consumer import start_threads

        def starter(stop: threading.Event) -> list[threading.Thread]:
            return start_threads(state=state, card=card, stop=stop)

    # The feed owns the threads so the "Refresh data" button can restart them (D-053).
    feed = Feed(state, starter)
    feed.start()

    url = f"http://{args.host if args.host != '0.0.0.0' else 'localhost'}:{args.port}"
    logger.info("ops-wall live at %s  (Ctrl-C to stop)%s", url, "  [MOCK FEED]" if args.mock else "")
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    app = create_app(state, feed)
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        feed.stop()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
