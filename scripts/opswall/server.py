"""FastAPI app for the ops-wall dashboard.

Serves the static frontend and pushes OpsWallState.snapshot() over a websocket
at ~4 Hz. The HTML is served with no-cache so a browser never holds a stale JS
across runs. A /healthz endpoint summarises the consumer connection for the
"empty dashboard, no reason given" case. ``POST /refresh`` (the page's "Refresh data"
button) restarts the feed behind the state: it clears the wall and re-reads the topics.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from scripts.opswall.feed import Feed
from scripts.opswall.state import OpsWallState

logger = logging.getLogger("ops-wall.server")

STATIC_DIR = Path(__file__).parent / "static"
PUSH_INTERVAL_SEC = 0.25  # 4 Hz, matches F1 pit wall
NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate"}


class NoCacheStaticFiles(StaticFiles):
    """Serve assets with no-store. Workshop attendees reopen the same URL across
    runs; a stale opswall.js is confusing in a way the cost of re-sending 30 KB
    of JS isn't."""

    def is_not_modified(self, response_headers, request_headers) -> bool:
        return False

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        response.headers.update(NO_CACHE)
        return response


def create_app(state: OpsWallState, feed: Feed | None = None) -> FastAPI:
    app = FastAPI(title="Ops wall")
    app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def index() -> HTMLResponse:
        return HTMLResponse((STATIC_DIR / "index.html").read_text(), headers=NO_CACHE)

    @app.get("/healthz")
    async def healthz() -> dict:
        snap = state.snapshot()
        return {
            "status": "ok",
            "updatedAt": snap["updatedAt"],
            "connectionErrors": snap["connectionErrors"],
            "serviceCount": len(snap["services"]),
        }

    @app.post("/refresh")
    def refresh() -> dict:
        """Clear the wall and re-read the topics. A POST because it changes state (D-053).
        A plain `def`, so FastAPI runs it in a worker thread: stopping and joining the old
        consumers must not block the websocket loop. Returns the new generation; the page
        waits for a snapshot that carries it."""
        if feed is None:
            raise HTTPException(status_code=503, detail="No feed is attached to this wall")
        feed.restart()
        return {"generation": state.generation}

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        await socket.accept()
        try:
            while True:
                # default=str: Avro decoders can return datetimes/Decimals the
                # stdlib json encoder refuses; stringifying is right for a UI
                # that only reads the value as a label.
                payload = json.dumps(state.snapshot(), separators=(",", ":"), default=str)
                await socket.send_text(payload)
                await asyncio.sleep(PUSH_INTERVAL_SEC)
        except WebSocketDisconnect:
            pass
        except (RuntimeError, ConnectionError):
            with contextlib.suppress(Exception):
                await socket.close()

    return app
