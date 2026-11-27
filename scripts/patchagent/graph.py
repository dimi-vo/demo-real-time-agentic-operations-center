"""The patch agent's LangGraph graph: one node, ``patch``, and no model.

The node pretends to patch: it waits a random few seconds, then answers with the reply of
``reply.py``. Before it waits, it streams the delay it drew (LangGraph's "custom" stream),
so the A2A side can report "patching… 6 s" while the patch runs.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import StreamWriter

from scripts.patchagent.reply import format_reply

DEFAULT_MIN_DELAY = 4.0  # seconds
DEFAULT_MAX_DELAY = 10.0


class PatchState(TypedDict, total=False):
    request: str  # the text the security agent sent
    reply: str  # what goes back to it


def build_graph(min_delay: float = DEFAULT_MIN_DELAY, max_delay: float = DEFAULT_MAX_DELAY) -> CompiledStateGraph:
    async def patch(state: PatchState, writer: StreamWriter) -> PatchState:
        delay = random.uniform(min_delay, max_delay)
        writer({"delay": delay})
        started = datetime.now(timezone.utc)
        await asyncio.sleep(delay)
        return {"reply": format_reply(state["request"], started, datetime.now(timezone.utc))}

    # A node that calls a model (to read the request, or to write the fix) can replace this
    # one later. The A2A side only reads `request` in and `reply` out, so it would not change.
    graph = StateGraph(PatchState)
    graph.add_node("patch", patch)
    graph.add_edge(START, "patch")
    graph.add_edge("patch", END)
    return graph.compile()
