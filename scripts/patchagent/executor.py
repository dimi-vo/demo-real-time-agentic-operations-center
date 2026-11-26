"""The A2A side of the patch agent: one task per call, around one run of the graph.

The task goes submitted → working ("patching… 6 s") → completed, with the graph's reply
as its ``result`` artifact, as the multi-agent repo's ``agents/app/agent_executor.py``
does (D-070). That repo used a2a-sdk 1.1.0; this follows the installed 1.3.1.
"""

from __future__ import annotations

from typing import Protocol

from a2a.helpers import new_task, new_text_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import InvalidParamsError, Part, TaskState, UnsupportedOperationError
from langgraph.graph.state import CompiledStateGraph


class CallListener(Protocol):
    """Told about each call, so the terminal can show it (``app.CallPrinter``)."""

    def patching(self, request: str, delay: float) -> None: ...

    def replied(self, request: str, reply: str) -> None: ...


class PatchAgentExecutor(AgentExecutor):
    def __init__(self, graph: CompiledStateGraph, listener: CallListener) -> None:
        self.graph = graph
        self.listener = listener

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        request = context.get_user_input()
        if not request.strip():
            raise InvalidParamsError(message="Send one line of text: the request to patch.")

        await event_queue.enqueue_event(new_task(context.task_id, context.context_id, TaskState.TASK_STATE_SUBMITTED))
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        reply = ""
        async for mode, chunk in self.graph.astream({"request": request}, stream_mode=["custom", "values"]):
            if mode == "custom":  # the node drew its delay and is about to wait
                self.listener.patching(request, chunk["delay"])
                status = new_text_message(
                    f"patching… {chunk['delay']:.0f} s", context_id=context.context_id, task_id=context.task_id
                )
                await updater.start_work(status)
            else:
                reply = chunk.get("reply", reply)

        await updater.add_artifact([Part(text=reply)], name="result")
        await updater.complete()
        self.listener.replied(request, reply)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise UnsupportedOperationError()
