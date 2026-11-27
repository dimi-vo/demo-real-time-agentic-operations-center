"""`uv run patch-agent`: serves the patch agent over A2A on http://localhost:8001.

Confluent Cloud reaches it through a tunnel the participant opens, whose URL is
``--public-url``. That URL goes first in the agent card and into lab 6.2's connection, which
this prints at start-up, ready to paste. Each call prints a block: the time, the request,
"patching… 6 s", the reply. ``--verbose`` adds every HTTP request's method, path, JSON-RPC
method and kind of credentials, for the smoke test (issue 02). Stop with Ctrl-C.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

import uvicorn
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill
from langgraph.graph.state import CompiledStateGraph
from rich.console import Console
from rich.text import Text
from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from scripts.common.paths import get_project_root
from scripts.patchagent.executor import PatchAgentExecutor
from scripts.patchagent.graph import DEFAULT_MAX_DELAY, DEFAULT_MIN_DELAY, build_graph

LAB_FILE = get_project_root() / "sql" / "6.2_patch_agent_tool.sql"
PLACEHOLDER = "<your patch-agent URL>"  # the connection's endpoint in lab 6.2
TEXT = ["text/plain"]


def agent_card(host: str, port: int, public_url: str | None) -> AgentCard:
    """The card Flink reads at /.well-known/agent-card.json: the tunnel's URL first, then localhost.

    Both interfaces say A2A 0.3 and JSON-RPC, as in the multi-agent repo's card, which worked
    with Flink; an A2A 0.3 card's main ``url`` is then the first of them. The same route
    answers A2A 1.0 (``SendMessage``) too. Which of the two Flink speaks is for issue 02.
    """
    urls = [f"http://{host}:{port}/"]
    if public_url:
        urls.insert(0, public_url.rstrip("/") + "/")
    return AgentCard(
        name="Patch Agent",
        description=(
            "Patches a vulnerable dependency in one of River Shop's repositories and answers with the commit. "
            "A demo: it pretends, takes a few seconds, and calls no model."
        ),
        version="1.0.0",
        supported_interfaces=[
            AgentInterface(url=url, protocol_binding="JSONRPC", protocol_version="0.3") for url in urls
        ],
        default_input_modes=TEXT,
        default_output_modes=TEXT,
        capabilities=AgentCapabilities(streaming=True),
        skills=[
            AgentSkill(
                id="patch_dependency",
                name="Patch a vulnerable dependency",
                description=(
                    "Send exactly one line: PATCH <repository> <cve_id> <package> <installed_version> -> "
                    "<fixed_version>. The answer names the commit, and when the patch started and finished."
                ),
                tags=["security", "cve", "patch"],
                examples=["PATCH checkout CVE-2026-41234 urllib3 1.26.4 -> 1.26.18"],
                input_modes=TEXT,
                output_modes=TEXT,
            )
        ],
    )


class CallPrinter:
    """What the terminal shows: a block per call, and with --verbose a line per HTTP request."""

    INDENT = " " * 10

    def __init__(self, console: Console) -> None:
        self.console = console

    def patching(self, request: str, delay: float) -> None:
        self.console.print(Text.assemble(f"{datetime.now():%H:%M:%S}  ", ("← ", "cyan"), (request.strip(), "bold")))
        self.console.print(Text(f"{self.INDENT}patching… {delay:.0f} s", style="yellow"))

    def replied(self, request: str, reply: str) -> None:
        self.console.print(Text.assemble(f"{datetime.now():%H:%M:%S}  ", ("→ ", "cyan"), (reply, "green")))
        self.console.print()

    def http(self, method: str, path: str, rpc_method: str | None, auth: str) -> None:
        line = f"{method} {path}" + (f"  {rpc_method}" if rpc_method else "") + f"  auth: {auth}"
        self.console.print(Text(f"{self.INDENT}{line}", style="dim"))


class RequestLog:
    """ASGI middleware for --verbose: prints each request before the app sees it.

    It reads the body to find the JSON-RPC method, then hands the app the same messages.
    Only the Authorization header's scheme is printed, never its value.
    """

    def __init__(self, app: ASGIApp, printer: CallPrinter) -> None:
        self.app = app
        self.printer = printer

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        messages: list[Message] = []
        body = b""
        while True:
            message = await receive()
            messages.append(message)
            if message["type"] != "http.request":
                break
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break
        authorization = Headers(scope=scope).get("authorization")
        auth = authorization.split(" ", 1)[0] if authorization else "none"
        self.printer.http(scope["method"], scope["path"], _rpc_method(body), auth)

        async def replay() -> Message:
            return messages.pop(0) if messages else await receive()

        await self.app(scope, replay, send)


def _rpc_method(body: bytes) -> str | None:
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    return payload.get("method") if isinstance(payload, dict) else None


def create_app(card: AgentCard, graph: CompiledStateGraph, console: Console, verbose: bool = False) -> Starlette:
    """The agent card's route and the JSON-RPC route at /, with A2A 0.3's method names kept on.

    No authentication: Flink's A2A connection must carry a username and a password, and
    nothing here checks them.
    """
    printer = CallPrinter(console)
    handler = DefaultRequestHandler(
        agent_executor=PatchAgentExecutor(graph, printer),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    routes = create_agent_card_routes(card) + create_jsonrpc_routes(handler, "/", enable_v0_3_compat=True)
    middleware = [Middleware(RequestLog, printer=printer)] if verbose else []
    return Starlette(routes=routes, middleware=middleware)


def lab_sql(public_url: str | None) -> str:
    """Lab 6.2 as written, with the endpoint filled in when there is a public URL."""
    sql = LAB_FILE.read_text()
    return sql.replace(PLACEHOLDER, public_url.rstrip("/")) if public_url else sql


def print_start(console: Console, host: str, port: int, public_url: str | None) -> None:
    """Where the agent listens, then lab 6.2 ready to paste (or a warning, without a public URL)."""
    local = f"http://{host}:{port}/"
    if public_url:
        public = public_url.rstrip("/")
        console.print(Text(f"Patch Agent, over A2A at {public}/ (and {local})", style="bold"))
        console.print(f"Agent card: {public}/.well-known/agent-card.json")
        console.print()
        console.print(f"Lab 6.2 (sql/{LAB_FILE.name}) with your URL filled in, ready to paste into a Flink workspace:")
    else:
        console.print(
            Text("Warning: no --public-url, so Confluent Cloud cannot reach this agent.", style="bold yellow")
        )
        console.print(Text(f"Patch Agent, over A2A at {local}", style="bold"))
        console.print()
        console.print(f"Lab 6.2 (sql/{LAB_FILE.name}), with its placeholder left in:")
    console.print()
    console.out(lab_sql(public_url), highlight=False, end="")
    console.print()
    if not public_url:
        console.print(
            Text(
                f"A tunnel is needed: open one to port {port}, then start again with --public-url <tunnel URL>.",
                style="yellow",
            )
        )
    console.print("Waiting for calls. Ctrl-C to stop.")
    console.print()


def _url(value: str) -> str:
    if not value.startswith(("https://", "http://")):
        raise argparse.ArgumentTypeError(f"{value!r} is not a URL: start it with https://")
    return value


def _seconds(value: str) -> float:
    seconds = float(value)
    if seconds < 0:
        raise argparse.ArgumentTypeError(f"{value} is negative")
    return seconds


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="patch-agent: the external agent of section 6, served over A2A")
    p.add_argument("--port", type=int, default=8001, help="Local port (default 8001, next to the ops wall's 8000)")
    p.add_argument("--host", default="localhost", help="Bind host (default localhost)")
    p.add_argument(
        "--public-url",
        type=_url,
        help="The tunnel's URL. It goes first in the agent card and into lab 6.2's connection",
    )
    p.add_argument("--min-delay", type=_seconds, default=DEFAULT_MIN_DELAY, help="Shortest patch in s (default 4)")
    p.add_argument("--max-delay", type=_seconds, default=DEFAULT_MAX_DELAY, help="Longest patch in s (default 10)")
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Also print every HTTP request: method, path, JSON-RPC method and kind of credentials",
    )
    args = p.parse_args(argv)
    if args.max_delay < args.min_delay:
        p.error("--max-delay is shorter than --min-delay")
    return args


def main() -> None:
    args = parse_args()
    console = Console(soft_wrap=True)  # a long reply stays one line, to copy whole
    print_start(console, args.host, args.port, args.public_url)
    card = agent_card(args.host, args.port, args.public_url)
    app = create_app(card, build_graph(args.min_delay, args.max_delay), console, verbose=args.verbose)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
