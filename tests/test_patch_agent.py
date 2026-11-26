"""`uv run patch-agent`, the external agent of section 6 (D-070), and lab 6.2's SQL.

The patch agent runs on the participant's laptop. Confluent Cloud calls it over A2A through
lab 6.2's connection and tool, and the security agent (lab 6.3) copies its reply into a
`Patch:` line that lab 6.4 parses. So the reply's format is one shared definition
(`scripts/patchagent/reply.py`): these tests pin it, and issue 07 tests lab 6.4's patterns
against the same regexes. The server is tested in-process with Starlette's test client and
the delays at 0, on both JSON-RPC routes (A2A 1.0's `SendMessage` and 0.3's `message/send`),
since which one Flink speaks is for the smoke test (issue 02) to find out. Offline: no
tunnel, no Confluent Cloud.
"""

from __future__ import annotations

import asyncio
import base64
import io
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from rich.console import Console
from starlette.testclient import TestClient

from scripts.patchagent.app import PLACEHOLDER, agent_card, create_app, lab_sql, parse_args, print_start
from scripts.patchagent.graph import DEFAULT_MAX_DELAY, DEFAULT_MIN_DELAY, build_graph
from scripts.patchagent.reply import (
    REPLY_COMMIT,
    REPLY_FINISHED,
    REPLY_STARTED,
    REQUEST,
    commit_for,
    format_reply,
)

LAB_62 = Path(__file__).resolve().parents[1] / "sql" / "6.2_patch_agent_tool.sql"
LINE = "PATCH checkout CVE-2026-41234 urllib3 1.26.4 -> 1.26.18"
PUBLIC_URL = "https://patch-demo.example.dev"
STARTED = datetime(2026, 12, 8, 11, 42, 3, 120_000, tzinfo=timezone.utc)
FINISHED = datetime(2026, 12, 8, 11, 42, 9, 870_000, tzinfo=timezone.utc)


def _utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


# --- the request line ---------------------------------------------------------------


def test_request_pulls_out_the_five_fields():
    m = REQUEST.fullmatch(LINE)
    assert m is not None
    assert m.groupdict() == {
        "repository": "checkout",
        "cve_id": "CVE-2026-41234",
        "package": "urllib3",
        "installed_version": "1.26.4",
        "fixed_version": "1.26.18",
    }


@pytest.mark.parametrize("text", [f"  {LINE}\n", f"{LINE}.", f" {LINE}. "])
def test_request_tolerates_surrounding_whitespace_and_a_trailing_full_stop(text):
    m = REQUEST.fullmatch(text)
    assert m is not None
    assert m["fixed_version"] == "1.26.18"
    assert m["repository"] == "checkout"


@pytest.mark.parametrize(
    "text",
    [
        "Please patch urllib3 in checkout, it is vulnerable.",
        "PATCH checkout urllib3 1.26.4 -> 1.26.18",  # no CVE id
        "PATCH checkout CVE-2026-41234 urllib3 1.26.4 1.26.18",  # no arrow
        "patch checkout CVE-2026-41234 urllib3 1.26.4 -> 1.26.18",  # not the keyword
        f"Sure, here is the call: {LINE}",
        "",
    ],
)
def test_request_rejects_anything_else(text):
    assert REQUEST.fullmatch(text) is None


# --- the commit ---------------------------------------------------------------------


def test_commit_is_seven_lowercase_hex_digits_of_the_requests_sha1():
    commit = commit_for(LINE)
    assert re.fullmatch(r"[0-9a-f]{7}", commit)
    assert commit == commit_for(LINE)
    assert commit != commit_for(LINE.replace("checkout", "payments"))


# --- the reply ----------------------------------------------------------------------


def test_reply_to_a_request_names_the_cve_repository_package_and_both_versions():
    assert format_reply(LINE, STARTED, FINISHED) == (
        f"Patched CVE-2026-41234 in checkout: bumped urllib3 1.26.4 → 1.26.18 in commit {commit_for(LINE)}, "
        "started 2026-12-08T11:42:03.120Z, finished 2026-12-08T11:42:09.870Z"
    )


def test_reply_to_text_that_is_not_a_request_echoes_it():
    text = "please fix urllib3 in checkout"
    assert format_reply(text, STARTED, FINISHED) == (
        f"Patched (please fix urllib3 in checkout) in commit {commit_for(text)}, "
        "started 2026-12-08T11:42:03.120Z, finished 2026-12-08T11:42:09.870Z"
    )


def test_an_echo_stays_on_one_line():
    reply = format_reply("  fix urllib3\nin checkout  ", STARTED, FINISHED)
    assert reply.startswith("Patched (fix urllib3 in checkout) in commit ")
    assert "\n" not in reply


def test_reply_times_are_utc_with_milliseconds_and_z():
    cet = timezone(timedelta(hours=1))
    reply = format_reply(LINE, STARTED.astimezone(cet), FINISHED.astimezone(cet))
    assert "started 2026-12-08T11:42:03.120Z, finished 2026-12-08T11:42:09.870Z" in reply


@pytest.mark.parametrize("request_text", [LINE, f"{LINE}.", "please fix urllib3 in checkout"])
def test_the_reply_regexes_read_back_the_commit_start_and_finish(request_text):
    started = datetime(2026, 12, 8, 23, 59, 58, 999_000, tzinfo=timezone.utc)
    finished = started + timedelta(seconds=6, milliseconds=750)
    reply = format_reply(request_text, started, finished)

    assert REPLY_COMMIT.search(reply).group(1) == commit_for(request_text)
    read_started = _utc(REPLY_STARTED.search(reply).group(1))
    read_finished = _utc(REPLY_FINISHED.search(reply).group(1))
    assert (read_started, read_finished) == (started, finished)
    assert read_finished >= read_started


def test_the_reply_regexes_find_nothing_in_a_reply_without_a_patch():
    for pattern in (REPLY_COMMIT, REPLY_STARTED, REPLY_FINISHED):
        assert pattern.search("N/A") is None
        assert pattern.search("The patch agent call failed: request timed out.") is None


def test_the_reply_regexes_are_plain_enough_for_flink():
    """Lab 6.4 runs them in REGEXP_EXTRACT (Java regex): one numbered group, no Python-only syntax."""
    for pattern in (REPLY_COMMIT, REPLY_STARTED, REPLY_FINISHED):
        assert pattern.groups == 1
        assert "(?P" not in pattern.pattern
        assert pattern.flags & ~re.UNICODE == 0


# --- the graph ----------------------------------------------------------------------


def _run(graph, request_text: str) -> tuple[list[dict], dict]:
    """The graph's custom events and its final state, for one request."""

    async def run():
        events, state = [], {}
        async for mode, chunk in graph.astream({"request": request_text}, stream_mode=["custom", "values"]):
            if mode == "custom":
                events.append(chunk)
            else:
                state = chunk
        return events, state

    return asyncio.run(run())


def test_the_graph_answers_a_request_with_its_reply():
    state = asyncio.run(build_graph(min_delay=0, max_delay=0).ainvoke({"request": LINE}))
    reply = state["reply"]
    assert reply.startswith(
        f"Patched CVE-2026-41234 in checkout: bumped urllib3 1.26.4 → 1.26.18 in commit {commit_for(LINE)}, "
    )
    assert _utc(REPLY_FINISHED.search(reply).group(1)) >= _utc(REPLY_STARTED.search(reply).group(1))


def test_the_graph_waits_a_random_delay_between_its_bounds_and_says_so_first():
    events, state = _run(build_graph(min_delay=0.02, max_delay=0.05), LINE)
    [event] = events
    assert 0.02 <= event["delay"] <= 0.05
    took = _utc(REPLY_FINISHED.search(state["reply"]).group(1)) - _utc(REPLY_STARTED.search(state["reply"]).group(1))
    assert took >= timedelta(seconds=event["delay"]) - timedelta(milliseconds=1)  # times are cut to ms


def test_the_default_delay_is_four_to_ten_seconds():
    assert (DEFAULT_MIN_DELAY, DEFAULT_MAX_DELAY) == (4, 10)


# --- the server ---------------------------------------------------------------------


def _server(public_url: str | None = PUBLIC_URL, verbose: bool = False) -> tuple[TestClient, io.StringIO]:
    """The app in-process, with the delays at 0, and what it prints."""
    out = io.StringIO()
    console = Console(file=out, width=200, color_system=None)
    app = create_app(agent_card("localhost", 8001, public_url), build_graph(0, 0), console, verbose=verbose)
    return TestClient(app), out


# A2A 1.0 names the method SendMessage and wants its version header; 0.3 says message/send.
SEND_MESSAGE = {
    "1.0": (
        {"A2A-Version": "1.0"},
        "SendMessage",
        {"role": "ROLE_USER", "messageId": "m-1", "parts": [{"text": LINE}]},
    ),
    "0.3": (
        {},
        "message/send",
        {"kind": "message", "role": "user", "messageId": "m-1", "parts": [{"kind": "text", "text": LINE}]},
    ),
}


def _send(client: TestClient, version: str = "0.3", **kwargs) -> tuple[str, str, str]:
    """One send-message call: the task's final state, and its artifact's name and text."""
    headers, method, message = SEND_MESSAGE[version]
    body = {"jsonrpc": "2.0", "id": "1", "method": method, "params": {"message": message}}
    response = client.post("/", json=body, headers=headers, **kwargs)
    assert response.status_code == 200
    result = response.json()["result"]
    task = result["task"] if version == "1.0" else result
    [artifact] = task["artifacts"]
    [part] = artifact["parts"]
    return task["status"]["state"], artifact["name"], part["text"]


def test_the_agent_card_lists_the_public_url_first():
    client, _ = _server()
    card = client.get("/.well-known/agent-card.json").json()
    assert [i["url"] for i in card["supportedInterfaces"]] == [f"{PUBLIC_URL}/", "http://localhost:8001/"]
    assert card["url"] == f"{PUBLIC_URL}/"  # the A2A 0.3 card's main URL is the first interface
    assert card["name"] == "Patch Agent"
    [skill] = card["skills"]
    assert skill["name"] == "Patch a vulnerable dependency"
    assert card["defaultInputModes"] == card["defaultOutputModes"] == ["text/plain"]


def test_without_a_public_url_the_card_lists_localhost_only():
    client, _ = _server(public_url=None)
    card = client.get("/.well-known/agent-card.json").json()
    assert [i["url"] for i in card["supportedInterfaces"]] == ["http://localhost:8001/"]


@pytest.mark.parametrize("version", sorted(SEND_MESSAGE))
def test_a_send_message_call_completes_with_the_reply_as_its_artifact(version):
    client, _ = _server()
    state, name, text = _send(client, version)
    assert state == {"1.0": "TASK_STATE_COMPLETED", "0.3": "completed"}[version]
    assert name == "result"
    assert text.startswith(
        f"Patched CVE-2026-41234 in checkout: bumped urllib3 1.26.4 → 1.26.18 in commit {commit_for(LINE)}, started "
    )
    assert REPLY_FINISHED.search(text)


def test_an_empty_request_is_refused_as_invalid_params():
    client, out = _server()
    message = {"kind": "message", "role": "user", "messageId": "m-1", "parts": [{"kind": "text", "text": "  "}]}
    body = {"jsonrpc": "2.0", "id": "1", "method": "message/send", "params": {"message": message}}
    response = client.post("/", json=body)
    assert response.json()["error"]["code"] == -32602
    assert "patching" not in out.getvalue()


@pytest.mark.parametrize("auth", [("workshop", "workshop"), ("anyone", "any password"), None])
def test_any_credentials_are_accepted(auth):
    client, _ = _server()
    state, _, _ = _send(client, auth=auth)
    assert state == "completed"


def test_each_call_prints_the_request_the_wait_and_the_reply():
    client, out = _server()
    _, _, reply = _send(client)
    printed = out.getvalue()
    assert re.search(rf"\d\d:\d\d:\d\d .*{re.escape(LINE)}", printed)
    assert "patching… 0 s" in printed
    assert reply in printed
    assert printed.index(LINE) < printed.index("patching…") < printed.index(reply)


def test_verbose_prints_each_requests_method_path_and_jsonrpc_method():
    client, out = _server(verbose=True)
    client.get("/.well-known/agent-card.json")
    _send(client, auth=("workshop", "a-secret-password"))
    printed = out.getvalue()
    assert "GET /.well-known/agent-card.json" in printed
    assert re.search(r"POST / +message/send +auth: Basic$", printed, re.MULTILINE)  # the scheme alone
    assert base64.b64encode(b"workshop:a-secret-password").decode() not in printed


def test_quiet_about_http_without_verbose():
    client, out = _server()
    client.get("/.well-known/agent-card.json")
    _send(client)
    assert "GET /" not in out.getvalue()
    assert "POST /" not in out.getvalue()


# --- lab 6.2 ------------------------------------------------------------------------


def _statements(sql: str) -> list[str]:
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in body.split(";") if statement.strip()]


def _options(statement: str) -> dict[str, str]:
    return dict(re.findall(r"'(\w+)' = '([^']*)'", statement))


def test_lab_62_creates_the_connection_then_the_tool():
    connection, tool = _statements(LAB_62.read_text())
    assert connection.startswith("CREATE CONNECTION `patch_agent_connection`\nWITH (")
    assert _options(connection) == {
        "type": "a2a",
        "endpoint": PLACEHOLDER,
        "username": "workshop",
        "password": "workshop",
    }
    assert tool.startswith("CREATE TOOL `patch_agent_tool`\nUSING CONNECTION `patch_agent_connection`\nWITH (")
    assert _options(tool) == {
        "type": "a2a",
        "agent_card_path": "/.well-known/agent-card.json",
        "request_timeout": "30",
        "max_retries": "0",  # a retry would patch twice
    }


def test_lab_62_has_no_apostrophe_outside_its_comments_and_its_quoted_values():
    for statement in _statements(LAB_62.read_text()):
        assert "''" not in statement
        assert "'" not in re.sub(r"'[^'\n]*'", "", statement)


def test_lab_62_names_the_placeholder_once_so_only_the_endpoint_is_filled_in():
    assert LAB_62.read_text().count(PLACEHOLDER) == 1


def test_lab_62_opens_no_section_and_leaves_the_tunnel_as_a_todo():
    header = [line for line in LAB_62.read_text().splitlines() if line.startswith("--")]
    assert header[0].startswith("-- 6.2 ")
    assert not any("Section 6" in line for line in header)  # lab 6.1 opens it
    assert any("TODO (tunnel)" in line for line in header)


# --- start-up -----------------------------------------------------------------------


def _start_output(public_url: str | None) -> str:
    out = io.StringIO()
    print_start(Console(file=out, width=80, color_system=None), "localhost", 8001, public_url)
    return out.getvalue()


@pytest.mark.parametrize("public_url", [PUBLIC_URL, f"{PUBLIC_URL}/"])
def test_start_up_prints_lab_62_with_only_the_placeholder_replaced_by_the_public_url(public_url):
    filled = LAB_62.read_text().replace(PLACEHOLDER, PUBLIC_URL)
    assert lab_sql(public_url) == filled
    printed = _start_output(public_url)
    assert filled in printed
    assert "Warning" not in printed


def test_without_a_public_url_start_up_warns_and_says_a_tunnel_is_needed():
    printed = _start_output(None)
    assert LAB_62.read_text() in printed
    assert printed.index("Warning") < printed.index(PLACEHOLDER)
    assert "tunnel" in printed.split(PLACEHOLDER)[-1]


def test_cli_defaults():
    args = parse_args([])
    assert (args.host, args.port, args.public_url, args.verbose) == ("localhost", 8001, None, False)
    assert (args.min_delay, args.max_delay) == (DEFAULT_MIN_DELAY, DEFAULT_MAX_DELAY)


@pytest.mark.parametrize(
    "argv",
    [["--public-url", "patch-demo.example.dev"], ["--min-delay", "5", "--max-delay", "1"], ["--min-delay", "-1"]],
)
def test_cli_rejects_a_url_without_a_scheme_and_delays_out_of_order(argv, capsys):
    with pytest.raises(SystemExit):
        parse_args(argv)
