"""`uv run setup-rtce` — register the Real-Time Context Engine as an MCP server with a
coding agent, or print a Lightning Query curl (`--lightning`).

RTCE exposes an RTCE-enabled topic to an MCP client (Claude Code, Codex CLI, GitHub
Copilot CLI, VS Code + Copilot). The card's RTCE credentials are the developer's own
Global key (`AIOPS_RTCE_API_KEY`/`SECRET`), and the endpoint is `AIOPS_RTCE_MCP_ENDPOINT`.
`--lightning` prints a ready-to-run curl against the Lightning Query REST API.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import shlex
import subprocess
import sys

from scripts.common.credentials import load_card

SERVER_NAME = "real-time-context-engine"
_EXPECTED = ("AIOPS_RTCE_MCP_ENDPOINT", "AIOPS_RTCE_API_KEY", "AIOPS_RTCE_API_SECRET")
# Lightning SQL supports only SELECT / WHERE / ORDER BY / LIMIT (D-008): no aggregates,
# no GROUP BY, no joins. One example per served table.
LIGHTNING_QUERIES = {
    "service_health": (
        "SELECT service_name, window_end, latency_status, traffic_status, avg_latency_ms "
        "FROM service_health ORDER BY window_end DESC LIMIT 10"
    ),
    "remediation_decisions": (
        "SELECT window_end, service_name, agent_name, action, target, reason "
        "FROM remediation_decisions WHERE trigger = 'LATENCY_HIGH' "
        "ORDER BY window_end DESC LIMIT 1"
    ),
    # RTCE-enabled by the platform team's Terraform (D-070): the latest patches and their commits.
    "patch_decisions": (
        "SELECT event_time, finding_id, repository, cve_id, commit_sha, reason "
        "FROM patch_decisions WHERE patch_status = 'PATCHED' "
        "ORDER BY event_time DESC LIMIT 5"
    ),
}
LIGHTNING_QUERY = LIGHTNING_QUERIES["service_health"]


def basic_token(key: str, secret: str) -> str:
    return base64.b64encode(f"{key}:{secret}".encode()).decode()


# --- Lightning ----------------------------------------------------------------------


def _lightning_request(card: dict[str, str], table: str = "service_health") -> tuple[str, str, str]:
    endpoint = card.get("AIOPS_RTCE_MCP_ENDPOINT", "")
    match = re.fullmatch(
        r"https://mcp\.([a-z0-9-]+)\.aws\.confluent\.cloud/"
        r"mcp/v1/context-engine/organizations/[^/]+/environments/[^/]+/kafka-clusters/[^/]+/?",
        endpoint,
    )
    if not match:
        raise ValueError("Missing or invalid AIOPS_RTCE_MCP_ENDPOINT on the credential card.")
    required = ("AIOPS_ENVIRONMENT_ID", "AIOPS_CLUSTER_ID", "AIOPS_RTCE_API_KEY", "AIOPS_RTCE_API_SECRET")
    missing = [name for name in required if not card.get(name)]
    if missing:
        raise ValueError("Missing credentials: " + ", ".join(missing))
    region = match.group(1)
    url = f"https://sql.{region}.aws.confluent.cloud/query/v1"
    payload = json.dumps(
        {
            "catalog_name": card["AIOPS_ENVIRONMENT_ID"],
            "database_name": card["AIOPS_CLUSTER_ID"],
            "query": LIGHTNING_QUERIES[table],
        },
        indent=2,
    )
    return url, payload, basic_token(card["AIOPS_RTCE_API_KEY"], card["AIOPS_RTCE_API_SECRET"])


def lightning_command(card: dict[str, str], table: str = "service_health") -> str:
    url, payload, token = _lightning_request(card, table)
    return " \\\n  ".join(
        (
            "curl -sS -X POST " + shlex.quote(url),
            "-H " + shlex.quote("Authorization: Basic " + token),
            "-H " + shlex.quote("Content-Type: application/json"),
            "-d " + shlex.quote(payload),
        )
    )


# --- MCP registration ---------------------------------------------------------------


def claude_argv(endpoint: str, token: str) -> list[str]:
    return [
        "claude", "mcp", "add", "--transport", "http", SERVER_NAME, endpoint,
        "--header", f"Authorization: Basic {token}",
    ]


def copilot_argv(endpoint: str, token: str) -> list[str]:
    return [
        "copilot", "mcp", "add", "--transport", "http", "--header",
        f"Authorization: Basic {token}", SERVER_NAME, endpoint,
    ]


def vscode_argv(endpoint: str, token: str) -> list[str]:
    entry = {"name": SERVER_NAME, "type": "http", "url": endpoint, "headers": {"Authorization": f"Basic {token}"}}
    return ["code", "--add-mcp", json.dumps(entry)]


def codex_snippet(endpoint: str, token: str) -> str:
    return (
        f'[mcp_servers.{SERVER_NAME.replace("-", "_")}]\n'
        f'url = "{endpoint}"\n'
        f'http_headers = {{ "Authorization" = "Basic {token}" }}\n'
    )


def _register(argv: list[str], client: str) -> bool:
    try:
        # Remove any prior entry first (idempotent re-register).
        subprocess.run([argv[0], "mcp", "remove", SERVER_NAME], capture_output=True, text=True, timeout=30)
        result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        print(f"  {client}: could not run its CLI ({e}).")
        return False
    if result.returncode == 0:
        print(f"  {client}: registered '{SERVER_NAME}'. Restart {client}.")
        return True
    print(f"  {client}: FAILED — {(result.stderr or result.stdout).strip()[:300]}")
    return False


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Register RTCE as an MCP server, or print a Lightning Query curl")
    p.add_argument("--creds", help="Path to the credential card (default: resolved from credentials.env)")
    p.add_argument("--client", choices=["claude", "codex", "vscode", "copilot", "all"], help="Which coding agent")
    p.add_argument(
        "--lightning",
        nargs="?",
        const="service_health",
        choices=sorted(LIGHTNING_QUERIES),
        help="Print a ready-to-run Lightning Query curl for this table and exit (default: service_health)",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    _path, card = load_card(args.creds)

    if args.lightning:
        try:
            print(f"\n# Lightning Query against {args.lightning} — copy and run:\n")
            print(lightning_command(card, args.lightning))
        except ValueError as e:
            sys.exit(f"Error: {e}")
        return

    missing = [k for k in _EXPECTED if not card.get(k)]
    if missing:
        sys.exit("Credential card is missing: " + ", ".join(missing))
    endpoint = card["AIOPS_RTCE_MCP_ENDPOINT"]
    token = basic_token(card["AIOPS_RTCE_API_KEY"], card["AIOPS_RTCE_API_SECRET"])

    client = args.client or input("Coding agent? [claude/codex/vscode/copilot/all]: ").strip().lower() or "claude"
    print()
    if client in ("claude", "all"):
        _register(claude_argv(endpoint, token), "Claude Code")
    if client in ("copilot", "all"):
        _register(copilot_argv(endpoint, token), "GitHub Copilot CLI")
    if client in ("vscode", "all"):
        _register(vscode_argv(endpoint, token), "VS Code (Copilot)")
    if client in ("codex", "all"):
        print("  Codex CLI: add this to ~/.codex/config.toml, then restart Codex:\n")
        print(codex_snippet(endpoint, token))


if __name__ == "__main__":
    main()
