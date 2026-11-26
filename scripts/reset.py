"""`uv run reset` — blank slate for a fresh run of the stream-processing labs.

Troubleshooting only; the continuous workshop never needs it in the normal flow. It:

1. refuses to run while a local `uv run ops-sim` is still producing (`--force` overrides);
2. stops the developer's running Flink statements and waits until they are gone;
3. drops the five agents, section 4's `release_agent` and `capacity_agent`, section 5's
   `ticket_classifier` and `ticket_responder` and section 6's `security_agent`, then lab
   6.2's `patch_agent_tool` and `patch_agent_connection`, in that order, since each uses the
   next (D-070). `CREATE AGENT`, `CREATE TOOL` and `CREATE CONNECTION` have no
   `IF NOT EXISTS`, so labs 4.2, 5.2, 5.3, 6.2 and 6.3 could not be run again otherwise;
4. empties fourteen topics, all with delete cleanup: the seven source topics (checkout's
   `support_tickets` and `ticket_answer_key`, D-068, and the scanner's `cve_scans` and
   `cve_answer_key`, D-070, among them) and the seven lab output topics
   (`service_error_rate`, `service_health`, `remediation_decisions`, section 5's
   `p1_tickets` and `ticket_triage`, and section 6's `critical_cves` and `patch_decisions`);
5. deletes the generator's persisted state (versions, incident counter, ticket numbers and
   finding number). Step 4 already emptied the ground truth: it lives in the
   `service_incidents`, `ticket_answer_key` and `cve_answer_key` topics (D-046).

The output tables are the platform team's (Terraform creates them, D-050), so they are
emptied, never dropped: a dropped table would not come back. Nothing is rebuilt
automatically afterwards: run the lab INSERTs again. `--keep-source` skips the seven source
topics in step 4 and keeps the state file in step 5: the kept topics hold the incidents,
versions, tickets and findings it counts past, so the next ops-sim never reuses an incident
id (D-046), a version (D-067), a ticket id (D-068) or a finding id (D-070) next to them. The
output topics are always emptied, because they would otherwise hold duplicates once their
INSERTs run again. Everything reports whether it worked; anything that didn't exits nonzero.
To clear only the detection and the agents' output, use `uv run reset-health`.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

from dotenv import dotenv_values

from opsgen.state import state_path
from scripts.common.credentials import resolve_card
from scripts.common.flink import TERMINAL_PHASES, FlinkSession, wait_until_gone
from scripts.common.paths import get_project_root
from scripts.common.topics import OUTPUT_TOPICS, SOURCE_TOPICS, empty_topics

# Labs 4.2, 5.2, 5.3 and 6.3 create them (D-068 added the two ticket agents, D-070 the security agent).
LAB_AGENTS = ("release_agent", "capacity_agent", "ticket_classifier", "ticket_responder", "security_agent")
# Lab 6.2 creates them (D-070). security_agent uses the tool and the tool uses the connection,
# so they are dropped after the agents, the tool first.
LAB_TOOLS_AND_CONNECTIONS = (("TOOL", "patch_agent_tool"), ("CONNECTION", "patch_agent_connection"))
OPS_SIM_TOKENS = ("ops-sim", "opsgen.cli", "opsgen")
STATEMENTS_GONE_TIMEOUT = 60  # seconds


def _local_ops_sim() -> list[str]:
    try:
        result = subprocess.run(["ps", "-eo", "pid=,args="], capture_output=True, text=True, timeout=15)
    except Exception:
        return []
    me = os.getpid()
    found = []
    for line in result.stdout.splitlines():
        pid_text, _, args = line.strip().partition(" ")
        if not pid_text.isdigit() or int(pid_text) == me:
            continue
        toks = args.split()
        if any(t == m or t.endswith(f"/{m}") for t in toks for m in OPS_SIM_TOKENS):
            found.append(f"{pid_text}  {args.strip()}")
    return found


def _stop_running_statements(session: FlinkSession) -> list[str]:
    """Delete every statement that is still running; returns the problems found."""
    print("\nStopping your running Flink statements...")
    stopped = []
    for st in session.list_statements():
        name = st.get("name", "")
        phase = st.get("status", {}).get("phase", "")
        if phase and phase not in TERMINAL_PHASES:
            session.stop(name)
            stopped.append(name)
    print(f"  Stopped {len(stopped)} statement(s).")
    # An INSERT that is still writing would leave stray rows in a topic emptied below.
    stuck = wait_until_gone(session, stopped, timeout=STATEMENTS_GONE_TIMEOUT)
    for name in stuck:
        print(f"  {name}: still running after {STATEMENTS_GONE_TIMEOUT} s")
    return [f"statement {name} is still running" for name in stuck]


def _drop(session: FlinkSession, heading: str, objects: list[tuple[str, str]]) -> list[str]:
    """`DROP <kind> IF EXISTS` each (kind, name) in turn, printing one line each; returns the
    problems found."""
    problems: list[str] = []
    print(f"\n{heading}")
    for kind, obj in objects:
        try:
            name = session.submit(f"DROP {kind} IF EXISTS `{obj}`;", label="aiops-reset")
        except RuntimeError as e:
            print(f"  {obj}: submit failed — {e}")
            problems.append(f"drop {obj}")
            continue
        phase = session.wait(name, timeout=120).get("status", {}).get("phase")
        if phase == "COMPLETED":
            print(f"  {obj}: dropped")
        else:
            print(f"  {obj}: {phase}")
            if phase == "FAILED":
                problems.append(f"drop {obj}")
    return problems


def _drop_agents(session: FlinkSession) -> list[str]:
    problems = _drop(session, "Dropping the five agents...", [("AGENT", agent) for agent in LAB_AGENTS])
    return problems + _drop(session, "Dropping lab 6.2's tool and connection...", list(LAB_TOOLS_AND_CONNECTIONS))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reset the workshop to a blank slate (troubleshooting only)")
    parser.add_argument("--creds", help="Path to the credential card (default: resolved from credentials.env)")
    parser.add_argument(
        "--keep-source",
        action="store_true",
        help="Keep the accumulated data in the seven source topics, and the generator state that numbers past it "
        "(the output topics are always emptied)",
    )
    parser.add_argument("--force", action="store_true", help="Reset even if a local ops-sim seems to be running")
    args = parser.parse_args(argv)

    running = _local_ops_sim()
    if running and not args.force:
        print("A local generator is still producing — clearing the topics now would just refill them:")
        for line in running:
            print(f"  {line}")
        print("Stop `uv run ops-sim` (Ctrl-C), then run reset again (or pass --force).")
        sys.exit(1)

    root = get_project_root()
    card_path = resolve_card(args.creds, root)
    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    print(f"Using credential card: {card_path.relative_to(root) if card_path.is_relative_to(root) else card_path}")

    session = FlinkSession(card)
    problems: list[str] = []
    problems += _stop_running_statements(session)
    problems += _drop_agents(session)
    if not args.keep_source:
        problems += empty_topics(card, SOURCE_TOPICS, "Emptying the source topics...")
    problems += empty_topics(card, OUTPUT_TOPICS, "Emptying the output topics (the tables stay)...")

    # Local generator state: its counters number past what the source topics hold, so it goes
    # with them (the ground truth lives in service_incidents, ticket_answer_key and cve_answer_key).
    if args.keep_source:
        print("\nKept the generator's persisted state, so ops-sim numbers past the kept source topics.")
    else:
        state_path(root).unlink(missing_ok=True)
        print("\nDeleted the generator's persisted state.")

    if problems:
        print("\nReset finished with problems: " + "; ".join(problems))
        sys.exit(1)
    print("\nReset complete. The output tables are still there, and nothing was rebuilt: start `uv run ops-sim`,")
    print("then run the lab INSERTs (2.2, 2.5, 4.3, 5.1, 5.2, 5.3, 6.1 and 6.4) again,")
    print("and create the agents again in labs 4.2, 5.2, 5.3, 6.2 (the patch agent's connection and tool) and 6.3.")


if __name__ == "__main__":
    main()
