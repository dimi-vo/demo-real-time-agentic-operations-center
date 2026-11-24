"""`uv run reset` — blank slate for a fresh run of the stream-processing labs.

Troubleshooting only; the continuous workshop never needs it in the normal flow. It:

1. refuses to run while a local `uv run ops-sim` is still producing (`--force` overrides);
2. stops the developer's running Flink statements and waits until they are gone;
3. drops the four agents, section 4's `release_agent` and `capacity_agent` and section 5's
   `ticket_classifier` and `ticket_responder` (`CREATE AGENT` has no `IF NOT EXISTS`, so
   labs 4.2, 5.2 and 5.3 could not be run again otherwise);
4. empties ten topics, all with delete cleanup: the five source topics (checkout's
   `support_tickets` and `ticket_answer_key` among them, D-068) and the five lab output
   topics (`service_error_rate`, `service_health`, `remediation_decisions`, and section 5's
   `p1_tickets` and `ticket_triage`);
5. deletes the generator's persisted state (versions, incident counter and ticket
   numbers). Step 4 already emptied the ground truth: it lives in the `service_incidents`
   and `ticket_answer_key` topics (D-046).

The output tables are the platform team's (Terraform creates them, D-050), so they are
emptied, never dropped: a dropped table would not come back. Nothing is rebuilt
automatically afterwards: run the lab INSERTs again. `--keep-source` skips the five source
topics in step 4; the output topics are always emptied, because they would otherwise
hold duplicates once their INSERTs run again. Everything reports whether it worked;
anything that didn't exits nonzero. To clear only the detection and the agents' output,
use `uv run reset-health`.
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

# Labs 4.2, 5.2 and 5.3 create them (D-068 added the two ticket agents).
LAB_AGENTS = ("release_agent", "capacity_agent", "ticket_classifier", "ticket_responder")
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


def _drop_agents(session: FlinkSession) -> list[str]:
    problems: list[str] = []
    print("\nDropping the four agents...")
    for agent in LAB_AGENTS:
        try:
            name = session.submit(f"DROP AGENT IF EXISTS `{agent}`;", label="aiops-reset")
        except RuntimeError as e:
            print(f"  {agent}: submit failed — {e}")
            problems.append(f"drop {agent}")
            continue
        phase = session.wait(name, timeout=120).get("status", {}).get("phase")
        if phase == "COMPLETED":
            print(f"  {agent}: dropped")
        else:
            print(f"  {agent}: {phase}")
            if phase == "FAILED":
                problems.append(f"drop {agent}")
    return problems


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reset the workshop to a blank slate (troubleshooting only)")
    parser.add_argument("--creds", help="Path to the credential card (default: resolved from credentials.env)")
    parser.add_argument(
        "--keep-source",
        action="store_true",
        help="Keep the accumulated data in the five source topics (the output topics are always emptied)",
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

    # Local generator state (the ground truth lives in service_incidents and ticket_answer_key,
    # emptied above).
    state_path(root).unlink(missing_ok=True)
    print("\nDeleted the generator's persisted state.")

    if problems:
        print("\nReset finished with problems: " + "; ".join(problems))
        sys.exit(1)
    print("\nReset complete. The output tables are still there, and nothing was rebuilt: start `uv run ops-sim`,")
    print("then run the lab INSERTs (2.2, 2.5, 4.3, 5.1, 5.2 and 5.3) again,")
    print("and create the agents again in labs 4.2, 5.2 and 5.3.")


if __name__ == "__main__":
    main()
