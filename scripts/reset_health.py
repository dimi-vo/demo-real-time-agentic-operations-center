"""`uv run reset-health` — a clean slate for your detection and your agents.

Deletes the Flink statements that write the five lab output tables, `service_error_rate`,
`service_health`, `remediation_decisions`, and section 5's `p1_tickets` and `ticket_triage`
(D-068), waits until they are gone, then empties those five topics. Section 5's three INSERTs
go with them: lab 5.1's P1 filter, lab 5.2's classifier and lab 5.3's responder, which writes
`remediation_decisions` like the agents of 4.3. Nothing else is touched: the source topics
keep their records, and the agents stay.

Use it to change your `service_error_rate` or `service_health` INSERT and run it again without
duplicate history: the SQL stays in your workspace, so click Run to start a statement again
(D-052).
"""

from __future__ import annotations

import argparse
import re
import sys

from dotenv import dotenv_values

from scripts.common.credentials import resolve_card
from scripts.common.flink import TERMINAL_PHASES, FlinkSession, wait_until_gone
from scripts.common.paths import get_project_root
from scripts.common.topics import OUTPUT_TOPICS, empty_topics

WAIT_TIMEOUT_SECONDS = 60
POLL_SECONDS = 2
_ANY_OUTPUT_TABLE = f"{', '.join(OUTPUT_TOPICS[:-1])} or {OUTPUT_TOPICS[-1]}"

_IDENT = r"(?:`[^`]+`|\w+)"
_INSERT_INTO = re.compile(rf"INSERT\s+INTO\s+({_IDENT}(?:\s*\.\s*{_IDENT})*)(?![\w`])", re.IGNORECASE)


def _strip_leading_comments(sql: str) -> str:
    """The SQL without the `--` and `/* */` comments it opens with (attendees paste whole files)."""
    sql = sql.lstrip()
    while True:
        if sql.startswith("--"):
            _, _, sql = sql.partition("\n")
        elif sql.startswith("/*") and "*/" in sql:
            sql = sql.split("*/", 1)[1]
        else:
            return sql
        sql = sql.lstrip()


def writes_output_table(sql: str) -> bool:
    """True when the statement is `INSERT INTO <an output table> ...` (OUTPUT_TOPICS).

    Matching is on the text: leading comments, backticks, catalog and database prefixes and
    letter case are ignored. A SELECT, or an INSERT into another table, is not a target."""
    m = _INSERT_INTO.match(_strip_leading_comments(sql))
    if not m:
        return False
    last = re.findall(_IDENT, m.group(1))[-1]
    return last.strip("`").lower() in OUTPUT_TOPICS


def _targets(session: FlinkSession) -> list[tuple[str, str]]:
    """(name, SQL) of every statement that is not finished and writes an output table."""
    found = []
    for st in session.list_statements():
        sql = st.get("spec", {}).get("statement", "") or ""
        if st.get("status", {}).get("phase", "") not in TERMINAL_PHASES and writes_output_table(sql):
            found.append((st.get("name", ""), sql))
    return found


def _print_next_steps() -> None:
    print("\nNext, start your statements again with Run in the Flink workspace, in this order:")
    print("  1. Your `INSERT INTO service_error_rate` and `INSERT INTO service_health` first. Each")
    print("     replays up to 24 hours of history.")
    print("  2. The agent INSERTs of 4.3 (`INSERT INTO remediation_decisions`), the release agent's")
    print("     error-rate one (4.3_insert_release_agent_errors.sql) first, only once the ops wall's")
    print("     charts reach the right-hand edge again. Agents started during the replay would call")
    print("     the model for every replayed anomaly.")
    print("  3. Section 5, in lab order: 5.1 (5.1_p1_tickets.sql), which replays the last 24 hours")
    print("     of P1 tickets; then 5.2's INSERT (5.2_insert_ticket_classifier.sql) only")
    print("     once 5.1 has caught up, so the classifier gets new tickets only; then 5.3's INSERT")
    print("     (5.3_insert_ticket_responder.sql).")
    print("\nRestart the ops wall (Ctrl-C, then `uv run ops-wall`) for a clean view.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Delete the statements that write service_error_rate, service_health, "
            "remediation_decisions, p1_tickets and ticket_triage, wait until they are gone, "
            "and empty those topics."
        ),
        epilog="Your SQL stays in your workspace: click Run to start a statement again.",
    )
    parser.add_argument("--creds", help="Path to the credential card (default: resolved from credentials.env)")
    args = parser.parse_args(argv)

    root = get_project_root()
    card_path = resolve_card(args.creds, root)
    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    print(f"Using credential card: {card_path.relative_to(root) if card_path.is_relative_to(root) else card_path}")

    session = FlinkSession(card)
    targets = _targets(session)
    if targets:
        print(f"\nDeleting {len(targets)} statement(s) that write {_ANY_OUTPUT_TABLE}:")
        for name, sql in targets:
            # Printed before anything is deleted: the SQL is the way back if the workspace cell is gone.
            print(f"\n--- {name} ---\n{sql.strip()}\n---")
            session.stop(name)
        stuck = wait_until_gone(session, [name for name, _ in targets], WAIT_TIMEOUT_SECONDS, POLL_SECONDS)
        if stuck:
            print(f"\nStill running after {WAIT_TIMEOUT_SECONDS} s: {', '.join(stuck)}")
            print("Nothing was emptied, because a running INSERT would write into the topic again.")
            print("Delete them in the Confluent Cloud console, then run `uv run reset-health` again.")
            sys.exit(1)
        print("\nThey are gone.")
    else:
        print(f"\nNo running statement writes {_ANY_OUTPUT_TABLE}.")

    problems = empty_topics(card, OUTPUT_TOPICS, "Emptying the output topics...")
    if problems:
        print("\nreset-health finished with problems: " + "; ".join(problems))
        sys.exit(1)

    _print_next_steps()


if __name__ == "__main__":
    main()
