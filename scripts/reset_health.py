"""`uv run reset-health` — a clean slate for your detection and your agents.

Deletes the Flink statements that write `service_error_rate`, `service_health` and
`remediation_decisions`, waits until they are gone, then empties those three topics. Nothing
else is touched.

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
    print("  2. The agent INSERTs (`INSERT INTO remediation_decisions`), the release agent's error-rate")
    print("     one (4.3_insert_release_agent_errors.sql) first, only once the ops wall's")
    print("     charts reach the right-hand edge again. Agents started during the replay would call")
    print("     the model for every replayed anomaly.")
    print("\nRestart the ops wall (Ctrl-C, then `uv run ops-wall`) for a clean view.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Delete the statements that write service_error_rate, service_health and "
            "remediation_decisions, wait until they are gone, and empty those topics."
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
        print(f"\nDeleting {len(targets)} statement(s) that write {' or '.join(OUTPUT_TOPICS)}:")
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
        print(f"\nNo running statement writes {' or '.join(OUTPUT_TOPICS)}.")

    problems = empty_topics(card, OUTPUT_TOPICS, "Emptying the output topics...")
    if problems:
        print("\nreset-health finished with problems: " + "; ".join(problems))
        sys.exit(1)

    _print_next_steps()


if __name__ == "__main__":
    main()
