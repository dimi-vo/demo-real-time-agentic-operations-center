"""`uv run reset` — blank slate for a fresh run of the stream-processing labs.

Troubleshooting only; the continuous workshop never needs it in the normal flow. It:

1. refuses to run while a local `uv run ops-sim` is still producing (`--force` overrides);
2. stops the developer's running Flink statements;
3. drops the lab objects — `service_health`, `remediation_decisions`, and the two
   agents — and their Schema Registry subjects;
4. clears the records from the source topics (all delete-cleanup, so this works);
5. deletes the generator's persisted state and `incidents.jsonl`.

`--with-labs` then rebuilds the lab statements from `sql/`. `--keep-source` skips step 4.
Everything reports whether it worked; anything that didn't exits nonzero.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

import requests
from dotenv import dotenv_values

from opsgen.groundtruth import incidents_path
from opsgen.state import state_path
from scripts.common.credentials import resolve_card
from scripts.common.flink import FlinkSession, create_lab_objects
from scripts.common.paths import get_project_root

LAB_TABLES = ("remediation_decisions", "service_health")  # drop downstream first
LAB_AGENTS = ("release_agent", "capacity_agent")
SOURCE_TOPICS = ("service_requests", "service_deployments", "service_incidents")
LAB_SUBJECT_TOPICS = ("service_health", "remediation_decisions")
OPS_SIM_TOKENS = ("ops-sim", "opsgen.cli", "opsgen")
TERMINAL = {"COMPLETED", "FAILED", "STOPPED"}


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


def _stop_running_statements(session: FlinkSession) -> None:
    print("\nStopping your running Flink statements...")
    stopped = 0
    for st in session.list_statements():
        name = st.get("name", "")
        phase = st.get("status", {}).get("phase", "")
        if phase and phase not in TERMINAL:
            session.stop(name)
            stopped += 1
    print(f"  Stopped {stopped} statement(s).")


def _drop_objects(session: FlinkSession) -> list[str]:
    problems: list[str] = []
    print("\nDropping the lab objects...")
    statements = (
        [(f"DROP TABLE IF EXISTS `{t}`;", t) for t in LAB_TABLES]
        + [(f"DROP AGENT IF EXISTS `{a}`;", a) for a in LAB_AGENTS]
        + [("DROP TABLE IF EXISTS `service_health`;", "service_health")]
    )
    for sql, obj in statements:
        try:
            name = session.submit(sql, label="aiops-reset")
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


def _kafka_admin(card: dict):
    from confluent_kafka.admin import AdminClient

    return AdminClient(
        {
            "bootstrap.servers": card["AIOPS_KAFKA_BOOTSTRAP"].split("://", 1)[-1],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": card["AIOPS_KAFKA_API_KEY"],
            "sasl.password": card["AIOPS_KAFKA_API_SECRET"],
        }
    )


def _clear_source_topics(card: dict) -> list[str]:
    from confluent_kafka import OFFSET_END, TopicPartition

    problems: list[str] = []
    print("\nClearing the source topics...")
    admin = _kafka_admin(card)
    try:
        present = set(admin.list_topics(timeout=30).topics)
    except Exception as e:
        print(f"  Could not list topics: {e}")
        return [f"could not list topics ({e})"]
    for topic in SOURCE_TOPICS:
        if topic not in present:
            print(f"  Topic {topic}: MISSING — ask your platform team to re-apply")
            problems.append(f"source topic {topic} is missing")
            continue
        meta = admin.list_topics(topic=topic, timeout=30).topics[topic]
        parts = [TopicPartition(topic, p, OFFSET_END) for p in meta.partitions]
        errors = []
        for tp, fut in admin.delete_records(parts).items():
            try:
                fut.result()
            except Exception as e:
                errors.append((tp.partition, e))
        if errors:
            print(f"  Topic {topic}: {len(errors)} partition(s) failed")
            problems.append(f"could not clear {topic}")
        else:
            print(f"  Topic {topic}: cleared")
    return problems


def _delete_lab_subjects(card: dict) -> list[str]:
    problems: list[str] = []
    print("\nDeleting the lab Schema Registry subjects...")
    base = card["AIOPS_SCHEMA_REGISTRY_URL"].rstrip("/")
    auth = (card["AIOPS_SR_API_KEY"], card["AIOPS_SR_API_SECRET"])
    for topic in LAB_SUBJECT_TOPICS:
        for subject in (f"{topic}-key", f"{topic}-value"):
            try:
                requests.delete(f"{base}/subjects/{subject}", auth=auth, timeout=30)  # soft
                hard = requests.delete(f"{base}/subjects/{subject}?permanent=true", auth=auth, timeout=30)
            except requests.RequestException as e:
                print(f"  SR {subject}: FAILED — {e}")
                problems.append(f"delete subject {subject}")
                continue
            if hard.status_code in (200, 404):
                print(f"  SR {subject}: cleaned")
            else:
                print(f"  SR {subject}: HTTP {hard.status_code}")
    return problems


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reset the workshop to a blank slate (troubleshooting only)")
    parser.add_argument("--creds", help="Path to the credential card (default: resolved from credentials.env)")
    parser.add_argument("--with-labs", action="store_true", help="Rebuild the lab statements from sql/ after clearing")
    parser.add_argument("--keep-source", action="store_true", help="Keep the accumulated data in the source topics")
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
    _stop_running_statements(session)
    problems += _drop_objects(session)
    problems += _delete_lab_subjects(card)
    if not args.keep_source:
        problems += _clear_source_topics(card)

    # Local generator state + ground truth.
    state_path(root).unlink(missing_ok=True)
    incidents_path(root).unlink(missing_ok=True)
    print("\nDeleted the generator's persisted state and incidents.jsonl.")

    if args.with_labs:
        print("\nRebuilding the lab statements from sql/...")
        if not create_lab_objects(card, root):
            problems.append("rebuild labs (--with-labs)")

    if problems:
        print("\nReset finished with problems: " + "; ".join(problems))
        sys.exit(1)
    print("\nReset complete. Start again with `uv run ops-sim`.")


if __name__ == "__main__":
    main()
