"""Empty Kafka topics without dropping them: the shared half of `reset` and `reset-health`.

Every workshop topic has delete cleanup (the DDL in `sql/` sets it), so Kafka's
delete-records call can move each partition's start offset up to its end. The topic,
its schema subject and the table on top of it stay in place; only the records go.
"""

from __future__ import annotations

# The five topics the generator writes to (checkout's tickets and their answer key since D-068),
# and the five the labs' INSERTs write to (lab 2.2's error-rate rule, D-067; the ARIMA
# detection; the agents' decisions; lab 5.1's P1 tickets and lab 5.2's verdicts, D-068). All
# ten are created by the platform team's Terraform (D-050).
SOURCE_TOPICS = ("service_metrics", "service_deployments", "service_incidents", "support_tickets", "ticket_answer_key")
OUTPUT_TOPICS = ("service_error_rate", "service_health", "remediation_decisions", "p1_tickets", "ticket_triage")


def kafka_admin(card: dict):
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


def empty_topics(card: dict, topics: tuple[str, ...] | list[str], heading: str) -> list[str]:
    """Delete every record in each of `topics`, printing one line per topic.

    Returns the problems found (empty when all went well). A topic that is missing is a
    problem, not something to skip: the platform team's Terraform creates them all, so a
    missing one means the apply never finished."""
    from confluent_kafka import OFFSET_END, TopicPartition

    problems: list[str] = []
    print(f"\n{heading}")
    admin = kafka_admin(card)
    try:
        present = set(admin.list_topics(timeout=30).topics)
    except Exception as e:
        print(f"  Could not list topics: {e}")
        return [f"could not list topics ({e})"]
    for topic in topics:
        if topic not in present:
            print(f"  Topic {topic}: MISSING — ask your platform team to re-apply")
            problems.append(f"topic {topic} is missing")
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
