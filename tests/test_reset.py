"""`uv run reset` must purge every source topic the generator writes to, not just
the two the walkthrough mentions by name. Pinning the list stops the ground-truth
topic from silently dropping off a future refactor."""

from scripts.reset import SOURCE_TOPICS


def test_source_topics_include_service_incidents():
    assert "service_incidents" in SOURCE_TOPICS


def test_source_topics_are_exactly_the_generator_streams():
    # Mirror of opsgen/kafka_io.py: the three topics the KafkaSink writes to.
    assert set(SOURCE_TOPICS) == {"service_requests", "service_deployments", "service_incidents"}
