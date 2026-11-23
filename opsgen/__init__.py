"""opsgen — the River Shop AIOps traffic and incident generator.

`uv run ops-sim` streams one metrics sample per service per second (`service_metrics`),
deployment state (`service_deployments`) and the incident answer key
(`service_incidents`) to Confluent Cloud, with recurring incidents,
so a Flink pipeline can detect anomalies and agents can recommend remediations.
Everything here is offline-testable; the Kafka path lives in `opsgen/kafka_io.py`
(Phase 2).
"""
