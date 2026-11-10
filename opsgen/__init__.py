"""opsgen — the River Shop AIOps traffic and incident generator.

`uv run ops-sim` streams per-service request logs (`service_requests`) and
deployment state (`service_deployments`) to Confluent Cloud, with benign deploys
and recurring incidents, so a Flink pipeline can detect anomalies and agents can
recommend remediations. Everything here is offline-testable; the Kafka path lives
in `opsgen/kafka_io.py` (Phase 2).
"""
