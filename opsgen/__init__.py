"""opsgen — the River Shop AIOps traffic and incident generator.

`uv run ops-sim` streams one metrics sample per second for each metric service, web-gateway
and payments (`service_metrics`), their deployment state (`service_deployments`) and the
incident answer key (`service_incidents`) to Confluent Cloud, with recurring incidents,
so a Flink pipeline can detect anomalies and agents can recommend remediations. For
checkout it streams support tickets (`support_tickets`) and their answer key
(`ticket_answer_key`) instead (D-068). A scanner's CVE findings in River Shop's repositories
go to `cve_scans`, with their answer key in `cve_answer_key` (D-070).
Everything here is offline-testable; the Kafka path lives in `opsgen/kafka_io.py`
(Phase 2).
"""
