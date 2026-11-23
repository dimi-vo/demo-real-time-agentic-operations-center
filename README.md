# Confluent Intelligence: Self-Healing IT

> 🚧 Under construction. This repository is being built from a build brief; see
> [`PLAN.md`](PLAN.md) for the plan and [`DECISIONS.md`](DECISIONS.md) for decisions.

A hands-on Confluent Cloud workshop. Three **River Shop** microservices stream one
metrics sample per second (traffic, latency, errors) and their deployment state into
Kafka; Flink SQL enriches them with a temporal join,
aggregates them into per-service windows, and uses built-in ML
(`ML_DETECT_ANOMALIES`) to flag latency and traffic anomalies. Two Streaming Agents
then recommend remediations, which are exposed to coding agents through the
Real-Time Context Engine and queried over REST with Lightning Queries.

This workshop ships the **pre-provisioned track**: a platform team provisions one
Confluent Cloud environment per developer with the [`platform-team/`](platform-team/)
Terraform module, and each developer follows
[`docs/tracks/PRE-PROVISIONED.md`](docs/tracks/PRE-PROVISIONED.md).
