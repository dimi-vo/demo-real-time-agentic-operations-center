# Confluent Intelligence: Self-Healing IT

> 🚧 Under construction. This repository is being built from a build brief; see
> [`PLAN.md`](PLAN.md) for the plan and [`DECISIONS.md`](DECISIONS.md) for decisions.

A hands-on Confluent Cloud workshop: three **River Shop** microservices, three stories and
four Streaming Agents. web-gateway and payments stream one metrics sample per second each
(traffic, latency, errors) and their deployment state into Kafka; Flink SQL enriches them
with a temporal join and aggregates them into per-service windows. **web-gateway**'s bad
deploys trip a fixed error-rate threshold, and the release agent rolls them back.
**payments** is watched by built-in ML (`ML_DETECT_ANOMALIES`, ARIMA) on latency and
traffic, with the release and capacity agents. **checkout** sends support tickets, as if
from Zendesk and ServiceNow: Flink routes the P1s, a classifier agent recovers their real
severity into `ticket_triage`, and a responder agent recommends an action. The release,
capacity and responder agents write to `remediation_decisions`, which is exposed to coding
agents through the Real-Time Context Engine and queried over REST with Lightning Queries.

This workshop ships the **pre-provisioned track**: a platform team provisions one
Confluent Cloud environment per developer with the [`platform-team/`](platform-team/)
Terraform module, and each developer follows
[`docs/tracks/PRE-PROVISIONED.md`](docs/tracks/PRE-PROVISIONED.md).
