-- Output table: remediation_decisions, the fan-in table every remediating agent writes to.
--
-- SINGLE SOURCE OF TRUTH for the remediation_decisions schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table up
-- front. Three agents write it, in four INSERT statements, which is why it is an explicit
-- table and not the result of one CREATE TABLE AS: three in lab 4.3 (the release agent on
-- error rates and on latency, the capacity agent) and one in lab 5.3 (the ticket responder,
-- on the tickets the classifier called HIGH). ops-sim, ops-wall and `uv run evaluate` read
-- it. (D-050, D-065, D-067, D-068)
--
-- Deliberately small (D-034): what happened, who answered, what to do. Everything the
-- agent was told is already in `service_health` (or, for ERROR_RATE_HIGH, in
-- `service_error_rate`) for the same (service_name, window_end), or, for a ticket
-- decision, in `ticket_triage` for the same ticket_id, so the dashboard and
-- `uv run evaluate` join it there instead of storing it twice.
--
-- A ticket decision (D-068) has the ticket's id in ticket_id, the ticket's event_time in
-- window_end (the column keeps its name), agent_name ticket_responder, trigger
-- HIGH_SEVERITY_TICKET and a null target. The three 4.3 legs write a null ticket_id. Only
-- a ROLLBACK carries a target.
--
-- `trigger` and `action` are quoted because they are reserved words in Flink SQL.
--
-- Append-only and read-uncommitted, like the source tables. Delete cleanup (not
-- compaction) with 24h matches them and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the
-- order. IF NOT EXISTS never changes an existing table: a changed DDL needs a
-- DROP TABLE first. ticket_id came after the first stacks were built, so those drop this
-- table before the apply that recreates it (platform-team/README.md).

CREATE TABLE IF NOT EXISTS `remediation_decisions` (
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window (service_health or service_error_rate) the decision was made on, or the ticket event_time for a ticket decision',
  `service_name` STRING NOT NULL,
  `agent_name` STRING NOT NULL COMMENT 'release_agent, capacity_agent or ticket_responder',
  `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, ERROR_RATE_HIGH, TRAFFIC_HIGH, TRAFFIC_LOW or HIGH_SEVERITY_TICKET',
  `action` STRING NOT NULL COMMENT 'ROLLBACK, SCALE_OUT (from ticket_responder only), INVESTIGATE_UPSTREAM, OBSERVE, or INVALID',
  `target` STRING COMMENT 'Version for ROLLBACK from the release agent, otherwise null',
  `reason` STRING COMMENT 'The agent explanation, one or two sentences',
  `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null',
  `ticket_id` STRING COMMENT 'The ticket a ticket_responder decision is about; null for the metric agents'
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'kafka.retention.time' = '24 h',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
