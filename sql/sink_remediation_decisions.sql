-- Output table: remediation_decisions, the fan-in table both agents write to.
--
-- SINGLE SOURCE OF TRUTH for the remediation_decisions schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table up
-- front; three INSERT statements (lab 4.3: the release agent on error rates and on latency,
-- the capacity agent) write it, which is why it is an explicit table and not the result of
-- one CREATE TABLE AS. ops-wall and `uv run evaluate` read it. (D-050, D-067)
--
-- Deliberately small (D-034): what happened, who answered, what to do. Everything the
-- agent was told is already in `service_health` (or, for ERROR_RATE_HIGH, in
-- `service_error_rate`) for the same (service_name, window_end), so the dashboard and
-- `uv run evaluate` join it there instead of storing it twice.
--
-- `trigger` and `action` are quoted because they are reserved words in Flink SQL.
--
-- Append-only and read-uncommitted, like the source tables. Delete cleanup (not
-- compaction) with 24h matches them and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the
-- order. IF NOT EXISTS never changes an existing table: a changed DDL needs a
-- DROP TABLE first.

CREATE TABLE IF NOT EXISTS `remediation_decisions` (
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window (service_health or service_error_rate) the decision was made on',
  `service_name` STRING NOT NULL,
  `agent_name` STRING NOT NULL COMMENT 'release_agent or capacity_agent',
  `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, ERROR_RATE_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
  `action` STRING NOT NULL COMMENT 'ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, OBSERVE, or INVALID',
  `target` STRING COMMENT 'Version for ROLLBACK, replica count for SCALE_OUT, otherwise null',
  `reason` STRING COMMENT 'The agent explanation, one or two sentences',
  `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null'
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
