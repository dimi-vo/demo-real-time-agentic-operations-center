-- 4.3 The fan-in table. Both agents write here, so it is created explicitly rather than
-- with CTAS: two INSERT statements cannot share a CREATE TABLE AS.
--
-- Deliberately small (D-034): what happened, who answered, what to do. Everything the
-- agent was told is already in `service_health` for the same (service_name, window_end),
-- so the dashboard and `uv run evaluate` join it there instead of storing it twice.
--
-- `trigger` and `action` are quoted because they are reserved words in Flink SQL.
CREATE TABLE `remediation_decisions` (
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the service_health window the decision was made on',
  `service_name` STRING NOT NULL,
  `agent_name` STRING NOT NULL COMMENT 'release_agent or capacity_agent',
  `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
  `action` STRING NOT NULL COMMENT 'ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, OBSERVE, or INVALID',
  `target` STRING COMMENT 'Version for ROLLBACK, replica count for SCALE_OUT, otherwise null',
  `reason` STRING COMMENT 'The agent explanation, one or two sentences',
  `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null'
) WITH (
  'changelog.mode' = 'append',
  'kafka.consumer.isolation-level' = 'read-uncommitted'
);
