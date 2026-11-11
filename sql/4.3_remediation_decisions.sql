-- 4.3 The fan-in table. Both agents write here, so it is created explicitly rather than
-- with CTAS: two INSERT statements cannot share a CREATE TABLE AS.
--
-- `trigger` and `action` are quoted because they are reserved words in Flink SQL.
-- The context snapshot (version ... max_replicas) is what the agent was told, kept next
-- to what it answered so a decision can be judged without re-joining service_health.
CREATE TABLE `remediation_decisions` (
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the service_health window the decision was made on',
  `service_name` STRING NOT NULL,
  `agent_name` STRING NOT NULL COMMENT 'release_agent or capacity_agent',
  `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
  `action` STRING NOT NULL COMMENT 'The agent choice, or INVALID if it answered outside its allowed set',
  `target_version` STRING COMMENT 'Set for ROLLBACK, otherwise null',
  `target_replicas` INT COMMENT 'Set for SCALE_OUT, otherwise null',
  `confidence` STRING COMMENT 'HIGH, MEDIUM or LOW',
  `summary` STRING,
  `reasoning` STRING,
  `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null',
  `version` STRING,
  `seconds_since_deploy` INT,
  `avg_latency_ms` DOUBLE,
  `expected_latency_ms` DOUBLE,
  `request_count` BIGINT,
  `expected_request_count` DOUBLE,
  `error_rate` DOUBLE,
  `ready_replicas` INT,
  `max_replicas` INT
) DISTRIBUTED BY (`service_name`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'kafka.consumer.isolation-level' = 'read-uncommitted'
);
