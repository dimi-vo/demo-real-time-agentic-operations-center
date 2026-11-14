-- Output table: service_health (one row per service per 10-second window, append-only)
--
-- SINGLE SOURCE OF TRUTH for the service_health schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table
-- together with the source tables, so it exists before lab 2.1. Attendees only
-- write the INSERT in sql/2.4_service_health.sql; ops-wall and `uv run evaluate`
-- read it, and the two agent INSERTs in section 4 read it too. (D-050)
--
-- The 23 columns are the 23 columns of the 2.3 status query, in the same order: the
-- INSERT is positional, and tests/test_sql_pipeline.py checks the two lists against
-- each other. Columns 16-17 and 19-21 are all DOUBLE, so a swapped pair would
-- type-check silently; the test is what catches it.
--
-- detected_at, the last column, is when the detector produced the row's verdict:
-- CURRENT_TIMESTAMP evaluated per row. It is the one TIMESTAMP_LTZ column, an instant, on
-- purpose: the Cloud Console workspace submits statements with sql.local-time-zone set to
-- the browser's zone (GMT+01:00 was observed), and a cast to TIMESTAMP(3) would store that
-- local wall-clock time, not UTC. Its gap to window_end is the real-time delay; the ops
-- wall draws each anomaly from it (D-054).
--
-- Types: only the two status columns were ever observed on Confluent Cloud
-- (VARCHAR(10), D-025). The rest are derived from the SQL (COUNT is BIGINT, SUM of
-- INT is INT, window bounds are NOT NULL, detector fields are nullable during warm-up)
-- and are proven by the first apply: if a type is wrong, DROP TABLE service_health,
-- fix this file and statements.tf together, and apply again. IF NOT EXISTS never
-- changes an existing table.
--
-- The status columns are declared STRING on purpose: a declared VARCHAR(10) would
-- truncate any longer value the query learns to write.
--
-- Append-only and read-uncommitted, like the source tables. Delete cleanup (not
-- compaction) with 24h matches them and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the
-- order. No custom watermark: the default $rowtime is the Kafka record time the
-- INSERT writes.

CREATE TABLE IF NOT EXISTS `service_health` (
  `service_name` STRING COMMENT 'River Shop service this window describes',
  `window_start` TIMESTAMP(3) NOT NULL COMMENT 'Start of the 10-second tumbling window',
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window; with service_name, the row identity',
  `request_count` BIGINT NOT NULL COMMENT 'Requests in the window (COUNT(*))',
  `error_count` INT NOT NULL COMMENT 'Requests with status_code >= 500',
  `error_rate` DOUBLE NOT NULL COMMENT 'error_count / request_count',
  `avg_latency_ms` DOUBLE COMMENT 'Mean request latency in the window',
  `max_latency_ms` DOUBLE COMMENT 'Slowest request in the window',
  `version` STRING COMMENT 'Version live at the end of the window (temporal join, LAST_VALUE)',
  `previous_version` STRING COMMENT 'Version before it; null until the first deploy',
  `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK',
  `seconds_since_deploy` INT COMMENT 'Seconds from the last deploy to window_end',
  `ready_replicas` INT COMMENT 'Ready replicas at the end of the window',
  `tier` STRING COMMENT 'CRITICAL or STANDARD',
  `latency_status` STRING NOT NULL COMMENT 'WARMING_UP, HIGH or NORMAL',
  `expected_latency_ms` DOUBLE COMMENT 'Detector forecast for avg_latency_ms; null while warming up',
  `latency_upper_bound` DOUBLE COMMENT 'Upper bound of the latency band',
  `traffic_status` STRING NOT NULL COMMENT 'WARMING_UP, HIGH, LOW or NORMAL',
  `expected_request_count` DOUBLE COMMENT 'Detector forecast for request_count',
  `traffic_lower_bound` DOUBLE COMMENT 'Lower bound of the traffic band',
  `traffic_upper_bound` DOUBLE COMMENT 'Upper bound of the traffic band',
  `error_rate_breach` BOOLEAN NOT NULL COMMENT 'error_rate above 5 percent',
  `detected_at` TIMESTAMP_LTZ(3) NOT NULL COMMENT 'When the detector produced this verdict (CURRENT_TIMESTAMP, an instant)'
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
