-- Output table: service_error_rate (one row per web-gateway 10-second window, append-only)
--
-- SINGLE SOURCE OF TRUTH for the service_error_rate schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table
-- together with the others, so it exists before lab 2.1. Attendees only write the
-- INSERT in lab 2.2: web-gateway's error rate over each window against a fixed 25 %
-- threshold (D-067). The release agent's error-rate INSERT (lab 4.3), ops-wall and
-- `uv run evaluate` read it.
--
-- The 12 columns are the 12 columns of the 2.2 query, in the same order: the INSERT is
-- positional. error_status is what the agent reads: HIGH when the window's errors are at
-- least 25 % of its requests, else NORMAL (a window with no requests is NORMAL, and its
-- error_rate is null). Every window is written, not only the HIGH ones, so the table shows
-- a normal window next to a breach. No tier or ready_replicas: this lab doesn't use them.
--
-- detected_at, the last column, is when the rule produced the row's verdict:
-- CURRENT_TIMESTAMP evaluated per row, a TIMESTAMP_LTZ instant for the reason given in
-- sql/sink_service_health.sql (the workspace's local time zone would shift a TIMESTAMP).
--
-- Types are derived from the SQL (SUM of a nullable INT is a nullable INT, window bounds
-- are NOT NULL, the CASE always yields a value) and are proven by the first apply: if a
-- type is wrong, DROP TABLE service_error_rate, fix this file and statements.tf together,
-- and apply again. IF NOT EXISTS never changes an existing table. error_status is
-- declared STRING on purpose, like service_health's status columns (D-025).
--
-- Append-only and read-uncommitted, like the other tables. Delete cleanup (not
-- compaction) with 24h matches them and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the
-- order. No custom watermark: the default $rowtime is the Kafka record time the
-- INSERT writes.

CREATE TABLE IF NOT EXISTS `service_error_rate` (
  `service_name` STRING COMMENT 'River Shop service this window describes (web-gateway)',
  `window_start` TIMESTAMP(3) NOT NULL COMMENT 'Start of the 10-second tumbling window',
  `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window; with service_name, the row identity',
  `traffic_count` INT COMMENT 'Requests in the window (SUM of the per-second traffic_count)',
  `error_count` INT COMMENT 'Failed requests in the window (SUM of the per-second error_count)',
  `error_rate` DOUBLE COMMENT 'error_count / traffic_count, from 0 to 1; null when the window has no requests',
  `error_status` STRING NOT NULL COMMENT 'HIGH when errors are at least 25 percent of the requests, else NORMAL',
  `version` STRING COMMENT 'Version live at the end of the window (temporal join, LAST_VALUE)',
  `previous_version` STRING COMMENT 'Version before it; null until the first deploy',
  `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK',
  `seconds_since_deploy` INT COMMENT 'Seconds from the last deploy to window_end',
  `detected_at` TIMESTAMP_LTZ(3) NOT NULL COMMENT 'When the rule produced this verdict (CURRENT_TIMESTAMP, an instant)'
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
