-- Source table: service_metrics (append-only, one metrics sample per service per second)
--
-- SINGLE SOURCE OF TRUTH for the service_metrics schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) and the dashboard (ops-wall) serialize/deserialize
-- against whatever schema Flink registers in Schema Registry from this DDL. The
-- .avsc copy in schemas/ is for --dry-run and the schema-vs-DDL drift test only
-- (tests/test_schema_vs_ddl.py), never for live serialization.
--
-- One immutable row per River Shop service per second, whatever the traffic: each row
-- sums up its one-second slice of time (D-058). An incident changes the values, never
-- the number of rows. There is no primary key and no version/replica columns here on
-- purpose: a metrics sample does not know which deployment served the requests it
-- counts. That is why the walkthrough joins this to service_deployments (temporal join).
-- event_time is the end of the second (tick + 999 ms), after a deploy record on the
-- same tick, so the join gives that second the new version. The Kafka message key is
-- the raw service name string (no registered key schema), mirroring the reference
-- car_telemetry.
--
-- The watermark trails event_time by 1 s: one producer writes the topic's one partition
-- in order, so no sample arrives late, and the tick at a window's end carries the
-- watermark past it. With service_deployments' watermark, a window closes on that tick:
-- detection waits for the window and nothing more (D-064).
--
-- delete retention (not compaction) with 24h: an append-only log is never
-- compacted, and 24h caps storage per attendee while covering a workshop day.

CREATE TABLE IF NOT EXISTS `service_metrics` (
  `event_time` TIMESTAMP(3) COMMENT 'End of the second this sample covers (tick + 999 ms); also the Kafka record timestamp',
  `service_name` STRING COMMENT 'Which River Shop service the sample describes (foreign key to service_deployments)',
  `traffic_count` INT COMMENT 'Requests served in that second',
  `latency_ms` DOUBLE COMMENT 'Mean latency of the requests in that second, in milliseconds; set even when traffic_count is 0',
  `error_count` INT COMMENT 'Failed requests in that second; lab 2.2 reads it for web-gateway',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '1' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'kafka.message-timestamp-type' = 'create-time',
  'kafka.retention.time' = '24 h',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
