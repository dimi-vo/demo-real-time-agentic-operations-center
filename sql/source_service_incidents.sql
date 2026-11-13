-- Source table: service_incidents (simulator ground truth, append-only)
--
-- SINGLE SOURCE OF TRUTH for the service_incidents schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) writes to it and the dashboard (ops-wall) reads from it.
-- The .avsc copy in schemas/ is for --dry-run and the schema-vs-DDL drift test
-- only (tests/test_schema_vs_ddl.py), never for live serialization.
--
-- The simulator knows which incidents it injected; the detector and the agents do
-- not. This topic publishes the truth so a dashboard (or a future Flink job) can
-- compare "what happened" against "what we caught". Two records per incident:
-- one at degradation_start (ended_at NULL) and one at degradation_end (both
-- timestamps set), tied together by incident_id. Append-only so the ground-truth
-- history can be replayed; keyed by service_name for ordering per service.
--
-- delete retention (not compaction) with 24h matches service_requests: an
-- append-only log is never compacted, and 24h caps storage per attendee.

CREATE TABLE IF NOT EXISTS `service_incidents` (
  `event_time` TIMESTAMP(3) COMMENT 'When this record was emitted; also the Kafka record timestamp',
  `incident_id` STRING COMMENT 'Stable id for this incident across its start and end records',
  `service_name` STRING COMMENT 'Service the incident affects (foreign key to service_deployments)',
  `incident_type` STRING COMMENT 'BAD_DEPLOY, TRAFFIC_SURGE or UPSTREAM_DROP',
  `started_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation began (for BAD_DEPLOY, the same tick as the deploy)',
  `ended_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation ended; NULL on the start record, set on the end record',
  `expected_trigger` STRING COMMENT 'What service_health should flag: LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
  `expected_action` STRING COMMENT 'What the agent should pick: ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(`service_name`) INTO 1 BUCKETS
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
