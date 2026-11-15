-- Source table: service_deployments (upsert, current state per service)
--
-- SINGLE SOURCE OF TRUTH for the service_deployments schema. The platform team's
-- Terraform creates this table; the generator (opsgen) and dashboard serialize
-- against the schema Flink registers from this DDL. The .avsc copy in schemas/ is
-- for --dry-run and the drift test only.
--
-- Current deployment state of each service, as a CI/CD system or orchestrator
-- would report it: one upsert row per service, keyed by service_name. The
-- generator re-emits every service's current state every 5 seconds (a controller
-- reconciliation loop) so the versioned side's watermark keeps advancing between
-- changes; without it the event-time temporal join against service_metrics would
-- stall. The watermark trails event_time by 1 ms (Flink's ascending-timestamps strategy:
-- the heartbeat stamps every service with the same tick, and a row stamped at the
-- watermark itself would be late), so the heartbeat on a window's end tick lets that
-- window close at once. A longer delay makes every window wait for the next heartbeat:
-- with 5 s, detection waited 5 s more (D-064). The Kafka message key is Avro-encoded against the registered
-- service_deployments-key subject, mirroring the reference race_standings.
--
-- DELETE RETENTION, NOT COMPACTION (DECISIONS.md D-006): the temporal join and the
-- anomaly detector replay this topic from the earliest offset and must attribute
-- each historical request to the version that was live at that request's event
-- time. Compaction would keep only the latest version per service and silently
-- mis-attribute old requests. 24h retention caps storage while covering a day.

CREATE TABLE IF NOT EXISTS `service_deployments` (
  `service_name` STRING COMMENT 'Service identifier (primary key)',
  `version` STRING COMMENT 'Currently running version (semver)',
  `previous_version` STRING COMMENT 'Version running before the current one; null until the first deploy',
  `deployed_at` TIMESTAMP(3) COMMENT 'When the current version went live',
  `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK (SCALE is reserved for a later project)',
  `desired_replicas` INT COMMENT 'Desired replica count (equal to ready_replicas in this phase)',
  `ready_replicas` INT COMMENT 'Ready replica count (fixed per service in this phase)',
  `tier` STRING COMMENT 'CRITICAL or STANDARD',
  `owner_team` STRING COMMENT 'Team that owns the service',
  `event_time` TIMESTAMP(3) COMMENT 'Emit time of this state record (changes on every re-emit)',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND,
  PRIMARY KEY (`service_name`) NOT ENFORCED
) DISTRIBUTED BY (`service_name`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'kafka.retention.time' = '24 h',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
