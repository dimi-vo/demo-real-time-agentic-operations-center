-- Source table: service_requests (append-only request log)
--
-- SINGLE SOURCE OF TRUTH for the service_requests schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) and the dashboard (ops-wall) serialize/deserialize
-- against whatever schema Flink registers in Schema Registry from this DDL. The
-- .avsc copy in schemas/ is for --dry-run and the schema-vs-DDL drift test only
-- (tests/test_schema_vs_ddl.py), never for live serialization.
--
-- One immutable row per HTTP request across the six River Shop services. There is
-- no primary key and no version/replica columns here on purpose: the request log
-- does not know which deployment served it. That is why the walkthrough joins this
-- to service_deployments (temporal join). The Kafka message key is the raw service
-- name string (no registered key schema), mirroring the reference car_telemetry.
--
-- delete retention (not compaction) with 24h: an append-only log is never
-- compacted, and 24h caps storage per attendee while covering a workshop day.

CREATE TABLE IF NOT EXISTS `service_requests` (
  `event_time` TIMESTAMP(3) COMMENT 'Wall-clock time of the request, within the current 1s tick; also the Kafka record timestamp',
  `service_name` STRING COMMENT 'Which River Shop service served the request (foreign key to service_deployments)',
  `instance_id` STRING COMMENT 'Serving instance: {service}-{version_hash}-{replica_suffix}, like a Kubernetes pod',
  `http_method` STRING COMMENT 'HTTP method of the route (GET, POST, DELETE)',
  `route` STRING COMMENT 'Templated route, e.g. GET /api/v1/products/{id}, so cardinality stays low',
  `status_code` INT COMMENT 'HTTP status code (2xx success, 4xx client error, 5xx server error)',
  `latency_ms` DOUBLE COMMENT 'Request latency in milliseconds',
  `response_bytes` INT COMMENT 'Response body size in bytes (~200 for error responses)',
  `trace_id` STRING COMMENT '32 random hex characters identifying the request trace',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
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
