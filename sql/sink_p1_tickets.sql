-- Output table: p1_tickets (checkout's P1 support tickets, append-only)
--
-- SINGLE SOURCE OF TRUTH for the p1_tickets schema. The platform team's Terraform
-- (platform-team/modules/developer/statements.tf) creates this table together with the
-- others, so it exists before section 5. Lab 5.1 writes it: an INSERT that copies the P1
-- tickets out of support_tickets, column for column. Lab 5.2 reads it: the classifier
-- agent gets one call per P1 ticket. ops-wall and `uv run evaluate` read it too. (D-050,
-- D-068)
--
-- The same seven columns and options as support_tickets, 5 s watermark included, so the
-- filter is a plain WHERE on priority and nothing else changes on the way. A P2 or P3
-- ticket in here is a filter leak, which `uv run evaluate` reports.
--
-- Read-uncommitted matters here: Flink commits its sink transactions about once a minute,
-- so a committed reader of this table would see each ticket up to a minute late, and the
-- classifier would wait that long for every P1 (D-063). Delete cleanup (not compaction)
-- with 24h matches the source tables and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the order.
-- IF NOT EXISTS never changes an existing table: a changed DDL needs a DROP TABLE first.

CREATE TABLE IF NOT EXISTS `p1_tickets` (
  `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created, copied from support_tickets',
  `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
  `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
  `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
  `priority` STRING COMMENT 'Always P1: lab 5.1 copies only the P1 tickets, so a P2 or P3 here is a filter leak',
  `subject` STRING COMMENT 'The ticket subject line',
  `description` STRING COMMENT 'The ticket text: one to three sentences in English',
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
