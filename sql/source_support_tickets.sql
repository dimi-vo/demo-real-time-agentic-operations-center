-- Source table: support_tickets (checkout's support tickets, append-only)
--
-- SINGLE SOURCE OF TRUTH for the support_tickets schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) writes to it, lab 5.1 routes its P1 tickets, and the
-- dashboard (ops-wall) reads from it. The .avsc copy in schemas/ is for --dry-run and
-- the schema-vs-DDL drift test only (tests/test_schema_vs_ddl.py), never for live
-- serialization.
--
-- checkout produces no metrics (D-068). It produces support tickets instead, as if
-- from Zendesk and ServiceNow, about 4 a minute: one record per ticket, written when
-- it is created. The priority is what the reporter picked, and reporters
-- over-prioritise, so a P1 is not always a real outage: an agent recovers the real
-- severity. Nothing here says what the ticket really is; the simulator writes that to
-- ticket_answer_key, which the agents never read. Append-only so the tickets can be
-- replayed. No key columns, exactly like service_incidents: the generator sends the raw
-- service_name string as the message key, and service_name stays in the value. (DISTRIBUTED
-- BY HASH would make service_name a key column, which Confluent Cloud requires first in
-- the schema and registers as an Avro key the generator doesn't write; one bucket keeps
-- the order anyway.)
--
-- delete retention (not compaction) with 24h matches service_metrics: an
-- append-only log is never compacted, and 24h caps storage per attendee.

CREATE TABLE IF NOT EXISTS `support_tickets` (
  `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created; also the Kafka record timestamp',
  `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
  `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
  `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
  `priority` STRING COMMENT 'P1, P2 or P3, as the reporter set it; lab 5.1 routes the P1 tickets',
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
