-- Source table: ticket_answer_key (simulator ground truth for tickets, append-only)
--
-- SINGLE SOURCE OF TRUTH for the ticket_answer_key schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) writes to it and the dashboard (ops-wall) and
-- `uv run evaluate` read from it. The .avsc copy in schemas/ is for --dry-run and the
-- schema-vs-DDL drift test only (tests/test_schema_vs_ddl.py), never for live
-- serialization.
--
-- FOR THE WALL AND EVALUATE ONLY. The simulator knows what each support ticket really
-- is; the agents must work it out from the ticket's text. This topic publishes the
-- truth so the wall and evaluate can score the classifier and the responder, the way
-- service_incidents is the answer key for metric incidents (D-046, D-068). No lab and
-- no agent reads it. One record per ticket, every priority, on the same tick as the
-- ticket in support_tickets, tied to it by ticket_id. Append-only so the history can be
-- replayed. No key columns: the generator sends the raw ticket_id string as the message
-- key, and ticket_id stays in the value. (DISTRIBUTED BY HASH would make it a key
-- column, which Confluent Cloud registers as an Avro key the generator doesn't write;
-- one bucket keeps the order anyway.)
--
-- delete retention (not compaction) with 24h matches service_metrics: an
-- append-only log is never compacted, and 24h caps storage per attendee.

CREATE TABLE IF NOT EXISTS `ticket_answer_key` (
  `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created (its support_tickets event_time); also the Kafka record timestamp',
  `ticket_id` STRING COMMENT 'The ticket this answers for (support_tickets.ticket_id)',
  `true_severity` STRING COMMENT 'What the simulator meant: HIGH (many customers cannot buy or pay) or LOW; P2 and P3 are always LOW',
  `expected_action` STRING COMMENT 'For a HIGH ticket, what the responder should pick: ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM; NULL when LOW',
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
