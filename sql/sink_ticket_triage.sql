-- Output table: ticket_triage (the classifier's verdict on each P1 ticket, append-only)
--
-- SINGLE SOURCE OF TRUTH for the ticket_triage schema. The platform team's Terraform
-- (platform-team/modules/developer/statements.tf) creates this table together with the
-- others, so it exists before section 5. Lab 5.2 writes it: the classifier agent's
-- answer for each p1_tickets row, one row per ticket. Lab 5.3 reads its HIGH rows: the
-- responder agent gets one call per ticket the classifier called HIGH, and writes its
-- decision to remediation_decisions. ops-wall and `uv run evaluate` read it too. (D-050,
-- D-068)
--
-- The seven ticket columns come first, copied from p1_tickets, so the responder reads
-- one table without a join. Then the verdict: severity is HIGH or LOW when the call
-- succeeded and the answer parsed to one of them, and INVALID otherwise (never null, like
-- remediation_decisions.action); reason is the classifier's explanation, which lab 5.3
-- passes on to the responder; raw_response keeps the full answer. Nothing here says what
-- the ticket really is: that is in ticket_answer_key, which no agent reads.
--
-- The same options as p1_tickets, 5 s watermark included. Read-uncommitted matters here
-- too: a committed reader would see each verdict up to a minute late, and the responder
-- would wait that long (D-063). Delete cleanup (not compaction) with 24h is what lets
-- `uv run reset` and `uv run reset-health` empty the topic with delete-records. One
-- bucket keeps the order. IF NOT EXISTS never changes an existing table: a changed DDL
-- needs a DROP TABLE first.

CREATE TABLE IF NOT EXISTS `ticket_triage` (
  `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created, copied from p1_tickets',
  `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
  `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
  `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
  `priority` STRING COMMENT 'Always P1, as in p1_tickets',
  `subject` STRING COMMENT 'The ticket subject line',
  `description` STRING COMMENT 'The ticket text: one to three sentences in English',
  `severity` STRING NOT NULL COMMENT 'The classifier verdict: HIGH or LOW, or INVALID when the call failed or the answer did not parse',
  `reason` STRING COMMENT 'The classifier explanation, one or two sentences; lab 5.3 passes it to the responder',
  `raw_response` STRING COMMENT 'The full classifier answer, kept for debugging when parsing returns null',
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
